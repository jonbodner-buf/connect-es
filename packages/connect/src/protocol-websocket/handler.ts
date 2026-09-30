// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { create } from "@bufbuild/protobuf";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { Code } from "../code.js";
import { ConnectError } from "../connect-error.js";
import { createHandlerContext } from "../implementation.js";
import type { MethodImplSpec } from "../implementation.js";
import {
  applyRequestGate,
  transformInvokeImplementation,
} from "../protocol/invoke-implementation.js";
import { createMethodSerializationLookup } from "../protocol/serialization.js";
import type { MethodSerializationLookup } from "../protocol/serialization.js";
import type { UniversalHandlerOptions } from "../protocol/universal-handler.js";
import { endStreamToJson } from "../protocol-connect/end-stream.js";
import {
  closeCodeForFault,
  closeInternalError,
  closeNormal,
  closeReason,
  decodeFrame,
  decodeJsonPayload,
  encodeFrame,
  encodeJsonFrame,
  faultOf,
  markerBody,
  markerClientEndStream,
  markerMetadata,
  markerName,
  markerServerEndStream,
  paramTimeout,
  peerFault,
  protocolName,
  subprotocolProto,
} from "./framing.js";
import type { WireMessage } from "./framing.js";
import {
  decodeMetadata,
  defaultInfrastructureHeaders,
  encodeMetadata,
  isStrippedHandshakeHeader,
  reservedHeaderReason,
} from "./metadata.js";
import type {
  UniversalServerWebSocket,
  UniversalWebSocketHandler,
  UniversalWebSocketRequest,
} from "./universal-websocket.js";

// setTimeout fires at once for anything above this, which would turn a
// generous bound into an expired one.
const maxTimerMs = 0x7fffffff;

/**
 * Create a handler that serves one RPC over a WebSocket connection, per the
 * Connect-over-WebSocket protocol.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function createWebSocketHandler<
  I extends DescMessage,
  O extends DescMessage,
>(
  opt: UniversalHandlerOptions,
  spec: MethodImplSpec<I, O>,
): UniversalWebSocketHandler {
  const serialization = createMethodSerializationLookup(
    spec.method,
    opt.binaryOptions,
    opt.jsonOptions,
    opt,
  );
  async function handle(req: UniversalWebSocketRequest): Promise<void> {
    const session = new Session(opt, spec, serialization, req);
    await session.serve();
  }
  return Object.assign(handle, { readMaxBytes: opt.readMaxBytes });
}

class Session<I extends DescMessage, O extends DescMessage> {
  private readonly socket: UniversalServerWebSocket;
  private readonly binary: boolean;
  private leadingSent = false;
  private endSent = false;
  private clientEnded = false;
  // The first framing mistake the peer made. It is the RPC's outcome
  // whatever the handler concluded: a handler may catch the error it raised.
  private fault: ConnectError | undefined;
  private context: ReturnType<typeof createHandlerContext> | undefined;

  constructor(
    private readonly opt: UniversalHandlerOptions,
    private readonly spec: MethodImplSpec<I, O>,
    private readonly serialization: MethodSerializationLookup<I, O>,
    private readonly req: UniversalWebSocketRequest,
  ) {
    this.socket = req.socket;
    this.binary = req.subprotocol === subprotocolProto;
  }

  async serve(): Promise<void> {
    const timeout = this.parseTimeout();
    if (timeout.error !== undefined) {
      // The handshake already committed the HTTP response, so a malformed
      // deadline can only be reported in band.
      return this.finish(timeout.error);
    }
    let requestHeader: Headers;
    try {
      requestHeader = await this.readLeadingMetadata();
    } catch (e) {
      return this.finish(e);
    }
    const context = createHandlerContext({
      ...this.spec,
      service: this.spec.method.parent,
      requestMethod: "GET",
      protocolName,
      timeoutMs: timeout.timeoutMs,
      shutdownSignal: this.opt.shutdownSignal,
      requestSignal: undefined,
      requestHeader,
      url: this.req.url,
      contextValues: this.req.contextValues,
    });
    this.context = context;
    const onSocketGone = () => {
      if (!this.endSent) {
        context.abort(
          new ConnectError(
            "websocket closed before end-of-stream",
            Code.Canceled,
          ),
        );
      }
    };
    if (this.socket.signal.aborted) {
      onSocketGone();
    } else {
      this.socket.signal.addEventListener("abort", onSocketGone);
    }
    let error: unknown;
    try {
      const it = await applyRequestGate(context, this.opt.requestGate, () =>
        transformInvokeImplementation<I, O>(
          this.spec,
          context,
          this.opt.interceptors,
        )(this.input()),
      );
      for (;;) {
        const r = await it.next();
        if (r.done === true) {
          break;
        }
        await this.flushLeadingMetadata();
        await this.socket.write(
          encodeFrame(
            markerBody,
            !this.binary,
            this.serialization.getO(this.binary).serialize(r.value),
          ),
        );
      }
    } catch (e) {
      error = e;
    } finally {
      this.socket.signal.removeEventListener("abort", onSocketGone);
    }
    await this.finish(error);
  }

  private parseTimeout(): { timeoutMs?: number; error?: ConnectError } {
    let serverMax = this.req.maxTimeoutMs ?? 0;
    if (this.opt.maxTimeoutMs > 0 && this.opt.maxTimeoutMs < maxTimerMs) {
      serverMax =
        serverMax > 0
          ? Math.min(serverMax, this.opt.maxTimeoutMs)
          : this.opt.maxTimeoutMs;
    }
    serverMax = Math.min(serverMax, maxTimerMs);
    const bound = serverMax > 0 ? serverMax : undefined;
    const value = new URL(this.req.url, "ws://localhost").searchParams.get(
      paramTimeout,
    );
    if (value === null) {
      return { timeoutMs: bound };
    }
    if (!/^[0-9]{1,10}$/.test(value)) {
      return {
        error: new ConnectError(
          `protocol error: invalid ${paramTimeout}: ${JSON.stringify(value)}`,
          Code.InvalidArgument,
        ),
      };
    }
    // A client may ask for less time than the server allows, never more.
    const requested = parseInt(value, 10);
    return {
      timeoutMs: Math.min(requested, bound ?? maxTimerMs),
    };
  }

  // Consumes the M message that opens every stream before the handler
  // exists, so request headers are complete for interceptors.
  private async readLeadingMetadata(): Promise<Headers> {
    const wire = await this.readMessage();
    if (wire.marker != markerMetadata) {
      throw this.recordFault(
        peerFault(
          "metadata",
          `client's first message must be M; got ${markerName(wire.marker)}`,
        ),
      );
    }
    if (!wire.text) {
      throw this.recordFault(
        peerFault("frame_type", "M message in a binary frame; M is JSON text"),
      );
    }
    const infrastructure =
      this.req.infrastructureHeaders ?? defaultInfrastructureHeaders;
    let meta: Headers;
    try {
      meta = decodeMetadata(decodeJsonPayload(wire.payload));
    } catch (e) {
      throw this.recordFault(e);
    }
    const header = new Headers();
    this.req.header.forEach((value, key) => {
      if (!isStrippedHandshakeHeader(key)) {
        header.append(key, value);
      }
    });
    // A reserved key ends the RPC rather than being dropped: a client that
    // believed it had set a header would never learn the server lacks it.
    for (const key of meta.keys()) {
      const reason = reservedHeaderReason(key, [...infrastructure]);
      if (reason !== undefined) {
        throw this.recordFault(
          peerFault(
            "metadata",
            `client set ${JSON.stringify(key)} in its M message, which is ${reason}`,
          ),
        );
      }
    }
    // An M key replaces the upgrade request's value for that key.
    for (const key of new Set(meta.keys())) {
      header.delete(key);
    }
    meta.forEach((value, key) => header.append(key, value));
    return header;
  }

  private async *input(): AsyncIterable<MessageShape<I>> {
    while (!this.clientEnded) {
      const wire = await this.readMessage();
      switch (wire.marker) {
        case markerBody:
          yield this.parseBody(wire);
          break;
        case markerClientEndStream:
          this.clientEnded = true;
          this.discardAfterEndOfStream();
          if (wire.payload.byteLength > 0) {
            yield this.parseBody(wire);
          }
          return;
        case markerMetadata:
          throw this.recordFault(
            peerFault(
              "metadata",
              "client sent a second M message; a stream carries exactly one, and it opens the stream",
            ),
          );
        case markerServerEndStream:
          throw this.recordFault(
            peerFault("marker", "client sent S, which only a server may send"),
          );
        default:
          throw this.recordFault(
            peerFault(
              "marker",
              `client sent unknown marker ${markerName(wire.marker)}`,
            ),
          );
      }
    }
  }

  private async readMessage(): Promise<WireMessage> {
    const r = await this.socket.read();
    switch (r.type) {
      case "close":
        throw new ConnectError(
          "websocket closed before end-of-stream",
          Code.Canceled,
        );
      case "too_big":
        throw this.recordFault(
          peerFault(
            "size_limit",
            `message exceeds the configured max of ${this.opt.readMaxBytes} bytes`,
            Code.ResourceExhausted,
          ),
        );
      case "invalid":
        throw this.recordFault(peerFault("marker", r.message));
      case "message":
        break;
    }
    let wire: WireMessage;
    try {
      wire = decodeFrame(r.frame);
    } catch (e) {
      throw this.recordFault(e);
    }
    if (wire.payload.byteLength > this.opt.readMaxBytes) {
      throw this.recordFault(
        peerFault(
          "size_limit",
          `message size ${wire.payload.byteLength} is larger than configured max ${this.opt.readMaxBytes}`,
          Code.ResourceExhausted,
        ),
      );
    }
    return wire;
  }

  private parseBody(wire: WireMessage): MessageShape<I> {
    if (wire.payload.byteLength == 0 && wire.text) {
      throw this.recordFault(
        peerFault(
          "frame_type",
          "empty body in a text frame; an empty JSON message is {}",
        ),
      );
    }
    if (wire.text === this.binary) {
      const got = wire.text ? "JSON" : "Protobuf binary";
      throw this.recordFault(
        peerFault(
          "frame_type",
          `client sent a ${got} body, but the negotiated subprotocol is ${this.req.subprotocol}`,
        ),
      );
    }
    if (wire.payload.byteLength == 0) {
      return create(this.spec.method.input);
    }
    try {
      return this.serialization.getI(this.binary).parse(wire.payload);
    } catch (e) {
      throw this.recordFault(
        peerFault(
          "message_encoding",
          `unmarshal message: ${e instanceof Error ? e.message : String(e)}`,
          undefined,
          e,
        ),
      );
    }
  }

  // After C, nothing reads the request stream again, so a peer that keeps
  // sending would fill the socket buffer. Keep reading until the connection
  // ends; any message is a protocol error.
  private discardAfterEndOfStream(): void {
    void (async () => {
      for (;;) {
        const r = await this.socket.read();
        if (r.type == "close") {
          return;
        }
        this.recordFault(
          peerFault("marker", "client sent a message after its C message"),
        );
        if (r.type != "message") {
          return;
        }
      }
    })();
  }

  private recordFault(error: unknown): unknown {
    if (this.fault === undefined && faultOf(error) !== undefined) {
      this.fault = error as ConnectError;
      this.context?.abort(error);
    }
    return error;
  }

  // Runs before the first body and before S alike: a stream that fails
  // without a body is where leading metadata is most wanted.
  private async flushLeadingMetadata(): Promise<void> {
    if (this.leadingSent) {
      return;
    }
    this.leadingSent = true;
    const header = this.context?.responseHeader ?? new Headers();
    await this.socket.write(
      encodeJsonFrame(markerMetadata, encodeMetadata(header)),
    );
  }

  private async finish(reason: unknown): Promise<void> {
    const context = this.context;
    let error: ConnectError | undefined;
    if (this.fault !== undefined) {
      error = this.fault;
    } else if (reason instanceof ConnectError) {
      error = reason;
    } else if (reason !== undefined) {
      error = new ConnectError(
        "internal error",
        Code.Internal,
        undefined,
        undefined,
        reason,
      );
    }
    context?.abort(error);
    // A peer that already left must not be written to: nobody would read S.
    if (this.socket.signal.aborted) {
      await this.socket.close(closeNormal, "");
      return;
    }
    let payload: unknown;
    try {
      payload = endStreamToJson(
        context?.responseTrailer ?? new Headers(),
        error,
        this.opt.jsonOptions,
      );
      JSON.stringify(payload);
    } catch (e) {
      const m = e instanceof Error ? e.message : String(e);
      await this.socket.close(closeInternalError, closeReason(m));
      return;
    }
    try {
      await this.flushLeadingMetadata();
      this.endSent = true;
      await this.socket.write(encodeJsonFrame(markerServerEndStream, payload));
    } catch (e) {
      await this.socket.close(closeInternalError, "");
      return;
    }
    const fault = faultOf(this.fault);
    if (fault !== undefined && error !== undefined) {
      await this.socket.close(
        closeCodeForFault(fault, "server"),
        closeReason(error.rawMessage),
      );
      return;
    }
    await this.socket.close(closeNormal, "");
  }
}
