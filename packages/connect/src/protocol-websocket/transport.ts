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

import type {
  BinaryReadOptions,
  BinaryWriteOptions,
  DescMessage,
  DescMethodStreaming,
  DescMethodUnary,
  JsonReadOptions,
  JsonWriteOptions,
  MessageInitShape,
  MessageShape,
} from "@bufbuild/protobuf";
import { create } from "@bufbuild/protobuf";
import { Code } from "../code.js";
import { ConnectError } from "../connect-error.js";
import type { ContextValues } from "../context-values.js";
import { createContextValues } from "../context-values.js";
import type {
  Interceptor,
  StreamRequest,
  StreamResponse,
  UnaryRequest,
  UnaryResponse,
} from "../interceptor.js";
import { runStreamingCall, runUnaryCall } from "../protocol/run-call.js";
import { createMethodSerializationLookup } from "../protocol/serialization.js";
import type { MethodSerializationLookup } from "../protocol/serialization.js";
import { getAbortSignalReason } from "../protocol/signals.js";
import type { Transport } from "../transport.js";
import { endStreamFromJson } from "../protocol-connect/end-stream.js";
import type { EndStreamResponse } from "../protocol-connect/end-stream.js";
import { ClientSocket } from "./client-socket.js";
import type { WebSocketFactory, WebSocketLike } from "./client-socket.js";
import {
  closeClientCompression,
  closeClientInternalError,
  closeClientProtocolError,
  closeCodeForFault,
  closeMessageTooBig,
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
  normalizeCloseCode,
  peerFault,
  subprotocolJson,
  subprotocolProto,
} from "./framing.js";
import type { WireMessage } from "./framing.js";
import { decodeMetadata, encodeMetadata } from "./metadata.js";
import { createWebSocketUrl } from "./url.js";

/**
 * Options for the Connect-over-WebSocket transport.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export interface WebSocketTransportOptions {
  /**
   * Base URI for all RPCs. An http or https scheme is rewritten to ws or
   * wss. A relative URI resolves against the page's location.
   */
  baseUrl: string;
  /**
   * Prepended to every procedure path, after the base URI. Both peers must
   * agree on it.
   */
  pathPrefix?: string;
  /**
   * Selects the connectrpc.1+proto subprotocol instead of connectrpc.1+json.
   */
  useBinaryFormat: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  readMaxBytes: number;
  writeMaxBytes: number;
  defaultTimeoutMs?: number;
  /**
   * Creates the WebSocket for each RPC.
   */
  webSocket: WebSocketFactory;
  /**
   * Accept a negotiated permessage-deflate reported without any parameters.
   * Firefox reports extensions by name only, so a client cannot see whether
   * the server imposed no-context-takeover, which the server must do anyway.
   * Parameters that are reported are always checked.
   */
  allowUnparameterizedDeflate?: boolean;
}

/**
 * Create a Transport for the Connect-over-WebSocket protocol. Every RPC,
 * including unary RPCs, opens its own connection.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function createTransport(opt: WebSocketTransportOptions): Transport {
  function resolveTimeout(timeoutMs: number | undefined): number | undefined {
    if (timeoutMs === undefined) {
      return opt.defaultTimeoutMs;
    }
    return timeoutMs <= 0 ? undefined : timeoutMs;
  }
  return {
    async unary<I extends DescMessage, O extends DescMessage>(
      method: DescMethodUnary<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      message: MessageInitShape<I>,
      contextValues?: ContextValues,
    ): Promise<UnaryResponse<I, O>> {
      const serialization = createMethodSerializationLookup(
        method,
        opt.binaryOptions,
        opt.jsonOptions,
        opt,
      );
      timeoutMs = resolveTimeout(timeoutMs);
      return await runUnaryCall<I, O>({
        interceptors: opt.interceptors,
        signal,
        timeoutMs,
        req: {
          stream: false,
          service: method.parent,
          method,
          requestMethod: "GET",
          url: createWebSocketUrl(opt, method, timeoutMs),
          header: new Headers(header),
          contextValues: contextValues ?? createContextValues(),
          message,
        },
        next: async (req: UnaryRequest<I, O>): Promise<UnaryResponse<I, O>> => {
          const call = await ClientCall.dial(opt, serialization, req);
          call.sendBody(req.message);
          call.sendEnd();
          const responseHeader = await call.readLeadingMetadata();
          let output: MessageShape<O> | undefined;
          for (;;) {
            const r = await call.readNext();
            if (r.type == "body") {
              if (output !== undefined) {
                throw new ConnectError(
                  "protocol error: received extra output message for unary method",
                  Code.Unimplemented,
                );
              }
              output = r.message;
              continue;
            }
            if (r.error !== undefined) {
              throw withHeader(r.error, responseHeader);
            }
            if (output === undefined) {
              throw new ConnectError(
                "protocol error: missing output message for unary method",
                Code.Unimplemented,
              );
            }
            return {
              stream: false,
              service: method.parent,
              method,
              header: responseHeader,
              message: output,
              trailer: r.trailer,
            };
          }
        },
      });
    },

    async stream<I extends DescMessage, O extends DescMessage>(
      method: DescMethodStreaming<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      input: AsyncIterable<MessageInitShape<I>>,
      contextValues?: ContextValues,
    ): Promise<StreamResponse<I, O>> {
      const serialization = createMethodSerializationLookup(
        method,
        opt.binaryOptions,
        opt.jsonOptions,
        opt,
      );
      timeoutMs = resolveTimeout(timeoutMs);
      return runStreamingCall<I, O>({
        interceptors: opt.interceptors,
        signal,
        timeoutMs,
        req: {
          stream: true,
          service: method.parent,
          method,
          requestMethod: "GET",
          url: createWebSocketUrl(opt, method, timeoutMs),
          header: new Headers(header),
          contextValues: contextValues ?? createContextValues(),
          message: input,
        },
        next: async (req: StreamRequest<I, O>) => {
          const call = await ClientCall.dial(opt, serialization, req);
          void call.pump(req.message);
          const responseHeader = await call.readLeadingMetadata();
          const res: StreamResponse<I, O> = {
            ...req,
            header: responseHeader,
            trailer: new Headers(),
            message: (async function* () {
              for (;;) {
                const r = await call.readNext();
                if (r.type == "body") {
                  yield r.message;
                  continue;
                }
                r.trailer.forEach((value, key) => res.trailer.set(key, value));
                if (r.error !== undefined) {
                  throw withHeader(r.error, responseHeader);
                }
                return;
              }
            })(),
          };
          return res;
        },
      });
    },
  };
}

type ReadResult<O extends DescMessage> =
  | { type: "body"; message: MessageShape<O> }
  | { type: "end"; trailer: Headers; error?: ConnectError };

class ClientCall<I extends DescMessage, O extends DescMessage> {
  private sawLeadingMetadata = false;
  private sawBody = false;
  private endSeen = false;
  private endSent = false;
  // Set when this side gave up; reads report it instead of the close.
  private localError: unknown;

  private constructor(
    private readonly socket: ClientSocket,
    private readonly binary: boolean,
    private readonly subprotocol: string,
    private readonly serialization: MethodSerializationLookup<I, O>,
    private readonly output: O,
    private readonly readMaxBytes: number,
    private readonly signal: AbortSignal,
  ) {}

  static async dial<I extends DescMessage, O extends DescMessage>(
    opt: WebSocketTransportOptions,
    serialization: MethodSerializationLookup<I, O>,
    req: UnaryRequest<I, O> | StreamRequest<I, O>,
  ): Promise<ClientCall<I, O>> {
    const subprotocol = opt.useBinaryFormat
      ? subprotocolProto
      : subprotocolJson;
    let ws: WebSocketLike;
    try {
      ws = opt.webSocket(req.url, subprotocol);
    } catch (e) {
      throw new ConnectError(
        `failed to create WebSocket: ${e instanceof Error ? e.message : String(e)}`,
        Code.Unavailable,
        undefined,
        undefined,
        e,
      );
    }
    const socket = new ClientSocket(ws);
    const onAbort = () => socket.close(closeNormal, "");
    if (req.signal.aborted) {
      onAbort();
    } else {
      req.signal.addEventListener("abort", onAbort);
    }
    await socket.opened;
    if (ws.protocol !== subprotocol) {
      socket.close(closeClientProtocolError, "unexpected subprotocol");
      throw new ConnectError(
        `server selected unexpected Sec-WebSocket-Protocol ${JSON.stringify(ws.protocol)} (want ${JSON.stringify(subprotocol)})`,
        Code.Internal,
      );
    }
    const extensionError = checkNoContextTakeover(
      ws.extensions,
      opt.allowUnparameterizedDeflate === true,
    );
    if (extensionError !== undefined) {
      socket.close(closeClientCompression, "no-context-takeover required");
      throw extensionError;
    }
    const call = new ClientCall(
      socket,
      opt.useBinaryFormat,
      subprotocol,
      serialization,
      req.method.output,
      opt.readMaxBytes,
      req.signal,
    );
    socket.send(encodeJsonFrame(markerMetadata, encodeMetadata(req.header)));
    return call;
  }

  sendBody(message: MessageShape<I>): void {
    if (this.endSent) {
      throw new ConnectError(
        "cannot send a message after closing the request stream",
        Code.Internal,
      );
    }
    this.socket.send(
      encodeFrame(
        markerBody,
        !this.binary,
        this.serialization.getI(this.binary).serialize(message),
      ),
    );
  }

  // A bare C takes the frame type of the negotiated codec.
  sendEnd(): void {
    if (this.endSent || this.endSeen) {
      return;
    }
    this.endSent = true;
    this.socket.send(encodeFrame(markerClientEndStream, !this.binary));
  }

  async pump(input: AsyncIterable<MessageShape<I>>): Promise<void> {
    try {
      for await (const message of input) {
        // Once the server has ended the stream, nothing more is read.
        if (this.endSeen) {
          return;
        }
        this.sendBody(message);
      }
      this.sendEnd();
    } catch (e) {
      if (this.endSeen || this.signal.aborted) {
        return;
      }
      this.localError = e;
      this.socket.close(closeClientInternalError, closeReason(String(e)));
    }
  }

  async readLeadingMetadata(): Promise<Headers> {
    const wire = await this.readMessage();
    if (wire.marker != markerMetadata) {
      throw this.fail(
        peerFault(
          "metadata",
          `server's first message must be M; got ${markerName(wire.marker)}`,
        ),
      );
    }
    this.sawLeadingMetadata = true;
    return this.decodeMetadataMessage(wire);
  }

  async readNext(): Promise<ReadResult<O>> {
    const wire = await this.readMessage();
    switch (wire.marker) {
      case markerBody:
        this.sawBody = true;
        return { type: "body", message: this.parseBody(wire) };
      case markerServerEndStream: {
        if (!wire.text) {
          throw this.fail(
            peerFault("frame_type", "S message in a binary frame; S is JSON"),
          );
        }
        let end: EndStreamResponse;
        try {
          end = endStreamFromJson(decodeJsonPayload(wire.payload));
        } catch (e) {
          throw this.fail(
            peerFault(
              "metadata",
              "invalid EndStreamResponse",
              Code.Internal,
              e,
            ),
          );
        }
        this.endSeen = true;
        this.socket.close(closeNormal, "");
        return { type: "end", trailer: end.metadata, error: end.error };
      }
      case markerMetadata:
        throw this.fail(
          peerFault(
            "metadata",
            this.sawBody
              ? "server sent M after a body; metadata is leading only before the first one"
              : "server sent a second M message; a stream carries exactly one, and it opens the stream",
          ),
        );
      case markerClientEndStream:
        throw this.fail(
          peerFault(
            "marker",
            "server sent C, which only a client may send",
            Code.Internal,
          ),
        );
      default:
        throw this.fail(
          peerFault(
            "marker",
            `server sent unknown marker ${markerName(wire.marker)}`,
          ),
        );
    }
  }

  private async readMessage(): Promise<WireMessage> {
    if (this.endSeen) {
      throw new ConnectError("stream already ended", Code.Internal);
    }
    const r = await this.socket.read();
    if (this.localError !== undefined) {
      throw this.localError;
    }
    if (r.type == "close") {
      if (this.signal.aborted) {
        throw ConnectError.from(
          getAbortSignalReason(this.signal),
          Code.Canceled,
        );
      }
      if (normalizeCloseCode(r.code) == closeMessageTooBig) {
        throw new ConnectError(
          `server rejected message as too big: ${r.reason}`,
          Code.ResourceExhausted,
        );
      }
      // The RPC did not complete, whatever the close code.
      throw new ConnectError(
        `server closed WebSocket without EndStreamResponse (code ${r.code})`,
        Code.Unavailable,
      );
    }
    let wire: WireMessage;
    try {
      wire = decodeFrame(r.frame);
    } catch (e) {
      throw this.fail(e);
    }
    if (wire.payload.byteLength > this.readMaxBytes) {
      throw this.fail(
        peerFault(
          "size_limit",
          `message exceeds the ${this.readMaxBytes} byte limit`,
          Code.ResourceExhausted,
        ),
      );
    }
    return wire;
  }

  private decodeMetadataMessage(wire: WireMessage): Headers {
    if (!wire.text) {
      throw this.fail(
        peerFault("frame_type", "M message in a binary frame; M is JSON"),
      );
    }
    try {
      return decodeMetadata(decodeJsonPayload(wire.payload));
    } catch (e) {
      throw this.fail(e);
    }
  }

  private parseBody(wire: WireMessage): MessageShape<O> {
    if (!this.sawLeadingMetadata) {
      throw this.fail(peerFault("metadata", "server sent B before M"));
    }
    if (wire.payload.byteLength == 0 && wire.text) {
      throw this.fail(
        peerFault(
          "frame_type",
          "empty body in a text frame; an empty JSON message is {}",
        ),
      );
    }
    if (wire.text === this.binary) {
      const got = wire.text ? "JSON" : "Protobuf binary";
      throw this.fail(
        peerFault(
          "frame_type",
          `server sent a ${got} body, but the negotiated subprotocol is ${this.subprotocol}`,
        ),
      );
    }
    if (wire.payload.byteLength == 0) {
      return create(this.output);
    }
    try {
      return this.serialization.getO(this.binary).parse(wire.payload);
    } catch (e) {
      throw this.fail(
        peerFault(
          "message_encoding",
          `unmarshal message: ${e instanceof Error ? e.message : String(e)}`,
          undefined,
          e,
        ),
      );
    }
  }

  // A protocol error ends the stream: fail the RPC and close with the
  // fault's 31xx code.
  private fail(error: unknown): unknown {
    const fault = faultOf(error);
    const message = error instanceof ConnectError ? error.rawMessage : "";
    this.socket.close(
      fault === undefined
        ? closeClientProtocolError
        : closeCodeForFault(fault, "client"),
      closeReason(message),
    );
    this.endSeen = true;
    return error;
  }
}

function withHeader(error: ConnectError, header: Headers): ConnectError {
  header.forEach((value, key) => error.metadata.append(key, value));
  return error;
}

// A compression context shared across messages leaks plaintext between them
// (the CRIME/BREACH family), so both directions must reset per message.
/**
 * Reject a negotiated permessage-deflate that shares a compression context
 * across messages.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function checkNoContextTakeover(
  extensions: string,
  allowUnparameterized: boolean,
): ConnectError | undefined {
  for (const extension of extensions.split(",")) {
    const params = extension.split(";").map((p) => p.trim());
    if (params[0] != "permessage-deflate") {
      continue;
    }
    if (allowUnparameterized && params.length == 1) {
      continue;
    }
    if (
      !params.includes("client_no_context_takeover") ||
      !params.includes("server_no_context_takeover")
    ) {
      return new ConnectError(
        `server negotiated ${JSON.stringify(extension.trim())} without no-context-takeover in both directions`,
        Code.Internal,
      );
    }
  }
  return undefined;
}
