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

import { createHash } from "node:crypto";
import type * as stream from "node:stream";
import type {
  UniversalServerWebSocket,
  WebSocketFrame,
  WebSocketReadResult,
} from "@connectrpc/connect/protocol-websocket";

// A server-side WebSocket connection per RFC 6455, without extensions:
// permessage-deflate is never negotiated, which the Connect-over-WebSocket
// protocol permits.

const handshakeGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const opContinuation = 0x0;
const opText = 0x1;
const opBinary = 0x2;
const opClose = 0x8;
const opPing = 0x9;
const opPong = 0xa;

// Reading pauses while this many message bytes wait for the handler.
const highWaterMark = 1024 * 1024;
// After the closing handshake starts, input is discarded unparsed, up to
// these bounds, so a TCP reset does not destroy the close frame in flight.
const drainMaxBytes = 1024 * 1024;
const drainTimeoutMs = 5000;

/**
 * Compute Sec-WebSocket-Accept for a Sec-WebSocket-Key.
 */
export function webSocketAccept(key: string): string {
  return createHash("sha1")
    .update(key + handshakeGuid)
    .digest("base64");
}

/**
 * An accepted WebSocket connection on a Node.js socket that has already
 * written its 101 response.
 */
export class NodeServerWebSocket implements UniversalServerWebSocket {
  private readonly controller = new AbortController();
  readonly signal = this.controller.signal;
  private chunks: Buffer[] = [];
  private chunksLength = 0;
  private fragments: Buffer[] = [];
  private fragmentsLength = 0;
  private fragmentText = false;
  private fragmenting = false;
  private readonly queue: WebSocketReadResult[] = [];
  private queuedBytes = 0;
  private readonly waiters: ((r: WebSocketReadResult) => void)[] = [];
  private terminal: WebSocketReadResult | undefined;
  private closeSent = false;
  private discarded = 0;
  private readonly textDecoder = new TextDecoder("utf-8", { fatal: true });

  /**
   * @param maxMessageBytes The largest reassembled message accepted. A
   * message is abandoned as soon as it passes this, never consumed in full.
   */
  constructor(
    private readonly socket: stream.Duplex,
    head: Buffer,
    private readonly maxMessageBytes: number,
  ) {
    socket.on("data", (chunk: Buffer) => this.onData(chunk));
    socket.on("end", () => this.onGone());
    socket.on("close", () => this.onGone());
    socket.on("error", () => this.onGone());
    if (head.byteLength > 0) {
      this.onData(head);
    }
  }

  read(): Promise<WebSocketReadResult> {
    const next = this.queue.shift();
    if (next !== undefined) {
      if (next.type == "message") {
        this.queuedBytes -= next.frame.data.byteLength;
        if (this.queuedBytes < highWaterMark && this.terminal === undefined) {
          this.socket.resume();
        }
      }
      return Promise.resolve(next);
    }
    if (this.terminal !== undefined) {
      return Promise.resolve(this.terminal);
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  async write(frame: WebSocketFrame): Promise<void> {
    if (this.closeSent || this.signal.aborted) {
      throw new Error("WebSocket is closed");
    }
    await this.writeFrame(frame.text ? opText : opBinary, frame.data);
  }

  async close(code: number, reason: string): Promise<void> {
    if (!this.closeSent && !this.socket.destroyed) {
      this.closeSent = true;
      const reasonBytes = Buffer.from(reason, "utf8");
      const payload = Buffer.alloc(2 + reasonBytes.byteLength);
      payload.writeUInt16BE(code, 0);
      reasonBytes.copy(payload, 2);
      try {
        await this.writeFrame(opClose, payload);
      } catch (e) {
        this.socket.destroy();
      }
    }
    this.stopInterpreting({ type: "close", code, reason });
    if (this.socket.destroyed) {
      return;
    }
    this.socket.end();
    this.socket.resume();
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => this.socket.destroy(), drainTimeoutMs);
      this.socket.once("close", () => {
        clearTimeout(timer);
        resolve();
      });
      if (this.socket.destroyed) {
        clearTimeout(timer);
        resolve();
      }
    });
  }

  private writeFrame(opcode: number, payload: Uint8Array): Promise<void> {
    const length = payload.byteLength;
    let header: Buffer;
    if (length < 126) {
      header = Buffer.alloc(2);
      header[1] = length;
    } else if (length < 0x10000) {
      header = Buffer.alloc(4);
      header[1] = 126;
      header.writeUInt16BE(length, 2);
    } else {
      header = Buffer.alloc(10);
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(length), 2);
    }
    header[0] = 0x80 | opcode;
    const data = Buffer.concat([header, payload]);
    return new Promise((resolve, reject) => {
      if (this.socket.destroyed) {
        reject(new Error("socket destroyed"));
        return;
      }
      if (this.socket.write(data)) {
        resolve();
        return;
      }
      const onDrain = () => {
        this.socket.off("close", onClose);
        resolve();
      };
      const onClose = () => {
        this.socket.off("drain", onDrain);
        reject(new Error("socket closed"));
      };
      this.socket.once("drain", onDrain);
      this.socket.once("close", onClose);
    });
  }

  private onData(chunk: Buffer): void {
    if (this.terminal !== undefined) {
      this.discarded += chunk.byteLength;
      if (this.discarded > drainMaxBytes) {
        this.socket.destroy();
      }
      return;
    }
    this.chunks.push(chunk);
    this.chunksLength += chunk.byteLength;
    while (this.terminal === undefined && this.parseFrame()) {
      // Parse every complete frame in the buffer.
    }
  }

  // Returns true if a frame was consumed and another may follow.
  private parseFrame(): boolean {
    if (this.chunksLength < 2) {
      return false;
    }
    const head = this.peek(Math.min(this.chunksLength, 14));
    const fin = (head[0] & 0x80) != 0;
    const rsv = head[0] & 0x70;
    const opcode = head[0] & 0x0f;
    const masked = (head[1] & 0x80) != 0;
    let length = head[1] & 0x7f;
    let offset = 2;
    if (length == 126) {
      if (head.byteLength < 4) {
        return false;
      }
      length = head.readUInt16BE(2);
      offset = 4;
    } else if (length == 127) {
      if (head.byteLength < 10) {
        return false;
      }
      const big = head.readBigUInt64BE(2);
      length =
        big > BigInt(Number.MAX_SAFE_INTEGER)
          ? Number.MAX_SAFE_INTEGER
          : Number(big);
      offset = 10;
    }
    if (rsv != 0) {
      return this.invalid("reserved bits set without a negotiated extension");
    }
    if (!masked) {
      return this.invalid("client frame is not masked");
    }
    const isControl = (opcode & 0x8) != 0;
    if (isControl) {
      if (opcode != opClose && opcode != opPing && opcode != opPong) {
        return this.invalid(`unknown opcode 0x${opcode.toString(16)}`);
      }
      if (!fin || length > 125) {
        return this.invalid("control frame is fragmented or too long");
      }
    } else if (opcode == opContinuation) {
      if (!this.fragmenting) {
        return this.invalid("continuation frame without a message");
      }
    } else if (opcode == opText || opcode == opBinary) {
      if (this.fragmenting) {
        return this.invalid("new message before the previous one finished");
      }
    } else {
      return this.invalid(`unknown opcode 0x${opcode.toString(16)}`);
    }
    // Checked from the header alone, so an oversized message is abandoned
    // before any of its payload is buffered.
    if (!isControl && this.fragmentsLength + length > this.maxMessageBytes) {
      this.stopInterpreting({ type: "too_big" });
      return false;
    }
    if (this.chunksLength < offset + 4 + length) {
      return false;
    }
    const frame = this.take(offset + 4 + length);
    const mask = frame.subarray(offset, offset + 4);
    const payload = Buffer.from(frame.subarray(offset + 4));
    for (let i = 0; i < payload.byteLength; i++) {
      payload[i] ^= mask[i % 4];
    }
    if (isControl) {
      return this.onControl(opcode, payload);
    }
    if (opcode != opContinuation) {
      this.fragmenting = true;
      this.fragmentText = opcode == opText;
    }
    this.fragments.push(payload);
    this.fragmentsLength += payload.byteLength;
    if (!fin) {
      return true;
    }
    const data = Buffer.concat(this.fragments, this.fragmentsLength);
    const text = this.fragmentText;
    this.fragments = [];
    this.fragmentsLength = 0;
    this.fragmenting = false;
    if (text && !this.isUtf8(data)) {
      return this.invalid("text message is not valid UTF-8");
    }
    this.push({ type: "message", frame: { text, data } });
    return true;
  }

  private onControl(opcode: number, payload: Buffer): boolean {
    switch (opcode) {
      case opPing:
        if (!this.closeSent) {
          this.writeFrame(opPong, payload).catch(() => {
            // The connection is ending; nothing is waiting for the pong.
          });
        }
        return true;
      case opPong:
        return true;
      default: {
        if (payload.byteLength == 1) {
          return this.invalid("close frame with a one-byte payload");
        }
        const code =
          payload.byteLength >= 2 ? payload.readUInt16BE(0) : undefined;
        const reasonBytes = payload.subarray(2);
        if (!this.isUtf8(reasonBytes)) {
          return this.invalid("close reason is not valid UTF-8");
        }
        const reason = reasonBytes.toString("utf8");
        if (!this.closeSent) {
          // Echo the peer's close to complete the handshake.
          this.closeSent = true;
          this.writeFrame(opClose, payload.subarray(0, 2))
            .catch(() => undefined)
            .finally(() => this.socket.end());
        }
        this.stopInterpreting({ type: "close", code, reason });
        this.socket.resume();
        this.controller.abort();
        return false;
      }
    }
  }

  private isUtf8(data: Uint8Array): boolean {
    try {
      this.textDecoder.decode(data);
      return true;
    } catch (e) {
      return false;
    }
  }

  private invalid(message: string): boolean {
    this.stopInterpreting({ type: "invalid", message });
    return false;
  }

  private onGone(): void {
    this.stopInterpreting({ type: "close", reason: "" });
    this.controller.abort();
  }

  private stopInterpreting(r: WebSocketReadResult): void {
    if (this.terminal !== undefined) {
      return;
    }
    this.terminal = r;
    this.chunks = [];
    this.chunksLength = 0;
    this.fragments = [];
    // Nothing more is read until close() starts the bounded drain, so the
    // drain limit cannot cut the connection before S is written.
    if (!this.closeSent) {
      this.socket.pause();
    }
    for (const waiter of this.waiters.splice(0)) {
      waiter(r);
    }
  }

  private push(r: WebSocketReadResult): void {
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter(r);
      return;
    }
    this.queue.push(r);
    if (r.type == "message") {
      this.queuedBytes += r.frame.data.byteLength;
      if (this.queuedBytes >= highWaterMark) {
        this.socket.pause();
      }
    }
  }

  private peek(n: number): Buffer {
    if (this.chunks[0].byteLength < n) {
      this.chunks = [Buffer.concat(this.chunks, this.chunksLength)];
    }
    return this.chunks[0];
  }

  private take(n: number): Buffer {
    const all = this.peek(n);
    const taken = all.subarray(0, n);
    const rest = all.subarray(n);
    this.chunks = rest.byteLength > 0 ? [rest] : [];
    this.chunksLength -= n;
    return taken;
  }
}
