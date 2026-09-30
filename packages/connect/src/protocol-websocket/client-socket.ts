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

import { Code } from "../code.js";
import { ConnectError } from "../connect-error.js";
import type { WebSocketFrame } from "./framing.js";

/**
 * The subset of the WHATWG WebSocket API the client transport uses. The
 * global WebSocket of browsers, Deno, Bun, and Node.js 22+ satisfies it.
 */
export interface WebSocketLike {
  binaryType: string;
  readonly protocol: string;
  readonly extensions: string;
  send(data: string | Uint8Array<ArrayBuffer>): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "error", listener: () => void): void;
  addEventListener(
    type: "message",
    listener: (event: { data: unknown }) => void,
  ): void;
  addEventListener(
    type: "close",
    listener: (event: { code: number; reason: string }) => void,
  ): void;
}

/**
 * Creates a WebSocket for one RPC, offering exactly one subprotocol.
 */
export type WebSocketFactory = (url: string, protocol: string) => WebSocketLike;

type ClientReadResult =
  | { type: "message"; frame: WebSocketFrame }
  | { type: "close"; code: number; reason: string };

/**
 * Adapts the event-driven WebSocket API to a pull-based reader.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export class ClientSocket {
  private readonly queue: ClientReadResult[] = [];
  private readonly waiters: ((r: ClientReadResult) => void)[] = [];
  private closed: ClientReadResult | undefined;
  private closeSent = false;
  readonly opened: Promise<void>;

  constructor(private readonly ws: WebSocketLike) {
    ws.binaryType = "arraybuffer";
    let settle: { resolve(): void; reject(e: unknown): void } | undefined;
    this.opened = new Promise<void>((resolve, reject) => {
      settle = { resolve, reject };
    });
    ws.addEventListener("open", () => {
      settle?.resolve();
      settle = undefined;
    });
    ws.addEventListener("message", (event) => {
      let frame: WebSocketFrame;
      if (typeof event.data == "string") {
        frame = { text: true, data: new TextEncoder().encode(event.data) };
      } else if (event.data instanceof ArrayBuffer) {
        frame = { text: false, data: new Uint8Array(event.data) };
      } else {
        return;
      }
      this.push({ type: "message", frame });
    });
    ws.addEventListener("close", (event) => {
      // A browser does not tell a script why a handshake failed: a rejected
      // upgrade and an unreachable host look the same.
      settle?.reject(
        new ConnectError(
          "WebSocket connection failed before it opened; the reason is not available to the client",
          Code.Unavailable,
        ),
      );
      settle = undefined;
      this.closeSent = true;
      this.push({ type: "close", code: event.code, reason: event.reason });
    });
  }

  read(): Promise<ClientReadResult> {
    const next = this.queue.shift();
    if (next !== undefined) {
      return Promise.resolve(next);
    }
    if (this.closed !== undefined) {
      return Promise.resolve(this.closed);
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  send(frame: WebSocketFrame): void {
    if (this.closed !== undefined || this.closeSent) {
      throw new ConnectError("WebSocket is closed", Code.Unavailable);
    }
    this.ws.send(
      frame.text
        ? new TextDecoder().decode(frame.data)
        : new Uint8Array(frame.data),
    );
  }

  close(code: number, reason: string): void {
    if (this.closeSent) {
      return;
    }
    this.closeSent = true;
    try {
      this.ws.close(code, reason);
    } catch (e) {
      // An invalid code or reason throws rather than closing.
      this.ws.close();
    }
  }

  private push(r: ClientReadResult): void {
    if (this.closed !== undefined) {
      return;
    }
    if (r.type == "close") {
      this.closed = r;
    }
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter(r);
    } else if (r.type == "message") {
      this.queue.push(r);
    }
    if (r.type == "close") {
      for (const w of this.waiters.splice(0)) {
        w(r);
      }
    }
  }
}
