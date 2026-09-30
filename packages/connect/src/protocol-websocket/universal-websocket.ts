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

import type { ContextValues } from "../context-values.js";
import type { WebSocketFrame } from "./framing.js";

/**
 * The outcome of reading from a server-side WebSocket.
 *
 * - "message": one complete, reassembled message. Text messages have been
 *   validated as UTF-8.
 * - "close": the connection ended. `code` is absent when the peer went away
 *   without a close frame.
 * - "too_big": a message exceeded the read limit. The connection stops
 *   interpreting input after this.
 * - "invalid": the peer broke RFC 6455 framing. The connection stops
 *   interpreting input after this.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export type WebSocketReadResult =
  | { type: "message"; frame: WebSocketFrame }
  | { type: "close"; code?: number; reason: string }
  | { type: "too_big" }
  | { type: "invalid"; message: string };

/**
 * A minimal abstraction of an accepted WebSocket connection on the server
 * side, implemented by runtime adapters.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export interface UniversalServerWebSocket {
  /**
   * Read the next message. After a result other than "message", every
   * further read returns the same result.
   */
  read(): Promise<WebSocketReadResult>;

  /**
   * Send one message. Rejects if the connection is closed.
   */
  write(frame: WebSocketFrame): Promise<void>;

  /**
   * Start the closing handshake, then stop interpreting input. Resolves once
   * the connection is gone, bounded in time and in the bytes discarded.
   */
  close(code: number, reason: string): Promise<void>;

  /**
   * Aborted when the connection ends, whether or not anyone is reading.
   */
  readonly signal: AbortSignal;
}

/**
 * A WebSocket connection that completed the handshake for one RPC.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export interface UniversalWebSocketRequest {
  /**
   * The handshake URL, including the query string.
   */
  url: string;
  /**
   * The headers of the upgrade request.
   */
  header: Headers;
  /**
   * The subprotocol the server echoed.
   */
  subprotocol: string;
  socket: UniversalServerWebSocket;
  contextValues?: ContextValues;
  /**
   * The server's own bound on the RPC. The effective deadline is the shorter
   * of this and the client's. Zero or undefined means no bound.
   */
  maxTimeoutMs?: number;
  /**
   * Request headers a client may not set in its Leading-Metadata message
   * because the deployment's own infrastructure sets them.
   */
  infrastructureHeaders?: readonly string[];
}

/**
 * Serves one RPC on an accepted WebSocket connection.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export type UniversalWebSocketHandlerFn = (
  request: UniversalWebSocketRequest,
) => Promise<void>;

/**
 * A WebSocket handler for one specific RPC.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export interface UniversalWebSocketHandler extends UniversalWebSocketHandlerFn {
  /**
   * The largest payload a message may carry. An adapter bounds reads at
   * this plus one byte for the marker.
   */
  readMaxBytes: number;
}
