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

import type * as http from "node:http";
import type * as stream from "node:stream";
import type { ContextValues } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import {
  normalizePathPrefix,
  subprotocolJson,
  subprotocolProto,
} from "@connectrpc/connect/protocol-websocket";
import { nodeHeaderToWebHeader } from "./node-universal-header.js";
import { NodeServerWebSocket, webSocketAccept } from "./node-websocket.js";

/**
 * Options for serving the Connect-over-WebSocket protocol.
 */
export interface NodeWebSocketOptions {
  /**
   * Serve WebSocket RPCs under this path prefix, so that every upgrade is
   * distinguishable by URL alone. Clients must use the same prefix.
   *
   * With a prefix, a plain HTTP request under the prefix is answered with
   * 426 Upgrade Required, and an upgrade at a bare procedure path with 400.
   */
  pathPrefix?: string;

  /**
   * Decide whether to accept a handshake from the given request. Replaces
   * the default check, which rejects a cross-origin handshake with 403 and
   * accepts one without an Origin header.
   *
   * A browser attaches the user's cookies to a WebSocket handshake without a
   * CORS preflight, so accepting every origin lets any page open an
   * authenticated stream.
   */
  checkOrigin?: (request: http.IncomingMessage) => boolean;

  /**
   * Bounds every WebSocket RPC. It is the deadline when a client requests
   * none, and the ceiling on one that does. Zero means no bound, which lets
   * a peer that upgrades and goes silent hold a connection indefinitely.
   *
   * The default is one hour.
   */
  maxTimeoutMs?: number;

  /**
   * Request headers a client may not set in its Leading-Metadata message,
   * because this deployment's own infrastructure sets them. A trailing "*"
   * matches any suffix. Replaces the default list of Forwarded,
   * X-Forwarded-*, and X-Real-IP.
   */
  infrastructureHeaders?: string[];
}

/**
 * NodeUpgradeHandlerFn is compatible with the "upgrade" event of
 * http.Server.
 */
export type NodeUpgradeHandlerFn = (
  request: http.IncomingMessage,
  socket: stream.Duplex,
  head: Buffer,
) => void;

const defaultMaxTimeoutMs = 60 * 60 * 1000;

/**
 * Create a handler for the "upgrade" event of http.Server.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function createNodeUpgradeHandler(
  handlers: UniversalHandler[],
  requestPathPrefix: string,
  options: NodeWebSocketOptions,
  contextValues: ((req: http.IncomingMessage) => ContextValues) | undefined,
): NodeUpgradeHandlerFn {
  const wsPrefix = normalizePathPrefix(options.pathPrefix);
  const paths = new Map<string, UniversalHandler>();
  const barePaths = new Set<string>();
  for (const uHandler of handlers) {
    if (uHandler.webSocket !== undefined) {
      paths.set(requestPathPrefix + wsPrefix + uHandler.requestPath, uHandler);
      barePaths.add(requestPathPrefix + uHandler.requestPath);
    }
  }
  const checkOrigin = options.checkOrigin ?? isSameOrigin;
  return function upgrade(request, socket, head) {
    socket.on("error", () => {
      // Errors surface as the connection closing.
    });
    if (request.method !== "GET" || !isWebSocketUpgrade(request)) {
      rejectHandshake(socket, 400, "not a WebSocket upgrade");
      return;
    }
    const path = request.url?.split("?", 2)[0] ?? "";
    const uHandler = paths.get(path);
    const webSocket = uHandler?.webSocket;
    if (webSocket === undefined) {
      if (wsPrefix != "" && barePaths.has(path)) {
        rejectHandshake(
          socket,
          400,
          `WebSocket RPCs are served under the path prefix ${wsPrefix}`,
        );
        return;
      }
      rejectHandshake(socket, 404, "not found");
      return;
    }
    if (!checkOrigin(request)) {
      rejectHandshake(socket, 403, "origin not allowed");
      return;
    }
    const subprotocol = negotiateSubprotocol(
      request.headers["sec-websocket-protocol"],
    );
    if (subprotocol === undefined) {
      rejectHandshake(
        socket,
        400,
        "no supported WebSocket subprotocol in Sec-WebSocket-Protocol",
      );
      return;
    }
    const key = request.headers["sec-websocket-key"];
    if (typeof key != "string" || Buffer.from(key, "base64").byteLength != 16) {
      rejectHandshake(socket, 400, "invalid Sec-WebSocket-Key");
      return;
    }
    if (request.headers["sec-websocket-version"] !== "13") {
      rejectHandshake(socket, 426, "unsupported WebSocket version", {
        "Sec-WebSocket-Version": "13",
      });
      return;
    }
    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\n" +
        "Upgrade: websocket\r\n" +
        "Connection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${webSocketAccept(key)}\r\n` +
        `Sec-WebSocket-Protocol: ${subprotocol}\r\n` +
        "\r\n",
    );
    if ("setNoDelay" in socket && typeof socket.setNoDelay == "function") {
      socket.setNoDelay(true);
    }
    // The limit counts the marker, which is one byte.
    const conn = new NodeServerWebSocket(
      socket,
      head,
      webSocket.readMaxBytes + 1,
    );
    const encrypted = "encrypted" in socket && socket.encrypted === true;
    webSocket({
      url: `${encrypted ? "https" : "http"}://${request.headers.host ?? "localhost"}${request.url ?? "/"}`,
      header: nodeHeaderToWebHeader(request.headers),
      subprotocol,
      socket: conn,
      contextValues: contextValues?.(request),
      maxTimeoutMs: options.maxTimeoutMs ?? defaultMaxTimeoutMs,
      infrastructureHeaders: options.infrastructureHeaders,
    }).catch((reason) => {
      console.error(
        `websocket handler for rpc ${uHandler?.method.name} of ${uHandler?.service.typeName} failed`,
        reason,
      );
      socket.destroy();
    });
  };
}

/**
 * Report whether a plain HTTP request is for a path under the WebSocket
 * prefix, and must therefore be answered with 426 Upgrade Required.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function isUnderWebSocketPrefix(
  path: string,
  requestPathPrefix: string,
  options: NodeWebSocketOptions | undefined,
): boolean {
  const wsPrefix = normalizePathPrefix(options?.pathPrefix);
  if (wsPrefix == "") {
    return false;
  }
  return path.startsWith(`${requestPathPrefix}${wsPrefix}/`);
}

function isWebSocketUpgrade(request: http.IncomingMessage): boolean {
  const connection = request.headers.connection ?? "";
  const hasUpgradeToken = connection
    .split(",")
    .some((token) => token.trim().toLowerCase() == "upgrade");
  return (
    hasUpgradeToken && request.headers.upgrade?.toLowerCase() == "websocket"
  );
}

// Picks the first offered token this binding recognizes. Every recognized
// token is served: connect-es always has both codecs.
function negotiateSubprotocol(header: string | undefined): string | undefined {
  for (const token of (header ?? "").split(",")) {
    const t = token.trim();
    if (t == subprotocolProto || t == subprotocolJson) {
      return t;
    }
  }
  return undefined;
}

// Same origin means the same host and port, compared exactly as written and
// case-insensitively. The scheme is left out so that TLS terminated at a
// proxy does not reject the deployment's own pages.
function isSameOrigin(request: http.IncomingMessage): boolean {
  const origin = request.headers.origin;
  if (origin === undefined) {
    return true;
  }
  const schemeEnd = origin.indexOf("://");
  if (schemeEnd < 0) {
    return false;
  }
  const originHost = origin.slice(schemeEnd + 3).replace(/\/.*$/, "");
  const host = request.headers.host ?? "";
  return originHost.toLowerCase() == host.toLowerCase();
}

function rejectHandshake(
  socket: stream.Duplex,
  status: number,
  message: string,
  extraHeaders: Record<string, string> = {},
): void {
  const reasons: Record<number, string> = {
    400: "Bad Request",
    403: "Forbidden",
    404: "Not Found",
    426: "Upgrade Required",
  };
  const body = Buffer.from(message, "utf8");
  let head = `HTTP/1.1 ${status} ${reasons[status] ?? ""}\r\n`;
  for (const [name, value] of Object.entries(extraHeaders)) {
    head += `${name}: ${value}\r\n`;
  }
  head +=
    "Content-Type: text/plain; charset=utf-8\r\n" +
    `Content-Length: ${body.byteLength}\r\n` +
    "Connection: close\r\n\r\n";
  socket.end(Buffer.concat([Buffer.from(head, "latin1"), body]));
}
