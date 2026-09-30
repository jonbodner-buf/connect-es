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

import { Code, ConnectError, createConnectRouter } from "@connectrpc/connect";
import type {
  ConnectRouter,
  ConnectRouterOptions,
  ContextValues,
} from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import { uResponseNotFound } from "@connectrpc/connect/protocol";
import {
  universalRequestFromNodeRequest,
  universalResponseToNodeResponse,
} from "./node-universal-handler.js";
import type {
  NodeHandlerFn,
  NodeServerRequest,
  NodeServerResponse,
} from "./node-universal-handler.js";
import { compressionBrotli, compressionGzip } from "./compression.js";
import {
  createNodeUpgradeHandler,
  isUnderWebSocketPrefix,
} from "./node-websocket-upgrade.js";
import type {
  NodeUpgradeHandlerFn,
  NodeWebSocketOptions,
} from "./node-websocket-upgrade.js";

export interface ConnectNodeAdapterOptions extends ConnectRouterOptions {
  /**
   * Route definitions. We recommend the following pattern:
   *
   * Create a file `connect.ts` with a default export such as this:
   *
   * ```ts
   * import {ConnectRouter} from "@connectrpc/connect";
   *
   * export default (router: ConnectRouter) => {
   *   router.service(ElizaService, {});
   * }
   * ```
   *
   * Then pass this function here.
   */
  routes: (router: ConnectRouter) => void;
  /**
   * If none of the handler request paths match, a 404 is served. This option
   * can provide a custom fallback for this case.
   */
  fallback?: NodeHandlerFn;
  /**
   * Serve all handlers under this prefix. For example, the prefix "/something"
   * will serve the RPC foo.FooService/Bar under "/something/foo.FooService/Bar".
   * Note that many gRPC client implementations do not allow for prefixes.
   */
  requestPathPrefix?: string;
  /**
   * Context values to extract from the request. These values are passed to
   * the handlers.
   */
  contextValues?: (req: NodeServerRequest) => ContextValues;
  /**
   * Options for the Connect-over-WebSocket protocol. The protocol is served
   * once the returned handler's `upgrade` function is attached to the
   * "upgrade" event of an http.Server. It requires HTTP/1.1.
   */
  webSocketOptions?: NodeWebSocketOptions;
}

/**
 * A Node.js request handler that also serves WebSocket upgrades.
 */
export type NodeHandlerFnWithUpgrade = NodeHandlerFn & {
  /**
   * Attach to the "upgrade" event of an http.Server to serve the
   * Connect-over-WebSocket protocol:
   *
   * ```ts
   * const handler = connectNodeAdapter({ routes });
   * const server = http.createServer(handler);
   * server.on("upgrade", handler.upgrade);
   * ```
   */
  upgrade: NodeUpgradeHandlerFn;
};

/**
 * Create a Node.js request handler from a ConnectRouter.
 *
 * The returned function is compatible with http.RequestListener and its equivalent for http2.
 * Its `upgrade` property serves WebSocket upgrades; see NodeHandlerFnWithUpgrade.
 */
export function connectNodeAdapter(
  options: ConnectNodeAdapterOptions,
): NodeHandlerFnWithUpgrade {
  if (options.acceptCompression === undefined) {
    options.acceptCompression = [compressionGzip, compressionBrotli];
  }
  const router = createConnectRouter(options);
  options.routes(router);
  const prefix = options.requestPathPrefix ?? "";
  const paths = new Map<string, UniversalHandler>();
  for (const uHandler of router.handlers) {
    paths.set(prefix + uHandler.requestPath, uHandler);
  }
  function nodeRequestHandler(
    req: NodeServerRequest,
    res: NodeServerResponse,
  ): void {
    // Strip the query parameter when matching paths.
    const path = req.url?.split("?", 2)[0] ?? "";
    if (isUnderWebSocketPrefix(path, prefix, options.webSocketOptions)) {
      res.writeHead(426, { Upgrade: "websocket", Connection: "Upgrade" });
      res.end();
      return;
    }
    const uHandler = paths.get(path);
    if (!uHandler) {
      (options.fallback ?? fallback)(req, res);
      return;
    }
    const uReq = universalRequestFromNodeRequest(
      req,
      res,
      undefined,
      options.contextValues?.(req),
    );
    uHandler(uReq)
      .then((uRes) => universalResponseToNodeResponse(uRes, res))
      .catch((reason) => {
        if (ConnectError.from(reason).code == Code.Aborted) {
          return;
        }
        console.error(
          `handler for rpc ${uHandler.method.name} of ${uHandler.service.typeName} failed`,
          reason,
        );
      });
  }
  return Object.assign(nodeRequestHandler, {
    upgrade: createNodeUpgradeHandler(
      router.handlers,
      prefix,
      options.webSocketOptions ?? {},
      options.contextValues,
    ),
  });
}

const fallback: NodeHandlerFn = (request, response) => {
  response.writeHead(uResponseNotFound.status);
  response.end();
};
