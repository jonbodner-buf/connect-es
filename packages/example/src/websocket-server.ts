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

import { connectNodeAdapter } from "@connectrpc/connect-node";
import { routes } from "./routes.js";
import * as esbuild from "esbuild";
import * as http from "node:http";
import { readFileSync } from "node:fs";
import { stdout } from "node:process";

const port = 8080;

const handler = connectNodeAdapter({
  routes,
  fallback(req, res) {
    switch (req.url) {
      case "/":
        res.writeHead(200, { "content-type": "text/html" });
        res.write(readFileSync("www/websocket.html", "utf8"), "utf8");
        res.end();
        break;
      case "/style.css":
        res.writeHead(200, { "content-type": "text/css" });
        res.write(readFileSync("www/style.css", "utf8"), "utf8");
        res.end();
        break;
      case "/websocket-webclient.js":
        void esbuild
          .build({
            entryPoints: ["src/websocket-webclient.ts"],
            bundle: true,
            write: false,
          })
          .then((result) => {
            const output = result.outputFiles[0];
            if (output === undefined) {
              throw new Error("esbuild produced no output");
            }
            res.writeHead(200, { "content-type": "application/javascript" });
            res.write(output.text, "utf8");
            res.end();
          });
        break;
      default:
        res.writeHead(404);
        res.end();
    }
  },
});

// A WebSocket handshake is an HTTP/1.1 request, so this server uses plain
// HTTP/1.1. The adapter's upgrade function serves Connect-over-WebSocket;
// every other request, including ordinary Connect RPCs, goes to the handler.
const server = http.createServer(handler);
server.on("upgrade", handler.upgrade);
server.listen(port, () => {
  stdout.write(`The server is listening on http://localhost:${port}\n`);
});
