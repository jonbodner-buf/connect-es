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

import { after, before, describe, it } from "node:test";
import * as assert from "node:assert";
import * as http from "node:http";
import type { AddressInfo } from "node:net";
import { Code, ConnectError } from "@connectrpc/connect";
import type { ConnectRouter } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import {
  browserNames,
  buildBrowserScript,
  parseCaseResults,
  runBrowserTest,
  testPage,
  withBrowser,
} from "./browser.js";
import type { CaseResult } from "./page-runner.js";
import { ElizaService } from "./gen/connectrpc/eliza/v1/eliza_pb.js";

// Runs the Connect-over-WebSocket client in headless Chrome against a
// connect-node server. Run with: npm run test:browser
// CONNECT_BROWSERS selects the browsers, for example "chrome,firefox,safari".

function routes(router: ConnectRouter): void {
  router.service(ElizaService, {
    say(req, ctx) {
      switch (req.sentence) {
        case "fail":
          ctx.responseTrailer.set("trailer-key", "trailer-value");
          throw new ConnectError("failed on request", Code.FailedPrecondition);
        case "headers":
          ctx.responseHeader.set("header-key", "header-value");
          ctx.responseTrailer.set("trailer-key", "trailer-value");
          return { sentence: ctx.requestHeader.get("x-custom") ?? "" };
        case "slow":
          return new Promise((_, reject) => {
            ctx.signal.addEventListener("abort", () =>
              reject(ctx.signal.reason as unknown),
            );
          });
        default:
          return { sentence: `you said: ${req.sentence}` };
      }
    },
    async *introduce(req) {
      for (let i = 0; i < 3; i++) {
        yield { sentence: `${req.name} ${i}` };
      }
    },
    async *converse(reqs) {
      for await (const req of reqs) {
        yield { sentence: `echo: ${req.sentence}` };
      }
    },
  });
}

for (const browserName of browserNames()) {
  describe(`WebSocket transport in ${browserName}`, () => {
    const script = buildBrowserScript("browserscript.ts");
    let server: http.Server;
    let results: Record<string, CaseResult> = {};

    before(async () => {
      const handler = connectNodeAdapter({
        routes,
        async fallback(req, res) {
          res.writeHead(200, { "content-type": "text/html" });
          res.end(testPage(await script));
        },
      });
      server = http.createServer(handler);
      server.on("upgrade", handler.upgrade);
      await new Promise<void>((resolve) => server.listen(0, resolve));
      const port = (server.address() as AddressInfo).port;
      results = await withBrowser(
        browserName,
        `http://127.0.0.1:${port}/`,
        async (browser) =>
          parseCaseResults(
            await runBrowserTest(browser, "webSocketTests", [
              `http://127.0.0.1:${port}`,
              // The page is on 127.0.0.1, so localhost is another origin.
              `http://localhost:${port}`,
            ]),
          ),
      );
    });

    after(async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    });

    for (const name of [
      "unaryJson",
      "unaryBinary",
      "serverStreaming",
      "bidiStreamingIsFullDuplex",
      "metadata",
      "error",
      "deadline",
      "cancel",
      "failedHandshake",
      "crossOriginIsRejected",
    ]) {
      it(name, () => {
        const result = results[name];
        assert.ok(result !== undefined, `no result for ${name}`);
        assert.ok(result.ok, result.detail);
      });
    }
  });
}
