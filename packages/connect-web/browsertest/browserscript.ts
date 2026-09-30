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

import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { createWebSocketTransport } from "../src/websocket-transport.js";
import { ElizaService } from "./gen/connectrpc/eliza/v1/eliza_pb.js";
import { registerBrowserTest } from "./page-runner.js";
import type { CaseResult } from "./page-runner.js";

// Runs in the browser, so every call goes through the browser's own
// WebSocket API rather than Node's.

async function runWebSocketTests(
  baseUrl: string,
  crossOriginBaseUrl: string,
): Promise<Record<string, CaseResult>> {
  const cases: Record<string, () => Promise<string>> = {
    async unaryJson() {
      const res = await client(baseUrl, false).say({ sentence: "hi" });
      expect(res.sentence == "you said: hi", res.sentence);
      return res.sentence;
    },
    async unaryBinary() {
      const res = await client(baseUrl, true).say({ sentence: "hi" });
      expect(res.sentence == "you said: hi", res.sentence);
      return res.sentence;
    },
    async serverStreaming() {
      const sentences: string[] = [];
      for await (const res of client(baseUrl, true).introduce({ name: "b" })) {
        sentences.push(res.sentence);
      }
      expect(sentences.join("|") == "b 0|b 1|b 2", sentences.join("|"));
      return sentences.join("|");
    },
    async bidiStreamingIsFullDuplex() {
      // The second request is only sent after the first response arrives,
      // which a half-duplex transport would deadlock on.
      let answered: () => void = () => {};
      const firstAnswered = new Promise<void>((resolve) => {
        answered = resolve;
      });
      async function* input() {
        yield { sentence: "one" };
        await firstAnswered;
        yield { sentence: "two" };
      }
      const sentences: string[] = [];
      for await (const res of client(baseUrl, false).converse(input())) {
        sentences.push(res.sentence);
        answered();
      }
      expect(sentences.join("|") == "echo: one|echo: two", sentences.join("|"));
      return sentences.join("|");
    },
    async metadata() {
      // A browser cannot set headers on a handshake; the M message carries
      // them instead.
      let header: Headers | undefined;
      let trailer: Headers | undefined;
      const res = await client(baseUrl, true).say(
        { sentence: "headers" },
        {
          headers: { "x-custom": "from-browser" },
          onHeader(h) {
            header = h;
          },
          onTrailer(t) {
            trailer = t;
          },
        },
      );
      expect(res.sentence == "from-browser", res.sentence);
      expect(header?.get("header-key") == "header-value", "missing header");
      expect(trailer?.get("trailer-key") == "trailer-value", "missing trailer");
      return res.sentence;
    },
    async error() {
      const err = await rejection(
        client(baseUrl, true).say({ sentence: "fail" }),
      );
      expect(err.code == Code.FailedPrecondition, err.message);
      expect(err.metadata.get("trailer-key") == "trailer-value", "no trailer");
      return err.message;
    },
    async deadline() {
      const err = await rejection(
        client(baseUrl, true).say({ sentence: "slow" }, { timeoutMs: 100 }),
      );
      expect(err.code == Code.DeadlineExceeded, err.message);
      return err.message;
    },
    async cancel() {
      const controller = new AbortController();
      setTimeout(() => controller.abort(), 100);
      const err = await rejection(
        client(baseUrl, true).say(
          { sentence: "slow" },
          { signal: controller.signal },
        ),
      );
      expect(err.code == Code.Canceled, err.message);
      return err.message;
    },
    async failedHandshake() {
      // The server answers 404, which a browser script cannot see.
      const err = await rejection(
        client(`${baseUrl}/no-such-prefix`, true).say({ sentence: "x" }),
      );
      expect(err.code == Code.Unavailable, err.message);
      expect(err.message.includes("not available to the client"), err.message);
      return err.message;
    },
    async crossOriginIsRejected() {
      const err = await rejection(
        client(crossOriginBaseUrl, true).say({ sentence: "x" }),
      );
      expect(err.code == Code.Unavailable, err.message);
      return err.message;
    },
  };
  const results: Record<string, CaseResult> = {};
  for (const [name, run] of Object.entries(cases)) {
    try {
      results[name] = { ok: true, detail: await run() };
    } catch (e) {
      results[name] = { ok: false, detail: String(e) };
    }
  }
  return results;
}

registerBrowserTest("webSocketTests", runWebSocketTests);

function client(baseUrl: string, useBinaryFormat: boolean) {
  return createClient(
    ElizaService,
    createWebSocketTransport({ baseUrl, useBinaryFormat }),
  );
}

async function rejection(promise: Promise<unknown>): Promise<ConnectError> {
  try {
    await promise;
  } catch (e) {
    return ConnectError.from(e);
  }
  throw new Error("expected the call to fail");
}

function expect(condition: boolean, detail: string): void {
  if (!condition) {
    throw new Error(`unexpected result: ${detail}`);
  }
}
