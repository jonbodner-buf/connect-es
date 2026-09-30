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
import {
  Code,
  ConnectError,
  createClient,
  encodeBinaryHeader,
} from "@connectrpc/connect";
import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { connectNodeAdapter } from "./connect-node-adapter.js";
import type { NodeWebSocketOptions } from "./node-websocket-upgrade.js";
import { createWebSocketTransport } from "./websocket-transport.js";
import { ElizaService } from "./testdata/gen/connectrpc/eliza/v1/eliza_pb.js";

function routes(router: ConnectRouter): void {
  router.service(ElizaService, {
    say(req, ctx) {
      if (req.sentence == "fail") {
        ctx.responseTrailer.set("trailer-key", "trailer-value");
        throw new ConnectError("failed on request", Code.FailedPrecondition);
      }
      if (req.sentence == "headers") {
        ctx.responseHeader.set("header-key", "header-value");
        ctx.responseTrailer.set("trailer-key", "trailer-value");
        return { sentence: ctx.requestHeader.get("x-custom") ?? "" };
      }
      if (req.sentence == "slow") {
        return new Promise((_, reject) => {
          ctx.signal.addEventListener("abort", () =>
            reject(ctx.signal.reason as unknown),
          );
        });
      }
      return { sentence: `you said: ${req.sentence}` };
    },
    async *introduce(req) {
      for (let i = 0; i < 3; i++) {
        yield { sentence: `${req.name} ${i}` };
      }
    },
    async *converse(reqs, ctx: HandlerContext) {
      for await (const req of reqs) {
        yield { sentence: `echo: ${req.sentence}` };
      }
      ctx.responseTrailer.set("done", "yes");
    },
  });
}

async function startServer(
  webSocketOptions?: NodeWebSocketOptions,
  readMaxBytes?: number,
): Promise<{ baseUrl: string; server: http.Server }> {
  const handler = connectNodeAdapter({
    routes,
    webSocketOptions,
    readMaxBytes,
  });
  const server = http.createServer(handler);
  server.on("upgrade", handler.upgrade);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  return { baseUrl: `http://127.0.0.1:${port}`, server };
}

async function stopServer(server: http.Server): Promise<void> {
  server.closeAllConnections();
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

for (const useBinaryFormat of [true, false]) {
  describe(`WebSocket transport (${useBinaryFormat ? "proto" : "json"})`, () => {
    let baseUrl = "";
    let server: http.Server;
    before(async () => {
      ({ baseUrl, server } = await startServer());
    });
    after(() => stopServer(server));

    function client() {
      return createClient(
        ElizaService,
        createWebSocketTransport({ baseUrl, useBinaryFormat }),
      );
    }

    it("calls a unary RPC", async () => {
      const res = await client().say({ sentence: "hi" });
      assert.strictEqual(res.sentence, "you said: hi");
    });

    it("carries request metadata, response headers, and trailers", async () => {
      let header: Headers | undefined;
      let trailer: Headers | undefined;
      const res = await client().say(
        { sentence: "headers" },
        {
          headers: { "x-custom": "from-m" },
          onHeader: (h) => {
            header = h;
          },
          onTrailer: (t) => {
            trailer = t;
          },
        },
      );
      assert.strictEqual(res.sentence, "from-m");
      assert.strictEqual(header?.get("header-key"), "header-value");
      assert.strictEqual(trailer?.get("trailer-key"), "trailer-value");
    });

    it("reports an error from S with its trailers", async () => {
      try {
        await client().say({ sentence: "fail" });
        assert.fail("expected an error");
      } catch (e) {
        const err = ConnectError.from(e);
        assert.strictEqual(err.code, Code.FailedPrecondition);
        assert.strictEqual(err.rawMessage, "failed on request");
        assert.strictEqual(err.metadata.get("trailer-key"), "trailer-value");
      }
    });

    it("calls a server-streaming RPC", async () => {
      const sentences: string[] = [];
      for await (const res of client().introduce({ name: "ws" })) {
        sentences.push(res.sentence);
      }
      assert.deepStrictEqual(sentences, ["ws 0", "ws 1", "ws 2"]);
    });

    it("calls a bidi-streaming RPC", async () => {
      async function* input() {
        yield { sentence: "a" };
        yield { sentence: "b" };
      }
      let trailer: Headers | undefined;
      const sentences: string[] = [];
      for await (const res of client().converse(input(), {
        onTrailer: (t) => {
          trailer = t;
        },
      })) {
        sentences.push(res.sentence);
      }
      assert.deepStrictEqual(sentences, ["echo: a", "echo: b"]);
      assert.strictEqual(trailer?.get("done"), "yes");
    });

    it("carries -bin metadata", async () => {
      const value = encodeBinaryHeader(new Uint8Array([0, 1, 255]));
      const res = await client().say(
        { sentence: "headers" },
        { headers: { "x-custom": "x", "x-thing-bin": value } },
      );
      assert.strictEqual(res.sentence, "x");
    });

    it("enforces the deadline", async () => {
      try {
        await client().say({ sentence: "slow" }, { timeoutMs: 50 });
        assert.fail("expected an error");
      } catch (e) {
        assert.strictEqual(ConnectError.from(e).code, Code.DeadlineExceeded);
      }
    });

    it("cancels an RPC", async () => {
      const controller = new AbortController();
      setTimeout(() => controller.abort(), 50);
      try {
        await client().say({ sentence: "slow" }, { signal: controller.signal });
        assert.fail("expected an error");
      } catch (e) {
        assert.strictEqual(ConnectError.from(e).code, Code.Canceled);
      }
    });
  });
}

// Opens a raw WebSocket, so a test can send what a conforming client would
// not, and collects everything the server sends back.
function rawCall(
  url: string,
  protocol: string,
  send: (ws: WebSocket) => void,
): Promise<{ messages: (string | Uint8Array)[]; code: number }> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, protocol);
    ws.binaryType = "arraybuffer";
    const messages: (string | Uint8Array)[] = [];
    ws.addEventListener("open", () => send(ws));
    ws.addEventListener("message", (event: MessageEvent) => {
      messages.push(
        typeof event.data == "string"
          ? event.data
          : new Uint8Array(event.data as ArrayBuffer),
      );
    });
    ws.addEventListener("close", (event: CloseEvent) =>
      resolve({ messages, code: event.code }),
    );
    ws.addEventListener("error", () => {
      // The close event follows with the code.
    });
    setTimeout(() => reject(new Error("timed out")), 5000);
  });
}

function endStreamError(
  messages: (string | Uint8Array)[],
): { code?: string; message?: string } | undefined {
  const last = messages[messages.length - 1];
  assert.strictEqual(typeof last, "string");
  assert.ok((last as string).startsWith("S"));
  const end = JSON.parse((last as string).slice(1)) as {
    error?: { code?: string; message?: string };
  };
  return end.error;
}

function handshakeStatus(
  url: string,
  headers: Record<string, string>,
): Promise<number> {
  return new Promise((resolve, reject) => {
    const req = http.get(url, {
      headers: {
        Connection: "Upgrade",
        Upgrade: "websocket",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
        ...headers,
      },
    });
    req.on("response", (res) => {
      res.resume();
      resolve(res.statusCode ?? 0);
    });
    req.on("upgrade", (res, socket) => {
      socket.destroy();
      resolve(res.statusCode ?? 0);
    });
    req.on("error", reject);
  });
}

describe("WebSocket server protocol rules", () => {
  let wsUrl = "";
  let httpUrl = "";
  let server: http.Server;
  before(async () => {
    const started = await startServer(undefined, 64);
    server = started.server;
    httpUrl = `${started.baseUrl}/connectrpc.eliza.v1.ElizaService/Say`;
    wsUrl = httpUrl.replace("http:", "ws:");
  });
  after(() => stopServer(server));

  it("follows the worked JSON exchange of the spec", async () => {
    const { messages, code } = await rawCall(
      wsUrl,
      "connectrpc.1+json",
      (ws) => {
        ws.send("M{}");
        ws.send('B{"sentence":"7"}');
        ws.send("C");
      },
    );
    assert.deepStrictEqual(messages, [
      "M{}",
      'B{"sentence":"you said: 7"}',
      "S{}",
    ]);
    assert.strictEqual(code, 1000);
  });

  it("rejects an unknown marker with S and 1002", async () => {
    const { messages, code } = await rawCall(
      wsUrl,
      "connectrpc.1+json",
      (ws) => {
        ws.send("M{}");
        ws.send("X{}");
      },
    );
    assert.strictEqual(messages[0], "M{}");
    assert.strictEqual(endStreamError(messages)?.code, "invalid_argument");
    assert.strictEqual(code, 1002);
  });

  it("requires M as the first message", async () => {
    const { messages, code } = await rawCall(
      wsUrl,
      "connectrpc.1+json",
      (ws) => {
        ws.send('B{"sentence":"7"}');
      },
    );
    assert.strictEqual(messages[0], "M{}");
    assert.match(
      endStreamError(messages)?.message ?? "",
      /first message must be M/,
    );
    assert.strictEqual(code, 1002);
  });

  it("rejects a reserved key in M", async () => {
    const { messages } = await rawCall(wsUrl, "connectrpc.1+json", (ws) => {
      ws.send('M{"x-forwarded-for":["1.2.3.4"]}');
    });
    assert.match(
      endStreamError(messages)?.message ?? "",
      /infrastructure deny list/,
    );
  });

  it("rejects keys that differ only in case", async () => {
    const { messages } = await rawCall(wsUrl, "connectrpc.1+json", (ws) => {
      ws.send('M{"acme":["a"],"Acme":["b"]}');
    });
    assert.match(endStreamError(messages)?.message ?? "", /are the same key/);
  });

  it("rejects a body in the wrong frame type with 1003", async () => {
    const { messages, code } = await rawCall(
      wsUrl,
      "connectrpc.1+json",
      (ws) => {
        ws.send("M{}");
        ws.send(new Uint8Array([0x42, 0x0a, 0x01, 0x37]));
      },
    );
    assert.match(
      endStreamError(messages)?.message ?? "",
      /negotiated subprotocol/,
    );
    assert.strictEqual(code, 1003);
  });

  it("rejects an oversized message with resource_exhausted and 1009", async () => {
    const { messages, code } = await rawCall(
      wsUrl,
      "connectrpc.1+json",
      (ws) => {
        ws.send("M{}");
        ws.send(`B{"sentence":"${"x".repeat(100)}"}`);
      },
    );
    assert.strictEqual(endStreamError(messages)?.code, "resource_exhausted");
    assert.strictEqual(code, 1009);
  });

  it("rejects a message after C", async () => {
    const { messages, code } = await rawCall(
      wsUrl.replace("Say", "Converse"),
      "connectrpc.1+json",
      (ws) => {
        ws.send("M{}");
        ws.send("C");
        ws.send('B{"sentence":"late"}');
      },
    );
    assert.match(
      endStreamError(messages)?.message ?? "",
      /after its C message/,
    );
    assert.strictEqual(code, 1002);
  });

  it("reports an invalid timeout in band", async () => {
    const { messages, code } = await rawCall(
      `${wsUrl}?connect-timeout-ms=abc`,
      "connectrpc.1+json",
      () => {
        // The server answers without waiting for M.
      },
    );
    assert.strictEqual(messages[0], "M{}");
    assert.strictEqual(endStreamError(messages)?.code, "invalid_argument");
    assert.strictEqual(code, 1000);
  });

  it("answers an unknown subprotocol with 400", async () => {
    assert.strictEqual(
      await handshakeStatus(httpUrl, { "Sec-WebSocket-Protocol": "chat" }),
      400,
    );
  });

  it("answers a cross-origin handshake with 403", async () => {
    assert.strictEqual(
      await handshakeStatus(httpUrl, {
        "Sec-WebSocket-Protocol": "connectrpc.1+json",
        Origin: "https://evil.example.com",
      }),
      403,
    );
  });

  it("accepts a same-origin handshake regardless of scheme", async () => {
    const host = new URL(httpUrl).host;
    assert.strictEqual(
      await handshakeStatus(httpUrl, {
        "Sec-WebSocket-Protocol": "connectrpc.1+json",
        Origin: `https://${host}`,
      }),
      101,
    );
  });
});

describe("WebSocket server cancellation", () => {
  it("aborts the handler's signal when the client goes away", async () => {
    let aborted: (reason: unknown) => void = () => {};
    const abortedReason = new Promise<unknown>((resolve) => {
      aborted = resolve;
    });
    const handler = connectNodeAdapter({
      routes(router) {
        router.service(ElizaService, {
          async *converse(_reqs, ctx) {
            ctx.signal.addEventListener("abort", () =>
              aborted(ctx.signal.reason),
            );
            yield { sentence: "first" };
            await new Promise(() => {
              // Wait for the client to go away.
            });
          },
        });
      },
    });
    const server = http.createServer(handler);
    server.on("upgrade", handler.upgrade);
    await new Promise<void>((resolve) => server.listen(0, resolve));
    const port = (server.address() as AddressInfo).port;
    try {
      const client = createClient(
        ElizaService,
        createWebSocketTransport({ baseUrl: `http://127.0.0.1:${port}` }),
      );
      const controller = new AbortController();
      async function* input() {
        yield { sentence: "a" };
        await new Promise(() => {
          // Keep the request stream open.
        });
      }
      for await (const res of client.converse(input(), {
        signal: controller.signal,
      })) {
        assert.strictEqual(res.sentence, "first");
        controller.abort();
        break;
      }
      const reason = await abortedReason;
      assert.strictEqual(ConnectError.from(reason).code, Code.Canceled);
    } finally {
      await stopServer(server);
    }
  });
});

describe("WebSocket path prefix", () => {
  let baseUrl = "";
  let server: http.Server;
  before(async () => {
    ({ baseUrl, server } = await startServer({ pathPrefix: "/ws/" }));
  });
  after(() => stopServer(server));

  it("serves RPCs under the prefix", async () => {
    const client = createClient(
      ElizaService,
      createWebSocketTransport({ baseUrl, pathPrefix: "ws" }),
    );
    const res = await client.say({ sentence: "prefixed" });
    assert.strictEqual(res.sentence, "you said: prefixed");
  });

  it("answers a plain request under the prefix with 426", async () => {
    const res = await fetch(
      `${baseUrl}/ws/connectrpc.eliza.v1.ElizaService/Say`,
      { method: "POST" },
    );
    assert.strictEqual(res.status, 426);
    assert.strictEqual(res.headers.get("upgrade"), "websocket");
  });

  it("answers an upgrade at the bare path with 400", async () => {
    assert.strictEqual(
      await handshakeStatus(`${baseUrl}/connectrpc.eliza.v1.ElizaService/Say`, {
        "Sec-WebSocket-Protocol": "connectrpc.1+json",
      }),
      400,
    );
  });
});
