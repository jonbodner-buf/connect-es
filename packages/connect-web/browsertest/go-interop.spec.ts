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
import { execFile, spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import * as http from "node:http";
import * as net from "node:net";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import {
  browserNames,
  buildBrowserScript,
  parseCaseResult,
  parseCaseResults,
  runBrowserTest,
  testPage,
  withBrowser,
} from "./browser.js";
import type { CaseResult } from "./page-runner.js";

// Runs the Connect-over-WebSocket client in headless Chrome against the
// connect-go interop server, which implements all four stream types. Set
// CONNECT_GO_DIR to a connect-go checkout with WebSocket support, then run:
// npm run test:browser:go
// CONNECT_BROWSERS selects the browsers, as for websocket.spec.ts.

const connectGoDir = process.env.CONNECT_GO_DIR;
const exec = promisify(execFile);

const caseNames = [
  "unary_json",
  "serverStreaming_json",
  "clientStreaming_json",
  "bidiStreaming_json",
  "error_json",
  "unary_proto",
  "serverStreaming_proto",
  "clientStreaming_proto",
  "bidiStreaming_proto",
  "error_proto",
  "crossOriginIsRejected",
];

for (const browserName of browserNames()) {
  describe(
    `WebSocket transport in ${browserName} against connect-go`,
    { skip: connectGoDir === undefined ? "CONNECT_GO_DIR is not set" : false },
    () => {
      let workDir = "";
      let pageServer: http.Server | undefined;
      let goServer: ChildProcess | undefined;
      let results: Record<string, CaseResult> = {};

      before(async () => {
        if (connectGoDir === undefined) {
          return;
        }
        workDir = await mkdtemp(join(tmpdir(), "connect-web-go-interop-"));
        const [descriptorSet, script] = await Promise.all([
          buildPingDescriptorSet(connectGoDir, workDir),
          buildBrowserScript("go-browserscript.ts"),
          buildGoServer(connectGoDir, workDir),
        ]);
        pageServer = http.createServer((_req, res) => {
          res.writeHead(200, { "content-type": "text/html" });
          res.end(testPage(script));
        });
        const pagePort = await listen(pageServer);
        const pageOrigin = `http://127.0.0.1:${pagePort}`;
        const goPort = await freePort();
        // The page is served from another port, so the Go server allows its
        // origin explicitly.
        goServer = await startGoServer(join(workDir, "server"), [
          "-addr",
          `127.0.0.1:${goPort}`,
          "-allow-origin",
          pageOrigin,
        ]);
        const base64 = descriptorSet.toString("base64");
        const goBaseUrl = `http://127.0.0.1:${goPort}`;
        results = await withBrowser(
          browserName,
          `${pageOrigin}/`,
          async (browser) => {
            const allowed = parseCaseResults(
              await runBrowserTest(browser, "goInteropTests", [
                goBaseUrl,
                base64,
              ]),
            );
            // localhost is a different origin from 127.0.0.1, and not allowed.
            await browser.url(`http://localhost:${pagePort}/`);
            const crossOrigin = parseCaseResult(
              await runBrowserTest(browser, "goCrossOriginTest", [
                goBaseUrl,
                base64,
              ]),
            );
            return { ...allowed, crossOriginIsRejected: crossOrigin };
          },
        );
      });

      after(async () => {
        goServer?.kill();
        if (pageServer !== undefined) {
          const server = pageServer;
          server.closeAllConnections();
          await new Promise<void>((resolve) => server.close(() => resolve()));
        }
        if (workDir !== "") {
          await rm(workDir, { recursive: true, force: true });
        }
      });

      for (const name of caseNames) {
        it(name, () => {
          const result = results[name];
          assert.ok(result !== undefined, `no result for ${name}`);
          assert.ok(result.ok, result.detail);
        });
      }
    },
  );
}

async function buildGoServer(goDir: string, outDir: string): Promise<void> {
  await exec(
    "go",
    ["build", "-o", join(outDir, "server"), "./websocket/all_streams/server"],
    { cwd: join(goDir, "internal", "example") },
  );
}

async function buildPingDescriptorSet(
  goDir: string,
  outDir: string,
): Promise<Buffer> {
  const buf = createRequire(import.meta.url).resolve("@bufbuild/buf/bin/buf");
  const out = join(outDir, "ping.binpb");
  await exec(buf, [
    "build",
    join(goDir, "internal", "proto"),
    "--path",
    join(goDir, "internal", "proto", "connect", "ping", "v1", "ping.proto"),
    "-o",
    out,
  ]);
  return readFile(out);
}

function startGoServer(binary: string, args: string[]): Promise<ChildProcess> {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, { stdio: ["ignore", "ignore", "pipe"] });
    let output = "";
    const timer = setTimeout(() => {
      child.kill();
      reject(new Error(`Go server did not start:\n${output}`));
    }, 10000);
    child.stderr.on("data", (chunk: Buffer) => {
      output += chunk.toString();
      if (output.includes("listening")) {
        clearTimeout(timer);
        resolve(child);
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`Go server exited with ${code}:\n${output}`));
    });
  });
}

async function listen(server: http.Server): Promise<number> {
  // All interfaces, so the page loads as both 127.0.0.1 and localhost.
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const address = server.address();
  if (address === null || typeof address == "string") {
    throw new Error("server has no port");
  }
  return address.port;
}

async function freePort(): Promise<number> {
  const server = net.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  await new Promise<void>((resolve) => server.close(() => resolve()));
  if (address === null || typeof address == "string") {
    throw new Error("server has no port");
  }
  return address.port;
}
