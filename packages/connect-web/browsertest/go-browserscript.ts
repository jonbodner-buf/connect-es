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

import {
  createFileRegistry,
  fromBinary,
  fromJson,
  toJson,
} from "@bufbuild/protobuf";
import type {
  DescMethod,
  DescMethodStreaming,
  DescMethodUnary,
  DescService,
  JsonValue,
} from "@bufbuild/protobuf";
import { base64Decode } from "@bufbuild/protobuf/wire";
import { FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";
import type { Transport } from "@connectrpc/connect";
import { createWebSocketTransport } from "../src/websocket-transport.js";
import { registerBrowserTest } from "./page-runner.js";
import type { CaseResult } from "./page-runner.js";

// Runs in the browser against the connect-go interop server. The schema is
// built at test time from connect-go's ping.proto, so calls go through the
// Transport with runtime descriptors rather than generated code.

async function runGoInteropTests(
  baseUrl: string,
  descriptorSetBase64: string,
): Promise<Record<string, CaseResult>> {
  const service = pingService(descriptorSetBase64);
  const cases: Record<string, () => Promise<string>> = {};
  for (const useBinaryFormat of [false, true]) {
    const codec = useBinaryFormat ? "proto" : "json";
    const transport = createWebSocketTransport({ baseUrl, useBinaryFormat });
    cases[`unary_${codec}`] = () => unary(transport, service);
    cases[`serverStreaming_${codec}`] = () =>
      serverStreaming(transport, service);
    cases[`clientStreaming_${codec}`] = () =>
      clientStreaming(transport, service);
    cases[`bidiStreaming_${codec}`] = () => bidiStreaming(transport, service);
    cases[`error_${codec}`] = () => failure(transport, service);
  }
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

registerBrowserTest("goInteropTests", runGoInteropTests);

// Must run on a page whose origin the Go server does not allow. The browser
// hides the 403, so the RPC fails as unavailable.
async function runGoCrossOriginTest(
  baseUrl: string,
  descriptorSetBase64: string,
): Promise<CaseResult> {
  const transport = createWebSocketTransport({ baseUrl });
  try {
    const err = await rejection(
      unary(transport, pingService(descriptorSetBase64)),
    );
    expect(err.code == Code.Unavailable, err.message);
    return { ok: true, detail: err.message };
  } catch (e) {
    return { ok: false, detail: String(e) };
  }
}

registerBrowserTest("goCrossOriginTest", runGoCrossOriginTest);

function pingService(descriptorSetBase64: string): DescService {
  const service = createFileRegistry(
    fromBinary(FileDescriptorSetSchema, base64Decode(descriptorSetBase64)),
  ).getService("connect.ping.v1.PingService");
  if (service === undefined) {
    throw new Error("PingService not found in the descriptor set");
  }
  return service;
}

async function unary(
  transport: Transport,
  service: DescService,
): Promise<string> {
  const ping = method(service, "Ping", "unary");
  const res = await transport.unary(
    ping,
    undefined,
    5000,
    { "x-custom": "from-browser" },
    fromJson(ping.input, { number: "42", text: "hello" }),
  );
  const output = JSON.stringify(toJson(ping.output, res.message));
  expect(output == '{"number":"42","text":"hello"}', output);
  expect(res.header.get("ping-header") == "from-browser", "missing header");
  expect(res.trailer.get("ping-trailer") == "trailer-value", "missing trailer");
  return output;
}

async function serverStreaming(
  transport: Transport,
  service: DescService,
): Promise<string> {
  const countUp = method(service, "CountUp", "server_streaming");
  const res = await transport.stream(
    countUp,
    undefined,
    5000,
    undefined,
    once(fromJson(countUp.input, { number: "5" })),
  );
  const numbers: JsonValue[] = [];
  for await (const message of res.message) {
    numbers.push(field(toJson(countUp.output, message), "number"));
  }
  expect(numbers.join(",") == "1,2,3,4,5", numbers.join(","));
  expect(res.trailer.get("ping-trailer") == "trailer-value", "missing trailer");
  return numbers.join(",");
}

async function clientStreaming(
  transport: Transport,
  service: DescService,
): Promise<string> {
  const sum = method(service, "Sum", "client_streaming");
  async function* input() {
    for (const number of ["1", "2", "3"]) {
      yield fromJson(sum.input, { number });
    }
  }
  const res = await transport.stream(sum, undefined, 5000, undefined, input());
  const sums: JsonValue[] = [];
  for await (const message of res.message) {
    sums.push(field(toJson(sum.output, message), "sum"));
  }
  expect(sums.join(",") == "6", sums.join(","));
  return sums.join(",");
}

// Each request is sent only after the previous response arrives, which a
// half-duplex transport would deadlock on.
async function bidiStreaming(
  transport: Transport,
  service: DescService,
): Promise<string> {
  const cumSum = method(service, "CumSum", "bidi_streaming");
  let answered: () => void = () => {};
  async function* input() {
    for (const number of ["1", "2", "3"]) {
      yield fromJson(cumSum.input, { number });
      await new Promise<void>((resolve) => {
        answered = resolve;
      });
    }
  }
  const res = await transport.stream(
    cumSum,
    undefined,
    5000,
    undefined,
    input(),
  );
  const sums: JsonValue[] = [];
  for await (const message of res.message) {
    sums.push(field(toJson(cumSum.output, message), "sum"));
    answered();
  }
  expect(sums.join(",") == "1,3,6", sums.join(","));
  return sums.join(",");
}

async function failure(
  transport: Transport,
  service: DescService,
): Promise<string> {
  const fail = method(service, "Fail", "unary");
  const err = await rejection(
    transport.unary(
      fail,
      undefined,
      5000,
      undefined,
      fromJson(fail.input, { code: Code.ResourceExhausted }),
    ),
  );
  expect(err.code == Code.ResourceExhausted, err.message);
  expect(err.rawMessage == "failed as requested", err.message);
  expect(
    err.metadata.get("ping-trailer") == "trailer-value",
    "missing trailer",
  );
  return err.message;
}

type MethodOfKind<K extends DescMethod["methodKind"]> = Extract<
  DescMethodUnary | DescMethodStreaming,
  { methodKind: K }
>;

function method<K extends DescMethod["methodKind"]>(
  service: DescService,
  name: string,
  kind: K,
): MethodOfKind<K> {
  const found = service.methods.find((m) => m.name == name);
  if (found === undefined || !isKind(found, kind)) {
    throw new Error(`no ${kind} method ${name}`);
  }
  return found;
}

function isKind<K extends DescMethod["methodKind"]>(
  m: DescMethod,
  kind: K,
): m is MethodOfKind<K> {
  return m.methodKind == kind;
}

async function* once<T>(value: T): AsyncIterable<T> {
  yield value;
}

function field(json: JsonValue, name: string): JsonValue {
  if (typeof json != "object" || json === null || Array.isArray(json)) {
    throw new Error(`expected an object, got ${JSON.stringify(json)}`);
  }
  return json[name] ?? null;
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
