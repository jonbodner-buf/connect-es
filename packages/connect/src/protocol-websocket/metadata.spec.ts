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

import { describe, it } from "node:test";
import * as assert from "node:assert";
import {
  decodeMetadata,
  defaultInfrastructureHeaders,
  encodeMetadata,
  reservedHeaderReason,
} from "./metadata.js";
import {
  closeCodeForFault,
  closeReason,
  decodeFrame,
  faultOf,
  normalizeCloseCode,
} from "./framing.js";
import { createWebSocketUrl, normalizePathPrefix } from "./url.js";
import { create } from "@bufbuild/protobuf";
import { FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";
import { createFileRegistry } from "@bufbuild/protobuf";

describe("decodeMetadata()", () => {
  it("accepts the empty object", () => {
    assert.deepStrictEqual([...decodeMetadata("{}").keys()], []);
  });
  it("rejects an empty payload", () => {
    assert.throws(() => decodeMetadata(""), /empty M message/);
  });
  it("rejects a non-object", () => {
    assert.throws(() => decodeMetadata("[]"), /not a JSON object/);
  });
  it("rejects bare string values", () => {
    assert.throws(() => decodeMetadata('{"a":"b"}'), /array of strings/);
  });
  it("rejects identical duplicate keys", () => {
    assert.throws(
      () => decodeMetadata('{"a":["1"], "a":["2"]}'),
      /are the same key/,
    );
  });
  it("rejects keys differing only in case", () => {
    assert.throws(
      () => decodeMetadata('{"a":{"x":["y"]},"b":["2"],"A":["2"]}'),
      /are the same key/,
    );
  });
  it("rejects an invalid field name", () => {
    assert.throws(
      () => decodeMetadata('{"a b":["1"]}'),
      /valid HTTP field name/,
    );
  });
  it("rejects CR, LF, and NUL in values", () => {
    for (const bad of ["a\\rb", "a\\nb", "a\\u0000b"]) {
      assert.throws(
        () => decodeMetadata(`{"a":["${bad}"]}`),
        /valid HTTP field value/,
      );
    }
  });
  it("accepts padded and unpadded base64 for -bin keys", () => {
    const h = decodeMetadata('{"x-bin":["AAE","AAE="]}');
    assert.strictEqual(h.get("x-bin"), "AAE, AAE=");
  });
  it("rejects invalid base64 for -bin keys", () => {
    assert.throws(() => decodeMetadata('{"x-bin":["A"]}'), /not base64/);
    assert.throws(() => decodeMetadata('{"x-bin":["a-b_"]}'), /not base64/);
  });
  it("accepts an empty array", () => {
    assert.strictEqual(decodeMetadata('{"a":[]}').has("a"), true);
  });
  it("classifies failures as metadata faults", () => {
    try {
      decodeMetadata("[]");
    } catch (e) {
      assert.strictEqual(faultOf(e), "metadata");
    }
  });
});

describe("encodeMetadata()", () => {
  it("lower-cases keys and removes base64 padding", () => {
    const h = new Headers({ "X-Custom": "v", "X-Thing-Bin": "AAE=" });
    assert.deepStrictEqual(encodeMetadata(h), {
      "x-custom": ["v"],
      "x-thing-bin": ["AAE"],
    });
  });
});

describe("reservedHeaderReason()", () => {
  const infra = [...defaultInfrastructureHeaders];
  it("forbids Fetch-forbidden names", () => {
    for (const key of [
      "Cookie",
      "host",
      "Sec-Anything",
      "proxy-x",
      "X-HTTP-Method",
    ]) {
      assert.match(reservedHeaderReason(key, infra) ?? "", /Fetch standard/);
    }
  });
  it("forbids protocol-controlled names", () => {
    for (const key of ["Content-Type", "connect-timeout-ms"]) {
      assert.match(reservedHeaderReason(key, infra) ?? "", /this protocol/);
    }
  });
  it("forbids the infrastructure deny list with wildcards", () => {
    for (const key of ["forwarded", "X-Forwarded-For", "x-real-ip"]) {
      assert.match(reservedHeaderReason(key, infra) ?? "", /deny list/);
    }
    assert.strictEqual(reservedHeaderReason("x-forwarded-for", []), undefined);
  });
  it("allows ordinary names", () => {
    assert.strictEqual(reservedHeaderReason("authorization", infra), undefined);
  });
});

describe("framing", () => {
  it("rejects an empty frame", () => {
    assert.throws(
      () => decodeFrame({ text: true, data: new Uint8Array() }),
      /no marker/,
    );
  });
  it("rejects the reserved high bit", () => {
    assert.throws(
      () => decodeFrame({ text: false, data: new Uint8Array([0x80]) }),
      /reserved high bit/,
    );
  });
  it("maps faults to close codes", () => {
    assert.strictEqual(closeCodeForFault("frame_type", "server"), 1003);
    assert.strictEqual(closeCodeForFault("size_limit", "client"), 3109);
    assert.strictEqual(closeCodeForFault("marker", "client"), 3102);
    assert.strictEqual(normalizeCloseCode(3109), 1009);
    assert.strictEqual(normalizeCloseCode(1000), 1000);
  });
  it("truncates a close reason to 123 bytes", () => {
    const reason = closeReason("é".repeat(100));
    assert.ok(new TextEncoder().encode(reason).byteLength <= 123);
  });
});

describe("createWebSocketUrl()", () => {
  const registry = createFileRegistry(
    create(FileDescriptorSetSchema, {
      file: [
        {
          name: "a.proto",
          package: "p",
          messageType: [{ name: "M" }],
          service: [
            {
              name: "S",
              method: [{ name: "Do", inputType: ".p.M", outputType: ".p.M" }],
            },
          ],
        },
      ],
    }),
  );
  const method = registry.getService("p.S")?.methods[0];
  assert.ok(method);
  it("joins base, prefix, and procedure without doubling slashes", () => {
    assert.strictEqual(
      createWebSocketUrl(
        { baseUrl: "https://a.com/api/", pathPrefix: "ws/" },
        method,
        undefined,
      ),
      "wss://a.com/api/ws/p.S/Do",
    );
    assert.strictEqual(
      createWebSocketUrl({ baseUrl: "http://a.com" }, method, undefined),
      "ws://a.com/p.S/Do",
    );
  });
  it("rounds the timeout up", () => {
    assert.strictEqual(
      createWebSocketUrl({ baseUrl: "ws://a.com" }, method, 1.2),
      "ws://a.com/p.S/Do?connect-timeout-ms=2",
    );
  });
  it("normalizes the prefix", () => {
    assert.strictEqual(normalizePathPrefix("/ws/"), "/ws");
    assert.strictEqual(normalizePathPrefix(""), "");
  });
});
