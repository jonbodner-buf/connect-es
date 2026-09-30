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
import { checkNoContextTakeover } from "./transport.js";

describe("checkNoContextTakeover()", () => {
  it("accepts no compression", () => {
    assert.strictEqual(checkNoContextTakeover("", false), undefined);
  });
  it("accepts no-context-takeover in both directions", () => {
    assert.strictEqual(
      checkNoContextTakeover(
        "permessage-deflate; client_no_context_takeover; server_no_context_takeover",
        false,
      ),
      undefined,
    );
  });
  it("rejects a shared context in either direction", () => {
    for (const extensions of [
      "permessage-deflate; client_no_context_takeover",
      "permessage-deflate; server_no_context_takeover",
    ]) {
      assert.match(
        checkNoContextTakeover(extensions, true)?.message ?? "",
        /without no-context-takeover/,
      );
    }
  });
  it("rejects a bare permessage-deflate unless parameters are hidden", () => {
    assert.match(
      checkNoContextTakeover("permessage-deflate", false)?.message ?? "",
      /without no-context-takeover/,
    );
    // Firefox reports the negotiated extension by name only.
    assert.strictEqual(
      checkNoContextTakeover("permessage-deflate", true),
      undefined,
    );
  });
});
