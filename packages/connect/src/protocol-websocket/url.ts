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

import type { DescMethod } from "@bufbuild/protobuf";
import { Code } from "../code.js";
import { ConnectError } from "../connect-error.js";
import { createMethodUrl } from "../protocol/create-method-url.js";
import { paramTimeout } from "./framing.js";

/**
 * Build the handshake URI: base, then prefix, then procedure. A deadline
 * travels as a query parameter, rounded up so the server never gives up
 * before the client does.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function createWebSocketUrl(
  opt: { baseUrl: string; pathPrefix?: string },
  method: DescMethod,
  timeoutMs: number | undefined,
): string {
  const base =
    opt.baseUrl.replace(/\/?$/, "") + normalizePathPrefix(opt.pathPrefix);
  const page = typeof location != "undefined" ? location.href : undefined;
  let url: URL;
  try {
    url = new URL(createMethodUrl(base, method), page);
  } catch (e) {
    throw new ConnectError(
      `invalid base URL: ${opt.baseUrl}`,
      Code.Unavailable,
      undefined,
      undefined,
      e,
    );
  }
  switch (url.protocol) {
    case "http:":
    case "ws:":
      url.protocol = "ws:";
      break;
    case "https:":
    case "wss:":
      url.protocol = "wss:";
      break;
    default:
      throw new ConnectError(
        `unsupported URL scheme ${url.protocol}`,
        Code.Unavailable,
      );
  }
  if (timeoutMs !== undefined && timeoutMs > 0) {
    const encoded = Math.ceil(timeoutMs).toString(10);
    if (encoded.length <= 10) {
      url.searchParams.set(paramTimeout, encoded);
    }
  }
  return url.toString();
}

/**
 * Normalize a path prefix to one leading slash and no trailing slash, so
 * "ws", "/ws" and "/ws/" are the same. The empty string disables it.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function normalizePathPrefix(prefix: string | undefined): string {
  const trimmed = (prefix ?? "").replace(/^\/+|\/+$/g, "");
  return trimmed == "" ? "" : `/${trimmed}`;
}
