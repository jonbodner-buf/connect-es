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

import { peerFault } from "./framing.js";

// Metadata on the wire is a JSON object of string arrays with lower-case
// keys. Headers already hold -bin values as base64, the same form as HTTP,
// so a -bin value only needs its padding removed and validated.

const binarySuffix = "-bin";

/**
 * Render headers for a Leading-Metadata message.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function encodeMetadata(header: Headers): Record<string, string[]> {
  const encoded: Record<string, string[]> = {};
  header.forEach((value, key) => {
    if (key.endsWith(binarySuffix)) {
      // Base64 has no commas, so splitting a joined value is lossless.
      encoded[key] = value
        .split(",")
        .map((v) => v.trim().replace(/=+$/, ""))
        .filter((v) => v.length > 0);
      return;
    }
    if (key == "set-cookie" && typeof header.getSetCookie == "function") {
      encoded[key] = header.getSetCookie();
      return;
    }
    encoded[key] = [value];
  });
  return encoded;
}

/**
 * Parse the payload of a Leading-Metadata message, enforcing the key and
 * value rules of the protocol. Raises a peer fault on any violation.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function decodeMetadata(json: string): Headers {
  if (json.length == 0) {
    throw peerFault(
      "metadata",
      "empty M message; an empty metadata object is {}",
    );
  }
  let value: unknown;
  try {
    value = JSON.parse(json);
  } catch (e) {
    throw peerFault("metadata", "M message is not valid JSON", undefined, e);
  }
  if (typeof value != "object" || value === null || Array.isArray(value)) {
    throw peerFault("metadata", "M message is not a JSON object");
  }
  // JSON.parse keeps only the last of two identical keys, so duplicates
  // have to be found in the source text.
  const spelling = new Map<string, string>();
  for (const key of objectKeys(json)) {
    const lower = key.toLowerCase();
    const first = spelling.get(lower);
    if (first !== undefined) {
      throw peerFault(
        "metadata",
        `metadata keys ${JSON.stringify(first)} and ${JSON.stringify(key)} are the same key; a key carries all its values in one array`,
      );
    }
    spelling.set(lower, key);
  }
  const header = new Headers();
  for (const [key, values] of Object.entries(value)) {
    if (!isToken(key)) {
      throw peerFault(
        "metadata",
        `metadata key ${JSON.stringify(key)} is not a valid HTTP field name`,
      );
    }
    if (!Array.isArray(values) || values.some((v) => typeof v != "string")) {
      throw peerFault(
        "metadata",
        `metadata ${JSON.stringify(key)} must be an array of strings`,
      );
    }
    const binary = key.toLowerCase().endsWith(binarySuffix);
    for (const v of values as string[]) {
      if (binary ? !isBase64(v) : !isFieldValue(v)) {
        throw peerFault(
          "metadata",
          binary
            ? `metadata ${JSON.stringify(key)} is not base64`
            : `metadata ${JSON.stringify(key)} has a value that is not a valid HTTP field value`,
        );
      }
      try {
        header.append(key, v);
      } catch (e) {
        throw peerFault(
          "metadata",
          `metadata ${JSON.stringify(key)} has a value that is not a valid HTTP field value`,
          undefined,
          e,
        );
      }
    }
    if (values.length == 0 && !header.has(key)) {
      header.set(key, "");
    }
  }
  return header;
}

/**
 * Return why a client may not set this key in its Leading-Metadata message,
 * or undefined if it may. A pattern in infrastructure may end with "*" to
 * match any suffix.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function reservedHeaderReason(
  key: string,
  infrastructure: string[],
): string | undefined {
  const lower = key.toLowerCase();
  if (
    fetchForbidden.has(lower) ||
    lower.startsWith("proxy-") ||
    lower.startsWith("sec-")
  ) {
    return "forbidden as a request header by the Fetch standard";
  }
  if (protocolControlled.has(lower)) {
    return "controlled by this protocol";
  }
  for (const pattern of infrastructure) {
    const p = pattern.toLowerCase();
    const matches = p.endsWith("*")
      ? lower.startsWith(p.slice(0, -1))
      : lower == p;
    if (matches) {
      return "on this server's infrastructure deny list";
    }
  }
  return undefined;
}

/**
 * The default infrastructure deny list: what a proxy in front of the server
 * sets, and what a client must not be able to forge.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export const defaultInfrastructureHeaders: readonly string[] = [
  "Forwarded",
  "X-Forwarded-*",
  "X-Real-IP",
];

/**
 * Report whether a handshake header must be kept out of the effective
 * headers. The version travels in the subprotocol and the deadline in the
 * query string, so either arriving as a header is not ours to honor.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function isStrippedHandshakeHeader(key: string): boolean {
  const lower = key.toLowerCase();
  return lower == "connect-protocol-version" || lower == "connect-timeout-ms";
}

// The three method-override names are conditional in Fetch; here the method
// they would override has no meaning, so they are forbidden outright.
const fetchForbidden = new Set([
  "accept-charset",
  "accept-encoding",
  "access-control-request-headers",
  "access-control-request-method",
  "connection",
  "content-length",
  "cookie",
  "cookie2",
  "date",
  "dnt",
  "expect",
  "host",
  "keep-alive",
  "origin",
  "referer",
  "set-cookie",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "via",
  "x-http-method",
  "x-http-method-override",
  "x-method-override",
]);

const protocolControlled = new Set([
  "connect-protocol-version",
  "connect-timeout-ms",
  "content-type",
  "content-encoding",
  "connect-content-encoding",
  "connect-accept-encoding",
]);

function isToken(key: string): boolean {
  return /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(key);
}

function isFieldValue(value: string): boolean {
  return !/[\r\n\0]/.test(value);
}

// Standard alphabet, padded or not.
function isBase64(value: string): boolean {
  const match = /^([A-Za-z0-9+/]*)(=*)$/.exec(value);
  if (match === null) {
    return false;
  }
  const [, body, padding] = match;
  if (body.length % 4 == 1 || padding.length > 2) {
    return false;
  }
  return padding.length == 0 || (body.length + padding.length) % 4 == 0;
}

/**
 * List the keys of a top-level JSON object in source order, duplicates
 * included. The input must already be known to be a valid JSON object.
 */
function objectKeys(json: string): string[] {
  const keys: string[] = [];
  let i = skipWhitespace(json, 0);
  if (json[i] != "{") {
    return keys;
  }
  i = skipWhitespace(json, i + 1);
  while (i < json.length && json[i] == '"') {
    const end = skipString(json, i);
    keys.push(JSON.parse(json.slice(i, end)) as string);
    i = skipWhitespace(json, end);
    i = skipWhitespace(json, i + 1); // the colon
    i = skipWhitespace(json, skipValue(json, i));
    if (json[i] != ",") {
      break;
    }
    i = skipWhitespace(json, i + 1);
  }
  return keys;
}

function skipWhitespace(json: string, i: number): number {
  while (i < json.length && /\s/.test(json[i])) {
    i++;
  }
  return i;
}

// Returns the index just past the closing quote.
function skipString(json: string, i: number): number {
  for (i = i + 1; i < json.length; i++) {
    if (json[i] == "\\") {
      i++;
    } else if (json[i] == '"') {
      return i + 1;
    }
  }
  return i;
}

function skipValue(json: string, i: number): number {
  let depth = 0;
  while (i < json.length) {
    const c = json[i];
    if (c == '"') {
      i = skipString(json, i);
      if (depth == 0) {
        return i;
      }
      continue;
    }
    if (c == "{" || c == "[") {
      depth++;
    } else if (c == "}" || c == "]") {
      if (depth == 0) {
        return i;
      }
      depth--;
      if (depth == 0) {
        return i + 1;
      }
    } else if (c == "," && depth == 0) {
      return i;
    }
    i++;
  }
  return i;
}
