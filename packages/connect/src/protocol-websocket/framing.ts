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

import { Code } from "../code.js";
import { ConnectError } from "../connect-error.js";

/**
 * The protocol name reported on HandlerContext.protocolName.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export const protocolName = "connect+ws";

/**
 * The WebSocket subprotocol selecting the Protobuf binary codec.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export const subprotocolProto = "connectrpc.1+proto";

/**
 * The WebSocket subprotocol selecting the Protobuf JSON codec.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export const subprotocolJson = "connectrpc.1+json";

/**
 * The query parameter carrying the client's deadline. A browser cannot set
 * headers on a handshake, so the URI is the only channel.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export const paramTimeout = "connect-timeout-ms";

// Markers are printable on purpose: a text frame then reads as its marker
// followed by its JSON in a browser's network panel.
export const markerBody = 0x42; // "B"
export const markerMetadata = 0x4d; // "M"
export const markerServerEndStream = 0x53; // "S"
export const markerClientEndStream = 0x43; // "C"
const markerHighBit = 0x80;

export const closeNormal = 1000;
export const closeGoingAway = 1001;
export const closeProtocolError = 1002;
export const closeUnsupportedData = 1003;
export const closeAbnormal = 1006;
export const closeMessageTooBig = 1009;
export const closeInternalError = 1011;
// A browser script may only pass 1000 or 3000-4999 to close(), so a client's
// codes live in the 31xx range and fold onto the 10xx code with the same
// last two digits.
export const closeClientProtocolError = 3102;
export const closeClientUnsupportedData = 3103;
export const closeClientMessageTooBig = 3109;
export const closeClientCompression = 3110;
export const closeClientInternalError = 3111;

/**
 * One WebSocket message. Text frames carry JSON, binary frames carry
 * Protobuf binary.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export interface WebSocketFrame {
  text: boolean;
  data: Uint8Array;
}

/**
 * A WebSocket message split into its marker and payload.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export interface WireMessage {
  marker: number;
  text: boolean;
  payload: Uint8Array;
}

/**
 * Classifies a peer's framing mistake. It selects the close code, and tells
 * a monitor what kind of mistake it was.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export type ProtocolFault =
  | "marker"
  | "frame_type"
  | "size_limit"
  | "metadata"
  | "message_encoding";

const faults = new WeakMap<ConnectError, ProtocolFault>();

/**
 * Create an error for a peer that broke the framing, and remember what kind
 * of fault it was.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function peerFault(
  fault: ProtocolFault,
  message: string,
  code: Code = Code.InvalidArgument,
  cause?: unknown,
): ConnectError {
  const error = new ConnectError(
    `protocol error: ${message}`,
    code,
    undefined,
    undefined,
    cause,
  );
  faults.set(error, fault);
  return error;
}

/**
 * Return the fault recorded for an error created by peerFault().
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function faultOf(error: unknown): ProtocolFault | undefined {
  if (error instanceof ConnectError) {
    return faults.get(error);
  }
  return undefined;
}

/**
 * Return the close code for a fault. A server sends 10xx codes, a client
 * sends the 31xx counterpart.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function closeCodeForFault(
  fault: ProtocolFault,
  role: "server" | "client",
): number {
  let code: number;
  switch (fault) {
    case "frame_type":
      code = closeUnsupportedData;
      break;
    case "size_limit":
      code = closeMessageTooBig;
      break;
    default:
      code = closeProtocolError;
      break;
  }
  return role == "server" ? code : code + 2100;
}

/**
 * Fold a 31xx close code onto the 10xx code with the same last two digits.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function normalizeCloseCode(code: number): number {
  if (code >= 3100 && code <= 3199) {
    return code - 2100;
  }
  return code;
}

/**
 * Truncate a close reason to the 123 bytes a close frame allows, without
 * splitting a UTF-8 sequence. An over-long reason makes the browser throw
 * rather than truncate.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function closeReason(reason: string): string {
  const maxBytes = 123;
  const encoder = new TextEncoder();
  if (encoder.encode(reason).byteLength <= maxBytes) {
    return reason;
  }
  let truncated = reason;
  while (encoder.encode(truncated).byteLength > maxBytes) {
    truncated = truncated.slice(0, -1);
  }
  // Slicing by UTF-16 unit can leave a lone high surrogate behind.
  const last = truncated.charCodeAt(truncated.length - 1);
  if (last >= 0xd800 && last <= 0xdbff) {
    truncated = truncated.slice(0, -1);
  }
  return truncated;
}

/**
 * Render a marker for a diagnostic, printably where possible.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function markerName(marker: number): string {
  if (marker >= 0x20 && marker < 0x7f) {
    return String.fromCharCode(marker);
  }
  return `0x${marker.toString(16).padStart(2, "0")}`;
}

/**
 * Split a frame into the message it carries. Raises a peer fault for an
 * empty frame, or a marker with the reserved high bit set.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function decodeFrame(frame: WebSocketFrame): WireMessage {
  if (frame.data.byteLength == 0) {
    throw peerFault("marker", "empty message carries no marker");
  }
  const marker = frame.data[0];
  if (marker >= markerHighBit) {
    throw peerFault(
      "marker",
      `marker ${markerName(marker)} sets the reserved high bit`,
    );
  }
  return {
    marker,
    text: frame.text,
    payload: frame.data.subarray(1),
  };
}

/**
 * Assemble marker and payload into one frame.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function encodeFrame(
  marker: number,
  text: boolean,
  payload?: Uint8Array,
): WebSocketFrame {
  const data = new Uint8Array(1 + (payload?.byteLength ?? 0));
  data[0] = marker;
  if (payload !== undefined) {
    data.set(payload, 1);
  }
  return { text, data };
}

/**
 * Encode a JSON control message (M or S), which is always a text frame.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function encodeJsonFrame(
  marker: number,
  value: unknown,
): WebSocketFrame {
  return encodeFrame(
    marker,
    true,
    new TextEncoder().encode(JSON.stringify(value)),
  );
}

/**
 * Parse the JSON payload of a control message.
 *
 * @private Internal code, does not follow semantic versioning.
 */
export function decodeJsonPayload(payload: Uint8Array): string {
  return new TextDecoder().decode(payload);
}
