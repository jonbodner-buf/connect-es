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

export { createTransport } from "./transport.js";
export type { WebSocketTransportOptions } from "./transport.js";
export type { WebSocketLike, WebSocketFactory } from "./client-socket.js";
export { createWebSocketHandler } from "./handler.js";
export type {
  UniversalServerWebSocket,
  UniversalWebSocketHandler,
  UniversalWebSocketHandlerFn,
  UniversalWebSocketRequest,
  WebSocketReadResult,
} from "./universal-websocket.js";
export type {
  WebSocketFrame,
  WireMessage,
  ProtocolFault,
} from "./framing.js";
export {
  protocolName,
  subprotocolProto,
  subprotocolJson,
  paramTimeout,
  markerBody,
  markerMetadata,
  markerServerEndStream,
  markerClientEndStream,
  closeNormal,
  closeGoingAway,
  closeProtocolError,
  closeUnsupportedData,
  closeMessageTooBig,
  closeInternalError,
  closeClientProtocolError,
  closeClientUnsupportedData,
  closeClientMessageTooBig,
  closeClientCompression,
  closeClientInternalError,
  normalizeCloseCode,
  closeReason,
  decodeFrame,
  encodeFrame,
} from "./framing.js";
export {
  encodeMetadata,
  decodeMetadata,
  reservedHeaderReason,
  defaultInfrastructureHeaders,
} from "./metadata.js";
export { createWebSocketUrl, normalizePathPrefix } from "./url.js";
