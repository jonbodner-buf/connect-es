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

import type {
  BinaryReadOptions,
  BinaryWriteOptions,
  JsonReadOptions,
  JsonWriteOptions,
} from "@bufbuild/protobuf";
import type { Interceptor, Transport } from "@connectrpc/connect";
import { validateReadWriteMaxBytes } from "@connectrpc/connect/protocol";
import { createTransport } from "@connectrpc/connect/protocol-websocket";
import type { WebSocketFactory } from "@connectrpc/connect/protocol-websocket";

/**
 * Options used to configure the Connect-over-WebSocket transport.
 *
 * See createWebSocketTransport().
 */
export interface WebSocketTransportOptions {
  /**
   * Base URI for all RPCs. An http or https scheme is rewritten to ws or
   * wss.
   *
   * RPCs connect to <baseUrl>/<pathPrefix>/<package>.<service>/method
   */
  baseUrl: string;

  /**
   * Path prefix for WebSocket RPCs. It must match the server's.
   */
  pathPrefix?: string;

  /**
   * Selects the connectrpc.1+proto subprotocol instead of connectrpc.1+json.
   * Uses JSON by default.
   */
  useBinaryFormat?: boolean;

  /**
   * Interceptors that should be applied to all calls running through
   * this transport. See the Interceptor type for details.
   */
  interceptors?: Interceptor[];

  /**
   * Options for the JSON format.
   * By default, unknown fields are ignored.
   */
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;

  /**
   * Options for the binary wire format.
   */
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;

  /**
   * Limits the size of a message received from the server. The default
   * limit is the maximum supported value of ~4GiB.
   */
  readMaxBytes?: number;

  /**
   * Prevents sending messages too large for the server to handle. The
   * default limit is the maximum supported value of ~4GiB.
   */
  writeMaxBytes?: number;

  /**
   * The timeout in milliseconds to apply to all requests.
   *
   * This can be overridden on a per-request basis by passing a timeoutMs.
   */
  defaultTimeoutMs?: number;

  /**
   * Creates the WebSocket for each RPC. Defaults to the global WebSocket.
   */
  webSocket?: WebSocketFactory;
}

/**
 * Create a Transport for the Connect-over-WebSocket protocol, which makes
 * every streaming type, including bidi streaming, available to web browsers.
 *
 * Every RPC opens its own WebSocket connection.
 */
export function createWebSocketTransport(
  options: WebSocketTransportOptions,
): Transport {
  const { readMaxBytes, writeMaxBytes } = validateReadWriteMaxBytes(
    options.readMaxBytes,
    options.writeMaxBytes,
    undefined,
  );
  return createTransport({
    ...options,
    useBinaryFormat: options.useBinaryFormat ?? false,
    readMaxBytes,
    writeMaxBytes,
    webSocket:
      options.webSocket ??
      ((url: string, protocol: string) => new WebSocket(url, protocol)),
    // Firefox reports negotiated extensions without their parameters.
    allowUnparameterizedDeflate: true,
  });
}
