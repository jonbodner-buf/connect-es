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

import { createClient } from "@connectrpc/connect";
import { createWebSocketTransport } from "@connectrpc/connect-web";
import { ElizaService } from "./gen/eliza_pb.js";

/**
 * MessageQueue is an async iterable that a caller feeds one message at a
 * time, for use as the request stream of a bidi RPC.
 */
class MessageQueue<T> implements AsyncIterable<T> {
  private readonly buffered: T[] = [];
  private waiting: ((value: T) => void) | undefined;

  push(value: T): void {
    const waiting = this.waiting;
    if (waiting !== undefined) {
      this.waiting = undefined;
      waiting(value);
      return;
    }
    this.buffered.push(value);
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    for (;;) {
      const next = this.buffered.shift();
      if (next !== undefined) {
        yield next;
        continue;
      }
      yield await new Promise<T>((resolve) => {
        this.waiting = resolve;
      });
    }
  }
}

// A relative base URL resolves against the page, with http rewritten to ws.
const transport = createWebSocketTransport({ baseUrl: "/" });

void (async () => {
  const client = createClient(ElizaService, transport);

  print("What is your name?");
  const name = await prompt();
  print(`> ${name}`);

  for await (const res of client.introduce({ name })) {
    print(res.sentence);
  }

  // The whole conversation is one bidi stream on one WebSocket, which the
  // fetch API cannot do in a browser.
  const sentences = new MessageQueue<{ sentence: string }>();
  void (async () => {
    for (;;) {
      const sentence = await prompt();
      print(`> ${sentence}`);
      sentences.push({ sentence });
    }
  })();
  try {
    for await (const res of client.converse(sentences)) {
      print(res.sentence);
    }
  } catch (e) {
    print(`The conversation ended: ${String(e)}`);
  }
})();

function print(text: string): void {
  const p = document.createElement("p");
  p.innerText = text;
  p.scrollIntoView();
  document.querySelector<HTMLElement>("#root")?.append(p);
}

function prompt(): Promise<string> {
  const input = document.createElement("input");
  input.value = "";
  document.querySelector<HTMLElement>("#root")?.append(input);
  input.focus();
  return new Promise<string>((resolve) => {
    input.onkeyup = (ev) => {
      if (ev.key == "Enter" && input.value.length > 0) {
        input.remove();
        input.onkeyup = null;
        resolve(input.value);
      }
    };
  });
}
