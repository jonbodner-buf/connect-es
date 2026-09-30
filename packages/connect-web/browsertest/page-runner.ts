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

// Lets the test driver start a long-running function in the page and poll
// for its outcome. Safari's WebDriver fails a script call that stays pending
// while these tests run, so no call waits on a test.

/**
 * The outcome of one browser test case.
 */
export interface CaseResult {
  ok: boolean;
  detail: string;
}

/**
 * The outcome of a function started with startBrowserTest(): its result or
 * error, or null while it is still running. Not undefined, which WebDriver
 * returns as null anyway.
 */
export type BrowserTestOutcome =
  | { done: true; result: unknown }
  | { done: true; error: string }
  | null;

declare global {
  interface Window {
    startBrowserTest: (name: string, args: string[]) => void;
    browserTestOutcome: () => BrowserTestOutcome;
  }
}

const tests = new Map<string, (...args: string[]) => Promise<unknown>>();
let outcome: BrowserTestOutcome = null;

/**
 * Make a function available to startBrowserTest() under a name.
 */
export function registerBrowserTest(
  name: string,
  run: (...args: string[]) => Promise<unknown>,
): void {
  tests.set(name, run);
}

window.startBrowserTest = function startBrowserTest(name, args) {
  outcome = null;
  const run = tests.get(name);
  if (run === undefined) {
    outcome = { done: true, error: `no browser test named ${name}` };
    return;
  }
  run(...args).then(
    (result) => {
      outcome = { done: true, result };
    },
    (e: unknown) => {
      outcome = { done: true, error: String(e) };
    },
  );
};

window.browserTestOutcome = function browserTestOutcome() {
  return outcome;
};
