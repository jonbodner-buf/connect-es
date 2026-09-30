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

import * as esbuild from "esbuild";
import { remote } from "webdriverio";
import type { BrowserTestOutcome, CaseResult } from "./page-runner.js";

/**
 * Bundle a script for the browser, given its path relative to this
 * directory.
 */
export async function buildBrowserScript(entry: string): Promise<string> {
  const result = await esbuild.build({
    entryPoints: [new URL(entry, import.meta.url).pathname],
    bundle: true,
    write: false,
  });
  if (result.outputFiles.length !== 1) {
    throw new Error("expected exactly one output file");
  }
  return result.outputFiles[0].text;
}

/**
 * Render a page that runs the given script.
 */
export function testPage(script: string): string {
  return `<!doctype html><html lang="en"><head><meta charset="UTF-8" />
    <title>connect-web browser test</title><link rel="icon" href="data:,">
    <script>${script}</script></head><body></body></html>`;
}

/**
 * A browser the tests can drive through WebDriver.
 */
export type BrowserName = "chrome" | "firefox" | "safari";

/**
 * The browser instance handed to withBrowser().
 */
export type Browser = Awaited<ReturnType<typeof remote>>;

/**
 * Return the browsers to test, from the comma-separated CONNECT_BROWSERS
 * environment variable. The default is Chrome.
 */
export function browserNames(): BrowserName[] {
  const names = (process.env.CONNECT_BROWSERS ?? "chrome")
    .split(",")
    .map((name) => name.trim())
    .filter((name) => name != "");
  const browsers: BrowserName[] = [];
  for (const name of names) {
    if (name != "chrome" && name != "firefox" && name != "safari") {
      throw new Error(`unsupported browser in CONNECT_BROWSERS: ${name}`);
    }
    browsers.push(name);
  }
  return browsers;
}

/**
 * Open a page in the given browser, run a function against it, and close the
 * browser. Chrome and Firefox run headless; Safari has no headless mode.
 */
export async function withBrowser<T>(
  browserName: BrowserName,
  pageUrl: string,
  run: (browser: Browser) => Promise<T>,
): Promise<T> {
  const browser = await remote({
    capabilities: {
      browserName,
      "goog:chromeOptions": { args: ["--headless", "--disable-gpu"] },
      "moz:firefoxOptions": { args: ["-headless"] },
    },
    logLevel: "error",
  });
  try {
    await browser.url(pageUrl);
    return await run(browser);
  } finally {
    await browser.deleteSession();
  }
}

/**
 * Start a test function registered in the page with registerBrowserTest(),
 * and poll until it settles. Rejects with the error it threw in the page.
 */
export async function runBrowserTest(
  browser: Browser,
  name: string,
  args: string[],
  timeoutMs = 60000,
): Promise<unknown> {
  await browser.execute(
    (testName: string, testArgs: string[]) => {
      window.startBrowserTest(testName, testArgs);
    },
    name,
    args,
  );
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const outcome: BrowserTestOutcome = await browser.execute(() =>
      window.browserTestOutcome(),
    );
    if (outcome !== null) {
      if ("error" in outcome) {
        throw new Error(`browser test ${name} failed: ${outcome.error}`);
      }
      return outcome.result;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`browser test ${name} did not finish in ${timeoutMs}ms`);
}

/**
 * Check that a value returned from the page is one CaseResult.
 */
export function parseCaseResult(value: unknown): CaseResult {
  if (
    typeof value == "object" &&
    value !== null &&
    "ok" in value &&
    typeof value.ok == "boolean" &&
    "detail" in value &&
    typeof value.detail == "string"
  ) {
    return { ok: value.ok, detail: value.detail };
  }
  throw new Error(`not a test case result: ${JSON.stringify(value)}`);
}

/**
 * Check that a value returned from the page maps case names to results.
 */
export function parseCaseResults(value: unknown): Record<string, CaseResult> {
  if (typeof value != "object" || value === null) {
    throw new Error(`not test case results: ${JSON.stringify(value)}`);
  }
  const results: Record<string, CaseResult> = {};
  for (const [name, result] of Object.entries(value)) {
    results[name] = parseCaseResult(result);
  }
  return results;
}
