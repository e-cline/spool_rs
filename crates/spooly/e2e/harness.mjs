// Shared test setup: a static file server for ./site, and one Chrome per test
// file.
//
// Chrome comes from the machine (Playwright's `channel: "chrome"`), so no
// browser is downloaded. Set E2E_CHROME_PATH to use a specific executable.

import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { after, before } from "node:test";
import { chromium } from "playwright-core";

import { siteDir } from "./build.mjs";

const contentTypes = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json",
};

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function serve() {
  return http.createServer((req, res) => {
    const url = new URL(req.url, "http://localhost");
    if (url.pathname === "/favicon.ico") {
      res.writeHead(204).end(); // Otherwise the browser logs a 404.
      return;
    }
    const file = path.join(siteDir, url.pathname.replace(/\/$/, "/index.html"));
    if (!file.startsWith(siteDir) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404).end();
      return;
    }
    res.writeHead(200, {
      "content-type": contentTypes[path.extname(file)] ?? "application/octet-stream",
    });
    fs.createReadStream(file).pipe(res);
  });
}

/**
 * Starts a server and a browser around the tests in the calling file.
 *
 * `open(name)` loads `site/<name>/` in a new page. The page collects its
 * uncaught errors and console errors in `page.problems`; end each test with
 * `noProblems(page)` so that a panic or a broken JS helper fails it.
 */
export function useBrowser() {
  let server;
  let browser;
  let base;

  before(async () => {
    if (!fs.existsSync(siteDir)) {
      throw new Error("crates/spooly/e2e/site is missing: run `npm run build` first");
    }
    server = serve().listen(0);
    base = `http://localhost:${server.address().port}`;
    browser = await chromium.launch(
      process.env.E2E_CHROME_PATH
        ? { executablePath: process.env.E2E_CHROME_PATH }
        : { channel: "chrome" },
    );
  });

  after(async () => {
    await browser?.close();
    server?.close();
  });

  return {
    async open(name) {
      const page = await browser.newPage();
      page.problems = [];
      page.on("pageerror", (error) => page.problems.push(`uncaught: ${error}`));
      page.on("console", (message) => {
        if (message.type() === "error") page.problems.push(`console: ${message.text()}`);
      });
      await page.goto(`${base}/${name}/`);
      return page;
    },
  };
}

/** Asserts that the page logged no errors. `allow` lists patterns to ignore. */
export function noProblems(page, allow = []) {
  const unexpected = page.problems.filter((p) => !allow.some((re) => re.test(p)));
  assert.deepEqual(unexpected, [], "the page logged errors");
}

/** The text of every element matching `selector`. */
export const texts = (page, selector) =>
  page.$$eval(selector, (elements) => elements.map((e) => e.textContent));
