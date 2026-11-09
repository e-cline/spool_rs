// examples/browser: the event loop's order, and a task woken by a click.
import assert from "node:assert/strict";
import { test } from "node:test";

import { noProblems, texts, useBrowser } from "./harness.mjs";

const browser = useBrowser();

test("sync code, then a microtask, then a timer", async () => {
  const page = await browser.open("browser");
  await page.waitForFunction(() => document.getElementById("order").children.length === 3);
  assert.deepEqual(await texts(page, "#order li"), [
    "1. synchronous code",
    "2. a microtask",
    "3. a timer",
  ]);
  noProblems(page);
  await page.close();
});

test("the clock ticks on an interval", async () => {
  const page = await browser.open("browser");
  await page.waitForFunction(() => document.getElementById("clock").textContent === "1s", null, {
    timeout: 5_000,
  });
  noProblems(page);
  await page.close();
});

test("a click, from outside the event loop, wakes a task", async () => {
  const page = await browser.open("browser");
  await page.waitForFunction(() => document.getElementById("order").children.length === 3);
  await page.click("#button");
  await page.click("#button");
  await page.waitForFunction(() => document.getElementById("clicks").textContent.startsWith("2"));
  noProblems(page);
  await page.close();
});
