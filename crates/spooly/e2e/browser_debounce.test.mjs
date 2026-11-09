// examples/browser_debounce: keystrokes (JS events) wake a task, `Sleep::reset`
// debounces them, and a stale search is aborted by dropping its task.
import assert from "node:assert/strict";
import { test } from "node:test";

import { noProblems, sleep, useBrowser } from "./harness.mjs";

const browser = useBrowser();

const results = (page, text) =>
  page.waitForFunction(
    (want) => document.getElementById("results").textContent === want,
    text,
    { timeout: 5_000 },
  );

test("a burst of keystrokes makes one search", async () => {
  const page = await browser.open("browser_debounce");
  // Six keystrokes 100ms apart: each is inside the 250ms debounce, but the
  // burst as a whole lasts 500ms. Only a timer that restarts on every
  // keystroke waits for the end of it; one that doesn't would search after
  // 250ms, then again.
  await page.locator("#box").pressSequentially("pineap", { delay: 100 });
  await results(page, "pineapple");
  // Let a wrongly early search, if any, show up in the counts.
  await sleep(700);
  assert.match(
    await page.textContent("#counts"),
    /^6 keystrokes, 1 searches started, 1 finished, 0 cancelled$/,
  );
  noProblems(page);
  await page.close();
});

test("typing during a search cancels it", async () => {
  const page = await browser.open("browser_debounce");
  await page.fill("#box", "berry");
  // The search starts 250ms after the keystroke and takes 400ms.
  await page.waitForFunction(() => /1 searches started/.test(document.getElementById("counts").textContent));
  await sleep(100); // Mid-search.
  await page.fill("#box", "blue");
  await results(page, "blueberry");
  const counts = await page.textContent("#counts");
  assert.match(counts, /2 searches started, 1 finished, 1 cancelled/, counts);
  noProblems(page);
  await page.close();
});

test("a query with no matches shows none", async () => {
  const page = await browser.open("browser_debounce");
  await page.fill("#box", "zzz");
  await page.waitForFunction(() => /^0 result\(s\)/.test(document.getElementById("status").textContent), null, {
    timeout: 5_000,
  });
  assert.equal(await page.textContent("#results"), "");
  noProblems(page);
  await page.close();
});
