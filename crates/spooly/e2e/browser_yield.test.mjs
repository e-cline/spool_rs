// examples/browser_yield: `yield_now` is a microtask yield, so it does not let
// the browser draw; a `sleep(1ms)` does.
//
// This is the one timing-sensitive test, so it compares the two gaps with each
// other rather than against fixed numbers, and keeps the margin wide.
import assert from "node:assert/strict";
import { test } from "node:test";

import { noProblems, useBrowser } from "./harness.mjs";

const browser = useBrowser();

// The primes below 3,000,000 (the example's LIMIT).
const PRIMES = 216_816;

async function run(page, mode) {
  await page.click(`#btn-${mode}`);
  await page.waitForFunction(
    (id) => /primes in/.test(document.getElementById(id).textContent),
    mode,
    { timeout: 60_000 },
  );
  const text = await page.textContent(`#${mode}`);
  const [, primes, gap] = text.match(/^(\d+) primes in \d+ms; longest gap between frames: (\d+)ms$/);
  return { primes: Number(primes), gap: Number(gap) };
}

test("every mode finds the same primes", async () => {
  const page = await browser.open("browser_yield");
  for (const mode of ["blocking", "yield", "sleep"]) {
    assert.equal((await run(page, mode)).primes, PRIMES, mode);
  }
  noProblems(page);
  await page.close();
});

test("yield_now leaves the page frozen; sleep(1ms) does not", async () => {
  const page = await browser.open("browser_yield");
  const yielded = await run(page, "yield");
  const slept = await run(page, "sleep");
  assert.ok(
    slept.gap * 4 < yielded.gap,
    `sleep(1ms) gap ${slept.gap}ms should be well under yield_now gap ${yielded.gap}ms`,
  );
  noProblems(page);
  await page.close();
});

test("the buttons are disabled while a job runs", async () => {
  const page = await browser.open("browser_yield");
  await page.click("#btn-sleep");
  await page.waitForFunction(() => document.getElementById("btn-yield").disabled);
  await page.waitForFunction(() => !document.getElementById("btn-yield").disabled, null, {
    timeout: 60_000,
  });
  noProblems(page);
  await page.close();
});
