// examples/browser_mock_clock: a test runner on a MockClock runtime. Its own
// tests wait for up to a day of virtual time, so this checks that the browser
// driver runs a mock-clock runtime to completion without real waiting.
import assert from "node:assert/strict";
import { test } from "node:test";

import { noProblems, texts, useBrowser } from "./harness.mjs";

const browser = useBrowser();

const finished = (page) =>
  page.waitForFunction(() => /passed\./.test(document.getElementById("summary").textContent), null, {
    timeout: 10_000,
  });

test("every virtual-time test passes", async () => {
  const page = await browser.open("browser_mock_clock");
  await finished(page);
  assert.match(
    await page.textContent("#summary"),
    /^6\/6 passed\. 48\.0 hours of virtual time in \d+ms\.$/,
  );
  assert.equal(await page.locator("tr.FAIL").count(), 0, (await texts(page, "tr.FAIL")).join("\n"));
  noProblems(page);
  await page.close();
});

test("48 hours of virtual time take milliseconds, not hours", async () => {
  const page = await browser.open("browser_mock_clock");
  await finished(page);
  const [, ms] = (await page.textContent("#summary")).match(/in (\d+)ms/);
  assert.ok(Number(ms) < 5_000, `took ${ms}ms`); // 48 virtual hours; typically ~100ms.
  await page.close();
});

test("running again works", async () => {
  const page = await browser.open("browser_mock_clock");
  await finished(page);
  // Mark the first run's rows, so a second run is recognizable by their
  // replacement (the "running..." text only shows for a moment).
  await page.$$eval("#rows tr", (rows) => rows.forEach((row) => (row.dataset.firstRun = "1")));
  await page.click("#again");
  await page.waitForFunction(
    () =>
      /passed\./.test(document.getElementById("summary").textContent) &&
      document.querySelectorAll("#rows tr").length === 6 &&
      document.querySelectorAll("#rows tr[data-first-run]").length === 0,
    null,
    { timeout: 10_000 },
  );
  assert.match(await page.textContent("#summary"), /^6\/6 passed\./);
  noProblems(page);
  await page.close();
});
