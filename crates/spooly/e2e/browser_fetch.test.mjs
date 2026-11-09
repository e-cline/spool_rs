// examples/browser_fetch: a JS fetch as a Spooly future. Dropping the future
// (a `timeout`, a cancelled task) must abort the request.
import assert from "node:assert/strict";
import { test } from "node:test";

import { noProblems, sleep, texts, useBrowser } from "./harness.mjs";

const browser = useBrowser();

const log = (page) => texts(page, "#log li");

const demoDone = (page) =>
  page.waitForFunction(() => /done\./.test(document.getElementById("log").textContent), null, {
    timeout: 10_000,
  });

test("a request succeeds, times out (and is aborted), and fails with a 404", async () => {
  const page = await browser.open("browser_fetch");
  await demoDone(page);
  const lines = await log(page);
  const has = (re) => lines.some((line) => re.test(line));
  assert.ok(has(/ok: .*hello from data\.json/), lines.join("\n"));
  assert.ok(has(/deadline has elapsed/), lines.join("\n"));
  assert.ok(has(/request dropped: aborted/), lines.join("\n"));
  assert.ok(has(/failed: Error: HTTP 404/), lines.join("\n"));
  // The timed-out request is aborted before its simulated latency ends: no
  // second "ok" for it, only the first request's.
  assert.equal(lines.filter((line) => /ok:/.test(line)).length, 1, lines.join("\n"));
  // Chrome logs the demo's deliberate 404 as a console error.
  noProblems(page, [/404/]);
  await page.close();
});

test("a cancelled request never reports a result", async () => {
  const page = await browser.open("browser_fetch");
  await demoDone(page);
  const before = (await log(page)).length;
  await page.click("#start");
  await sleep(300);
  await page.click("#cancel");
  // Wait out the 3s the request would have taken; a result would show here.
  await sleep(3_300);
  const after = (await log(page)).slice(before);
  assert.equal(after.length, 3, after.join("\n")); // The GET, "cancel", "aborted".
  assert.match(after[1], /cancel/);
  assert.match(after[2], /request dropped: aborted/);
  noProblems(page, [/404/]);
  await page.close();
});

test("a request that is not cancelled finishes", async () => {
  const page = await browser.open("browser_fetch");
  await demoDone(page);
  const before = (await log(page)).length;
  await page.click("#start");
  await page.waitForFunction(
    (n) => document.querySelectorAll("#log li").length === n + 2,
    before,
    { timeout: 6_000 },
  );
  const after = (await log(page)).slice(before);
  assert.match(after[1], /ok: .*hello from data\.json/);
  noProblems(page, [/404/]);
  await page.close();
});

test("starting a second request aborts the first", async () => {
  const page = await browser.open("browser_fetch");
  await demoDone(page);
  const before = (await log(page)).length;
  await page.click("#start");
  await sleep(200);
  await page.click("#start");
  await page.waitForFunction(
    (n) => /ok:/.test([...document.querySelectorAll("#log li")].slice(n).map((e) => e.textContent).join("\n")),
    before,
    { timeout: 6_000 },
  );
  const after = (await log(page)).slice(before);
  assert.equal(after.filter((line) => /GET/.test(line)).length, 2, after.join("\n"));
  assert.equal(after.filter((line) => /request dropped: aborted/.test(line)).length, 1, after.join("\n"));
  assert.equal(after.filter((line) => /ok:/.test(line)).length, 1, after.join("\n"));
  noProblems(page, [/404/]);
  await page.close();
});
