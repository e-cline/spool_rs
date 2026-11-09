# Browser example tests

End-to-end tests for the pages in [`../examples`](../examples) (`browser`,
`browser_debounce`, `browser_fetch`, `browser_mock_clock`, `browser_yield`).
Each test builds the real page, loads it in headless Chrome, and checks what
the user would see: the DOM, the page's timing, and that nothing logged an
error.

This is separate from the library's browser tests (`../tests/browser.rs`, run
by `wasm-pack test`), which exercise the driver directly, in Chrome and
Firefox. These check that the example *pages* work: the `index.html`, the JS
helpers, the `wasm-bindgen` glue, and the Spooly behavior each page
demonstrates.

## What you need

- **Node 20 or later.** The only package is `playwright-core`; the test runner
  is Node's built-in `node:test`.
- **Google Chrome** (or Chromium). Playwright uses the one installed on the
  machine, so no browser is downloaded. To use a different executable, set
  `E2E_CHROME_PATH`.
- **The `wasm32-unknown-unknown` Rust target**:
  `rustup target add wasm32-unknown-unknown`.
- **The `wasm-bindgen` CLI at the same version as the crate in `Cargo.lock`.**
  `build.mjs` checks this and tells you the command to run if it doesn't match:
  `cargo install wasm-bindgen-cli --version <version> --locked`.

## Running

```sh
cd crates/spooly/e2e
npm ci
npm run check    # build the pages, then run the tests
```

Or in two steps, which is handy while writing tests, since only the build
needs repeating when an example changes:

```sh
npm run build    # cargo build + wasm-bindgen, into ./site (ignored by git)
npm test
npm test -- browser_fetch.test.mjs   # one file
```

## How it works

- `build.mjs` builds the examples in release mode for
  `wasm32-unknown-unknown` with the `browser` feature, runs `wasm-bindgen
  --target web`, and copies each example's `index.html` and data files next to
  the generated module, into `site/<name>/`. Nothing is written into the
  examples' own folders.
- `harness.mjs` serves `site/` on a random port and starts Chrome once per test
  file. `open(name)` loads a page and collects its uncaught errors and console
  errors; each test ends with `noProblems(page)`, so a Rust panic or a broken
  JS helper fails it, whatever else it checks.
- Test files run one at a time (`--test-concurrency=1`), so two browsers don't
  compete for the CPU. That matters to `browser_yield`, which measures frame
  gaps.

## Writing tests

- The examples use real timers, so wait for a condition
  (`page.waitForFunction`) instead of sleeping. Where a test has to wait a fixed
  time, as when checking that a cancelled request *never* reports back, say
  why in a comment.
- Compare timings with each other, not with fixed numbers, and keep the margin
  wide. See `browser_yield.test.mjs`.
- Check that a new test can fail: break the example in a way the test should
  notice, rebuild, and watch it go red. This caught a test that passed no matter
  what (the debounce test's first burst was shorter than the debounce window).

## In CI

The `browser-examples` job in `.github/workflows/ci.yml` does the same on
`ubuntu-latest`: it installs the matching `wasm-bindgen` with
`taiki-e/install-action`, then runs `npm ci`, `npm run build` and `npm test`
against the runner's preinstalled Chrome.
