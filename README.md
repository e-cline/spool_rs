# spooly

**Spooly** is a lightweight, single-threaded Rust async executor, with a
scheduling model inspired by the JavaScript event loop. It runs on native
platforms and in the browser; WASI support is in progress.

WARNING: This is an experiment. Nothing here is not intended for production use.

- No `Send` or `Sync` required: use `Rc`, `RefCell`, and `Cell` freely in tasks.
- Simple, predictable ordering: a woken task is queued as a microtask, in
  FIFO order, like the code after an `await` in JS. Timers run one at a
  time, and the microtask queue drains after each.
- Panics behave the same everywhere: Spooly never catches them, so a panic
  stops the loop. Errors a caller should handle are `Result` values.
- Zero runtime dependencies on native targets (the optional `browser`
  feature adds `wasm-bindgen`), and no `unsafe`.

```rust
use std::time::Duration;

spooly::run(async {
    spooly::set_timeout(Duration::from_millis(40), || println!("timeout 40ms"));
    spooly::queue_microtask(|| println!("microtask"));
    println!("sync");

    let task = spooly::spawn(async {
        spooly::sleep(Duration::from_millis(10)).await;
        42
    });
    println!("joined: {}", task.await);
});
// Prints: sync, microtask, joined: 42, timeout 40ms
```

Spooly borrows JS names where they're the clearest ones (`set_timeout`,
`queue_microtask`), but it is not a reimplementation of Node or the browser:
the design ([`docs/INITIAL_PLAN.md`](docs/INITIAL_PLAN.md)) defines its
semantics.

> **Status:** early development. Done: M0–M6 (the core loop, microtasks,
> timers, cancellation, liveness and threads, and polish), M7 (realigning
> with design v4: no `next_tick`, intervals that don't drift, and a
> reference model as the test oracle), and M7.5 (design v5: Spooly no longer
> catches panics, so they behave the same natively and on WASM), and M8 (a
> non-blocking core: the loop is a `step()` that a per-target driver calls,
> and the crate builds for `wasm32-unknown-unknown` and `wasm32-wasip1`),
> and M9 (the browser driver). Next: M10, WASI. See
> [`docs/INITIAL_PLAN.md`](docs/INITIAL_PLAN.md) §12.

The crate docs (`cargo doc --open`) are the guide: the closest JS
counterparts, the event loop, cancellation, panics, threads, and testing
with `MockClock`.

## Examples

```sh
cargo run --example hello          # tasks interleaving at each `.await`
cargo run --example timers         # the program above
cargo run --example intervals      # intervals on a fixed schedule, shared state
cargo run --example cancellation   # timeouts, abort, abort_on_drop, cancelling timers
cargo run --example panics         # errors as `Result`s, try_join on abort, a panic stopping the loop
cargo run --example threads        # waking `!Send` tasks from worker threads, KeepAlive
```

## In the browser

On `wasm32-unknown-unknown`, enable the `browser` feature and start the loop
with `browser::start`, which returns at once. Each step of the event loop then
runs from a JS callback, with the same ordering as on native:

```rust
spooly::browser::start(async {
    spooly::sleep(std::time::Duration::from_millis(500)).await;
    // ... update the page ...
});
```

[`examples/browser`](examples/browser) is a small page: a clock on an
interval, the event loop's ordering, and a task woken by button clicks.
Build it with the `wasm-bindgen` CLI (the same version as the crate in
`Cargo.lock`) and serve the folder:

```sh
cargo build --release --example browser --target wasm32-unknown-unknown --features browser
wasm-bindgen --target web --out-dir examples/browser/pkg \
    target/wasm32-unknown-unknown/release/examples/browser.wasm
python3 -m http.server -d examples/browser   # then open http://localhost:8000
```

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo +nightly miri test   # needs: rustup +nightly component add miri
```

`tests/model.rs` is the oracle: it generates random programs (microtasks,
timers, intervals, cancellation, slow callbacks, tasks, aborts, panics), runs
each on Spooly and on a small reference simulator of the design, and requires
identical traces.

`tests/node_parity.rs` also runs a handful of scenarios as JS in Node, as an
optional sanity check that Spooly still behaves like a JS event loop. It needs
`node` on `PATH`:

```sh
SPOOLY_COMPARE_NODE=1 cargo test --test node_parity
```

The suite also runs in a headless browser, except for the tests that need
threads or catch panics (WASM aborts on a panic), plus `tests/browser.rs`
for the browser driver. It needs [`wasm-pack`](https://github.com/drager/wasm-pack)
and Chrome or Firefox:

```sh
WASM_BINDGEN_USE_BROWSER=1 wasm-pack test --headless --firefox -- --features browser
```

Stress tests (a million tasks, 100k timers with random cancellation, very
long microtask and join chains, heavy cross-thread waking) are `#[ignore]`d;
CI runs them weekly:

```sh
cargo test --release --test stress -- --ignored
```

Benchmarks are dependency-free; the v1 baseline is in
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md):

```sh
cargo bench
```

Coverage (CI requires at least 90% of lines):

```sh
cargo install cargo-llvm-cov   # once
cargo llvm-cov --fail-under-lines 90
```
