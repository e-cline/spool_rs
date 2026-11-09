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
- Minimal dependencies: none at runtime on native targets today (the
  optional `browser` feature adds `wasm-bindgen`), and no `unsafe`.

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
>
> Async I/O is in progress ([`docs/ASYNC_IO_PLAN.md`](docs/ASYNC_IO_PLAN.md)):
> M11 (a Cargo workspace), M12 (an I/O driver hook in the core), M13
> ([`crates/spooly-io`](crates/spooly-io): TCP, UDP and Unix sockets), and
> M14 (`spooly::spawn_blocking`, a thread pool in the core; `spooly_io::fs`
> and DNS on it), and M15 (in-memory test streams, `Async<T>` for any
> file descriptor) are done. Next: M16, WASI 0.2 I/O, after the core's
> WASI driver (M10).

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
cargo run --example event_loop_order  # sync code, microtasks, spawn, yield_now, timers: who runs when
cargo run --example mock_clock     # MockClock: retries with backoff, hours of timers in microseconds
cargo run --example channel        # a bounded `!Send` channel from scratch, with backpressure
cargo run --example actor          # a task owning its state, with request/reply and no locks
cargo run -p spooly-io --example echo   # an echo server and clients on one thread
cargo run -p spooly-io --example copy   # a file copy on the blocking pool, with a ticker running meanwhile
cargo run -p spooly-io --example scan   # a concurrent TCP port scan with timeouts
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

[`crates/spooly/examples/browser`](crates/spooly/examples/browser) is a small page: a clock on an
interval, the event loop's ordering, and a task woken by button clicks.
Build it with the `wasm-bindgen` CLI (the same version as the crate in
`Cargo.lock`) and serve the folder:

```sh
cargo build --release -p spooly --example browser --target wasm32-unknown-unknown --features browser
wasm-bindgen --target web --out-dir crates/spooly/examples/browser/pkg \
    target/wasm32-unknown-unknown/release/examples/browser.wasm
python3 -m http.server -d crates/spooly/examples/browser   # then open http://localhost:8000
```

Two more pages build the same way (swap the example name):
[`browser_debounce`](crates/spooly/examples/browser_debounce) (a debounced search box, with
`Sleep::reset` and cancelling a stale search) and
[`browser_yield`](crates/spooly/examples/browser_yield) (why `yield_now` doesn't keep a page
responsive, and what does).

## Development

The repository is a Cargo workspace. The core runtime is
[`crates/spooly`](crates/spooly); async I/O (planned, see
[`docs/ASYNC_IO_PLAN.md`](docs/ASYNC_IO_PLAN.md)) will be a separate crate
next to it. Commands run from the repository root:

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo +nightly miri test   # needs: rustup +nightly component add miri
```

`crates/spooly/tests/model.rs` is the oracle: it generates random programs (microtasks,
timers, intervals, cancellation, slow callbacks, tasks, aborts, panics), runs
each on Spooly and on a small reference simulator of the design, and requires
identical traces.

`crates/spooly/tests/node_parity.rs` also runs a handful of scenarios as JS in Node, as an
optional sanity check that Spooly still behaves like a JS event loop. It needs
`node` on `PATH`:

```sh
SPOOLY_COMPARE_NODE=1 cargo test -p spooly --test node_parity
```

The suite also runs in a headless browser, except for the tests that need
threads or catch panics (WASM aborts on a panic), plus `crates/spooly/tests/browser.rs`
for the browser driver. It needs [`wasm-pack`](https://github.com/drager/wasm-pack)
and Chrome or Firefox:

```sh
WASM_BINDGEN_USE_BROWSER=1 wasm-pack test --headless --firefox crates/spooly --features browser
```

Stress tests (a million tasks, 100k timers with random cancellation, very
long microtask and join chains, heavy cross-thread waking) are `#[ignore]`d;
CI runs them weekly:

```sh
cargo test --release -p spooly --test stress -- --ignored
```

Benchmarks use a small built-in harness (no benchmark framework); the v1
baseline is in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md):

```sh
cargo bench
```

Coverage (CI requires at least 90% of lines):

```sh
cargo install cargo-llvm-cov   # once
cargo llvm-cov --fail-under-lines 90
```
