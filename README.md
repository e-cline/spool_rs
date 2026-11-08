# spool_rs

**Spool** is a lightweight, single-threaded Rust async executor, with a
scheduling model inspired by the JavaScript event loop. It targets native
platforms and WASM (browser and WASI support are in progress).

- No `Send` or `Sync` required: use `Rc`, `RefCell`, and `Cell` freely in tasks.
- Simple, predictable ordering: a woken task is queued as a microtask, in
  FIFO order, like the code after an `await` in JS. Timers run one at a
  time, and the microtask queue drains after each.
- Panics behave the same everywhere: Spool never catches them, so a panic
  stops the loop. Errors a caller should handle are `Result` values.
- Zero runtime dependencies, no `unsafe`.

```rust
use std::time::Duration;

spool::run(async {
    spool::set_timeout(Duration::from_millis(40), || println!("timeout 40ms"));
    spool::queue_microtask(|| println!("microtask"));
    println!("sync");

    let task = spool::spawn(async {
        spool::sleep(Duration::from_millis(10)).await;
        42
    });
    println!("joined: {}", task.await);
});
// Prints: sync, microtask, joined: 42, timeout 40ms
```

Spool borrows JS names where they're the clearest ones (`set_timeout`,
`queue_microtask`), but it is not a reimplementation of Node or the browser:
the design ([`docs/INITIAL_PLAN.md`](docs/INITIAL_PLAN.md)) defines its
semantics.

> **Status:** early development. Done: M0–M6 (the core loop, microtasks,
> timers, cancellation, liveness and threads, and polish), M7 (realigning
> with design v4: no `next_tick`, intervals that don't drift, and a
> reference model as the test oracle), and M7.5 (design v5: Spool no longer
> catches panics, so they behave the same natively and on WASM). Next: M8, a
> non-blocking core so the loop can run in the browser. See
> [`docs/INITIAL_PLAN.md`](docs/INITIAL_PLAN.md) §12.

The crate docs (`cargo doc --open`) are the guide: the closest JS
counterparts, the event loop, cancellation, panics, threads, and testing
with `MockClock`.

## Examples

```sh
cargo run --example hello          # tasks interleaving at each `.await`
cargo run --example timers         # the program above, plus an interval
cargo run --example cancellation   # timeouts, abort, abort_on_drop, cancelling timers
cargo run --example panics         # errors as `Result`s, try_join on abort, a panic stopping the loop
cargo run --example threads        # waking `!Send` tasks from worker threads, KeepAlive
```

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo +nightly miri test   # needs: rustup +nightly component add miri
```

`tests/model.rs` is the oracle: it generates random programs (microtasks,
timers, intervals, cancellation, slow callbacks, tasks, aborts, panics), runs
each on Spool and on a small reference simulator of the design, and requires
identical traces.

`tests/node_parity.rs` also runs a handful of scenarios as JS in Node, as an
optional sanity check that Spool still behaves like a JS event loop. It needs
`node` on `PATH`:

```sh
SPOOL_COMPARE_NODE=1 cargo test --test node_parity
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
