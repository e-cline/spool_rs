# spool_rs

**Spool** is a lightweight, single-threaded Rust async executor with
JavaScript / Node.js event-loop semantics.

- No `Send` or `Sync` required: use `Rc`, `RefCell`, and `Cell` freely in tasks.
- Predictable, Node-like ordering: a woken task is queued as a microtask, in
  FIFO order, just like the code after an `await` in JS.
- Zero runtime dependencies, no `unsafe`.

```rust
use std::time::Duration;

spool::run(async {
    spool::set_timeout(Duration::from_millis(40), || println!("timeout 40ms"));
    spool::queue_microtask(|| println!("microtask"));
    spool::next_tick(|| println!("nextTick"));
    println!("sync");

    let task = spool::spawn(async {
        spool::sleep(Duration::from_millis(10)).await;
        42
    });
    println!("joined: {}", task.await);
});
// Prints, exactly as the equivalent Node program does:
// sync, nextTick, microtask, joined: 42, timeout 40ms
```

> **Status:** early development. Implemented so far: M0 (scaffold), M1 (core
> loop: `block_on`, `spawn`, `JoinHandle`, `yield_now`), M2 (`queue_microtask`,
> `next_tick`, Node's drain order, `run`), M3 (timers: `sleep`,
> `timeout`, `interval`, `set_timeout`, `set_interval`, `MockClock`), M4
> (cancellation and panics: `abort`, `AbortHandle`, `abort_on_drop`,
> `try_join`, the JS-exception panic model, poisoning), and M5 (liveness and
> threads: `KeepAlive`, stronger stall detection, randomized differential
> tests against Node), and M6 (polish: a guide-style rustdoc with JS
> equivalence tables, examples, stress tests, benchmarks, a coverage gate).
> The v1 plan is complete; next is the performance phase. See
> [`docs/INITIAL_PLAN.md`](docs/INITIAL_PLAN.md).

The crate docs (`cargo doc --open`) are the guide: the JS-to-Spool table, the
event loop, cancellation, panics, threads, and testing with `MockClock`.

## Examples

```sh
cargo run --example hello          # tasks interleaving at each `.await`
cargo run --example timers         # the program above: ticks, microtasks, tasks, timers
cargo run --example cancellation   # timeouts, abort, abort_on_drop, cancelling timers
cargo run --example panics         # try_join, re-throwing, unobserved panics, poisoning
cargo run --example threads        # waking `!Send` tasks from worker threads, KeepAlive
```

## Development

```sh
cargo test                 # full suite (Node parity tests skip if `node` is missing)
cargo clippy --all-targets -- -D warnings
cargo +nightly miri test   # needs: rustup +nightly component add miri
```

The Node parity tests (`tests/node_parity.rs`) run each scenario in Spool and,
as a JS twin in `tests/node_parity/`, in Node, and require identical traces.
Set `SPOOL_REQUIRE_NODE=1` to make a missing `node` an error instead of a skip.

`tests/fuzz_ordering.rs` goes further: it generates random programs, runs
each in Spool and (as generated JS, one worker per program) in Node, and
compares the traces. To search harder:

```sh
SPOOL_FUZZ_CASES=5000 SPOOL_FUZZ_SEED=7 cargo test --test fuzz_ordering
```

Stress tests (a million tasks, 100k timers with random cancellation, very
long tick and join chains, heavy cross-thread waking) are `#[ignore]`d; CI
runs them weekly:

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
