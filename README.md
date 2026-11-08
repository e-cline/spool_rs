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
> `timeout`, `interval`, `set_timeout`, `set_interval`, `MockClock`), and M4
> (cancellation and panics: `abort`, `AbortHandle`, `abort_on_drop`,
> `try_join`, the JS-exception panic model, poisoning). Next up are liveness
> and threads (M5). See [`docs/INITIAL_PLAN.md`](docs/INITIAL_PLAN.md).

## Development

```sh
cargo test                 # full suite (Node parity tests skip if `node` is missing)
cargo clippy --all-targets -- -D warnings
cargo +nightly miri test   # needs: rustup +nightly component add miri
```

The Node parity tests (`tests/node_parity.rs`) run each scenario in Spool and,
as a JS twin in `tests/node_parity/`, in Node, and require identical traces.
Set `SPOOL_REQUIRE_NODE=1` to make a missing `node` an error instead of a skip.
