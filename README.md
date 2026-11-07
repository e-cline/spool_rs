# spool_rs

**Spool** is a lightweight, single-threaded Rust async executor with
JavaScript / Node.js event-loop semantics.

- No `Send` or `Sync` required: use `Rc`, `RefCell`, and `Cell` freely in tasks.
- Predictable, Node-like ordering: a woken task is queued as a microtask, in
  FIFO order, just like the code after an `await` in JS.
- Zero runtime dependencies, no `unsafe`.

```rust
let answer = spool::block_on(async {
    let task = spool::spawn(async {
        spool::yield_now().await;
        6 * 7
    });
    task.await
});
assert_eq!(answer, 42);
```

> **Status:** early development. Milestones M0 (scaffold) and M1 (core loop:
> `block_on`, `spawn`, `JoinHandle`, `yield_now`) are implemented. Timers,
> `queue_microtask`, `next_tick`, cancellation, and the rest of the plan are
> next. See [`docs/INITIAL_PLAN.md`](docs/INITIAL_PLAN.md).

## Development

```sh
cargo test                 # full suite (Node parity tests skip if `node` is missing)
cargo clippy --all-targets -- -D warnings
cargo +nightly miri test   # needs: rustup +nightly component add miri
```

The Node parity tests (`tests/node_parity.rs`) run each scenario in Spool and,
as a JS twin in `tests/node_parity/`, in Node, and require identical traces.
Set `SPOOL_REQUIRE_NODE=1` to make a missing `node` an error instead of a skip.
