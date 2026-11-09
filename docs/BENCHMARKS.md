# Benchmarks

`crates/spooly/benches/throughput.rs` measures the hot paths: spawning, waking, the
microtask queue, and timers. It uses a small built-in harness rather than a
benchmark framework: each benchmark runs a batch of operations 11 times and
reports the median and best time per operation.

```sh
cargo bench                  # everything
cargo bench -- timer         # only benchmarks whose name contains "timer"
```

Timer benchmarks use a `MockClock`, so they measure the runtime's own
bookkeeping, not real waiting.

## Baseline (v1, safe code)

*(Measured before M7, which removed `next_tick` and its benchmark, and changed
how intervals re-arm. The rest of the hot paths are unchanged.)*

The numbers the performance phase (plan §9.6) has to beat. Measured at the
end of M6 on an Apple M2 Pro with rustc 1.98.1, release profile.

| Benchmark | Median | Throughput | What one operation is |
|---|---:|---:|---|
| `spawn/detached` | 118 ns | 8.5 M/s | spawn a task, drop its handle, run it to completion |
| `spawn/join` | 117 ns | 8.6 M/s | spawn a task, then await its handle |
| `spawn/block_on (new runtime each)` | 221 ns | 4.5 M/s | create a runtime and `block_on` a ready future |
| `wake/yield_now (one task)` | 13.5 ns | 74 M/s | one `yield_now().await`: a local wake and a re-poll |
| `wake/yield_now (1k tasks)` | 16.6 ns | 60 M/s | the same, across 1,000 interleaved tasks |
| `wake/remote (from another thread)` | 195 ns | 5.1 M/s | wake a task from another thread, and run it |
| `queue/queue_microtask` | 29.2 ns | 34 M/s | queue a microtask callback and run it |
| `timer/set_timeout + fire` | 260 ns | 3.9 M/s | arm a callback timer (1M pending, 1,000 deadlines), fire it |
| `timer/set_timeout + cancel` | 19.7 ns | 51 M/s | arm a callback timer and cancel it at once |
| `timer/sleep (1k tasks)` | 125 ns | 8.0 M/s | one `sleep().await`, across 1,000 tasks |
| `timer/interval ticks` | 81.1 ns | 12 M/s | one `Interval::tick().await` |
| `timer/timeout (completes first)` | 54.3 ns | 18 M/s | `timeout` around a ready future (arm, then disarm) |

Observations for §9.6:
- Spawning costs about 8x a wake: the task allocations (boxed future,
  `Arc<TaskHeader>`, join cell) dominate. Single-allocation tasks target this.
- Firing a timer from a large heap costs about 13x arming and cancelling one:
  the `BinaryHeap` pop with a million entries is cache-unfriendly. A timer
  wheel targets this.
- Cancellation is cheap (lazy deletion), but cancelled timers stay in the
  heap until their deadline. Intrusive lists would remove them at once.
