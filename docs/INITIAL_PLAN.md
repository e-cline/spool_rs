# Spool — Initial Design & Plan

> Status: **Approved design (v3)**. Nothing is implemented yet. Decisions are
> tagged `D#`; §11 records what was settled in review, and §12 lists the
> milestones.

Spool is a small, single-threaded Rust async executor with JavaScript / Node.js
event-loop semantics. Futures and tasks **do not need `Send` or `Sync`**, so
you can freely use `Rc`, `RefCell`, and `Cell` inside spawned tasks.

---

## 1. Goals and non-goals

### Goals (v1)

- A single-threaded event loop with **predictable, Node-like ordering**:
  synchronous code → `next_tick` queue → microtask queue → timers.
- No `Send`/`Sync` bounds on futures, outputs, or callbacks.
- APIs for v1:
  - `spawn` / `JoinHandle` (a task is the Rust equivalent of a running async function)
  - timers: `sleep`, `sleep_until`, `timeout`, `set_timeout`, `set_interval`, `interval`
  - microtasks: `yield_now`, `queue_microtask`, `next_tick`
- **Rust-idiomatic cancellation** for everything that can be cancelled (§5).
- **Panics behave like JS exceptions in async code**: an awaited panic is
  re-thrown in whoever awaits it, and a panic nobody observes crashes the loop
  (§6).
- Zero runtime dependencies. v1 has **no `unsafe`**; a later optimization phase
  may add it (§9.6).
- A **thorough test suite**, including differential testing against real Node (§8).
- The core is designed so WASM support (browser and WASI) can be added later
  without restructuring it (§9.4).

### Non-goals (v1)

- Multi-threaded scheduling or work stealing.
- Network and file I/O (future work, but the design leaves room for it).
- `set_immediate` and a check phase (future work).
- Compatibility with Tokio-specific APIs. Executor-agnostic futures should just
  work.

---

## 2. User-facing API sketch

```rust
use std::time::Duration;

fn main() {
    // Node-like: runs until `main` finishes AND the loop has nothing left to do.
    spool::run(async {
        spool::set_timeout(Duration::from_millis(10), || println!("timeout 10ms"));
        spool::queue_microtask(|| println!("microtask"));
        spool::next_tick(|| println!("nextTick"));
        println!("sync");

        let h = spool::spawn(async {
            spool::sleep(Duration::from_millis(5)).await;
            42
        });
        println!("joined: {}", h.await);
    });
}
```

Expected output (the same as the equivalent Node program):

```
sync
nextTick
microtask
joined: 42        (after ~5ms)
timeout 10ms      (after ~10ms; `run` returns after this)
```

### Public surface (v1)

```rust
// ── Entry points ────────────────────────────────────────────────────────────
pub fn run<F: Future + 'static>(main: F) -> F::Output;      // wait for an empty loop
pub fn block_on<F: Future>(main: F) -> F::Output;           // return as soon as `main` completes
                                                            // (no 'static: main lives on the stack)
pub struct Runtime;   // Runtime::builder().clock(MockClock::new()).build().block_on(..)

// ── Tasks ───────────────────────────────────────────────────────────────────
pub fn spawn<F: Future + 'static>(fut: F) -> JoinHandle<F::Output>;

#[must_use = "dropping a JoinHandle detaches the task"]
pub struct JoinHandle<T>;      // impl Future<Output = T>  (re-throws the task's panic)
impl<T> JoinHandle<T> {
    pub fn abort(&self);                                   // cancel the task
    pub fn abort_handle(&self) -> AbortHandle;             // Clone, cancel from elsewhere
    pub fn is_finished(&self) -> bool;
    pub fn abort_on_drop(self) -> AbortOnDrop<T>;          // scope-bound task
    pub fn try_join(self) -> TryJoin<T>;                   // Future<Output = Result<T, JoinError>>
}
pub struct AbortHandle;        // Clone; abort(&self), is_finished(&self)
pub enum JoinError { Panicked(Box<dyn Any + Send>), Aborted }

// ── Microtasks / ticks ──────────────────────────────────────────────────────
pub fn queue_microtask(f: impl FnOnce() + 'static);
pub fn next_tick(f: impl FnOnce() + 'static);
pub async fn yield_now();            // ≈ `await null` / `await Promise.resolve()`

// ── Timers: future-style (drop = cancel) ────────────────────────────────────
pub fn sleep(d: Duration) -> Sleep;                       // Future<Output = ()>
pub fn sleep_until(deadline: Instant) -> Sleep;
pub fn timeout<F: Future>(d: Duration, f: F) -> Timeout<F>; // Output = Result<F::Output, Elapsed>
pub fn timeout_at<F: Future>(deadline: Instant, f: F) -> Timeout<F>;
pub fn now() -> Instant;                                  // the current runtime's clock
pub fn interval(period: Duration) -> Interval;            // async fn tick(&mut self) -> Instant

// ── Timers: callback-style (handle drop = keep running; .cancel() = cancel) ─
pub fn set_timeout(d: Duration, f: impl FnOnce() + 'static) -> TimerHandle;
pub fn set_interval(period: Duration, f: impl FnMut(&TimerHandle) + 'static) -> TimerHandle;
pub struct TimerHandle;        // Clone; cancel(&self), is_active(&self)
```

The free functions use a thread-local "current runtime" and panic if called
outside `run` or `block_on`, the same way `tokio::spawn` does.

**D1. Naming.** The API uses snake_case versions of the JS names. `yield` is a
reserved word in Rust, so the yield function is `yield_now`. There are no
`clear_timeout` or `clear_interval` free functions: cancelling is a method on
the handle (§5).

**D2. Both futures and callbacks.** The callback forms mirror JS. The future
forms are what idiomatic Rust code will mostly use. A callback that needs to do
async work can call `spawn` inside it.

---

## 3. Execution model & ordering semantics

This follows Node's event loop (Node ≥ 11 semantics: the queues are drained
between *every* timer callback).

### 3.1 Queues

| Queue | Contents | JS analogue | Drained |
|---|---|---|---|
| **tick queue** | `next_tick` closures | `process.nextTick` | Fully, before the microtask queue |
| **microtask queue** | `queue_microtask` closures **and task polls** (woken tasks) | V8 microtask / promise-job queue | Fully, including items added while draining |
| **timer heap** | `set_timeout`, `set_interval`, `sleep`, `interval`, `timeout` entries | libuv timers phase | One expired timer at a time, with both queues drained after each |

**D3. A woken task is a microtask.** In JS, the code after an `await` resumes
as a promise job on the microtask queue. In Spool, the matching event is a
`Waker` firing, so `wake()` puts a "poll task #N" entry on the **same FIFO
queue** as `queue_microtask` closures. That makes the interleaving match JS:

```rust
spool::queue_microtask(|| println!("A"));
spool::spawn(async { println!("B"); spool::yield_now().await; println!("D"); });
spool::queue_microtask(|| println!("C"));
// prints A B C D, same as the JS equivalent
```

**D4. `spawn` does not poll eagerly.** It puts the task's first poll on the
microtask queue. A JS async function runs synchronously until its first
`await`; matching that would need re-entrant polling, which conflicts with
`RefCell`-based state. Spool's ordering is the same as
`queueMicrotask(() => asyncFn())`. This intentional difference is documented.

**D4a. Main's first poll is a macrotask (the "script").** `run` and `block_on`
poll the main future once directly, outside the microtask queue, the way a
JS program's top-level code runs. Ticks and microtasks it queues run after it,
ticks first. Later polls of main (after an `.await`) are microtasks, like any
other task. Before that first poll, any work already queued on the runtime
(left over from an earlier `block_on`, or spawned with `Runtime::spawn`) is
drained first, keeping Node's invariant that the queues are empty when a
macrotask starts. *(Settled in M2.)*

**D4b. Awaiting a ready future doesn't yield.** In JS, `await x` always
takes at least one microtask hop, even when `x` is already resolved. In Rust,
`.await` on a future that is already `Ready` (for example, the `JoinHandle` of
a task that has finished) continues synchronously, without a hop. This is an
inherent difference between Rust and JS. Node parity scenarios avoid it, and
the documentation states it. *(Settled in M2.)*

### 3.2 Draining "between macrotasks"

This is the same loop as Node's `processTicksAndRejections`:

```
fn drain_ticks_and_microtasks():
    loop:
        while let Some(t) = tick_queue.pop_front():  run t
        while let Some(m) = microtask_queue.pop_front(): run m   // includes newly queued
        if both empty: break
```

So, as in Node, a `next_tick` queued *from inside a microtask* runs after the
current microtask queue has been drained, and before the next macrotask.

### 3.3 One loop iteration ("turn")

```
turn():
    drain_ticks_and_microtasks()
    absorb remote wakes (other threads) → microtask queue; drain again
    raise any unobserved panic (§6)

    // timers phase
    now  = clock.now()
    seq0 = timers.next_seq          // only timers that existed when the phase started
    while let Some(t) = timers.pop_expired(now, before_seq = seq0):
        fire t   (run its callback / wake its future / re-arm if interval)
        drain_ticks_and_microtasks()

    // (future: poll/I-O phase, check/set_immediate phase, close phase)

    return Busy | WaitUntil(next_deadline) | WaitForever | Idle
```

The outer driver loop:

```
loop:
    if mode == BlockOn && main is done: break
    match turn():
        Busy            => continue
        WaitUntil(t)    => park(until t)          // or until a remote wake
        WaitForever     => park(None)             // main can still be woken remotely (§3.5)
        Idle            => break                  // finished, or stalled → panic (§3.5)
```

**D5. `turn()` is separate from `park()`.** The event-loop logic never blocks;
only the `Park` implementation does. This is what makes the browser WASM
target possible later, and it is where the I/O reactor plugs in.

### 3.4 Timers

- Stored in a `BinaryHeap` keyed by `(deadline, seq)`. The `seq` number keeps
  FIFO order among equal deadlines, matching JS's insertion order.
- Entries live in a slab with generation counters. Cancelling marks the slot
  dead, and the heap skips dead entries when it pops them.
- **D6. No 1ms clamp.** Node treats `setTimeout(0)` as 1ms; Spool doesn't. The
  `seq0` snapshot still gives the behavior that matters: a timer scheduled
  *during* the timers phase runs in the **next** turn, so a timer that re-arms
  itself can't starve the loop. *(M3 detail: a timer armed mid-phase with a
  deadline in the past, via `sleep_until`, sorts to the top of the heap. It is
  set aside for the rest of the phase so that it can't block older due timers
  behind it.)*
- **D7. Interval semantics follow Node.** When an interval fires, it re-arms
  at *the timers phase's start time* + `period` (Node re-arms from the loop's
  cached time, read before the callback runs). The time the callback itself
  takes doesn't shift the schedule. But a late timers phase (for example,
  after another slow callback) makes the interval drift, and missed ticks are
  never replayed in a burst. `interval()` behaves the same way: each tick is
  due `period` after the previous tick completed. `interval().tick()`
  completes immediately the first time, as in Tokio. *(Refined in M3.)*
- `set_interval`'s callback receives `&TimerHandle`, so it can cancel itself
  without the `Rc<Cell<Option<Handle>>>` workaround.
- A zero `period` panics, because it would make an infinite loop of
  macrotasks. Node clamps it to 1ms instead.
- **D7a. Timers start when they are created.** `sleep(d)` arms its timer
  right away, like `setTimeout`, rather than on first poll. So an un-awaited
  `Sleep` keeps `run` alive until its deadline (or until it is dropped), and
  timers created in sequence fire in creation order. A `Sleep` keeps only the
  latest `Waker`. Even `sleep(Duration::ZERO)` waits for the next timers
  phase. *(Settled in M3.)*
- **Equal deadlines race in creation order**, as with `Promise.race` over two
  `setTimeout`s. `timeout(d, fut)` arms its own timer when it is called, so it
  beats a `sleep(d)` that `fut` creates later. If the inner future and the
  deadline are both ready at the same poll, the output wins.

### 3.5 Liveness, exit, and stalls

Node keeps the process alive while there are *ref'd handles* (timers, sockets,
and so on). A pending promise does **not** keep it alive. Spool uses the same
rule for **background work**:

The loop is **alive** if any of these is true:
- the tick or microtask queue is non-empty
- there are active timers (`unref()` is future work)
- there are `KeepAlive` guards *(M5)*; a remote wake is queued
- (future) there are I/O registrations

*(M5)* `KeepAlive` is a `Send + Sync + Clone` guard (`spool::keep_alive()`,
`Runtime::keep_alive()`). While one exists, an idle loop parks instead of
exiting, still serving remote wakes, and dropping the last guard unparks it.
It is how a detached task that waits on another thread keeps `run` alive. The
thread should wake the task *before* it drops the guard. After the loop sees
the last guard (or the last outside waker) go, it re-checks the remote queue
behind an acquire fence, so a wake queued just before that drop is never
missed.

Detached tasks that are pending on something that isn't ref'd (for example a
channel that nothing will ever send on) don't keep `run` alive. As in Node,
they are dropped when the runtime is torn down.

The **main future is a special case.** If the loop is not alive and `main` is
still pending:

- If some `Waker` for `main` still exists outside the runtime (checked with
  `Arc::strong_count` on the task header; see §4.3), something could still
  wake it, for example a `std::thread` finishing a job. The loop **parks
  indefinitely** (`WaitForever`) until that wake arrives.
- If no waker for `main` exists anywhere, nothing can ever wake it. This is a
  **provable stall**.

**D8. A stalled `main` panics** in both `run` and `block_on`, with a
diagnostic message:

```
spool: main future stalled: it is pending but nothing can wake it
       (no queued work, no timers, and no outstanding wakers)
```

Here is why it panics rather than returning `Result`. A stall is a bug in the
program, like a deadlock: the code can't do anything useful to recover from it,
because the future is stuck forever. Returning a `Result` would make every
caller `.unwrap()` a case that can't happen in correct code. A panic is also
what Rust users expect from a `block_on`-style function that can't produce its
value. If tests or embedders turn out to need it, `Runtime::try_block_on`
returning `Result<T, Stalled>` can be added later without breaking anything.

*(Hardened in M5.)* The check looks at every task, not just `main`. Each
header's references are its owner's plus its wakers. A waker that a pending
task's `JoinHandle` state holds for its awaiter (recognized by the waker's
data pointer) is **accounted for**: it can only fire after that task finishes,
which needs that task to be woken first. If no header (main or any task) has
an unaccounted waker, nothing can ever run again, so it is a stall. That
catches `main` awaiting a stuck task, and chains of them, which used to park
forever. Any waker the runtime can't identify counts as outside, so the check
never reports a stall that isn't one. A `KeepAlive` postpones the check until
it is dropped.

Known limitation: stall detection is still **best-effort, not complete**. A
waker stored in user data (an `Rc<RefCell<Option<Waker>>>`, a channel) counts
as outside, even if whatever holds it is itself stuck, and then the loop parks
forever instead of panicking. That is the same failure mode as a deadlocked
mutex.

**`run` vs `block_on`:**
- `run(main)` returns `main`'s output once `main` is done **and** the loop is
  no longer alive.
- `block_on(main)` returns as soon as `main` is done. Remaining tasks and
  timers are dropped along with the runtime.

### 3.6 Starvation

As in JS, a task that loops on `yield_now()` (a microtask yield), or a
microtask that keeps queueing microtasks, starves timers. This is faithful to
JS and documented. The way out is `sleep(Duration::ZERO).await`, which yields
to the timers phase. `set_immediate` is future work.

---

## 4. Internal architecture

### 4.1 Crate layout

```
spool/
├── Cargo.toml          # edition 2024, no runtime dependencies
├── src/
│   ├── lib.rs          # public re-exports + free functions
│   ├── runtime.rs      # Runtime, builder, run/block_on, driver loop, turn()
│   ├── context.rs      # thread-local CURRENT runtime (enter/exit guard)
│   ├── task.rs         # task slab, spawn, JoinHandle, AbortHandle, JoinError
│   ├── waker.rs        # Arc-based Waker (std::task::Wake), local/remote paths
│   ├── queue.rs        # tick queue + microtask queue
│   ├── timer/
│   │   ├── mod.rs      # timer heap/slab, TimerHandle, set_timeout/set_interval
│   │   ├── sleep.rs    # Sleep, sleep_until
│   │   ├── interval.rs # Interval
│   │   └── timeout.rs  # Timeout, Elapsed
│   ├── clock.rs        # Instant, Clock trait, SystemClock, MockClock
│   └── park.rs         # Park trait, ThreadPark
├── tests/              # see §8
└── examples/
```

### 4.2 Core data structures (sketch)

```rust
struct Inner {                                  // owned via Rc, !Send
    ticks:      RefCell<VecDeque<Box<dyn FnOnce()>>>,
    microtasks: RefCell<VecDeque<Microtask>>,
    tasks:      RefCell<Slab<TaskSlot>>,        // tiny internal slab, no dep
    timers:     RefCell<TimerQueue>,
    unobserved_panic: RefCell<Option<Box<dyn Any + Send>>>,
    clock:      Rc<dyn Clock>,
    shared:     Arc<Shared>,                    // the only thread-safe part
}

enum Microtask { Callback(Box<dyn FnOnce()>), Poll(TaskId) }

struct TaskSlot {
    future: Option<Pin<Box<dyn Future<Output = ()>>>>, // writes into JoinState
    header: Arc<TaskHeader>,
    join:   Rc<dyn JoinStateDyn>,               // so abort/panic can resolve it
}

struct TaskHeader {                 // Send + Sync, so it can back a std Waker
    id: TaskId,                     // slab index + generation
    scheduled: AtomicBool,          // dedupes repeated wakes before the next poll
    shared: Arc<Shared>,
    owner: ThreadId,
}
```

### 4.3 Wakers without `Send` futures

`std::task::Waker` is always `Send + Sync`, so the waker must not point at the
`!Send` future.

**D9.** The waker is an `Arc<TaskHeader>`, built through the safe
`std::task::Wake` trait. The header holds only a `TaskId` and thread-safe
state; the future stays in the runtime's thread-local slab. `wake()` does this:

1. If `scheduled.swap(true)` was already `true`, it stops: the task is already queued.
2. **Same thread** and this runtime is `CURRENT`: it pushes
   `Microtask::Poll(id)` onto the local queue.
3. **Other thread**: it pushes `id` onto `shared.remote_queue` (a `Mutex`) and
   unparks the loop.
4. If the runtime is gone, or the generation doesn't match because the task
   finished or was aborted, nothing happens.

The slab owns exactly one strong reference to each header. `JoinHandle` and
`AbortHandle` refer to the task by `TaskId`, not through the header. So when
the loop is idle, `Arc::strong_count(header) > 1` means a waker exists somewhere
— that is the stall check in §3.5.

### 4.4 Polling and re-entrancy rules

- Before polling a task, the runtime takes the future out of its slot and
  releases every `RefCell` borrow. After the poll, the future goes back into
  the slot, or the slot is freed.
- `scheduled` is cleared *before* the poll, so a wake that happens during the
  poll correctly queues the task again.
- If a task aborts **itself** during its own poll, it is marked aborted, and
  its future is dropped as soon as the poll returns.
- Callbacks are popped and the borrow is released before they run.
- A future's `Drop` can run user code (spawn, wake, abort, set timers). Drops
  therefore always happen with no runtime borrows held. That includes drops
  during abort, during teardown, and when `timeout` elapses.

---

## 5. Cancellation

**D10. Idiomatic rule: futures cancel on drop. Handles detach on drop and
cancel explicitly.** This is the convention in `std` and Tokio. Every future
already cancels when you drop it. A handle that cancelled on drop would turn a
fire-and-forget `set_timeout(...)` or `spawn(...)` into a silent no-op the
moment the returned handle was ignored.

| Thing | How to cancel | Effect |
|---|---|---|
| `Sleep`, `Interval`, `Timeout`, any future | `drop` it | Timer entry removed; inner future dropped |
| `timeout(d, fut)` elapses | automatic | `fut` is dropped; returns `Err(Elapsed)` |
| Spawned task | `handle.abort()` or `abort_handle.abort()` | Future dropped (at once, or after its current poll if it is running); queued polls become no-ops |
| Scope-bound task | `handle.abort_on_drop()` → drop the guard | Same as `abort()` |
| `set_timeout` / `set_interval` | `timer_handle.cancel()` (idempotent; Clone-able handle) | Callback never runs again; the closure is dropped immediately |

Notes:
- Aborting a task **does not** abort the tasks it spawned, since tasks are
  detached and not structured. A `TaskSet`/`JoinSet` that cancels on drop is
  future work.
- Calling `abort()` on a task that has already finished does nothing.
  `is_finished()` reports `true` for a task that completed, panicked, or was
  aborted.
- Awaiting an aborted task's `JoinHandle` **panics** with "task was aborted",
  because no `T` exists. Code that may abort a task it also awaits uses
  `try_join()` and gets `Err(JoinError::Aborted)`.
- `queue_microtask` and `next_tick` callbacks **cannot** be cancelled. They
  run within the current turn, so there is almost no window in which to cancel
  them. (Decided in review.)

---

## 6. Panics

The recommendation is to treat a panic like a **JS exception inside async code**:

| Situation | JS | Spool |
|---|---|---|
| Task panics; someone awaits its `JoinHandle` | Awaiting a rejected promise re-throws in the awaiter | `handle.await` **re-throws** the panic in the awaiting task (`resume_unwind`) |
| Same, using `try_join()` | `try { await p } catch` | Returns `Err(JoinError::Panicked(payload))` |
| Task panics and its handle was dropped (detached) | Unhandled rejection → crash (Node ≥ 15) | Panic **unwinds out of `run` / `block_on`** |
| Task panics while its handle is alive but not awaited yet | Rejection is held until it's handled | Payload is held in the handle; if the handle is later dropped without being observed, the panic unwinds out of `run` at the next turn |
| Callback panics (`set_timeout`, microtask, tick) | Uncaught exception → crash | Unwinds out of `run` / `block_on` at once |
| `main` panics | Crash | Unwinds out of `run` / `block_on` at once |

**D11.** How it works: each task poll is wrapped in
`catch_unwind(AssertUnwindSafe(..))`. That costs nothing on the non-panicking
path. A caught payload is either stored in the task's `JoinState` (if the
handle is alive) or put in `unobserved_panic`, which the loop re-raises at a
safe point, after all borrows are released. Callbacks are not wrapped; their
panics unwind straight out.

Why this is the best default:
- `JoinHandle<T>` stays `Future<Output = T>`, with no `.unwrap()` on every join,
  and a panic still reaches the code that is waiting for the result, like `?`.
- No panic is ever silently swallowed. Tokio does swallow panics in detached
  tasks, and that is a common source of confusion.
- It matches the mental model of JS users.

Caveats (documented):
- With `panic = "abort"`, every panic aborts the process. That is normal in
  browser WASM builds and fine here.
- If the runtime is torn down while an unobserved payload is still held inside
  a live `JoinHandle`, that payload is dropped silently, because panicking
  inside `Drop` is not allowed. An `on_unhandled_panic` hook is future work.
- After a panic unwinds out of `run` or `block_on`, that runtime is
  **poisoned** and can't be reused. A fresh `Runtime` works normally.

As built (M4):
- **Safe points.** An unobserved panic is re-raised right after the tick,
  microtask, task poll, or main poll that produced it, and at the start of
  each `run` / `block_on` call (which covers a handle dropped between calls).
  Nothing else runs first, so the loop stops at once, as Node does on an
  unhandled rejection. Only the first unobserved panic is kept.
- **Resolving the handle.** The wrapped task future owns a guard. If the
  future is dropped before it finishes (on abort or at teardown), the guard
  marks the task `Aborted` and wakes the joiner. A panic overrides that
  state, since unwinding out of the `async` block already ran the guard.
  So abort needs no type-erased access to the join state; only panics use
  `JoinDyn`.
- **Self-abort.** A task that aborts itself and then completes in the same
  poll keeps its output: it finished before its future could be dropped.
- A **detached task's output** is dropped as soon as the task finishes.

---

## 7. Clock and park

```rust
pub struct Instant(Duration);   // offset from the clock's epoch; wasm-safe
pub trait Clock {
    fn now(&self) -> Instant;
    // Called when idle until `deadline`; return true if the clock jumped
    // there itself (MockClock), so the runtime shouldn't sleep.
    fn advance_when_idle(&self, deadline: Instant) -> bool { false }
}
Runtime::builder().clock(MockClock::new()).build()
```

- `SystemClock` is backed by `std::time::Instant`. Spool has its own `Instant`
  type because `std`'s panics on `wasm32-unknown-unknown`.
- `MockClock` stays frozen until something advances it. When the loop would
  otherwise sleep until the next timer, it **always auto-advances** to that
  deadline (like Tokio's `start_paused`), so timer tests are instant and
  deterministic. Tests can also call `MockClock::advance(d)`, for example
  from inside a callback to simulate a slow one. *(M3: there's no
  "manual-only" mode. A frozen clock with nothing left to advance it would
  hang the loop.)*
- Parking (`src/park.rs`) is a plain function on top of
  `std::thread::park` / `park_timeout`. The `Park` trait is left for the I/O
  reactor milestone, which will need more than one implementation.

---

## 8. Test suite

The goal is a suite thorough enough that ordering bugs and soundness edge cases
can't slip through. Runtime dependencies stay at zero. These test-only
**dev-dependencies** are approved: `proptest`, `trybuild`, and
`static_assertions`.

### 8.1 Layers

| Layer | Location | What it covers |
|---|---|---|
| **Unit tests** | `src/**` `#[cfg(test)]` | Slab generations, timer heap ordering and cancellation, queue drains, waker dedupe, the strong-count logic |
| **Ordering tests** | `tests/ordering.rs` | Hand-written scenarios with exact expected traces (§8.2) |
| **Node differential tests** | `tests/node_parity/` | Each scenario has a `.js` twin; the harness runs it with `node` and compares the trace with Spool's |
| **Randomized differential tests** | `tests/fuzz_ordering.rs` | `proptest` generates random trees of `next_tick`/`queue_microtask`/`set_timeout`/`set_interval`/`spawn`/`yield_now`/`sleep`/cancel operations. The same tree is (a) run in Spool and (b) emitted as JS and run in Node; the traces must match. *(M5: each program runs in its own Node worker, so one Node process checks hundreds of programs in parallel. Delays are multiples of 20ms; programs where timers of different durations share a deadline are skipped, because Node orders those by its per-duration timer lists. `SPOOL_FUZZ_CASES` and `SPOOL_FUZZ_SEED` widen the search.)* |
| **Reference-model tests** | `tests/model.rs` | A small, obviously-correct queue simulator in test code, compared against Spool with `proptest`. Runs without Node, so it covers CI machines where Node isn't installed |
| **Timer tests** | `tests/timers.rs` | `MockClock`-driven checks of deadlines, interval drift, timeouts, and zero durations; no real sleeps |
| **Cancellation tests** | `tests/cancel.rs` | Every row of the §5 table, plus edge cases (§8.3) |
| **Panic tests** | `tests/panics.rs` | Every row of the §6 table, plus poisoning |
| **Liveness tests** | `tests/liveness.rs` | `run` vs `block_on`, stall panic, remote-wake waits, background work not keeping the loop alive |
| **Drop/leak tests** | `tests/drops.rs` | Drop-counting futures and closures check that everything is dropped exactly once, and that nothing leaks via `Rc` cycles after completion, abort, timeout, or teardown |
| **Re-entrancy tests** | `tests/reentrancy.rs` | User code calling into the runtime from inside polls, callbacks, and `Drop` impls (§8.3) |
| **Thread tests** | `tests/remote.rs` | Wakes from other threads: single, many, racing with local wakes, arriving after the task finished, arriving after the runtime was dropped |
| **Type-level tests** | `tests/types.rs` + `trybuild` | `!Send` futures compile; `JoinHandle`, `TimerHandle`, and `AbortHandle` are `!Send` (static assertions); misuse that should not compile doesn't |
| **Stress tests** | `tests/stress.rs` (`#[ignore]` by default) | 1M spawned tasks, 100k timers with random cancellation, deep `next_tick` chains; checks for no stack growth and roughly linear time |
| **Doc tests** | rustdoc | Every public item has a runnable example |

### 8.2 Core ordering scenarios (each one also a Node parity case)

1. `sync → nextTick → microtask → setTimeout(0)`.
2. A `next_tick` queued inside a microtask runs after the microtask queue drains.
3. Two `setTimeout(0)` callbacks: a microtask queued in the first runs before the second.
4. A timer that re-arms itself with delay 0 doesn't starve another expired timer.
5. Equal-delay timers fire in the order they were registered; shorter delays
   fire first regardless of registration order.
6. The `A B C D` interleaving of `yield_now` and `queue_microtask` (§3.1).
7. An interval interleaved with timeouts, and an interval cancelling itself on
   its Nth tick.
8. Nested: a timeout scheduling a microtask, which schedules a tick, which
   schedules a timeout.
9. `spawn` inside a microtask, inside a tick, and inside a timer callback.
10. A chain of `JoinHandle` awaits (A awaits B, which awaits C); completion order
    matches a chain of resolved promises.

### 8.3 Edge cases that must have tests

- A task wakes itself during its own poll; a task wakes itself multiple times.
- A task aborts itself; a task aborts the task currently awaiting it.
- `abort()` while the task is queued in the microtask queue: the queued poll
  becomes a no-op.
- A future's `Drop` spawns a task, sets a timer, or wakes another task — during
  abort, during timeout, and during runtime teardown.
- A timer callback cancels itself, cancels a sibling that has also expired in
  the same phase, or schedules a new timer with an earlier deadline.
- `Sleep` polled with a different waker each time; `Sleep` polled after it has
  completed; a zero-duration `Sleep`.
- A `JoinHandle` polled after it has completed; `try_join` after the task panicked.
- Calling the free functions outside a runtime (clear panic); a nested
  `block_on` inside a running runtime (clear panic); two runtimes one after the
  other on the same thread.
- A remote wake that arrives after the slab slot was reused, rejected by its
  generation.

### 8.4 Tooling and CI

- CI matrix: Linux, macOS, Windows, on latest stable. Beta also runs, but
  isn't required to pass.
- `cargo test`, then `cargo test -- --ignored` (stress tests) on a scheduled job.
- The Node parity and randomized differential tests run when `node` is on
  `PATH`, which CI installs. Locally they are skipped with a message if Node
  isn't installed.
- `cargo clippy -D warnings`, `cargo fmt --check`, and `cargo doc -D warnings`.
- Coverage with `cargo-llvm-cov`. The target is ≥ 90% of lines, and every
  branch in `runtime.rs`, `task.rs`, and `timer/`.
- Miri runs on the full suite from the start. It is cheap, and it becomes
  essential once `unsafe` lands (§9.6).

---

## 9. Future work

### 9.1 Async network I/O
- Add a **poll phase** to `turn()`, between timers and check, as in libuv.
- Add a reactor `Park` implementation on top of the OS readiness API (kqueue,
  epoll, and IOCP or `wepoll`-style). The choice between writing a minimal one
  and depending on `mio` / `polling` is made at that point.
- `!Send` `TcpListener`, `TcpStream`, and `UdpSocket`, implementing the
  `futures-io` traits, with optional Tokio-io traits behind a feature flag.
- I/O registrations count toward liveness, with `unref()` support.

### 9.2 Async file I/O
- Regular files are not pollable for readiness, so as in libuv this uses a
  **small blocking thread pool** that completes back to the loop over the
  remote-wake path. On Linux, `io_uring` is an optional backend.
- `spawn_blocking(f) -> JoinHandle<T>`, where `f: Send` but the handle is `!Send`.

### 9.3 More JS-like scheduling and utilities
- `set_immediate` with a check phase, and `yield_macrotask()`.
- `TimerHandle::unref()` / `ref()`.
- `KeepAlive` guards, and a `Send` `RemoteHandle` for waking or sending work
  into the loop from other threads. These also make remote waits for detached
  tasks keep `run` alive.
- `TaskSet` / `JoinSet` (cancel all on drop), as a step toward structured
  concurrency.
- Local (`!Send`) sync primitives: `oneshot`, `mpsc`, `Mutex`, `RwLock`,
  `Notify`, `Semaphore`.
- An `AbortController` / `AbortSignal`-style cancellation token.
- An `on_unhandled_panic` hook (like `process.on('unhandledRejection')`), and
  `Runtime::try_block_on` returning `Result<T, Stalled>`.
- Tracing hooks for scheduling events.

### 9.4 WebAssembly (browser and WASI)
- **Browser (`wasm32-unknown-unknown` + `wasm-bindgen`)**: the main thread
  can't block, so there is no `Park`. A `spool::wasm::start(fut)` entry point
  would instead:
  - run `turn()` from JS-scheduled callbacks, using the browser's
    `queueMicrotask` so Spool microtasks interleave with JS promise jobs
  - schedule the next timer deadline with JS `setTimeout`
  - read time from `performance.now()` (through `Clock`)
  - make cross-thread wakes a no-op unless the build has atomics or threads
  - accept `JsFuture` as an ordinary future
- **WASI (`wasm32-wasip1` / `wasm32-wasip2`, for wasmtime and others)**: a
  `Park` implementation on `poll_oneoff` (p1) or `wasi:io/poll` pollables (p2).
  That gives timers natively and network I/O later on p2.
- Both are behind feature flags, so the native build stays dependency-free.
  Because `turn()` never blocks and `Clock`/`Park` are abstractions, the core
  works on these targets unchanged.
- The test suite runs under `wasm-bindgen-test` (headless browser) and on
  wasmtime.

### 9.5 Other
- Task names or ids, and a debug dump of the queues and timers.

### 9.6 Performance phase (`unsafe`)
v1 is entirely safe code. The M6 benchmarks (baseline in
[`BENCHMARKS.md`](BENCHMARKS.md)) are the starting point: a profiling-driven
pass may switch to `unsafe` in the hot paths where it is clearly worth it:
- **Single-allocation tasks**: header, future, and output in one allocation,
  with a hand-written `RawWaker` vtable, instead of `Box<dyn Future>` + slab +
  `Arc<TaskHeader>`.
- Intrusive linked lists for the run queue and timers, replacing
  `VecDeque<Microtask>` + `BinaryHeap` and the lazy-deletion scheme. That gives
  O(1) cancellation with no tombstones.
- A hierarchical timer wheel, if timer-heavy benchmarks show it helps.
- Unboxed `Timeout<F>`: pin-project the inner future instead of boxing it
  (v1 boxes it to stay free of `unsafe`).
- A local-wake fast path that avoids atomics where it's sound.

Each change must keep the full test suite and Miri passing, and must show a
measured improvement in benchmarks before it merges.

---

## 10. Open questions

None that block implementation.

- **Publishing (deferred).** We haven't decided whether to publish on
  crates.io. Until then the package is named `spool` locally, with
  `publish = false` in `Cargo.toml`. If we do publish and `spool` is taken,
  only the package name changes (for example `spool-rt`); the library can stay
  `spool` through `[lib] name`.

---

## 11. Resolved in review

| Topic | Decision |
|---|---|
| Entry points | Both `run` (Node-like) and `block_on` |
| Stalled main | Panic with a diagnostic message (D8); `try_block_on` later if needed |
| `set_interval` | In v1 (callback `set_interval` + future `interval()`) |
| `set_immediate` | Not in v1 (future work) |
| `setTimeout(0)` | Unclamped (D6) |
| JoinHandle drop | Detach; `abort()` / `abort_on_drop()` to cancel (D10) |
| Cancellation | Idiomatic: futures cancel on drop, handles cancel by method; no `clear_*` functions |
| `unsafe` | None in v1; performance phase later (§9.6) |
| WASM | Browser and WASI, both future work |
| Testing | Comprehensive, multi-layer suite with Node differential testing (§8) |
| Panic model | JS-exception model (§6): re-throw into the awaiter, `try_join` for `Result`, unobserved panics crash `run` |
| Microtask/tick cancellation | Not needed; `queue_microtask` / `next_tick` return `()` |
| Dev-dependencies | `proptest`, `trybuild`, `static_assertions` are approved (test-only) |
| Toolchain | Edition 2024; the MSRV is the latest stable (1.98 at time of writing), raised freely |
| Publishing | Undecided; `publish = false` for now (§10) |

---

## 12. Milestones

| # | Milestone | Contents |
|---|---|---|
| M0 | Scaffold | Crate, CI matrix, lint/fmt/doc gates, test harness skeleton, Node-parity runner |
| M1 | Core loop | `Runtime`, `block_on`, context, task slab, `Wake`-based waker, microtask queue, `spawn`, `JoinHandle`, `yield_now`, plus their unit and re-entrancy tests |
| M2 | JS queues | `queue_microtask`, `next_tick`, drain semantics; ordering scenarios 1, 2, 6, 9, 10; reference model + `proptest`; basic `run` (queues-only liveness, pulled forward from M5 so Node parity scenarios can run to completion) |
| M3 | Timers | `Clock`, `MockClock`, parking with a deadline, timer heap, `sleep`, `sleep_until`, `timeout`, `timeout_at`, `set_timeout`, `set_interval`, `interval`, `TimerHandle::cancel` (pulled forward from M4, since `set_interval` needs it); scenarios 3–5, 7, 8; timer tests; timers added to the reference model |
| M4 | Cancellation & panics | `abort`, `AbortHandle`, `abort_on_drop`, `try_join`, `catch_unwind` + unobserved-panic path, poisoning; cancel, panic, and drop/leak tests |
| M5 | Liveness & threads | Full liveness for `run` (timers, `KeepAlive`), stall-detection hardening, remote wakes; liveness and thread tests; randomized Node differential tests |
| M6 | Polish | Rustdoc with JS-equivalence tables, examples, stress tests, benchmarks (spawn, wake, timer throughput) to give §9.6 a baseline, coverage gate |
