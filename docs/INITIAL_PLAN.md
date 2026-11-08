# Spool — Design & Plan

> Status: **Design v4.** v1 (milestones M0–M6) is implemented. v4 changes
> direction: Spool is *inspired by* the JavaScript event loop, not a
> reimplementation of Node's, and it targets native and WASM equally. §11.2
> lists what v4 changed, and §12 has the milestones that bring the code in
> line (M7 onward). Decisions are tagged `D#`; a decision marked
> *(revised in v4)* is not implemented yet.

Spool is a small, single-threaded Rust async executor whose scheduling model
is inspired by the JavaScript event loop: woken tasks run as FIFO microtasks,
and timers run one at a time as macrotasks. Futures and tasks **do not need
`Send` or `Sync`**, so you can freely use `Rc`, `RefCell`, and `Cell` inside
spawned tasks. It runs natively and in WASM (browser and WASI).

---

## 1. Goals and non-goals

### Goals

- A single-threaded event loop with **simple, predictable, documented
  ordering**: the code of a macrotask → the microtask queue → timers, one
  at a time. Spool's own spec (§3) and reference model (§8) define the order.
- **Native and WASM**: the same core runs on desktop and server OSes, in the
  browser (`wasm32-unknown-unknown`), and under WASI. The loop never blocks
  on its own; a per-target driver waits for it (D5).
- No `Send`/`Sync` bounds on futures, outputs, or callbacks.
- APIs:
  - `spawn` / `JoinHandle` (a task is the Rust equivalent of a running async function)
  - timers: `sleep`, `sleep_until`, `timeout`, `set_timeout`, `set_interval`, `interval`
  - microtasks: `yield_now`, `queue_microtask`
- **Rust-idiomatic cancellation** for everything that can be cancelled (§5).
- **Panics propagate to whoever awaits them**, like exceptions in async JS,
  and a panic nobody observes stops the loop (§6). Native only: see §6 for
  WASM.
- Zero runtime dependencies on native targets. Target-specific drivers may
  depend on platform bindings behind feature flags (§7). No `unsafe` until
  the performance phase (§9.6).
- A **thorough test suite**, with a reference model as the oracle (§8).

### Non-goals

- **Exact parity with Node.js or browser semantics, or with JS APIs.** JS is
  the inspiration and a useful reference for readers. Where its behavior is a
  historical quirk (Node's `process.nextTick`, its 1ms timer clamp, timer
  drift, per-duration timer lists), Spool picks what is simplest to reason
  about and documents it.
- Multi-threaded scheduling or work stealing.
- Network and file I/O (future work, but the design leaves room for it).
- Compatibility with Tokio-specific APIs. Executor-agnostic futures should just
  work.

---

## 2. User-facing API sketch

```rust
use std::time::Duration;

fn main() {
    // Runs until `main` finishes AND the loop has nothing left to do.
    spool::run(async {
        spool::set_timeout(Duration::from_millis(10), || println!("timeout 10ms"));
        spool::queue_microtask(|| println!("microtask"));
        println!("sync");

        let h = spool::spawn(async {
            spool::sleep(Duration::from_millis(5)).await;
            42
        });
        println!("joined: {}", h.await);
    });
}
```

Expected output:

```
sync
microtask
joined: 42        (after ~5ms)
timeout 10ms      (after ~10ms; `run` returns after this)
```

### Public surface

```rust
// ── Entry points ────────────────────────────────────────────────────────────
pub fn run<F: Future + 'static>(main: F) -> F::Output;      // native: wait for an empty loop
pub fn block_on<F: Future>(main: F) -> F::Output;           // native: return as soon as `main` completes
                                                            // (no 'static: main lives on the stack)
pub struct Runtime;   // Runtime::builder().clock(MockClock::new()).build().block_on(..)
// Browser: `spool::browser::start(main)` returns at once; the loop runs from
// host callbacks (§7, M9).

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

// ── Microtasks ──────────────────────────────────────────────────────────────
pub fn queue_microtask(f: impl FnOnce() + 'static);
pub async fn yield_now();            // one microtask hop, like `await null`

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

// ── Liveness (native) ───────────────────────────────────────────────────────
pub fn keep_alive() -> KeepAlive;  // Send + Sync + Clone guard (§3.5)
```

The free functions use a thread-local "current runtime" and panic if called
outside one, the same way `tokio::spawn` does.

**D1. Naming** *(revised in v4)*. Names come from JS where the JS name is
the clearest one for the concept (`set_timeout`, `set_interval`,
`queue_microtask`), in snake_case. Borrowing a name does not promise
identical behavior: each item documents Spool's own semantics. New APIs use
Rust names (for example a cancellation token, not `AbortController`). `yield`
is a reserved word in Rust, so the yield function is `yield_now`. There are no
`clear_timeout` or `clear_interval` free functions: cancelling is a method on
the handle (§5).

**D2. Both futures and callbacks.** The callback forms are convenient for
fire-and-forget work. The future forms are what idiomatic Rust code will
mostly use. A callback that needs to do async work can call `spawn` inside it.

**D12. No `next_tick`** *(new in v4)*. v1 had a second, higher-priority
queue modeled on Node's `process.nextTick`. It exists only in Node, it is
most of the complexity in the drain rules, and it has no browser
counterpart. v4 removes it (M7). Code that wants "run this before other
queued work" can usually restructure, or run it inline.

---

## 3. Execution model & ordering semantics

The model is two levels, the same shape as in browsers and Node: a
**macrotask** (main's first poll, or one timer firing) runs, and then the
**microtask queue** drains completely before the next macrotask. The details
below are Spool's spec; JS is the inspiration, not the reference.

### 3.1 Queues

| Queue | Contents | Drained |
|---|---|---|
| **microtask queue** | `queue_microtask` closures **and task polls** (woken tasks) | Fully, including items added while draining |
| **timer heap** | `set_timeout`, `set_interval`, `sleep`, `interval`, `timeout` entries | One expired timer at a time, with the microtask queue drained after each |

**D3. A woken task is a microtask.** In JS, the code after an `await` resumes
as a promise job on the microtask queue. In Spool, the matching event is a
`Waker` firing, so `wake()` puts a "poll task #N" entry on the **same FIFO
queue** as `queue_microtask` closures:

```rust
spool::queue_microtask(|| println!("A"));
spool::spawn(async { println!("B"); spool::yield_now().await; println!("D"); });
spool::queue_microtask(|| println!("C"));
// prints A B C D
```

**D4. `spawn` does not poll eagerly.** It puts the task's first poll on the
microtask queue. A JS async function runs synchronously until its first
`await`; doing that would need re-entrant polling, which conflicts with
`RefCell`-based state.

**D4a. Main's first poll is a macrotask.** The first poll of the main future
runs directly, outside the microtask queue, like a program's top-level code:
microtasks it queues run after it. Later polls of main (after an `.await`)
are microtasks, like any other task. Work already queued on the runtime
before that first poll (left over from an earlier `block_on`, or spawned with
`Runtime::spawn`) is drained first, so every macrotask starts with an empty
queue. *(Settled in M2.)*

**D4b. Awaiting a ready future doesn't yield.** `.await` on a future that is
already `Ready` (for example, the `JoinHandle` of a task that has finished)
continues synchronously, without a microtask hop. That is how Rust futures
work, and it differs from JS, where `await` always takes a hop. *(Settled in
M2.)*

### 3.2 Draining

```
fn drain_microtasks():
    while let Some(m) = microtask_queue.pop_front(): run m   // includes newly queued
```

*(v1 also had a tick queue, drained before microtasks and again whenever a
microtask queued a tick. It goes away with `next_tick` in M7, D12.)*

### 3.3 One step of the loop

**D5. The loop is a non-blocking step; drivers do the waiting** *(revised
in v4)*. The event-loop logic lives in one function that does a bounded
amount of work and returns, and never blocks:

```
step() -> Step:
    drain_microtasks()
    absorb remote wakes (other threads) → microtask queue; drain again
    raise any unobserved panic (§6)

    // timers phase
    now  = clock.now()
    seq0 = timers.next_seq          // only timers that existed when the phase started
    while let Some(t) = timers.pop_expired(now, before_seq = seq0):
        fire t   (run its callback / wake its future / re-arm if interval)
        drain_microtasks()

    // (future: an I/O poll phase)

    return Busy | WaitUntil(next_deadline) | WaitForWake | Idle
```

A **driver** owns the waiting. Each target has one (§7):

```
native driver (run / block_on):
loop:
    if mode == BlockOn && main is done: break
    match step():
        Busy          => continue
        WaitUntil(t)  => park the thread until t, or until a remote wake
        WaitForWake   => park until a remote wake (§3.5)
        Idle          => break                  // finished, or stalled → panic (§3.5)

browser driver: runs step() from a host callback, then schedules the next
callback (a JS timeout for WaitUntil; nothing for WaitForWake, since a wake
schedules one). It never blocks and never panics on a stall.
```

To make this possible, the main future becomes a task the driver can resume,
instead of a value pinned on `drive()`'s stack (M8). *(As built in v1, the
step and the native park loop are one function, `drive()`, which blocks the
thread. That works natively but not in the browser.)*

### 3.4 Timers

- Stored in a `BinaryHeap` keyed by `(deadline, seq)`. The `seq` number keeps
  FIFO order among equal deadlines.
- Entries live in a slab with generation counters. Cancelling marks the slot
  dead, and the heap skips dead entries when it pops them.
- **D6. No delay clamp.** A zero delay is allowed. The `seq0` snapshot still
  gives the behavior that matters: a timer scheduled *during* the timers
  phase runs in the **next** step, so a timer that re-arms itself can't
  starve the loop. *(M3 detail: a timer armed mid-phase with a deadline in
  the past, via `sleep_until`, sorts to the top of the heap. It is set aside
  for the rest of the phase so that it can't block older due timers behind
  it.)*
- **D7. Intervals don't drift, and skip missed ticks** *(revised in v4)*.
  An interval created at `t0` with period `p` is due at `t0 + p`,
  `t0 + 2p`, and so on. Each tick re-arms at the *previous deadline* + `p`,
  so neither the time a callback takes nor a late timers phase shifts the
  schedule. If the loop falls so far behind that one or more deadlines have
  already passed, those ticks are **skipped**: the interval re-arms at the
  first `t0 + k·p` after now, and fires once, not in a burst. (This is
  Tokio's `MissedTickBehavior::Skip`.) `set_interval` and `interval()` behave
  the same way. `interval().tick()` completes immediately the first time.
  *(v1 copied Node instead: it re-armed from the timers phase's start time,
  so late phases made the schedule drift. M7 changes it.)*
- `set_interval`'s callback receives `&TimerHandle`, so it can cancel itself
  without the `Rc<Cell<Option<Handle>>>` workaround.
- A zero `period` panics, because it would make an infinite loop of
  macrotasks.
- **D7a. Timers start when they are created.** `sleep(d)` arms its timer
  right away rather than on first poll. So an un-awaited `Sleep` keeps `run`
  alive until its deadline (or until it is dropped), and timers created in
  sequence fire in creation order. A `Sleep` keeps only the latest `Waker`.
  Even `sleep(Duration::ZERO)` waits for the next timers phase. *(Settled in
  M3.)*
- **Equal deadlines fire in creation order.** `timeout(d, fut)` arms its own
  timer when it is called, so it beats a `sleep(d)` that `fut` creates later.
  If the inner future and the deadline are both ready at the same poll, the
  output wins.

### 3.5 Liveness, exit, and stalls

This section is about the native driver, where `run` and `block_on` have to
decide when to return. The browser driver never exits: when there's nothing
to do it just stops scheduling callbacks until something wakes it.

The loop is **alive** if any of these is true:
- the microtask queue is non-empty
- there are active timers (`unref()` is future work)
- there are `KeepAlive` guards; a remote wake is queued
- (future) there are I/O registrations

A pending task does **not** keep the loop alive on its own: something has to
be able to wake it.

`KeepAlive` is a `Send + Sync + Clone` guard (`spool::keep_alive()`,
`Runtime::keep_alive()`). While one exists, an idle loop parks instead of
exiting, still serving remote wakes, and dropping the last guard unparks it.
It is how a detached task that waits on another thread keeps `run` alive. The
thread should wake the task *before* it drops the guard. After the loop sees
the last guard (or the last outside waker) go, it re-checks the remote queue
behind an acquire fence, so a wake queued just before that drop is never
missed. *(M5.)*

Detached tasks that are pending on something that can't wake them (for
example a channel that nothing will ever send on) don't keep `run` alive.
They are dropped when the runtime is torn down.

The **main future is a special case.** If the loop is not alive and `main` is
still pending:

- If some `Waker` for `main` still exists outside the runtime (checked with
  `Arc::strong_count` on the task header; see §4.3), something could still
  wake it, for example a `std::thread` finishing a job. The loop **parks
  indefinitely** (`WaitForWake`) until that wake arrives.
- If no waker for `main` exists anywhere, nothing can ever wake it. This is a
  **provable stall**.

**D8. A stalled `main` panics** in both `run` and `block_on`, with a
diagnostic message:

```
spool: main future stalled: it is pending but nothing can wake it
       (no queued work, no timers, and no outstanding wakers)
```

A stall is a bug in the program, like a deadlock: the code can't do anything
useful to recover from it. Returning a `Result` would make every caller
`.unwrap()` a case that can't happen in correct code.
`Runtime::try_block_on` returning `Result<T, Stalled>` can be added later
without breaking anything.

*(Hardened in M5.)* The check looks at every task, not just `main`. A waker
that a pending task's `JoinHandle` state holds for its awaiter (recognized by
the waker's data pointer) is **accounted for**: it can only fire after that
task finishes, which needs that task to be woken first. If no header (main or
any task) has an unaccounted waker, nothing can ever run again, so it is a
stall. Any waker the runtime can't identify counts as outside, so the check
never reports a stall that isn't one. A `KeepAlive` postpones the check.

Known limitation: stall detection is **best-effort, not complete**. A waker
stored in user data (an `Rc<RefCell<Option<Waker>>>`, a channel) counts as
outside, even if whatever holds it is itself stuck, and then the loop parks
forever instead of panicking.

**`run` vs `block_on`:**
- `run(main)` returns `main`'s output once `main` is done **and** the loop is
  no longer alive.
- `block_on(main)` returns as soon as `main` is done. Remaining tasks and
  timers are dropped along with the runtime.

### 3.6 Starvation

A task that loops on `yield_now()` (a microtask yield), or a microtask that
keeps queueing microtasks, starves timers, as in JS. This is documented. The
way out is `sleep(Duration::ZERO).await`, which yields to the timers phase.

---

## 4. Internal architecture

### 4.1 Crate layout

```
spool/
├── Cargo.toml          # edition 2024; no dependencies on native targets
├── src/
│   ├── lib.rs          # public re-exports + free functions
│   ├── runtime.rs      # Runtime, builder, step() (§3.3)
│   ├── context.rs      # thread-local CURRENT runtime (enter/exit guard)
│   ├── task.rs         # task slab, spawn, JoinHandle, AbortHandle, JoinError
│   ├── waker.rs        # Arc-based Waker (std::task::Wake), local/remote paths
│   ├── queue.rs        # microtask queue
│   ├── timer/
│   │   ├── mod.rs      # timer heap/slab, TimerHandle, set_timeout/set_interval
│   │   ├── sleep.rs    # Sleep, sleep_until
│   │   ├── interval.rs # Interval
│   │   └── timeout.rs  # Timeout, Elapsed
│   ├── clock.rs        # Instant, Clock trait, SystemClock, MockClock
│   └── driver/         # (M8) the per-target drivers of §7
│       ├── native.rs   # run / block_on: thread park, remote wakes
│       ├── browser.rs  # (M9, feature "browser") host callbacks, JS timers
│       └── wasi.rs     # (M10, if native.rs doesn't already work there)
├── tests/              # see §8
└── examples/
```

### 4.2 Core data structures (sketch)

```rust
struct Inner {                                  // owned via Rc, !Send
    microtasks: RefCell<VecDeque<Microtask>>,
    tasks:      RefCell<Slab<TaskSlot>>,        // tiny internal slab, no dep
    timers:     RefCell<TimerQueue>,
    unobserved_panic: Cell<Option<Box<dyn Any + Send>>>,
    clock:      Rc<dyn Clock>,
    shared:     Arc<Shared>,                    // the only thread-safe part
}

enum Microtask { Callback(Box<dyn FnOnce()>), Poll(TaskId) }

struct TaskSlot {
    future: Option<Pin<Box<dyn Future<Output = ()>>>>, // writes into JoinState
    header: Arc<TaskHeader>,
    join:   Rc<dyn JoinStateDyn>,               // so panics can resolve it
}

struct TaskHeader {                 // Send + Sync, so it can back a std Waker
    id: TaskId,                     // slab index + generation
    scheduled: AtomicBool,          // dedupes repeated wakes before the next poll
    shared: Arc<Shared>,
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
   notifies the driver (natively, it unparks the loop's thread).
4. If the runtime is gone, or the generation doesn't match because the task
   finished or was aborted, nothing happens.

The slab owns exactly one strong reference to each header. `JoinHandle` and
`AbortHandle` refer to the task by `TaskId`, not through the header. So when
the loop is idle, `Arc::strong_count(header) > 1` means a waker exists somewhere
— that is the stall check in §3.5.

On WASM targets without threads, step 3 never happens; the code path stays,
so builds with atomics and threads can use it.

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
  detached and not structured. A `JoinSet` that cancels on drop is future
  work.
- Calling `abort()` on a task that has already finished does nothing.
  `is_finished()` reports `true` for a task that completed, panicked, or was
  aborted.
- Awaiting an aborted task's `JoinHandle` **panics** with "task was aborted",
  because no `T` exists. Code that may abort a task it also awaits uses
  `try_join()` and gets `Err(JoinError::Aborted)`.
- `queue_microtask` callbacks **cannot** be cancelled. They run within the
  current step, so there is almost no window in which to cancel them.

---

## 6. Panics

A panic behaves like an **exception in async code**: it travels to whoever
awaits the task, and a panic nobody can observe stops the loop rather than
being swallowed.

| Situation | Spool |
|---|---|
| Task panics; someone awaits its `JoinHandle` | `handle.await` **re-throws** the panic in the awaiting task (`resume_unwind`) |
| Same, using `try_join()` | Returns `Err(JoinError::Panicked(payload))` |
| Task panics and its handle was dropped (detached) | Panic **unwinds out of `run` / `block_on`** |
| Task panics while its handle is alive but not awaited yet | Payload is held in the handle; if the handle is later dropped without being observed, the panic unwinds out of `run` at the next safe point |
| Callback panics (`set_timeout`, microtask) | Unwinds out of `run` / `block_on` at once |
| `main` panics | Unwinds out of `run` / `block_on` at once |

**D11.** How it works: each task poll is wrapped in
`catch_unwind(AssertUnwindSafe(..))`. That costs nothing on the non-panicking
path. A caught payload is either stored in the task's `JoinState` (if the
handle is alive) or put in `unobserved_panic`, which the loop re-raises at a
safe point, after all borrows are released. Callbacks are not wrapped; their
panics unwind straight out.

Why this is the default:
- `JoinHandle<T>` stays `Future<Output = T>`, with no `.unwrap()` on every join,
  and a panic still reaches the code that is waiting for the result, like `?`.
- No panic is ever silently swallowed. Tokio does swallow panics in detached
  tasks, and that is a common source of confusion.

**Targets** *(new in v4)*. All of this needs `panic = "unwind"`. That is the
default on native targets. WASM targets abort on panic by default, so there
every panic ends the program (in the browser, the instance traps), and
`JoinError::Panicked` is never produced. Nothing in the runtime relies on
catching a panic for correctness, so the rest of Spool works the same with
`panic = "abort"`. The panic tests are native-only. If a WASM build turns
on unwinding, the model works there unchanged.

Caveats (documented):
- If the runtime is torn down while an unobserved payload is still held inside
  a live `JoinHandle`, that payload is dropped silently, because panicking
  inside `Drop` is not allowed. An unobserved-panic hook is future work.
- After a panic unwinds out of `run` or `block_on`, that runtime is
  **poisoned** and can't be reused. A fresh `Runtime` works normally.

As built (M4):
- **Safe points.** An unobserved panic is re-raised right after the
  microtask, task poll, or main poll that produced it, and at the start of
  each `run` / `block_on` call (which covers a handle dropped between calls).
  Nothing else runs first, so the loop stops at once. Only the first
  unobserved panic is kept.
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

## 7. Targets: clocks and drivers

*(Revised in v4: WASM moves from future work into the milestones.)*

```rust
pub struct Instant(Duration);   // offset from the clock's epoch; wasm-safe
pub trait Clock {
    fn now(&self) -> Instant;
    // Called when idle until `deadline`; return true if the clock jumped
    // there itself (MockClock), so the driver shouldn't wait.
    fn advance_when_idle(&self, deadline: Instant) -> bool { false }
}
Runtime::builder().clock(MockClock::new()).build()
```

- Spool has its own `Instant` because `std::time::Instant::now()` panics on
  `wasm32-unknown-unknown`.
- `MockClock` stays frozen until something advances it. When the loop would
  otherwise wait for the next timer, it **always auto-advances** to that
  deadline (like Tokio's `start_paused`), so timer tests are instant and
  deterministic. Tests can also call `MockClock::advance(d)`. *(M3: there's
  no "manual-only" mode. A frozen clock with nothing left to advance it would
  hang the loop.)* It works the same on every target.

| | Native | Browser (`wasm32-unknown-unknown`) | WASI (`wasm32-wasip1` / `-wasip2`) |
|---|---|---|---|
| Default clock | `SystemClock` (`std::time::Instant`) | `PerformanceClock` (`performance.now()`) | `SystemClock` (`std` supports it) |
| Driver | thread park / unpark (`run`, `block_on`) | host callbacks (`browser::start`); next deadline via JS `setTimeout` | thread sleep (`poll_oneoff`), or the native driver if it already works |
| Entry point | `run`, `block_on` | `browser::start(main)`, returns at once | `run`, `block_on` |
| Remote wakes | yes | only with atomics/threads | no threads (p1) |
| Panics | unwind (§6) | abort by default | abort by default |
| Dependencies | none | `wasm-bindgen` + `js-sys`, behind feature `browser` | none expected |
| Tests | full suite | `wasm-bindgen-test`, headless browser | full suite (minus threads) under wasmtime |

- **D13. Default clock per target** *(new in v4)*. `Runtime::new()` picks
  the target's default clock with `cfg`, so the default runtime never panics
  on a supported target. *(As built in v1, it always uses `SystemClock`,
  which panics in the browser.)*
- **D14. Browser microtasks** *(new in v4)*. The browser driver runs a
  whole Spool step inside one host callback (a JS microtask for `Busy`, a JS
  timeout for `WaitUntil`). Spool's microtasks are therefore ordered among
  themselves exactly as on native, but not interleaved one-by-one with JS
  promise jobs. That keeps the semantics identical across targets. A
  `JsFuture` awaited in a task works like any other future.
- On every target, `cargo check` must pass in CI from M8, so a regression in
  portability is caught before the target's own driver lands.

---

## 8. Test suite

The goal is a suite thorough enough that ordering bugs and soundness edge cases
can't slip through. Runtime dependencies stay at zero. These test-only
**dev-dependencies** are approved: `proptest`, `trybuild`, and
`static_assertions`.

**D15. The reference model is the oracle** *(revised in v4)*. Spool's
behavior is defined by this plan (§3) and checked against the reference model
in `tests/model.rs`: a small, obviously-correct simulator of §3, compared
with Spool on random programs by `proptest`. It runs everywhere, with no
external tools and no real time. Node is no longer the definition of correct.
*(v1 required Node in CI, and fuzzed random programs against it, skipping
programs where Node's internal timer lists would decide the order. M7 moves
that coverage into the model, then removes the Node fuzzer.)*

### 8.1 Layers

| Layer | Location | What it covers |
|---|---|---|
| **Unit tests** | `src/**` `#[cfg(test)]` | Slab generations, timer heap ordering and cancellation, queue drains, waker dedupe, the strong-count logic |
| **Reference-model tests** | `tests/model.rs` | The oracle (D15). Random trees of `queue_microtask` / `set_timeout` / `set_interval` / cancel / `spawn` / `abort` / `yield_now` / `sleep` / join / panic operations, run on Spool (with a `MockClock`) and on the model; the traces must match. *(M7 adds intervals and timer cancellation, which only the Node fuzzer covered in v1.)* |
| **Ordering tests** | `tests/ordering.rs` | Hand-written scenarios with exact expected traces (§8.2) |
| **Timer tests** | `tests/timers.rs` | `MockClock`-driven checks of deadlines, interval schedules and skipped ticks, timeouts, and zero durations; no real sleeps |
| **Cancellation tests** | `tests/abort.rs` and others | Every row of the §5 table, plus edge cases (§8.3) |
| **Panic tests** | `tests/panics.rs` | Every row of the §6 table, plus poisoning (native only) |
| **Liveness tests** | `tests/liveness.rs`, `tests/run.rs` | `run` vs `block_on`, stall panic, remote-wake waits, background work not keeping the loop alive |
| **Drop/leak tests** | `tests/drops.rs` | Drop-counting futures and closures check that everything is dropped exactly once, and that nothing leaks via `Rc` cycles after completion, abort, timeout, or teardown |
| **Re-entrancy tests** | `tests/reentrancy.rs` | User code calling into the runtime from inside polls, callbacks, and `Drop` impls (§8.3) |
| **Thread tests** | `tests/remote.rs` | Wakes from other threads: single, many, racing with local wakes, arriving after the task finished, arriving after the runtime was dropped |
| **Type-level tests** | `tests/types.rs` + `trybuild` | `!Send` futures compile; `JoinHandle`, `TimerHandle`, and `AbortHandle` are `!Send` (static assertions); misuse that should not compile doesn't |
| **Stress tests** | `tests/stress.rs` (`#[ignore]` by default) | 1M spawned tasks, 100k timers with random cancellation, deep microtask and join chains; checks for no stack growth and roughly linear time |
| **Doc tests** | rustdoc | Every public item has a runnable example |
| **Node comparison** (optional) | `tests/node_parity.rs` | A handful of §8.2 scenarios also run as JS in Node, as a sanity check that Spool's model is still recognizably JS-like. Skipped when Node isn't installed; not required in CI. A scenario where Spool deliberately differs (intervals, D7) is not compared. |
| **WASM** (M8–M10) | CI | `cargo check` on WASM targets; the suite under `wasm-bindgen-test` (browser) and wasmtime (WASI) |

### 8.2 Core ordering scenarios

1. `sync → microtask → timeout(0)`.
2. Two `timeout(0)` callbacks: a microtask queued in the first runs before the second.
3. A timer that re-arms itself with delay 0 doesn't starve another expired timer.
4. Equal-delay timers fire in the order they were registered; shorter delays
   fire first regardless of registration order.
5. The `A B C D` interleaving of `yield_now` and `queue_microtask` (§3.1).
6. An interval interleaved with timeouts; an interval cancelling itself on
   its Nth tick; an interval whose callback is slower than its period
   (ticks are skipped, the schedule doesn't drift).
7. Nested: a timeout scheduling a microtask, which schedules a timeout.
8. `spawn` inside a microtask and inside a timer callback.
9. A chain of `JoinHandle` awaits (A awaits B, which awaits C).

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
  isn't required to pass (and skips the `trybuild` comparison, whose expected
  output pins stable's wording).
- `cargo test`, then the stress tests on a weekly schedule.
- The optional Node comparison runs when `node` is on `PATH`. CI doesn't
  require it.
- `cargo clippy -D warnings`, `cargo fmt --check`, and `cargo doc -D warnings`.
- Coverage with `cargo-llvm-cov`: CI requires ≥ 90% of lines.
- Miri runs on the full suite. It becomes essential once `unsafe` lands (§9.6).
- From M8: `cargo check` for `wasm32-unknown-unknown` and `wasm32-wasip1`.

---

## 9. Future work

### 9.1 Async network I/O
- Add an **I/O poll phase** to `step()`, after timers.
- Add a reactor driver on top of the OS readiness API (kqueue, epoll, and
  IOCP or `wepoll`-style). The choice between writing a minimal one and
  depending on `mio` / `polling` is made at that point. On WASI p2,
  `wasi:io/poll` pollables play the same role.
- `!Send` `TcpListener`, `TcpStream`, and `UdpSocket`, implementing the
  `futures-io` traits, with optional Tokio-io traits behind a feature flag.
- I/O registrations count toward liveness, with `unref()` support.

### 9.2 Async file I/O
- Regular files are not pollable for readiness, so this uses a **small
  blocking thread pool** that completes back to the loop over the remote-wake
  path. On Linux, `io_uring` is an optional backend. Native only.
- `spawn_blocking(f) -> JoinHandle<T>`, where `f: Send` but the handle is `!Send`.

### 9.3 More scheduling and utilities

Each is judged on its own merits, with Rust names, not on whether JS has it.

- A yield that lets due timers run first (`sleep(ZERO)` does this today; a
  dedicated name may read better).
- `TimerHandle::unref()` / `ref()`: a timer that doesn't keep `run` alive.
- `JoinSet` (cancel all on drop), as a step toward structured concurrency.
- Local (`!Send`) sync primitives: `oneshot`, `mpsc`, `Mutex`, `RwLock`,
  `Notify`, `Semaphore`.
- A cancellation token (cancel a tree of work from one place).
- `Builder::on_unobserved_panic` (a hook instead of crashing), and
  `Runtime::try_block_on` returning `Result<T, Stalled>`.
- `MissedTickBehavior`-style options on `interval()`, if the D7 default
  turns out not to fit some uses.

### 9.4 Threads in WASM
- Browser builds with atomics and shared memory (`-C target-feature=+atomics`)
  could support remote wakes from web workers, through the same path as
  native threads.

### 9.5 Other
- Tracing hooks for scheduling events.
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
measured improvement in benchmarks before it merges. It runs after M10, so
that the hot paths are the ones every target shares.

---

## 10. Open questions

- **Publishing (deferred).** We haven't decided whether to publish on
  crates.io. Until then the package is named `spool` locally, with
  `publish = false` in `Cargo.toml`. If we do publish and `spool` is taken,
  only the package name changes (for example `spool-rt`); the library can stay
  `spool` through `[lib] name`.
- **Public `step()` (decide in M8).** The step function could stay internal
  to the drivers, or be public so embedders (a game loop, a GUI event loop)
  can drive Spool themselves. Public is more useful but commits to the
  `Step` type.
- **Browser bindings (decide in M9).** `wasm-bindgen` + `js-sys` is the
  standard choice. Hand-written imports would avoid the dependency but cost
  more to maintain.
- **WASI driver (decide in M10).** Whether the native driver works unchanged
  on `wasm32-wasip1`, or WASI needs its own sleep-based driver because
  `std::thread::park` isn't supported there.

---

## 11. Decisions

### 11.1 Resolved in review (v1–v3)

| Topic | Decision |
|---|---|
| Entry points | Both `run` (waits for an empty loop) and `block_on` |
| Stalled main | Panic with a diagnostic message (D8); `try_block_on` later if needed |
| `set_interval` | Callback `set_interval` + future `interval()` |
| Delay clamp | None (D6) |
| JoinHandle drop | Detach; `abort()` / `abort_on_drop()` to cancel (D10) |
| Cancellation | Idiomatic: futures cancel on drop, handles cancel by method; no `clear_*` functions |
| `unsafe` | None until the performance phase (§9.6) |
| Panic model | Re-throw into the awaiter, `try_join` for `Result`, unobserved panics crash `run` (§6) |
| Microtask cancellation | Not needed; `queue_microtask` returns `()` |
| Dev-dependencies | `proptest`, `trybuild`, `static_assertions` are approved (test-only) |
| Toolchain | Edition 2024; the MSRV is the latest stable (1.98 at time of writing), raised freely |
| Publishing | Undecided; `publish = false` for now (§10) |

### 11.2 Changed in v4

| Topic | v1–v3 | v4 |
|---|---|---|
| Positioning | Node.js event-loop semantics | Inspired by the JS event loop; exact Node or JS parity is a non-goal (§1) |
| Targets | Native; WASM as future work | Native, browser, and WASI are all goals (§7, M8–M10) |
| `next_tick` | A tick queue drained before microtasks, as in Node | Removed (D12) |
| Intervals | Re-arm from the timers phase's start time (drifts, like Node) | Re-arm from the previous deadline; skip missed ticks (D7) |
| Correctness oracle | Real Node, required in CI, plus a Node fuzzer | The reference model (D15); a small optional Node comparison |
| Loop structure | One blocking `drive()` | A non-blocking `step()` plus per-target drivers (D5) |
| Default clock | Always `SystemClock` | Per target (D13) |
| Naming | snake_case JS names | JS names where clearest, with Spool's own documented semantics; Rust names for new APIs (D1) |
| `set_immediate` | Future work, with a check phase | Dropped; a "yield to timers" helper may replace it (§9.3) |
| JS-specific extras (`AbortController`, `unhandledRejection`) | Future work, JS-shaped | Rust-shaped equivalents, judged on merit (§9.3) |

---

## 12. Milestones

| # | Milestone | Status | Contents |
|---|---|---|---|
| M0 | Scaffold | done | Crate, CI matrix, lint/fmt/doc gates, test harness skeleton |
| M1 | Core loop | done | `Runtime`, `block_on`, context, task slab, `Wake`-based waker, microtask queue, `spawn`, `JoinHandle`, `yield_now` |
| M2 | Queues | done | `queue_microtask`, `next_tick`, drain semantics, reference model + `proptest`, basic `run` |
| M3 | Timers | done | `Clock`, `MockClock`, timer heap, `sleep`, `sleep_until`, `timeout`, `timeout_at`, `set_timeout`, `set_interval`, `interval`, `TimerHandle::cancel` |
| M4 | Cancellation & panics | done | `abort`, `AbortHandle`, `abort_on_drop`, `try_join`, the panic model, poisoning |
| M5 | Liveness & threads | done | `KeepAlive`, stall-detection hardening, remote wakes, the Node fuzzer |
| M6 | Polish | done | Rustdoc guide, examples, stress tests, benchmarks (§9.6 baseline), coverage gate |
| M7 | Realign with v4 | | Remove `next_tick` and the tick queue (D12), from the API, the model, tests, docs, and examples. New interval semantics (D7) for `set_interval` and `interval()`, with model and timer tests for skipped ticks. Extend the reference model with intervals and timer cancellation (D15), then remove the Node fuzzer; trim `node_parity` to the optional comparison and drop `SPOOL_REQUIRE_NODE` and the Node install from CI. Reword the crate docs, README, and examples from "matches Node" to "inspired by JS". Update the "§8.2 scenario N" references in the tests to the v4 numbering. |
| M8 | Non-blocking core | | Split `drive()` into `step()` (D5) and a native driver; make main a resumable task; per-target default clock (D13); per-target panic docs (§6); `cargo check` for `wasm32-unknown-unknown` and `wasm32-wasip1` in CI. Settle the public-`step()` question (§10). No behavior change on native: the full suite must pass unchanged. |
| M9 | Browser | | Feature `browser`: `PerformanceClock`, the host-callback driver (D14), `browser::start`, an example page, and the suite (minus threads and panics) under `wasm-bindgen-test` in a headless browser in CI. |
| M10 | WASI | | Make the suite (minus threads) pass under wasmtime on `wasm32-wasip1` (and `-wasip2` if it is no extra work), adding a WASI driver only if the native one doesn't work there; add it to CI. |
| M11 | Performance | | §9.6, measured against `BENCHMARKS.md`. |
