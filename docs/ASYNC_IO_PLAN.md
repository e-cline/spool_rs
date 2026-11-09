# Spooly — Async I/O Design & Plan

> Status: **v2, reviewed.** M11–M16 are built; see §12. This document
> expands §9.1 and §9.2 of [`INITIAL_PLAN.md`](INITIAL_PLAN.md) into a
> design and milestones. Decisions are tagged `IO#` so they don't collide
> with the core plan's `D#` numbers. v2 settles draft v1's open questions
> (§11); they stay open to revisiting.

Spooly runs tasks and timers today, but a task can only wait on another
task, a timer, or a wake from another thread. This plan adds **network I/O**
(TCP, UDP, Unix sockets) and **file I/O** on native targets, without
changing what Spooly is: one thread, no `Send` bounds, documented ordering,
and no runtime dependencies in the core. It also keeps WASI 0.2 (and, where
it makes sense, the browser) possible later.

---

## 1. Goals and non-goals

### Goals

- Async **TCP, UDP and Unix-domain sockets** on Linux, macOS and Windows.
- Async **file system** operations on the same targets.
- **`spawn_blocking`**: run a blocking closure on a thread pool and await its
  result from a `!Send` task.
- I/O types are **`!Send`**, owned by one runtime, and work with `Rc`/`RefCell`
  code like everything else in Spooly.
- The standard ecosystem traits: **`futures-io`** `AsyncRead`/`AsyncWrite`
  (and `AsyncBufRead` where it applies), so crates such as `futures-rustls`
  or `async-tungstenite` can be used. (Tokio's traits are punted, IO14.)
- **Predictable place in the loop**: I/O readiness has a defined phase in
  `step()` and wakes tasks as microtasks, like everything else (§4).
- **The core stays dependency-free on native targets.** I/O is opt-in.
- **WASM later, without a redesign**: the core's extension point (§5) must be
  implementable by a WASI 0.2 backend that has no threads and no `mio`.
  *(As built in M16, WASI uses `mio` too, §8.2; the shape still allows a
  backend without it.)*

### Non-goals (for now)

- Raw sockets, `TcpStream` from a browser, or any I/O on
  `wasm32-unknown-unknown` beyond what JS already offers (§8.3).
- A completion-based (`io_uring`, IOCP-completion) backend. It stays possible
  (§10), but the first backend is readiness-based.
- Processes, signals, TTYs, `stdin`/`stdout`. They fit the same design (§10)
  and come after the basics.
- TLS, HTTP, DNS beyond `std`'s resolver. Those are ecosystem crates.
- Multi-threaded I/O. Each runtime has its own reactor on its own thread.

---

## 2. Summary of the recommendation

| Question | Recommendation |
|---|---|
| Library | **`mio` 1.x** for the native reactor, hidden behind a small internal trait so it could be swapped (§3) |
| File I/O | A **small, self-written blocking thread pool** in the core (`spooly::spawn_blocking`, IO17), completing over the existing remote-wake path (§6) |
| Crate split | **Yes**: a Cargo workspace with `spooly` (the core, unchanged name) and a new **`spooly-io`** crate. The core gains one narrow, public extension point for I/O drivers (§5, §7) |
| Loop change | An **I/O phase** after the timers phase, and drivers that wait on the reactor instead of parking the thread (§4) |
| WASM | WASI 0.2 (the only WASI version supported): the same `mio` reactor, which runs there on wasi-libc's `poll` over `wasi:io/poll` (M16, IO15). Browser: no reactor; JS APIs are already awaitable (§8) |

---

## 3. Libraries evaluated

The I/O stack has three layers, and each can be bought or built:

1. **Readiness polling**: epoll (Linux), kqueue (macOS/BSD), and something
   for Windows, where sockets have no readiness API of their own (the
   `wepoll`/AFD technique fills that gap).
2. **Non-blocking socket types** on top of it.
3. **Async types** (`TcpStream` with `AsyncRead`, etc.) tied to an executor.

Layer 3 is always ours: it's where Spooly's `!Send`, single-runtime model
lives. The question is layers 1 and 2.

### 3.1 Candidates

| Crate | What it is | Fit for Spooly |
|---|---|---|
| **`mio`** (Tokio project) | Layers 1 + 2: `Poll`, `Registry`, `Token`s, edge-triggered events; `mio::net::{TcpStream, TcpListener, UdpSocket, UnixStream, …}`; `mio::Waker` to interrupt a poll from another thread. epoll, kqueue, Windows via AFD, and `wasm32-wasip1` (sockets, limited). | **Good.** Very mature (under Tokio), small dependency tree (`libc` / `windows-sys`; `log` is optional), executor-agnostic, and needs no `unsafe` from us. It has no timers, so it doesn't compete with Spooly's. |
| **`polling`** (smol project) | Layer 1 only: a `Poller` with oneshot interest and `notify()`. More backends (event ports, `poll()` fallback, some embedded/RTOS targets). | **Good**, but we'd also build layer 2 ourselves from `std::net` + `set_nonblocking`, and oneshot mode costs a re-arm syscall per wait. Pulls in `rustix` and (in recent versions) `tracing`; to be re-checked at implementation time. A solid fallback if `mio` ever doesn't fit. |
| **`async-io`** (smol) | Layers 1–3: a global reactor with its own timers, `Async<T>`, and a background "async-io" thread that drives it. | **Poor.** It brings a second timer system and a helper thread, and ordering would no longer be Spooly's. Its `Async<T>` adapter is a design worth copying (§7.3), not depending on. |
| **`tokio`** (`net` feature) | The full stack, bound to Tokio's runtime. | **No.** Its I/O types need a Tokio runtime context. Its `AsyncRead`/`AsyncWrite` traits could be implemented behind a feature, but that is punted (IO14). |
| **`rustix` / `libc` directly** | Write our own epoll/kqueue/AFD layer. | **Not now.** epoll and kqueue are a few hundred lines each, but the Windows AFD path is subtle and needs `unsafe`, which the core plan rules out until §9.6. Revisit only if `mio` becomes a problem. |
| **`io-uring`, `tokio-uring`, `compio`** | Completion-based I/O (Linux `io_uring`; `compio` also IOCP). | **Later, optional.** Completion I/O needs owned buffers (the kernel writes into them after the call returns), which doesn't fit `AsyncRead`'s `&mut [u8]` and complicates cancellation. See §10. |
| **`blocking`** (smol) | A thread pool for blocking work. | **Not needed.** It pulls in `async-channel`, `async-task` and more; the pool we need is ~150 lines of `std` (§6). |
| **`socket2`** | Socket options `std` lacks (`SO_REUSEPORT`, keepalive tuning, …). | Optional, later, as a `spooly-io` feature. `std` + `mio` cover the basics. |
| **`futures-io`** | Just the `AsyncRead`/`AsyncWrite`/`AsyncBufRead`/`AsyncSeek` traits. | **Yes.** Tiny, no dependencies of note, the common denominator of the executor-agnostic ecosystem. |
| **`wasi`** (Bytecode Alliance) | Generated bindings for WASI 0.2 interfaces (`wasi:io/poll`, `wasi:sockets`, `wasi:filesystem`). | Planned for a WASI backend; **not needed** after all (M16, IO15): `mio` runs on WASI 0.2 (§8.2). |

### 3.2 Decision

**IO1. Use `mio` for the native reactor.** It gives us both readiness polling
and the non-blocking socket types, on all three desktop OSes, maintained by
the team that depends on it most. It has no opinions about executors or
timers, and wraps cleanly without `unsafe`.

The reactor is still written against a small **internal** trait (§5.3), so
that a `polling`-based or hand-written backend can replace `mio` in one
module, and so that the WASI backend has the same shape.

---

## 4. Where I/O fits in the loop

### 4.1 Today

```
step():
    drain microtasks
    absorb remote wakes → Busy
    timers phase (snapshot of due timers, drain after each) → Busy if any fired
    decide: WaitUntil(next timer) | WaitForWake | Idle
driver: Busy → step again; WaitUntil → park_timeout; WaitForWake → park; Idle → exit / stall panic
```

### 4.2 Proposed

**IO2. An I/O phase runs after the timers phase, in every step.**

```
step():
    drain microtasks
    absorb remote wakes → Busy
    timers phase
    I/O phase:                                     // new
        events = io.poll_ready()                   // never blocks
        for each ready registration: wake its waiting task(s)   // → microtasks
        drain microtasks
    if timers fired or I/O woke anything → Busy
    decide:
        armed timers            → WaitUntil(next deadline)
        io.is_active()          → WaitForWake          // new: waiting on I/O keeps the loop alive
        KeepAlive / outside wakers → WaitForWake
        otherwise               → Idle

native driver:
    WaitUntil(t)  → io.wait(Some(t - now))   // was: park_timeout
    WaitForWake   → io.wait(None)            // was: park
```

Details:

- **The I/O phase runs even when timers fired.** Today a step that fired a
  timer returns `Busy` right away. If the I/O phase came after that return, a
  timer that re-arms itself with delay 0 would starve I/O forever. Both
  phases run, then the step returns `Busy`.
- **Readiness is a wake, not a callback.** A ready socket wakes the task that
  is waiting on it, which queues a `Poll(id)` microtask (D3). The task's own
  code then does the read or write. So I/O adds no new kind of queue entry,
  and everything in §3 of the core plan (FIFO microtasks, draining, D4b) still
  holds.
- **Order within the phase.** Wakes happen in the order the OS reports the
  events. That order is not specified, and Spooly doesn't promise one. What
  is specified is the *phase*: readiness observed in a step is delivered
  after that step's timers, and before the next step's.
- **Waiting and dispatching are separate.** `io.wait(timeout)` only blocks
  until something is ready (or the timeout, or a remote wake). It records the
  events but wakes nothing. The next step's I/O phase dispatches them. That
  keeps the phase the only place I/O wakes tasks, whichever driver waited.
- **Bounded work.** One I/O phase dispatches at most one batch of events (the
  `mio::Events` capacity, e.g. 1024). The rest are seen by the next step.
- **Remote wakes interrupt the wait.** Today a remote wake calls
  `thread.unpark()`. With an I/O driver it must wake the reactor instead
  (`mio::Waker`). This becomes the driver's **unparker** (§5).
- **No driver, no change.** Without an I/O driver the step is exactly
  today's, so the reference model (D15), the browser driver, and all existing
  tests are unaffected.
- **Cost.** A busy loop with an I/O driver does one non-blocking
  `epoll_wait`/`kevent` per step. The phase skips the syscall when nothing is
  registered. If benchmarks show it matters, the phase can poll only when
  the previous step did no other work, at the price of later I/O delivery
  under load. Measure first (§9.6 of the core plan).

### 4.3 Liveness and stalls

**IO3. A task waiting on I/O keeps the loop alive; an idle socket does not.**

The core rule is "a pending task doesn't keep the loop alive on its own:
something has to be able to wake it" (§3.5). The reactor *can* wake a task
that is waiting for readiness, so:

- `io.is_active()` is `true` while any registration has a stored waker (some
  task is waiting to read, write, accept, …). (A `spawn_blocking` job keeps
  the loop alive through its own `KeepAlive`, in the core: §6.)
- A socket that exists but that nobody is awaiting does not keep `run`
  alive. A detached accept loop *does* (it is always waiting on `accept`),
  which is what a server wants.
- `unref()` on a socket (and on a timer, §9.3 of the core plan) makes its
  waits not count, for background work that shouldn't hold `run` open.
- The stall check (D8) is unchanged: wakers held by the reactor show up as
  outside wakers, so the check stays conservative, and `is_active()` returns
  `WaitForWake` before the check even runs.

### 4.4 `MockClock` and real I/O

When the loop would wait for a timer, the native driver lets a `MockClock`
jump to the deadline. With real sockets that would make every `timeout`
fire before the network could answer. **IO4.** With an I/O driver, the driver
first does a zero-timeout `io.wait`, and only lets the clock jump if nothing
became ready. (This is what Tokio's paused time does.) Tests that need
deterministic I/O should use the in-memory pipes from §7.5, not loopback
sockets.

### 4.5 Starvation

A task that loops on a socket with data always available never returns
`Pending`, and so never yields. This is the same as a task that never awaits
(§3.6 of the core plan): documented, not prevented. **IO5.** v1 has no
cooperative budget. If real programs hit this, a per-task budget (Tokio's
`coop`) can be added in `spooly-io` alone: after N ready operations in one
poll, an I/O future returns `Pending` and wakes itself.

---

## 5. The core's extension point

### 5.1 Why the core needs one

To support I/O, the core has to let something else (1) take over waiting,
(2) receive remote wakes, (3) run in the I/O phase, and (4) report liveness.
Everything else (sockets, files, the reactor itself) can live outside.

### 5.2 Shape

**IO6. A public `IoDriver` trait in the core**, in a new module
`spooly::driver`:

```rust
/// Something the event loop can wait on besides timers (an I/O reactor).
/// Installed with `Builder::io_driver`; one per runtime, `!Send`.
pub trait IoDriver: Any {
    /// The I/O phase (IO2): wake every task whose I/O is ready, without
    /// blocking. Returns `true` if it woke anything.
    fn poll_ready(&self) -> bool;

    /// Blocks until some I/O is ready, `timeout` passes, or the unparker is
    /// called. Records what's ready for the next `poll_ready`; wakes
    /// nothing. Returns `true` if I/O became ready. Only drivers that may
    /// block (the native one) call this.
    fn wait(&self, timeout: Option<Duration>) -> bool;

    /// A handle another thread can call to end a `wait` early (remote wakes,
    /// the last `KeepAlive` dropping).
    fn unparker(&self) -> Arc<dyn Fn() + Send + Sync>;

    /// `true` while a task is waiting on this driver (IO3).
    fn is_active(&self) -> bool;
}

impl Builder {
    pub fn io_driver(self, driver: impl IoDriver) -> Self;
}

/// The current runtime's I/O driver, if it has one of type `D`.
/// (How `spooly-io` types find their reactor.)
pub fn current_io_driver<D: IoDriver>() -> Option<Rc<D>>;
```

Inside the core:

- `Shared::thread: Thread` becomes an unparker: the thread's `unpark` by
  default, or the driver's `unparker()`. `push_remote` and
  `release_keep_alive` call it.
- `Inner` holds `Option<Rc<dyn IoDriver>>`. `IoDriver: Any`, so
  `current_io_driver` upcasts it to `Rc<dyn Any>` and downcasts, with no
  `unsafe`.
- `step()` gains the I/O phase and the `is_active()` check (§4.2).
- The native driver calls `io.wait(..)` instead of parking when a driver is
  installed (IO4 for `MockClock`).
- The browser driver ignores `wait` (it never blocks), but still runs
  `poll_ready` through the step. A browser runtime with an I/O driver isn't a
  goal, but nothing forbids it.

*As built (M12):* `wait` returns whether I/O became ready. IO4 needs that
signal: without it, the native driver couldn't tell a zero-timeout wait that
found I/O from one that found nothing, and would alternate between the two
and an empty step forever instead of letting a `MockClock` jump. The
remote-wake path holds an `Unpark` (the thread, or the driver's unparker).
`tests/io_driver.rs` checks the hook with a fake reactor whose `wait`
blocks on a condition variable.

This is about 100 lines in the core, all safe, and it's the same hook an
embedder needs to fold Spooly into a GUI or game loop: it moves the "public
`step()`" question in §10 of the core plan a step closer.

### 5.3 Inside `spooly-io`

`spooly-io` implements `IoDriver` once per backend, all behind one internal
`Reactor` shape:

```rust
struct Reactor {                     // !Send, one per runtime
    poll: mio::Poll,                 // on WASI too (M16, §8.2)
    events: RefCell<mio::Events>,    // filled by wait, drained by poll_ready
    sources: RefCell<Slab<ScheduledIo>>,   // reuses the core's generational slab idea
    unparker: Arc<mio::Waker>,
    blocking: BlockingPool,          // §6, for liveness
}

struct ScheduledIo {                 // one per registered socket
    readiness: Cell<Ready>,          // edge-triggered bits, cleared on WouldBlock
    reader: RefCell<Option<Waker>>,  // one waiting reader...
    writer: RefCell<Option<Waker>>,  // ...and one waiting writer
    unref: Cell<bool>,
}
```

- `mio` is edge-triggered, so readiness bits are cached per source and
  cleared when an operation returns `WouldBlock`, as in Tokio. An operation
  tries the syscall first and only registers its waker when it would block.
- **One waker per direction.** Two tasks reading the same socket at once
  overwrite each other's waker. That is a misuse (data would interleave
  anyway); it's documented, and the normal pattern is one reader task and one
  writer task, which `split()` (§7.2) supports.
- `Token`s carry the slab key plus generation, so an event for a closed and
  reused slot is ignored, like a stale remote wake in the core.

---

## 6. File I/O and `spawn_blocking`

Regular files are always "ready" to epoll and kqueue, so readiness doesn't
help. **IO7. File operations run on a blocking thread pool**, as Tokio and
smol do.

```rust
// In the core: spooly::spawn_blocking (IO17).
pub fn spawn_blocking<F, T>(f: F) -> BlockingHandle<T>
where F: FnOnce() -> T + Send + 'static, T: Send + 'static;
```

- `BlockingHandle<T>` awaits to `T`. Dropping it detaches the job.
  `cancel()` stops a job that hasn't started (and drops its closure at
  once); nothing can stop one that is running.
- The job's result goes into an `Arc<Mutex<…>>` slot and its waker is woken
  from the pool thread: the **existing remote-wake path** (§4.3 of the core
  plan), which wakes the reactor through the unparker when there is one. No
  new mechanism.
- Each job holds a `KeepAlive` until it is done or cancelled, so `run`
  waits for it, even detached (a dropped `File`'s last write finishes before
  `run` returns, as a Node `fs` call without a callback would).
- **IO19.** A `MockClock` doesn't jump while a job is queued or running:
  the loop waits for it in real time instead, as Tokio's paused time does,
  so `timeout(d, spawn_blocking(…))` is deterministic in tests.
- The pool (IO18): one per runtime, created on the first `spawn_blocking`.
  `std::thread` workers, a `Mutex<VecDeque>` + `Condvar`, at most 64
  threads by default (`Builder::max_blocking_threads`, IO13), workers
  spawned lazily and exiting after 10 s idle
  (`Builder::blocking_idle_timeout`). Dropping the runtime drops the jobs
  that haven't started; running ones finish on their own. It needs `std`
  threads, so it doesn't exist on WASM.
- Panics in a job: no catching (D11). The pool catches the unwind only to
  keep its worker alive, then re-raises it in the awaiting task when it polls
  the handle (`resume_unwind`), so the panic surfaces on the runtime's thread
  and unwinds out of `run`, as a task's panic would. A detached job's panic
  is printed by the panic hook and otherwise lost. (IO12.)

On top of that, `spooly_io::fs` mirrors `std::fs`:

- **Whole-file and metadata functions** (`read`, `read_to_string`, `write`,
  `metadata`, `create_dir_all`, `remove_file`, `rename`, `read_dir`, …): one
  `spawn_blocking` each.
- **`File`** implementing `AsyncRead`, `AsyncWrite`, `AsyncSeek`: like
  Tokio's, it holds the `std::fs::File` in an `Arc` and keeps at most one
  operation in flight, copying through an internal buffer. Writes are
  buffered, so `flush().await` (or `sync_all`) is needed before relying on
  them; dropping a `File` with a write in flight lets that write finish in
  the background.
- DNS (`TcpStream::connect("host:port")`, `lookup_host`) also uses the pool,
  since `std`'s resolver blocks.

### 6.1 Where the pool lives (IO17)

The first draft put the pool in `spooly-io`. It moved to the core, as
`spooly::spawn_blocking`, because it depends on nothing I/O-specific:

- **What it needs is already core API.** A job completes through a `Send`
  waker (the remote-wake path) and holds a `KeepAlive`, both public in the
  core. It never touches the `IoDriver`. In `spooly-io` it would have
  needed a `Driver` for no reason.
- **No new dependencies.** It is `std` threads, a mutex and a condvar
  (~350 lines), so the core stays dependency-free on native targets: the
  reason for the split (§9.1) was `mio` and its platform crates, not this.
- **Useful without I/O.** CPU-heavy work and blocking libraries (a SQLite
  binding, compression) are a common need for a single-threaded executor,
  with no sockets involved, like Node's libuv thread pool.
- **The loop's rules belong to the core.** Liveness (`KeepAlive`), panics
  (IO12, D11), and the `MockClock` rule (IO19) are core semantics; the core
  is where they are tested (with Miri), not through a second crate.

The costs: the core gains process-visible threads (but tasks still never
leave the runtime's thread), and two `Builder` options. On WASM it is
compiled out. `spooly_io::fs` and `lookup_host` build on it, so they need a
Spooly runtime but not a `Driver`: plain `spooly::run` works.

*As built (M14):*

- `spooly::spawn_blocking`, `BlockingHandle` (`cancel`, `is_finished`),
  `Builder::max_blocking_threads` (panics on 0) and
  `Builder::blocking_idle_timeout`; all `cfg(not(target_family = "wasm"))`.
- The handle isn't forced `!Send` (the first draft said it would be): it
  is `Send` when `T` is, which costs nothing, since it only holds the
  shared slot.
- Polling a handle whose job was dropped unrun (its runtime shut down)
  panics with a message, rather than staying pending forever.
- `spooly_io::net::ToSocketAddrs` replaces `std`'s in every signature: a
  sealed trait for the same input types, which resolves IP addresses on the
  spot and sends only host names to the pool (`std`'s trait can't be used
  there: a borrowed `&str` can't move to another thread). `&String` is not
  implemented (`.as_str()` is); `&T` is, for `Copy` address types.
- `lookup_host` returns `impl Iterator<Item = SocketAddr> + Debug`.
- `fs`: the free functions, `try_exists`, `read_dir` (entries fetched 32
  at a time; `next_entry` and `poll_next_entry`), `DirEntry` (async
  `metadata` / `file_type`), `File` and `OpenOptions` (`From` a
  `std::fs::OpenOptions`, for platform options such as a Unix `mode`).
- `File` keeps the caller's position exact: data read ahead but not handed
  out (only possible when a read is abandoned and retried smaller) is
  accounted for by the next seek, write, `set_len` and `into_std`.
  A write-behind error is returned once, by the next write or flush.

---

## 7. `spooly-io` API

### 7.1 Setup

```rust
// The simple way: a runtime with an I/O driver, then run.
spooly_io::run(async { /* … */ });
spooly_io::block_on(async { /* … */ });

// Or explicitly, to combine with a MockClock or other builder options.
let rt = spooly::Runtime::builder()
    .io_driver(spooly_io::Driver::new()?)
    .build();
rt.run(async { /* … */ });
```

Using a `spooly-io` type on a runtime without a driver panics with a clear
message ("this runtime has no I/O driver; use `spooly_io::run` or
`Builder::io_driver`"), like calling `spawn` outside a runtime does today.

### 7.2 Networking (`spooly_io::net`)

```rust
TcpListener::bind(addr).await? ; listener.accept().await? -> (TcpStream, SocketAddr)
TcpStream::connect(addr).await? ; stream.halves() -> (ReadHalf<'_>, WriteHalf<'_>)
UdpSocket::bind(addr).await? ; send_to / recv_from / connect / send / recv
#[cfg(unix)] UnixListener, UnixStream, UnixDatagram
lookup_host("example.com:80").await?
```

- All `!Send`, tied to the runtime they were created on.
- `TcpStream`, `UnixStream`: `futures_io::AsyncRead + AsyncWrite`, also on
  `&TcpStream` (as `std` implements `Read for &TcpStream`), which makes
  `split()` a pair of borrows, and `into_split()` a pair of `Rc` halves. On
  one thread, neither needs a lock.
- `readable().await` / `writable().await` and `try_read` / `try_write`, for
  code that wants readiness directly.
- `from_std(std::net::TcpStream)` / `into_std()`, for sockets made
  elsewhere (`socket2`, systemd socket activation, …).
- Socket options through `std` (`set_nodelay`, `set_ttl`, …).
- Cancellation (core §5): dropping an I/O future is always safe and loses
  no data for reads and `accept` (nothing was consumed). A partly done
  `write_all` stops at an unknown point, as everywhere in Rust async.
  Dropping a socket deregisters it and closes it.

*As built (M13):*

- `TcpListener::bind`, `TcpStream::connect`, `UdpSocket::bind`,
  `UdpSocket::connect` and `send_to` are `async` and take `impl
  ToSocketAddrs`. Until M14 they resolved with `std`, which blocked the
  thread for a host name; since M14 they take `spooly_io::net::ToSocketAddrs`
  and resolve names on the pool (§6, *As built (M14)*).
- `UnixListener::bind`, `UnixDatagram::bind` and `connect` are plain
  functions (nothing to resolve); `UnixStream::connect` is `async`.
- `poll_accept`, `poll_send_to` and `poll_recv_from` are public, for
  hand-written futures and streams. `into_std` returns a socket still in
  non-blocking mode.
- The halves (`ReadHalf`, `WriteHalf`, `OwnedReadHalf`, `OwnedWriteHalf`)
  are generic over the stream, shared by TCP and Unix streams. The
  borrowing split is `halves(&self)`, not `split`: method resolution
  prefers `futures::AsyncReadExt::split(self)` (by value) over an inherent
  `split(&self)`, so with that trait in scope `stream.split()` would
  silently call it instead.
  `OwnedReadHalf::reunite` puts a stream back together.
- A wait cancelled while pending (a read inside an expired `timeout`)
  leaves its waker behind: it keeps counting toward liveness (IO3) until
  the socket is next ready, completes an operation, or is dropped. Clearing
  it on cancellation would need every I/O future to be a named type with a
  `Drop`, which `futures-io`'s poll-based traits can't offer.
- The reactor doesn't use `mio`'s `log` feature, so `spooly-io` adds only
  `mio` (with `libc` or `windows-sys`) and `futures-io`.
- Miri can't run real sockets, so CI runs Miri on the core only.

### 7.3 Arbitrary file descriptors

`spooly_io::Async<T: AsFd>` (Unix) and `Async<T: AsSocket>` (Windows):
register any non-blocking fd/socket with the reactor and get
`readable()`/`writable()` plus `AsyncRead`/`AsyncWrite` when `T: Read +
Write`. Modeled on smol's `async-io`. It covers pipes, `eventfd`,
third-party sockets and so on. (Later milestone, M15.)

*As built (M15):*

- **Unix only (IO20).** `mio` registers an arbitrary descriptor on Unix
  (`SourceFd`, its `os-ext` feature), but on Windows only its own socket
  types: smol supports Windows here through `polling`, a different
  reactor. Third-party sockets on Windows convert to `std::net` types and
  use `from_std`.
- **The caller makes the descriptor non-blocking (IO22).** Setting
  `O_NONBLOCK` on an arbitrary `AsFd` needs `fcntl`, so `unsafe` code
  (forbidden in the workspace) or a new dependency (`rustix`); `Async::new`
  documents the requirement instead. A blocking descriptor blocks the
  runtime's thread, but is not unsound.
- API: `new`, `get_ref` / `get_mut` / `into_inner`, `readable` /
  `writable` (and `poll_` forms), `read_with` / `write_with` (and `_mut`
  forms for `&mut T`), `unref`; `AsyncRead` / `AsyncWrite` for `Async<T>`
  when `T: Read` / `Write`, and for `&Async<T>` when `&T` is; `AsFd`.
  `Async<T>` is `Unpin` for every `T` (nothing in it is pinned).

### 7.4 Features of `spooly-io`

| Feature | Default | Contents |
|---|---|---|
| `net` | yes | TCP, UDP, Unix sockets, `lookup_host` |
| `fs` | yes | `spooly_io::fs` |
| ~~`socket2`~~ | — | Dropped in M15 (IO21): see below |

(`spawn_blocking` and the pool are in the core since M14, IO17. None of
these features exist yet: everything is always built.)

*As built (M15):* instead of a `socket2` feature, every socket type
implements the standard OS-handle traits (`AsFd` + `AsRawFd` on Unix,
`AsSocket` + `AsRawSocket` on Windows). Then `socket2::SockRef::from(&s)`
sets any option, with no dependency of ours, and a socket configured
before use (options before `bind`, say) is built with `socket2` and handed
over through `from_std`. `tests/socket2.rs` does both. (A `TcpSocket`-like
builder for options before an *async* `connect` is left for later.)

### 7.5 Testing utilities

`spooly_io::test::{duplex, pipe}`: in-memory, `!Send` byte streams that
implement the same traits, with a configurable buffer size. They involve no
reactor and no OS: a write wakes the reader as an ordinary local wake. They
give deterministic tests of protocol code (with `MockClock`), and let the
reference model (D15) grow I/O-like operations later without real sockets.

*As built (M15):* `pipe(capacity) -> (PipeWriter, PipeReader)` and
`duplex(capacity) -> (DuplexStream, DuplexStream)`, a duplex being two
pipes (`into_split` gives its halves). Closing a writer ends the stream
for its reader (which can still write back on a duplex); dropping a reader
makes writes fail with `BrokenPipe`. A zero capacity panics. The module is
built on every target, WASM included, and needs no `Driver`. The
reference-model operations were not done (optional); they remain possible
later.

---

## 8. WASM

### 8.1 What the core already gives

The core runs on `wasm32-unknown-unknown` (browser driver, M9) and on
`wasm32-wasip2` (M10: `run` and `block_on`, with the core's suite under
wasmtime in CI). `IoDriver` (§5) is designed so that a backend with **no
threads** and **no `mio`** fits it: `unparker` can be a no-op, `wait` can
block on the host's poll call.

On WASI the core's parking driver, without an I/O driver, sleeps until the
next timer, and panics if it would wait for a wake with no timer due,
since nothing could deliver one (core plan D16). With an I/O driver it
calls `IoDriver::wait` instead, as natively, so the backend below takes
over both: `wait(None)` blocks on its pollables alone, and is the only way
a WASI program can wait without a timer.

### 8.2 WASI 0.2 (`wasm32-wasip2`)

WASI 0.2 has exactly the readiness primitive we need: every stream, socket
and timer exposes a `pollable`, and `wasi:io/poll.poll(list)` blocks until
some are ready. *(Draft v1 and v2 planned a second backend on it, with the
`wasi` crate's bindings: `ScheduledIo` holding a `Pollable`, and `wait`
polling them plus a `monotonic-clock` pollable. M16 found that
unnecessary.)*

**As built (M16): the same `mio` reactor (IO15).** `mio` 1.x supports
`wasm32-wasip2` through its Unix backend: its `Poll` is wasi-libc's
`poll()`, which wasi-libc implements on `wasi:io/poll` over the sockets'
pollables, and `mio::net`'s TCP and UDP types are wasi-libc's sockets over
`wasi:sockets`. So `Driver`, `ScheduledIo`, `net` and the readiness logic
are unchanged; the differences are small and local:

- **Unparker.** `mio` has no `Waker` on WASI. None is needed: with no
  threads, nothing can run while the driver waits, so the unparker does
  nothing there (`reactor.rs`, `new_unparker`).
- **Blocking work runs inline.** There is no pool (core plan D16), so the
  work `spooly-io` sends to `spawn_blocking` natively, every `fs` operation
  and host-name lookups, runs on the runtime's thread instead, and the loop
  waits for it. One internal helper, `blocking::spawn`, is
  `spooly::spawn_blocking` where there are threads and runs the closure at
  once on WASM (returning a ready future), so `fs` and `net` have no `cfg`s
  for it. File operations go through `wasi:filesystem` (via `std::fs`),
  which hosts implement synchronously anyway.
- **Gaps in wasi-libc's socket emulation.** `peek` / `peek_from`
  (`MSG_PEEK`), and some options (`SO_BROADCAST`, `SO_REUSEADDR`,
  `SO_LINGER`) fail with an error; `writev` sends only its first buffer (a
  legal partial write). `fs::set_permissions` is unsupported too. The crate
  docs list these; the tests that use them are ignored on WASI.
- **Host permissions.** The host grants network and file access: the test
  runner (`.cargo/config.toml`) runs `wasmtime run -S inherit-network
  -S allow-ip-name-lookup --dir /tmp`.

Not done: `Async<T>` (Unix-only, IO20), and a non-blocking host-name
lookup. `wasi:sockets/ip-name-lookup` has its own pollable, but `mio` can't
wait on a raw pollable, so using it would mean the `wasi` crate plus a way
to poll it alongside `mio` (or the hand-written backend after all). Worth
it only if lookups on WASI prove slow in practice.

- **WASI 0.3** adds native async to the component model (`future`/`stream`
  types and an async ABI). That could replace the pollable backend later; it
  doesn't change the `IoDriver` shape.
- **WASI 0.1** (`wasm32-wasip1`): **not supported** (decided in M10, core
  plan §11.4), by the core or by `spooly-io`. `mio` supports it, but 0.1
  sockets can't connect, only accept on preopened listeners.

### 8.3 Browser (`wasm32-unknown-unknown`)

There is no socket or file-descriptor API, so there's nothing for a reactor
to poll. Network and storage are JS APIs (`fetch`, `WebSocket`,
`WebTransport`, OPFS, IndexedDB) that return promises or fire events, and a
`JsFuture` already works in any Spooly task (D14). So:

- **IO8. `spooly-io` has no browser backend.** It compiles there with only
  the portable pieces (traits, `test::duplex`), so libraries generic over
  `AsyncRead`/`AsyncWrite` still build.
- A later, separate crate (or a `web` feature) could wrap `WebSocket` or
  `fetch` bodies as `AsyncRead`/`AsyncWrite` streams. That is an adapter
  over JS events, not a reactor, and is out of scope here.

### 8.4 Portability rule

Code written against `futures-io` traits (or `spooly_io::test` pipes)
compiles everywhere. `spooly_io::net` and `fs` exist where a backend exists;
on other targets they're absent (`cfg`), not stubs that fail at run time.

---

## 9. Crate split and workspace

### 9.1 Should I/O be a separate crate?

**IO9. Yes.** Reasons:

- **The core stays dependency-free on native targets** (a stated goal).
  `mio` brings `libc`/`windows-sys`; future compat features would bring
  more. (The blocking pool needs only `std`, so it went into the core after
  all: IO17, §6.1.) None of that belongs in a user's build that only wants
  tasks and timers, or in a browser build.
- **A clean boundary.** A separate crate can only use the core's public API,
  which forces the extension point (§5) to be real and documented. A feature
  flag would let I/O reach into `Inner` and couple the two.
- **Independent pace.** `spooly-io` will churn (new socket types, backends,
  `io_uring`) while the core's semantics should be stable. Separate versions
  let the core reach 1.0 first.
- **Precedent**: `async-io`/`async-net`/`async-fs` around `async-executor`;
  `tokio`'s internal split (`mio`, `tokio-util`); `embassy-*`.

The costs, and how they're handled:

- *The extension point becomes public API.* It's small (four methods), and
  it's useful on its own (custom reactors, GUI loops). It lives in a
  `spooly::driver` module documented as unstable until 1.0 (IO10).
- *Two crates to add.* `spooly_io::run` covers the common case. If that
  turns out awkward, a later facade crate (re-exporting `spooly` and, under a
  feature, `spooly-io`) costs nothing to add. Not doing it now keeps the
  `spooly` name on the core, and publishing is undecided anyway (§10 of the
  core plan).
- *Cross-crate testing.* The workspace runs everything together in CI.

Considered and rejected: an `io` feature on the single `spooly` crate
(simpler at first, but couples I/O to internals and grows the core's
dependency list); separate `spooly-net` and `spooly-fs` crates (finer than
anyone needs: both share the reactor's pool and driver, and features cover
the choice).

The browser driver could move into a `spooly-browser` crate for the same
reasons, but it is small, already optional, and needs internals (the remote
path), so it stays.

### 9.2 Layout

```
spool_rs/
├── Cargo.toml              # [workspace]: members, [workspace.package]
│                           # (edition, rust-version, license, repository),
│                           # [workspace.lints] (unsafe_code = forbid, …),
│                           # [workspace.dependencies]
├── Cargo.lock
├── README.md               # points at both crates
├── docs/                   # INITIAL_PLAN.md, ASYNC_IO_PLAN.md, BENCHMARKS.md
├── crates/
│   ├── spooly/             # moved as is: src/, tests/, examples/, benches/
│   └── spooly-io/
│       ├── src/
│       │   ├── lib.rs      # run / block_on, Driver, features
│       │   ├── reactor.rs  # Driver, ScheduledIo (one backend: mio, M16)
│       │   ├── blocking.rs # (M16) blocking work: the pool, or inline on WASI
│       │   ├── net/        # tcp.rs, udp.rs, unix.rs, lookup.rs
│       │   ├── fs/         # mod.rs (free functions), file.rs
│       │   ├── async_fd.rs # Async<T> (M15)
│       │   └── test.rs     # duplex / pipe
│       ├── tests/          # loopback TCP/UDP, fs in a temp dir, liveness, stalls
│       └── examples/       # echo server, file copy, port scanner
└── .github/workflows/ci.yml   # `--workspace`; per-crate coverage
```

- `unsafe_code = "forbid"` stays workspace-wide: `mio`'s safe API is enough.
- `spooly-io` depends on `spooly` by path (`spooly = { path = "../spooly" }`
  in `[workspace.dependencies]`).
- CI: the existing matrix with `--workspace`; the WASM checks gain
  `spooly-io` (`cargo check` on `wasm32-unknown-unknown` must pass with no
  backend, IO8). Loopback tests run on all three OSes.

---

## 10. Later and optional

- **`io_uring` backend (Linux).** Completion-based: better for file I/O and
  high connection counts. It needs owned-buffer APIs
  (`read(buf: Vec<u8>) -> (io::Result<usize>, Vec<u8>)`), and cancellation
  means submitting a cancel and keeping the buffer alive until the kernel
  lets go. It would be an opt-in `uring` feature with its own types (as in
  `tokio-uring`), not a drop-in behind `AsyncRead`. Files first, where the
  thread pool is the weakest.
- **Processes** (`Command` with async `wait` and piped stdio): pipes through
  the reactor, child exit through `pidfd` on Linux or a `SIGCHLD`-driven or
  pool-based wait elsewhere.
- **Signals** (`ctrl_c()`), **`stdin`/`stdout`** (pool-based, like Tokio:
  making a shared fd non-blocking affects other processes).
- **A cooperative budget** (IO5) if starvation shows up in practice.
- **`TimerHandle::unref`** in the core, to match socket `unref`.
- **A `tokio-io` feature** (Tokio `AsyncRead`/`AsyncWrite` impls), punted
  for now (IO14).

---

## 11. Decisions from review

Settled in review of draft v1. Each is a starting point, not a commitment:
any of them can be revisited when implementation or real use gives a reason
to, and the "revisit if" column says what would prompt that.

| # | Question | Decision | Revisit if |
|---|---|---|---|
| IO10 | Public extension point, or hidden? | **Public**: `spooly::driver::IoDriver`, documented as unstable until 1.0 (§5.2) | The trait needs breaking changes often, or nobody but `spooly-io` implements it (then `#[doc(hidden)]` is an option) |
| IO11 | I/O phase on every step, or only on otherwise idle steps? | **Every step that has registrations** (IO2); measure before changing | Benchmarks show the per-step non-blocking poll costs noticeably under load |
| IO12 | `spawn_blocking` panics | **Re-raised in the awaiting task** with `resume_unwind`, so they unwind out of `run` like a task's panic (closest to D11); a detached job's panic is only printed (§6) | It proves confusing, or aborting turns out simpler and good enough |
| IO13 | Blocking pool size | **At most 64 threads by default** (`Builder::max_blocking_threads`); idle workers exit after 10 s (`Builder::blocking_idle_timeout`) (§6) | Workloads (many concurrent file operations) hit the cap |
| IO14 | Tokio `AsyncRead`/`AsyncWrite` impls (`tokio-io` feature) | **Punted.** Not in the plan's milestones; `spooly-io` implements only the `futures-io` traits. Users can bridge with `tokio-util`'s `compat` adapters meanwhile | Users need `hyper` or other Tokio-trait crates and the compat adapters aren't enough |
| IO15 | WASI 0.2 sockets: the `wasi` crate, or `std::net` on `wasip2`? | **Neither: `mio`** (decided in M16). `mio` 1.x runs on `wasip2` (wasi-libc's `poll` and sockets), so the native reactor serves WASI unchanged, with no new dependency and no `unsafe` of ours. `std::net` alone has no readiness API without `unsafe`; the `wasi` crate would mean a second reactor and socket types on `wasi:sockets` streams (§8.2) | Host-name lookups blocking the loop, or a socket feature wasi-libc lacks, matter to a real user (then: the `wasi` crate for that piece) |
| IO16 | A facade crate re-exporting core and I/O | **Not now.** Add one later only if two crates prove awkward (§9.1) | Users find `spooly` + `spooly-io` awkward, or publishing makes a single entry point desirable |
| IO17 | `spawn_blocking` in the core or in `spooly-io`? | **The core** (`spooly::spawn_blocking`): it needs only `KeepAlive` and remote wakes, and `std` (§6.1). Decided in M14 | The pool needs something I/O-specific, or its threads become a burden for core-only users |
| IO18 | One pool per process, or per runtime? | **Per runtime**, created on first use: the `Builder` configures it, it shuts down with its runtime, and tests stay isolated (Tokio's pool is per runtime too) | Many short-lived runtimes each start threads that a shared pool would reuse |
| IO19 | Does a `MockClock` jump while a blocking job runs? | **No**: the loop waits for the job in real time first, as Tokio's paused time does (§6) | A test needs time to pass during a job that never ends (a server loop on the pool) |
| IO20 | `Async<T>` on Windows? | **Unix only**: `mio` can't register an arbitrary Windows socket (§7.3) | Users need it on Windows (then: `polling` as a second backend, or `mio` support) |
| IO21 | A `socket2` feature? | **No**: the socket types implement `AsFd`/`AsSocket`, so `socket2::SockRef` works with no dependency of ours (§7.4) | Options must be set between creating a socket and an async `connect` (a `TcpSocket` builder) |
| IO22 | Does `Async::new` make the descriptor non-blocking? | **No**: the caller does (it needs `unsafe` or `rustix`) (§7.3) | It proves a common mistake; `rustix` is then a small, safe dependency |

---

## 12. Milestones

These continue the core plan's numbering. M10 (the core on WASI 0.2), the
prerequisite for M16, is done. The core plan's performance milestone,
first numbered M11, is now M17.

| # | Milestone | Status | Contents |
|---|---|---|---|
| M11 | Workspace | done | Move the crate to `crates/spooly`; root `[workspace]` with shared package fields, lints and dependencies; CI with `--workspace`. No code change; every test passes unchanged. |
| M12 | Core hook | done | `spooly::driver::IoDriver`, `Builder::io_driver`, `current_io_driver`; the unparker in `Shared`; the I/O phase and `is_active` in `step()` (IO2, IO3); native driver waits through the driver and handles `MockClock` (IO4). Tested with a fake in-crate driver (scripted readiness, a counting unparker): phase order, timers-plus-I/O fairness, liveness, remote wakes interrupting `wait`. No behavior change without a driver; model and browser suites unchanged. |
| M13 | `spooly-io` net | done | The crate, `mio` reactor (IO1, §5.3), `Driver`, `run`/`block_on`, TCP/UDP/Unix, `futures-io` traits, `halves`/`into_split`, `readable`/`writable`, `unref`. Loopback tests on all three OSes; liveness and stall tests; drop and cancellation tests; an echo-server example. |
| M14 | Blocking + fs | done | The pool and `spooly::spawn_blocking` in the core (IO7, IO17–IO19), `lookup_host` and string-address `connect`, `spooly_io::fs` (free functions, then `File`). Tests in temp dirs, pool shutdown and idle exit, panic propagation; the pool's tests run under Miri. |
| M15 | Extras | done | `test::{duplex, pipe}`; `Async<T>` (Unix, IO20, IO22); the OS-handle traits on every socket instead of a `socket2` feature (IO21); `copy` and `scan` examples. Not done: I/O operations in the reference model through `duplex` (optional). |
| M16 | WASI 0.2 | done | IO15: the `mio` reactor on `wasm32-wasip2` instead of a pollable backend (§8.2): `net` (TCP, UDP), `fs` and lookups, with a no-op unparker and blocking work run inline (`blocking::spawn`). The `spooly-io` suite under wasmtime in CI next to the core's, with network and `/tmp` access; tests needing threads, the pool, or socket features WASI lacks are ignored there, with the reason. Not done: `Async<T>` (Unix only), non-blocking lookups, and the core's `io_driver.rs` on WASI (the real reactor now tests that path). |
| — | Later | | `io_uring`, processes, signals, stdio, a web-streams adapter crate (§8.3, §10). |
