# Spooly — Async I/O Design & Plan

> Status: **v2, reviewed.** M11 and M12 are built; see §12. This document
> expands §9.1 and §9.2 of [`INITIAL_PLAN.md`](INITIAL_PLAN.md) into a
> design and milestones. Decisions are tagged `IO#` so they don't collide
> with the core plan's `D#` numbers. v2 settles draft v1's open questions
> (§11); they stay open to revisiting.

Spooly runs tasks and timers today, but a task can only wait on another
task, a timer, or a wake from another thread. This plan adds **network I/O**
(TCP, UDP, Unix sockets) and **file I/O** on native targets, without
changing what Spooly is: one thread, no `Send` bounds, documented ordering,
and no runtime dependencies in the core. It also keeps WASI (and, where it
makes sense, the browser) possible later.

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
| File I/O | A **small, self-written blocking thread pool** (`spawn_blocking`), completing over the existing remote-wake path (§6) |
| Crate split | **Yes**: a Cargo workspace with `spooly` (the core, unchanged name) and a new **`spooly-io`** crate. The core gains one narrow, public extension point for I/O drivers (§5, §7) |
| Loop change | An **I/O phase** after the timers phase, and drivers that wait on the reactor instead of parking the thread (§4) |
| WASM | WASI 0.2: a second `spooly-io` backend on `wasi:io/poll`, after M10. Browser: no reactor; JS APIs are already awaitable (§8) |

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
| **`wasi`** (Bytecode Alliance) | Generated bindings for WASI 0.2 interfaces (`wasi:io/poll`, `wasi:sockets`, `wasi:filesystem`). | **Yes, for the WASI backend** (§8.2), target-gated. |

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
  task is waiting to read, write, accept, …), or a `spawn_blocking` job is in
  flight.
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
    poll: mio::Poll,                 // or the WASI pollable list (§8.2)
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
pub fn spawn_blocking<F, T>(f: F) -> BlockingHandle<T>
where F: FnOnce() -> T + Send + 'static, T: Send + 'static;
```

- `BlockingHandle<T>` is `!Send` and awaits to `T`. Dropping it detaches
  the job. It can't abort a closure that is already running; it can cancel
  one that hasn't started.
- The job's result goes into an `Arc<Mutex<…>>` slot and its waker is woken
  from the pool thread: the **existing remote-wake path** (§4.3 of the core
  plan), which now wakes the reactor through the unparker. No new mechanism.
- In-flight jobs count toward `is_active()` (IO3), so `run` waits for them.
- The pool: `std::thread` workers, a `Mutex<VecDeque>` + `Condvar`, at
  most 64 threads by default (configurable, IO13), workers exiting after
  ~10 s idle, spawned lazily. It
  is shared by all runtimes in the process, like Tokio's. It needs `std`
  threads, so it doesn't exist on targets without them.
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
TcpStream::connect(addr).await? ; stream.split() -> (ReadHalf<'_>, WriteHalf<'_>)
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

### 7.3 Arbitrary file descriptors

`spooly_io::Async<T: AsFd>` (Unix) and `Async<T: AsSocket>` (Windows):
register any non-blocking fd/socket with the reactor and get
`readable()`/`writable()` plus `AsyncRead`/`AsyncWrite` when `T: Read +
Write`. Modeled on smol's `async-io`. It covers pipes, `eventfd`,
third-party sockets and so on. (Later milestone, M15.)

### 7.4 Features of `spooly-io`

| Feature | Default | Contents |
|---|---|---|
| `net` | yes | TCP, UDP, Unix sockets, `lookup_host` (needs `blocking`) |
| `fs` | yes | `spooly_io::fs` (needs `blocking`) |
| `blocking` | yes | `spawn_blocking` and the pool |
| `socket2` | no | `from_socket2` / extra options |

### 7.5 Testing utilities

`spooly_io::test::{duplex, pipe}`: in-memory, `!Send` byte streams that
implement the same traits, with a configurable buffer size. They involve no
reactor and no OS: a write wakes the reader as an ordinary local wake. They
give deterministic tests of protocol code (with `MockClock`), and let the
reference model (D15) grow I/O-like operations later without real sockets.

---

## 8. WASM

### 8.1 What the core already gives

The core runs on `wasm32-unknown-unknown` (browser driver, M9) and builds
for `wasm32-wasip1`; the WASI driver is M10 (deferred). `IoDriver` (§5) is
designed so that a backend with **no threads** and **no `mio`** fits it:
`unparker` can be a no-op, `wait` can block on the host's poll call.

### 8.2 WASI 0.2 (`wasm32-wasip2`)

WASI 0.2 has exactly the readiness primitive we need: every stream, socket
and timer exposes a `pollable`, and `wasi:io/poll.poll(list)` blocks until
some are ready. The plan:

- A `spooly-io` backend for `target_os = "wasi"` + `target_env = "p2"`,
  using the `wasi` crate's bindings, with the same `Reactor` shape (§5.3):
  `ScheduledIo` holds a `Pollable` instead of a `mio` token, `wait(timeout)`
  calls `poll` on the waiting pollables plus a
  `monotonic-clock.subscribe-duration(timeout)` pollable, and `poll_ready`
  wakes the tasks whose pollables are ready.
- Network: `wasi:sockets` (`tcp`, `udp`, `ip-name-lookup`, which is
  non-blocking there, so no thread pool is needed for DNS).
- Files: `wasi:filesystem` offers stream-based reads and writes with
  pollables, but hosts implement most file operations synchronously. Without
  threads there's no pool, so `fs` runs operations inline (they don't block
  on the network, only on the host's disk), possibly yielding between chunks.
  `spawn_blocking` isn't available there.
- It depends on M10 (a WASI driver that steps the loop), and on choosing
  between the `wasi` crate and `std`'s own WASI 0.2 networking
  (`std::net` on `wasip2`) for the socket types. To be decided then (IO15).
- **WASI 0.3** adds native async to the component model (`future`/`stream`
  types and an async ABI). That could replace the pollable backend later; it
  doesn't change the `IoDriver` shape.
- **WASI 0.1** (`wasm32-wasip1`): `mio` supports it, but 0.1 sockets can't
  connect, only accept on preopened listeners. Not worth a backend unless
  someone needs it; `spooly-io` simply offers no `net` there.

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
  `mio` brings `libc`/`windows-sys`; `fs` brings a thread pool; future
  compat features would bring more. None of that belongs in a user's build that only wants
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
│       │   ├── reactor/    # mod.rs (Reactor, ScheduledIo), mio.rs, wasi.rs
│       │   ├── net/        # tcp.rs, udp.rs, unix.rs, lookup.rs
│       │   ├── fs/         # mod.rs (free functions), file.rs
│       │   ├── blocking.rs # pool + spawn_blocking
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
| IO13 | Blocking pool size | **At most 64 threads by default**, configurable through a builder option; idle workers exit after ~10 s (§6) | Workloads (many concurrent file operations) hit the cap |
| IO14 | Tokio `AsyncRead`/`AsyncWrite` impls (`tokio-io` feature) | **Punted.** Not in the plan's milestones; `spooly-io` implements only the `futures-io` traits. Users can bridge with `tokio-util`'s `compat` adapters meanwhile | Users need `hyper` or other Tokio-trait crates and the compat adapters aren't enough |
| IO15 | WASI 0.2 sockets: the `wasi` crate, or `std::net` on `wasip2`? | **Deferred to M16**, decided when the WASI backend is built (§8.2) | — |
| IO16 | A facade crate re-exporting core and I/O | **Not now.** Add one later only if two crates prove awkward (§9.1) | Users find `spooly` + `spooly-io` awkward, or publishing makes a single entry point desirable |

---

## 12. Milestones

These continue the core plan's numbering. M10 (WASI driver) stays deferred
and is only a prerequisite for M16.

| # | Milestone | Status | Contents |
|---|---|---|---|
| M11 | Workspace | done | Move the crate to `crates/spooly`; root `[workspace]` with shared package fields, lints and dependencies; CI with `--workspace`. No code change; every test passes unchanged. |
| M12 | Core hook | done | `spooly::driver::IoDriver`, `Builder::io_driver`, `current_io_driver`; the unparker in `Shared`; the I/O phase and `is_active` in `step()` (IO2, IO3); native driver waits through the driver and handles `MockClock` (IO4). Tested with a fake in-crate driver (scripted readiness, a counting unparker): phase order, timers-plus-I/O fairness, liveness, remote wakes interrupting `wait`. No behavior change without a driver; model and browser suites unchanged. |
| M13 | `spooly-io` net | | The crate, `mio` reactor (IO1, §5.3), `Driver`, `run`/`block_on`, TCP/UDP/Unix, `futures-io` traits, `split`, `readable`/`writable`, `unref`. Loopback tests on all three OSes; liveness and stall tests; drop and cancellation tests; an echo-server example. |
| M14 | Blocking + fs | | The pool, `spawn_blocking` (IO7), `lookup_host` and string-address `connect`, `spooly_io::fs` (free functions, then `File`). Tests in temp dirs, pool shutdown and idle exit, panic propagation. |
| M15 | Extras | | `test::{duplex, pipe}`; `Async<T>`; `socket2` feature; I/O operations in the reference model through `duplex` (optional). |
| M16 | WASI 0.2 | | After M10: the pollable backend (§8.2), `wasi:sockets`, inline `fs`; the `spooly-io` suite (minus threads) under wasmtime in CI. |
| — | Later | | `io_uring`, processes, signals, stdio, a web-streams adapter crate (§8.3, §10). |
