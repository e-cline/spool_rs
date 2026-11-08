//! # Spool
//!
//! A lightweight, single-threaded async executor with JavaScript / Node.js
//! event-loop semantics.
//!
//! - **No `Send` or `Sync` required.** Futures, outputs, and callbacks can
//!   hold `Rc`, `RefCell`, and `Cell`.
//! - **Predictable, Node-like ordering.** A woken task is queued as a
//!   microtask, in FIFO order, just like the code after an `await` in JS.
//!   [`next_tick`] callbacks run before microtasks, and timers run one at a
//!   time, with both queues drained after each, as in Node.
//! - **Deterministic tests.** A [`MockClock`] makes timers fire instantly, in
//!   a fixed order.
//! - **Zero dependencies** and no `unsafe` code.
//!
//! ```
//! use std::{cell::RefCell, rc::Rc};
//!
//! let log = spool::block_on(async {
//!     let log = Rc::new(RefCell::new(Vec::new()));
//!     let (a, b) = (log.clone(), log.clone());
//!     let task_a = spool::spawn(async move {
//!         a.borrow_mut().push("a1");
//!         spool::yield_now().await;
//!         a.borrow_mut().push("a2");
//!     });
//!     let task_b = spool::spawn(async move {
//!         b.borrow_mut().push("b1");
//!         spool::yield_now().await;
//!         b.borrow_mut().push("b2");
//!     });
//!     task_a.await;
//!     task_b.await;
//!     Rc::try_unwrap(log).unwrap().into_inner()
//! });
//! // Tasks interleave at each `.await`, as JS async functions would.
//! assert_eq!(log, ["a1", "b1", "a2", "b2"]);
//! ```
//!
//! ## Coming from JavaScript
//!
//! | Spool | JS / Node |
//! |---|---|
//! | [`run`] | a Node process: runs until the event loop is empty |
//! | [`spawn`] | `queueMicrotask(asyncFn)` (the task starts on the microtask queue) |
//! | `handle.await` | `await promise` |
//! | [`yield_now`]`().await` | `await null` |
//! | [`queue_microtask`] | `queueMicrotask` |
//! | [`next_tick`] | `process.nextTick` |
//! | [`set_timeout`] / [`TimerHandle::cancel`] | `setTimeout` / `clearTimeout` |
//! | [`set_interval`] / [`TimerHandle::cancel`] | `setInterval` / `clearInterval` |
//! | [`sleep`]`(d).await` | `await new Promise(r => setTimeout(r, ms))` |
//! | [`timeout`]`(d, fut)` | `Promise.race([p, rejectAfter(ms)])` (but `fut` is dropped) |
//! | [`keep_alive`] / dropping the [`KeepAlive`] | a ref'd handle / `handle.unref()` |
//! | `handle.await` on a task that panicked | `await` on a rejected promise: re-throws |
//! | [`JoinHandle::try_join`] | `try { await p } catch (e) { .. }` |
//! | a panic in a detached task | an unhandled rejection: crashes the process |
//!
//! Two differences from JS:
//! - In JS, `await` always takes a microtask hop. In Rust, `.await` on a
//!   future that is already ready (such as the [`JoinHandle`] of a finished
//!   task) continues without one.
//! - Node clamps timer delays to at least 1ms. Spool doesn't, but a zero
//!   delay still waits for the next timers phase.
//!
//! ## The event loop
//!
//! A runtime has a tick queue ([`next_tick`]), a microtask queue (woken
//! tasks and [`queue_microtask`] callbacks, in FIFO order), and timers. Each
//! turn of the loop:
//!
//! 1. **Drains the queues**, as Node does: every tick, then every microtask,
//!    then again if the microtasks queued more ticks.
//! 2. **Runs the timers phase**: every timer that is due fires, one at a
//!    time, in deadline order (creation order for equal deadlines), and the
//!    queues are drained after each one. A timer armed during the phase waits
//!    for the next one, so a timer that re-arms itself can't starve the loop.
//! 3. **Waits** for the next timer, or for a wake from another thread.
//!
//! The first poll of the future given to [`run`] / [`block_on`] is like a JS
//! program's top-level code: work it queues runs after it. Each later poll
//! (after an `.await`) is a microtask.
//!
//! ```
//! use std::{cell::RefCell, rc::Rc, time::Duration};
//!
//! let log = Rc::new(RefCell::new(Vec::new()));
//! let l = log.clone();
//! spool::run(async move {
//!     let (a, b, c) = (l.clone(), l.clone(), l.clone());
//!     spool::set_timeout(Duration::ZERO, move || a.borrow_mut().push("timeout"));
//!     spool::queue_microtask(move || b.borrow_mut().push("microtask"));
//!     spool::next_tick(move || c.borrow_mut().push("tick"));
//!     l.borrow_mut().push("sync");
//! });
//! // The same order as the equivalent Node program.
//! assert_eq!(*log.borrow(), ["sync", "tick", "microtask", "timeout"]);
//! ```
//!
//! ## `run` vs `block_on`
//!
//! - [`run`] waits until `main` is done **and** the loop has nothing left to
//!   do: no queued work, no armed timers, no [`KeepAlive`] guards. Like Node,
//!   it doesn't wait for detached tasks that are pending on something else.
//! - [`block_on`] returns as soon as `main` completes.
//!
//! Both panic if `main` provably **stalls**: it is pending, and nothing can
//! ever wake it or any task it waits on.
//!
//! ## Cancellation
//!
//! Futures cancel when dropped; handles detach when dropped, and cancel
//! explicitly.
//!
//! | Thing | How to cancel |
//! |---|---|
//! | [`Sleep`], [`Interval`], [`Timeout`], any future | drop it |
//! | a spawned task | [`JoinHandle::abort`], or an [`AbortHandle`] |
//! | a task tied to a scope | [`JoinHandle::abort_on_drop`], then drop the guard |
//! | [`set_timeout`] / [`set_interval`] | [`TimerHandle::cancel`] |
//!
//! Ticks and microtasks can't be cancelled: they run within the current turn.
//!
//! ## Panics
//!
//! Panics behave like JS exceptions in async code.
//!
//! - A task's panic is caught, and re-thrown in whoever awaits its
//!   [`JoinHandle`] (or returned as an `Err` by [`JoinHandle::try_join`]).
//! - A panic that nobody can observe (in a detached task, or held by a
//!   handle that is dropped without being awaited), and any panic in `main`
//!   or a callback, unwinds out of [`run`] / [`block_on`]. The runtime is then
//!   **poisoned**: it can't be driven again, but a new one works normally.
//!
//! ## Threads
//!
//! Everything runs on the runtime's thread, and tasks don't need to be
//! `Send`. Wakers are still `Send + Sync`, as `std` requires, and can be woken
//! from any thread: the wake is queued for the runtime's thread, which is
//! unparked. While a detached task waits on another thread, hold a
//! [`KeepAlive`] so that `run` doesn't exit first.
//!
//! ## Testing
//!
//! Build the runtime with a [`MockClock`]: when the loop would sleep until
//! the next timer, the clock jumps straight there, so timer-heavy tests run
//! instantly and deterministically.
//!
//! ```
//! use std::time::Duration;
//!
//! let clock = spool::MockClock::new();
//! let rt = spool::Runtime::builder().clock(clock.clone()).build();
//! let result = rt.block_on(async {
//!     spool::timeout(Duration::from_secs(30), std::future::pending::<()>()).await
//! });
//! assert!(result.is_err());
//! assert_eq!(clock.elapsed(), Duration::from_secs(30)); // In no real time.
//! ```
//!
//! See `docs/INITIAL_PLAN.md` in the repository for the full design.

#![warn(unreachable_pub)]

mod clock;
mod context;
mod keep_alive;
mod park;
mod queue;
mod runtime;
mod slab;
mod task;
mod timer;
mod waker;
mod yield_now;

pub use clock::{Clock, Instant, MockClock, SystemClock, now};
pub use keep_alive::{KeepAlive, keep_alive};
pub use queue::{next_tick, queue_microtask};
pub use runtime::{Builder, Runtime, block_on, run};
pub use task::{AbortHandle, AbortOnDrop, JoinError, JoinHandle, TryJoin, spawn};
pub use timer::{
    Elapsed, Interval, Sleep, Timeout, TimerHandle, interval, set_interval, set_timeout, sleep,
    sleep_until, timeout, timeout_at,
};
pub use yield_now::{YieldNow, yield_now};
