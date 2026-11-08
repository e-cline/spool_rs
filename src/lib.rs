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
//! | Spool | JS / Node |
//! |---|---|
//! | [`run`] | a Node process: runs until the event loop is empty |
//! | [`spawn`] | `queueMicrotask(asyncFn)` (the task starts on the microtask queue) |
//! | [`yield_now`]`().await` | `await null` |
//! | [`queue_microtask`] | `queueMicrotask` |
//! | [`next_tick`] | `process.nextTick` |
//! | [`set_timeout`] / [`TimerHandle::cancel`] | `setTimeout` / `clearTimeout` |
//! | [`set_interval`] / [`TimerHandle::cancel`] | `setInterval` / `clearInterval` |
//! | [`sleep`]`(d).await` | `await new Promise(r => setTimeout(r, ms))` |
//!
//! Two differences from JS:
//! - In JS, `await` always takes a microtask hop. In Rust, `.await` on a
//!   future that is already ready (such as the [`JoinHandle`] of a finished
//!   task) continues without one.
//! - Node clamps timer delays to at least 1ms. Spool doesn't, but a zero
//!   delay still waits for the next timers phase.
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
//! See `docs/INITIAL_PLAN.md` in the repository for the full design.

#![warn(unreachable_pub)]

mod clock;
mod context;
mod park;
mod queue;
mod runtime;
mod slab;
mod task;
mod timer;
mod waker;
mod yield_now;

pub use clock::{Clock, Instant, MockClock, SystemClock, now};
pub use queue::{next_tick, queue_microtask};
pub use runtime::{Builder, Runtime, block_on, run};
pub use task::{JoinHandle, spawn};
pub use timer::{
    Elapsed, Interval, Sleep, Timeout, TimerHandle, interval, set_interval, set_timeout, sleep,
    sleep_until, timeout, timeout_at,
};
pub use yield_now::{YieldNow, yield_now};
