//! # Spooly
//!
//! A lightweight, single-threaded async executor, with a scheduling model
//! inspired by the JavaScript event loop.
//!
//! - **No `Send` or `Sync` required.** Futures, outputs, and callbacks can
//!   hold `Rc`, `RefCell`, and `Cell`.
//! - **Simple, predictable ordering.** A woken task is queued as a microtask,
//!   in FIFO order, like the code after an `await` in JS. Timers run one at a
//!   time, and the microtask queue drains completely after each.
//! - **Deterministic tests.** A [`MockClock`] makes timers fire instantly, in
//!   a fixed order.
//! - **Native and the browser.** [`run`] and [`block_on`] drive the loop on
//!   native targets; in the browser, `browser::start` drives it from JS
//!   callbacks (see [In the browser](#in-the-browser)).
//! - **Minimal dependencies** (none at runtime on native targets), and no
//!   `unsafe` code.
//!
//! ```
//! use std::{cell::RefCell, rc::Rc};
//!
//! let log = spooly::block_on(async {
//!     let log = Rc::new(RefCell::new(Vec::new()));
//!     let (a, b) = (log.clone(), log.clone());
//!     let task_a = spooly::spawn(async move {
//!         a.borrow_mut().push("a1");
//!         spooly::yield_now().await;
//!         a.borrow_mut().push("a2");
//!     });
//!     let task_b = spooly::spawn(async move {
//!         b.borrow_mut().push("b1");
//!         spooly::yield_now().await;
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
//! Spooly borrows JS names where they are the clearest ones, but it is not a
//! reimplementation of Node or the browser: each item documents its own
//! behavior. The closest JS counterparts:
//!
//! | Spooly | Closest JS |
//! |---|---|
//! | [`spawn`] | `queueMicrotask(asyncFn)` (the task starts on the microtask queue) |
//! | `handle.await` | `await promise` |
//! | [`yield_now`]`().await` | `await null` |
//! | [`queue_microtask`] | `queueMicrotask` |
//! | [`set_timeout`] / [`TimerHandle::cancel`] | `setTimeout` / `clearTimeout` |
//! | [`set_interval`] / [`TimerHandle::cancel`] | `setInterval` / `clearInterval` |
//! | [`sleep`]`(d).await` | `await new Promise(r => setTimeout(r, ms))` |
//! | [`timeout`]`(d, fut)` | `Promise.race([p, rejectAfter(ms)])` (but `fut` is dropped) |
//!
//! Some deliberate differences:
//! - In JS, `await` always takes a microtask hop. In Rust, `.await` on a
//!   future that is already ready (such as the [`JoinHandle`] of a finished
//!   task) continues without one.
//! - Intervals keep a fixed schedule and skip runs they missed, rather than
//!   drifting when the loop is late.
//! - There is one microtask queue: no equivalent of Node's
//!   `process.nextTick`.
//! - Delays aren't clamped to 1ms, but a zero delay still waits for the next
//!   timers phase.
//! - A panic is not a rejected promise: Spooly never catches it (see
//!   [Panics](#panics)). Errors a caller should handle are `Result` values.
//!
//! ## The event loop
//!
//! A runtime has a microtask queue (woken tasks and [`queue_microtask`]
//! callbacks, in FIFO order) and timers. Each turn of the loop:
//!
//! 1. **Drains the microtask queue**, including microtasks queued meanwhile.
//! 2. **Runs the timers phase**: every timer that is due fires, one at a
//!    time, in deadline order (creation order for equal deadlines), and the
//!    microtask queue is drained after each one. A timer armed during the
//!    phase waits for the next one, so a timer that re-arms itself can't
//!    starve the loop.
//! 3. **Waits** for the next timer, or for a wake from another thread.
//!
//! The first poll of the future given to [`run`] / [`block_on`] is like a
//! program's top-level code: work it queues runs after it. Each later poll
//! (after an `.await`) is a microtask.
//!
//! ```
//! use std::{cell::RefCell, rc::Rc, time::Duration};
//!
//! let log = Rc::new(RefCell::new(Vec::new()));
//! let l = log.clone();
//! spooly::run(async move {
//!     let (a, b) = (l.clone(), l.clone());
//!     spooly::set_timeout(Duration::ZERO, move || a.borrow_mut().push("timeout"));
//!     spooly::queue_microtask(move || b.borrow_mut().push("microtask"));
//!     l.borrow_mut().push("sync");
//! });
//! assert_eq!(*log.borrow(), ["sync", "microtask", "timeout"]);
//! ```
//!
//! ## `run` vs `block_on`
//!
//! - [`run`] waits until `main` is done **and** the loop has nothing left to
//!   do: no queued work, no armed timers, no [`KeepAlive`] guards. It doesn't
//!   wait for detached tasks that are pending on something else.
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
//! Microtasks can't be cancelled: they run within the current turn.
//!
//! ## Panics
//!
//! Spooly doesn't catch panics, so they behave the same on every target. A
//! panic in `main`, in any task (awaited or not), or in a microtask or timer
//! callback stops the loop at once:
//!
//! - with `panic = "unwind"` (the default on native targets), it unwinds out
//!   of [`run`] / [`block_on`];
//! - with `panic = "abort"` (the default on WASM), it ends the program.
//!
//! Which of the two happens is your application's choice: Spooly doesn't
//! override it. To make native behave exactly like WASM, set this in your
//! application's `Cargo.toml` (tests always unwind, whatever it says):
//!
//! ```toml
//! [profile.release]
//! panic = "abort"
//! ```
//!
//! After a panic unwinds out, drop the runtime (which drops its remaining
//! tasks and timers as usual) rather than driving it again: the interrupted
//! turn may have lost work, so what it would do is unspecified (though never
//! unsound). A new [`Runtime`] works normally.
//!
//! A task that can fail should return a `Result`. [`JoinHandle::try_join`]
//! only reports an abort.
//!
//! ## Threads
//!
//! Everything runs on the runtime's thread, and tasks don't need to be
//! `Send`. Wakers are still `Send + Sync`, as `std` requires, and can be woken
//! from any thread: the wake is queued for the runtime's thread, which is
//! unparked. While a detached task waits on another thread, hold a
//! [`KeepAlive`] so that `run` doesn't exit first.
//!
//! ## I/O
//!
//! Spooly itself does no I/O. An I/O crate plugs a reactor into the loop
//! through the (unstable) [`driver`] module: the loop polls it after the
//! timers in each step, and waits on it when there is nothing else to do.
//!
//! ## In the browser
//!
//! On `wasm32-unknown-unknown`, enable the `browser` feature (which adds the
//! crate's only dependency, `wasm-bindgen`) and start the loop with
//! `spooly::browser::start(main)` instead of `run`. It returns at once, and
//! each step of the loop then runs from a JS callback: a page can't block
//! its thread. The ordering is the same as on native. The `browser` module
//! docs (built for that target) have the details, and
//! `crates/spooly/examples/browser` in the repository is a small page.
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
//! let clock = spooly::MockClock::new();
//! let rt = spooly::Runtime::builder().clock(clock.clone()).build();
//! let result = rt.block_on(async {
//!     spooly::timeout(Duration::from_secs(30), std::future::pending::<()>()).await
//! });
//! assert!(result.is_err());
//! assert_eq!(clock.elapsed(), Duration::from_secs(30)); // In no real time.
//! ```
//!
//! See `docs/INITIAL_PLAN.md` in the repository for the full design.

#![warn(unreachable_pub)]

#[cfg(all(feature = "browser", target_arch = "wasm32"))]
pub mod browser;
mod clock;
mod context;
pub mod driver;
mod keep_alive;
mod queue;
mod runtime;
mod slab;
mod task;
mod timer;
mod waker;
mod yield_now;

pub use clock::{Clock, Instant, MockClock, SystemClock, now};
pub use keep_alive::{KeepAlive, keep_alive};
pub use queue::queue_microtask;
pub use runtime::{Builder, Runtime, block_on, run};
pub use task::{AbortHandle, AbortOnDrop, JoinError, JoinHandle, TryJoin, spawn};
pub use timer::{
    Elapsed, Interval, Sleep, Timeout, TimerHandle, interval, set_interval, set_timeout, sleep,
    sleep_until, timeout, timeout_at,
};
pub use yield_now::{YieldNow, yield_now};
