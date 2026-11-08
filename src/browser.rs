//! Running Spooly in the browser: `wasm32-unknown-unknown` with the
//! `browser` feature.
//!
//! A browser page can't block its thread, so [`run`](crate::run) and
//! [`block_on`](crate::block_on) aren't the way in. [`start`] returns at
//! once instead, and the runtime runs from JS callbacks: each step of the
//! event loop runs inside one callback, and the driver schedules the next
//! one (a JS microtask when there is more work, a JS `setTimeout` for the
//! next Spooly timer). Spooly's ordering is the same as on native: its
//! microtasks run among themselves exactly as there, but a whole step runs
//! without JS promise jobs interleaved.
//!
//! A started runtime keeps running for the rest of the page. When it has
//! nothing to do it schedules nothing, until a wake (a JS promise resolving,
//! for example) gives it more work. It never panics on a stall.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! spooly::browser::start(async {
//!     spooly::sleep(Duration::from_millis(500)).await;
//!     // ... update the page ...
//! });
//! // Returns at once; the task runs from JS callbacks.
//! ```
//!
//! [`PerformanceClock`], backed by `performance.now()`, is the default clock
//! of [`Runtime::new`] with this feature.
//!
//! # Panics
//!
//! WASM programs abort on a panic by default (D11): the panic becomes a JS
//! exception from the callback that was running, and the runtime can't be
//! used afterwards.

use std::future::Future;
use std::time::Duration;

use crate::clock::{Clock, Instant};
use crate::driver::browser as driver;
use crate::runtime::Runtime;

/// The browser's real-time clock, backed by `performance.now()`. The
/// default clock on `wasm32-unknown-unknown` with the `browser` feature.
///
/// It works on a page and in a worker, since both have a global
/// `performance`.
#[derive(Debug, Clone, Copy)]
pub struct PerformanceClock {
    /// `performance.now()` at creation, in milliseconds.
    epoch_ms: f64,
}

impl PerformanceClock {
    /// Creates a clock whose epoch is now.
    pub fn new() -> Self {
        PerformanceClock {
            epoch_ms: driver::performance_now(),
        }
    }
}

impl Default for PerformanceClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for PerformanceClock {
    fn now(&self) -> Instant {
        // `performance.now()` is monotonic; `max` guards against rounding.
        let ms = (driver::performance_now() - self.epoch_ms).max(0.0);
        Instant::from_epoch_offset(Duration::from_secs_f64(ms / 1000.0))
    }
}

/// Starts `main` on a new runtime, run from JS callbacks, and returns at
/// once. This is shorthand for `start_on(Runtime::new(), main)`; see
/// [`start_on`].
pub fn start(main: impl Future<Output = ()> + 'static) {
    start_on(Runtime::new(), main);
}

/// Starts `main` on `runtime`, run from JS callbacks, and returns at once.
///
/// Nothing runs until the current JS code returns. Then, as with
/// [`Runtime::run`], work already queued on `runtime` (tasks spawned with
/// [`Runtime::spawn`], say) runs first, and `main`'s first poll runs like a
/// program's top-level code. The runtime keeps running after `main`
/// completes, for the rest of the page: there is no exit, and tasks that are
/// still pending stay alive.
///
/// ```no_run
/// let rt = spooly::Runtime::new();
/// let greeting = rt.spawn(async { "hello" });
/// spooly::browser::start_on(rt, async move {
///     let greeting = greeting.await;
///     // ... show `greeting` on the page ...
/// });
/// ```
pub fn start_on(runtime: Runtime, main: impl Future<Output = ()> + 'static) {
    driver::start(runtime, Box::pin(main));
}
