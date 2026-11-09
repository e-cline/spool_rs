//! The browser: `wasm32-unknown-unknown`. A page can't block its thread,
//! so the browser driver (`browser::start`) runs steps from JS callbacks
//! and never parks. `run` and `block_on` still work, but only for code that
//! never waits for real time or a wake: `park` returns at once, so they
//! busy-wait (§7).

use std::rc::Rc;
use std::time::Duration;

use crate::clock::Clock;

pub(crate) fn default_clock() -> Rc<dyn Clock> {
    #[cfg(feature = "browser")]
    {
        Rc::new(crate::browser::PerformanceClock::new())
    }
    // Without the `browser` feature there's no way to read the time here
    // (`std::time::Instant::now()` panics), so fail early, with a clearer
    // message.
    #[cfg(not(feature = "browser"))]
    {
        panic!(
            "spooly: wasm32-unknown-unknown has no default clock without the `browser` \
             feature; enable it, or pass a clock with `Runtime::builder().clock(...)`"
        )
    }
}

pub(crate) fn park(_timeout: Option<Duration>) {}

/// Nothing to unpark: nothing parks. A remote wake reaches the browser
/// driver through [`notify_remote_wake`] instead.
pub(crate) struct Unparker;

impl Unparker {
    pub(crate) fn current() -> Self {
        Unparker
    }

    pub(crate) fn unpark(&self) {}
}

/// Queues a step, if the runtime was started by the browser driver on this
/// thread.
pub(crate) fn notify_remote_wake(_runtime_id: u64) {
    #[cfg(feature = "browser")]
    crate::driver::browser::wake(_runtime_id);
}
