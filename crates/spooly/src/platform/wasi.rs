//! WASI 0.2 (`wasm32-wasip2`). There are no threads, so nothing outside the
//! runtime can run while it waits.
//!
//! `std::thread::park` exists here but does nothing (it returns at once),
//! so parking would make the loop spin through every wait. `thread::sleep`
//! does block: on `wasip2` it subscribes to `wasi:clocks/monotonic-clock`
//! and polls the pollable. Sleeping for the whole timeout is safe because
//! no other thread can wake a task meanwhile; I/O, which can, is waited on
//! by the I/O driver instead (`spooly-io`, M16).
//!
//! Waiting with no timeout means waiting for a wake that can never come (a
//! `KeepAlive`, or a waker held outside the runtime, that only this thread
//! could release), so that is reported as a stall instead of hanging the
//! program.

use std::rc::Rc;
use std::time::Duration;

use crate::clock::{Clock, SystemClock};

const WAIT_FOREVER_MESSAGE: &str = "spooly: the event loop is waiting for a wake from outside \
     the runtime (a `KeepAlive`, or a waker held elsewhere), but nothing can deliver one: WASI \
     has no threads, and the runtime has no I/O driver";

pub(crate) fn default_clock() -> Rc<dyn Clock> {
    // `std::time::Instant` is `wasi:clocks/monotonic-clock`.
    Rc::new(SystemClock::new())
}

pub(crate) fn park(timeout: Option<Duration>) {
    match timeout {
        Some(timeout) => std::thread::sleep(timeout),
        None => panic!("{WAIT_FOREVER_MESSAGE}"),
    }
}

/// Nothing to unpark: with no threads, a remote wake can only come from
/// this thread while the runtime isn't waiting.
pub(crate) struct Unparker;

impl Unparker {
    pub(crate) fn current() -> Self {
        Unparker
    }

    pub(crate) fn unpark(&self) {}
}

pub(crate) fn notify_remote_wake(_runtime_id: u64) {}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    #[test]
    fn park_sleeps_for_the_timeout() {
        let start = Instant::now();
        super::park(Some(Duration::from_millis(20)));
        assert!(start.elapsed() >= Duration::from_millis(20));
    }
}
