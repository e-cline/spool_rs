//! Targets with threads: Unix, Windows, and anything else `std` runs on
//! with a real thread parker.

use std::rc::Rc;
use std::thread::{self, Thread};
use std::time::Duration;

use crate::clock::{Clock, SystemClock};

pub(crate) fn default_clock() -> Rc<dyn Clock> {
    Rc::new(SystemClock::new())
}

pub(crate) fn park(timeout: Option<Duration>) {
    match timeout {
        Some(timeout) => thread::park_timeout(timeout),
        None => thread::park(),
    }
}

/// Unparks the thread that owns the runtime (the only thread that runs it).
pub(crate) struct Unparker(Thread);

impl Unparker {
    pub(crate) fn current() -> Self {
        Unparker(thread::current())
    }

    pub(crate) fn unpark(&self) {
        self.0.unpark();
    }
}

pub(crate) fn notify_remote_wake(_runtime_id: u64) {}
