//! Blocking the runtime's thread while it is idle.
//!
//! The event loop itself never blocks (design decision D5); only this module
//! does. A future I/O reactor (epoll/kqueue) or WASI `poll` driver replaces
//! this, and the browser target has no blocking at all.

use std::time::Duration;

/// Blocks the current thread until it is unparked (a remote wake), the
/// timeout passes, or a spurious wake-up. `None` waits indefinitely.
pub(crate) fn park(timeout: Option<Duration>) {
    match timeout {
        Some(timeout) => std::thread::park_timeout(timeout),
        None => std::thread::park(),
    }
}
