//! The native driver behind `run` and `block_on`: steps the loop, and parks
//! the thread (or waits on the I/O driver) while it waits for a timer, I/O,
//! or a remote wake.

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use crate::clock::{Clock, Instant};
use crate::driver::MainTask;
use crate::runtime::{Inner, Main, Step};

const STALL_MESSAGE: &str = "spooly: main future stalled: it is pending but nothing can wake it \
     (no queued work, no timers, no `KeepAlive` guards, and no wakers outside the runtime; \
     every pending task is stuck too)";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Return as soon as main completes.
    BlockOn,
    /// Return once main has completed and the loop has nothing left to do.
    Run,
}

/// Runs `future` as main until `mode` says to return. The caller has
/// entered the runtime.
pub(crate) fn drive<F: Future>(inner: &Inner, future: F, mode: Mode) -> F::Output {
    // Every macrotask starts with an empty queue (D4a): flush work left over
    // from an earlier `block_on` or spawned before this call. (A leftover
    // `Poll(MAIN_TASK)` is a stale wake from an earlier main future; it is
    // ignored.)
    inner.drain(&mut ());

    let stop_when_done = mode == Mode::BlockOn;
    let future = pin!(future);
    let mut main = MainTask::new(future, inner.main_header(), stop_when_done);

    // Main's first poll is a macrotask, like a program's top-level code
    // (D4a). Microtasks it queues run after it. Later polls are microtasks,
    // run by the step.
    main.poll();

    loop {
        if stop_when_done && main.is_done() {
            return main.take_output();
        }
        match inner.step(&mut main) {
            Step::Busy => {}
            Step::WaitUntil(deadline) => wait_until(inner, deadline),
            Step::WaitForWake => {
                inner.wait(None);
            }
            Step::Idle if main.is_done() => return main.take_output(),
            Step::Idle => panic!("{STALL_MESSAGE}"),
        }
    }
}

/// Waits until `deadline`: lets the clock jump there if it can (a
/// `MockClock`), else waits until then, or until a remote wake (or I/O)
/// ends the wait. (A wait may also end spuriously; the loop just steps
/// again.)
///
/// With an I/O driver, I/O that is already ready comes before a clock
/// jump (IO4): the driver is polled without blocking first, and the clock
/// only jumps if nothing became ready and no remote wake is queued.
///
/// Nor does the clock jump while a `spawn_blocking` job is queued or
/// running: the loop waits (in real time) for it to finish first. A job
/// queues its wake before it stops counting as in flight, so the
/// pending-remote check after the in-flight one can't miss it.
fn wait_until(inner: &Inner, deadline: Instant) {
    let clock: &dyn Clock = inner.clock();
    let now = clock.now();
    if deadline <= now {
        return;
    }
    if inner.has_io_driver() && (inner.wait(Some(Duration::ZERO)) || inner.has_pending_remote()) {
        return;
    }
    if inner.has_blocking_in_flight() {
        // A finished job wakes the loop (through its waker or `KeepAlive`).
        inner.wait(Some(deadline.saturating_duration_since(now)));
        return;
    }
    if inner.has_pending_remote() {
        return; // A job finished (or a thread woke a task) meanwhile.
    }
    if clock.advance_when_idle(deadline) {
        return;
    }
    inner.wait(Some(deadline.saturating_duration_since(now)));
}
