//! The native driver behind `run` and `block_on`: steps the loop, and parks
//! the thread while it waits for a timer or a remote wake.

use std::future::Future;
use std::pin::pin;

use crate::clock::{Clock, Instant};
use crate::driver::MainTask;
use crate::park;
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
            Step::WaitUntil(deadline) => wait_until(inner.clock(), deadline),
            Step::WaitForWake => park::park(None),
            Step::Idle if main.is_done() => return main.take_output(),
            Step::Idle => panic!("{STALL_MESSAGE}"),
        }
    }
}

/// Waits until `deadline`: lets the clock jump there if it can (a
/// `MockClock`), else parks the thread (or until a remote wake).
fn wait_until(clock: &dyn Clock, deadline: Instant) {
    let now = clock.now();
    if deadline <= now || clock.advance_when_idle(deadline) {
        return;
    }
    park::park(Some(deadline.saturating_duration_since(now)));
}
