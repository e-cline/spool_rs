//! `KeepAlive` guards: keep `run` from exiting while work happens elsewhere.

use std::fmt;
use std::sync::Arc;

use crate::context;
use crate::waker::Shared;

/// Keeps [`run`](crate::run) from exiting while it exists, like a ref'd
/// handle (a timer or a socket) keeps a Node.js process alive.
///
/// A `run` loop with nothing left to do normally returns, dropping any task
/// that is still pending: a task waiting on something the runtime doesn't
/// know about, such as a `std::thread` sending it a result, doesn't count as
/// work. Hold a `KeepAlive` for as long as such a wake may still arrive. While
/// it exists, an idle loop parks instead of exiting. Dropping the last guard
/// wakes the loop, which then exits if it has nothing else to do.
///
/// `KeepAlive` is `Send + Sync`, so it can be handed to the thread doing the
/// work, and `Clone`: the loop stays alive until every clone is dropped. Drop
/// it *after* waking the task, so that the wake is seen first.
///
/// It has no effect on [`block_on`](crate::block_on), which returns as soon
/// as its future completes, or after the runtime has been dropped.
///
/// A `KeepAlive` that is never dropped keeps `run` parked forever.
///
/// ```
/// use std::sync::{Arc, Mutex};
/// use std::task::{Poll, Waker};
/// use std::{cell::Cell, rc::Rc};
///
/// // A minimal cross-thread slot: a value, and the waker of its reader.
/// let slot: Arc<Mutex<(Option<u32>, Option<Waker>)>> = Arc::default();
/// let got = Rc::new(Cell::new(0));
/// let (reader, g) = (slot.clone(), got.clone());
/// spool::run(async move {
///     // A detached task, waiting on the slot.
///     drop(spool::spawn(async move {
///         let value = std::future::poll_fn(|cx| {
///             let mut s = reader.lock().unwrap();
///             match s.0.take() {
///                 Some(v) => Poll::Ready(v),
///                 None => {
///                     s.1 = Some(cx.waker().clone());
///                     Poll::Pending
///                 }
///             }
///         })
///         .await;
///         g.set(value);
///     }));
///     let keep = spool::keep_alive();
///     std::thread::spawn(move || {
///         let waker = {
///             let mut s = slot.lock().unwrap();
///             s.0 = Some(42);
///             s.1.take()
///         };
///         if let Some(waker) = waker {
///             waker.wake();
///         }
///         drop(keep); // Last, after the wake.
///     });
///     // `main` is done here; without `keep`, `run` would return and drop
///     // the waiting task.
/// });
/// assert_eq!(got.get(), 42);
/// ```
#[must_use = "the loop is only kept alive while the guard exists"]
pub struct KeepAlive {
    shared: Arc<Shared>,
}

impl KeepAlive {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        shared.acquire_keep_alive();
        KeepAlive { shared }
    }
}

impl Clone for KeepAlive {
    fn clone(&self) -> Self {
        KeepAlive::new(Arc::clone(&self.shared))
    }
}

impl Drop for KeepAlive {
    fn drop(&mut self) {
        self.shared.release_keep_alive();
    }
}

impl fmt::Debug for KeepAlive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeepAlive")
            .field("runtime", &self.shared.runtime_id)
            .finish()
    }
}

/// Returns a [`KeepAlive`] guard for the current runtime.
///
/// # Panics
///
/// If called outside a Spool runtime. Use
/// [`Runtime::keep_alive`](crate::Runtime::keep_alive) to create one before
/// the runtime is running.
pub fn keep_alive() -> KeepAlive {
    context::current("keep_alive").keep_alive()
}
