//! Tasks, `spawn`, and `JoinHandle`.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::context;
use crate::slab::Key;
use crate::waker::TaskHeader;

/// Identifies a task within its runtime: a slab key (index + generation).
pub(crate) type TaskId = Key;

/// The id used for the `block_on` main future, which never lives in the slab.
pub(crate) const MAIN_TASK: TaskId = Key::SENTINEL;

/// A type-erased task future. Its output is written into a [`JoinState`].
pub(crate) type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;

/// A task's entry in the runtime's slab.
pub(crate) struct TaskSlot {
    /// `None` while the task is being polled (or after its poll panicked).
    pub(crate) future: Option<BoxFuture>,
    /// The slab holds exactly one strong reference; every other reference is
    /// an outstanding `Waker`.
    pub(crate) header: Arc<TaskHeader>,
}

enum JoinState<T> {
    /// Still running. Holds the waker of whoever is awaiting the handle.
    Pending { waker: Option<Waker> },
    /// Finished; the output is waiting to be taken.
    Ready(T),
    /// The output was taken by awaiting the handle.
    Consumed,
}

/// Wraps a user future so that its output is delivered to its `JoinHandle`.
pub(crate) fn wrap<F>(fut: F) -> (BoxFuture, JoinHandle<F::Output>)
where
    F: Future + 'static,
    F::Output: 'static,
{
    let state = Rc::new(RefCell::new(JoinState::Pending { waker: None }));
    let task_state = Rc::clone(&state);
    let wrapped = Box::pin(async move {
        // `fut` (and everything it owns) is dropped as soon as it completes,
        // before the joiner is woken.
        let output = fut.await;
        complete(&task_state, output);
    });
    (wrapped, JoinHandle { state })
}

fn complete<T>(state: &Rc<RefCell<JoinState<T>>>, output: T) {
    if Rc::strong_count(state) == 1 {
        // The handle was dropped (the task is detached): nobody wants the
        // output. Drop it here, outside of any borrow.
        drop(output);
        return;
    }
    let previous = std::mem::replace(&mut *state.borrow_mut(), JoinState::Ready(output));
    match previous {
        JoinState::Pending { waker } => {
            if let Some(waker) = waker {
                // Waking the joiner queues it as a microtask, just like a
                // promise resolving.
                waker.wake();
            }
        }
        JoinState::Ready(_) | JoinState::Consumed => {
            unreachable!("spool: task completed twice")
        }
    }
}

/// An owned handle to a spawned task, which can be awaited to get the task's
/// output.
///
/// Dropping a `JoinHandle` **detaches** the task: it keeps running, and its
/// output is dropped when it finishes. (JS promises can't be cancelled either.)
///
/// `JoinHandle` is `!Send`: it belongs to the runtime's thread.
///
/// # Panics
///
/// Polling a `JoinHandle` again after it has returned the output panics.
///
/// # Example
///
/// ```
/// let answer = spool::block_on(async {
///     let handle = spool::spawn(async { 6 * 7 });
///     handle.await
/// });
/// assert_eq!(answer, 42);
/// ```
#[must_use = "dropping a JoinHandle detaches the task; `.await` it to get its output"]
pub struct JoinHandle<T> {
    state: Rc<RefCell<JoinState<T>>>,
}

impl<T> JoinHandle<T> {
    /// Returns `true` if the task has finished.
    ///
    /// ```
    /// spool::block_on(async {
    ///     let handle = spool::spawn(async {});
    ///     assert!(!handle.is_finished());
    ///     spool::yield_now().await;
    ///     assert!(handle.is_finished());
    /// });
    /// ```
    pub fn is_finished(&self) -> bool {
        !matches!(&*self.state.borrow(), JoinState::Pending { .. })
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut state = self.state.borrow_mut();
        match &mut *state {
            JoinState::Pending { waker } => {
                match waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => *waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
            JoinState::Ready(_) => match std::mem::replace(&mut *state, JoinState::Consumed) {
                JoinState::Ready(output) => Poll::Ready(output),
                _ => unreachable!(),
            },
            JoinState::Consumed => panic!("spool: `JoinHandle` polled after it completed"),
        }
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("finished", &self.is_finished())
            .finish()
    }
}

/// Spawns a task onto the current runtime.
///
/// The future doesn't need to be `Send`. Its first poll is queued as a
/// microtask, so it runs after the current task yields, in FIFO order with
/// other queued work. (Unlike a JS async function, it does *not* run
/// synchronously up to its first `.await`; see design decision D4.)
///
/// # Panics
///
/// If called outside a Spool runtime.
///
/// # Example
///
/// ```
/// use std::{cell::RefCell, rc::Rc};
///
/// spool::block_on(async {
///     let log = Rc::new(RefCell::new(Vec::new()));
///     let l = log.clone();
///     let handle = spool::spawn(async move { l.borrow_mut().push("task") });
///     log.borrow_mut().push("main");
///     handle.await;
///     assert_eq!(*log.borrow(), ["main", "task"]);
/// });
/// ```
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    context::current("spawn").spawn(future)
}
