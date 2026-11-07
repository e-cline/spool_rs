//! The runtime and its event loop.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use crate::context;
use crate::queue::{Microtask, MicrotaskQueue};
use crate::slab::Slab;
use crate::task::{self, JoinHandle, MAIN_TASK, TaskId, TaskSlot};
use crate::waker::{Shared, TaskHeader};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

const STALL_MESSAGE: &str = "spool: main future stalled: it is pending but nothing can wake it \
     (no queued work, no timers, and no outstanding wakers)";

/// The runtime's thread-local state. Never shared across threads.
pub(crate) struct Inner {
    microtasks: MicrotaskQueue,
    tasks: RefCell<Slab<TaskSlot>>,
    shared: Arc<Shared>,
}

impl Inner {
    fn new() -> Self {
        let id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
        Inner {
            microtasks: MicrotaskQueue::default(),
            tasks: RefCell::new(Slab::new()),
            shared: Arc::new(Shared::new(id)),
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.shared.runtime_id
    }

    /// Queues a poll of `id`. Only called on the runtime's own thread.
    pub(crate) fn schedule_local(&self, id: TaskId) {
        self.microtasks.push(Microtask::Poll(id));
    }

    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let (wrapped, handle) = task::wrap(future);
        let id = {
            let mut tasks = self.tasks.borrow_mut();
            let id = tasks.next_key();
            let header = Arc::new(TaskHeader::new_scheduled(id, Arc::clone(&self.shared)));
            let inserted = tasks.insert(TaskSlot {
                future: Some(wrapped),
                header,
            });
            debug_assert_eq!(id, inserted);
            id
        };
        self.schedule_local(id);
        handle
    }

    /// Polls one spawned task. Holds no `RefCell` borrow while user code runs.
    fn poll_task(&self, id: TaskId) {
        let taken = {
            let mut tasks = self.tasks.borrow_mut();
            tasks.get_mut(id).and_then(|slot| {
                slot.future
                    .take()
                    .map(|future| (future, Arc::clone(&slot.header)))
            })
        };
        // A stale id (the task already finished) is a no-op.
        let Some((mut future, header)) = taken else {
            return;
        };

        // Clear before polling, so that a wake during the poll re-queues it.
        header.clear_scheduled();
        let waker = Waker::from(header);
        let poll = future.as_mut().poll(&mut Context::from_waker(&waker));
        drop(waker);

        match poll {
            Poll::Ready(()) => {
                drop(future);
                let slot = self.tasks.borrow_mut().remove(id);
                drop(slot);
            }
            Poll::Pending => {
                let leftover = match self.tasks.borrow_mut().get_mut(id) {
                    Some(slot) => {
                        slot.future = Some(future);
                        None
                    }
                    None => Some(future),
                };
                drop(leftover);
            }
        }
    }

    /// Moves remotely woken tasks onto the microtask queue. Returns `true` if
    /// anything was moved.
    fn absorb_remote(&self) -> bool {
        let woken = self.shared.take_remote();
        let any = !woken.is_empty();
        for id in woken {
            self.schedule_local(id);
        }
        any
    }

    /// Drops every task and queued microtask, repeating until nothing is left
    /// (a future's `Drop` may spawn more tasks).
    fn tear_down(&self) {
        loop {
            self.microtasks.clear();
            let slots = self.tasks.borrow_mut().drain();
            if slots.is_empty() {
                break;
            }
            drop(slots);
        }
        self.microtasks.clear();
    }
}

/// A single-threaded Spool runtime.
///
/// Most programs can use the [`block_on`](crate::block_on) free function
/// instead. A `Runtime` is useful when you want to spawn tasks before running,
/// or to run several `block_on` calls on the same runtime.
///
/// `Runtime` is `!Send` and `!Sync`. Dropping it drops every task that is
/// still pending.
///
/// ```
/// let rt = spool::Runtime::new();
/// let handle = rt.spawn(async { 1 + 1 });
/// assert_eq!(rt.block_on(handle), 2);
/// ```
pub struct Runtime {
    inner: Rc<Inner>,
}

impl Runtime {
    /// Creates a new runtime on the current thread.
    pub fn new() -> Self {
        Runtime {
            inner: Rc::new(Inner::new()),
        }
    }

    /// Spawns a task onto this runtime. The task runs once the runtime is
    /// driven by [`block_on`](Self::block_on).
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.inner.spawn(future)
    }

    /// Runs `future` to completion on this runtime, driving spawned tasks
    /// along the way, and returns its output.
    ///
    /// Returns as soon as `future` completes. Tasks that are still pending
    /// stay on the runtime: a later `block_on` keeps driving them, and
    /// dropping the runtime drops them.
    ///
    /// # Panics
    ///
    /// - If called from inside a running Spool runtime (nested `block_on`).
    /// - If `future` stalls: it is pending, no work is queued, and no `Waker`
    ///   for it exists anywhere, so nothing can ever wake it.
    /// - If any task panics (the panic unwinds out of `block_on`).
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _enter = context::enter(&self.inner);
        let inner = &*self.inner;
        let mut main = pin!(future);
        let main_header = Arc::new(TaskHeader::new_scheduled(
            MAIN_TASK,
            Arc::clone(&inner.shared),
        ));
        inner.schedule_local(MAIN_TASK);

        loop {
            while let Some(microtask) = inner.microtasks.pop() {
                match microtask {
                    Microtask::Poll(id) if id == MAIN_TASK => {
                        main_header.clear_scheduled();
                        let waker = Waker::from(Arc::clone(&main_header));
                        if let Poll::Ready(output) =
                            main.as_mut().poll(&mut Context::from_waker(&waker))
                        {
                            return output;
                        }
                    }
                    Microtask::Poll(id) => inner.poll_task(id),
                }
            }

            if inner.absorb_remote() {
                continue;
            }

            // Idle: nothing is queued. Main can only make progress if someone
            // still holds one of its wakers (the slab-free header is held
            // once by this frame). That someone may be another thread, so
            // park until a remote wake arrives.
            if Arc::strong_count(&main_header) > 1 {
                std::thread::park();
                continue;
            }
            panic!("{STALL_MESSAGE}");
        }
    }

    #[cfg(test)]
    pub(crate) fn task_count(&self) -> usize {
        self.inner.tasks.borrow().len()
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Enter the runtime so that `Drop` impls of pending futures can still
        // call `spawn` and friends.
        let _enter = context::enter_for_teardown(&self.inner);
        self.inner.tear_down();
        self.inner.shared.shut_down();
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("id", &self.inner.id())
            .field("tasks", &self.inner.tasks.borrow().len())
            .field("microtasks_empty", &self.inner.microtasks.is_empty())
            .finish()
    }
}

/// Runs `future` to completion on a new runtime and returns its output.
///
/// This is shorthand for `Runtime::new().block_on(future)`. It returns as soon
/// as `future` completes; any tasks still pending are dropped with the
/// runtime.
///
/// # Panics
///
/// See [`Runtime::block_on`].
///
/// ```
/// use std::rc::Rc;
///
/// // Futures don't need to be `Send`.
/// let shared = Rc::new(5);
/// let doubled = spool::block_on(async move { *shared * 2 });
/// assert_eq!(doubled, 10);
/// ```
pub fn block_on<F: Future>(future: F) -> F::Output {
    Runtime::new().block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finished_tasks_are_removed_from_the_slab() {
        let rt = Runtime::new();
        let handles: Vec<_> = (0..10).map(|i| rt.spawn(async move { i })).collect();
        assert_eq!(rt.task_count(), 10);
        let sum = rt.block_on(async {
            let mut sum = 0;
            for h in handles {
                sum += h.await;
            }
            sum
        });
        assert_eq!(sum, 45);
        assert_eq!(rt.task_count(), 0);
    }

    #[test]
    fn duplicate_wakes_queue_one_poll() {
        let rt = Runtime::new();
        let polls = Rc::new(std::cell::Cell::new(0));
        let p = polls.clone();
        let handle = rt.spawn(std::future::poll_fn(move |cx| {
            p.set(p.get() + 1);
            if p.get() == 1 {
                cx.waker().wake_by_ref();
                cx.waker().wake_by_ref();
                #[allow(clippy::waker_clone_wake)] // Deliberately tests by-value `wake`.
                cx.waker().clone().wake();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }));
        rt.block_on(handle);
        assert_eq!(polls.get(), 2);
        assert!(rt.inner.microtasks.is_empty());
    }

    #[test]
    fn stale_wake_after_completion_is_ignored() {
        let rt = Runtime::new();
        let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
        let s = stash.clone();
        let first = rt.spawn(std::future::poll_fn(move |cx| {
            *s.borrow_mut() = Some(cx.waker().clone());
            Poll::Ready(())
        }));
        rt.block_on(first);
        // The slot is reused by a new task with a new generation.
        let second = rt.spawn(async { 7 });
        stash.borrow_mut().take().unwrap().wake();
        assert_eq!(rt.block_on(second), 7);
        assert_eq!(rt.task_count(), 0);
    }
}
