//! The tick and microtask queues, and their public APIs.
//!
//! In JS, the code after an `await` resumes as a promise job on the microtask
//! queue. In Spool, the matching event is a task's `Waker` firing, so a woken
//! task is queued on the microtask queue as a [`Microtask::Poll`], in the same
//! FIFO order as [`queue_microtask`] callbacks (design decision D3).
//!
//! `next_tick` callbacks live in a separate queue that is drained before the
//! microtask queue, following Node's `processTicksAndRejections` (§3.2).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;

use crate::context;
use crate::task::TaskId;

/// A boxed one-shot callback.
pub(crate) type Callback = Box<dyn FnOnce()>;

/// One entry in the microtask queue.
pub(crate) enum Microtask {
    /// Poll the task with this id.
    Poll(TaskId),
    /// Run a `queue_microtask` callback.
    Callback(Callback),
}

impl fmt::Debug for Microtask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Microtask::Poll(id) => f.debug_tuple("Poll").field(id).finish(),
            Microtask::Callback(_) => f.write_str("Callback(..)"),
        }
    }
}

/// A FIFO queue behind a `RefCell`. Borrows are only held inside these
/// methods, never while user code runs.
pub(crate) struct Queue<T> {
    items: RefCell<VecDeque<T>>,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Queue {
            items: RefCell::new(VecDeque::new()),
        }
    }
}

impl<T> Queue<T> {
    pub(crate) fn push(&self, item: T) {
        self.items.borrow_mut().push_back(item);
    }

    pub(crate) fn pop(&self) -> Option<T> {
        self.items.borrow_mut().pop_front()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.items.borrow().len()
    }

    /// Removes and returns everything queued. The caller drops the items
    /// *after* the borrow is released, so a `Drop` impl that queues more work
    /// doesn't hit a `BorrowMutError`.
    pub(crate) fn take_all(&self) -> VecDeque<T> {
        std::mem::take(&mut *self.items.borrow_mut())
    }
}

/// Queues `callback` to run as a microtask, like JS's `queueMicrotask`.
///
/// Microtasks run in FIFO order, interleaved with woken tasks (which are also
/// microtasks). The microtask queue is drained completely, including
/// microtasks queued while draining, before the runtime moves on to the next
/// round of [`next_tick`] callbacks or to the next macrotask.
///
/// Microtasks can't be cancelled.
///
/// # Panics
///
/// If called outside a Spool runtime. A panic inside `callback` unwinds out of
/// `run` / `block_on`, like an uncaught exception in JS.
///
/// # Example
///
/// ```
/// use std::{cell::RefCell, rc::Rc};
///
/// let log = Rc::new(RefCell::new(Vec::new()));
/// let l = log.clone();
/// spool::run(async move {
///     let l2 = l.clone();
///     spool::queue_microtask(move || l2.borrow_mut().push("microtask"));
///     l.borrow_mut().push("sync");
/// });
/// assert_eq!(*log.borrow(), ["sync", "microtask"]);
/// ```
pub fn queue_microtask(callback: impl FnOnce() + 'static) {
    context::current("queue_microtask").queue_microtask(Box::new(callback));
}

/// Queues `callback` to run on the tick queue, like Node's `process.nextTick`.
///
/// The tick queue is drained completely (including ticks queued while
/// draining) **before** the microtask queue. A tick queued from inside a
/// microtask runs after the whole microtask queue has drained, but before the
/// next macrotask.
///
/// Ticks can't be cancelled.
///
/// # Panics
///
/// If called outside a Spool runtime. A panic inside `callback` unwinds out of
/// `run` / `block_on`.
///
/// # Example
///
/// ```
/// use std::{cell::RefCell, rc::Rc};
///
/// let log = Rc::new(RefCell::new(Vec::new()));
/// let l = log.clone();
/// spool::run(async move {
///     let (a, b) = (l.clone(), l.clone());
///     spool::queue_microtask(move || a.borrow_mut().push("microtask"));
///     spool::next_tick(move || b.borrow_mut().push("tick"));
///     l.borrow_mut().push("sync");
/// });
/// // Ticks run before microtasks, even when queued after them.
/// assert_eq!(*log.borrow(), ["sync", "tick", "microtask"]);
/// ```
pub fn next_tick(callback: impl FnOnce() + 'static) {
    context::current("next_tick").next_tick(Box::new(callback));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_is_fifo() {
        let q = Queue::default();
        for i in 0..5 {
            q.push(i);
        }
        assert_eq!(q.len(), 5);
        let out: Vec<_> = std::iter::from_fn(|| q.pop()).collect();
        assert_eq!(out, [0, 1, 2, 3, 4]);
        assert!(q.is_empty());
    }

    #[test]
    fn take_all_releases_the_borrow_before_items_drop() {
        use std::rc::Rc;

        // An item whose `Drop` pushes onto the very queue it came from.
        struct PushOnDrop(Option<Rc<Queue<PushOnDrop>>>);
        impl Drop for PushOnDrop {
            fn drop(&mut self) {
                if let Some(q) = self.0.take() {
                    q.push(PushOnDrop(None));
                }
            }
        }
        let q = Rc::new(Queue::default());
        q.push(PushOnDrop(Some(q.clone())));
        drop(q.take_all()); // Must not panic with `BorrowMutError`.
        assert_eq!(q.len(), 1, "the drop-time push landed");
    }

    #[test]
    fn microtask_debug() {
        let cb = Microtask::Callback(Box::new(|| {}));
        assert_eq!(format!("{cb:?}"), "Callback(..)");
    }
}
