//! The microtask queue.
//!
//! In JS, the code after an `await` resumes as a promise job on the microtask
//! queue. In Spool, the matching event is a task's `Waker` firing, so a woken
//! task is queued here as a [`Microtask::Poll`] (design decision D3).

use std::cell::RefCell;
use std::collections::VecDeque;

use crate::task::TaskId;

/// One entry in the microtask queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Microtask {
    /// Poll the task with this id.
    Poll(TaskId),
}

/// A FIFO microtask queue. Borrows are never held while user code runs.
#[derive(Default)]
pub(crate) struct MicrotaskQueue {
    queue: RefCell<VecDeque<Microtask>>,
}

impl MicrotaskQueue {
    pub(crate) fn push(&self, task: Microtask) {
        self.queue.borrow_mut().push_back(task);
    }

    pub(crate) fn pop(&self) -> Option<Microtask> {
        self.queue.borrow_mut().pop_front()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.borrow().is_empty()
    }

    /// Removes every queued entry.
    pub(crate) fn clear(&self) {
        // Entries are plain ids, so dropping them runs no user code.
        self.queue.borrow_mut().clear();
    }
}
