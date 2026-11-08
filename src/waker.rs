//! Thread-safe wakers for `!Send` tasks.
//!
//! `std::task::Waker` is always `Send + Sync`, so a waker must never point at
//! a task's (`!Send`) future. Instead, each task has an [`Arc<TaskHeader>`]
//! that holds only its id and some thread-safe state. The future itself stays
//! in the runtime's thread-local slab.
//!
//! Waking takes one of two paths:
//! - **Local**: the waker is woken on the runtime's own thread while that
//!   runtime is running. The task is pushed straight onto the microtask queue.
//! - **Remote**: anything else (another thread, or the runtime isn't running
//!   right now). The id goes onto a mutex-protected queue, and the runtime's
//!   thread is unparked. The loop moves remote wakes into the microtask queue.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Wake;
use std::thread::Thread;

use crate::context;
use crate::task::TaskId;

/// Runtime state that can be touched from any thread.
pub(crate) struct Shared {
    /// Unique per runtime, used to tell runtimes apart on the same thread.
    pub(crate) runtime_id: u64,
    /// The thread that owns the runtime (the only thread that runs it).
    pub(crate) thread: Thread,
    /// Ids woken from outside the local fast path.
    remote_queue: Mutex<Vec<TaskId>>,
    /// Fast-path flag: `true` if `remote_queue` may be non-empty.
    has_remote: AtomicBool,
    /// Cleared when the runtime is dropped; later remote wakes are discarded.
    alive: AtomicBool,
    /// The number of live `KeepAlive` guards.
    keep_alive: AtomicUsize,
}

impl Shared {
    pub(crate) fn new(runtime_id: u64) -> Self {
        Shared {
            runtime_id,
            thread: std::thread::current(),
            remote_queue: Mutex::new(Vec::new()),
            has_remote: AtomicBool::new(false),
            alive: AtomicBool::new(true),
            keep_alive: AtomicUsize::new(0),
        }
    }

    pub(crate) fn acquire_keep_alive(&self) {
        self.keep_alive.fetch_add(1, Ordering::AcqRel);
    }

    /// Drops one `KeepAlive`. When the last one goes, the loop is unparked so
    /// that it re-checks whether it should exit.
    pub(crate) fn release_keep_alive(&self) {
        if self.keep_alive.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.thread.unpark();
        }
    }

    /// Returns `true` if a remote wake is queued and not yet absorbed.
    pub(crate) fn has_pending_remote(&self) -> bool {
        self.has_remote.load(Ordering::Acquire)
    }

    pub(crate) fn is_kept_alive(&self) -> bool {
        self.keep_alive.load(Ordering::Acquire) > 0
    }

    fn push_remote(&self, id: TaskId) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        self.remote_queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(id);
        self.has_remote.store(true, Ordering::Release);
        self.thread.unpark();
    }

    /// Takes every remotely woken id, in wake order.
    pub(crate) fn take_remote(&self) -> Vec<TaskId> {
        if !self.has_remote.swap(false, Ordering::Acquire) {
            return Vec::new();
        }
        std::mem::take(&mut *self.remote_queue.lock().unwrap_or_else(|e| e.into_inner()))
    }

    pub(crate) fn shut_down(&self) {
        self.alive.store(false, Ordering::Release);
        self.remote_queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

/// The thread-safe part of a task, which backs its `Waker`.
pub(crate) struct TaskHeader {
    pub(crate) id: TaskId,
    /// `true` while the task is queued for a poll. Dedupes repeated wakes.
    scheduled: AtomicBool,
    shared: Arc<Shared>,
}

impl TaskHeader {
    /// Creates a header for a task that is about to be queued for its first
    /// poll (so it starts out `scheduled`).
    pub(crate) fn new_scheduled(id: TaskId, shared: Arc<Shared>) -> Self {
        TaskHeader {
            id,
            scheduled: AtomicBool::new(true),
            shared,
        }
    }

    /// Clears the scheduled flag. Called right before the task is polled, so
    /// that a wake during the poll queues it again.
    pub(crate) fn clear_scheduled(&self) {
        self.scheduled.store(false, Ordering::Release);
    }

    fn schedule(&self) {
        if self.scheduled.swap(true, Ordering::AcqRel) {
            return; // Already queued.
        }
        let queued_locally = context::try_schedule_local(self.shared.runtime_id, self.id);
        if !queued_locally {
            self.shared.push_remote(self.id);
        }
    }
}

impl Wake for TaskHeader {
    fn wake(self: Arc<Self>) {
        self.schedule();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.schedule();
    }
}
