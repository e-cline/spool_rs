//! The thread-local "current runtime".
//!
//! The free functions (`spawn`, `yield_now`, ...) find their runtime here.
//! A runtime is "entered" for the duration of `block_on`, and while it tears
//! down (so that `Drop` impls can still call into it).

use std::cell::RefCell;
use std::rc::Rc;

use crate::runtime::Inner;
use crate::task::TaskId;

thread_local! {
    static CURRENT: RefCell<Option<Rc<Inner>>> = const { RefCell::new(None) };
}

/// Restores the previous "current runtime" on drop (including on unwind).
pub(crate) struct EnterGuard {
    prev: Option<Rc<Inner>>,
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        // Drop the replaced value outside the TLS borrow.
        let _old = CURRENT.with(|c| std::mem::replace(&mut *c.borrow_mut(), prev));
    }
}

/// Makes `inner` the current runtime.
///
/// # Panics
///
/// If another runtime (or the same one) is already running on this thread.
pub(crate) fn enter(inner: &Rc<Inner>) -> EnterGuard {
    CURRENT.with(|c| {
        let mut slot = c.borrow_mut();
        assert!(
            slot.is_none(),
            "spool: cannot start a runtime from inside a running Spool runtime \
             (nested `block_on` is not supported)"
        );
        *slot = Some(Rc::clone(inner));
    });
    EnterGuard { prev: None }
}

/// Like [`enter`], but tolerates an already-current runtime and restores it
/// afterwards. Used for teardown, which may happen while another runtime runs.
pub(crate) fn enter_for_teardown(inner: &Rc<Inner>) -> EnterGuard {
    let prev = CURRENT.with(|c| c.borrow_mut().replace(Rc::clone(inner)));
    EnterGuard { prev }
}

/// Returns the current runtime.
///
/// # Panics
///
/// If called outside a Spool runtime. `what` names the API for the message.
pub(crate) fn current(what: &str) -> Rc<Inner> {
    CURRENT
        .with(|c| c.borrow().clone())
        .unwrap_or_else(|| panic!("spool: `{what}` called outside of a Spool runtime"))
}

/// Queues `id` on the current runtime if it is the runtime `runtime_id`.
/// Returns `false` if that runtime isn't current on this thread (the caller
/// then falls back to the remote path).
pub(crate) fn try_schedule_local(runtime_id: u64, id: TaskId) -> bool {
    CURRENT
        .try_with(|c| match &*c.borrow() {
            Some(inner) if inner.id() == runtime_id => {
                inner.schedule_local(id);
                true
            }
            _ => false,
        })
        .unwrap_or(false)
}
