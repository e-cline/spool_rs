use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Yields to the microtask queue, like `await null` in JS.
///
/// The current task is re-queued at the **back** of the microtask queue, so
/// every task and microtask queued before it runs first.
///
/// This is a *microtask* yield: a task that loops on `yield_now` starves
/// timers, exactly like a JS microtask loop does.
///
/// ```
/// use std::{cell::RefCell, rc::Rc};
///
/// spool::block_on(async {
///     let log = Rc::new(RefCell::new(Vec::new()));
///     let l = log.clone();
///     let task = spool::spawn(async move { l.borrow_mut().push("task") });
///     log.borrow_mut().push("before yield");
///     spool::yield_now().await;
///     log.borrow_mut().push("after yield");
///     task.await;
///     assert_eq!(*log.borrow(), ["before yield", "task", "after yield"]);
/// });
/// ```
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// The future returned by [`yield_now`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
