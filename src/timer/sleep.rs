use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use super::TimerState;
use crate::clock::Instant;
use crate::context;
use crate::runtime::Inner;
use crate::slab::Key;

/// Waits for `duration`, like `await new Promise(r => setTimeout(r, ms))`.
///
/// The timer starts when `sleep` is called, not when the future is first
/// polled. The future completes in a timers phase, so even a zero duration
/// yields past every queued tick and microtask:
/// `sleep(Duration::ZERO).await` is the way to yield to timers.
///
/// Dropping the `Sleep` cancels its timer.
///
/// # Panics
///
/// If called outside a Spool runtime.
///
/// ```
/// use std::time::Duration;
///
/// let clock = spool::MockClock::new();
/// let rt = spool::Runtime::builder().clock(clock.clone()).build();
/// rt.block_on(async {
///     let start = spool::now();
///     spool::sleep(Duration::from_millis(250)).await;
///     assert_eq!(start.elapsed(), Duration::from_millis(250));
/// });
/// ```
pub fn sleep(duration: Duration) -> Sleep {
    let inner = context::current("sleep");
    let deadline = inner.now().saturating_add(duration);
    Sleep::register(&inner, deadline)
}

/// Waits until `deadline` on the current runtime's clock. A deadline in the
/// past completes in the next timers phase.
///
/// # Panics
///
/// If called outside a Spool runtime.
pub fn sleep_until(deadline: Instant) -> Sleep {
    let inner = context::current("sleep_until");
    Sleep::register(&inner, deadline)
}

/// The future returned by [`sleep`] and [`sleep_until`].
///
/// `Sleep` is `!Send`, and `Unpin`.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Sleep {
    inner: Weak<Inner>,
    key: Key,
    deadline: Instant,
    /// The timer fired and this future returned `Ready`; its entry is gone.
    done: bool,
}

impl Sleep {
    fn register(inner: &Rc<Inner>, deadline: Instant) -> Sleep {
        let key = inner.timers().borrow_mut().insert(
            deadline,
            TimerState::Sleep {
                waker: None,
                fired: false,
            },
        );
        Sleep {
            inner: Rc::downgrade(inner),
            key,
            deadline,
            done: false,
        }
    }

    /// The instant this sleep completes at.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Returns `true` if the timer has fired.
    pub fn is_elapsed(&self) -> bool {
        if self.done {
            return true;
        }
        let Some(inner) = self.inner.upgrade() else {
            return false;
        };
        matches!(
            inner
                .timers()
                .borrow_mut()
                .get_mut(self.key)
                .map(|e| &e.state),
            Some(TimerState::Sleep { fired: true, .. })
        )
    }

    /// Restarts the sleep with a new deadline, whether or not it has
    /// completed. The timer is re-armed as a new timer (so it is ordered
    /// after timers already armed for the same deadline).
    ///
    /// # Panics
    ///
    /// If the runtime has been dropped.
    pub fn reset(&mut self, deadline: Instant) {
        let inner = self.runtime("Sleep::reset");
        self.cancel(&inner);
        *self = Sleep::register(&inner, deadline);
    }

    pub(crate) fn runtime(&self, what: &str) -> Rc<Inner> {
        self.inner
            .upgrade()
            .unwrap_or_else(|| panic!("spool: `{what}` used after its runtime was dropped"))
    }

    fn cancel(&mut self, inner: &Inner) {
        if !self.done {
            self.done = true;
            let removed = inner.timers().borrow_mut().remove(self.key);
            drop(removed);
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.done {
            return Poll::Ready(());
        }
        let inner = self.runtime("Sleep");
        let mut timers = inner.timers().borrow_mut();
        let fired = match timers.get_mut(self.key).map(|e| &mut e.state) {
            Some(TimerState::Sleep { fired: true, .. }) | None => true,
            Some(TimerState::Sleep { waker, .. }) => {
                match waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => *waker = Some(cx.waker().clone()),
                }
                false
            }
            Some(_) => unreachable!("spool: a Sleep's key points at a non-sleep timer"),
        };
        if !fired {
            return Poll::Pending;
        }
        let removed = timers.remove(self.key);
        drop(timers);
        drop(removed);
        self.done = true;
        Poll::Ready(())
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            self.cancel(&inner);
        }
    }
}

impl std::fmt::Debug for Sleep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sleep")
            .field("deadline", &self.deadline)
            .field("elapsed", &self.is_elapsed())
            .finish()
    }
}
