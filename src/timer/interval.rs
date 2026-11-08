use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use super::next_interval_deadline;
use super::sleep::{Sleep, sleep};
use crate::clock::Instant;

/// Creates an [`Interval`] that ticks every `period`. The first tick
/// completes immediately.
///
/// This is the future-style counterpart of [`set_interval`](crate::set_interval),
/// with the same fixed schedule: created at `t0`, the ticks are due at `t0`,
/// `t0 + period`, `t0 + 2·period`, and so on, however long the code between
/// ticks takes. If the caller falls behind by a whole period or more, the
/// missed ticks are skipped: the next `tick` completes once, and the one
/// after it waits for the first scheduled time that hasn't passed yet.
///
/// Dropping the `Interval` cancels it.
///
/// # Panics
///
/// If `period` is zero, or if called outside a Spool runtime.
///
/// ```
/// use std::time::Duration;
///
/// let clock = spool::MockClock::new();
/// let rt = spool::Runtime::builder().clock(clock.clone()).build();
/// rt.block_on(async {
///     let mut every_second = spool::interval(Duration::from_secs(1));
///     let start = every_second.tick().await; // Immediately.
///     every_second.tick().await;
///     let third = every_second.tick().await;
///     assert_eq!(third - start, Duration::from_secs(2));
/// });
/// ```
pub fn interval(period: Duration) -> Interval {
    assert!(
        !period.is_zero(),
        "spool: `interval` period must be non-zero"
    );
    Interval {
        sleep: sleep(period),
        period,
        first_tick: true,
    }
}

/// A stream of ticks, created by [`interval`].
///
/// `Interval` is `!Send`, and `Unpin`.
///
/// `tick` is cancel-safe, so an `Interval` works well in a loop that races
/// it against other work, here with [`timeout`](crate::timeout):
///
/// ```
/// use std::time::Duration;
///
/// let rt = spool::Runtime::builder().clock(spool::MockClock::new()).build();
/// rt.block_on(async {
///     let mut every_10s = spool::interval(Duration::from_secs(10));
///     assert_eq!(every_10s.period(), Duration::from_secs(10));
///     every_10s.tick().await; // The first tick is immediate.
///     let mut ticks = 0;
///     // Wait up to 3s at a time: the dropped `tick` futures lose nothing.
///     while ticks < 2 {
///         if spool::timeout(Duration::from_secs(3), every_10s.tick()).await.is_ok() {
///             ticks += 1;
///         }
///     }
///     assert_eq!(spool::now().epoch_offset(), Duration::from_secs(20));
/// });
/// ```
#[derive(Debug)]
pub struct Interval {
    sleep: Sleep,
    period: Duration,
    first_tick: bool,
}

impl Interval {
    /// Waits for the next tick, and returns the time it was scheduled for
    /// (which is earlier than [`now`](crate::now) if the tick is late).
    ///
    /// Cancel-safe: if the returned future is dropped before it completes,
    /// no tick is lost.
    pub async fn tick(&mut self) -> Instant {
        poll_fn(|cx| self.poll_tick(cx)).await
    }

    /// Polls for the next tick. See [`tick`](Self::tick).
    pub fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Instant> {
        let inner = self.sleep.runtime("Interval");
        if self.first_tick {
            self.first_tick = false;
            // The creation time: the sleep is armed one period later.
            let deadline = self.sleep.deadline();
            return Poll::Ready(deadline.checked_sub(self.period).unwrap_or(deadline));
        }
        ready!(Pin::new(&mut self.sleep).poll(cx));
        let scheduled = self.sleep.deadline();
        let next = next_interval_deadline(scheduled, self.period, inner.now());
        self.sleep.reset(next);
        Poll::Ready(scheduled)
    }

    /// The interval's period.
    pub fn period(&self) -> Duration {
        self.period
    }
}
