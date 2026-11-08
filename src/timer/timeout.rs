use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use super::sleep::{Sleep, sleep, sleep_until};
use crate::clock::Instant;

/// Runs `future`, giving up after `duration`.
///
/// Returns `Ok(output)` if `future` completes first, or `Err(Elapsed)` if the
/// time runs out first. When the time runs out, `future` is dropped right
/// away. If both are ready at the same poll, the output wins.
///
/// # Panics
///
/// If called outside a Spooly runtime.
///
/// ```
/// use std::time::Duration;
///
/// let clock = spooly::MockClock::new();
/// let rt = spooly::Runtime::builder().clock(clock).build();
/// rt.block_on(async {
///     let slow = spooly::sleep(Duration::from_secs(10));
///     let result = spooly::timeout(Duration::from_secs(1), slow).await;
///     assert_eq!(result, Err(spooly::Elapsed::new()));
///
///     let quick = async { 7 };
///     assert_eq!(spooly::timeout(Duration::from_secs(1), quick).await, Ok(7));
/// });
/// ```
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout {
        future: Some(Box::pin(future)),
        sleep: sleep(duration),
    }
}

/// Like [`timeout`], with an absolute deadline.
///
/// ```
/// use std::time::Duration;
///
/// let rt = spooly::Runtime::builder().clock(spooly::MockClock::new()).build();
/// rt.block_on(async {
///     // One deadline shared by several steps.
///     let deadline = spooly::now() + Duration::from_secs(5);
///     let first = spooly::timeout_at(deadline, spooly::sleep(Duration::from_secs(3))).await;
///     assert!(first.is_ok());
///     let second = spooly::timeout_at(deadline, spooly::sleep(Duration::from_secs(3))).await;
///     assert!(second.is_err(), "only 2s were left");
///     assert_eq!(spooly::now(), deadline);
/// });
/// ```
///
/// # Panics
///
/// If called outside a Spooly runtime.
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
    Timeout {
        future: Some(Box::pin(future)),
        sleep: sleep_until(deadline),
    }
}

/// The future returned by [`timeout`] and [`timeout_at`].
///
/// The inner future is boxed, which keeps `Timeout` free of `unsafe` pin
/// projection. (Plan §9.6 lists removing this allocation.)
///
/// The timer starts when the `Timeout` is created, not when it is first
/// polled:
///
/// ```
/// use std::time::Duration;
///
/// let rt = spooly::Runtime::builder().clock(spooly::MockClock::new()).build();
/// rt.block_on(async {
///     let t = spooly::timeout(Duration::from_secs(2), async { "done" });
///     assert_eq!(t.deadline().epoch_offset(), Duration::from_secs(2));
///     assert_eq!(t.await, Ok("done"));
/// });
/// ```
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Timeout<F: Future> {
    future: Option<Pin<Box<F>>>,
    sleep: Sleep,
}

impl<F: Future> Timeout<F> {
    /// The instant the timeout elapses at.
    pub fn deadline(&self) -> Instant {
        self.sleep.deadline()
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let future = this
            .future
            .as_mut()
            .expect("spooly: `Timeout` polled after it completed");
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            this.future = None;
            return Poll::Ready(Ok(output));
        }
        if Pin::new(&mut this.sleep).poll(cx).is_ready() {
            this.future = None; // Drop the inner future right away.
            return Poll::Ready(Err(Elapsed(())));
        }
        Poll::Pending
    }
}

impl<F: Future> fmt::Debug for Timeout<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timeout")
            .field("deadline", &self.deadline())
            .field("completed", &self.future.is_none())
            .finish()
    }
}

/// The error returned by [`timeout`] when the time runs out.
///
/// ```
/// let err = spooly::Elapsed::new();
/// assert_eq!(err.to_string(), "deadline has elapsed");
/// let boxed: Box<dyn std::error::Error> = Box::new(err); // Works with `?`.
/// # drop(boxed);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Elapsed(());

impl Elapsed {
    /// Creates an `Elapsed` error (mostly useful in tests).
    pub fn new() -> Self {
        Elapsed(())
    }
}

impl Default for Elapsed {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl Error for Elapsed {}
