//! Time: Spooly's own [`Instant`], the [`Clock`] trait, and the two clocks.
//!
//! Spooly has its own `Instant` instead of using `std::time::Instant`, because
//! `std`'s panics on `wasm32-unknown-unknown`. In the browser, a future clock
//! will read `performance.now()` through the same [`Clock`] trait.

use std::cell::Cell;
use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::rc::Rc;
use std::time::Duration;

use crate::context;

/// A point in time on a runtime's clock.
///
/// An `Instant` is an offset from its clock's epoch (the moment the clock
/// was created), so it is only meaningful together with the runtime whose
/// clock produced it. Use [`now`] to read the current runtime's clock.
///
/// Arithmetic that would overflow panics, like `std::time::Instant`. Use the
/// `checked_*` methods to avoid that.
///
/// ```
/// use std::time::Duration;
/// use spooly::Instant;
///
/// let a = Instant::from_epoch_offset(Duration::from_millis(250));
/// let b = a + Duration::from_millis(750);
/// assert_eq!(b.epoch_offset(), Duration::from_secs(1));
/// assert_eq!(b - a, Duration::from_millis(750));
/// assert_eq!(a.checked_duration_since(b), None); // `b` is later.
/// assert_eq!(a.saturating_duration_since(b), Duration::ZERO);
/// assert_eq!(Instant::MAX.checked_add(Duration::from_secs(1)), None);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Instant(Duration);

impl Instant {
    /// The clock's epoch.
    pub const ZERO: Instant = Instant(Duration::ZERO);

    /// The latest representable instant. Timers with a deadline this far out
    /// never fire in practice.
    pub const MAX: Instant = Instant(Duration::MAX);

    /// Creates an instant `offset` after the clock's epoch.
    pub const fn from_epoch_offset(offset: Duration) -> Self {
        Instant(offset)
    }

    /// The time since the clock's epoch.
    pub const fn epoch_offset(self) -> Duration {
        self.0
    }

    /// The time from `earlier` to `self`. Panics if `earlier` is later than
    /// `self`.
    pub fn duration_since(self, earlier: Instant) -> Duration {
        self.checked_duration_since(earlier)
            .expect("spooly: `earlier` is later than `self`")
    }

    /// The time from `earlier` to `self`, or `None` if `earlier` is later.
    pub fn checked_duration_since(self, earlier: Instant) -> Option<Duration> {
        self.0.checked_sub(earlier.0)
    }

    /// The time from `earlier` to `self`, or zero if `earlier` is later.
    pub fn saturating_duration_since(self, earlier: Instant) -> Duration {
        self.0.saturating_sub(earlier.0)
    }

    /// `self + d`, or `None` on overflow.
    pub fn checked_add(self, d: Duration) -> Option<Instant> {
        self.0.checked_add(d).map(Instant)
    }

    /// `self - d`, or `None` if that would be before the epoch.
    pub fn checked_sub(self, d: Duration) -> Option<Instant> {
        self.0.checked_sub(d).map(Instant)
    }

    /// `self + d`, saturating at [`Instant::MAX`].
    pub fn saturating_add(self, d: Duration) -> Instant {
        Instant(self.0.saturating_add(d))
    }

    /// The time elapsed since `self`, on the current runtime's clock
    /// (zero if `self` is in the future).
    ///
    /// # Panics
    ///
    /// If called outside a Spooly runtime.
    pub fn elapsed(self) -> Duration {
        now().saturating_duration_since(self)
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Instant(epoch + {:?})", self.0)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;
    fn add(self, d: Duration) -> Instant {
        self.checked_add(d)
            .expect("spooly: overflow when adding a duration to an instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, d: Duration) {
        *self = *self + d;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;
    fn sub(self, d: Duration) -> Instant {
        self.checked_sub(d)
            .expect("spooly: overflow when subtracting a duration from an instant")
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, d: Duration) {
        *self = *self - d;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;
    fn sub(self, earlier: Instant) -> Duration {
        self.duration_since(earlier)
    }
}

/// A source of time for a runtime.
///
/// Spooly ships with [`SystemClock`] (the default natively and on WASI) and
/// [`MockClock`] (for tests). Pass a clock to [`Builder::clock`](crate::Builder::clock).
///
/// A custom clock only needs [`now`](Clock::now). When the runtime is idle
/// until a timer, it sleeps for `deadline - now()` of real time, so a custom
/// clock should advance at real-time speed (or jump ahead itself, in
/// [`advance_when_idle`](Clock::advance_when_idle), like [`MockClock`]).
/// Here, a real-time clock whose epoch is an hour in the past:
///
/// ```
/// use std::time::Duration;
/// use spooly::{Clock, Instant, SystemClock};
///
/// const HOUR: Duration = Duration::from_secs(3600);
///
/// struct StartedAnHourAgo(SystemClock);
///
/// impl Clock for StartedAnHourAgo {
///     fn now(&self) -> Instant {
///         self.0.now() + HOUR
///     }
/// }
///
/// let rt = spooly::Runtime::builder()
///     .clock(StartedAnHourAgo(SystemClock::new()))
///     .build();
/// let start = rt.now();
/// assert!(start.epoch_offset() >= HOUR);
/// rt.block_on(async { spooly::sleep(Duration::from_millis(10)).await });
/// assert!(rt.now() - start >= Duration::from_millis(10));
/// ```
pub trait Clock {
    /// The current time.
    fn now(&self) -> Instant;

    /// Called when the runtime has nothing to do until `deadline` (the next
    /// timer). Return `true` if the clock jumped forward to `deadline`
    /// itself, so the runtime should *not* sleep; return `false` (the
    /// default) to have the runtime sleep until then in real time.
    fn advance_when_idle(&self, deadline: Instant) -> bool {
        let _ = deadline;
        false
    }
}

/// The real-time clock, backed by `std::time::Instant`. The default natively
/// and on WASI ([`Runtime::new`](crate::Runtime::new)).
///
/// ```
/// use std::time::Duration;
///
/// let rt = spooly::Runtime::builder().clock(spooly::SystemClock::new()).build();
/// let start = rt.now();
/// rt.block_on(async { spooly::sleep(Duration::from_millis(10)).await });
/// assert!(rt.now() - start >= Duration::from_millis(10));
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SystemClock {
    epoch: std::time::Instant,
}

impl SystemClock {
    /// Creates a clock whose epoch is now.
    pub fn new() -> Self {
        SystemClock {
            epoch: std::time::Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant(self.epoch.elapsed())
    }
}

/// A virtual clock for deterministic tests.
///
/// Time stands still until something advances it:
/// - **Automatically**: whenever the runtime would otherwise sleep until the
///   next timer, the clock jumps straight to that timer's deadline. Timer
///   tests run instantly, and always in the same order.
/// - **Manually**: [`MockClock::advance`] moves time forward. Timers that
///   become due fire in the next timers phase.
///
/// `MockClock` is cheap to clone; clones share the same time, so a test can
/// keep a clone to read or advance the runtime's clock.
///
/// ```
/// use std::time::Duration;
///
/// let clock = spooly::MockClock::new();
/// let rt = spooly::Runtime::builder().clock(clock.clone()).build();
/// rt.run(async {
///     spooly::sleep(Duration::from_secs(3600)).await; // Returns instantly.
/// });
/// assert_eq!(clock.elapsed(), Duration::from_secs(3600));
/// ```
#[derive(Clone, Default)]
pub struct MockClock {
    now: Rc<Cell<Duration>>,
}

impl MockClock {
    /// Creates a clock at its epoch ([`Instant::ZERO`]).
    pub fn new() -> Self {
        Self::default()
    }

    /// The current time.
    pub fn now(&self) -> Instant {
        Instant(self.now.get())
    }

    /// The time since the clock's epoch.
    pub fn elapsed(&self) -> Duration {
        self.now.get()
    }

    /// Moves time forward by `d`. Timers that become due fire in the next
    /// timers phase.
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let clock = spooly::MockClock::new();
    /// let rt = spooly::Runtime::builder().clock(clock.clone()).build();
    /// let fired = rt.block_on(async {
    ///     let sleep = spooly::sleep(Duration::from_secs(5));
    ///     clock.advance(Duration::from_secs(5));
    ///     // Due now: completes in the next timers phase, with no jump.
    ///     sleep.await;
    ///     clock.elapsed()
    /// });
    /// assert_eq!(fired, Duration::from_secs(5));
    /// ```
    ///
    /// # Panics
    ///
    /// If the clock would overflow.
    pub fn advance(&self, d: Duration) {
        self.now.set(
            self.now
                .get()
                .checked_add(d)
                .expect("spooly: MockClock overflow"),
        );
    }
}

impl fmt::Debug for MockClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockClock")
            .field("elapsed", &self.now.get())
            .finish()
    }
}

impl Clock for MockClock {
    fn now(&self) -> Instant {
        MockClock::now(self)
    }

    fn advance_when_idle(&self, deadline: Instant) -> bool {
        if deadline.0 > self.now.get() {
            self.now.set(deadline.0);
        }
        true
    }
}

/// The current time on the current runtime's clock.
///
/// ```
/// use std::time::Duration;
///
/// let rt = spooly::Runtime::builder().clock(spooly::MockClock::new()).build();
/// rt.block_on(async {
///     let start = spooly::now();
///     spooly::sleep(Duration::from_millis(30)).await;
///     assert_eq!(spooly::now() - start, Duration::from_millis(30));
/// });
/// ```
///
/// # Panics
///
/// If called outside a Spooly runtime.
pub fn now() -> Instant {
    context::current("now").now()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn instant_arithmetic() {
        let a = Instant::from_epoch_offset(10 * MS);
        let b = a + 5 * MS;
        assert_eq!(b.epoch_offset(), 15 * MS);
        assert_eq!(b - a, 5 * MS);
        assert_eq!(b - 5 * MS, a);
        assert_eq!(a.checked_duration_since(b), None);
        assert_eq!(a.saturating_duration_since(b), Duration::ZERO);
        assert_eq!(Instant::MAX.checked_add(MS), None);
        assert_eq!(Instant::MAX.saturating_add(MS), Instant::MAX);
        assert_eq!(Instant::ZERO.checked_sub(MS), None);
        let mut c = a;
        c += MS;
        c -= 2 * MS;
        assert_eq!(c.epoch_offset(), 9 * MS);
        assert!(a < b);
        assert_eq!(format!("{a:?}"), "Instant(epoch + 10ms)");
    }

    #[test]
    #[should_panic(expected = "overflow")]
    fn instant_add_overflow_panics() {
        let _ = Instant::MAX + MS;
    }

    #[test]
    #[should_panic(expected = "later than")]
    fn duration_since_later_panics() {
        let _ = Instant::ZERO.duration_since(Instant::ZERO + MS);
    }

    #[test]
    fn mock_clock_is_shared_between_clones() {
        let a = MockClock::new();
        let b = a.clone();
        a.advance(7 * MS);
        assert_eq!(b.elapsed(), 7 * MS);
        assert_eq!(Clock::now(&b), Instant::ZERO + 7 * MS);
    }

    #[test]
    fn mock_clock_advances_when_idle_but_never_backwards() {
        let clock = MockClock::new();
        assert!(clock.advance_when_idle(Instant::ZERO + 5 * MS));
        assert_eq!(clock.elapsed(), 5 * MS);
        assert!(clock.advance_when_idle(Instant::ZERO + 2 * MS));
        assert_eq!(clock.elapsed(), 5 * MS);
    }

    #[test]
    fn system_clock_moves_forward() {
        let clock = SystemClock::new();
        let a = clock.now();
        std::thread::sleep(MS);
        assert!(clock.now() > a);
        assert!(!clock.advance_when_idle(Instant::MAX));
    }
}
