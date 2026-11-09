//! Timers: the timer queue, `set_timeout` / `set_interval`, and
//! [`TimerHandle`]. The future-style timers live in the submodules.
//!
//! Every timer is an entry in a generational slab, plus an item in a binary
//! heap keyed by `(deadline, seq)`. `seq` is unique to each (re-)arm, so
//! timers with equal deadlines fire in the order they were armed, and an
//! item is *live* only while its `seq` matches its entry's. Cancelling
//! removes the entry, and re-arming gives it a new `seq`: either way the old
//! heap item goes stale, and is skipped when it reaches the top (lazy
//! deletion).

mod interval;
mod sleep;
mod timeout;

pub use interval::{Interval, interval};
pub use sleep::{Sleep, sleep, sleep_until};
pub use timeout::{Elapsed, Timeout, timeout, timeout_at};

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::rc::{Rc, Weak};
use std::task::Waker;
use std::time::Duration;

use crate::clock::Instant;
use crate::context;
use crate::queue::Callback;
use crate::runtime::Inner;
use crate::slab::{Key, Slab};

/// An interval callback. Shared so that it stays alive while it runs, even if
/// it cancels its own timer.
pub(crate) type IntervalCallback = Rc<RefCell<Box<dyn FnMut(&TimerHandle)>>>;

pub(crate) enum TimerState {
    /// Backs a [`Sleep`] future (also used by `Interval` and `Timeout`).
    Sleep { waker: Option<Waker>, fired: bool },
    /// A `set_timeout` callback.
    Once(Callback),
    /// A `set_interval` callback.
    Interval {
        period: Duration,
        callback: IntervalCallback,
    },
}

pub(crate) struct TimerEntry {
    deadline: Instant,
    /// The `seq` of the entry's live heap item (its latest arming).
    seq: u64,
    pub(crate) state: TimerState,
}

/// The deadline after `previous` on an interval's fixed schedule
/// (`previous + k·period`): the first one that isn't before `now`, so the
/// schedule never drifts, and ticks that were missed are skipped rather than
/// replayed in a burst (D7).
pub(crate) fn next_interval_deadline(previous: Instant, period: Duration, now: Instant) -> Instant {
    let next = previous.saturating_add(period);
    if next >= now {
        return next;
    }
    // Behind by at least one period: jump to the first deadline >= now.
    let behind = now.duration_since(previous).as_nanos();
    let period_ns = period.as_nanos();
    let periods = behind.div_ceil(period_ns);
    let skip = u32::try_from(periods).map_or(Duration::MAX, |k| period.saturating_mul(k));
    previous.saturating_add(skip)
}

/// What firing a timer should do, computed under the borrow and carried out
/// after it is released.
pub(crate) enum FireAction {
    Wake(Waker),
    Once(Callback),
    Interval(IntervalCallback, Duration),
    Nothing,
}

/// A heap item: `(deadline, seq, key)`. Also what
/// [`take_due`](TimerQueue::take_due) returns.
pub(crate) type Due = (Instant, u64, Key);

#[derive(Default)]
pub(crate) struct TimerQueue {
    entries: Slab<TimerEntry>,
    heap: BinaryHeap<Reverse<Due>>,
    next_seq: u64,
    /// The buffer [`take_due`](Self::take_due) fills, kept between phases
    /// so that a phase doesn't allocate.
    spare: Vec<Due>,
}

impl TimerQueue {
    pub(crate) fn insert(&mut self, deadline: Instant, state: TimerState) -> Key {
        let seq = self.bump_seq();
        let key = self.entries.insert(TimerEntry {
            deadline,
            seq,
            state,
        });
        self.heap.push(Reverse((deadline, seq, key)));
        key
    }

    fn bump_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    pub(crate) fn contains(&self, key: Key) -> bool {
        self.entries.contains(key)
    }

    pub(crate) fn get_mut(&mut self, key: Key) -> Option<&mut TimerEntry> {
        self.entries.get_mut(key)
    }

    /// Removes a timer. The caller drops the entry after releasing the
    /// borrow, since it may own a user callback.
    #[must_use = "drop the entry after releasing the borrow"]
    pub(crate) fn remove(&mut self, key: Key) -> Option<TimerEntry> {
        let entry = self.entries.remove(key);
        self.maybe_compact();
        entry
    }

    /// Re-arms an interval after it fired: at its previous deadline +
    /// `period`, skipping any deadlines already before `now` (D7). Does
    /// nothing if the timer was cancelled.
    pub(crate) fn rearm_interval(&mut self, key: Key, period: Duration, now: Instant) {
        let seq = self.bump_seq();
        if let Some(entry) = self.entries.get_mut(key) {
            let deadline = next_interval_deadline(entry.deadline, period, now);
            entry.deadline = deadline;
            entry.seq = seq;
            self.heap.push(Reverse((deadline, seq, key)));
        }
    }

    /// `true` if `item` is its timer's latest arming.
    fn is_live(&self, &(_, seq, key): &Due) -> bool {
        self.entries.get(key).is_some_and(|entry| entry.seq == seq)
    }

    /// Pops stale heap items until the top is live, then returns it.
    fn peek_live(&mut self) -> Option<Due> {
        while let Some(&Reverse(top)) = self.heap.peek() {
            if self.is_live(&top) {
                return Some(top);
            }
            self.heap.pop();
        }
        None
    }

    /// The earliest deadline among armed timers, if any. Armed timers are
    /// what keep the event loop alive.
    pub(crate) fn next_deadline(&mut self) -> Option<Instant> {
        self.peek_live().map(|(deadline, _, _)| deadline)
    }

    /// Takes every timer that is due at `now` out of the heap, in firing
    /// order: the snapshot a timers phase works through (D6). Timers armed
    /// later, during the phase, aren't in it, so they wait for the next
    /// phase whatever their deadline.
    ///
    /// Hand the list back with [`recycle`](Self::recycle) when done.
    pub(crate) fn take_due(&mut self, now: Instant) -> Vec<Due> {
        let mut due = std::mem::take(&mut self.spare);
        while let Some(item) = self.peek_live() {
            if item.0 > now {
                break;
            }
            self.heap.pop();
            due.push(item);
        }
        due
    }

    /// Puts back items from [`take_due`](Self::take_due) that a phase didn't
    /// reach (it stopped early).
    pub(crate) fn restore(&mut self, items: &[Due]) {
        self.heap.extend(items.iter().copied().map(Reverse));
    }

    /// Keeps a list from [`take_due`](Self::take_due) for the next phase.
    pub(crate) fn recycle(&mut self, mut due: Vec<Due>) {
        due.clear();
        self.spare = due;
    }

    /// Fires a due item, unless it went stale since it was taken (its timer
    /// was cancelled or re-armed). Decides what firing does; `Once` timers
    /// are removed here, `Sleep` and `Interval` entries stay.
    pub(crate) fn fire(&mut self, (_, seq, key): Due) -> FireAction {
        let entry = match self.entries.get_mut(key) {
            Some(entry) if entry.seq == seq => entry,
            _ => return FireAction::Nothing, // Stale.
        };
        match &mut entry.state {
            TimerState::Sleep { waker, fired } => {
                *fired = true;
                return waker.take().map_or(FireAction::Nothing, FireAction::Wake);
            }
            TimerState::Interval { period, callback } => {
                return FireAction::Interval(Rc::clone(callback), *period);
            }
            TimerState::Once(_) => {}
        }
        match self.remove(key).map(|entry| entry.state) {
            Some(TimerState::Once(callback)) => FireAction::Once(callback),
            _ => unreachable!(),
        }
    }

    /// Removes every timer. The caller drops them outside the borrow.
    pub(crate) fn take_all(&mut self) -> Vec<TimerEntry> {
        self.heap.clear();
        self.spare = Vec::new();
        self.entries.drain()
    }

    /// Rebuilds the heap when stale items dominate it (for example, after
    /// many cancelled or reset timers).
    fn maybe_compact(&mut self) {
        if self.heap.len() > 64 && self.heap.len() > 4 * self.entries.len() {
            let mut heap = std::mem::take(&mut self.heap).into_vec();
            heap.retain(|Reverse(item)| self.is_live(item));
            self.heap = BinaryHeap::from(heap);
        }
    }

    #[cfg(test)]
    pub(crate) fn heap_len(&self) -> usize {
        self.heap.len()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// A handle to a [`set_timeout`] or [`set_interval`] timer.
///
/// Dropping the handle does **not** cancel the timer; call
/// [`cancel`](Self::cancel). Handles are cheap to clone, and all clones refer
/// to the same timer.
///
/// `TimerHandle` is `!Send`: it belongs to the runtime's thread.
///
/// ```
/// use std::time::Duration;
/// use std::{cell::Cell, rc::Rc};
///
/// let fired = Rc::new(Cell::new(false));
/// let f = fired.clone();
/// let rt = spooly::Runtime::builder().clock(spooly::MockClock::new()).build();
/// rt.run(async move {
///     let handle = spooly::set_timeout(Duration::from_secs(1), move || f.set(true));
///     assert!(handle.is_active());
///     handle.cancel(); // Like `clearTimeout(handle)`.
///     assert!(!handle.is_active());
/// });
/// assert!(!fired.get());
/// ```
#[derive(Clone)]
pub struct TimerHandle {
    inner: Weak<Inner>,
    key: Key,
}

impl TimerHandle {
    pub(crate) fn new(inner: Weak<Inner>, key: Key) -> Self {
        TimerHandle { inner, key }
    }

    /// Cancels the timer: its callback never runs (again), and the callback
    /// is dropped right away.
    ///
    /// Cancelling is idempotent, and cancelling a timer that already fired
    /// (or whose runtime is gone) does nothing. An interval may cancel itself
    /// from inside its own callback.
    pub fn cancel(&self) {
        if let Some(inner) = self.inner.upgrade() {
            let removed = inner.timers().borrow_mut().remove(self.key);
            drop(removed); // Outside the borrow: may run the callback's `Drop`.
        }
    }

    /// Returns `true` until the timer is cancelled, or (for a
    /// [`set_timeout`] timer) until its callback starts running.
    pub fn is_active(&self) -> bool {
        self.inner
            .upgrade()
            .is_some_and(|inner| inner.timers().borrow().contains(self.key))
    }
}

impl fmt::Debug for TimerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerHandle")
            .field("active", &self.is_active())
            .finish()
    }
}

/// Runs `callback` once after `delay`, like JS's `setTimeout`.
///
/// The callback runs in the timers phase, as a macrotask: microtasks it
/// queues run right after it, before the next timer. A zero delay means "as
/// soon as possible", which is still after the current microtasks. Delays
/// aren't clamped to a minimum.
///
/// The returned handle can cancel the timer. Dropping it does not.
///
/// # Panics
///
/// If called outside a Spooly runtime. A panic in `callback` unwinds out of
/// `run` / `block_on`.
///
/// # Example
///
/// ```
/// use std::{cell::RefCell, rc::Rc, time::Duration};
///
/// // A MockClock keeps the example deterministic: on the real clock, time
/// // passes between the calls below, which matters on a very slow machine.
/// let rt = spooly::Runtime::builder().clock(spooly::MockClock::new()).build();
/// let log = Rc::new(RefCell::new(Vec::new()));
/// let l = log.clone();
/// rt.run(async move {
///     let (a, b) = (l.clone(), l.clone());
///     spooly::set_timeout(Duration::from_millis(20), move || a.borrow_mut().push("20ms"));
///     spooly::set_timeout(Duration::from_millis(10), move || b.borrow_mut().push("10ms"));
///     let cancelled = spooly::set_timeout(Duration::from_millis(5), || unreachable!());
///     cancelled.cancel();
/// });
/// assert_eq!(*log.borrow(), ["10ms", "20ms"]);
/// ```
pub fn set_timeout(delay: Duration, callback: impl FnOnce() + 'static) -> TimerHandle {
    let inner = context::current("set_timeout");
    let deadline = inner.now().saturating_add(delay);
    let key = inner
        .timers()
        .borrow_mut()
        .insert(deadline, TimerState::Once(Box::new(callback)));
    TimerHandle::new(Rc::downgrade(&inner), key)
}

/// Runs `callback` every `period`, like JS's `setInterval`, until the
/// returned handle is cancelled.
///
/// The callback receives the interval's own [`TimerHandle`], so it can
/// cancel itself. The interval keeps a fixed schedule: created at `t0`, it is
/// due at `t0 + period`, `t0 + 2·period`, and so on, however long each
/// callback takes. If the loop falls behind by a whole period or more, the
/// missed runs are skipped: the callback runs once, and the next run is on
/// the first scheduled time that hasn't passed yet.
///
/// # Panics
///
/// If `period` is zero, or if called outside a Spooly runtime. A panic in
/// `callback` unwinds out of `run` / `block_on`.
///
/// # Example
///
/// ```
/// use std::{cell::Cell, rc::Rc, time::Duration};
///
/// let ticks = Rc::new(Cell::new(0));
/// let t = ticks.clone();
/// spooly::run(async move {
///     spooly::set_interval(Duration::from_millis(10), move |handle| {
///         t.set(t.get() + 1);
///         if t.get() == 3 {
///             handle.cancel();
///         }
///     });
/// });
/// assert_eq!(ticks.get(), 3);
/// ```
pub fn set_interval(period: Duration, callback: impl FnMut(&TimerHandle) + 'static) -> TimerHandle {
    assert!(
        !period.is_zero(),
        "spooly: `set_interval` period must be non-zero"
    );
    let inner = context::current("set_interval");
    let deadline = inner.now().saturating_add(period);
    let callback: IntervalCallback = Rc::new(RefCell::new(Box::new(callback)));
    let key = inner
        .timers()
        .borrow_mut()
        .insert(deadline, TimerState::Interval { period, callback });
    TimerHandle::new(Rc::downgrade(&inner), key)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn at(ms: u32) -> Instant {
        Instant::ZERO + ms * MS
    }

    fn sleep_state() -> TimerState {
        TimerState::Sleep {
            waker: None,
            fired: false,
        }
    }

    fn keys(due: &[Due]) -> Vec<Key> {
        due.iter().map(|&(_, _, key)| key).collect()
    }

    #[test]
    fn take_due_returns_due_timers_by_deadline_then_seq() {
        let mut q = TimerQueue::default();
        let c = q.insert(at(30), sleep_state());
        let a = q.insert(at(10), sleep_state());
        let b = q.insert(at(10), sleep_state());
        assert_eq!(q.next_deadline(), Some(at(10)));
        assert_eq!(keys(&q.take_due(at(20))), [a, b]);
        assert_eq!(q.next_deadline(), Some(at(30)), "`c` isn't due yet");
        assert_eq!(keys(&q.take_due(at(100))), [c]);
        assert!(q.take_due(at(100)).is_empty());
    }

    #[test]
    fn timers_armed_after_the_snapshot_wait_for_the_next_one() {
        let mut q = TimerQueue::default();
        let a = q.insert(at(5), sleep_state());
        let due = q.take_due(at(5));
        // Armed mid-phase with a past deadline: not in this phase's snapshot.
        let late = q.insert(at(0), sleep_state());
        assert_eq!(keys(&due), [a]);
        assert_eq!(q.next_deadline(), Some(at(0)));
        assert_eq!(keys(&q.take_due(at(5))), [late]);
    }

    #[test]
    fn items_that_went_stale_after_the_snapshot_dont_fire() {
        let mut q = TimerQueue::default();
        let cancelled = q.insert(at(1), sleep_state());
        let period = Duration::from_millis(10);
        let callback: IntervalCallback = Rc::new(RefCell::new(Box::new(|_: &TimerHandle| {})));
        let rearmed = q.insert(at(2), TimerState::Interval { period, callback });
        let due = q.take_due(at(2));
        assert_eq!(keys(&due), [cancelled, rearmed]);
        drop(q.remove(cancelled));
        q.rearm_interval(rearmed, period, at(2));
        for item in due {
            assert!(matches!(q.fire(item), FireAction::Nothing));
        }
        assert_eq!(q.next_deadline(), Some(at(12)), "the re-armed one is live");
    }

    #[test]
    fn restore_puts_back_unreached_items() {
        let mut q = TimerQueue::default();
        let a = q.insert(at(1), sleep_state());
        let b = q.insert(at(2), sleep_state());
        let due = q.take_due(at(2));
        assert!(
            matches!(q.fire(due[0]), FireAction::Nothing),
            "no waker yet"
        );
        q.restore(&due[1..]);
        assert_eq!(keys(&q.take_due(at(2))), [b]);
        assert!(q.contains(a), "a fired sleep stays until it is polled");
    }

    #[test]
    fn removed_items_go_stale() {
        let mut q = TimerQueue::default();
        let a = q.insert(at(1), sleep_state());
        let b = q.insert(at(50), sleep_state());
        drop(q.remove(a));
        assert_eq!(q.next_deadline(), Some(at(50)));
        assert!(q.take_due(at(10)).is_empty());
        assert_eq!(keys(&q.take_due(at(50))), [b]);
        assert_eq!(q.next_deadline(), None, "taken items leave the heap");
        assert!(q.contains(b), "but the entry stays until removed");
    }

    #[test]
    fn heap_compacts_after_many_cancellations() {
        let mut q = TimerQueue::default();
        let keep = q.insert(at(1000), sleep_state());
        for i in 0..1000 {
            let k = q.insert(at(i), sleep_state());
            drop(q.remove(k));
        }
        assert!(q.heap_len() < 100, "heap was compacted: {}", q.heap_len());
        assert_eq!(q.len(), 1);
        assert_eq!(q.next_deadline(), Some(at(1000)));
        assert!(q.contains(keep));
    }

    #[test]
    fn fire_once_removes_the_timer_and_returns_its_callback() {
        let mut q = TimerQueue::default();
        let k = q.insert(at(0), TimerState::Once(Box::new(|| {})));
        let due = q.take_due(at(0));
        assert!(matches!(q.fire(due[0]), FireAction::Once(_)));
        assert!(!q.contains(k));
    }

    #[test]
    fn fire_sleep_marks_fired() {
        let mut q = TimerQueue::default();
        let k = q.insert(at(0), sleep_state());
        let due = q.take_due(at(0));
        assert!(
            matches!(q.fire(due[0]), FireAction::Nothing),
            "no waker registered yet"
        );
        assert!(matches!(
            q.get_mut(k).unwrap().state,
            TimerState::Sleep { fired: true, .. }
        ));
    }

    #[test]
    fn take_all_empties_everything() {
        let mut q = TimerQueue::default();
        q.insert(at(1), sleep_state());
        q.insert(at(2), TimerState::Once(Box::new(|| {})));
        assert_eq!(q.take_all().len(), 2);
        assert_eq!(q.next_deadline(), None);
        assert_eq!(q.heap_len(), 0);
    }

    #[test]
    fn interval_deadlines_keep_a_fixed_schedule() {
        let p = Duration::from_millis(10);
        // On time, or late by less than a period: no drift.
        assert_eq!(next_interval_deadline(at(10), p, at(10)), at(20));
        assert_eq!(next_interval_deadline(at(10), p, at(19)), at(20));
        // Exactly one period late: the next deadline is due now, not missed.
        assert_eq!(next_interval_deadline(at(10), p, at(20)), at(20));
        // Late by more than a period: skip to the first deadline not passed.
        assert_eq!(next_interval_deadline(at(10), p, at(21)), at(30));
        assert_eq!(next_interval_deadline(at(10), p, at(55)), at(60));
        assert_eq!(next_interval_deadline(at(10), p, at(60)), at(60));
        // A clock that hasn't reached the previous deadline (it can't go
        // backwards, but the math shouldn't care).
        assert_eq!(next_interval_deadline(at(10), p, at(5)), at(20));
    }

    #[test]
    fn rearm_interval_skips_missed_deadlines() {
        let mut q = TimerQueue::default();
        let callback: IntervalCallback = Rc::new(RefCell::new(Box::new(|_: &TimerHandle| {})));
        let period = Duration::from_millis(10);
        let key = q.insert(at(10), TimerState::Interval { period, callback });
        assert_eq!(keys(&q.take_due(at(10))), [key]);
        q.rearm_interval(key, period, at(35));
        assert_eq!(q.next_deadline(), Some(at(40)));
        // A cancelled interval stays cancelled.
        drop(q.remove(key));
        q.rearm_interval(key, period, at(45));
        assert_eq!(q.next_deadline(), None);
    }
}
