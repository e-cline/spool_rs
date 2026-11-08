//! Timers: the timer queue, `set_timeout` / `set_interval`, and
//! [`TimerHandle`]. The future-style timers live in the submodules.
//!
//! Every timer is an entry in a generational slab, plus an item in a binary
//! heap keyed by `(deadline, seq)`. `seq` increases with every (re-)arm, so
//! timers with equal deadlines fire in the order they were armed,
//! and the timers phase can skip timers armed *during* the phase (D6).
//! Cancelling removes the slab entry; its heap item goes stale and is
//! skipped when it reaches the top (lazy deletion).

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
    /// A `set_timeout` callback (taken when it fires).
    Once(Option<Callback>),
    /// A `set_interval` callback.
    Interval {
        period: Duration,
        callback: IntervalCallback,
    },
}

pub(crate) struct TimerEntry {
    deadline: Instant,
    seq: u64,
    /// `true` while the entry has a live item in the heap.
    armed: bool,
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

type HeapItem = (Instant, u64, Key);

#[derive(Default)]
pub(crate) struct TimerQueue {
    entries: Slab<TimerEntry>,
    heap: BinaryHeap<Reverse<HeapItem>>,
    /// Items set aside during a timers phase because they were armed after
    /// the phase started. Restored by [`TimerQueue::end_phase`].
    deferred: Vec<HeapItem>,
    next_seq: u64,
}

impl TimerQueue {
    pub(crate) fn insert(&mut self, deadline: Instant, state: TimerState) -> Key {
        let seq = self.bump_seq();
        let key = self.entries.insert(TimerEntry {
            deadline,
            seq,
            armed: true,
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

    /// The `seq` the next armed timer will get. Timers with a lower `seq`
    /// existed before this call.
    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq
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
        if let Some(entry) = self.entries.get(key) {
            let deadline = next_interval_deadline(entry.deadline, period, now);
            self.rearm(key, deadline);
        }
    }

    /// Re-arms an existing timer with a new deadline (and a new `seq`).
    pub(crate) fn rearm(&mut self, key: Key, deadline: Instant) {
        let seq = self.bump_seq();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.deadline = deadline;
            entry.seq = seq;
            entry.armed = true;
            self.heap.push(Reverse((deadline, seq, key)));
        }
    }

    fn is_live(&self, &(deadline, seq, key): &(Instant, u64, Key)) -> bool {
        matches!(
            self.entries.get(key),
            Some(e) if e.armed && e.seq == seq && e.deadline == deadline
        )
    }

    /// Pops stale heap items until the top is live, then returns it.
    fn peek_live(&mut self) -> Option<(Instant, u64, Key)> {
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
        self.end_phase();
        self.peek_live().map(|(deadline, _, _)| deadline)
    }

    /// Pops the next timer that is due at `now` and was armed before
    /// `before_seq`, disarming it.
    ///
    /// Timers armed at or after `before_seq` (during the current phase) are
    /// set aside rather than allowed to block older due timers behind them.
    /// Call [`end_phase`](Self::end_phase) when the phase is over.
    pub(crate) fn pop_expired(&mut self, now: Instant, before_seq: u64) -> Option<Key> {
        loop {
            let (deadline, seq, key) = self.peek_live()?;
            if deadline > now {
                return None;
            }
            self.heap.pop();
            if seq >= before_seq {
                self.deferred.push((deadline, seq, key));
                continue;
            }
            if let Some(entry) = self.entries.get_mut(key) {
                entry.armed = false;
            }
            return Some(key);
        }
    }

    /// Puts back the items [`pop_expired`](Self::pop_expired) set aside.
    pub(crate) fn end_phase(&mut self) {
        self.heap.extend(self.deferred.drain(..).map(Reverse));
    }

    /// Decides what firing `key` does. `Once` timers are removed here;
    /// `Sleep` and `Interval` entries stay.
    pub(crate) fn fire(&mut self, key: Key) -> (FireAction, Option<TimerEntry>) {
        let Some(entry) = self.entries.get_mut(key) else {
            return (FireAction::Nothing, None);
        };
        match &mut entry.state {
            TimerState::Sleep { waker, fired } => {
                *fired = true;
                let action = waker.take().map_or(FireAction::Nothing, FireAction::Wake);
                (action, None)
            }
            TimerState::Interval { period, callback } => {
                (FireAction::Interval(Rc::clone(callback), *period), None)
            }
            TimerState::Once(_) => {
                let mut entry = self.remove(key);
                let action = match entry.as_mut().map(|e| &mut e.state) {
                    Some(TimerState::Once(cb)) => {
                        cb.take().map_or(FireAction::Nothing, FireAction::Once)
                    }
                    _ => FireAction::Nothing,
                };
                (action, entry)
            }
        }
    }

    /// Removes every timer. The caller drops them outside the borrow.
    pub(crate) fn take_all(&mut self) -> Vec<TimerEntry> {
        self.heap.clear();
        self.deferred.clear();
        self.entries.drain()
    }

    /// Rebuilds the heap when stale items dominate it (for example, after
    /// many cancelled or reset timers).
    fn maybe_compact(&mut self) {
        // Never compact mid-phase: `deferred` items must keep their place.
        if self.deferred.is_empty()
            && self.heap.len() > 64
            && self.heap.len() > 4 * self.entries.len()
        {
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
        .insert(deadline, TimerState::Once(Some(Box::new(callback))));
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

    #[test]
    fn pops_by_deadline_then_seq() {
        let mut q = TimerQueue::default();
        let c = q.insert(at(30), sleep_state());
        let a = q.insert(at(10), sleep_state());
        let b = q.insert(at(10), sleep_state());
        let cutoff = q.next_seq();
        assert_eq!(q.next_deadline(), Some(at(10)));
        assert_eq!(q.pop_expired(at(100), cutoff), Some(a));
        assert_eq!(q.pop_expired(at(100), cutoff), Some(b));
        assert_eq!(q.pop_expired(at(100), cutoff), Some(c));
        assert_eq!(q.pop_expired(at(100), cutoff), None);
    }

    #[test]
    fn respects_now_and_seq_cutoff() {
        let mut q = TimerQueue::default();
        let a = q.insert(at(5), sleep_state());
        let cutoff = q.next_seq();
        // Armed after the cutoff, with an earlier deadline: it sorts first.
        let late = q.insert(at(0), sleep_state());
        assert_eq!(q.pop_expired(at(4), cutoff), None, "`a` isn't due yet");
        // `late` is set aside instead of blocking `a`.
        assert_eq!(q.pop_expired(at(5), cutoff), Some(a));
        assert_eq!(q.pop_expired(at(5), cutoff), None);
        q.end_phase();
        // Next phase: a new cutoff lets `late` through.
        let cutoff = q.next_seq();
        assert_eq!(q.pop_expired(at(5), cutoff), Some(late));
    }

    #[test]
    fn next_deadline_restores_deferred_items() {
        let mut q = TimerQueue::default();
        let cutoff = q.next_seq();
        q.insert(at(0), sleep_state());
        assert_eq!(q.pop_expired(at(0), cutoff), None);
        // Even without an explicit `end_phase`, the deferred item counts.
        assert_eq!(q.next_deadline(), Some(at(0)));
    }

    #[test]
    fn removed_and_rearmed_items_go_stale() {
        let mut q = TimerQueue::default();
        let a = q.insert(at(1), sleep_state());
        let b = q.insert(at(2), sleep_state());
        drop(q.remove(a));
        q.rearm(b, at(50));
        assert_eq!(q.next_deadline(), Some(at(50)));
        let cutoff = q.next_seq();
        assert_eq!(q.pop_expired(at(10), cutoff), None);
        assert_eq!(q.pop_expired(at(50), cutoff), Some(b));
        assert_eq!(q.next_deadline(), None, "fired entries are disarmed");
        assert!(q.contains(b), "but stay until removed");
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
    fn fire_once_removes_and_returns_the_callback() {
        let mut q = TimerQueue::default();
        let k = q.insert(at(0), TimerState::Once(Some(Box::new(|| {}))));
        let cutoff = q.next_seq();
        assert_eq!(q.pop_expired(at(0), cutoff), Some(k));
        let (action, entry) = q.fire(k);
        assert!(matches!(action, FireAction::Once(_)));
        assert!(entry.is_some());
        assert!(!q.contains(k));
    }

    #[test]
    fn fire_sleep_marks_fired() {
        let mut q = TimerQueue::default();
        let k = q.insert(at(0), sleep_state());
        let (action, entry) = q.fire(k);
        assert!(
            matches!(action, FireAction::Nothing),
            "no waker registered yet"
        );
        assert!(entry.is_none());
        assert!(matches!(
            q.get_mut(k).unwrap().state,
            TimerState::Sleep { fired: true, .. }
        ));
    }

    #[test]
    fn take_all_empties_everything() {
        let mut q = TimerQueue::default();
        q.insert(at(1), sleep_state());
        q.insert(at(2), TimerState::Once(None));
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
        assert_eq!(q.pop_expired(at(10), q.next_seq()), Some(key));
        q.rearm_interval(key, period, at(35));
        assert_eq!(q.next_deadline(), Some(at(40)));
        // A cancelled interval stays cancelled.
        drop(q.remove(key));
        q.rearm_interval(key, period, at(45));
        assert_eq!(q.next_deadline(), None);
    }
}
