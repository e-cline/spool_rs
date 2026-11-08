//! The runtime and its event loop.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use crate::clock::{self, Clock, Instant};
use crate::context;
use crate::driver::native::{self, Mode};
use crate::keep_alive::KeepAlive;
use crate::queue::{Callback, Microtask, Queue};
use crate::slab::Slab;
use crate::task::{self, BoxFuture, JoinDyn, JoinHandle, MAIN_TASK, TaskId, TaskSlot};
use crate::timer::{FireAction, TimerHandle, TimerQueue};
use crate::waker::{Shared, TaskHeader};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

/// What a driver should do after a [`step`](Inner::step) (D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// More work may be ready (or main asked the step to stop early): step
    /// again.
    Busy,
    /// Nothing to do until this deadline (the next timer), or a remote wake.
    WaitUntil(Instant),
    /// Nothing to do until a remote wake: a `KeepAlive` exists, or a waker
    /// outside the runtime might still wake a pending main.
    WaitForWake,
    /// Nothing can ever happen again. If main is still pending, it stalled.
    Idle,
}

/// The main future of a driver, as the step sees it. It isn't in the task
/// slab (it needn't be `'static`) and uses the reserved id `MAIN_TASK`.
pub(crate) trait Main {
    /// Polls main, unless it already completed (a stale wake). Returns
    /// `true` if the step should stop now: main completed, and its driver
    /// returns as soon as it does.
    fn poll(&mut self) -> bool;

    /// Main's header while it is pending, for the stall check; `None` once
    /// it completed (or if there is no main).
    fn pending_header(&self) -> Option<&Arc<TaskHeader>>;
}

/// No main future: a `Poll(MAIN_TASK)` entry is a stale wake.
impl Main for () {
    fn poll(&mut self) -> bool {
        false
    }

    fn pending_header(&self) -> Option<&Arc<TaskHeader>> {
        None
    }
}

/// The runtime's thread-local state. Never shared across threads.
pub(crate) struct Inner {
    /// A weak reference to this `Inner`, for handing out `TimerHandle`s.
    this: Weak<Inner>,
    microtasks: Queue<Microtask>,
    tasks: RefCell<Slab<TaskSlot>>,
    timers: RefCell<TimerQueue>,
    clock: Rc<dyn Clock>,
    shared: Arc<Shared>,
}

impl Inner {
    fn new(clock: Rc<dyn Clock>) -> Rc<Self> {
        let id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
        Rc::new_cyclic(|this| Inner {
            this: this.clone(),
            microtasks: Queue::default(),
            tasks: RefCell::new(Slab::new()),
            timers: RefCell::new(TimerQueue::default()),
            clock,
            shared: Arc::new(Shared::new(id)),
        })
    }

    pub(crate) fn id(&self) -> u64 {
        self.shared.runtime_id
    }

    pub(crate) fn now(&self) -> Instant {
        self.clock.now()
    }

    pub(crate) fn clock(&self) -> &dyn Clock {
        &*self.clock
    }

    /// A header for a new main future, already scheduled (its first poll
    /// is due).
    pub(crate) fn main_header(&self) -> Arc<TaskHeader> {
        Arc::new(TaskHeader::new_scheduled(
            MAIN_TASK,
            Arc::clone(&self.shared),
        ))
    }

    pub(crate) fn timers(&self) -> &RefCell<TimerQueue> {
        &self.timers
    }

    /// Queues a poll of `id`. Only called on the runtime's own thread.
    pub(crate) fn schedule_local(&self, id: TaskId) {
        self.microtasks.push(Microtask::Poll(id));
    }

    pub(crate) fn queue_microtask(&self, callback: Callback) {
        self.microtasks.push(Microtask::Callback(callback));
    }

    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        task::spawn_on(self, self.this.clone(), future)
    }

    /// Inserts a task and queues its first poll.
    pub(crate) fn insert_task(&self, future: BoxFuture, join: Rc<dyn JoinDyn>) -> TaskId {
        let id = {
            let mut tasks = self.tasks.borrow_mut();
            let id = tasks.next_key();
            let header = Arc::new(TaskHeader::new_scheduled(id, Arc::clone(&self.shared)));
            let inserted = tasks.insert(TaskSlot {
                future: Some(future),
                header,
                join,
                aborted: false,
            });
            debug_assert_eq!(id, inserted);
            id
        };
        self.schedule_local(id);
        id
    }

    /// Returns `true` if task `id` hasn't finished (it may be running).
    pub(crate) fn has_task(&self, id: TaskId) -> bool {
        self.tasks.borrow().contains(id)
    }

    /// Aborts task `id` (§5). An idle task's future is dropped now, outside
    /// any borrow; a running task's future is dropped when its poll returns.
    pub(crate) fn abort_task(&self, id: TaskId) {
        let removed = {
            let mut tasks = self.tasks.borrow_mut();
            let Some(slot) = tasks.get_mut(id) else {
                return; // Already finished.
            };
            if slot.future.is_none() {
                slot.aborted = true; // Running: see `poll_task`.
                return;
            }
            tasks.remove(id)
        };
        // Dropping the future runs its abort guard (resolving the
        // `JoinHandle`) and any user `Drop` impls.
        drop(removed);
    }

    /// Polls one spawned task. Holds no `RefCell` borrow while user code runs.
    ///
    /// A panic in the poll isn't caught (§6): it unwinds out through here,
    /// dropping the future (whose abort guard resolves the `JoinHandle`).
    fn poll_task(&self, id: TaskId) {
        let taken = {
            let mut tasks = self.tasks.borrow_mut();
            tasks.get_mut(id).and_then(|slot| {
                slot.future
                    .take()
                    .map(|future| (future, Arc::clone(&slot.header)))
            })
        };
        // A stale id (the task finished or was aborted) is a no-op.
        let Some((mut future, header)) = taken else {
            return;
        };

        // Clear before polling, so that a wake during the poll re-queues it.
        header.clear_scheduled();
        let waker = Waker::from(header);
        let poll = future.as_mut().poll(&mut Context::from_waker(&waker));
        drop(waker);

        match poll {
            Poll::Ready(()) => {
                drop(future);
                let slot = self.tasks.borrow_mut().remove(id);
                drop(slot);
            }
            Poll::Pending => {
                let leftover = {
                    let mut tasks = self.tasks.borrow_mut();
                    match tasks.get_mut(id) {
                        Some(slot) if !slot.aborted => {
                            slot.future = Some(future);
                            None
                        }
                        // Aborted during its own poll.
                        Some(_) => Some((future, tasks.remove(id))),
                        None => unreachable!("spool: a running task left the slab"),
                    }
                };
                drop(leftover);
            }
        }
    }

    /// Drains the microtask queue, including microtasks queued meanwhile
    /// (§3.2 of the plan).
    ///
    /// `main` is polled for each `Poll(MAIN_TASK)` entry. If it asks to stop,
    /// draining stops immediately and this returns `true`.
    pub(crate) fn drain(&self, main: &mut dyn Main) -> bool {
        while let Some(microtask) = self.microtasks.pop() {
            match microtask {
                Microtask::Poll(id) if id == MAIN_TASK => {
                    if main.poll() {
                        return true;
                    }
                }
                Microtask::Poll(id) => self.poll_task(id),
                Microtask::Callback(callback) => callback(),
            }
        }
        false
    }

    /// Moves remotely woken tasks onto the microtask queue. Returns `true` if
    /// anything was moved.
    fn absorb_remote(&self) -> bool {
        let woken = self.shared.take_remote();
        let any = !woken.is_empty();
        for id in woken {
            self.schedule_local(id);
        }
        any
    }

    /// The timers phase (§3.3). Fires every timer that is due at the phase's
    /// start time and was armed before the phase started, draining the
    /// microtask queue after each one.
    ///
    /// Returns `(fired_any, stop)`, where `stop` means `main` asked to stop.
    fn run_timers(&self, main: &mut dyn Main) -> (bool, bool) {
        let now = self.clock.now();
        let cutoff = self.timers.borrow().next_seq();
        let mut fired_any = false;
        let stop = loop {
            let next = self.timers.borrow_mut().pop_expired(now, cutoff);
            let Some(key) = next else {
                break false;
            };
            fired_any = true;
            let (action, removed) = self.timers.borrow_mut().fire(key);
            drop(removed);
            match action {
                FireAction::Wake(waker) => waker.wake(),
                FireAction::Once(callback) => callback(),
                FireAction::Interval(callback, period) => {
                    let handle = TimerHandle::new(self.this.clone(), key);
                    (callback.borrow_mut())(&handle);
                    drop(callback);
                    // Re-arm from the previous deadline, skipping any ticks
                    // already missed (D7), unless the callback cancelled it.
                    let after = self.clock.now();
                    self.timers.borrow_mut().rearm_interval(key, period, after);
                }
                FireAction::Nothing => {}
            }
            if self.drain(main) {
                break true;
            }
        };
        // Timers armed past their deadline during the phase were set aside;
        // they become due again.
        self.timers.borrow_mut().end_phase();
        (fired_any, stop)
    }

    /// Drops every task, microtask, and timer, repeating until nothing
    /// is left (a `Drop` impl may queue more work). Everything is dropped
    /// outside of any `RefCell` borrow.
    fn tear_down(&self) {
        loop {
            let microtasks = self.microtasks.take_all();
            let slots = self.tasks.borrow_mut().drain();
            let timers = self.timers.borrow_mut().take_all();
            if microtasks.is_empty() && slots.is_empty() && timers.is_empty() {
                break;
            }
            drop(microtasks);
            drop(slots);
            drop(timers);
        }
    }

    /// One step of the event loop (D5): drains the microtask queue, absorbs
    /// remote wakes, and runs the timers phase. Does a bounded amount of
    /// work, never blocks, and tells the driver what to wait for.
    ///
    /// The caller has entered the runtime (`context::enter`).
    pub(crate) fn step(&self, main: &mut dyn Main) -> Step {
        if self.drain(main) {
            return Step::Busy;
        }
        if self.absorb_remote() {
            return Step::Busy;
        }
        let (fired, stop) = self.run_timers(main);
        if fired || stop {
            return Step::Busy;
        }

        // Idle: the queues are empty and no timer is due.
        if let Some(deadline) = self.timers.borrow_mut().next_deadline() {
            // Armed timers keep the loop alive.
            return Step::WaitUntil(deadline);
        }
        if self.shared.is_kept_alive() {
            // Wait for a remote wake, or for the last guard to be dropped
            // (which unparks the native driver).
            return Step::WaitForWake;
        }
        // Only a pending main can keep the loop alive now, and only if a
        // waker exists outside the runtime (it may be on another thread).
        let outside = main
            .pending_header()
            .is_some_and(|header| self.has_outside_wakers(header));
        // A thread wakes (queueing a remote wake) *before* it drops its
        // waker or its `KeepAlive`. If we just saw that drop, the wake must
        // not be missed. The fence pairs the relaxed reference-count reads
        // above with the release of those drops.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        if self.shared.has_pending_remote() {
            return Step::Busy;
        }
        if outside {
            Step::WaitForWake
        } else {
            Step::Idle
        }
    }

    pub(crate) fn keep_alive(&self) -> KeepAlive {
        KeepAlive::new(Arc::clone(&self.shared))
    }

    /// The stall check (§3.5). Called when the loop is idle: no queued work,
    /// no remote wakes, no timers, and no `KeepAlive`. Returns `true` if any
    /// task (or main) has a waker that the runtime can't account for, which
    /// means something might still wake it.
    ///
    /// Each header is referenced once by its owner (the slab, or main's
    /// frame); every other reference is a `Waker`. A waker stored in a
    /// pending task's `JoinHandle` state (by whoever awaits that task) is
    /// accounted for: it only fires once that task finishes, which needs that
    /// task to be woken first. If no header has an unaccounted waker, nothing
    /// can ever run again: a provable stall. Any waker we can't identify
    /// counts as outside, so this never reports a stall that isn't one.
    fn has_outside_wakers(&self, main_header: &Arc<TaskHeader>) -> bool {
        use std::collections::HashMap;

        let tasks = self.tasks.borrow();
        // Joiner wakers held inside the runtime, by header address.
        let mut inside: HashMap<*const (), usize> = HashMap::new();
        for slot in tasks.values() {
            if let Some(data) = slot.join.joiner_waker_data() {
                *inside.entry(data).or_default() += 1;
            }
        }
        let outside = |header: &Arc<TaskHeader>| {
            let wakers = Arc::strong_count(header) - 1;
            let key = Arc::as_ptr(header).cast::<()>();
            wakers > inside.get(&key).copied().unwrap_or(0)
        };
        outside(main_header) || tasks.values().any(|slot| outside(&slot.header))
    }
}

/// A single-threaded Spool runtime.
///
/// Most programs can use the [`run`](crate::run) or
/// [`block_on`](crate::block_on) free functions instead. A `Runtime` is useful
/// when you want to spawn tasks before running, or to drive the same runtime
/// several times.
///
/// `Runtime` is `!Send` and `!Sync`. Dropping it drops every task,
/// microtask, and timer that is still pending.
///
/// ```
/// let rt = spool::Runtime::new();
/// let handle = rt.spawn(async { 1 + 1 });
/// assert_eq!(rt.block_on(handle), 2);
/// ```
pub struct Runtime {
    inner: Rc<Inner>,
}

impl Runtime {
    /// Creates a new runtime on the current thread, with the target's
    /// default clock: a [`SystemClock`](crate::SystemClock) natively and on
    /// WASI.
    ///
    /// # Panics
    ///
    /// On `wasm32-unknown-unknown`, which has no default clock yet: use
    /// [`Runtime::builder`] and pass a [`Clock`].
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Returns a [`Builder`] for configuring a runtime.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// The current time on this runtime's clock.
    pub fn now(&self) -> Instant {
        self.inner.now()
    }

    /// Returns a [`KeepAlive`] guard for this runtime, which keeps
    /// [`run`](Self::run) from exiting while it exists. Unlike
    /// [`keep_alive`](crate::keep_alive), this works before the runtime is
    /// running.
    ///
    /// ```
    /// let rt = spool::Runtime::new();
    /// let keep = rt.keep_alive();
    /// std::thread::spawn(move || {
    ///     // ... work that will wake a task on `rt` ...
    ///     drop(keep);
    /// });
    /// rt.run(async {}); // Returns once `keep` is dropped.
    /// ```
    pub fn keep_alive(&self) -> KeepAlive {
        self.inner.keep_alive()
    }

    /// Spawns a task onto this runtime. The task runs once the runtime is
    /// driven by [`run`](Self::run) or [`block_on`](Self::block_on). This
    /// works without the runtime being entered, unlike [`spawn`](crate::spawn).
    ///
    /// ```
    /// use std::{cell::RefCell, rc::Rc};
    ///
    /// let rt = spool::Runtime::new();
    /// let log = Rc::new(RefCell::new(Vec::new()));
    /// let l = log.clone();
    /// let task = rt.spawn(async move { l.borrow_mut().push("pre-spawned") });
    /// assert!(log.borrow().is_empty(), "nothing runs until the runtime is driven");
    /// rt.block_on(task);
    /// assert_eq!(*log.borrow(), ["pre-spawned"]);
    /// ```
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.inner.spawn(future)
    }

    /// Runs `future` to completion, then keeps running until the event loop
    /// has nothing left to do. Returns `future`'s output.
    ///
    /// The loop has "nothing left to do" when the microtask queue is empty,
    /// no timer is armed, and no [`KeepAlive`] guard exists. A detached task
    /// that is pending on something else (not a timer) doesn't keep the loop
    /// alive: it stays on the runtime, and is dropped
    /// with it. Hold a `KeepAlive` while such a task waits for another
    /// thread.
    ///
    /// Before `future` is first polled, any work already queued on the runtime
    /// (for example, tasks spawned with [`Runtime::spawn`]) runs first. The
    /// first poll of `future` then runs like a program's top-level code:
    /// microtasks it queues run after it. Later polls (after an
    /// `.await`) are microtasks.
    ///
    /// # Panics
    ///
    /// - If called from inside a running Spool runtime (nested `run` /
    ///   `block_on`).
    /// - If `future` stalls: it is pending, no work is queued, no timer or
    ///   `KeepAlive` exists, and nothing can ever wake it or any task. A
    ///   waker that a pending task holds for whoever awaits it (as when
    ///   `future` awaits a stuck task) doesn't count; any other waker might
    ///   be woken (from another thread, say), so the loop parks instead.
    /// - If `future`, any task, or any microtask or timer callback panics.
    ///   Spool doesn't catch panics: the panic unwinds out of `run` at once
    ///   (or, with `panic = "abort"`, ends the program). Don't drive the
    ///   runtime again after that; drop it. See the
    ///   [crate docs](crate#panics).
    ///
    /// ```
    /// use std::{cell::Cell, rc::Rc};
    ///
    /// let rt = spool::Runtime::new();
    /// let ran = Rc::new(Cell::new(false));
    /// let r = ran.clone();
    /// rt.run(async move {
    ///     // A detached task: `run` still waits for it, since it is queued.
    ///     drop(spool::spawn(async move { r.set(true) }));
    /// });
    /// assert!(ran.get());
    /// ```
    pub fn run<F: Future>(&self, future: F) -> F::Output {
        let _enter = context::enter(&self.inner);
        native::drive(&self.inner, future, Mode::Run)
    }

    /// Runs `future` to completion on this runtime, driving spawned tasks
    /// along the way, and returns its output.
    ///
    /// Unlike [`run`](Self::run), this returns **as soon as** `future`
    /// completes. Work that is still queued, and tasks that are still pending,
    /// stay on the runtime: a later `run` / `block_on` keeps driving them, and
    /// dropping the runtime drops them.
    ///
    /// # Panics
    ///
    /// The same as [`run`](Self::run).
    ///
    /// ```
    /// use std::{cell::Cell, rc::Rc};
    ///
    /// let rt = spool::Runtime::new();
    /// let count = Rc::new(Cell::new(0));
    /// let c = count.clone();
    /// let n = rt.block_on(async move {
    ///     let c2 = c.clone();
    ///     // Queued, but not awaited: `block_on` doesn't wait for it.
    ///     spool::queue_microtask(move || c2.set(c2.get() + 1));
    ///     c.get()
    /// });
    /// assert_eq!((n, count.get()), (0, 0));
    /// // The leftover microtask runs at the start of the next call.
    /// rt.block_on(async {});
    /// assert_eq!(count.get(), 1);
    /// ```
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _enter = context::enter(&self.inner);
        native::drive(&self.inner, future, Mode::BlockOn)
    }

    #[cfg(test)]
    pub(crate) fn task_count(&self) -> usize {
        self.inner.tasks.borrow().len()
    }
}

/// Configures and builds a [`Runtime`].
///
/// ```
/// let clock = spool::MockClock::new();
/// let rt = spool::Runtime::builder().clock(clock.clone()).build();
/// assert_eq!(rt.now(), clock.now());
/// ```
#[derive(Default)]
pub struct Builder {
    clock: Option<Rc<dyn Clock>>,
}

impl Builder {
    /// Uses `clock` as the runtime's time source. The default is the
    /// target's (see [`Runtime::new`]); tests usually want a
    /// [`MockClock`](crate::MockClock).
    pub fn clock(mut self, clock: impl Clock + 'static) -> Self {
        self.clock = Some(Rc::new(clock));
        self
    }

    /// Builds the runtime.
    ///
    /// # Panics
    ///
    /// Without a [`clock`](Self::clock), on a target with no default clock
    /// (see [`Runtime::new`]).
    pub fn build(self) -> Runtime {
        let clock = self.clock.unwrap_or_else(clock::default_clock);
        Runtime {
            inner: Inner::new(clock),
        }
    }
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("custom_clock", &self.clock.is_some())
            .finish()
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Enter the runtime so that `Drop` impls of pending futures and
        // callbacks can still call `spawn` and friends.
        let _enter = context::enter_for_teardown(&self.inner);
        self.inner.tear_down();
        self.inner.shared.shut_down();
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("id", &self.inner.id())
            .field("tasks", &self.inner.tasks.borrow().len())
            .field("now", &self.inner.now())
            .field("microtasks_empty", &self.inner.microtasks.is_empty())
            .finish()
    }
}

/// Runs `future` on a new runtime until it completes **and** the event loop
/// has nothing left to do. Returns `future`'s output.
///
/// This is shorthand for `Runtime::new().run(future)`. See [`Runtime::run`].
///
/// ```
/// use std::{cell::RefCell, rc::Rc, time::Duration};
///
/// let log = Rc::new(RefCell::new(Vec::new()));
/// let l = log.clone();
/// spool::run(async move {
///     let (a, b) = (l.clone(), l.clone());
///     spool::set_timeout(Duration::ZERO, move || a.borrow_mut().push("timeout"));
///     spool::queue_microtask(move || b.borrow_mut().push("microtask"));
///     l.borrow_mut().push("sync");
/// });
/// // `run` also waits for the timer, which `main` didn't await.
/// assert_eq!(*log.borrow(), ["sync", "microtask", "timeout"]);
/// ```
pub fn run<F: Future>(future: F) -> F::Output {
    Runtime::new().run(future)
}

/// Runs `future` to completion on a new runtime and returns its output.
///
/// This is shorthand for `Runtime::new().block_on(future)`. It returns as soon
/// as `future` completes; any work still queued or pending is dropped with the
/// runtime. Use [`run`] to wait for that work too.
///
/// # Panics
///
/// See [`Runtime::block_on`].
///
/// ```
/// use std::rc::Rc;
///
/// // Futures don't need to be `Send`.
/// let shared = Rc::new(5);
/// let doubled = spool::block_on(async move { *shared * 2 });
/// assert_eq!(doubled, 10);
/// ```
pub fn block_on<F: Future>(future: F) -> F::Output {
    Runtime::new().block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finished_tasks_are_removed_from_the_slab() {
        let rt = Runtime::new();
        let handles: Vec<_> = (0..10).map(|i| rt.spawn(async move { i })).collect();
        assert_eq!(rt.task_count(), 10);
        let sum = rt.block_on(async {
            let mut sum = 0;
            for h in handles {
                sum += h.await;
            }
            sum
        });
        assert_eq!(sum, 45);
        assert_eq!(rt.task_count(), 0);
    }

    #[test]
    fn duplicate_wakes_queue_one_poll() {
        let rt = Runtime::new();
        let polls = Rc::new(std::cell::Cell::new(0));
        let p = polls.clone();
        let handle = rt.spawn(std::future::poll_fn(move |cx| {
            p.set(p.get() + 1);
            if p.get() == 1 {
                cx.waker().wake_by_ref();
                cx.waker().wake_by_ref();
                #[allow(clippy::waker_clone_wake)] // Deliberately tests by-value `wake`.
                cx.waker().clone().wake();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }));
        rt.block_on(handle);
        assert_eq!(polls.get(), 2);
        assert!(rt.inner.microtasks.is_empty());
    }

    #[test]
    fn stale_wake_after_completion_is_ignored() {
        let rt = Runtime::new();
        let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
        let s = stash.clone();
        let first = rt.spawn(std::future::poll_fn(move |cx| {
            *s.borrow_mut() = Some(cx.waker().clone());
            Poll::Ready(())
        }));
        rt.block_on(first);
        // The slot is reused by a new task with a new generation.
        let second = rt.spawn(async { 7 });
        stash.borrow_mut().take().unwrap().wake();
        assert_eq!(rt.block_on(second), 7);
        assert_eq!(rt.task_count(), 0);
    }

    #[test]
    fn run_drains_queues_after_main_completes() {
        let rt = Runtime::new();
        let hits = Rc::new(std::cell::Cell::new(0));
        let h = hits.clone();
        rt.run(async move {
            let (a, b) = (h.clone(), h.clone());
            rt_helpers::queue_both(a, b);
        });
        assert_eq!(hits.get(), 2);
        assert!(rt.inner.microtasks.is_empty());
    }

    #[test]
    fn block_on_leaves_queued_work_for_later() {
        let rt = Runtime::new();
        let hits = Rc::new(std::cell::Cell::new(0));
        let h = hits.clone();
        rt.block_on(async move {
            let (a, b) = (h.clone(), h.clone());
            rt_helpers::queue_both(a, b);
        });
        assert_eq!(hits.get(), 0);
        assert_eq!(rt.inner.microtasks.len(), 2);
        // The next drive flushes the leftovers before polling its main.
        let h = hits.clone();
        rt.block_on(async move { assert_eq!(h.get(), 2) });
    }

    #[test]
    fn stale_main_wake_does_not_repoll_a_completed_main() {
        let rt = Runtime::new();
        let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
        let s = stash.clone();
        let mut polls = 0;
        rt.run(std::future::poll_fn(|cx| {
            polls += 1;
            *s.borrow_mut() = Some(cx.waker().clone());
            // Queue our own wake, then complete: the queued Poll(MAIN) must
            // be skipped while `run` drains.
            cx.waker().wake_by_ref();
            Poll::Ready(())
        }));
        assert_eq!(polls, 1);
        drop(stash);
    }

    /// Steps `rt` once with no main future, inside the runtime.
    fn step(rt: &Runtime) -> Step {
        let _enter = context::enter(&rt.inner);
        rt.inner.step(&mut ())
    }

    #[test]
    fn step_on_an_empty_runtime_is_idle() {
        assert_eq!(step(&Runtime::new()), Step::Idle);
    }

    #[test]
    fn step_drains_microtasks_then_reports_idle() {
        let rt = Runtime::new();
        let hits = Rc::new(std::cell::Cell::new(0));
        let h = hits.clone();
        drop(rt.spawn(async move { h.set(h.get() + 1) }));
        assert_eq!(step(&rt), Step::Idle);
        assert_eq!(hits.get(), 1);
    }

    #[test]
    fn step_waits_for_the_next_timer_and_is_busy_once_it_fires() {
        let clock = crate::MockClock::new();
        let rt = Runtime::builder().clock(clock.clone()).build();
        let fired = Rc::new(std::cell::Cell::new(false));
        let f = fired.clone();
        let delay = std::time::Duration::from_millis(5);
        {
            let _enter = context::enter(&rt.inner);
            crate::set_timeout(delay, move || f.set(true));
        }
        // The step never advances the clock itself: that is the driver's job.
        assert_eq!(step(&rt), Step::WaitUntil(Instant::ZERO + delay));
        assert!(!fired.get());
        clock.advance(delay);
        assert_eq!(step(&rt), Step::Busy);
        assert!(fired.get());
        assert_eq!(step(&rt), Step::Idle);
    }

    #[test]
    fn step_waits_for_a_wake_while_kept_alive() {
        let rt = Runtime::new();
        let keep = rt.keep_alive();
        assert_eq!(step(&rt), Step::WaitForWake);
        drop(keep);
        assert_eq!(step(&rt), Step::Idle);
    }

    #[test]
    fn step_absorbs_a_remote_wake() {
        let rt = Runtime::new();
        let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
        let s = stash.clone();
        let polls = Rc::new(std::cell::Cell::new(0));
        let p = polls.clone();
        drop(rt.spawn(std::future::poll_fn(move |cx| {
            p.set(p.get() + 1);
            *s.borrow_mut() = Some(cx.waker().clone());
            Poll::<()>::Pending
        })));
        // No main, so the task's outside waker doesn't keep the loop alive.
        assert_eq!(step(&rt), Step::Idle);
        let waker = stash.borrow_mut().take().unwrap();
        std::thread::spawn(move || waker.wake()).join().unwrap();
        assert_eq!(step(&rt), Step::Busy);
        assert_eq!(polls.get(), 1, "absorbed, but not polled yet");
        assert_eq!(step(&rt), Step::Idle);
        assert_eq!(polls.get(), 2);
    }

    mod rt_helpers {
        use std::cell::Cell;
        use std::rc::Rc;

        pub(super) fn queue_both(a: Rc<Cell<i32>>, b: Rc<Cell<i32>>) {
            crate::queue_microtask(move || a.set(a.get() + 1));
            crate::queue_microtask(move || b.set(b.get() + 1));
        }
    }
}
