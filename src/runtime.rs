//! The runtime and its event loop.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::{Pin, pin};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use crate::clock::{Clock, Instant, SystemClock};
use crate::context;
use crate::park;
use crate::queue::{Callback, Microtask, Queue};
use crate::slab::Slab;
use crate::task::{self, BoxFuture, JoinDyn, JoinHandle, MAIN_TASK, Payload, TaskId, TaskSlot};
use crate::timer::{FireAction, TimerHandle, TimerQueue};
use crate::waker::{Shared, TaskHeader};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

const STALL_MESSAGE: &str = "spool: main future stalled: it is pending but nothing can wake it \
     (no queued work, no timers, and no outstanding wakers)";

const POISONED_MESSAGE: &str = "spool: this runtime is poisoned: a panic unwound out of an \
     earlier `run` / `block_on` call on it (create a new `Runtime` instead)";

/// The runtime's thread-local state. Never shared across threads.
pub(crate) struct Inner {
    /// A weak reference to this `Inner`, for handing out `TimerHandle`s.
    this: Weak<Inner>,
    ticks: Queue<Callback>,
    microtasks: Queue<Microtask>,
    tasks: RefCell<Slab<TaskSlot>>,
    timers: RefCell<TimerQueue>,
    clock: Rc<dyn Clock>,
    shared: Arc<Shared>,
    /// A panic that nobody can observe (§6): re-raised out of `run` /
    /// `block_on` at the next safe point. Only the first one is kept.
    unobserved: Cell<Option<Payload>>,
    /// A panic unwound out of `run` / `block_on`; the runtime can't be
    /// driven again.
    poisoned: Cell<bool>,
}

impl Inner {
    fn new(clock: Rc<dyn Clock>) -> Rc<Self> {
        let id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
        Rc::new_cyclic(|this| Inner {
            this: this.clone(),
            ticks: Queue::default(),
            microtasks: Queue::default(),
            tasks: RefCell::new(Slab::new()),
            timers: RefCell::new(TimerQueue::default()),
            clock,
            shared: Arc::new(Shared::new(id)),
            unobserved: Cell::new(None),
            poisoned: Cell::new(false),
        })
    }

    pub(crate) fn id(&self) -> u64 {
        self.shared.runtime_id
    }

    pub(crate) fn now(&self) -> Instant {
        self.clock.now()
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

    pub(crate) fn next_tick(&self, callback: Callback) {
        self.ticks.push(callback);
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

    /// Records a panic that nobody can observe. It is re-raised by
    /// [`raise_unobserved`](Self::raise_unobserved).
    pub(crate) fn report_unobserved(&self, payload: Payload) {
        match self.unobserved.take() {
            None => self.unobserved.set(Some(payload)),
            Some(first) => {
                // Keep the first; drop the later one outside the cell.
                self.unobserved.set(Some(first));
                drop(payload);
            }
        }
    }

    /// Re-raises an unobserved panic, if there is one. Only called at safe
    /// points, with no borrows held.
    fn raise_unobserved(&self) {
        if let Some(payload) = self.unobserved.take() {
            resume_unwind(payload);
        }
    }

    /// Polls one spawned task. Holds no `RefCell` borrow while user code runs.
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
        // A panic is caught, like an exception rejecting a promise (§6).
        let poll = catch_unwind(AssertUnwindSafe(|| {
            future.as_mut().poll(&mut Context::from_waker(&waker))
        }));
        drop(waker);

        match poll {
            Ok(Poll::Ready(())) => {
                drop(future);
                let slot = self.tasks.borrow_mut().remove(id);
                drop(slot);
            }
            Ok(Poll::Pending) => {
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
            Err(payload) => {
                let slot = self.tasks.borrow_mut().remove(id);
                let slot = slot.expect("spool: a running task left the slab");
                // Resolve the handle before dropping the future, so its abort
                // guard sees the task as finished.
                let unobserved = slot.join.reject(payload);
                drop(slot);
                drop(future);
                if let Some(payload) = unobserved {
                    self.report_unobserved(payload);
                }
            }
        }
    }

    /// Drains the tick and microtask queues, following Node's
    /// `processTicksAndRejections` (§3.2 of the plan):
    ///
    /// ```text
    /// loop:
    ///     run every tick (including ticks queued meanwhile)
    ///     run every microtask (including microtasks queued meanwhile)
    ///     if no ticks were queued by the microtasks: done
    /// ```
    ///
    /// `poll_main` is called for each `Poll(MAIN_TASK)` entry. If it returns
    /// `true`, draining stops immediately and this returns `true`.
    ///
    /// An unobserved panic is re-raised before the next item runs.
    fn drain(&self, poll_main: &mut dyn FnMut() -> bool) -> bool {
        self.raise_unobserved();
        loop {
            while let Some(tick) = self.ticks.pop() {
                tick();
                self.raise_unobserved();
            }
            while let Some(microtask) = self.microtasks.pop() {
                match microtask {
                    Microtask::Poll(id) if id == MAIN_TASK => {
                        let done = poll_main();
                        self.raise_unobserved();
                        if done {
                            return true;
                        }
                    }
                    Microtask::Poll(id) => self.poll_task(id),
                    Microtask::Callback(callback) => callback(),
                }
                self.raise_unobserved();
            }
            if self.ticks.is_empty() {
                return false;
            }
        }
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
    /// start time and was armed before the phase started, draining ticks and
    /// microtasks after each one.
    ///
    /// Returns `(fired_any, stop)`, where `stop` means `poll_main` asked to
    /// stop.
    fn run_timers(&self, poll_main: &mut dyn FnMut() -> bool) -> (bool, bool) {
        /// Restores set-aside timers when the phase ends, even on unwind.
        struct EndPhase<'a>(&'a RefCell<TimerQueue>);
        impl Drop for EndPhase<'_> {
            fn drop(&mut self) {
                if let Ok(mut timers) = self.0.try_borrow_mut() {
                    timers.end_phase();
                }
            }
        }

        let now = self.clock.now();
        let cutoff = self.timers.borrow().next_seq();
        let _end_phase = EndPhase(&self.timers);
        let mut fired_any = false;
        loop {
            let next = self.timers.borrow_mut().pop_expired(now, cutoff);
            let Some(key) = next else {
                return (fired_any, false);
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
                    // Re-arm (Node-style, from this phase's time) unless the
                    // callback cancelled it.
                    self.timers
                        .borrow_mut()
                        .rearm(key, now.saturating_add(period));
                }
                FireAction::Nothing => {}
            }
            if self.drain(poll_main) {
                return (true, true);
            }
        }
    }

    /// Waits until `deadline`: lets the clock jump there if it can (a
    /// `MockClock`), else parks the thread (or until a remote wake).
    fn wait_until(&self, deadline: Instant) {
        let now = self.clock.now();
        if deadline <= now || self.clock.advance_when_idle(deadline) {
            return;
        }
        park::park(Some(deadline.saturating_duration_since(now)));
    }

    /// Drops every task, tick, microtask, and timer, repeating until nothing
    /// is left (a `Drop` impl may queue more work). Everything is dropped
    /// outside of any `RefCell` borrow.
    fn tear_down(&self) {
        loop {
            let ticks = self.ticks.take_all();
            let microtasks = self.microtasks.take_all();
            let slots = self.tasks.borrow_mut().drain();
            let timers = self.timers.borrow_mut().take_all();
            if ticks.is_empty() && microtasks.is_empty() && slots.is_empty() && timers.is_empty() {
                break;
            }
            drop(ticks);
            drop(microtasks);
            drop(slots);
            drop(timers);
        }
        // A panic nobody observed before the runtime went away is dropped
        // silently: panicking inside `Drop` is not an option (§6).
        drop(self.unobserved.take());
    }

    /// The shared driver behind `block_on` and `run`.
    fn drive<F: Future>(&self, future: F, mode: Mode) -> F::Output {
        /// Poisons the runtime if a panic unwinds out of `drive`.
        struct PoisonOnUnwind<'a>(&'a Cell<bool>);
        impl Drop for PoisonOnUnwind<'_> {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    self.0.set(true);
                }
            }
        }

        assert!(!self.poisoned.get(), "{POISONED_MESSAGE}");
        let _poison = PoisonOnUnwind(&self.poisoned);

        // Uphold Node's invariant that the queues are empty before a
        // macrotask starts: flush work left over from an earlier `block_on`
        // or spawned before this call. (A leftover `Poll(MAIN_TASK)` is a
        // stale wake from an earlier main future; it is ignored.) This also
        // re-raises a panic that went unobserved between calls.
        self.drain(&mut || false);

        let mut main = MainTask {
            future: pin!(future),
            header: Arc::new(TaskHeader::new_scheduled(
                MAIN_TASK,
                Arc::clone(&self.shared),
            )),
            output: None,
        };
        let stop_when_main_done = mode == Mode::BlockOn;

        // Main's first poll is the "script": a macrotask, like a JS program's
        // top-level code. Ticks and microtasks it queues run after it.
        let done = main.poll_if_pending();
        self.raise_unobserved();
        if done && stop_when_main_done {
            return main.take_output();
        }

        loop {
            // Later polls of main are microtasks, like code after an `await`.
            let stop = self.drain(&mut || main.poll_if_pending() && stop_when_main_done);
            if stop {
                return main.take_output();
            }
            if self.absorb_remote() {
                continue;
            }

            let (fired, stop) =
                self.run_timers(&mut || main.poll_if_pending() && stop_when_main_done);
            if stop {
                return main.take_output();
            }
            if fired {
                continue;
            }

            // Idle: the queues are empty and no timer is due.
            let next_deadline = self.timers.borrow_mut().next_deadline();
            if let Some(deadline) = next_deadline {
                // Armed timers keep the loop alive.
                self.wait_until(deadline);
                continue;
            }
            if main.output.is_some() {
                return main.take_output(); // `run`: the loop is empty.
            }
            // Main is pending. It can only make progress if someone still
            // holds one of its wakers (this frame holds the header once).
            // That someone may be another thread, so park until a remote
            // wake arrives.
            if Arc::strong_count(&main.header) > 1 {
                park::park(None);
                continue;
            }
            panic!("{STALL_MESSAGE}");
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Return as soon as main completes.
    BlockOn,
    /// Return once main has completed and the loop has nothing left to do.
    Run,
}

/// The main future of a `block_on` / `run` call. It lives on the caller's
/// stack, not in the slab, and uses the reserved id `MAIN_TASK`.
struct MainTask<'a, F: Future> {
    future: Pin<&'a mut F>,
    header: Arc<TaskHeader>,
    output: Option<F::Output>,
}

impl<F: Future> MainTask<'_, F> {
    /// Polls main unless it already completed. Returns `true` if this poll
    /// completed it.
    fn poll_if_pending(&mut self) -> bool {
        if self.output.is_some() {
            return false; // A stale wake after completion.
        }
        self.header.clear_scheduled();
        let waker = Waker::from(Arc::clone(&self.header));
        match self.future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(output) => {
                self.output = Some(output);
                true
            }
            Poll::Pending => false,
        }
    }

    fn take_output(&mut self) -> F::Output {
        self.output.take().expect("main future completed")
    }
}

/// A single-threaded Spool runtime.
///
/// Most programs can use the [`run`](crate::run) or
/// [`block_on`](crate::block_on) free functions instead. A `Runtime` is useful
/// when you want to spawn tasks before running, or to drive the same runtime
/// several times.
///
/// `Runtime` is `!Send` and `!Sync`. Dropping it drops every task, tick,
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
    /// Creates a new runtime on the current thread, with a [`SystemClock`].
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

    /// Spawns a task onto this runtime. The task runs once the runtime is
    /// driven by [`run`](Self::run) or [`block_on`](Self::block_on).
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.inner.spawn(future)
    }

    /// Runs `future` to completion, then keeps running until the event loop
    /// has nothing left to do, like a Node.js process. Returns `future`'s
    /// output.
    ///
    /// The loop has "nothing left to do" when the tick and microtask queues
    /// are empty and no timer is armed. As in Node, a detached task that is
    /// pending on something else (not a timer) doesn't keep the loop alive:
    /// it stays on the runtime, and is dropped with it.
    ///
    /// Before `future` is first polled, any work already queued on the runtime
    /// (for example, tasks spawned with [`Runtime::spawn`]) runs first. The
    /// first poll of `future` then runs like a JS program's top-level code:
    /// ticks and microtasks it queues run after it. Later polls (after an
    /// `.await`) are microtasks.
    ///
    /// # Panics
    ///
    /// - If called from inside a running Spool runtime (nested `run` /
    ///   `block_on`).
    /// - If `future` stalls: it is pending, no work is queued, and no `Waker`
    ///   for it exists anywhere, so nothing can ever wake it.
    /// - If the runtime is poisoned (see below).
    ///
    /// Panics follow the JS-exception model. A panic unwinds out of `run`
    /// (the runtime then stays **poisoned**: later `run` / `block_on` calls on
    /// it panic, but a new `Runtime` works normally) when:
    ///
    /// - `future` itself, or a tick, microtask, or timer callback, panics;
    /// - a task panics and nobody can observe it: its [`JoinHandle`] was
    ///   dropped, or is dropped later without being awaited (like an
    ///   unhandled promise rejection);
    /// - a task re-throws the panic of a task it awaited, and so on up.
    ///
    /// A task's panic that *is* observed (by awaiting its `JoinHandle`, or
    /// with [`try_join`](JoinHandle::try_join)) does not reach `run`.
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
        self.inner.drive(future, Mode::Run)
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
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _enter = context::enter(&self.inner);
        self.inner.drive(future, Mode::BlockOn)
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
    /// Uses `clock` as the runtime's time source. The default is a
    /// [`SystemClock`]; tests usually want a [`MockClock`](crate::MockClock).
    pub fn clock(mut self, clock: impl Clock + 'static) -> Self {
        self.clock = Some(Rc::new(clock));
        self
    }

    /// Builds the runtime.
    pub fn build(self) -> Runtime {
        let clock = self.clock.unwrap_or_else(|| Rc::new(SystemClock::new()));
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
            .field("ticks_empty", &self.inner.ticks.is_empty())
            .field("microtasks_empty", &self.inner.microtasks.is_empty())
            .finish()
    }
}

/// Runs `future` on a new runtime until it completes **and** the event loop
/// has nothing left to do, like a Node.js process. Returns `future`'s output.
///
/// This is shorthand for `Runtime::new().run(future)`. See [`Runtime::run`].
///
/// ```
/// use std::{cell::RefCell, rc::Rc};
///
/// let log = Rc::new(RefCell::new(Vec::new()));
/// let l = log.clone();
/// spool::run(async move {
///     let (a, b) = (l.clone(), l.clone());
///     spool::queue_microtask(move || a.borrow_mut().push("microtask"));
///     spool::next_tick(move || b.borrow_mut().push("tick"));
///     l.borrow_mut().push("sync");
/// });
/// assert_eq!(*log.borrow(), ["sync", "tick", "microtask"]);
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
        assert!(rt.inner.ticks.is_empty());
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
        assert_eq!(rt.inner.ticks.len(), 1);
        assert_eq!(rt.inner.microtasks.len(), 1);
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

    mod rt_helpers {
        use std::cell::Cell;
        use std::rc::Rc;

        pub(super) fn queue_both(a: Rc<Cell<i32>>, b: Rc<Cell<i32>>) {
            crate::queue_microtask(move || a.set(a.get() + 1));
            crate::next_tick(move || b.set(b.get() + 1));
        }
    }
}
