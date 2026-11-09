//! `spawn_blocking`: run blocking or CPU-heavy work on a thread pool.
//!
//! Each runtime has its own pool, created on first use. Workers are plain
//! `std` threads that wait on a `Mutex<VecDeque>` + `Condvar`; there are at
//! most `max_threads` of them, spawned only when no idle worker can take a
//! job, and each exits after `idle_timeout` without work.
//!
//! A job's result goes into a slot shared with its [`BlockingHandle`]. The
//! worker wakes the handle's waker (the core's ordinary remote-wake path),
//! so nothing here knows about the event loop beyond two hooks: a
//! [`KeepAlive`] per job (so `run` waits for it), and an in-flight count
//! (so a `MockClock` doesn't jump while a job runs).

use std::cell::OnceCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use crate::context;
use crate::keep_alive::KeepAlive;

/// The default for [`Builder::max_blocking_threads`](crate::Builder::max_blocking_threads).
const DEFAULT_MAX_THREADS: usize = 64;
/// The default for [`Builder::blocking_idle_timeout`](crate::Builder::blocking_idle_timeout).
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// The pool's settings (`Builder::max_blocking_threads`, ...).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Config {
    pub(crate) max_threads: usize,
    pub(crate) idle_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_threads: DEFAULT_MAX_THREADS,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
    }
}

impl Config {
    /// Adds the settings to the `Builder`'s `Debug` output.
    pub(crate) fn debug_fields(&self, f: &mut fmt::DebugStruct<'_, '_>) {
        f.field("max_blocking_threads", &self.max_threads)
            .field("blocking_idle_timeout", &self.idle_timeout);
    }
}

/// A runtime's pool, created on first use.
pub(crate) struct LazyPool {
    config: Config,
    pool: OnceCell<Pool>,
}

impl LazyPool {
    pub(crate) fn new(config: Config) -> Self {
        LazyPool {
            config,
            pool: OnceCell::new(),
        }
    }

    fn get(&self) -> &Pool {
        self.pool
            .get_or_init(|| Pool::new(self.config.max_threads, self.config.idle_timeout))
    }

    /// Whether any job is queued or running.
    pub(crate) fn has_in_flight(&self) -> bool {
        self.pool.get().is_some_and(Pool::has_in_flight)
    }

    /// Shuts the pool down, if it was ever created (see [`Pool::shut_down`]).
    pub(crate) fn shut_down(&self) {
        if let Some(pool) = self.pool.get() {
            pool.shut_down();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Jobs run outside every lock, and nothing in a critical section
    // panics, so poisoning would only follow a bug. Carry on regardless.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `f` on the current runtime's blocking thread pool, and returns a
/// handle that awaits its result.
///
/// Use it for work that would block the event loop: blocking system calls
/// (files, DNS), blocking libraries, or long computations. The runtime's
/// thread keeps running tasks meanwhile. Since `f` runs on another thread,
/// it and its result must be `Send`.
///
/// ```
/// let sum = spooly::block_on(async {
///     spooly::spawn_blocking(|| (1..=100u64).sum::<u64>()).await
/// });
/// assert_eq!(sum, 5050);
/// ```
///
/// - **Liveness.** [`run`](crate::run) waits for every job that hasn't
///   finished, even one whose handle was dropped, the way it waits for a
///   [`KeepAlive`] guard.
/// - **Cancellation.** Dropping the handle detaches the job: it still runs.
///   [`BlockingHandle::cancel`] stops a job that hasn't started; nothing
///   can stop one that has.
/// - **Panics.** A panic in `f` is caught on the pool thread (keeping the
///   thread alive) and re-raised, with its original payload, in the task
///   awaiting the handle. Like any task's panic, it then unwinds out of
///   `run` / `block_on`. A detached job's panic is only printed, by the
///   panic hook.
/// - **The pool.** At most [`max_blocking_threads`] threads (64 by
///   default): further jobs queue, in FIFO order. Threads start when needed
///   and exit after [`blocking_idle_timeout`] (10 s by default) without
///   work. Dropping the runtime drops the jobs that haven't started; those
///   that are running finish on their own.
/// - **Testing.** A [`MockClock`](crate::MockClock) doesn't jump to the next
///   timer while a job is queued or running: the loop waits for the job
///   (in real time) instead, so a `timeout` around a job only fires if the
///   job is still running when the loop is otherwise idle.
///
/// Not available on WASM targets, which have no threads.
///
/// [`max_blocking_threads`]: crate::Builder::max_blocking_threads
/// [`blocking_idle_timeout`]: crate::Builder::blocking_idle_timeout
///
/// # Panics
///
/// If called outside a Spooly runtime, or if the OS can't spawn the pool's
/// first thread.
pub fn spawn_blocking<F, T>(f: F) -> BlockingHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let inner = context::current("spawn_blocking");
    let pool = inner.blocking.get();
    pool.in_flight.fetch_add(1, Ordering::Relaxed);
    let slot = Arc::new(Slot {
        state: Mutex::new(SlotState {
            phase: Phase::Queued,
            f: Some(Box::new(f)),
            result: None,
            waker: None,
            keep_alive: Some(inner.keep_alive()),
        }),
        in_flight: Arc::clone(&pool.in_flight),
    });
    pool.push(Box::new(Job {
        slot: Arc::clone(&slot),
    }));
    BlockingHandle { slot }
}

/// The result of a job, as stored for its handle.
type JobResult<T> = thread::Result<T>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Queued,
    Running,
    /// Finished (or panicked); the result is in the slot until taken.
    Done,
    /// Cancelled before it started, or dropped unrun with its runtime.
    Cancelled,
}

/// The state a job shares with its handle.
struct Slot<T> {
    state: Mutex<SlotState<T>>,
    /// The pool's count of jobs queued or running, decremented once per job.
    in_flight: Arc<AtomicUsize>,
}

struct SlotState<T> {
    phase: Phase,
    /// The closure, while the job is queued. Kept here, not in the pool's
    /// queue, so that `cancel` can drop it at once.
    f: Option<Box<dyn FnOnce() -> T + Send>>,
    result: Option<JobResult<T>>,
    waker: Option<Waker>,
    /// Keeps `run` alive until the job is done or cancelled.
    keep_alive: Option<KeepAlive>,
}

impl<T> Slot<T> {
    /// Marks a queued job cancelled, and drops its closure. Returns `false`
    /// if it already started.
    fn cancel(&self) -> bool {
        let (f, keep_alive) = {
            let mut state = lock(&self.state);
            if state.phase != Phase::Queued {
                return false;
            }
            state.phase = Phase::Cancelled;
            self.in_flight.fetch_sub(1, Ordering::Release);
            (state.f.take(), state.keep_alive.take())
        };
        // Outside the lock, the closure first: `run` may return as soon as
        // the `KeepAlive` goes. Any waker stays: polling a handle cancelled
        // this way (only by the runtime's shutdown) panics.
        drop(f);
        drop(keep_alive);
        true
    }

    fn complete(&self, result: JobResult<T>) {
        let (waker, keep_alive) = {
            let mut state = lock(&self.state);
            state.phase = Phase::Done;
            state.result = Some(result);
            (state.waker.take(), state.keep_alive.take())
        };
        // The order matters: the wake is queued before the job stops
        // counting as in flight (so a `MockClock` loop that sees the count
        // drop to zero also sees the wake), and before the `KeepAlive` goes.
        if let Some(waker) = waker {
            waker.wake();
        }
        if self.in_flight.fetch_sub(1, Ordering::Release) == 1 {
            // The last job: a `MockClock` loop waiting for jobs to finish
            // may jump now. Wake it even if nothing else would (a detached
            // job has no waker, and other `KeepAlive`s may exist).
            if let Some(keep_alive) = &keep_alive {
                keep_alive.wake_loop();
            }
        }
        drop(keep_alive);
    }
}

/// A queued job, type-erased for the pool.
trait Runnable: Send {
    fn run(self: Box<Self>);
}

struct Job<T> {
    slot: Arc<Slot<T>>,
}

impl<T: Send> Runnable for Job<T> {
    fn run(self: Box<Self>) {
        let f = {
            let mut state = lock(&self.slot.state);
            if state.phase != Phase::Queued {
                return; // Cancelled.
            }
            state.phase = Phase::Running;
            state.f.take().expect("a queued job has its closure")
        };
        let result = panic::catch_unwind(AssertUnwindSafe(f));
        self.slot.complete(result);
    }
}

impl<T> Drop for Job<T> {
    fn drop(&mut self) {
        // Dropped unrun if still queued: its runtime shut down. (Otherwise
        // it ran, or was cancelled, and this does nothing.)
        self.slot.cancel();
    }
}

/// An owned permission to await the result of a [`spawn_blocking`] job.
///
/// Awaiting it returns the closure's result, or re-raises its panic.
/// Dropping it detaches the job, which still runs to completion.
#[must_use = "dropping a `BlockingHandle` detaches the job; await it to get its result"]
pub struct BlockingHandle<T> {
    slot: Arc<Slot<T>>,
}

impl<T> BlockingHandle<T> {
    /// Cancels the job if it hasn't started yet: it then never runs, and
    /// its closure is dropped at once. Returns `false`, and lets the job run
    /// on, detached, if it already started.
    ///
    /// ```
    /// let rt = spooly::Runtime::builder().max_blocking_threads(1).build();
    /// rt.block_on(async {
    ///     let (tx, rx) = std::sync::mpsc::channel::<()>();
    ///     // Occupy the only thread, so the next job has to queue.
    ///     let busy = spooly::spawn_blocking(move || rx.recv().unwrap());
    ///     let queued = spooly::spawn_blocking(|| unreachable!());
    ///     assert!(queued.cancel());
    ///     tx.send(()).unwrap();
    ///     busy.await;
    /// });
    /// ```
    pub fn cancel(self) -> bool {
        self.slot.cancel()
    }

    /// Whether the job has finished (or panicked), so that awaiting the
    /// handle returns at once.
    pub fn is_finished(&self) -> bool {
        lock(&self.slot.state).phase == Phase::Done
    }
}

impl<T> Future for BlockingHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut state = lock(&self.slot.state);
        match state.phase {
            Phase::Queued | Phase::Running => {
                match &state.waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => state.waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
            Phase::Done => match state.result.take() {
                Some(Ok(value)) => Poll::Ready(value),
                Some(Err(payload)) => {
                    drop(state);
                    panic::resume_unwind(payload)
                }
                None => panic!("spooly: `BlockingHandle` polled after it completed"),
            },
            Phase::Cancelled => panic!(
                "spooly: the blocking job never ran: its runtime was dropped while it was queued"
            ),
        }
    }
}

impl<T> fmt::Debug for BlockingHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingHandle")
            .field("phase", &lock(&self.slot.state).phase)
            .finish()
    }
}

/// A runtime's blocking thread pool.
struct Pool {
    shared: Arc<PoolShared>,
    /// Jobs queued or running (not cancelled).
    in_flight: Arc<AtomicUsize>,
}

struct PoolShared {
    state: Mutex<PoolState>,
    /// Signalled when a job is queued, and on shutdown.
    work: Condvar,
    max_threads: usize,
    idle_timeout: Duration,
}

struct PoolState {
    queue: VecDeque<Box<dyn Runnable>>,
    /// Worker threads alive (or being spawned).
    threads: usize,
    /// Workers waiting for a job.
    idle: usize,
    shutdown: bool,
}

impl Pool {
    fn new(max_threads: usize, idle_timeout: Duration) -> Pool {
        Pool {
            shared: Arc::new(PoolShared {
                state: Mutex::new(PoolState {
                    queue: VecDeque::new(),
                    threads: 0,
                    idle: 0,
                    shutdown: false,
                }),
                work: Condvar::new(),
                max_threads,
                idle_timeout,
            }),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Whether any job is queued or running.
    fn has_in_flight(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) > 0
    }

    fn push(&self, job: Box<dyn Runnable>) {
        let spawn = {
            let mut state = lock(&self.shared.state);
            state.queue.push_back(job);
            if state.idle > 0 {
                self.shared.work.notify_one();
            }
            // Idle workers can take `idle` jobs (counting those already
            // notified but not yet awake): spawn a thread for the rest.
            let spawn = state.queue.len() > state.idle && state.threads < self.shared.max_threads;
            if spawn {
                state.threads += 1;
            }
            spawn
        };
        if spawn {
            self.spawn_worker();
        }
    }

    fn spawn_worker(&self) {
        let shared = Arc::clone(&self.shared);
        let spawned = thread::Builder::new()
            .name("spooly-blocking".into())
            .spawn(move || shared.work());
        if let Err(e) = spawned {
            let mut state = lock(&self.shared.state);
            state.threads -= 1;
            // Other workers will get to the job; with none, nothing would.
            assert!(
                state.threads > 0,
                "spooly: failed to spawn a blocking pool thread: {e}"
            );
        }
    }

    /// Drops the queued jobs, and lets the idle workers exit. Running jobs
    /// finish; their workers then exit.
    fn shut_down(&self) {
        let queued = {
            let mut state = lock(&self.shared.state);
            state.shutdown = true;
            std::mem::take(&mut state.queue)
        };
        self.shared.work.notify_all();
        // Outside the lock: dropping a job drops its closure, which may do
        // anything.
        drop(queued);
    }
}

impl PoolShared {
    /// A worker's loop.
    fn work(&self) {
        let mut state = lock(&self.state);
        loop {
            if let Some(job) = state.queue.pop_front() {
                drop(state);
                job.run();
                state = lock(&self.state);
                continue;
            }
            if state.shutdown {
                break;
            }
            state.idle += 1;
            let (guard, wait) = self
                .work
                .wait_timeout(state, self.idle_timeout)
                .unwrap_or_else(PoisonError::into_inner);
            state = guard;
            state.idle -= 1;
            if wait.timed_out() && state.queue.is_empty() {
                break;
            }
        }
        state.threads -= 1;
    }
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.shared.state);
        f.debug_struct("Pool")
            .field("threads", &state.threads)
            .field("idle", &state.idle)
            .field("queued", &state.queue.len())
            .finish()
    }
}
