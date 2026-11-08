//! Tasks, `spawn`, `JoinHandle`, and cancellation (`abort`).

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::Future;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::context;
use crate::runtime::Inner;
use crate::slab::Key;
use crate::waker::TaskHeader;

/// Identifies a task within its runtime: a slab key (index + generation).
pub(crate) type TaskId = Key;

/// The id used for the `block_on` main future, which never lives in the slab.
pub(crate) const MAIN_TASK: TaskId = Key::SENTINEL;

/// A type-erased task future. Its output is written into a [`JoinCell`].
pub(crate) type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;

/// A caught panic.
pub(crate) type Payload = Box<dyn Any + Send + 'static>;

/// A task's entry in the runtime's slab.
pub(crate) struct TaskSlot {
    /// `None` while the task is being polled.
    pub(crate) future: Option<BoxFuture>,
    /// The slab holds exactly one strong reference; every other reference is
    /// an outstanding `Waker`.
    pub(crate) header: Arc<TaskHeader>,
    /// Where a panic during a poll is delivered.
    pub(crate) join: Rc<dyn JoinDyn>,
    /// `abort()` was called while the task was being polled: drop the future
    /// as soon as the poll returns.
    pub(crate) aborted: bool,
}

enum JoinState<T> {
    /// Still running. Holds the waker of whoever is awaiting the handle.
    Pending { waker: Option<Waker> },
    /// Finished; the output is waiting to be taken.
    Ready(T),
    /// The task panicked; the payload is waiting to be observed.
    Panicked(Payload),
    /// The task was aborted (or dropped with its runtime) before finishing.
    Aborted,
    /// The result was taken by awaiting the handle.
    Consumed,
}

/// The state shared by a task and its `JoinHandle`.
struct JoinCell<T> {
    state: RefCell<JoinState<T>>,
    /// The `JoinHandle` was dropped: nobody will observe the result.
    detached: Cell<bool>,
}

impl<T> JoinCell<T> {
    /// Resolves a pending task and wakes its joiner. Returns the previous
    /// state's waker; the caller wakes it once nothing is borrowed.
    fn resolve(&self, new: JoinState<T>) -> Option<Waker> {
        let previous = std::mem::replace(&mut *self.state.borrow_mut(), new);
        match previous {
            JoinState::Pending { waker } => waker,
            _ => unreachable!("spool: task resolved twice"),
        }
    }

    fn is_pending(&self) -> bool {
        matches!(&*self.state.borrow(), JoinState::Pending { .. })
    }
}

/// The type-erased side of a [`JoinCell`] that the runtime needs.
pub(crate) trait JoinDyn {
    /// Delivers a panic from the task's poll. Returns the payload back if
    /// nobody can observe it (the handle was dropped): it is then an
    /// unobserved panic.
    fn reject(&self, payload: Payload) -> Option<Payload>;

    /// The data pointer of the waker stored by whoever awaits this task's
    /// handle, if any. That waker can only fire once this task finishes,
    /// which the stall check (§3.5) uses.
    fn joiner_waker_data(&self) -> Option<*const ()>;
}

impl<T> JoinDyn for JoinCell<T> {
    fn reject(&self, payload: Payload) -> Option<Payload> {
        if self.detached.get() {
            *self.state.borrow_mut() = JoinState::Consumed;
            return Some(payload);
        }
        // Unwinding out of the task's `async` block already dropped its
        // locals, including the abort guard, which marked the task aborted
        // (and woke the joiner). The panic is the real outcome.
        let previous =
            std::mem::replace(&mut *self.state.borrow_mut(), JoinState::Panicked(payload));
        match previous {
            JoinState::Pending { waker: Some(waker) } => waker.wake(),
            JoinState::Pending { waker: None } | JoinState::Aborted => {}
            _ => unreachable!("spool: a task panicked after it finished"),
        }
        None
    }

    fn joiner_waker_data(&self) -> Option<*const ()> {
        match &*self.state.borrow() {
            JoinState::Pending { waker: Some(waker) } => Some(waker.data()),
            _ => None,
        }
    }
}

/// Lives inside the task's future. If the future is dropped before the task
/// finished (abort, or runtime teardown), it marks the task aborted and wakes
/// the joiner.
struct AbortGuard<T>(Rc<JoinCell<T>>);

impl<T> Drop for AbortGuard<T> {
    fn drop(&mut self) {
        if self.0.is_pending()
            && let Some(waker) = self.0.resolve(JoinState::Aborted)
        {
            waker.wake();
        }
    }
}

/// Wraps a user future so that its output is delivered to its `JoinHandle`.
/// Returns the boxed future, the type-erased join state for the slab, and the
/// join cell for the handle.
fn wrap<F>(fut: F) -> (BoxFuture, Rc<dyn JoinDyn>, Rc<JoinCell<F::Output>>)
where
    F: Future + 'static,
    F::Output: 'static,
{
    let cell = Rc::new(JoinCell {
        state: RefCell::new(JoinState::Pending { waker: None }),
        detached: Cell::new(false),
    });
    let guard = AbortGuard(Rc::clone(&cell));
    let wrapped = Box::pin(async move {
        let guard = guard;
        // `fut` (and everything it owns) is dropped as soon as it completes,
        // before the joiner is woken.
        let output = fut.await;
        complete(&guard.0, output);
    });
    let join: Rc<dyn JoinDyn> = Rc::clone(&cell) as Rc<dyn JoinDyn>;
    (wrapped, join, cell)
}

fn complete<T>(cell: &JoinCell<T>, output: T) {
    if cell.detached.get() {
        // Nobody wants the output: drop it here, outside of any borrow.
        *cell.state.borrow_mut() = JoinState::Consumed;
        drop(output);
        return;
    }
    if let Some(waker) = cell.resolve(JoinState::Ready(output)) {
        // Waking the joiner queues it as a microtask, just like a promise
        // resolving.
        waker.wake();
    }
}

/// Inserts a new task into `inner`'s slab and queues its first poll.
pub(crate) fn spawn_on<F>(inner: &Inner, this: Weak<Inner>, future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let (wrapped, join, cell) = wrap(future);
    let id = inner.insert_task(wrapped, join);
    JoinHandle {
        cell,
        task: AbortHandle { inner: this, id },
        abort_on_drop: false,
    }
}

/// An owned handle to a spawned task, which can be awaited to get the task's
/// output.
///
/// Dropping a `JoinHandle` **detaches** the task: it keeps running, and its
/// output is dropped when it finishes. To cancel the task instead, call
/// [`abort`](Self::abort), or turn the handle into an [`AbortOnDrop`].
///
/// `JoinHandle` is `!Send`: it belongs to the runtime's thread.
///
/// # Panics
///
/// Awaiting a `JoinHandle` behaves like awaiting a JS promise:
///
/// - If the task **panicked**, the panic is re-thrown in the awaiting task
///   (with [`resume_unwind`]). Use [`try_join`](Self::try_join) to get it as
///   an `Err` instead.
/// - If the task was **aborted**, awaiting panics with "task was aborted".
/// - Polling the handle again after it returned panics.
///
/// If the task panics and its handle is dropped without the panic being
/// observed, the panic unwinds out of `run` / `block_on`, rather than being
/// silently swallowed.
///
/// # Example
///
/// ```
/// let answer = spool::block_on(async {
///     let handle = spool::spawn(async { 6 * 7 });
///     handle.await
/// });
/// assert_eq!(answer, 42);
/// ```
#[must_use = "dropping a JoinHandle detaches the task; `.await` it to get its output"]
pub struct JoinHandle<T> {
    cell: Rc<JoinCell<T>>,
    task: AbortHandle,
    abort_on_drop: bool,
}

impl<T> JoinHandle<T> {
    /// Returns `true` if the task has finished: it completed, panicked, or
    /// was aborted.
    ///
    /// ```
    /// spool::block_on(async {
    ///     let handle = spool::spawn(async {});
    ///     assert!(!handle.is_finished());
    ///     spool::yield_now().await;
    ///     assert!(handle.is_finished());
    /// });
    /// ```
    pub fn is_finished(&self) -> bool {
        !self.cell.is_pending()
    }

    /// Cancels the task. See [`AbortHandle::abort`].
    ///
    /// ```
    /// spool::block_on(async {
    ///     let task = spool::spawn(std::future::pending::<()>());
    ///     task.abort();
    ///     let err = task.try_join().await.unwrap_err();
    ///     assert!(err.is_aborted());
    /// });
    /// ```
    pub fn abort(&self) {
        self.task.abort();
    }

    /// Returns a cloneable handle that can abort the task from elsewhere,
    /// without owning its output.
    pub fn abort_handle(&self) -> AbortHandle {
        self.task.clone()
    }

    /// Turns this handle into one that aborts the task when dropped, which
    /// ties the task to a scope.
    ///
    /// ```
    /// use std::{cell::Cell, rc::Rc};
    ///
    /// spool::block_on(async {
    ///     let ran = Rc::new(Cell::new(false));
    ///     let r = ran.clone();
    ///     let task = spool::spawn(async move { r.set(true) }).abort_on_drop();
    ///     drop(task); // Aborted before it ever ran.
    ///     spool::yield_now().await;
    ///     assert!(!ran.get());
    /// });
    /// ```
    pub fn abort_on_drop(mut self) -> AbortOnDrop<T> {
        self.abort_on_drop = true;
        AbortOnDrop { handle: self }
    }

    /// Waits for the task without re-throwing: returns `Err` if the task
    /// panicked or was aborted, like `try { await p } catch (e) { ... }`.
    ///
    /// ```
    /// spool::block_on(async {
    ///     let handle = spool::spawn(async { panic!("oh no") });
    ///     let err = handle.try_join().await.unwrap_err();
    ///     assert!(err.is_panic());
    /// });
    /// ```
    pub fn try_join(self) -> TryJoin<T> {
        TryJoin { handle: self }
    }

    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        let mut state = self.cell.state.borrow_mut();
        match &mut *state {
            JoinState::Pending { waker } => {
                match waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => *waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
            JoinState::Consumed => panic!("spool: `JoinHandle` polled after it completed"),
            _ => match std::mem::replace(&mut *state, JoinState::Consumed) {
                JoinState::Ready(output) => Poll::Ready(Ok(output)),
                JoinState::Panicked(payload) => Poll::Ready(Err(JoinError::Panicked(payload))),
                JoinState::Aborted => Poll::Ready(Err(JoinError::Aborted)),
                JoinState::Pending { .. } | JoinState::Consumed => unreachable!(),
            },
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match self.poll_result(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(output)) => Poll::Ready(output),
            Poll::Ready(Err(JoinError::Panicked(payload))) => resume_unwind(payload),
            Poll::Ready(Err(JoinError::Aborted)) => {
                panic!("spool: task was aborted (use `try_join` to handle this)")
            }
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        self.cell.detached.set(true);
        // Take what the handle owns (the joiner's waker, or a finished
        // result), and drop it outside the borrow.
        let old = {
            let mut state = self.cell.state.borrow_mut();
            match &mut *state {
                JoinState::Pending { waker } => JoinState::Pending {
                    waker: waker.take(),
                },
                _ => std::mem::replace(&mut *state, JoinState::Consumed),
            }
        };
        match old {
            // A panic nobody observed: hand it to the runtime, which
            // re-raises it. If the runtime is gone, it is dropped silently.
            JoinState::Panicked(payload) => {
                if let Some(inner) = self.task.inner.upgrade() {
                    inner.report_unobserved(payload);
                }
            }
            other => drop(other),
        }
        if self.abort_on_drop {
            self.task.abort();
        }
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("finished", &self.is_finished())
            .finish()
    }
}

/// A cloneable handle that cancels a task, created by
/// [`JoinHandle::abort_handle`].
///
/// Unlike the `JoinHandle`, it doesn't own the task's output, and dropping it
/// does nothing. `AbortHandle` is `!Send`.
///
/// ```
/// use std::time::Duration;
///
/// let rt = spool::Runtime::builder().clock(spool::MockClock::new()).build();
/// rt.block_on(async {
///     let worker = spool::spawn(async {
///         spool::sleep(Duration::from_secs(60)).await;
///         "finished"
///     });
///     // A watchdog that cancels the worker after 5s.
///     let abort = worker.abort_handle();
///     spool::set_timeout(Duration::from_secs(5), move || abort.abort());
///     assert!(worker.try_join().await.unwrap_err().is_aborted());
///     assert_eq!(spool::now().epoch_offset(), Duration::from_secs(5));
/// });
/// ```
#[derive(Clone)]
pub struct AbortHandle {
    inner: Weak<Inner>,
    id: TaskId,
}

impl AbortHandle {
    /// Cancels the task: its future is dropped, and it never runs again.
    ///
    /// - If the task is idle (or queued to run), its future is dropped right
    ///   away, and any queued poll becomes a no-op.
    /// - If the task is running (it aborted itself, or it is being polled
    ///   further up the stack), its future is dropped as soon as its current
    ///   poll returns. If that poll completes the task, the output is kept.
    /// - If the task already finished, this does nothing.
    ///
    /// Awaiting an aborted task's `JoinHandle` panics; use
    /// [`try_join`](JoinHandle::try_join) to get `Err(JoinError::Aborted)`.
    /// Tasks spawned by the aborted task are *not* aborted.
    pub fn abort(&self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.abort_task(self.id);
        }
    }

    /// Returns `true` if the task has finished: it completed, panicked, or
    /// was aborted (or its runtime was dropped).
    pub fn is_finished(&self) -> bool {
        self.inner
            .upgrade()
            .is_none_or(|inner| !inner.has_task(self.id))
    }
}

impl fmt::Debug for AbortHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortHandle")
            .field("finished", &self.is_finished())
            .finish()
    }
}

/// A [`JoinHandle`] that aborts its task when dropped, created by
/// [`JoinHandle::abort_on_drop`].
///
/// Awaiting it behaves exactly like awaiting the `JoinHandle`.
///
/// ```
/// use std::{cell::Cell, rc::Rc};
///
/// spool::block_on(async {
///     let beats = Rc::new(Cell::new(0));
///     {
///         let b = beats.clone();
///         // A heartbeat that lives exactly as long as this scope.
///         let _heartbeat = spool::spawn(async move {
///             loop {
///                 b.set(b.get() + 1);
///                 spool::yield_now().await;
///             }
///         })
///         .abort_on_drop();
///         for _ in 0..3 {
///             spool::yield_now().await;
///         }
///     }
///     let seen = beats.get();
///     spool::yield_now().await;
///     assert_eq!(beats.get(), seen, "aborted at the end of the scope");
/// });
/// ```
#[must_use = "dropping an AbortOnDrop aborts the task"]
pub struct AbortOnDrop<T> {
    handle: JoinHandle<T>,
}

impl<T> AbortOnDrop<T> {
    /// Returns `true` if the task has finished. See
    /// [`JoinHandle::is_finished`].
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Cancels the task now. See [`AbortHandle::abort`].
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// Returns a cloneable handle that can abort the task from elsewhere.
    pub fn abort_handle(&self) -> AbortHandle {
        self.handle.abort_handle()
    }

    /// Like [`JoinHandle::try_join`]. The returned future still aborts the
    /// task if it is dropped before the task finishes.
    pub fn try_join(self) -> TryJoin<T> {
        self.handle.try_join()
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        Pin::new(&mut self.handle).poll(cx)
    }
}

impl<T> fmt::Debug for AbortOnDrop<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortOnDrop")
            .field("finished", &self.is_finished())
            .finish()
    }
}

/// The future returned by [`JoinHandle::try_join`]: the task's output, or a
/// [`JoinError`] if it panicked or was aborted.
///
/// ```
/// spool::block_on(async {
///     let ok = spool::spawn(async { 1 }).try_join().await;
///     assert_eq!(ok.unwrap(), 1);
///     let tj = spool::spawn(std::future::pending::<i32>()).try_join();
///     tj.abort();
///     assert!(tj.is_finished());
///     assert!(tj.await.is_err());
/// });
/// ```
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TryJoin<T> {
    handle: JoinHandle<T>,
}

impl<T> TryJoin<T> {
    /// Returns `true` if the task has finished.
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Cancels the task. See [`AbortHandle::abort`].
    pub fn abort(&self) {
        self.handle.abort();
    }
}

impl<T> Future for TryJoin<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.handle.poll_result(cx)
    }
}

impl<T> fmt::Debug for TryJoin<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TryJoin")
            .field("finished", &self.is_finished())
            .finish()
    }
}

/// Why a task produced no output, returned by [`JoinHandle::try_join`].
///
/// ```
/// use spool::JoinError;
///
/// spool::block_on(async {
///     match spool::spawn(async { panic!("bad input") }).try_join().await {
///         Ok(()) => unreachable!(),
///         Err(JoinError::Panicked(payload)) => {
///             assert_eq!(payload.downcast_ref::<&str>(), Some(&"bad input"));
///             // Or re-throw it: std::panic::resume_unwind(payload).
///         }
///         Err(JoinError::Aborted) => unreachable!(),
///     }
/// });
/// ```
pub enum JoinError {
    /// The task panicked. Holds the panic payload, which can be re-thrown
    /// with [`std::panic::resume_unwind`].
    Panicked(Box<dyn Any + Send + 'static>),
    /// The task was aborted before it finished.
    Aborted,
}

impl JoinError {
    /// Returns `true` if the task panicked.
    pub fn is_panic(&self) -> bool {
        matches!(self, JoinError::Panicked(_))
    }

    /// Returns `true` if the task was aborted.
    pub fn is_aborted(&self) -> bool {
        matches!(self, JoinError::Aborted)
    }

    /// Returns the panic payload, or `Err(self)` if the task was aborted.
    ///
    /// ```
    /// spool::block_on(async {
    ///     let err = spool::spawn(async { panic!("boom") }).try_join().await.unwrap_err();
    ///     let payload = err.try_into_panic().ok().unwrap();
    ///     assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
    /// });
    /// ```
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, JoinError> {
        match self {
            JoinError::Panicked(payload) => Ok(payload),
            JoinError::Aborted => Err(self),
        }
    }

    /// The panic message, if the task panicked with a string message.
    fn panic_message(&self) -> Option<&str> {
        match self {
            JoinError::Panicked(payload) => payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str)),
            JoinError::Aborted => None,
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Panicked(_) => match self.panic_message() {
                Some(msg) => f.debug_tuple("Panicked").field(&msg).finish(),
                None => f.write_str("Panicked(..)"),
            },
            JoinError::Aborted => f.write_str("Aborted"),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Panicked(_) => match self.panic_message() {
                Some(msg) => write!(f, "task panicked: {msg}"),
                None => f.write_str("task panicked"),
            },
            JoinError::Aborted => f.write_str("task was aborted"),
        }
    }
}

impl std::error::Error for JoinError {}

/// Spawns a task onto the current runtime.
///
/// The future doesn't need to be `Send`. Its first poll is queued as a
/// microtask, so it runs after the current task yields, in FIFO order with
/// other queued work. (Unlike a JS async function, it does *not* run
/// synchronously up to its first `.await`; see design decision D4.)
///
/// # Panics
///
/// If called outside a Spool runtime.
///
/// # Example
///
/// ```
/// use std::{cell::RefCell, rc::Rc};
///
/// spool::block_on(async {
///     let log = Rc::new(RefCell::new(Vec::new()));
///     let l = log.clone();
///     let handle = spool::spawn(async move { l.borrow_mut().push("task") });
///     log.borrow_mut().push("main");
///     handle.await;
///     assert_eq!(*log.borrow(), ["main", "task"]);
/// });
/// ```
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    context::current("spawn").spawn(future)
}
