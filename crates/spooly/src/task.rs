//! Tasks, `spawn`, `JoinHandle`, and cancellation (`abort`).

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
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

/// A task's entry in the runtime's slab.
pub(crate) struct TaskSlot {
    /// `None` while the task is being polled.
    pub(crate) future: Option<BoxFuture>,
    /// The slab holds exactly one strong reference; every other reference is
    /// an outstanding `Waker`.
    pub(crate) header: Arc<TaskHeader>,
    /// The task's join state, which the stall check reads.
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
    /// The task was aborted (or dropped with its runtime, or unwound by a
    /// panic) before finishing.
    Aborted,
    /// The result was taken by awaiting the handle, or dropped with it.
    Consumed,
    /// The handle was dropped while the task was still running: nobody will
    /// take the output, so it is dropped as soon as the task completes.
    Detached,
}

/// The state shared by a task and its `JoinHandle`.
struct JoinCell<T> {
    state: RefCell<JoinState<T>>,
}

impl<T> JoinCell<T> {
    /// Resolves a pending task and wakes its joiner. Returns the previous
    /// state's waker; the caller wakes it once nothing is borrowed.
    fn resolve(&self, new: JoinState<T>) -> Option<Waker> {
        let previous = std::mem::replace(&mut *self.state.borrow_mut(), new);
        match previous {
            JoinState::Pending { waker } => waker,
            _ => unreachable!("spooly: task resolved twice"),
        }
    }

    fn is_pending(&self) -> bool {
        matches!(&*self.state.borrow(), JoinState::Pending { .. })
    }

    fn is_detached(&self) -> bool {
        matches!(&*self.state.borrow(), JoinState::Detached)
    }
}

/// The type-erased side of a [`JoinCell`] that the runtime needs.
pub(crate) trait JoinDyn {
    /// The data pointer of the waker stored by whoever awaits this task's
    /// handle, if any. That waker can only fire once this task finishes,
    /// which the stall check (§3.5) uses.
    fn joiner_waker_data(&self) -> Option<*const ()>;
}

impl<T> JoinDyn for JoinCell<T> {
    fn joiner_waker_data(&self) -> Option<*const ()> {
        match &*self.state.borrow() {
            JoinState::Pending { waker: Some(waker) } => Some(waker.data()),
            _ => None,
        }
    }
}

/// Lives inside the task's future. If the future is dropped before the task
/// finished (abort, runtime teardown, or a panic unwinding through it), it
/// marks the task aborted and wakes the joiner.
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
    if cell.is_detached() {
        // Nobody wants the output: drop it here, outside of any borrow.
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
/// - If the task was **aborted**, awaiting panics with "task was aborted".
///   Use [`try_join`](Self::try_join) to get an `Err` instead.
/// - Polling the handle again after it returned panics.
///
/// A panic *inside* the task is not delivered here: Spooly doesn't catch
/// panics, so it unwinds out of `run` / `block_on` (see the
/// [crate docs](crate#panics)). A task that can fail should return a
/// `Result`.
///
/// # Example
///
/// ```
/// let answer = spooly::block_on(async {
///     let handle = spooly::spawn(async { 6 * 7 });
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
    /// Returns `true` if the task has finished: it completed or was aborted.
    ///
    /// ```
    /// spooly::block_on(async {
    ///     let handle = spooly::spawn(async {});
    ///     assert!(!handle.is_finished());
    ///     spooly::yield_now().await;
    ///     assert!(handle.is_finished());
    /// });
    /// ```
    pub fn is_finished(&self) -> bool {
        !self.cell.is_pending()
    }

    /// Cancels the task. See [`AbortHandle::abort`].
    ///
    /// ```
    /// spooly::block_on(async {
    ///     let task = spooly::spawn(std::future::pending::<()>());
    ///     task.abort();
    ///     assert!(task.try_join().await.is_err());
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
    /// spooly::block_on(async {
    ///     let ran = Rc::new(Cell::new(false));
    ///     let r = ran.clone();
    ///     let task = spooly::spawn(async move { r.set(true) }).abort_on_drop();
    ///     drop(task); // Aborted before it ever ran.
    ///     spooly::yield_now().await;
    ///     assert!(!ran.get());
    /// });
    /// ```
    pub fn abort_on_drop(mut self) -> AbortOnDrop<T> {
        self.abort_on_drop = true;
        AbortOnDrop { handle: self }
    }

    /// Waits for the task without panicking if it was aborted: returns
    /// `Err(JoinError)` instead.
    ///
    /// ```
    /// spooly::block_on(async {
    ///     let handle = spooly::spawn(std::future::pending::<()>());
    ///     handle.abort();
    ///     let err = handle.try_join().await.unwrap_err();
    ///     assert_eq!(err.to_string(), "task was aborted");
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
            JoinState::Consumed => panic!("spooly: `JoinHandle` polled after it completed"),
            _ => match std::mem::replace(&mut *state, JoinState::Consumed) {
                JoinState::Ready(output) => Poll::Ready(Ok(output)),
                JoinState::Aborted => Poll::Ready(Err(JoinError(()))),
                // A live handle never sees `Detached`.
                JoinState::Pending { .. } | JoinState::Consumed | JoinState::Detached => {
                    unreachable!()
                }
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
            Poll::Ready(Err(_)) => {
                panic!("spooly: task was aborted (use `try_join` to handle this)")
            }
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        // Take what the handle owns (the joiner's waker, or a finished
        // result), and drop it outside the borrow.
        let old = {
            let mut state = self.cell.state.borrow_mut();
            let next = match *state {
                JoinState::Pending { .. } => JoinState::Detached,
                _ => JoinState::Consumed,
            };
            std::mem::replace(&mut *state, next)
        };
        drop(old);
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
/// let rt = spooly::Runtime::builder().clock(spooly::MockClock::new()).build();
/// rt.block_on(async {
///     let worker = spooly::spawn(async {
///         spooly::sleep(Duration::from_secs(60)).await;
///         "finished"
///     });
///     // A watchdog that cancels the worker after 5s.
///     let abort = worker.abort_handle();
///     spooly::set_timeout(Duration::from_secs(5), move || abort.abort());
///     assert!(worker.try_join().await.is_err());
///     assert_eq!(spooly::now().epoch_offset(), Duration::from_secs(5));
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
    /// [`try_join`](JoinHandle::try_join) to get `Err(JoinError)`.
    /// Tasks spawned by the aborted task are *not* aborted.
    pub fn abort(&self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.abort_task(self.id);
        }
    }

    /// Returns `true` if the task has finished: it completed or was aborted
    /// (or its runtime was dropped).
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
/// spooly::block_on(async {
///     let beats = Rc::new(Cell::new(0));
///     {
///         let b = beats.clone();
///         // A heartbeat that lives exactly as long as this scope.
///         let _heartbeat = spooly::spawn(async move {
///             loop {
///                 b.set(b.get() + 1);
///                 spooly::yield_now().await;
///             }
///         })
///         .abort_on_drop();
///         for _ in 0..3 {
///             spooly::yield_now().await;
///         }
///     }
///     let seen = beats.get();
///     spooly::yield_now().await;
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
/// [`JoinError`] if it was aborted.
///
/// ```
/// spooly::block_on(async {
///     let ok = spooly::spawn(async { 1 }).try_join().await;
///     assert_eq!(ok.unwrap(), 1);
///     let tj = spooly::spawn(std::future::pending::<i32>()).try_join();
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

/// Why a task produced no output, returned by [`JoinHandle::try_join`]: it
/// was aborted before it finished.
///
/// The only cause today is an abort (including the task's runtime being
/// dropped, or a panic unwinding through the task). It is opaque so that
/// more causes can be added later.
///
/// ```
/// spooly::block_on(async {
///     let task = spooly::spawn(std::future::pending::<()>());
///     task.abort();
///     let err = task.try_join().await.unwrap_err();
///     assert_eq!(err.to_string(), "task was aborted");
/// });
/// ```
pub struct JoinError(());

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JoinError(Aborted)")
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("task was aborted")
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
/// If called outside a Spooly runtime.
///
/// # Example
///
/// ```
/// use std::{cell::RefCell, rc::Rc};
///
/// spooly::block_on(async {
///     let log = Rc::new(RefCell::new(Vec::new()));
///     let l = log.clone();
///     let handle = spooly::spawn(async move { l.borrow_mut().push("task") });
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
