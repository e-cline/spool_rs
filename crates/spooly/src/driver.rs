//! Extending the event loop with an I/O driver.
//!
//! An [`IoDriver`] lets the loop wait on something besides timers: usually
//! an I/O reactor (epoll, kqueue, WASI pollables) from a crate such as
//! `spooly-io`. Install one with
//! [`Builder::io_driver`](crate::Builder::io_driver); the types that use it
//! find it again with [`current_io_driver`].
//!
//! Most programs never touch this module: it is for crates that implement
//! I/O on top of Spooly. **It is unstable**: it may change in any release
//! before 1.0.
//!
//! # Where the driver runs in the loop
//!
//! Each step of the event loop drains the microtask queue, runs the timers
//! that are due, and then runs the **I/O phase**: it calls
//! [`IoDriver::poll_ready`], which wakes every task whose I/O is ready.
//! Those tasks are queued as microtasks, like any other wake, and the queue
//! is drained again. The I/O phase runs in every step, even one in which
//! timers fired, so timers can't starve I/O.
//!
//! When there is nothing to do until the next timer or some I/O, the native
//! driver (behind [`run`](crate::run) and [`block_on`](crate::block_on))
//! calls [`IoDriver::wait`] instead of parking its thread. The browser
//! driver never blocks, so it never calls `wait`.

// The drivers below are Spooly's own (internal): the per-target code that
// calls the event loop's `step` and waits between steps (D5, §7). The step
// never blocks; a driver decides how to wait. Natively, that is parking the
// thread, or the I/O driver's `wait` (`native`); in the browser, steps run
// from host callbacks (`browser`).

#[cfg(all(feature = "browser", target_arch = "wasm32"))]
pub(crate) mod browser;
pub(crate) mod native;

use std::any::Any;
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::context;
use crate::runtime::Main;
use crate::waker::TaskHeader;

/// Something the event loop can wait on besides timers, such as an I/O
/// reactor. See the [module docs](self).
///
/// A runtime has at most one, installed with
/// [`Builder::io_driver`](crate::Builder::io_driver). It lives on the
/// runtime's thread and needn't be `Send`. The loop calls it with no
/// borrows held, from inside the running runtime, so wakes it performs are
/// queued directly as microtasks.
///
/// **Unstable**: this trait may change in any release before 1.0.
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use spooly::driver::IoDriver;
///
/// /// A driver with nothing to wait on: it just sleeps.
/// struct Sleepy;
///
/// impl IoDriver for Sleepy {
///     fn poll_ready(&self) -> bool {
///         false
///     }
///     fn wait(&self, timeout: Option<Duration>) -> bool {
///         // A real driver would block on its reactor here, and return early
///         // when the unparker is called.
///         std::thread::sleep(timeout.unwrap_or(Duration::from_millis(1)));
///         false
///     }
///     fn unparker(&self) -> Arc<dyn Fn() + Send + Sync> {
///         Arc::new(|| {})
///     }
///     fn is_active(&self) -> bool {
///         false
///     }
/// }
///
/// let rt = spooly::Runtime::builder().io_driver(Sleepy).build();
/// rt.block_on(async {
///     spooly::sleep(Duration::from_millis(5)).await;
///     assert!(spooly::driver::current_io_driver::<Sleepy>().is_some());
/// });
/// ```
pub trait IoDriver: Any {
    /// The I/O phase: wakes every task whose I/O is ready, without
    /// blocking. That includes readiness that [`wait`](Self::wait) saw and
    /// recorded. Returns `true` if it woke anything.
    ///
    /// Called once per step of the loop. It should be cheap when nothing is
    /// registered (skip the system call).
    fn poll_ready(&self) -> bool;

    /// Blocks until some I/O is ready, `timeout` passes (`None`: no
    /// timeout), or the [unparker](Self::unparker) is called. Returns
    /// `true` if I/O became ready.
    ///
    /// It must not wake tasks: it records what is ready, and the next
    /// [`poll_ready`](Self::poll_ready) wakes them. It may return early
    /// (spuriously); the loop just steps again. A call to the unparker made
    /// *before* `wait` must still make it return promptly, so that a wake
    /// isn't lost.
    ///
    /// `wait(Some(Duration::ZERO))` must not block: with a clock that can
    /// jump ([`MockClock`](crate::MockClock)), the loop calls it first, and
    /// only lets the clock jump to the next timer if it returns `false`.
    ///
    /// Only drivers that may block call this: the native one does, the
    /// browser one never does.
    fn wait(&self, timeout: Option<Duration>) -> bool;

    /// A function that any thread can call to end a [`wait`](Self::wait)
    /// early. The runtime calls it for wakes from other threads, and when
    /// its last [`KeepAlive`](crate::KeepAlive) is dropped.
    ///
    /// Called once, when the runtime is built. It may be called after the
    /// runtime is dropped (it must then do nothing harmful).
    fn unparker(&self) -> Arc<dyn Fn() + Send + Sync>;

    /// `true` while some task is waiting on this driver: something it can
    /// wake. An active driver keeps [`run`](crate::run) alive, as an armed
    /// timer does, and keeps a pending main from being reported as stalled.
    fn is_active(&self) -> bool;
}

/// The current runtime's I/O driver, if it has one of type `D`.
///
/// This is how I/O types find the driver they register with. `None` if the
/// runtime has no I/O driver, or one of another type.
///
/// **Unstable**: this function may change in any release before 1.0.
///
/// # Panics
///
/// If called outside a Spooly runtime.
pub fn current_io_driver<D: IoDriver>() -> Option<Rc<D>> {
    let io: Rc<dyn Any> = context::current("current_io_driver").io_driver()?;
    io.downcast::<D>().ok()
}

/// A driver's main future, resumable across steps. `P` is how the future is
/// held (as a `Pin<P>`): `&mut F` on the native driver's stack (so
/// `block_on` accepts futures that aren't `'static`), or an owned
/// `Box<dyn Future>` in the browser driver.
pub(crate) struct MainTask<P: DerefMut<Target: Future>> {
    future: Pin<P>,
    header: Arc<TaskHeader>,
    output: Option<<P::Target as Future>::Output>,
    /// Stop the step as soon as main completes (`block_on`).
    stop_when_done: bool,
}

impl<P: DerefMut<Target: Future>> MainTask<P> {
    /// `header` comes from [`Inner::main_header`](crate::runtime::Inner::main_header).
    pub(crate) fn new(future: Pin<P>, header: Arc<TaskHeader>, stop_when_done: bool) -> Self {
        MainTask {
            future,
            header,
            output: None,
            stop_when_done,
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        self.output.is_some()
    }

    pub(crate) fn take_output(&mut self) -> <P::Target as Future>::Output {
        self.output.take().expect("main future completed")
    }
}

impl<P: DerefMut<Target: Future>> Main for MainTask<P> {
    fn poll(&mut self) -> bool {
        if self.output.is_some() {
            return false; // A stale wake after completion.
        }
        self.header.clear_scheduled();
        let waker = Waker::from(Arc::clone(&self.header));
        match self.future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(output) => {
                self.output = Some(output);
                self.stop_when_done
            }
            Poll::Pending => false,
        }
    }

    fn pending_header(&self) -> Option<&Arc<TaskHeader>> {
        self.output.is_none().then_some(&self.header)
    }
}
