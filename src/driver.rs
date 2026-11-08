//! Drivers: the per-target code that calls the event loop's
//! [`step`](crate::runtime::Inner::step) and waits between steps (D5, §7).
//!
//! The step never blocks; a driver decides how to wait. Natively, that is
//! parking the thread ([`native`]). The browser driver (M9) will run steps
//! from host callbacks instead.

#[cfg(all(feature = "browser", target_arch = "wasm32"))]
pub(crate) mod browser;
pub(crate) mod native;

use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::runtime::Main;
use crate::waker::TaskHeader;

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
