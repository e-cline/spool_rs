//! Shared helpers for integration tests.
#![allow(dead_code)] // Not every test binary uses every helper.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// An append-only event log, for checking execution order.
#[derive(Clone, Default)]
pub struct Trace(Rc<RefCell<Vec<String>>>);

impl Trace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn log(&self, event: impl Into<String>) {
        self.0.borrow_mut().push(event.into());
    }

    pub fn events(&self) -> Vec<String> {
        self.0.borrow().clone()
    }

    #[track_caller]
    pub fn assert(&self, expected: &[&str]) {
        assert_eq!(self.events(), expected, "unexpected event order");
    }
}

/// Counts how many times it has been dropped.
#[derive(Clone, Default)]
pub struct DropCounter(Rc<Cell<usize>>);

impl DropCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn token(&self) -> DropToken {
        DropToken(self.0.clone())
    }

    pub fn count(&self) -> usize {
        self.0.get()
    }
}

/// Increments its counter when dropped.
pub struct DropToken(Rc<Cell<usize>>);

impl Drop for DropToken {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

/// Runs `f` when dropped.
pub struct OnDrop<F: FnMut()>(pub F);

impl<F: FnMut()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

/// A future that never completes and never registers its waker anywhere.
pub fn never() -> std::future::Pending<()> {
    std::future::pending()
}

/// A future that stays pending until `Gate::open` is called. Keeps its waker
/// in a local slot, so it can be woken from the same thread.
#[derive(Clone, Default)]
pub struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&self) {
        let waker = {
            let mut s = self.0.borrow_mut();
            s.0 = true;
            s.1.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    pub fn wait(&self) -> GateWait {
        GateWait(self.clone())
    }
}

pub struct GateWait(Gate);

impl Future for GateWait {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut s = (self.0).0.borrow_mut();
        if s.0 {
            Poll::Ready(())
        } else {
            s.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// A minimal thread-safe one-shot channel, used to test wakes from other
/// threads.
pub fn oneshot<T: Send>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Mutex::new((None, None)));
    (Sender(shared.clone()), Receiver(shared))
}

type OneshotState<T> = Arc<Mutex<(Option<T>, Option<Waker>)>>;

pub struct Sender<T>(OneshotState<T>);
pub struct Receiver<T>(OneshotState<T>);

impl<T> Sender<T> {
    pub fn send(self, value: T) {
        let waker = {
            let mut s = self.0.lock().unwrap();
            s.0 = Some(value);
            s.1.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut s = self.0.lock().unwrap();
        match s.0.take() {
            Some(v) => Poll::Ready(v),
            None => {
                s.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Extracts a panic payload's message.
pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Runs `f`, asserts that it panics, and returns the panic message.
#[track_caller]
pub fn expect_panic<R>(f: impl FnOnce() -> R) -> String {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(_) => panic!("expected a panic, but the closure returned normally"),
        Err(payload) => panic_message(&*payload),
    }
}
