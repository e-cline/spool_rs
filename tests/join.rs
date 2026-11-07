//! `JoinHandle` behavior.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use common::{DropCounter, Gate, expect_panic};

#[test]
fn await_returns_output() {
    let out = spool::block_on(async { spool::spawn(async { "hi".to_string() }).await });
    assert_eq!(out, "hi");
}

#[test]
fn output_can_be_non_send() {
    let out = spool::block_on(async { spool::spawn(async { Rc::new(9) }).await });
    assert_eq!(*out, 9);
}

#[test]
fn is_finished_tracks_completion() {
    spool::block_on(async {
        let gate = Gate::new();
        let g = gate.clone();
        let h = spool::spawn(async move { g.wait().await });
        assert!(!h.is_finished());
        spool::yield_now().await;
        assert!(!h.is_finished(), "still waiting on the gate");
        gate.open();
        spool::yield_now().await;
        assert!(h.is_finished());
        h.await;
    });
}

#[test]
fn handle_awaited_after_the_task_finished() {
    spool::block_on(async {
        let h = spool::spawn(async { 3 });
        for _ in 0..3 {
            spool::yield_now().await;
        }
        assert!(h.is_finished());
        assert_eq!(h.await, 3);
    });
}

#[test]
fn polling_after_completion_panics() {
    let msg = expect_panic(|| {
        spool::block_on(async {
            let mut h = spool::spawn(async { 1 });
            assert_eq!((&mut h).await, 1);
            (&mut h).await
        })
    });
    assert!(msg.contains("polled after it completed"), "{msg}");
}

#[test]
fn dropping_the_handle_detaches_the_task() {
    let rt = spool::Runtime::new();
    let ran = Rc::new(std::cell::Cell::new(false));
    let r = ran.clone();
    drop(rt.spawn(async move {
        spool::yield_now().await;
        r.set(true);
    }));
    rt.block_on(async {
        for _ in 0..3 {
            spool::yield_now().await;
        }
    });
    assert!(ran.get(), "a detached task still runs to completion");
}

#[test]
fn detached_output_is_dropped_on_completion() {
    let counter = DropCounter::new();
    let c = counter.clone();
    let rt = spool::Runtime::new();
    drop(rt.spawn(async move { c.token() }));
    rt.block_on(spool::yield_now());
    assert_eq!(counter.count(), 1);
}

#[test]
fn unconsumed_output_is_dropped_with_the_handle() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spool::block_on(async move {
        let h = spool::spawn(async move { c.token() });
        spool::yield_now().await;
        assert!(h.is_finished());
        assert_eq!(counter.count(), 0, "output is held for the handle");
        drop(h);
        assert_eq!(counter.count(), 1);
    });
}

/// Polls the inner future with a different waker each time.
struct SwapWakers<F> {
    inner: Pin<Box<F>>,
    polls: usize,
}

impl<F: Future> Future for SwapWakers<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.polls += 1;
        if self.polls == 1 {
            // First poll with a no-op waker that will never wake anything.
            let noop = Waker::noop();
            let _ = self.inner.as_mut().poll(&mut Context::from_waker(noop));
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        // Later polls use the real waker, which must replace the stale one.
        self.inner.as_mut().poll(cx)
    }
}

#[test]
fn join_handle_updates_its_waker() {
    let out = spool::block_on(async {
        let gate = Gate::new();
        let g = gate.clone();
        let h = spool::spawn(async move {
            g.wait().await;
            11
        });
        let opener = spool::spawn(async move {
            spool::yield_now().await;
            spool::yield_now().await;
            gate.open();
        });
        let v = SwapWakers {
            inner: Box::pin(h),
            polls: 0,
        }
        .await;
        opener.await;
        v
    });
    assert_eq!(out, 11);
}

#[test]
fn join_handle_debug() {
    spool::block_on(async {
        let h = spool::spawn(async {});
        assert_eq!(format!("{h:?}"), "JoinHandle { finished: false }");
        spool::yield_now().await;
        assert_eq!(format!("{h:?}"), "JoinHandle { finished: true }");
    });
}

#[test]
fn awaiting_from_another_task() {
    let out = spool::block_on(async {
        let a = spool::spawn(async { 2 });
        let b = spool::spawn(async move { a.await * 10 });
        b.await
    });
    assert_eq!(out, 20);
}
