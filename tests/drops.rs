//! Everything is dropped exactly once, and nothing leaks.

mod common;

use std::rc::{Rc, Weak};

use common::{DropCounter, never};

#[test]
fn completed_task_future_is_dropped_before_join_resumes() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spool::block_on(async move {
        let tok = c.token();
        spool::spawn(async move {
            let _tok = tok;
        })
        .await;
        assert_eq!(c.count(), 1);
    });
    assert_eq!(counter.count(), 1);
}

#[test]
fn pending_tasks_are_dropped_with_the_runtime() {
    let counter = DropCounter::new();
    let rt = spool::Runtime::new();
    for _ in 0..10 {
        let tok = counter.token();
        drop(rt.spawn(async move {
            let _tok = tok;
            never().await;
        }));
    }
    rt.block_on(spool::yield_now());
    assert_eq!(counter.count(), 0);
    drop(rt);
    assert_eq!(counter.count(), 10);
}

#[test]
fn never_polled_tasks_are_dropped_with_the_runtime() {
    let counter = DropCounter::new();
    let rt = spool::Runtime::new();
    let tok = counter.token();
    drop(rt.spawn(async move {
        let _tok = tok;
    }));
    drop(rt);
    assert_eq!(counter.count(), 1);
}

#[test]
fn block_on_free_function_drops_leftover_tasks() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spool::block_on(async move {
        let tok = c.token();
        drop(spool::spawn(async move {
            let _tok = tok;
            never().await;
        }));
        spool::yield_now().await;
    });
    assert_eq!(counter.count(), 1);
}

#[test]
fn no_rc_cycle_between_handle_and_task() {
    let weak: Weak<()> = spool::block_on(async {
        let rc = Rc::new(());
        let weak = Rc::downgrade(&rc);
        let h = spool::spawn(async move { rc });
        let out = h.await;
        drop(out);
        weak
    });
    assert!(weak.upgrade().is_none());
}

#[test]
fn main_future_is_dropped_when_block_on_returns() {
    let counter = DropCounter::new();
    let tok = counter.token();
    spool::block_on(async move {
        let _tok = tok;
        spool::yield_now().await;
    });
    assert_eq!(counter.count(), 1);
}

#[test]
fn main_future_is_dropped_on_panic() {
    let counter = DropCounter::new();
    let tok = counter.token();
    let _ = common::expect_panic(move || {
        spool::block_on(async move {
            let _tok = tok;
            spool::yield_now().await;
            panic!("boom");
        })
    });
    assert_eq!(counter.count(), 1);
}
