//! Everything is dropped exactly once, and nothing leaks.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

mod common;

use std::rc::{Rc, Weak};

use common::{DropCounter, never};

#[test]
fn completed_task_future_is_dropped_before_join_resumes() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spooly::block_on(async move {
        let tok = c.token();
        spooly::spawn(async move {
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
    let rt = spooly::Runtime::new();
    for _ in 0..10 {
        let tok = counter.token();
        drop(rt.spawn(async move {
            let _tok = tok;
            never().await;
        }));
    }
    rt.block_on(spooly::yield_now());
    assert_eq!(counter.count(), 0);
    drop(rt);
    assert_eq!(counter.count(), 10);
}

#[test]
fn never_polled_tasks_are_dropped_with_the_runtime() {
    let counter = DropCounter::new();
    let rt = spooly::Runtime::new();
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
    spooly::block_on(async move {
        let tok = c.token();
        drop(spooly::spawn(async move {
            let _tok = tok;
            never().await;
        }));
        spooly::yield_now().await;
    });
    assert_eq!(counter.count(), 1);
}

#[test]
fn no_rc_cycle_between_handle_and_task() {
    let weak: Weak<()> = spooly::block_on(async {
        let rc = Rc::new(());
        let weak = Rc::downgrade(&rc);
        let h = spooly::spawn(async move { rc });
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
    spooly::block_on(async move {
        let _tok = tok;
        spooly::yield_now().await;
    });
    assert_eq!(counter.count(), 1);
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn main_future_is_dropped_on_panic() {
    let counter = DropCounter::new();
    let tok = counter.token();
    let _ = common::expect_panic(move || {
        spooly::block_on(async move {
            let _tok = tok;
            spooly::yield_now().await;
            panic!("boom");
        })
    });
    assert_eq!(counter.count(), 1);
}

// ── Abort, panic, and teardown (M4) ─────────────────────────────────────────

#[test]
fn aborted_task_is_dropped_once_and_leaves_no_cycle() {
    let counter = DropCounter::new();
    let rc = Rc::new(());
    let weak = Rc::downgrade(&rc);
    let c = counter.clone();
    spooly::block_on(async move {
        let tok = c.token();
        let h = spooly::spawn(async move {
            let _tok = tok;
            let _rc = rc;
            never().await;
        });
        spooly::yield_now().await;
        h.abort();
        h.abort();
        let _ = h.try_join().await;
    });
    assert_eq!(counter.count(), 1);
    assert!(weak.upgrade().is_none());
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn panicked_task_leaves_no_cycle() {
    let rc = Rc::new(());
    let weak = Rc::downgrade(&rc);
    let rt = spooly::Runtime::new();
    let handle = rt.spawn(async move {
        let _rc = rc;
        spooly::yield_now().await;
        panic!("cycle check");
    });
    let _ = common::expect_panic(|| rt.block_on(spooly::yield_now()));
    assert!(weak.upgrade().is_none(), "dropped while unwinding");
    drop((handle, rt));
    assert!(weak.upgrade().is_none());
}

#[test]
fn detached_output_is_dropped_once_when_the_task_finishes() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spooly::block_on(async move {
        let tok = c.token();
        drop(spooly::spawn(async move { tok }));
        assert_eq!(c.count(), 0);
        spooly::yield_now().await;
        assert_eq!(c.count(), 1, "nobody wants the output");
    });
    assert_eq!(counter.count(), 1);
}

#[test]
fn held_output_is_dropped_with_the_handle() {
    let counter = DropCounter::new();
    let c = counter.clone();
    let rt = spooly::Runtime::new();
    #[allow(clippy::async_yields_async)] // Returning the handle is the point.
    let h = rt.block_on(async move {
        let h = spooly::spawn(async move { c.token() });
        spooly::yield_now().await;
        h
    });
    assert_eq!(counter.count(), 0);
    drop(h);
    assert_eq!(counter.count(), 1);
}

#[test]
fn abort_on_drop_during_teardown_is_harmless() {
    // A task owns an AbortOnDrop for another task; teardown drops both.
    let counter = DropCounter::new();
    let rt = spooly::Runtime::new();
    let (t1, t2) = (counter.token(), counter.token());
    let inner = rt.spawn(async move {
        let _t = t2;
        never().await;
    });
    drop(rt.spawn(async move {
        let _t = t1;
        let _guard = inner.abort_on_drop();
        never().await;
    }));
    rt.block_on(spooly::yield_now());
    drop(rt);
    assert_eq!(counter.count(), 2);
}

#[test]
fn drop_during_teardown_can_spawn_and_abort() {
    let counter = DropCounter::new();
    let rt = spooly::Runtime::new();
    let c = counter.clone();
    let victim = rt.spawn(async move {
        let _t = c.token();
        never().await;
    });
    let abort = victim.abort_handle();
    let c = counter.clone();
    drop(rt.spawn(async move {
        let _on_drop = common::OnDrop(move || {
            abort.abort();
            let tok = c.token();
            drop(spooly::spawn(async move {
                let _tok = tok;
            }));
        });
        never().await;
    }));
    rt.block_on(spooly::yield_now());
    drop(rt);
    assert_eq!(
        counter.count(),
        2,
        "the victim and the task spawned from drop"
    );
    assert!(victim.is_finished());
}
