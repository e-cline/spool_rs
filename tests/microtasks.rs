//! `queue_microtask`: API behavior and edge cases.

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test as test;

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use common::{DropCounter, OnDrop, Trace, expect_panic};

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn queue_microtask_outside_a_runtime_panics() {
    let msg = expect_panic(|| spooly::queue_microtask(|| {}));
    assert!(msg.contains("`queue_microtask` called outside"), "{msg}");
}

#[test]
fn callbacks_can_capture_non_send_state() {
    let hits = Rc::new(Cell::new(0));
    let h = hits.clone();
    spooly::run(async move {
        let (a, b) = (h.clone(), h.clone());
        spooly::queue_microtask(move || a.set(a.get() + 1));
        spooly::queue_microtask(move || b.set(b.get() + 10));
    });
    assert_eq!(hits.get(), 11);
}

#[test]
fn microtasks_and_task_polls_share_one_fifo() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::run(async move {
        for i in 0..3 {
            let (t1, t2) = (t.clone(), t.clone());
            spooly::queue_microtask(move || t1.log(format!("cb{i}")));
            drop(spooly::spawn(async move { t2.log(format!("task{i}")) }));
        }
    });
    trace.assert(&["cb0", "task0", "cb1", "task1", "cb2", "task2"]);
}

#[test]
fn microtasks_queued_while_draining_run_before_the_next_timer() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::run(async move {
        let (t1, tt) = (t.clone(), t.clone());
        spooly::set_timeout(Duration::ZERO, move || tt.log("timeout"));
        spooly::queue_microtask(move || {
            t1.log("m1");
            let t2 = t1.clone();
            spooly::queue_microtask(move || {
                t2.log("m2 (queued by m1)");
                let t3 = t2.clone();
                spooly::queue_microtask(move || t3.log("m3 (queued by m2)"));
            });
        });
    });
    trace.assert(&["m1", "m2 (queued by m1)", "m3 (queued by m2)", "timeout"]);
}

#[test]
fn long_microtask_chain_does_not_grow_the_stack() {
    fn chain(counter: Rc<Cell<u32>>, n: u32) {
        if n == 0 {
            return;
        }
        spooly::queue_microtask(move || {
            counter.set(counter.get() + 1);
            chain(counter, n - 1);
        });
    }
    let counter = Rc::new(Cell::new(0));
    let c = counter.clone();
    spooly::run(async move { chain(c, 200_000) });
    assert_eq!(counter.get(), 200_000);
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn callback_panic_unwinds_out_of_run() {
    let msg = expect_panic(|| spooly::run(async { spooly::queue_microtask(|| panic!("cb boom")) }));
    assert_eq!(msg, "cb boom");
}

#[test]
fn unrun_callbacks_are_dropped_with_the_runtime() {
    let counter = DropCounter::new();
    let rt = spooly::Runtime::new();
    let (a, b) = (counter.token(), counter.token());
    // `block_on` returns as soon as main completes, before the callbacks run.
    rt.block_on(async move {
        spooly::queue_microtask(move || drop(a));
        spooly::queue_microtask(move || drop(b));
    });
    assert_eq!(counter.count(), 0);
    drop(rt);
    assert_eq!(counter.count(), 2);
}

#[test]
fn dropping_a_callback_during_teardown_can_queue_more_callbacks() {
    let counter = DropCounter::new();
    let rt = spooly::Runtime::new();
    let c = counter.clone();
    rt.block_on(async move {
        let guard = OnDrop(move || {
            // Queue more work from inside a drop during teardown. It must be
            // dropped too, without running and without a `BorrowMutError`.
            for _ in 0..2 {
                let tok = c.token();
                spooly::queue_microtask(move || {
                    let _tok = tok;
                    panic!("must not run");
                });
            }
        });
        spooly::queue_microtask(move || drop(guard));
    });
    drop(rt);
    assert_eq!(counter.count(), 2);
}

#[test]
fn callbacks_run_when_main_waits_on_them() {
    let out = spooly::block_on(async {
        let gate = common::Gate::new();
        let g = gate.clone();
        spooly::queue_microtask(move || g.open());
        gate.wait().await;
        "opened by a microtask"
    });
    assert_eq!(out, "opened by a microtask");
}

#[test]
fn block_on_returns_without_running_callbacks_when_main_is_done() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let t1 = t.clone();
        spooly::queue_microtask(move || t1.log("microtask"));
        t.log("main");
    });
    trace.assert(&["main"]);
}

#[test]
fn a_microtask_queued_by_a_task_runs_in_fifo_order() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::run(async move {
        let t1 = t.clone();
        drop(spooly::spawn(async move {
            t1.log("task 1");
            let t2 = t1.clone();
            spooly::queue_microtask(move || t2.log("microtask from task"));
            spooly::yield_now().await;
            t1.log("task 2");
        }));
        let t3 = t.clone();
        spooly::queue_microtask(move || t3.log("microtask"));
    });
    // The task's resumption is queued after the microtask it queued.
    trace.assert(&["task 1", "microtask", "microtask from task", "task 2"]);
}
