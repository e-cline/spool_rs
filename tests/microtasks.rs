//! `queue_microtask` and `next_tick`: API behavior and edge cases.
//! (Ordering against Node lives in tests/node_parity.rs.)

mod common;

use std::cell::Cell;
use std::rc::Rc;

use common::{DropCounter, OnDrop, Trace, expect_panic};

#[test]
fn queue_microtask_outside_a_runtime_panics() {
    let msg = expect_panic(|| spool::queue_microtask(|| {}));
    assert!(msg.contains("`queue_microtask` called outside"), "{msg}");
}

#[test]
fn next_tick_outside_a_runtime_panics() {
    let msg = expect_panic(|| spool::next_tick(|| {}));
    assert!(msg.contains("`next_tick` called outside"), "{msg}");
}

#[test]
fn callbacks_can_capture_non_send_state() {
    let hits = Rc::new(Cell::new(0));
    let h = hits.clone();
    spool::run(async move {
        let (a, b) = (h.clone(), h.clone());
        spool::queue_microtask(move || a.set(a.get() + 1));
        spool::next_tick(move || b.set(b.get() + 10));
    });
    assert_eq!(hits.get(), 11);
}

#[test]
fn microtasks_and_task_polls_share_one_fifo() {
    let trace = Trace::new();
    let t = trace.clone();
    spool::run(async move {
        for i in 0..3 {
            let (t1, t2) = (t.clone(), t.clone());
            spool::queue_microtask(move || t1.log(format!("cb{i}")));
            drop(spool::spawn(async move { t2.log(format!("task{i}")) }));
        }
    });
    trace.assert(&["cb0", "task0", "cb1", "task1", "cb2", "task2"]);
}

#[test]
fn microtask_queued_while_draining_runs_before_ticks() {
    let trace = Trace::new();
    let t = trace.clone();
    spool::run(async move {
        let t1 = t.clone();
        spool::queue_microtask(move || {
            t1.log("m1");
            let t2 = t1.clone();
            spool::queue_microtask(move || t2.log("m2 (queued by m1)"));
            let t3 = t1.clone();
            spool::next_tick(move || t3.log("tick (queued by m1)"));
        });
    });
    trace.assert(&["m1", "m2 (queued by m1)", "tick (queued by m1)"]);
}

#[test]
fn ticks_starve_microtasks_until_the_tick_queue_is_empty() {
    // Like Node: a chain of ticks keeps the microtask queue waiting.
    fn chain(t: Trace, n: u32) {
        if n == 0 {
            return;
        }
        spool::next_tick(move || {
            t.log(format!("tick {n}"));
            chain(t, n - 1);
        });
    }
    let trace = Trace::new();
    let t = trace.clone();
    spool::run(async move {
        let tm = t.clone();
        spool::queue_microtask(move || tm.log("microtask"));
        chain(t, 3);
    });
    trace.assert(&["tick 3", "tick 2", "tick 1", "microtask"]);
}

#[test]
fn long_microtask_chain_does_not_grow_the_stack() {
    fn chain(counter: Rc<Cell<u32>>, n: u32) {
        if n == 0 {
            return;
        }
        spool::queue_microtask(move || {
            counter.set(counter.get() + 1);
            chain(counter, n - 1);
        });
    }
    let counter = Rc::new(Cell::new(0));
    let c = counter.clone();
    spool::run(async move { chain(c, 200_000) });
    assert_eq!(counter.get(), 200_000);
}

#[test]
fn long_tick_chain_does_not_grow_the_stack() {
    fn chain(counter: Rc<Cell<u32>>, n: u32) {
        if n == 0 {
            return;
        }
        spool::next_tick(move || {
            counter.set(counter.get() + 1);
            chain(counter, n - 1);
        });
    }
    let counter = Rc::new(Cell::new(0));
    let c = counter.clone();
    spool::run(async move { chain(c, 200_000) });
    assert_eq!(counter.get(), 200_000);
}

#[test]
fn callback_panic_unwinds_out_of_run() {
    let msg = expect_panic(|| spool::run(async { spool::queue_microtask(|| panic!("cb boom")) }));
    assert_eq!(msg, "cb boom");
    let msg = expect_panic(|| spool::run(async { spool::next_tick(|| panic!("tick boom")) }));
    assert_eq!(msg, "tick boom");
}

#[test]
fn unrun_callbacks_are_dropped_with_the_runtime() {
    let counter = DropCounter::new();
    let rt = spool::Runtime::new();
    let (a, b) = (counter.token(), counter.token());
    // `block_on` returns as soon as main completes, before the callbacks run.
    rt.block_on(async move {
        spool::queue_microtask(move || drop(a));
        spool::next_tick(move || drop(b));
    });
    assert_eq!(counter.count(), 0);
    drop(rt);
    assert_eq!(counter.count(), 2);
}

#[test]
fn dropping_a_callback_during_teardown_can_queue_more_callbacks() {
    let counter = DropCounter::new();
    let rt = spool::Runtime::new();
    let c = counter.clone();
    rt.block_on(async move {
        let guard = OnDrop(move || {
            // Queue more work from inside a drop during teardown. It must be
            // dropped too, without running and without a `BorrowMutError`.
            let tok = c.token();
            spool::queue_microtask(move || {
                let _tok = tok;
                panic!("must not run");
            });
            let tok = c.token();
            spool::next_tick(move || {
                let _tok = tok;
                panic!("must not run");
            });
        });
        spool::queue_microtask(move || drop(guard));
    });
    drop(rt);
    assert_eq!(counter.count(), 2);
}

#[test]
fn callbacks_run_when_main_waits_on_them() {
    let out = spool::block_on(async {
        let gate = common::Gate::new();
        let g = gate.clone();
        spool::next_tick(move || g.open());
        gate.wait().await;
        "opened by a tick"
    });
    assert_eq!(out, "opened by a tick");
}

#[test]
fn block_on_returns_without_running_callbacks_when_main_is_done() {
    let trace = Trace::new();
    let t = trace.clone();
    spool::block_on(async move {
        let (t1, t2) = (t.clone(), t.clone());
        spool::queue_microtask(move || t1.log("microtask"));
        spool::next_tick(move || t2.log("tick"));
        t.log("main");
    });
    trace.assert(&["main"]);
}

#[test]
fn a_tick_queued_by_a_task_runs_after_the_microtask_queue_drains() {
    let trace = Trace::new();
    let t = trace.clone();
    spool::run(async move {
        let t1 = t.clone();
        drop(spool::spawn(async move {
            t1.log("task 1");
            let t2 = t1.clone();
            spool::next_tick(move || t2.log("tick from task"));
            spool::yield_now().await;
            t1.log("task 2");
        }));
        let t3 = t.clone();
        spool::queue_microtask(move || t3.log("microtask"));
    });
    // The task's resumption is a microtask, so it runs before the tick.
    trace.assert(&["task 1", "microtask", "task 2", "tick from task"]);
}
