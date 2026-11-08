//! `run`: Node-like "until the loop is empty" semantics (plan §3.5).

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test as test;

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use common::{DropCounter, Gate, Trace, expect_panic, never, oneshot};

#[test]
fn returns_mains_output() {
    assert_eq!(spooly::run(async { "done" }), "done");
}

#[test]
fn main_can_borrow_from_the_caller() {
    let mut v = vec![1];
    spooly::run(async {
        spooly::yield_now().await;
        v.push(2);
    });
    assert_eq!(v, [1, 2]);
}

#[test]
fn waits_for_detached_tasks() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::run(async move {
        drop(spooly::spawn(async move {
            for i in 0..3 {
                t.log(format!("step {i}"));
                spooly::yield_now().await;
            }
        }));
    });
    trace.assert(&["step 0", "step 1", "step 2"]);
}

#[test]
fn waits_for_work_queued_after_main_completes() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::run(async move {
        let t1 = t.clone();
        spooly::queue_microtask(move || {
            t1.log("microtask");
            let t2 = t1.clone();
            spooly::set_timeout(std::time::Duration::ZERO, move || {
                t2.log("timeout");
                let t3 = t2.clone();
                drop(spooly::spawn(async move { t3.log("task") }));
            });
        });
        t.log("main done");
    });
    trace.assert(&["main done", "microtask", "timeout", "task"]);
}

#[test]
fn pending_detached_tasks_do_not_keep_the_loop_alive() {
    // As in Node, an unresolved promise doesn't keep the process alive.
    let counter = DropCounter::new();
    let tok = counter.token();
    let started = Rc::new(Cell::new(false));
    let s = started.clone();
    spooly::run(async move {
        drop(spooly::spawn(async move {
            let _tok = tok;
            s.set(true);
            never().await;
        }));
    });
    assert!(started.get());
    assert_eq!(
        counter.count(),
        1,
        "the pending task was dropped with the runtime"
    );
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn remote_waits_of_detached_tasks_do_not_keep_the_loop_alive() {
    // Documented v1 behavior: only main's outstanding wakers keep the loop
    // waiting. (`KeepAlive` guards are future work.)
    let (tx, rx) = oneshot::<()>();
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        tx.send(());
    });
    let finished = Rc::new(Cell::new(false));
    let f = finished.clone();
    spooly::run(async move {
        drop(spooly::spawn(async move {
            rx.await;
            f.set(true);
        }));
    });
    assert!(!finished.get());
    worker.join().unwrap();
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn main_waiting_on_another_thread_keeps_the_loop_alive() {
    let (tx, rx) = oneshot();
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send(5);
    });
    assert_eq!(spooly::run(rx), 5);
    worker.join().unwrap();
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn stalled_main_panics() {
    let msg = expect_panic(|| spooly::run(never()));
    assert!(msg.contains("main future stalled"), "{msg}");
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn nested_run_panics() {
    let msg = expect_panic(|| spooly::run(async { spooly::run(async {}) }));
    assert!(msg.contains("nested `block_on`"), "{msg}");
}

#[test]
fn runtime_run_can_be_called_repeatedly() {
    let rt = spooly::Runtime::new();
    let total = Rc::new(Cell::new(0));
    for i in 1..=3 {
        let t = total.clone();
        rt.run(async move {
            drop(spooly::spawn(async move { t.set(t.get() + i) }));
        });
    }
    assert_eq!(total.get(), 6);
}

#[test]
fn run_flushes_work_left_by_an_earlier_block_on() {
    let rt = spooly::Runtime::new();
    let trace = Trace::new();
    let t = trace.clone();
    rt.block_on(async move {
        let t1 = t.clone();
        spooly::queue_microtask(move || t1.log("leftover"));
    });
    trace.assert(&[]);
    let t = trace.clone();
    rt.run(async move { t.log("main") });
    // Leftover work runs before the new main's first poll.
    trace.assert(&["leftover", "main"]);
}

#[test]
fn main_first_poll_runs_before_its_own_queued_work() {
    // The first poll is a macrotask, like a program's top-level code (D4a).
    let trace = Trace::new();
    let t = trace.clone();
    spooly::run(async move {
        let t1 = t.clone();
        spooly::queue_microtask(move || t1.log("microtask"));
        t.log("script 1");
        let t2 = t.clone();
        drop(spooly::spawn(async move { t2.log("task") }));
        t.log("script 2");
    });
    trace.assert(&["script 1", "script 2", "microtask", "task"]);
}

#[test]
fn main_waiting_on_a_gate_opened_after_other_work() {
    let out = spooly::run(async {
        let gate = Gate::new();
        let g = gate.clone();
        drop(spooly::spawn(async move {
            for _ in 0..5 {
                spooly::yield_now().await;
            }
            g.open();
        }));
        gate.wait().await;
        1
    });
    assert_eq!(out, 1);
}
