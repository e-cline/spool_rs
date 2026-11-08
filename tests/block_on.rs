//! Entry points: `block_on`, `Runtime`, context, and stall detection.

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test as test;

mod common;

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::task::{Poll, Waker};

use common::{Gate, Trace, expect_panic, never};

#[test]
fn returns_the_output_of_main() {
    assert_eq!(spooly::block_on(async { 42 }), 42);
}

#[test]
fn main_can_borrow_from_the_caller() {
    // `block_on` doesn't require `'static`.
    let mut local = vec![1, 2, 3];
    let sum: i32 = spooly::block_on(async {
        local.push(4);
        local.iter().sum()
    });
    assert_eq!(sum, 10);
    assert_eq!(local, [1, 2, 3, 4]);
}

#[test]
fn main_can_hold_non_send_state_across_awaits() {
    let out = spooly::block_on(async {
        let rc = Rc::new(Cell::new(1));
        spooly::yield_now().await;
        rc.set(rc.get() + 1);
        spooly::yield_now().await;
        rc.get()
    });
    assert_eq!(out, 2);
}

#[test]
fn block_on_returns_as_soon_as_main_completes() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let t2 = t.clone();
        // Detached task that would run after main's next yield.
        drop(spooly::spawn(async move { t2.log("task") }));
        t.log("main done");
    });
    // The task never got to run: main completed first, and the runtime
    // (with the task) was dropped.
    trace.assert(&["main done"]);
}

#[test]
fn runtime_can_be_reused_and_keeps_pending_tasks() {
    let rt = spooly::Runtime::new();
    let trace = Trace::new();
    let gate = Gate::new();
    let (t, g) = (trace.clone(), gate.clone());
    let handle = rt.spawn(async move {
        t.log("start");
        g.wait().await;
        t.log("end");
        5
    });
    rt.block_on(spooly::yield_now());
    trace.assert(&["start"]);
    gate.open();
    assert_eq!(rt.block_on(handle), 5);
    trace.assert(&["start", "end"]);
}

#[test]
fn tasks_spawned_before_block_on_run_in_order() {
    let rt = spooly::Runtime::new();
    let trace = Trace::new();
    for i in 0..3 {
        let t = trace.clone();
        drop(rt.spawn(async move { t.log(format!("task {i}")) }));
    }
    let t = trace.clone();
    rt.block_on(async move { t.log("main") });
    // Pre-spawned tasks were queued first, so they run first.
    trace.assert(&["task 0", "task 1", "task 2", "main"]);
}

#[test]
fn sequential_runtimes_on_one_thread() {
    for i in 0..5 {
        assert_eq!(spooly::block_on(async move { i }), i);
    }
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn spawn_outside_a_runtime_panics() {
    let msg = expect_panic(|| drop(spooly::spawn(async {})));
    assert!(
        msg.contains("`spawn` called outside of a Spooly runtime"),
        "{msg}"
    );
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn nested_block_on_panics() {
    let msg = expect_panic(|| spooly::block_on(async { spooly::block_on(async {}) }));
    assert!(msg.contains("nested `block_on`"), "{msg}");
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn nested_block_on_on_another_runtime_panics() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            let other = spooly::Runtime::new();
            other.block_on(async {})
        })
    });
    assert!(msg.contains("nested `block_on`"), "{msg}");
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn context_is_restored_after_a_panic() {
    let _ = expect_panic(|| spooly::block_on(async { panic!("boom") }));
    // The thread-local runtime was cleared on unwind, so this works.
    assert_eq!(spooly::block_on(async { 1 }), 1);
    // And the free functions correctly report "no runtime" again.
    let msg = expect_panic(|| drop(spooly::spawn(async {})));
    assert!(msg.contains("outside of a Spooly runtime"), "{msg}");
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn stalled_main_panics() {
    let msg = expect_panic(|| spooly::block_on(never()));
    assert!(msg.contains("main future stalled"), "{msg}");
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn stalled_main_after_work_panics() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            spooly::yield_now().await;
            spooly::spawn(async { 1 }).await;
            never().await
        })
    });
    assert!(msg.contains("main future stalled"), "{msg}");
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn main_waiting_on_a_dropped_waker_stalls() {
    // A task holds main's waker, then drops it without waking: main can never
    // be woken again, which is a provable stall.
    let msg = expect_panic(|| {
        spooly::block_on(async {
            let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
            let s = stash.clone();
            drop(spooly::spawn(async move {
                spooly::yield_now().await;
                drop(s.borrow_mut().take()); // Drop main's waker; don't wake it.
            }));
            std::future::poll_fn(|cx| {
                *stash.borrow_mut() = Some(cx.waker().clone());
                Poll::<()>::Pending
            })
            .await
        })
    });
    assert!(msg.contains("main future stalled"), "{msg}");
}

#[test]
fn runtime_debug_output() {
    let rt = spooly::Runtime::default();
    let dbg = format!("{rt:?}");
    assert!(dbg.starts_with("Runtime {"), "{dbg}");
}
