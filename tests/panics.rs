//! The panic model (§6 of the plan): Spooly doesn't catch panics. Every kind
//! of panic unwinds straight out of `run` / `block_on` and stops the loop at
//! once, and the runtime can still be dropped cleanly afterwards.
//!
//! These tests need `panic = "unwind"`, so they are native-only.

mod common;

use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::time::Duration;

use common::{DropCounter, Trace, expect_panic, never};

// ── Main and callbacks ──────────────────────────────────────────────────────

#[test]
fn panic_in_main_unwinds_out_of_block_on() {
    let msg = expect_panic(|| spooly::block_on(async { panic!("main boom") }));
    assert_eq!(msg, "main boom");
}

#[test]
fn panic_in_main_unwinds_out_of_run() {
    let msg = expect_panic(|| {
        spooly::run(async {
            spooly::yield_now().await;
            panic!("main boom")
        })
    });
    assert_eq!(msg, "main boom");
}

#[test]
fn panics_in_callbacks_unwind_out_at_once() {
    let msg = expect_panic(|| spooly::run(async { spooly::queue_microtask(|| panic!("micro")) }));
    assert_eq!(msg, "micro");
    let msg = expect_panic(|| {
        spooly::run(async {
            spooly::set_timeout(Duration::ZERO, || panic!("timer"));
        })
    });
    assert_eq!(msg, "timer");
    let msg = expect_panic(|| {
        spooly::run(async {
            spooly::set_interval(Duration::from_millis(1), |_| panic!("interval"));
        })
    });
    assert_eq!(msg, "interval");
}

// ── Tasks ───────────────────────────────────────────────────────────────────

#[test]
fn panic_in_a_detached_task_unwinds_out() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            drop(spooly::spawn(async { panic!("task boom") }));
            spooly::yield_now().await;
            spooly::yield_now().await;
        })
    });
    assert_eq!(msg, "task boom");
}

#[test]
fn panic_in_an_awaited_task_unwinds_out_not_into_the_awaiter() {
    let trace = Trace::new();
    let t = trace.clone();
    let msg = expect_panic(move || {
        spooly::block_on(async move {
            let h = spooly::spawn(async {
                spooly::yield_now().await;
                panic!("later boom");
            });
            h.await;
            t.log("main resumed (must not happen)");
        })
    });
    assert_eq!(msg, "later boom");
    trace.assert(&[]);
}

#[test]
fn try_join_does_not_catch_a_panic() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            spooly::spawn(async { panic!("not caught") })
                .try_join()
                .await
        })
    });
    assert_eq!(msg, "not caught");
}

#[test]
fn the_payload_is_preserved() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        spooly::block_on(async { spooly::spawn(async { panic_any(42_i32) }).await })
    }));
    assert_eq!(*result.unwrap_err().downcast::<i32>().unwrap(), 42);
}

#[test]
fn a_panic_stops_the_loop_at_once() {
    let trace = Trace::new();
    let t = trace.clone();
    let msg = expect_panic(move || {
        spooly::run(async move {
            let (t1, t2, t3) = (t.clone(), t.clone(), t.clone());
            drop(spooly::spawn(async move {
                t1.log("bad runs");
                panic!("stop");
            }));
            drop(spooly::spawn(async move {
                t2.log("queued after (must not run)")
            }));
            spooly::set_timeout(Duration::ZERO, move || t3.log("timer (must not run)"));
            t.log("main");
        })
    });
    assert_eq!(msg, "stop");
    trace.assert(&["main", "bad runs"]);
}

#[test]
fn panic_in_a_task_after_a_sleep() {
    let clock = spooly::MockClock::new();
    let rt = spooly::Runtime::builder().clock(clock.clone()).build();
    let msg = expect_panic(|| {
        rt.run(async {
            drop(spooly::spawn(async {
                spooly::sleep(Duration::from_secs(1)).await;
                panic!("after sleep");
            }));
        })
    });
    assert_eq!(msg, "after sleep");
    assert_eq!(clock.elapsed(), Duration::from_secs(1));
}

#[test]
fn the_aborted_panic_unwinds_out_like_any_other() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            let h = spooly::spawn(never());
            h.abort();
            h.await
        })
    });
    assert!(msg.contains("task was aborted"), "{msg}");
}

// ── After a panic ───────────────────────────────────────────────────────────

#[test]
fn a_panicking_tasks_future_is_dropped_once() {
    let counter = DropCounter::new();
    let rt = spooly::Runtime::new();
    let tok = counter.token();
    let _ = expect_panic(|| {
        rt.block_on(async move {
            drop(spooly::spawn(async move {
                let _tok = tok;
                spooly::yield_now().await;
                panic!("drop me");
            }));
            never().await;
        })
    });
    assert_eq!(counter.count(), 1, "dropped while unwinding");
    drop(rt);
    assert_eq!(counter.count(), 1);
}

#[test]
fn a_panicking_tasks_handle_resolves_as_aborted() {
    let rt = spooly::Runtime::new();
    let h = rt.spawn(async { panic!("p") });
    let _ = expect_panic(|| rt.block_on(async { spooly::yield_now().await }));
    assert!(h.is_finished());
}

#[test]
fn the_runtime_still_drops_its_tasks_and_timers() {
    let counter = DropCounter::new();
    let rt = spooly::Runtime::new();
    for _ in 0..3 {
        let tok = counter.token();
        drop(rt.spawn(async move {
            let _tok = tok;
            never().await;
        }));
    }
    let _ = expect_panic(|| {
        rt.block_on(async {
            let tok = counter.token();
            spooly::set_timeout(Duration::from_secs(60), move || drop(tok));
            spooly::yield_now().await;
            panic!("boom");
        })
    });
    assert_eq!(counter.count(), 0);
    drop(rt);
    assert_eq!(counter.count(), 4);
}

#[test]
fn a_panic_in_a_timer_phase_still_drops_cleanly() {
    let counter = DropCounter::new();
    let clock = spooly::MockClock::new();
    let rt = spooly::Runtime::builder().clock(clock.clone()).build();
    let (c, clk) = (counter.clone(), clock.clone());
    let _ = expect_panic(|| {
        rt.run(async move {
            spooly::set_timeout(Duration::ZERO, move || {
                // Armed in the past during the phase: set aside for the next.
                clk.advance(Duration::from_millis(5));
                let tok = c.token();
                spooly::set_timeout(Duration::ZERO, move || drop(tok));
                panic!("mid-phase");
            });
        })
    });
    drop(rt);
    assert_eq!(counter.count(), 1);
}

#[test]
fn a_fresh_runtime_works_after_a_panic() {
    let _ = expect_panic(|| {
        spooly::block_on(async {
            drop(spooly::spawn(async { panic!("x") }));
            spooly::yield_now().await;
        })
    });
    assert_eq!(
        spooly::block_on(async { spooly::spawn(async { 1 }).await }),
        1
    );
}

#[test]
fn a_panic_leaves_no_runtime_entered() {
    let _ = expect_panic(|| spooly::block_on(async { panic!("x") }));
    let msg = expect_panic(|| spooly::spawn(async {}));
    assert!(msg.contains("outside of a Spooly runtime"), "{msg}");
}

#[test]
fn a_nested_block_on_panic_leaves_the_inner_runtime_usable() {
    let other = spooly::Runtime::new();
    let msg = expect_panic(|| spooly::block_on(async { other.block_on(async {}) }));
    assert!(msg.contains("nested"), "{msg}");
    assert_eq!(other.block_on(async { 5 }), 5);
}
