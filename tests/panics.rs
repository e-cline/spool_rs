//! The JS-exception panic model (§6 of the plan): every row of the table,
//! plus poisoning.

// `spawn(async move { h.await })` deliberately makes a separate awaiting
// task, and some tests return a `JoinHandle` out of `block_on`.
#![allow(clippy::redundant_async_block, clippy::async_yields_async)]

mod common;

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::time::Duration;

use common::{DropCounter, Gate, Trace, expect_panic, never, panic_message};
use spool::JoinError;

const POISONED: &str = "runtime is poisoned";

// ── Row: main panics → unwinds out at once ──────────────────────────────────

#[test]
fn panic_in_main_unwinds_out_of_block_on() {
    let msg = expect_panic(|| spool::block_on(async { panic!("main boom") }));
    assert_eq!(msg, "main boom");
}

#[test]
fn panic_in_main_unwinds_out_of_run() {
    let msg = expect_panic(|| {
        spool::run(async {
            spool::yield_now().await;
            panic!("main boom")
        })
    });
    assert_eq!(msg, "main boom");
}

// ── Row: callbacks panic → unwind out at once ───────────────────────────────

#[test]
fn panics_in_callbacks_unwind_out_at_once() {
    let msg = expect_panic(|| spool::run(async { spool::queue_microtask(|| panic!("micro")) }));
    assert_eq!(msg, "micro");
    let msg = expect_panic(|| spool::run(async { spool::next_tick(|| panic!("tick")) }));
    assert_eq!(msg, "tick");
    let msg = expect_panic(|| {
        spool::run(async {
            spool::set_timeout(Duration::ZERO, || panic!("timer"));
        })
    });
    assert_eq!(msg, "timer");
    let msg = expect_panic(|| {
        spool::run(async {
            spool::set_interval(Duration::from_millis(1), |_| panic!("interval"));
        })
    });
    assert_eq!(msg, "interval");
}

#[test]
fn a_callback_panic_inside_a_task_is_not_caught_by_the_task() {
    // The callback runs on the event loop, not inside the task's poll, so
    // awaiting the task can't observe it.
    let msg = expect_panic(|| {
        spool::block_on(async {
            let h = spool::spawn(async {
                spool::queue_microtask(|| panic!("cb"));
                spool::yield_now().await;
            });
            h.try_join().await
        })
    });
    assert_eq!(msg, "cb");
}

// ── Row: awaited task panics → re-thrown in the awaiter ─────────────────────

#[test]
fn awaiting_a_panicked_task_rethrows_in_main() {
    let msg = expect_panic(|| {
        spool::block_on(async {
            spool::spawn(async {
                spool::yield_now().await;
                panic!("later boom");
            })
            .await
        })
    });
    assert_eq!(msg, "later boom");
}

#[test]
fn rethrow_preserves_the_payload() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        spool::block_on(async { spool::spawn(async { panic_any(42_i32) }).await })
    }));
    assert_eq!(*result.unwrap_err().downcast::<i32>().unwrap(), 42);
}

#[test]
fn panics_propagate_up_a_chain_of_awaits() {
    let err = spool::block_on(async {
        let c = spool::spawn(async {
            spool::yield_now().await;
            panic!("deep");
        });
        let b = spool::spawn(async move { c.await });
        let a = spool::spawn(async move { b.await });
        a.try_join().await.unwrap_err()
    });
    assert_eq!(err.to_string(), "task panicked: deep");
}

#[test]
fn a_rethrown_panic_in_a_detached_awaiter_unwinds_out() {
    let msg = expect_panic(|| {
        spool::run(async {
            let inner = spool::spawn(async { panic!("inner") });
            drop(spool::spawn(async move { inner.await }));
        })
    });
    assert_eq!(msg, "inner");
}

#[test]
fn the_rethrow_can_be_caught_like_any_panic() {
    // The re-throw is an ordinary unwind inside the awaiter's poll.
    let out = spool::block_on(async {
        let mut h = spool::spawn(async { panic!("boom") });
        spool::yield_now().await;
        let caught = catch_unwind(AssertUnwindSafe(|| {
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            std::pin::Pin::new(&mut h).poll(&mut cx)
        }));
        panic_message(&*caught.unwrap_err())
    });
    assert_eq!(out, "boom");
}

// ── Row: try_join → Err(Panicked) ───────────────────────────────────────────

#[test]
fn try_join_returns_the_panic() {
    let err = spool::block_on(async { spool::spawn(async { panic!("caught") }).try_join().await });
    let err = err.unwrap_err();
    assert!(err.is_panic());
    assert_eq!(err.to_string(), "task panicked: caught");
    let payload = err.try_into_panic().unwrap();
    assert_eq!(panic_message(&*payload), "caught");
}

#[test]
fn try_join_after_the_task_already_panicked() {
    spool::block_on(async {
        let h = spool::spawn(async { panic!("early") });
        for _ in 0..3 {
            spool::yield_now().await;
        }
        assert!(h.is_finished());
        assert!(matches!(h.try_join().await, Err(JoinError::Panicked(_))));
    });
}

#[test]
fn an_observed_panic_does_not_disturb_other_tasks() {
    let trace = Trace::new();
    let t = trace.clone();
    spool::run(async move {
        let t2 = t.clone();
        let bystander = spool::spawn(async move {
            for i in 0..3 {
                t2.log(format!("bystander {i}"));
                spool::yield_now().await;
            }
        });
        let bad = spool::spawn(async {
            spool::yield_now().await;
            panic!("bad");
        });
        let r = bad.try_join().await;
        t.log(format!("main saw {r:?}"));
        bystander.await;
    });
    // The panic wakes main like a rejection resolving: main is queued behind
    // the bystander's already-queued poll.
    trace.assert(&[
        "bystander 0",
        "bystander 1",
        "bystander 2",
        "main saw Err(Panicked(\"bad\"))",
    ]);
}

#[test]
fn an_observed_panic_does_not_poison() {
    let rt = spool::Runtime::new();
    let r = rt.block_on(async { spool::spawn(async { panic!("x") }).try_join().await });
    assert!(r.is_err());
    assert_eq!(rt.block_on(async { 1 }), 1);
}

// ── Row: detached task panics → unwinds out ─────────────────────────────────

#[test]
fn panic_in_a_detached_task_unwinds_out_of_block_on() {
    let msg = expect_panic(|| {
        spool::block_on(async {
            drop(spool::spawn(async { panic!("task boom") }));
            spool::yield_now().await;
            spool::yield_now().await;
        })
    });
    assert_eq!(msg, "task boom");
}

#[test]
fn panic_in_a_detached_task_stops_the_loop_at_once() {
    let trace = Trace::new();
    let t = trace.clone();
    let msg = expect_panic(move || {
        spool::run(async move {
            let (t1, t2) = (t.clone(), t.clone());
            drop(spool::spawn(async move {
                t1.log("bad runs");
                panic!("detached");
            }));
            drop(spool::spawn(async move {
                t2.log("queued after (must not run)")
            }));
            t.log("main");
        })
    });
    assert_eq!(msg, "detached");
    trace.assert(&["main", "bad runs"]);
}

#[test]
fn panic_in_a_detached_task_inside_a_timer() {
    let clock = spool::MockClock::new();
    let rt = spool::Runtime::builder().clock(clock.clone()).build();
    let msg = expect_panic(|| {
        rt.run(async {
            drop(spool::spawn(async {
                spool::sleep(Duration::from_secs(1)).await;
                panic!("after sleep");
            }));
        })
    });
    assert_eq!(msg, "after sleep");
    assert_eq!(clock.elapsed(), Duration::from_secs(1));
}

#[test]
fn only_the_first_unobserved_panic_is_raised() {
    let msg = expect_panic(|| {
        spool::block_on(async {
            let a = spool::spawn(async { panic!("first") });
            let b = spool::spawn(async { panic!("second") });
            spool::yield_now().await;
            spool::yield_now().await;
            // Both are held in live handles; dropping both in one turn makes
            // two unobserved panics.
            drop(a);
            drop(b);
            spool::yield_now().await;
        })
    });
    assert_eq!(msg, "first");
}

// ── Row: held in a live handle, then dropped unobserved ─────────────────────

#[test]
fn a_held_panic_waits_for_the_handle() {
    let trace = Trace::new();
    let t = trace.clone();
    let msg = expect_panic(move || {
        spool::run(async move {
            let h = spool::spawn(async { panic!("held") });
            for i in 0..3 {
                spool::yield_now().await;
                t.log(format!("turn {i}"));
            }
            assert!(h.is_finished());
            drop(h); // Unobserved: re-raised at the next safe point.
            t.log("same turn");
            spool::yield_now().await;
            t.log("next turn (must not run)");
        })
    });
    assert_eq!(msg, "held");
    trace.assert(&["turn 0", "turn 1", "turn 2", "same turn"]);
}

#[test]
fn a_held_panic_dropped_as_main_completes_still_unwinds() {
    let msg = expect_panic(|| {
        spool::block_on(async {
            let h = spool::spawn(async { panic!("last moment") });
            spool::yield_now().await;
            drop(h);
            "main finished"
        })
    });
    assert_eq!(msg, "last moment");
}

#[test]
fn a_held_panic_dropped_during_mains_first_poll() {
    let rt = spool::Runtime::new();
    let h = rt.spawn(async { panic!("pre-spawned") });
    let msg = expect_panic(|| {
        rt.block_on(async move {
            // The pre-drain already ran the task.
            assert!(h.is_finished());
            drop(h);
        })
    });
    assert_eq!(msg, "pre-spawned");
}

#[test]
fn a_held_panic_dropped_between_calls_is_raised_by_the_next_call() {
    let rt = spool::Runtime::new();
    let h = rt.block_on(async {
        let h = spool::spawn(async { panic!("between") });
        spool::yield_now().await;
        h
    });
    drop(h); // Outside any `block_on`.
    let msg = expect_panic(|| rt.block_on(async {}));
    assert_eq!(msg, "between");
}

#[test]
fn a_held_panic_is_dropped_silently_with_the_runtime() {
    let rt = spool::Runtime::new();
    let h = rt.block_on(async {
        let h = spool::spawn(async { panic!("silenced") });
        spool::yield_now().await;
        h
    });
    drop(rt);
    drop(h); // No runtime left to report to: no panic.
}

#[test]
fn an_unobserved_panic_is_dropped_silently_with_the_runtime() {
    let rt = spool::Runtime::new();
    let h = rt.block_on(async {
        let h = spool::spawn(async { panic!("silenced") });
        spool::yield_now().await;
        h
    });
    drop(h); // Reported, but never raised...
    drop(rt); // ...and dropped here without panicking.
}

#[test]
fn a_held_panic_can_still_be_awaited_later() {
    let rt = spool::Runtime::new();
    let h = rt.block_on(async {
        let h = spool::spawn(async { panic!("later") });
        spool::yield_now().await;
        h
    });
    let err = rt.block_on(h.try_join()).unwrap_err();
    assert_eq!(err.to_string(), "task panicked: later");
}

// ── What a panicking task leaves behind ─────────────────────────────────────

#[test]
fn a_panicking_tasks_future_is_dropped_once() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spool::block_on(async move {
        let tok = c.token();
        let h = spool::spawn(async move {
            let _tok = tok;
            spool::yield_now().await;
            panic!("drop me");
        });
        assert!(h.try_join().await.is_err());
        assert_eq!(c.count(), 1);
    });
    assert_eq!(counter.count(), 1);
}

#[test]
fn a_panicking_task_is_finished() {
    spool::block_on(async {
        let gate = Gate::new();
        let g = gate.clone();
        let h = spool::spawn(async move {
            g.wait().await;
            panic!("p");
        });
        let a = h.abort_handle();
        spool::yield_now().await;
        assert!(!h.is_finished() && !a.is_finished());
        gate.open();
        spool::yield_now().await;
        assert!(h.is_finished() && a.is_finished());
        a.abort(); // No-op: it already finished.
        assert!(h.try_join().await.unwrap_err().is_panic());
    });
}

#[test]
fn a_panicking_tasks_wakers_go_stale() {
    let rt = spool::Runtime::new();
    let stash: std::rc::Rc<std::cell::RefCell<Option<std::task::Waker>>> = Default::default();
    let s = stash.clone();
    let h = rt.spawn(std::future::poll_fn(move |cx| -> std::task::Poll<()> {
        *s.borrow_mut() = Some(cx.waker().clone());
        panic!("stale");
    }));
    let r = rt.block_on(h.try_join());
    assert!(r.is_err());
    stash.borrow_mut().take().unwrap().wake(); // Ignored.
    assert_eq!(rt.block_on(async { 3 }), 3);
}

#[test]
fn the_abort_panic_when_awaiting_does_not_poison_if_caught() {
    let rt = spool::Runtime::new();
    let err = rt.block_on(async {
        let target = spool::spawn(never());
        target.abort();
        spool::spawn(async move { target.await })
            .try_join()
            .await
            .unwrap_err()
    });
    assert!(err.to_string().contains("task was aborted"), "{err}");
    assert_eq!(rt.block_on(async { 2 }), 2);
}

// ── Poisoning ───────────────────────────────────────────────────────────────

#[test]
fn a_panic_out_of_block_on_poisons_the_runtime() {
    let rt = spool::Runtime::new();
    let _ = expect_panic(|| rt.block_on(async { panic!("first") }));
    let msg = expect_panic(|| rt.block_on(async { 1 }));
    assert!(msg.contains(POISONED), "{msg}");
    let msg = expect_panic(|| rt.run(async { 1 }));
    assert!(msg.contains(POISONED), "{msg}");
}

#[test]
fn every_kind_of_escaping_panic_poisons() {
    type Case = Box<dyn Fn(&spool::Runtime)>;
    let cases: Vec<(&str, Case)> = vec![
        ("main", Box::new(|rt| rt.block_on(async { panic!("x") }))),
        (
            "callback",
            Box::new(|rt| rt.run(async { spool::queue_microtask(|| panic!("x")) })),
        ),
        (
            "detached task",
            Box::new(|rt| {
                rt.run(async {
                    drop(spool::spawn(async { panic!("x") }));
                })
            }),
        ),
        ("stall", Box::new(|rt| rt.block_on(never()))),
    ];
    for (name, case) in cases {
        let rt = spool::Runtime::new();
        let _ = expect_panic(|| case(&rt));
        let msg = expect_panic(|| rt.block_on(async {}));
        assert!(msg.contains(POISONED), "{name}: {msg}");
    }
}

#[test]
fn a_nested_block_on_panic_does_not_poison_the_inner_runtime() {
    let other = spool::Runtime::new();
    let msg = expect_panic(|| spool::block_on(async { other.block_on(async {}) }));
    assert!(msg.contains("nested"), "{msg}");
    assert_eq!(other.block_on(async { 5 }), 5);
}

#[test]
fn a_poisoned_runtime_still_drops_its_tasks() {
    let counter = DropCounter::new();
    let rt = spool::Runtime::new();
    for _ in 0..3 {
        let tok = counter.token();
        drop(rt.spawn(async move {
            let _tok = tok;
            never().await;
        }));
    }
    let _ = expect_panic(|| {
        rt.block_on(async {
            spool::yield_now().await;
            panic!("poison");
        })
    });
    assert_eq!(counter.count(), 0);
    drop(rt);
    assert_eq!(counter.count(), 3);
}

#[test]
fn a_fresh_runtime_works_after_a_panic() {
    let _ = expect_panic(|| {
        spool::block_on(async {
            drop(spool::spawn(async { panic!("x") }));
            spool::yield_now().await;
        })
    });
    assert_eq!(
        spool::block_on(async { spool::spawn(async { 1 }).await }),
        1
    );
}

#[test]
fn spawning_on_a_poisoned_runtime_is_allowed_but_it_cannot_run() {
    let rt = spool::Runtime::new();
    let _ = expect_panic(|| rt.block_on(async { panic!("x") }));
    let h = rt.spawn(async { 1 });
    let msg = expect_panic(|| rt.block_on(h));
    assert!(msg.contains(POISONED), "{msg}");
}
