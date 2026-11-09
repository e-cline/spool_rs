//! Task cancellation: `abort`, `AbortHandle`, `abort_on_drop` (§5).

// `spawn(async move { h.await })` deliberately makes a separate awaiting task.
#![allow(clippy::redundant_async_block)]

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

mod common;

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use common::{DropCounter, Gate, OnDrop, Trace, expect_panic, never};
use spooly::{AbortHandle, JoinError};

fn mock() -> (spooly::Runtime, spooly::MockClock) {
    let clock = spooly::MockClock::new();
    let rt = spooly::Runtime::builder().clock(clock.clone()).build();
    (rt, clock)
}

// ── abort() basics ──────────────────────────────────────────────────────────

#[test]
fn abort_drops_an_idle_task_immediately() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spooly::block_on(async move {
        let tok = c.token();
        let h = spooly::spawn(async move {
            let _tok = tok;
            never().await;
        });
        spooly::yield_now().await; // The task is now pending on `never`.
        assert_eq!(c.count(), 0);
        h.abort();
        assert_eq!(c.count(), 1, "dropped synchronously by abort()");
        assert!(h.is_finished());
        assert!(h.try_join().await.is_err());
    });
}

#[test]
fn abort_before_the_first_poll_means_it_never_runs() {
    let ran = Rc::new(Cell::new(false));
    let r = ran.clone();
    spooly::run(async move {
        let h = spooly::spawn(async move { r.set(true) });
        h.abort();
        assert!(h.try_join().await.is_err());
    });
    assert!(!ran.get());
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn awaiting_an_aborted_handle_panics() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            let h = spooly::spawn(never());
            h.abort();
            h.await
        })
    });
    assert!(msg.contains("task was aborted"), "{msg}");
}

#[test]
#[cfg_attr(
    target_arch = "wasm32",
    ignore = "catches a panic, but WASM aborts on a panic"
)]
fn the_aborted_panic_in_an_awaiting_task_unwinds_out() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            let inner = spooly::spawn(never());
            inner.abort();
            let outer = spooly::spawn(async move { inner.await });
            outer.await;
        })
    });
    assert!(msg.contains("task was aborted"), "{msg}");
}

#[test]
fn abort_after_completion_does_nothing() {
    spooly::block_on(async {
        let h = spooly::spawn(async { 5 });
        spooly::yield_now().await;
        assert!(h.is_finished());
        h.abort();
        h.abort();
        assert_eq!(h.await, 5);
    });
}

#[test]
fn abort_is_idempotent() {
    spooly::block_on(async {
        let h = spooly::spawn(never());
        let a = h.abort_handle();
        h.abort();
        h.abort();
        a.abort();
        assert!(h.try_join().await.is_err());
    });
}

#[test]
fn abort_while_queued_turns_the_queued_poll_into_a_no_op() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let gate = Gate::new();
        let (g, t2) = (gate.clone(), t.clone());
        let h = spooly::spawn(async move {
            t2.log("start");
            g.wait().await;
            t2.log("woken (must not happen)");
        });
        spooly::yield_now().await;
        gate.open(); // Queues a poll of the task...
        h.abort(); // ...which is then aborted.
        spooly::yield_now().await;
        spooly::yield_now().await;
        t.log("main done");
    });
    trace.assert(&["start", "main done"]);
}

#[test]
fn stale_wakes_after_abort_are_ignored() {
    let rt = spooly::Runtime::new();
    let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
    let s = stash.clone();
    let h = rt.spawn(std::future::poll_fn(move |cx| {
        *s.borrow_mut() = Some(cx.waker().clone());
        Poll::<()>::Pending
    }));
    rt.block_on(spooly::yield_now());
    h.abort();
    // The slot is reused by a new task with a new generation.
    let next = rt.spawn(async { 1 });
    stash.borrow_mut().take().unwrap().wake();
    assert_eq!(rt.block_on(next), 1);
    assert!(h.is_finished());
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn a_remote_wake_after_abort_is_ignored() {
    let (tx, rx) = common::oneshot::<u32>();
    let rt = spooly::Runtime::new();
    let h = rt.spawn(rx);
    rt.block_on(spooly::yield_now()); // The task registered its waker.
    h.abort();
    std::thread::spawn(move || tx.send(1)).join().unwrap();
    let next = rt.spawn(async { 2 });
    assert_eq!(rt.block_on(next), 2);
    assert!(rt.block_on(h.try_join()).is_err());
}

#[test]
fn abort_wakes_a_task_awaiting_the_handle() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let target = spooly::spawn(never());
        let abort = target.abort_handle();
        let t2 = t.clone();
        let watcher = spooly::spawn(async move {
            let result = target.try_join().await;
            t2.log(format!("watcher saw {result:?}"));
        });
        spooly::yield_now().await; // The watcher is now waiting.
        t.log("aborting");
        abort.abort();
        watcher.await;
    });
    trace.assert(&["aborting", "watcher saw Err(JoinError(Aborted))"]);
}

#[test]
fn abort_does_not_abort_child_tasks() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let gate = Gate::new();
        let g = gate.clone();
        let t2 = t.clone();
        let parent = spooly::spawn(async move {
            let child = spooly::spawn(async move {
                g.wait().await;
                t2.log("child finished");
            });
            drop(child); // Detached.
            never().await;
        });
        spooly::yield_now().await;
        spooly::yield_now().await;
        parent.abort();
        gate.open();
        for _ in 0..3 {
            spooly::yield_now().await;
        }
    });
    trace.assert(&["child finished"]);
}

#[test]
fn abort_from_a_timer_callback() {
    let (rt, clock) = mock();
    let out = rt.block_on(async {
        let h = spooly::spawn(async {
            spooly::sleep(Duration::from_secs(10)).await;
            "finished"
        });
        let a = h.abort_handle();
        let _t = spooly::set_timeout(Duration::from_secs(1), move || a.abort());
        h.try_join().await
    });
    assert!(out.is_err());
    assert_eq!(clock.elapsed(), Duration::from_secs(1));
}

#[test]
fn aborted_tasks_dont_keep_run_alive() {
    let (rt, clock) = mock();
    rt.run(async {
        let h = spooly::spawn(async {
            spooly::sleep(Duration::from_secs(60)).await;
        });
        spooly::yield_now().await;
        h.abort();
    });
    assert_eq!(clock.elapsed(), Duration::ZERO, "the sleep was cancelled");
}

// ── Self-abort and aborting the awaiter ─────────────────────────────────────

#[test]
fn a_task_can_abort_itself() {
    let trace = Trace::new();
    let counter = DropCounter::new();
    let (t, c) = (trace.clone(), counter.clone());
    spooly::block_on(async move {
        let slot: Rc<RefCell<Option<AbortHandle>>> = Rc::default();
        let (s, t2, c2) = (slot.clone(), t.clone(), c.clone());
        let tok = c.token();
        let h = spooly::spawn(async move {
            let _tok = tok;
            spooly::yield_now().await;
            s.borrow().as_ref().unwrap().abort();
            // Still running: the future is dropped when this poll returns.
            t2.log(format!("after abort, dropped={}", c2.count()));
            spooly::yield_now().await;
            t2.log("never");
        });
        *slot.borrow_mut() = Some(h.abort_handle());
        let result = h.try_join().await;
        t.log(format!("{result:?}, dropped={}", c.count()));
    });
    trace.assert(&[
        "after abort, dropped=0",
        "Err(JoinError(Aborted)), dropped=1",
    ]);
    assert_eq!(counter.count(), 1);
}

#[test]
fn a_task_that_aborts_itself_then_completes_keeps_its_output() {
    spooly::block_on(async {
        let slot: Rc<RefCell<Option<AbortHandle>>> = Rc::default();
        let s = slot.clone();
        let h = spooly::spawn(async move {
            spooly::yield_now().await;
            s.borrow().as_ref().unwrap().abort();
            7
        });
        *slot.borrow_mut() = Some(h.abort_handle());
        assert_eq!(h.try_join().await.unwrap(), 7);
    });
}

#[test]
fn a_task_can_abort_the_task_awaiting_it() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let awaiter_slot: Rc<RefCell<Option<AbortHandle>>> = Rc::default();
        let a = awaiter_slot.clone();
        let t2 = t.clone();
        let inner = spooly::spawn(async move {
            spooly::yield_now().await;
            // The awaiter is idle (waiting on us): it is dropped right now,
            // and with it our JoinHandle, which detaches us.
            a.borrow().as_ref().unwrap().abort();
            t2.log("inner kept running");
            spooly::yield_now().await;
            t2.log("inner done");
        });
        let t3 = t.clone();
        let awaiter = spooly::spawn(async move {
            inner.await;
            t3.log("awaiter resumed (must not happen)");
        });
        *awaiter_slot.borrow_mut() = Some(awaiter.abort_handle());
        let result = awaiter.try_join().await;
        t.log(format!("awaiter: {result:?}"));
        spooly::yield_now().await;
    });
    trace.assert(&[
        "inner kept running",
        "awaiter: Err(JoinError(Aborted))",
        "inner done",
    ]);
}

// ── AbortHandle ─────────────────────────────────────────────────────────────

#[test]
fn abort_handle_is_cloneable_and_tracks_completion() {
    spooly::block_on(async {
        let gate = Gate::new();
        let g = gate.clone();
        let h = spooly::spawn(async move { g.wait().await });
        let a = h.abort_handle();
        let b = a.clone();
        assert!(!a.is_finished());
        gate.open();
        h.await;
        assert!(a.is_finished());
        assert!(b.is_finished());
        b.abort(); // No-op after completion.
        assert!(format!("{a:?}").contains("finished: true"));
    });
}

#[test]
fn abort_handle_outlives_the_join_handle() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spooly::block_on(async move {
        let tok = c.token();
        let a = spooly::spawn(async move {
            let _tok = tok;
            never().await;
        })
        .abort_handle(); // The JoinHandle is dropped: detached.
        spooly::yield_now().await;
        assert!(!a.is_finished());
        a.abort();
        assert!(a.is_finished());
        assert_eq!(c.count(), 1);
    });
}

#[test]
fn abort_handle_after_the_runtime_is_dropped() {
    let rt = spooly::Runtime::new();
    let h = rt.spawn(never());
    let a = h.abort_handle();
    drop(rt);
    assert!(a.is_finished());
    a.abort(); // No-op, no panic.
    assert!(
        h.is_finished(),
        "dropped with its runtime: resolved as aborted"
    );
}

#[test]
fn tasks_dropped_with_their_runtime_resolve_as_aborted() {
    let rt = spooly::Runtime::new();
    let h = rt.spawn(never());
    drop(rt);
    // `TryJoin` doesn't need the task's runtime to report the result.
    let result = spooly::block_on(h.try_join());
    assert!(result.is_err());
}

// ── abort_on_drop ───────────────────────────────────────────────────────────

#[test]
fn abort_on_drop_aborts_when_dropped() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spooly::block_on(async move {
        let tok = c.token();
        let guard = spooly::spawn(async move {
            let _tok = tok;
            never().await;
        })
        .abort_on_drop();
        let a = guard.abort_handle();
        spooly::yield_now().await;
        assert!(!guard.is_finished());
        drop(guard);
        assert!(a.is_finished());
        assert_eq!(c.count(), 1);
    });
}

#[test]
fn abort_on_drop_can_be_awaited() {
    let out = spooly::block_on(async {
        spooly::spawn(async {
            spooly::yield_now().await;
            "ok"
        })
        .abort_on_drop()
        .await
    });
    assert_eq!(out, "ok");
}

#[test]
fn abort_on_drop_scopes_a_task() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        {
            let t2 = t.clone();
            let _ticker = spooly::spawn(async move {
                loop {
                    t2.log("tick");
                    spooly::yield_now().await;
                }
            })
            .abort_on_drop();
            spooly::yield_now().await;
            spooly::yield_now().await;
        } // `_ticker` dropped: the loop is aborted.
        t.log("scope left");
        for _ in 0..3 {
            spooly::yield_now().await;
        }
    });
    trace.assert(&["tick", "tick", "scope left"]);
}

#[test]
fn abort_on_drop_explicit_abort_and_try_join() {
    spooly::block_on(async {
        let guard = spooly::spawn(never()).abort_on_drop();
        guard.abort();
        assert!(guard.try_join().await.is_err());
    });
}

#[test]
fn try_join_from_abort_on_drop_still_aborts_on_drop() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spooly::block_on(async move {
        let tok = c.token();
        let tj = spooly::spawn(async move {
            let _tok = tok;
            never().await;
        })
        .abort_on_drop()
        .try_join();
        spooly::yield_now().await;
        assert!(!tj.is_finished());
        drop(tj);
        assert_eq!(c.count(), 1);
    });
}

#[test]
fn abort_on_drop_after_completion_drops_the_output() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spooly::block_on(async move {
        let tok = c.token();
        let guard = spooly::spawn(async move { tok }).abort_on_drop();
        spooly::yield_now().await;
        assert!(guard.is_finished());
        assert_eq!(c.count(), 0, "output held for the handle");
        drop(guard);
        assert_eq!(c.count(), 1);
    });
}

#[test]
fn try_join_abort() {
    spooly::block_on(async {
        let tj = spooly::spawn(never()).try_join();
        assert!(!tj.is_finished());
        tj.abort();
        assert!(tj.is_finished());
        assert!(format!("{tj:?}").contains("finished: true"));
        assert!(tj.await.is_err());
    });
}

// ── Drop impls that run during abort ────────────────────────────────────────

#[test]
fn drop_during_abort_can_spawn_set_timers_and_wake() {
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let gate = Gate::new();
        let g = gate.clone();
        let t2 = t.clone();
        let t3 = t.clone();
        let waiter = spooly::spawn(async move {
            g.wait().await;
            t3.log("waiter woken");
        });
        let victim = spooly::spawn(async move {
            let _on_drop = OnDrop(move || {
                let (a, b) = (t2.clone(), t2.clone());
                drop(spooly::spawn(async move { a.log("spawned from drop") }));
                spooly::set_timeout(Duration::from_millis(5), move || b.log("timer from drop"));
                gate.open();
            });
            never().await;
        });
        spooly::yield_now().await;
        victim.abort();
        t.log("aborted");
        waiter.await;
    });
    // FIFO: the drop spawned before it opened the gate.
    trace.assert(&[
        "aborted",
        "spawned from drop",
        "waiter woken",
        "timer from drop",
    ]);
    assert_eq!(clock.elapsed(), Duration::from_millis(5));
}

#[test]
fn drop_during_abort_can_abort_another_task() {
    let counter = DropCounter::new();
    let c = counter.clone();
    spooly::block_on(async move {
        let (t1, t2) = (c.token(), c.token());
        let second = spooly::spawn(async move {
            let _t = t2;
            never().await;
        });
        let second_abort = second.abort_handle();
        let first = spooly::spawn(async move {
            let _t = t1;
            let _on_drop = OnDrop(move || second_abort.abort());
            never().await;
        });
        spooly::yield_now().await;
        first.abort();
        assert_eq!(c.count(), 2);
        assert!(second.is_finished());
    });
}

// ── JoinError ───────────────────────────────────────────────────────────────

#[test]
fn join_error_display_and_debug() {
    let err: JoinError = spooly::block_on(async {
        let h = spooly::spawn(never());
        h.abort();
        h.try_join().await.unwrap_err()
    });
    assert_eq!(err.to_string(), "task was aborted");
    assert_eq!(format!("{err:?}"), "JoinError(Aborted)");
    let _: &dyn std::error::Error = &err;
}
