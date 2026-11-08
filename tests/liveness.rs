//! Liveness (§3.5): when `run` exits, `KeepAlive` guards, and stall
//! detection.

// `spawn(async move { h.await })` deliberately makes a separate awaiting task.
#![allow(clippy::redundant_async_block)]

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use common::{DropCounter, Gate, Trace, expect_panic, never, oneshot};

const STALLED: &str = "main future stalled";

fn mock() -> (spooly::Runtime, spooly::MockClock) {
    let clock = spooly::MockClock::new();
    let rt = spooly::Runtime::builder().clock(clock.clone()).build();
    (rt, clock)
}

// ── KeepAlive ───────────────────────────────────────────────────────────────

#[test]
fn keep_alive_lets_a_detached_task_finish_its_remote_wait() {
    let got = Rc::new(Cell::new(0));
    let g = got.clone();
    spooly::run(async move {
        let (tx, rx) = oneshot::<u32>();
        drop(spooly::spawn(async move { g.set(rx.await) }));
        let keep = spooly::keep_alive();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(5);
            drop(keep);
        });
    });
    assert_eq!(got.get(), 5);
}

#[test]
fn without_keep_alive_run_drops_the_waiting_task() {
    // The contrast with the test above: Node semantics.
    let counter = DropCounter::new();
    let c = counter.clone();
    let (tx, rx) = oneshot::<u32>();
    spooly::run(async move {
        let tok = c.token();
        drop(spooly::spawn(async move {
            let _tok = tok;
            rx.await;
        }));
    });
    assert_eq!(counter.count(), 1, "dropped with the runtime");
    tx.send(1); // Nobody is listening; harmless.
}

#[test]
fn run_returns_once_the_last_guard_is_dropped() {
    let start = Instant::now();
    spooly::run(async {
        let keep = spooly::keep_alive();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            drop(keep);
        });
    });
    assert!(start.elapsed() >= Duration::from_millis(30));
}

#[test]
fn every_clone_must_be_dropped() {
    let start = Instant::now();
    spooly::run(async {
        let a = spooly::keep_alive();
        let b = a.clone();
        drop(a); // `b` still holds the loop.
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            drop(b);
        });
    });
    assert!(start.elapsed() >= Duration::from_millis(30));
}

#[test]
fn a_guard_dropped_on_the_loop_thread_releases_the_loop() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::run(async move {
        let keep = spooly::keep_alive();
        drop(spooly::spawn(async move {
            spooly::yield_now().await;
            t.log("task drops the guard");
            drop(keep);
        }));
    });
    trace.assert(&["task drops the guard"]);
}

#[test]
fn a_guard_made_before_running() {
    let rt = spooly::Runtime::new();
    let keep = rt.keep_alive();
    let (tx, rx) = oneshot::<()>();
    let done = Rc::new(Cell::new(false));
    let d = done.clone();
    drop(rt.spawn(async move {
        rx.await;
        d.set(true);
        drop(keep);
    }));
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        tx.send(());
    });
    rt.run(async {});
    assert!(done.get());
}

#[test]
fn block_on_ignores_keep_alive() {
    let rt = spooly::Runtime::new();
    let keep = rt.keep_alive();
    assert_eq!(rt.block_on(async { 1 }), 1);
    drop(keep);
}

#[test]
fn keep_alive_together_with_timers() {
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let keep = spooly::keep_alive();
        let t2 = t.clone();
        spooly::set_timeout(Duration::from_secs(5), move || {
            t2.log("timer");
            drop(keep);
        });
        t.log("main done");
    });
    trace.assert(&["main done", "timer"]);
    assert_eq!(clock.elapsed(), Duration::from_secs(5));
}

#[test]
fn a_remote_wake_while_kept_alive_is_handled() {
    // The loop parks for the guard, but still serves remote wakes.
    let trace = Trace::new();
    let t = trace.clone();
    spooly::run(async move {
        let keep = spooly::keep_alive();
        let (tx1, rx1) = oneshot::<u32>();
        let (tx2, rx2) = oneshot::<u32>();
        let t2 = t.clone();
        drop(spooly::spawn(async move {
            t2.log(format!("got {}", rx1.await));
            t2.log(format!("got {}", rx2.await));
        }));
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            tx1.send(1);
            thread::sleep(Duration::from_millis(10));
            tx2.send(2);
            thread::sleep(Duration::from_millis(10));
            drop(keep);
        });
    });
    trace.assert(&["got 1", "got 2"]);
}

#[test]
fn keep_alive_outside_a_runtime_panics() {
    let msg = expect_panic(|| drop(spooly::keep_alive()));
    assert!(msg.contains("`keep_alive` called outside"), "{msg}");
}

#[test]
fn a_guard_can_outlive_its_runtime() {
    let rt = spooly::Runtime::new();
    let keep = rt.keep_alive();
    let copy = keep.clone();
    drop(rt);
    drop(keep);
    thread::spawn(move || drop(copy)).join().unwrap();
}

#[test]
fn keep_alive_debug_output() {
    let rt = spooly::Runtime::new();
    let keep = rt.keep_alive();
    assert!(format!("{keep:?}").starts_with("KeepAlive"));
}

#[test]
fn a_stalled_main_waits_for_the_guard_then_panics() {
    // While a guard exists, the loop can't know nothing will happen; once
    // it is gone, the stall is reported.
    let start = Instant::now();
    let msg = expect_panic(|| {
        spooly::block_on(async {
            let keep = spooly::keep_alive();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(30));
                drop(keep);
            });
            never().await;
        })
    });
    assert!(msg.contains(STALLED), "{msg}");
    assert!(start.elapsed() >= Duration::from_millis(30));
}

// ── Stall detection: stalls that are now detected ───────────────────────────

#[test]
fn main_awaiting_a_stuck_task_is_a_stall() {
    // The stuck task holds main's waker (in its JoinHandle state), but can
    // never be woken itself.
    let msg = expect_panic(|| spooly::block_on(async { spooly::spawn(never()).await }));
    assert!(msg.contains(STALLED), "{msg}");
    let msg = expect_panic(|| spooly::run(async { spooly::spawn(never()).await }));
    assert!(msg.contains(STALLED), "{msg}");
}

#[test]
fn a_chain_of_stuck_tasks_is_a_stall() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            let c = spooly::spawn(never());
            let b = spooly::spawn(async move { c.await });
            let a = spooly::spawn(async move { b.await });
            a.await
        })
    });
    assert!(msg.contains(STALLED), "{msg}");
}

#[test]
fn every_join_flavor_is_seen_through() {
    let msg = expect_panic(|| {
        spooly::block_on(async { spooly::spawn(never()).try_join().await.is_ok() })
    });
    assert!(msg.contains(STALLED), "try_join: {msg}");
    let msg =
        expect_panic(|| spooly::block_on(async { spooly::spawn(never()).abort_on_drop().await }));
    assert!(msg.contains(STALLED), "abort_on_drop: {msg}");
}

#[test]
fn a_task_stuck_after_a_timer_is_a_stall_once_the_timer_is_gone() {
    let (rt, clock) = mock();
    let msg = expect_panic(|| {
        rt.block_on(async {
            spooly::spawn(async {
                spooly::sleep(Duration::from_secs(3)).await;
                never().await;
            })
            .await
        })
    });
    assert!(msg.contains(STALLED), "{msg}");
    assert_eq!(clock.elapsed(), Duration::from_secs(3));
}

#[test]
fn stuck_detached_tasks_dont_hide_a_stall() {
    let msg = expect_panic(|| {
        spooly::block_on(async {
            for _ in 0..5 {
                drop(spooly::spawn(never()));
            }
            spooly::spawn(never()).await
        })
    });
    assert!(msg.contains(STALLED), "{msg}");
}

#[test]
fn the_stalled_runtime_drops_its_stuck_tasks() {
    let counter = DropCounter::new();
    let c = counter.clone();
    let _ = expect_panic(move || {
        spooly::block_on(async move {
            let tok = c.token();
            spooly::spawn(async move {
                let _tok = tok;
                never().await;
            })
            .await
        })
    });
    assert_eq!(counter.count(), 1);
}

// ── Stall detection: things that look stuck but aren't ──────────────────────

#[test]
fn main_awaiting_a_task_that_waits_on_a_thread() {
    let out = spooly::block_on(async {
        let (tx, rx) = oneshot::<u32>();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(9);
        });
        spooly::spawn(rx).await
    });
    assert_eq!(out, 9);
}

#[test]
fn main_awaiting_a_chain_that_ends_on_a_thread() {
    let out = spooly::block_on(async {
        let (tx, rx) = oneshot::<u32>();
        let c = spooly::spawn(rx);
        let b = spooly::spawn(async move { c.await + 1 });
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(1);
        });
        spooly::spawn(async move { b.await + 1 }).await
    });
    assert_eq!(out, 3);
}

#[test]
fn a_detached_task_on_a_thread_can_unstick_the_awaited_task() {
    // Main awaits T, which waits on a gate. Nothing about T or main is
    // remote, but a detached task D (waiting on a thread) opens the gate.
    let out = spooly::block_on(async {
        let gate = Gate::new();
        let g = gate.clone();
        let (tx, rx) = oneshot::<()>();
        drop(spooly::spawn(async move {
            rx.await;
            gate.open();
        }));
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(());
        });
        spooly::spawn(async move {
            g.wait().await;
            "unstuck"
        })
        .await
    });
    assert_eq!(out, "unstuck");
}

#[test]
fn a_task_waking_main_from_a_thread() {
    // Main's own waker is sent to another thread (not through a JoinHandle).
    let out = spooly::block_on(async {
        let (tx, rx) = oneshot::<u32>();
        let _detached = spooly::spawn(never());
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(4);
        });
        rx.await
    });
    assert_eq!(out, 4);
}

#[test]
fn awaiting_an_already_finished_task_is_not_a_stall() {
    let out = spooly::block_on(async {
        let h = spooly::spawn(async { 8 });
        spooly::yield_now().await;
        h.await
    });
    assert_eq!(out, 8);
}

// ── When `run` exits ────────────────────────────────────────────────────────

#[test]
fn run_waits_for_timers_but_not_for_stuck_tasks() {
    let (rt, clock) = mock();
    let counter = DropCounter::new();
    let c = counter.clone();
    rt.run(async move {
        let tok = c.token();
        drop(spooly::spawn(async move {
            let _tok = tok;
            spooly::sleep(Duration::from_secs(1)).await;
            never().await;
        }));
    });
    assert_eq!(
        clock.elapsed(),
        Duration::from_secs(1),
        "the timer kept it alive"
    );
    assert_eq!(counter.count(), 0, "the stuck task stays on the runtime");
    drop(rt);
    assert_eq!(counter.count(), 1);
}

#[test]
fn run_exits_when_only_stuck_detached_tasks_remain() {
    // Main finished; detached tasks that can't be woken don't keep `run`
    // alive (no stall panic either: main isn't pending).
    let rt = spooly::Runtime::new();
    rt.run(async {
        let c = spooly::spawn(never());
        drop(spooly::spawn(async move { c.await }));
    });
}

#[test]
fn a_cancelled_timer_does_not_keep_run_alive() {
    let (rt, clock) = mock();
    rt.run(async {
        let h = spooly::set_timeout(Duration::from_secs(10), || {});
        spooly::sleep(Duration::from_secs(1)).await;
        h.cancel();
    });
    assert_eq!(clock.elapsed(), Duration::from_secs(1));
}
