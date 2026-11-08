//! The browser driver (`browser::start`), in a real browser: steps run from
//! JS callbacks, real timers use `setTimeout`, and wakes from outside a step
//! queue a step.
//!
//! Each test starts a runtime, then waits (as an async `wasm_bindgen_test`,
//! on the test harness's own executor) for a one-shot that the runtime
//! completes.
#![cfg(all(target_arch = "wasm32", feature = "browser"))]

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use common::{Trace, oneshot};
use spooly::browser;
use wasm_bindgen_test::wasm_bindgen_test;

const MS: Duration = Duration::from_millis(1);

#[wasm_bindgen_test]
async fn start_returns_at_once_and_runs_main_later() {
    let ran = Rc::new(Cell::new(false));
    let (done, finished) = oneshot();
    let r = ran.clone();
    browser::start(async move {
        r.set(true);
        done.send(());
    });
    assert!(
        !ran.get(),
        "main runs from a JS callback, not inside `start`"
    );
    finished.await;
    assert!(ran.get());
}

#[wasm_bindgen_test]
async fn queued_work_runs_before_main_and_main_runs_like_top_level_code() {
    let trace = Trace::new();
    let rt = spooly::Runtime::new();
    let t = trace.clone();
    drop(rt.spawn(async move { t.log("pre-spawned task") }));
    let (done, finished) = oneshot();
    let t = trace.clone();
    browser::start_on(rt, async move {
        let t2 = t.clone();
        spooly::queue_microtask(move || t2.log("microtask"));
        t.log("main");
        spooly::yield_now().await;
        t.log("main after yield");
        done.send(());
    });
    finished.await;
    trace.assert(&["pre-spawned task", "main", "microtask", "main after yield"]);
}

#[wasm_bindgen_test]
async fn real_timers_fire_in_order_after_their_delay() {
    let trace = Trace::new();
    let (done, finished) = oneshot();
    let t = trace.clone();
    browser::start(async move {
        let start = spooly::now();
        let (a, b) = (t.clone(), t.clone());
        spooly::set_timeout(30 * MS, move || a.log("30ms"));
        spooly::set_timeout(10 * MS, move || b.log("10ms"));
        spooly::sleep(40 * MS).await;
        assert!(start.elapsed() >= 40 * MS);
        t.log("slept 40ms");
        done.send(());
    });
    finished.await;
    trace.assert(&["10ms", "30ms", "slept 40ms"]);
}

#[wasm_bindgen_test]
async fn a_mock_clock_jumps_instead_of_waiting() {
    let clock = spooly::MockClock::new();
    let rt = spooly::Runtime::builder().clock(clock.clone()).build();
    let (done, finished) = oneshot();
    browser::start_on(rt, async move {
        spooly::sleep(Duration::from_secs(3600)).await;
        done.send(());
    });
    finished.await;
    assert_eq!(clock.elapsed(), Duration::from_secs(3600));
}

#[wasm_bindgen_test]
async fn a_wake_from_outside_a_step_resumes_the_task() {
    // `wake` is called from the test's own executor, outside any Spooly
    // step: it takes the remote path, which queues a step.
    let (started, has_started) = oneshot();
    let (send, recv) = oneshot::<u32>();
    let (done, finished) = oneshot();
    browser::start(async move {
        started.send(());
        let value = recv.await;
        done.send(value + 1);
    });
    has_started.await;
    send.send(41);
    assert_eq!(finished.await, 42);
}

#[wasm_bindgen_test]
async fn the_runtime_keeps_running_after_main_completes() {
    let (send, recv) = oneshot::<&str>();
    let (done, finished) = oneshot();
    let main_done = Rc::new(Cell::new(false));
    let m = main_done.clone();
    browser::start(async move {
        // A detached task, waiting on a wake from outside, and a timer.
        drop(spooly::spawn(async move {
            let message = recv.await;
            spooly::sleep(5 * MS).await;
            done.send(message);
        }));
        m.set(true);
    });
    // Let main finish first: wait 10ms, on a second runtime.
    let (tick, ticked) = oneshot();
    browser::start(async move {
        spooly::sleep(10 * MS).await;
        tick.send(());
    });
    ticked.await;
    assert!(main_done.get());
    send.send("still alive");
    assert_eq!(finished.await, "still alive");
}

#[wasm_bindgen_test]
async fn intervals_tick_on_real_time() {
    let (done, finished) = oneshot();
    browser::start(async move {
        let start = spooly::now();
        let mut interval = spooly::interval(5 * MS);
        for _ in 0..4 {
            interval.tick().await;
        }
        // Ticks at 0, 5, 10 and 15ms.
        assert!(start.elapsed() >= 15 * MS);
        done.send(());
    });
    finished.await;
}

#[wasm_bindgen_test]
async fn several_runtimes_run_side_by_side() {
    let (a_done, a_finished) = oneshot();
    let (b_done, b_finished) = oneshot();
    browser::start(async move {
        spooly::sleep(10 * MS).await;
        a_done.send("a");
    });
    browser::start(async move {
        spooly::sleep(5 * MS).await;
        b_done.send("b");
    });
    assert_eq!((a_finished.await, b_finished.await), ("a", "b"));
}

#[wasm_bindgen_test]
fn performance_clock_moves_forward() {
    use spooly::Clock;
    let clock = browser::PerformanceClock::new();
    let a = clock.now();
    let b = clock.now();
    assert!(b >= a);
}
