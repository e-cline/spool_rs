//! Timers, driven by `MockClock` so that every test is instant and
//! deterministic (except the few marked "real clock").

mod common;

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use common::{DropCounter, OnDrop, Trace, expect_panic, oneshot};
use spool::{Instant, MockClock, Runtime};

const MS: Duration = Duration::from_millis(1);

fn mock() -> (Runtime, MockClock) {
    let clock = MockClock::new();
    let rt = Runtime::builder().clock(clock.clone()).build();
    (rt, clock)
}

fn at(ms: u32) -> Instant {
    Instant::ZERO + ms * MS
}

/// Logs `"{label}@{ms}"` with the current mock time.
fn log_at(t: &Trace, label: &str) {
    let now = spool::now().epoch_offset().as_millis();
    t.log(format!("{label}@{now}"));
}

// ── sleep ───────────────────────────────────────────────────────────────────

#[test]
fn sleep_advances_the_mock_clock_exactly() {
    let (rt, clock) = mock();
    rt.block_on(async {
        spool::sleep(250 * MS).await;
        assert_eq!(spool::now(), at(250));
        spool::sleep(Duration::from_secs(3600)).await;
    });
    assert_eq!(clock.elapsed(), Duration::from_secs(3600) + 250 * MS);
}

#[test]
fn sleep_zero_yields_past_queued_microtasks() {
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let (t1, t2) = (t.clone(), t.clone());
        spool::queue_microtask(move || {
            t1.log("microtask");
            spool::queue_microtask(move || t1.log("microtask from microtask"));
        });
        spool::queue_microtask(move || t2.log("microtask 2"));
        spool::sleep(Duration::ZERO).await;
        t.log("after sleep(0)");
    });
    trace.assert(&[
        "microtask",
        "microtask 2",
        "microtask from microtask",
        "after sleep(0)",
    ]);
    assert_eq!(clock.elapsed(), Duration::ZERO, "no time passed");
}

#[test]
fn sleep_until_in_the_past_fires_in_the_next_timers_phase() {
    let (rt, clock) = mock();
    clock.advance(100 * MS);
    rt.block_on(async {
        let s = spool::sleep_until(at(10));
        assert_eq!(s.deadline(), at(10));
        assert!(!s.is_elapsed());
        s.await;
        assert_eq!(spool::now(), at(100));
    });
}

#[test]
fn sleep_reset_and_is_elapsed() {
    let (rt, _clock) = mock();
    rt.block_on(async {
        let mut s = spool::sleep(10 * MS);
        s.reset(at(50));
        assert_eq!(s.deadline(), at(50));
        (&mut s).await;
        assert!(s.is_elapsed());
        assert_eq!(spool::now(), at(50));
        // Reset after completion re-arms it.
        s.reset(at(70));
        assert!(!s.is_elapsed());
        (&mut s).await;
        assert_eq!(spool::now(), at(70));
        // Polling a completed sleep again is fine.
        (&mut s).await;
        assert_eq!(
            format!("{s:?}"),
            "Sleep { deadline: Instant(epoch + 70ms), elapsed: true }"
        );
    });
}

#[test]
fn sleep_with_a_huge_duration_saturates() {
    let (rt, _clock) = mock();
    rt.block_on(async {
        let s = spool::sleep(Duration::MAX);
        assert_eq!(s.deadline(), Instant::MAX);
        let r = spool::timeout(MS, s).await;
        assert!(r.is_err());
    });
}

#[test]
fn dropping_a_sleep_cancels_its_timer() {
    let (rt, clock) = mock();
    rt.run(async {
        let s = spool::sleep(Duration::from_secs(10));
        drop(s);
    });
    assert_eq!(clock.elapsed(), Duration::ZERO, "run didn't wait for it");
}

#[test]
fn an_unawaited_sleep_keeps_run_alive_until_its_deadline() {
    // Like `setTimeout`, the timer starts when `sleep` is called.
    let (rt, clock) = mock();
    let s = Rc::new(RefCell::new(None));
    let s2 = s.clone();
    rt.run(async move {
        *s2.borrow_mut() = Some(spool::sleep(40 * MS));
    });
    assert_eq!(clock.elapsed(), 40 * MS);
    drop(s);
}

#[test]
fn sleeps_complete_in_deadline_order() {
    let (rt, _clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        for ms in [30, 10, 20, 10] {
            let t = t.clone();
            drop(spool::spawn(async move {
                spool::sleep(ms * MS).await;
                log_at(&t, &format!("sleep {ms}"));
            }));
        }
    });
    trace.assert(&["sleep 10@10", "sleep 10@10", "sleep 20@20", "sleep 30@30"]);
}

// ── set_timeout ─────────────────────────────────────────────────────────────

#[test]
fn set_timeout_fires_by_deadline_then_registration_order() {
    let (rt, _clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        for (label, ms) in [("x", 30), ("y", 10), ("z", 10), ("w", 20)] {
            let t = t.clone();
            spool::set_timeout(ms * MS, move || log_at(&t, label));
        }
    });
    trace.assert(&["y@10", "z@10", "w@20", "x@30"]);
}

#[test]
fn many_equal_deadline_timers_are_fifo() {
    let (rt, _clock) = mock();
    let order = Rc::new(RefCell::new(Vec::new()));
    let o = order.clone();
    rt.run(async move {
        for i in 0..500 {
            let o = o.clone();
            spool::set_timeout(5 * MS, move || o.borrow_mut().push(i));
        }
    });
    assert_eq!(*order.borrow(), (0..500).collect::<Vec<_>>());
}

#[test]
fn microtasks_drain_between_timer_callbacks() {
    // Plan §8.2 scenario 2.
    let (rt, _clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let t1 = t.clone();
        spool::set_timeout(Duration::ZERO, move || {
            t1.log("timer 1");
            let t2 = t1.clone();
            spool::queue_microtask(move || {
                t2.log("microtask from timer 1");
                spool::queue_microtask(move || t2.log("nested microtask"));
            });
        });
        let t4 = t.clone();
        spool::set_timeout(Duration::ZERO, move || t4.log("timer 2"));
    });
    trace.assert(&[
        "timer 1",
        "microtask from timer 1",
        "nested microtask",
        "timer 2",
    ]);
}

#[test]
fn a_self_rearming_zero_delay_timer_does_not_starve_others() {
    // Plan §8.2 scenario 3.
    fn rearm(t: Trace, n: u32) {
        spool::set_timeout(Duration::ZERO, move || {
            t.log(format!("a{n}"));
            if n < 3 {
                rearm(t, n + 1);
            }
        });
    }
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        rearm(t.clone(), 1);
        let tb = t.clone();
        spool::set_timeout(Duration::ZERO, move || tb.log("b"));
    });
    trace.assert(&["a1", "b", "a2", "a3"]);
    assert_eq!(clock.elapsed(), Duration::ZERO);
}

#[test]
fn a_timer_armed_mid_phase_with_an_earlier_deadline_does_not_block_due_timers() {
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let t1 = t.clone();
        spool::set_timeout(10 * MS, move || {
            t1.log("A");
            // Armed during the phase, with a deadline earlier than B's.
            let late = spool::sleep_until(at(5));
            let t2 = t1.clone();
            drop(spool::spawn(async move {
                late.await;
                t2.log("late sleep");
            }));
        });
        let tb = t.clone();
        spool::set_timeout(10 * MS, move || tb.log("B"));
    });
    // B existed when the phase started, so it fires in that phase.
    trace.assert(&["A", "B", "late sleep"]);
    assert_eq!(clock.elapsed(), 10 * MS);
}

#[test]
fn nested_timeout_microtask_timeout() {
    // Plan §8.2 scenario 7.
    let (rt, _clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let ta = t.clone();
        spool::set_timeout(Duration::ZERO, move || {
            ta.log("A");
            let tm = ta.clone();
            spool::queue_microtask(move || {
                tm.log("M");
                let tb = tm.clone();
                spool::set_timeout(Duration::ZERO, move || tb.log("B"));
            });
        });
        let tc = t.clone();
        spool::set_timeout(Duration::ZERO, move || tc.log("C"));
    });
    trace.assert(&["A", "M", "C", "B"]);
}

#[test]
fn spawn_from_a_timer_callback() {
    // Plan §8.2 scenario 8 (timer part).
    let (rt, _clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let t1 = t.clone();
        spool::set_timeout(Duration::ZERO, move || {
            t1.log("timer 1");
            let t2 = t1.clone();
            drop(spool::spawn(async move { t2.log("task from timer 1") }));
        });
        let t3 = t.clone();
        spool::set_timeout(Duration::ZERO, move || t3.log("timer 2"));
    });
    trace.assert(&["timer 1", "task from timer 1", "timer 2"]);
}

#[test]
fn cancel_before_firing() {
    let (rt, clock) = mock();
    let fired = Rc::new(Cell::new(false));
    let f = fired.clone();
    rt.run(async move {
        let h = spool::set_timeout(Duration::from_secs(5), move || f.set(true));
        assert!(h.is_active());
        h.cancel();
        assert!(!h.is_active());
        h.cancel(); // Idempotent.
    });
    assert!(!fired.get());
    assert_eq!(
        clock.elapsed(),
        Duration::ZERO,
        "a cancelled timer doesn't keep run alive"
    );
}

#[test]
fn cancel_drops_the_callback_immediately() {
    let (rt, _clock) = mock();
    let counter = DropCounter::new();
    let tok = counter.token();
    let c = counter.clone();
    rt.run(async move {
        let h = spool::set_timeout(MS, move || drop(tok));
        assert_eq!(c.count(), 0);
        h.cancel();
        assert_eq!(c.count(), 1);
    });
}

#[test]
fn cancel_from_a_clone_and_after_firing() {
    let (rt, _clock) = mock();
    let hits = Rc::new(Cell::new(0));
    let h1 = hits.clone();
    rt.run(async move {
        let a = spool::set_timeout(MS, || unreachable!());
        a.clone().cancel();
        assert!(!a.is_active());

        let h2 = h1.clone();
        let b = spool::set_timeout(MS, move || h2.set(h2.get() + 1));
        spool::sleep(2 * MS).await;
        assert!(!b.is_active(), "a fired timeout is no longer active");
        b.cancel(); // No-op.
    });
    assert_eq!(hits.get(), 1);
}

#[test]
fn set_timeout_is_inactive_while_its_callback_runs() {
    let (rt, _clock) = mock();
    let slot: Rc<RefCell<Option<spool::TimerHandle>>> = Rc::default();
    let s = slot.clone();
    let seen = Rc::new(Cell::new(None));
    let seen2 = seen.clone();
    rt.run(async move {
        let s2 = s.clone();
        let h = spool::set_timeout(MS, move || {
            seen2.set(Some(s2.borrow().as_ref().unwrap().is_active()));
        });
        *s.borrow_mut() = Some(h);
    });
    assert_eq!(seen.get(), Some(false));
}

#[test]
fn timer_handle_debug() {
    let (rt, _clock) = mock();
    rt.block_on(async {
        let h = spool::set_timeout(MS, || {});
        assert_eq!(format!("{h:?}"), "TimerHandle { active: true }");
        h.cancel();
        assert_eq!(format!("{h:?}"), "TimerHandle { active: false }");
    });
}

// ── set_interval ────────────────────────────────────────────────────────────

#[test]
fn set_interval_ticks_on_schedule_and_cancels_itself() {
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let mut n = 0;
        spool::set_interval(10 * MS, move |handle| {
            n += 1;
            log_at(&t, &format!("tick {n}"));
            if n == 3 {
                handle.cancel();
                assert!(!handle.is_active());
            }
        });
    });
    trace.assert(&["tick 1@10", "tick 2@20", "tick 3@30"]);
    assert_eq!(clock.elapsed(), 30 * MS);
}

#[test]
fn set_interval_interleaves_with_timeouts() {
    // Plan §8.2 scenario 6.
    let (rt, _clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let ti = t.clone();
        let mut n = 0;
        spool::set_interval(20 * MS, move |h| {
            n += 1;
            log_at(&ti, &format!("i{n}"));
            if n == 3 {
                h.cancel();
            }
        });
        for ms in [30, 50] {
            let t = t.clone();
            spool::set_timeout(ms * MS, move || log_at(&t, &format!("t{ms}")));
        }
    });
    trace.assert(&["i1@20", "t30@30", "i2@40", "t50@50", "i3@60"]);
}

#[test]
fn a_late_set_interval_run_does_not_shift_the_schedule() {
    // A slow timer callback delays the interval's first run to 17ms. The
    // schedule stays on multiples of 10ms (D7).
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    let c = clock.clone();
    rt.run(async move {
        spool::set_timeout(5 * MS, move || c.advance(12 * MS)); // "Slow": until 17ms.
        let mut n = 0;
        spool::set_interval(10 * MS, move |h| {
            n += 1;
            log_at(&t, &format!("i{n}"));
            if n == 3 {
                h.cancel();
            }
        });
    });
    trace.assert(&["i1@17", "i2@20", "i3@30"]);
    assert_eq!(clock.elapsed(), 30 * MS);
}

#[test]
fn set_interval_skips_missed_runs_instead_of_bursting() {
    // A slow timer callback holds the loop until 32ms, past the interval's
    // 10, 20, and 30ms runs. It runs once, late, then waits for 40ms.
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    let c = clock.clone();
    rt.run(async move {
        spool::set_timeout(5 * MS, move || c.advance(27 * MS)); // Until 32ms.
        let mut n = 0;
        spool::set_interval(10 * MS, move |h| {
            n += 1;
            log_at(&t, &format!("i{n}"));
            if n == 3 {
                h.cancel();
            }
        });
    });
    trace.assert(&["i1@32", "i2@40", "i3@50"]);
}

#[test]
fn a_set_interval_callback_slower_than_its_period_skips_runs() {
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    let c = clock.clone();
    rt.run(async move {
        let mut n = 0;
        spool::set_interval(10 * MS, move |h| {
            n += 1;
            log_at(&t, &format!("i{n}"));
            c.advance(15 * MS); // Each run takes 15ms.
            if n == 3 {
                h.cancel();
            }
        });
    });
    // 10 (busy until 25, so 20 is skipped), 30 (until 45), 50.
    trace.assert(&["i1@10", "i2@30", "i3@50"]);
}

#[test]
fn set_interval_callback_duration_does_not_shift_the_schedule() {
    // Each run is due a whole period after the previous *deadline* (D7).
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    rt.run(async move {
        let mut n = 0;
        spool::set_interval(10 * MS, move |h| {
            n += 1;
            log_at(&t, &format!("i{n}"));
            clock.advance(3 * MS); // The callback itself takes 3ms.
            if n == 3 {
                h.cancel();
            }
        });
    });
    trace.assert(&["i1@10", "i2@20", "i3@30"]);
}

#[test]
fn set_interval_cancelled_from_outside() {
    let (rt, _clock) = mock();
    let hits = Rc::new(Cell::new(0));
    let h = hits.clone();
    rt.run(async move {
        let handle = spool::set_interval(10 * MS, move |_| h.set(h.get() + 1));
        spool::sleep(35 * MS).await;
        handle.cancel();
    });
    assert_eq!(hits.get(), 3);
}

#[test]
fn set_interval_callback_survives_cancelling_itself_mid_call() {
    // The callback's own state must stay alive for the rest of the call.
    let (rt, _clock) = mock();
    let counter = DropCounter::new();
    let tok = counter.token();
    let c = counter.clone();
    rt.run(async move {
        spool::set_interval(MS, move |h| {
            h.cancel();
            let _still_here = &tok;
            assert_eq!(c.count(), 0, "not dropped mid-call");
        });
    });
    assert_eq!(counter.count(), 1, "dropped after the call");
}

#[test]
fn zero_period_intervals_panic() {
    let (rt, _clock) = mock();
    let msg = expect_panic(|| rt.block_on(async { spool::set_interval(Duration::ZERO, |_| {}) }));
    assert!(msg.contains("period must be non-zero"), "{msg}");
    let (rt, _clock) = mock();
    let msg = expect_panic(|| rt.block_on(async { drop(spool::interval(Duration::ZERO)) }));
    assert!(msg.contains("period must be non-zero"), "{msg}");
}

// ── interval() ──────────────────────────────────────────────────────────────

#[test]
fn interval_first_tick_is_immediate() {
    let (rt, clock) = mock();
    clock.advance(7 * MS);
    rt.block_on(async {
        let mut iv = spool::interval(10 * MS);
        assert_eq!(iv.period(), 10 * MS);
        assert_eq!(iv.tick().await, at(7));
        assert_eq!(iv.tick().await, at(17));
        assert_eq!(iv.tick().await, at(27));
    });
}

#[test]
fn interval_skips_missed_ticks_and_keeps_its_schedule() {
    let (rt, clock) = mock();
    let c = clock.clone();
    rt.block_on(async move {
        let mut iv = spool::interval(10 * MS);
        assert_eq!(iv.tick().await, at(0));
        c.advance(25 * MS); // Busy until 25ms: the 20ms tick is missed.
        // One late tick, reporting the time it was scheduled for...
        assert_eq!(iv.tick().await, at(10));
        assert_eq!(spool::now(), at(25));
        // ...then the schedule continues on multiples of 10ms, skipping 20.
        assert_eq!(iv.tick().await, at(30));
        assert_eq!(iv.tick().await, at(40));
    });
}

#[test]
fn interval_does_not_drift_when_slightly_late() {
    let (rt, clock) = mock();
    let c = clock.clone();
    rt.block_on(async move {
        let mut iv = spool::interval(10 * MS);
        iv.tick().await;
        assert_eq!(iv.tick().await, at(10));
        c.advance(4 * MS); // Work between ticks.
        assert_eq!(iv.tick().await, at(20));
        assert_eq!(spool::now(), at(20), "not 24");
    });
}

#[test]
fn dropping_an_interval_cancels_it() {
    let (rt, clock) = mock();
    rt.run(async {
        let mut iv = spool::interval(10 * MS);
        iv.tick().await;
        iv.tick().await;
        drop(iv);
    });
    assert_eq!(clock.elapsed(), 10 * MS);
}

#[test]
fn interval_tick_is_cancel_safe() {
    let (rt, _clock) = mock();
    rt.block_on(async {
        let mut iv = spool::interval(10 * MS);
        iv.tick().await;
        // Give up on a tick partway through.
        assert!(spool::timeout(4 * MS, iv.tick()).await.is_err());
        // The next tick is still due at 10ms.
        assert_eq!(iv.tick().await, at(10));
    });
}

// ── timeout ─────────────────────────────────────────────────────────────────

#[test]
fn timeout_ok_when_the_future_wins() {
    let (rt, clock) = mock();
    let out = rt.block_on(async { spool::timeout(10 * MS, async { 5 }).await });
    assert_eq!(out, Ok(5));
    assert_eq!(clock.elapsed(), Duration::ZERO);
}

#[test]
fn timeout_err_when_time_runs_out_and_the_future_is_dropped_at_once() {
    let (rt, clock) = mock();
    let counter = DropCounter::new();
    let tok = counter.token();
    let c = counter.clone();
    rt.block_on(async move {
        let fut = async move {
            let _tok = tok;
            spool::sleep(Duration::from_secs(1)).await;
        };
        let t = spool::timeout(10 * MS, fut);
        assert_eq!(t.deadline(), at(10));
        let mut t = std::pin::pin!(t);
        let r = (&mut t).await;
        assert_eq!(r, Err(spool::Elapsed::new()));
        assert_eq!(
            c.count(),
            1,
            "inner future dropped on elapse, before the Timeout"
        );
    });
    assert_eq!(clock.elapsed(), 10 * MS);
}

#[test]
fn timeout_prefers_the_output_when_both_are_ready() {
    let (rt, _clock) = mock();
    let out = rt.block_on(async {
        let t = spool::timeout(10 * MS, spool::sleep(10 * MS));
        // Both timers fire before the Timeout is first polled.
        spool::sleep(20 * MS).await;
        t.await
    });
    assert_eq!(out, Ok(()));
}

#[test]
fn equal_deadlines_race_in_registration_order() {
    // Like `Promise.race` over two `setTimeout`s with the same delay: the
    // timer registered first fires first, and the waiting task runs between
    // the two timers.
    let (rt, _clock) = mock();
    let (inner_first, timeout_first) = rt.block_on(async {
        let inner = spool::sleep(10 * MS); // Registered before the timeout.
        let a = spool::timeout(10 * MS, inner).await;
        // Here the inner sleep is created lazily, after the timeout's.
        let b = spool::timeout(10 * MS, async { spool::sleep(10 * MS).await }).await;
        (a, b)
    });
    assert_eq!(inner_first, Ok(()));
    assert_eq!(timeout_first, Err(spool::Elapsed::new()));
}

#[test]
fn timeout_at_uses_an_absolute_deadline() {
    let (rt, clock) = mock();
    clock.advance(100 * MS);
    let r = rt.block_on(async { spool::timeout_at(at(130), std::future::pending::<()>()).await });
    assert!(r.is_err());
    assert_eq!(clock.elapsed(), 130 * MS);
}

#[test]
fn completed_timeout_does_not_keep_run_alive() {
    let (rt, clock) = mock();
    rt.run(async {
        assert_eq!(
            spool::timeout(Duration::from_secs(60), async { 1 }).await,
            Ok(1)
        );
    });
    assert_eq!(clock.elapsed(), Duration::ZERO);
}

#[test]
fn timeout_polled_after_completion_panics() {
    let (rt, _clock) = mock();
    let msg = expect_panic(|| {
        rt.block_on(async {
            let mut t = spool::timeout(MS, async {});
            (&mut t).await.unwrap();
            (&mut t).await.unwrap();
        })
    });
    assert!(msg.contains("polled after it completed"), "{msg}");
}

#[test]
fn elapsed_is_an_error() {
    let e = spool::Elapsed::default();
    assert_eq!(e.to_string(), "deadline has elapsed");
    let _: &dyn std::error::Error = &e;
}

// ── MockClock ───────────────────────────────────────────────────────────────

#[test]
fn manual_advance_fires_due_timers_in_order() {
    let (rt, clock) = mock();
    let trace = Trace::new();
    let t = trace.clone();
    let c = clock.clone();
    rt.run(async move {
        for ms in [30, 10, 20] {
            let t = t.clone();
            spool::set_timeout(ms * MS, move || log_at(&t, &format!("t{ms}")));
        }
        c.advance(25 * MS); // 10 and 20 are now due together.
        let t2 = t.clone();
        spool::queue_microtask(move || t2.log("microtask"));
    });
    trace.assert(&["microtask", "t10@25", "t20@25", "t30@30"]);
}

#[test]
fn runtime_now_reads_its_clock() {
    let (rt, clock) = mock();
    clock.advance(3 * MS);
    assert_eq!(rt.now(), at(3));
    rt.block_on(async { assert_eq!(spool::now(), at(3)) });
    assert_eq!(format!("{clock:?}"), "MockClock { elapsed: 3ms }");
}

// ── Liveness ────────────────────────────────────────────────────────────────

#[test]
fn run_waits_for_timers_but_block_on_does_not() {
    let (rt, clock) = mock();
    let fired = Rc::new(Cell::new(false));
    let f = fired.clone();
    rt.block_on(async move {
        spool::set_timeout(10 * MS, move || f.set(true));
    });
    assert!(!fired.get());
    assert_eq!(clock.elapsed(), Duration::ZERO);
    rt.run(async {});
    assert!(fired.get(), "the leftover timer kept `run` alive");
    assert_eq!(clock.elapsed(), 10 * MS);
}

#[test]
fn main_waiting_on_a_timer_is_not_stalled() {
    let (rt, clock) = mock();
    rt.block_on(async { spool::sleep(5 * MS).await });
    assert_eq!(clock.elapsed(), 5 * MS);
}

#[test]
fn main_pending_after_the_last_timer_stalls() {
    let (rt, _clock) = mock();
    let msg = expect_panic(|| {
        rt.block_on(async {
            spool::sleep(5 * MS).await;
            std::future::pending::<()>().await
        })
    });
    assert!(msg.contains("main future stalled"), "{msg}");
}

#[test]
fn timer_callback_panic_unwinds_out_of_run() {
    let (rt, _clock) = mock();
    let msg = expect_panic(|| rt.run(async { spool::set_timeout(MS, || panic!("timer boom")) }));
    assert_eq!(msg, "timer boom");
}

// ── Teardown and dropped runtimes ───────────────────────────────────────────

#[test]
fn pending_timers_are_dropped_with_the_runtime() {
    let (rt, _clock) = mock();
    let counter = DropCounter::new();
    let (a, b, c) = (counter.token(), counter.token(), counter.token());
    rt.block_on(async move {
        spool::set_timeout(MS, move || drop(a));
        spool::set_interval(MS, move |_| {
            let _b = &b;
        });
        drop(spool::spawn(async move {
            let _c = c;
            spool::sleep(MS).await;
        }));
    });
    assert_eq!(counter.count(), 0);
    drop(rt);
    assert_eq!(counter.count(), 3);
}

#[test]
fn dropping_a_timer_callback_during_teardown_can_set_timers() {
    let (rt, _clock) = mock();
    rt.block_on(async {
        let guard = OnDrop(|| {
            let h = spool::set_timeout(MS, || unreachable!());
            assert!(h.is_active());
            drop(spool::sleep(MS));
        });
        spool::set_timeout(MS, move || drop(guard));
    });
    drop(rt); // Must not panic with `BorrowMutError` or loop forever.
}

#[test]
fn handles_outliving_their_runtime_are_harmless() {
    let (rt, _clock) = mock();
    let (handle, sleep) = rt.block_on(async { (spool::set_timeout(MS, || {}), spool::sleep(MS)) });
    drop(rt);
    assert!(!handle.is_active());
    handle.cancel();
    assert!(!sleep.is_elapsed());
    drop(sleep);
}

#[test]
fn polling_a_sleep_after_its_runtime_is_gone_panics() {
    let (rt, _clock) = mock();
    // Create the Sleep inside the runtime, but don't await it there.
    let sleep = rt.block_on(async { std::cell::Cell::new(Some(spool::sleep(MS))) });
    drop(rt);
    let sleep = sleep.take().unwrap();
    let msg = expect_panic(|| spool::block_on(sleep));
    assert!(msg.contains("used after its runtime was dropped"), "{msg}");
}

#[test]
fn timer_apis_outside_a_runtime_panic() {
    for (name, f) in [
        (
            "sleep",
            Box::new(|| drop(spool::sleep(MS))) as Box<dyn Fn()>,
        ),
        ("sleep_until", Box::new(|| drop(spool::sleep_until(at(1))))),
        (
            "set_timeout",
            Box::new(|| drop(spool::set_timeout(MS, || {}))),
        ),
        (
            "set_interval",
            Box::new(|| drop(spool::set_interval(MS, |_| {}))),
        ),
        (
            "now",
            Box::new(|| {
                let _ = spool::now();
            }),
        ),
        ("sleep", Box::new(|| drop(spool::interval(MS)))),
        ("sleep", Box::new(|| drop(spool::timeout(MS, async {})))),
    ] {
        let msg = expect_panic(f);
        assert!(
            msg.contains(&format!("`{name}` called outside")),
            "{name}: {msg}"
        );
    }
}

// ── Real clock ──────────────────────────────────────────────────────────────

#[test]
fn real_clock_sleep_takes_at_least_its_duration() {
    let start = std::time::Instant::now();
    spool::block_on(async {
        let t0 = spool::now();
        spool::sleep(30 * MS).await;
        assert!(t0.elapsed() >= 30 * MS);
    });
    assert!(start.elapsed() >= 30 * MS);
}

#[test]
fn real_clock_remote_wake_interrupts_a_long_timer_wait() {
    let start = std::time::Instant::now();
    let (tx, rx) = oneshot();
    let worker = thread::spawn(move || {
        thread::sleep(20 * MS);
        tx.send(1);
    });
    let out = spool::block_on(async {
        // A far-off timer is armed, so the loop parks with a long timeout.
        let _far = spool::sleep(Duration::from_secs(30));
        rx.await
    });
    assert_eq!(out, 1);
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "woken by the remote wake"
    );
    worker.join().unwrap();
}
