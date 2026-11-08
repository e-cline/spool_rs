//! An optional comparison with Node (plan §8.1): each scenario runs in Spooly
//! and, as a JS twin, in Node, and both traces must match a hand-written
//! expectation. Spooly's semantics are defined by the plan and the reference
//! model (tests/model.rs), not by Node. These scenarios are ones where the
//! two agree, as a sanity check that Spooly still behaves like a JS event
//! loop. Where Spooly deliberately differs (intervals, plan D7), there is no
//! comparison.
//!
//! JS ↔ Spooly mapping used by the twins:
//!
//! | Spooly                                | JS                                   |
//! |--------------------------------------|--------------------------------------|
//! | `spooly::run(async { body })`         | `async function main() { body } main();` |
//! | `spawn(async { .. })` (detached)     | `queueMicrotask(async () => { .. })` |
//! | `spawn(..)` then `handle.await`      | a deferred promise resolved at the end of the task, then `await promise` |
//! | `yield_now().await`                  | `await null`                         |
//! | `queue_microtask(f)`                 | `queueMicrotask(f)`                  |
//! | `set_timeout(d, f)`                  | `setTimeout(f, ms)`                  |
//! | `sleep(d).await`                     | `await new Promise(r => setTimeout(r, ms))` |
//! | `trace.log(x)`                       | `console.log(x)`                     |
//!
//! Spooly runs on a `MockClock`, and Node in real time. Timer scenarios use
//! delays far enough apart (10ms or more) that real-time jitter can't
//! reorder them, and never mix delays of 0 and 1ms (Node clamps 0 to 1ms).
//!
//! The Node half is opt-in: it runs only with `SPOOLY_COMPARE_NODE=1` (and
//! `node` on `PATH`). Otherwise, as in CI, only the Spooly half (the
//! expectation) runs, so real-time jitter in Node can't fail a build.

mod common;

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use common::Trace;

const MS: Duration = Duration::from_millis(1);

/// Runs `main` with `run`, which waits until the loop is empty, like a Node
/// process, on a `MockClock`. Returns the trace.
fn run_spooly(main: impl AsyncFnOnce(Trace)) -> Vec<String> {
    let trace = Trace::new();
    let rt = spooly::Runtime::builder()
        .clock(spooly::MockClock::new())
        .build();
    rt.run(main(trace.clone()));
    trace.events()
}

/// `set_timeout` with a callback that gets its own clone of the trace.
fn timeout(t: &Trace, ms: u32, f: impl FnOnce(Trace) + 'static) {
    let t = t.clone();
    spooly::set_timeout(ms * MS, move || f(t));
}

fn run_node(script: &str) -> Option<Vec<String>> {
    if std::env::var_os("SPOOLY_COMPARE_NODE").is_none_or(|v| v != "1") {
        return None;
    }
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/node_parity")
        .join(script);
    let output = match Command::new("node").arg(&path).output() {
        Ok(output) => output,
        Err(err) => {
            eprintln!("skipping Node parity check for {script}: `node` not available ({err})");
            return None;
        }
    };
    assert!(
        output.status.success(),
        "node {script} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Some(
        String::from_utf8(output.stdout)
            .expect("node output is UTF-8")
            .lines()
            .map(str::to_owned)
            .collect(),
    )
}

#[track_caller]
fn check(script: &str, spooly_trace: Vec<String>, expected: &[&str]) {
    assert_eq!(spooly_trace, expected, "Spooly trace for {script}");
    if let Some(node_trace) = run_node(script) {
        assert_eq!(node_trace, expected, "Node trace for {script}");
    }
}

/// Spawns a detached task that gets its own clone of the trace.
fn detach(t: &Trace, f: impl AsyncFnOnce(Trace) + 'static) {
    let t = t.clone();
    drop(spooly::spawn(async move { f(t).await }));
}

/// Queues a microtask that gets its own clone of the trace.
fn micro(t: &Trace, f: impl FnOnce(Trace) + 'static) {
    let t = t.clone();
    spooly::queue_microtask(move || f(t));
}

// ── Tasks and yields ────────────────────────────────────────────────────────

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn interleave() {
    let trace = run_spooly(async |t| {
        for name in ["a", "b", "c"] {
            detach(&t, async move |t| {
                for step in 1..=3 {
                    t.log(format!("{name}{step}"));
                    spooly::yield_now().await;
                }
            });
        }
    });
    check(
        "interleave.js",
        trace,
        &["a1", "b1", "c1", "a2", "b2", "c2", "a3", "b3", "c3"],
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn uneven_yields() {
    let trace = run_spooly(async |t| {
        for (name, yields) in [("a", 1), ("b", 3), ("c", 0)] {
            detach(&t, async move |t| {
                t.log(format!("{name} start"));
                for i in 0..yields {
                    spooly::yield_now().await;
                    t.log(format!("{name} y{i}"));
                }
                t.log(format!("{name} end"));
            });
        }
    });
    check(
        "uneven_yields.js",
        trace,
        &[
            "a start", "b start", "c start", "c end", "a y0", "a end", "b y0", "b y1", "b y2",
            "b end",
        ],
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn nested_spawn() {
    let trace = run_spooly(async |t| {
        detach(&t, async |t| {
            t.log("outer start");
            detach(&t, async |t| {
                t.log("inner start");
                spooly::yield_now().await;
                t.log("inner end");
            });
            spooly::yield_now().await;
            t.log("outer end");
        });
        detach(&t, async |t| {
            t.log("sibling start");
            spooly::yield_now().await;
            t.log("sibling end");
        });
    });
    check(
        "nested_spawn.js",
        trace,
        &[
            "outer start",
            "sibling start",
            "inner start",
            "outer end",
            "sibling end",
            "inner end",
        ],
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn spawn_after_yield() {
    let trace = run_spooly(async |t| {
        detach(&t, async |t| {
            t.log("a1");
            spooly::yield_now().await;
            t.log("a2");
            detach(&t, async |t| t.log("a-child"));
            t.log("a3");
        });
        detach(&t, async |t| {
            t.log("b1");
            spooly::yield_now().await;
            t.log("b2");
            spooly::yield_now().await;
            t.log("b3");
        });
    });
    check(
        "spawn_after_yield.js",
        trace,
        &["a1", "b1", "a2", "a3", "b2", "a-child", "b3"],
    );
}

// ── Microtasks ──────────────────────────────────────────────────────────────

/// Plan §8.2 scenario 1.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn sync_microtask_timeout() {
    let trace = run_spooly(async |t| {
        timeout(&t, 0, |t| t.log("timeout"));
        micro(&t, |t| t.log("microtask"));
        t.log("sync");
    });
    check(
        "sync_microtask_timeout.js",
        trace,
        &["sync", "microtask", "timeout"],
    );
}

/// Plan §8.2 scenario 5.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn yield_microtask_interleave() {
    let trace = run_spooly(async |t| {
        micro(&t, |t| t.log("A"));
        detach(&t, async |t| {
            t.log("B");
            spooly::yield_now().await;
            t.log("D");
        });
        micro(&t, |t| t.log("C"));
    });
    check(
        "yield_microtask_interleave.js",
        trace,
        &["A", "B", "C", "D"],
    );
}

/// Plan §8.2 scenario 8 (microtask part).
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn spawn_from_callbacks() {
    let trace = run_spooly(async |t| {
        micro(&t, |t| detach(&t, async |t| t.log("task from microtask")));
        detach(&t, async |t| t.log("task from main"));
    });
    check(
        "spawn_from_callbacks.js",
        trace,
        &["task from main", "task from microtask"],
    );
}

/// Plan §8.2 scenario 9, with a sibling task that yields alongside the chain
/// so that every microtask hop is visible in the trace.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn join_chain() {
    let trace = run_spooly(async |t| {
        detach(&t, async |t| {
            for i in 0..6 {
                t.log(format!("s{i}"));
                spooly::yield_now().await;
            }
        });
        let ta = t.clone();
        let a = spooly::spawn(async move {
            ta.log("A start");
            let tb = ta.clone();
            let b = spooly::spawn(async move {
                tb.log("B start");
                let tc = tb.clone();
                let c = spooly::spawn(async move {
                    tc.log("C");
                    3
                });
                let v = c.await;
                tb.log("B end");
                v + 2
            });
            let v = b.await;
            ta.log("A end");
            v + 1
        });
        let v = a.await;
        t.log(format!("main end {v}"));
    });
    check(
        "join_chain.js",
        trace,
        &[
            "s0",
            "A start",
            "s1",
            "B start",
            "s2",
            "C",
            "s3",
            "B end",
            "s4",
            "A end",
            "s5",
            "main end 6",
        ],
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn main_yields() {
    let trace = run_spooly(async |t| {
        micro(&t, |t| t.log("m1"));
        t.log("main before yield");
        spooly::yield_now().await;
        t.log("main after yield");
        micro(&t, |t| t.log("m2"));
    });
    check(
        "main_yields.js",
        trace,
        &["main before yield", "m1", "main after yield", "m2"],
    );
}

// ── Timers ──────────────────────────────────────────────────────────────────

/// The example from plan §2 (with delays spread out for real-time Node).
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn plan_example() {
    let trace = run_spooly(async |t| {
        timeout(&t, 40, |t| t.log("timeout 40ms"));
        micro(&t, |t| t.log("microtask"));
        t.log("sync");
        let h = spooly::spawn(async {
            spooly::sleep(10 * MS).await;
            42
        });
        t.log(format!("joined: {}", h.await));
    });
    check(
        "plan_example.js",
        trace,
        &["sync", "microtask", "joined: 42", "timeout 40ms"],
    );
}

/// Plan §8.2 scenario 2.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn microtasks_drain_between_timers() {
    let trace = run_spooly(async |t| {
        timeout(&t, 0, |t| {
            t.log("timer 1");
            micro(&t, |t| {
                t.log("microtask from timer 1");
                micro(&t, |t| t.log("nested microtask"));
            });
        });
        timeout(&t, 0, |t| t.log("timer 2"));
    });
    check(
        "microtasks_drain_between_timers.js",
        trace,
        &[
            "timer 1",
            "microtask from timer 1",
            "nested microtask",
            "timer 2",
        ],
    );
}

/// Plan §8.2 scenario 3.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn self_rearming_timer() {
    fn rearm(t: &Trace, n: u32) {
        timeout(t, 0, move |t| {
            t.log(format!("a{n}"));
            if n < 3 {
                rearm(&t, n + 1);
            }
        });
    }
    let trace = run_spooly(async |t| {
        rearm(&t, 1);
        timeout(&t, 0, |t| t.log("b"));
    });
    check("self_rearming_timer.js", trace, &["a1", "b", "a2", "a3"]);
}

/// Plan §8.2 scenario 4.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn timer_deadline_order() {
    let trace = run_spooly(async |t| {
        for (label, ms) in [("x40", 40), ("y10", 10), ("z10", 10), ("w25", 25)] {
            timeout(&t, ms, move |t| t.log(label));
        }
    });
    check(
        "timer_deadline_order.js",
        trace,
        &["y10", "z10", "w25", "x40"],
    );
}

/// Plan §8.2 scenario 7.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn timeout_microtask_timeout() {
    let trace = run_spooly(async |t| {
        timeout(&t, 0, |t| {
            t.log("A");
            micro(&t, |t| {
                t.log("M");
                timeout(&t, 0, |t| t.log("B"));
            });
        });
        timeout(&t, 0, |t| t.log("C"));
    });
    check("timeout_microtask_timeout.js", trace, &["A", "M", "C", "B"]);
}

/// Plan §8.2 scenario 8 (timer part).
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn spawn_from_timer() {
    let trace = run_spooly(async |t| {
        timeout(&t, 0, |t| {
            t.log("timer 1");
            detach(&t, async |t| t.log("task from timer 1"));
        });
        timeout(&t, 0, |t| t.log("timer 2"));
    });
    check(
        "spawn_from_timer.js",
        trace,
        &["timer 1", "task from timer 1", "timer 2"],
    );
}

/// A task woken by a timer resumes right after that timer, before the next
/// due timer. Timers armed by the woken task wait for the next phase.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn sleep_between_timeouts() {
    let trace = run_spooly(async |t| {
        timeout(&t, 0, |t| t.log("A"));
        let s = spooly::sleep(Duration::ZERO);
        timeout(&t, 0, |t| t.log("B"));
        s.await;
        t.log("main woke");
        timeout(&t, 0, |t| t.log("C"));
    });
    check(
        "sleep_between_timeouts.js",
        trace,
        &["A", "main woke", "B", "C"],
    );
}
