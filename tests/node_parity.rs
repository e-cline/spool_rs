//! Differential tests: each scenario runs in Spool and, as a JS twin, in Node.
//! The two event traces must match exactly, and must also match a
//! hand-written expectation (so both can't drift together).
//!
//! JS ↔ Spool mapping used by the twins:
//!
//! | Spool                                | JS                                   |
//! |--------------------------------------|--------------------------------------|
//! | `spool::run(async { body })`         | `async function main() { body } main();` |
//! | `spawn(async { .. })` (detached)     | `queueMicrotask(async () => { .. })` |
//! | `spawn(..)` then `handle.await`      | a deferred promise resolved at the end of the task, then `await promise` |
//! | `yield_now().await`                  | `await null`                         |
//! | `queue_microtask(f)`                 | `queueMicrotask(f)`                  |
//! | `next_tick(f)`                       | `process.nextTick(f)`                |
//! | `trace.log(x)`                       | `console.log(x)`                     |
//!
//! If `node` isn't on `PATH`, the Node half is skipped with a message, unless
//! `SPOOL_REQUIRE_NODE=1` is set (as in CI), in which case it fails.

mod common;

use std::path::Path;
use std::process::Command;

use common::Trace;

/// Runs `main` with `spool::run`, which waits until the loop is empty, like a
/// Node process. Returns the trace.
fn run_spool(main: impl AsyncFnOnce(Trace)) -> Vec<String> {
    let trace = Trace::new();
    spool::run(main(trace.clone()));
    trace.events()
}

fn run_node(script: &str) -> Option<Vec<String>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/node_parity")
        .join(script);
    let output = match Command::new("node").arg(&path).output() {
        Ok(output) => output,
        Err(err) => {
            if std::env::var_os("SPOOL_REQUIRE_NODE").is_some() {
                panic!("SPOOL_REQUIRE_NODE is set but `node` could not run: {err}");
            }
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
fn check(script: &str, spool_trace: Vec<String>, expected: &[&str]) {
    assert_eq!(spool_trace, expected, "Spool trace for {script}");
    if let Some(node_trace) = run_node(script) {
        assert_eq!(node_trace, expected, "Node trace for {script}");
    }
}

/// Spawns a detached task that gets its own clone of the trace.
fn detach(t: &Trace, f: impl AsyncFnOnce(Trace) + 'static) {
    let t = t.clone();
    drop(spool::spawn(async move { f(t).await }));
}

/// Queues a microtask that gets its own clone of the trace.
fn micro(t: &Trace, f: impl FnOnce(Trace) + 'static) {
    let t = t.clone();
    spool::queue_microtask(move || f(t));
}

/// Queues a tick that gets its own clone of the trace.
fn tick(t: &Trace, f: impl FnOnce(Trace) + 'static) {
    let t = t.clone();
    spool::next_tick(move || f(t));
}

// ── M1 scenarios: tasks and yields ──────────────────────────────────────────

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn interleave() {
    let trace = run_spool(async |t| {
        for name in ["a", "b", "c"] {
            detach(&t, async move |t| {
                for step in 1..=3 {
                    t.log(format!("{name}{step}"));
                    spool::yield_now().await;
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
    let trace = run_spool(async |t| {
        for (name, yields) in [("a", 1), ("b", 3), ("c", 0)] {
            detach(&t, async move |t| {
                t.log(format!("{name} start"));
                for i in 0..yields {
                    spool::yield_now().await;
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
    let trace = run_spool(async |t| {
        detach(&t, async |t| {
            t.log("outer start");
            detach(&t, async |t| {
                t.log("inner start");
                spool::yield_now().await;
                t.log("inner end");
            });
            spool::yield_now().await;
            t.log("outer end");
        });
        detach(&t, async |t| {
            t.log("sibling start");
            spool::yield_now().await;
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
    let trace = run_spool(async |t| {
        detach(&t, async |t| {
            t.log("a1");
            spool::yield_now().await;
            t.log("a2");
            detach(&t, async |t| t.log("a-child"));
            t.log("a3");
        });
        detach(&t, async |t| {
            t.log("b1");
            spool::yield_now().await;
            t.log("b2");
            spool::yield_now().await;
            t.log("b3");
        });
    });
    check(
        "spawn_after_yield.js",
        trace,
        &["a1", "b1", "a2", "a3", "b2", "a-child", "b3"],
    );
}

// ── M2 scenarios: ticks and microtasks ──────────────────────────────────────

/// Plan §8.2 scenario 1 (minus timers, which arrive in M3).
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn sync_tick_microtask() {
    let trace = run_spool(async |t| {
        micro(&t, |t| t.log("microtask"));
        tick(&t, |t| t.log("tick"));
        t.log("sync");
    });
    check(
        "sync_tick_microtask.js",
        trace,
        &["sync", "tick", "microtask"],
    );
}

/// Plan §8.2 scenario 2.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn tick_inside_microtask() {
    let trace = run_spool(async |t| {
        micro(&t, |t| {
            t.log("m1");
            tick(&t, |t| t.log("t2 (queued by m1)"));
        });
        micro(&t, |t| t.log("m2"));
        tick(&t, |t| t.log("t1"));
    });
    check(
        "tick_inside_microtask.js",
        trace,
        &["t1", "m1", "m2", "t2 (queued by m1)"],
    );
}

/// Plan §8.2 scenario 6.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn yield_microtask_interleave() {
    let trace = run_spool(async |t| {
        micro(&t, |t| t.log("A"));
        detach(&t, async |t| {
            t.log("B");
            spool::yield_now().await;
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

/// Plan §8.2 scenario 9 (minus the timer callback, which arrives in M3).
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn spawn_from_callbacks() {
    let trace = run_spool(async |t| {
        tick(&t, |t| detach(&t, async |t| t.log("task from tick")));
        micro(&t, |t| detach(&t, async |t| t.log("task from microtask")));
        detach(&t, async |t| t.log("task from main"));
    });
    check(
        "spawn_from_callbacks.js",
        trace,
        &["task from main", "task from tick", "task from microtask"],
    );
}

/// Plan §8.2 scenario 10, with a sibling task that yields alongside the chain
/// so that every microtask hop is visible in the trace.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn join_chain() {
    let trace = run_spool(async |t| {
        detach(&t, async |t| {
            for i in 0..6 {
                t.log(format!("s{i}"));
                spool::yield_now().await;
            }
        });
        let ta = t.clone();
        let a = spool::spawn(async move {
            ta.log("A start");
            let tb = ta.clone();
            let b = spool::spawn(async move {
                tb.log("B start");
                let tc = tb.clone();
                let c = spool::spawn(async move {
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
fn tick_inside_tick() {
    let trace = run_spool(async |t| {
        tick(&t, |t| {
            t.log("t1");
            tick(&t, |t| t.log("t3 (queued by t1)"));
        });
        tick(&t, |t| t.log("t2"));
        micro(&t, |t| t.log("m"));
    });
    check(
        "tick_inside_tick.js",
        trace,
        &["t1", "t2", "t3 (queued by t1)", "m"],
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn microtask_inside_tick() {
    let trace = run_spool(async |t| {
        tick(&t, |t| {
            t.log("t1");
            micro(&t, |t| t.log("m2 (queued by t1)"));
        });
        micro(&t, |t| t.log("m1"));
    });
    check(
        "microtask_inside_tick.js",
        trace,
        &["t1", "m1", "m2 (queued by t1)"],
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn main_yields() {
    let trace = run_spool(async |t| {
        tick(&t, |t| t.log("t1"));
        micro(&t, |t| t.log("m1"));
        t.log("main before yield");
        spool::yield_now().await;
        t.log("main after yield");
        tick(&t, |t| t.log("t2"));
        micro(&t, |t| t.log("m2"));
    });
    check(
        "main_yields.js",
        trace,
        &[
            "main before yield",
            "t1",
            "m1",
            "main after yield",
            "m2",
            "t2",
        ],
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn alternating_rounds() {
    // Each microtask queues a tick and each tick queues a microtask, a few
    // levels deep: exercises repeated rounds of the drain loop.
    fn m(t: &Trace, depth: u32) {
        micro(t, move |t| {
            t.log(format!("m{depth}"));
            if depth < 3 {
                k(&t, depth + 1);
                micro(&t, move |t| t.log(format!("m{depth}b")));
            }
        });
    }
    fn k(t: &Trace, depth: u32) {
        tick(t, move |t| {
            t.log(format!("t{depth}"));
            if depth < 3 {
                m(&t, depth + 1);
                tick(&t, move |t| t.log(format!("t{depth}b")));
            }
        });
    }
    let trace = run_spool(async |t| {
        m(&t, 0);
        k(&t, 0);
    });
    check(
        "alternating_rounds.js",
        trace,
        &[
            "t0", "t0b", "m0", "m1", "m0b", "m1b", "t1", "t2", "t1b", "t2b", "m2", "m3", "m2b",
            "t3",
        ],
    );
}
