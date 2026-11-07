//! Differential tests: each scenario runs in Spool and, as a JS twin, in Node.
//! The two event traces must match exactly.
//!
//! JS ↔ Spool mapping used by the twins:
//!
//! | Spool                         | JS                                  |
//! |-------------------------------|-------------------------------------|
//! | `spawn(async { .. })` (detached) | `queueMicrotask(async () => { .. })` |
//! | `yield_now().await`           | `await null`                        |
//! | `trace.log(x)`                | `console.log(x)`                    |
//!
//! If `node` isn't on `PATH`, these tests are skipped with a message, unless
//! `SPOOL_REQUIRE_NODE=1` is set (as in CI), in which case they fail.

mod common;

use std::path::Path;
use std::process::Command;

use common::Trace;

/// Runs `setup` on a fresh runtime, then drives the runtime until every task
/// it spawned (directly) has finished. Returns the trace.
fn run_spool(setup: impl FnOnce(&Trace) -> Vec<spool::JoinHandle<()>>) -> Vec<String> {
    let trace = Trace::new();
    let rt = spool::Runtime::new();
    let handles = rt.block_on(async { setup(&trace) });
    rt.block_on(async {
        // This waiter is just another task; it doesn't log, so its position
        // in the queue doesn't affect the trace.
        for h in handles {
            h.await;
        }
        // Let any grandchildren finish as well.
        for _ in 0..64 {
            spool::yield_now().await;
        }
    });
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
    // The hand-written expectation documents the behavior, and guards
    // against both implementations drifting together.
    assert_eq!(spool_trace, expected, "Spool trace for {script}");
    if let Some(node_trace) = run_node(script) {
        assert_eq!(node_trace, expected, "Node trace for {script}");
    }
}

fn detach(t: &Trace, f: impl AsyncFnOnce(Trace) + 'static) -> spool::JoinHandle<()> {
    let t = t.clone();
    spool::spawn(async move { f(t).await })
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn interleave() {
    let trace = run_spool(|t| {
        ["a", "b", "c"]
            .into_iter()
            .map(|name| {
                detach(t, async move |t| {
                    for step in 1..=3 {
                        t.log(format!("{name}{step}"));
                        spool::yield_now().await;
                    }
                })
            })
            .collect()
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
    let trace = run_spool(|t| {
        [("a", 1), ("b", 3), ("c", 0)]
            .into_iter()
            .map(|(name, yields)| {
                detach(t, async move |t| {
                    t.log(format!("{name} start"));
                    for i in 0..yields {
                        spool::yield_now().await;
                        t.log(format!("{name} y{i}"));
                    }
                    t.log(format!("{name} end"));
                })
            })
            .collect()
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
    let trace = run_spool(|t| {
        vec![
            detach(t, async |t| {
                t.log("outer start");
                drop(detach(&t, async |t| {
                    t.log("inner start");
                    spool::yield_now().await;
                    t.log("inner end");
                }));
                spool::yield_now().await;
                t.log("outer end");
            }),
            detach(t, async |t| {
                t.log("sibling start");
                spool::yield_now().await;
                t.log("sibling end");
            }),
        ]
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
    let trace = run_spool(|t| {
        vec![
            detach(t, async |t| {
                t.log("a1");
                spool::yield_now().await;
                t.log("a2");
                drop(detach(&t, async |t| t.log("a-child")));
                t.log("a3");
            }),
            detach(t, async |t| {
                t.log("b1");
                spool::yield_now().await;
                t.log("b2");
                spool::yield_now().await;
                t.log("b3");
            }),
        ]
    });
    check(
        "spawn_after_yield.js",
        trace,
        &["a1", "b1", "a2", "a3", "b2", "a-child", "b3"],
    );
}
