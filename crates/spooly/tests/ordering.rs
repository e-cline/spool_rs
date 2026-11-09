//! Scheduling order of tasks and `yield_now` (design decisions D3, D4).

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test as test;

mod common;

use common::Trace;

#[test]
fn spawn_does_not_poll_eagerly() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let t2 = t.clone();
        let h = spooly::spawn(async move { t2.log("task") });
        t.log("after spawn");
        h.await;
    });
    trace.assert(&["after spawn", "task"]);
}

#[test]
fn spawned_tasks_run_in_fifo_order() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let t = t.clone();
                spooly::spawn(async move { t.log(format!("{i}")) })
            })
            .collect();
        for h in handles {
            h.await;
        }
    });
    trace.assert(&["0", "1", "2", "3", "4"]);
}

#[test]
fn yield_now_requeues_at_the_back() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let mut handles = Vec::new();
        for name in ["a", "b", "c"] {
            let t = t.clone();
            handles.push(spooly::spawn(async move {
                for step in 1..=3 {
                    t.log(format!("{name}{step}"));
                    spooly::yield_now().await;
                }
            }));
        }
        for h in handles {
            h.await;
        }
    });
    trace.assert(&["a1", "b1", "c1", "a2", "b2", "c2", "a3", "b3", "c3"]);
}

#[test]
fn main_yield_lets_queued_tasks_run_first() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        for i in 0..3 {
            let t = t.clone();
            drop(spooly::spawn(async move { t.log(format!("task {i}")) }));
        }
        t.log("main before");
        spooly::yield_now().await;
        t.log("main after");
    });
    trace.assert(&["main before", "task 0", "task 1", "task 2", "main after"]);
}

#[test]
fn spawn_from_inside_a_task_queues_behind_existing_work() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let (t1, t2) = (t.clone(), t.clone());
        let outer = spooly::spawn(async move {
            t1.log("outer");
            let t3 = t1.clone();
            spooly::spawn(async move { t3.log("inner") }).await;
            t1.log("outer done");
        });
        let sibling = spooly::spawn(async move { t2.log("sibling") });
        outer.await;
        sibling.await;
    });
    trace.assert(&["outer", "sibling", "inner", "outer done"]);
}

#[test]
fn join_chain_resolves_like_promise_chain() {
    // main awaits A, A awaits B, B awaits C. Completion propagates outward,
    // one microtask hop per level.
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let (ta, tb, tc) = (t.clone(), t.clone(), t.clone());
        let a = spooly::spawn(async move {
            ta.log("A start");
            let b = spooly::spawn(async move {
                tb.log("B start");
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
        assert_eq!(a.await, 6);
        t.log("main end");
    });
    trace.assert(&["A start", "B start", "C", "B end", "A end", "main end"]);
}

#[test]
fn completion_wake_is_queued_behind_existing_microtasks() {
    // When a task completes, its joiner is queued at the back of the
    // microtask queue (like a promise resolution), behind work already queued.
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let t1 = t.clone();
        let quick = spooly::spawn(async move { t1.log("quick") });
        let t2 = t.clone();
        let slow = spooly::spawn(async move {
            t2.log("slow 1");
            spooly::yield_now().await;
            t2.log("slow 2");
        });
        quick.await;
        t.log("main resumed");
        slow.await;
    });
    // `quick` completes and queues main; `slow` (already queued) runs first.
    trace.assert(&["quick", "slow 1", "main resumed", "slow 2"]);
}

#[test]
fn self_wake_during_poll_requeues_the_task() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let t1 = t.clone();
        let mut polls = 0;
        let a = spooly::spawn(std::future::poll_fn(move |cx| {
            polls += 1;
            t1.log(format!("a poll {polls}"));
            if polls < 3 {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(())
            }
        }));
        let t2 = t.clone();
        let b = spooly::spawn(async move { t2.log("b") });
        a.await;
        b.await;
    });
    trace.assert(&["a poll 1", "b", "a poll 2", "a poll 3"]);
}

#[test]
fn many_tasks_interleave_fairly() {
    let trace = Trace::new();
    let t = trace.clone();
    const N: usize = 50;
    const STEPS: usize = 4;
    spooly::block_on(async move {
        let handles: Vec<_> = (0..N)
            .map(|i| {
                let t = t.clone();
                spooly::spawn(async move {
                    for s in 0..STEPS {
                        t.log(format!("{i}:{s}"));
                        spooly::yield_now().await;
                    }
                })
            })
            .collect();
        for h in handles {
            h.await;
        }
    });
    let expected: Vec<String> = (0..STEPS)
        .flat_map(|s| (0..N).map(move |i| format!("{i}:{s}")))
        .collect();
    assert_eq!(trace.events(), expected);
}
