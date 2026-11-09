//! User code calling back into the runtime from polls and `Drop` impls.
//! None of these may hit a `RefCell` `BorrowMutError`.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::task::{Poll, Waker};

use common::{DropCounter, OnDrop, Trace};

#[test]
fn spawn_during_poll() {
    let out = spooly::block_on(async {
        let hs: Vec<_> = (0..10).map(|i| spooly::spawn(async move { i })).collect();
        let mut sum = 0;
        for h in hs {
            sum += h.await;
        }
        sum
    });
    assert_eq!(out, 45);
}

#[test]
fn wake_another_task_during_poll() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
        let (s1, t1) = (stash.clone(), t.clone());
        let sleeper = spooly::spawn(std::future::poll_fn(move |cx| {
            if s1.borrow().is_some() {
                *s1.borrow_mut() = Some(cx.waker().clone());
                t1.log("sleeper parked");
                Poll::Pending
            } else {
                t1.log("sleeper woken");
                Poll::Ready(())
            }
        }));
        *stash.borrow_mut() = Some(Waker::noop().clone());
        spooly::yield_now().await; // Sleeper parks.
        let w = stash.borrow_mut().take().unwrap();
        w.wake(); // Wake it from main's poll.
        sleeper.await;
    });
    trace.assert(&["sleeper parked", "sleeper woken"]);
}

#[test]
fn drop_of_completed_future_spawns_a_task() {
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        let t1 = t.clone();
        let h = spooly::spawn(async move {
            let t2 = t1.clone();
            let _guard = OnDrop(move || {
                let t3 = t2.clone();
                drop(spooly::spawn(async move { t3.log("spawned from drop") }));
            });
            t1.log("task body");
        });
        h.await;
        spooly::yield_now().await;
    });
    trace.assert(&["task body", "spawned from drop"]);
}

#[test]
fn drop_during_teardown_can_spawn() {
    let trace = Trace::new();
    let t = trace.clone();
    let rt = spooly::Runtime::new();
    let counter = DropCounter::new();
    let token = counter.token();
    drop(rt.spawn(async move {
        let _guard = OnDrop(move || {
            // Spawn during teardown: allowed, the task is dropped unrun.
            let tok = DropCounter::new().token();
            let t2 = t.clone();
            drop(spooly::spawn(async move {
                let _tok = tok;
                t2.log("must not run");
            }));
        });
        let _token = token;
        common::never().await;
    }));
    rt.block_on(spooly::yield_now());
    drop(rt);
    assert_eq!(counter.count(), 1, "pending task dropped exactly once");
    trace.assert(&[]);
}

#[test]
fn drop_during_teardown_can_wake_tasks() {
    let rt = spooly::Runtime::new();
    let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
    let s = stash.clone();
    drop(rt.spawn(std::future::poll_fn(move |cx| {
        *s.borrow_mut() = Some(cx.waker().clone());
        Poll::<()>::Pending
    })));
    let s2 = stash.clone();
    drop(rt.spawn(async move {
        let _guard = OnDrop(move || {
            if let Some(w) = s2.borrow_mut().take() {
                w.wake();
            }
        });
        common::never().await;
    }));
    rt.block_on(async {
        spooly::yield_now().await;
        spooly::yield_now().await;
    });
    drop(rt); // Must not panic or loop forever.
    drop(stash);
}

#[test]
fn runtime_dropped_inside_another_runtimes_task() {
    let out = spooly::block_on(async {
        let inner = spooly::Runtime::new();
        let counter = DropCounter::new();
        let tok = counter.token();
        drop(inner.spawn(async move {
            let _tok = tok;
            common::never().await;
        }));
        drop(inner); // Teardown while another runtime is current.
        assert_eq!(counter.count(), 1);
        // The outer runtime is still current and works.
        spooly::spawn(async { 5 }).await
    });
    assert_eq!(out, 5);
}

#[test]
fn spawn_inside_the_output_drop_of_a_detached_task() {
    // The detached task's output is dropped inside the task's own poll.
    struct SpawnOnDrop(Trace);
    impl Drop for SpawnOnDrop {
        fn drop(&mut self) {
            let t = self.0.clone();
            drop(spooly::spawn(async move { t.log("from output drop") }));
        }
    }
    let trace = Trace::new();
    let t = trace.clone();
    spooly::block_on(async move {
        drop(spooly::spawn(async move { SpawnOnDrop(t) }));
        spooly::yield_now().await;
        spooly::yield_now().await;
    });
    trace.assert(&["from output drop"]);
}

#[test]
fn waker_woken_after_task_finished_is_harmless() {
    spooly::block_on(async {
        let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
        let s = stash.clone();
        spooly::spawn(std::future::poll_fn(move |cx| {
            *s.borrow_mut() = Some(cx.waker().clone());
            Poll::Ready(())
        }))
        .await;
        let w = stash.borrow_mut().take().unwrap();
        // The slot may be reused; the stale wake must not poll the new task.
        let counter = Rc::new(std::cell::Cell::new(0));
        let c = counter.clone();
        let h = spooly::spawn(std::future::poll_fn(move |_| {
            c.set(c.get() + 1);
            if c.get() >= 2 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }));
        spooly::yield_now().await; // First poll: pending, no wake registered.
        w.wake_by_ref();
        w.wake();
        spooly::yield_now().await;
        assert_eq!(counter.get(), 1, "stale wakes must not poll the new task");
        drop(h);
    });
}
