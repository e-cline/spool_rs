//! Wakes that come from other threads (the remote path in design D9).

mod common;

use std::sync::{Arc, Barrier, Mutex};
use std::task::{Poll, Waker};
use std::thread;
use std::time::Duration;

use common::oneshot;

#[test]
fn another_thread_wakes_main() {
    let (tx, rx) = oneshot();
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send(7);
    });
    let out = spool::block_on(rx);
    assert_eq!(out, 7);
    worker.join().unwrap();
}

#[test]
fn another_thread_wakes_a_spawned_task() {
    let out = spool::block_on(async {
        let (tx, rx) = oneshot();
        let task = spool::spawn(async move { rx.await * 2 });
        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            tx.send(21);
        });
        let v = task.await;
        worker.join().unwrap();
        v
    });
    assert_eq!(out, 42);
}

#[test]
fn many_threads_wake_many_tasks() {
    const N: usize = 32;
    let out = spool::block_on(async {
        let mut handles = Vec::new();
        let mut workers = Vec::new();
        let barrier = Arc::new(Barrier::new(N));
        for i in 0..N {
            let (tx, rx) = oneshot();
            handles.push(spool::spawn(rx));
            let b = barrier.clone();
            workers.push(thread::spawn(move || {
                b.wait();
                tx.send(i);
            }));
        }
        let mut sum = 0;
        for h in handles {
            sum += h.await;
        }
        for w in workers {
            w.join().unwrap();
        }
        sum
    });
    assert_eq!(out, (0..N).sum());
}

#[test]
fn remote_wake_before_the_loop_parks() {
    // The worker wakes main immediately, possibly before main gets to park.
    // The thread's unpark token must prevent a lost wake-up.
    for _ in 0..200 {
        let (tx, rx) = oneshot();
        let worker = thread::spawn(move || tx.send(()));
        spool::block_on(rx);
        worker.join().unwrap();
    }
}

#[test]
fn remote_wake_after_task_finished_is_ignored() {
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    spool::block_on(async {
        let s = stash.clone();
        spool::spawn(std::future::poll_fn(move |cx| {
            *s.lock().unwrap() = Some(cx.waker().clone());
            Poll::Ready(())
        }))
        .await;
        let w = stash.lock().unwrap().take().unwrap();
        thread::spawn(move || w.wake()).join().unwrap();
        // The stale id is absorbed and ignored.
        spool::yield_now().await;
    });
}

#[test]
fn remote_wake_after_runtime_dropped_is_ignored() {
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    {
        let rt = spool::Runtime::new();
        let s = stash.clone();
        drop(rt.spawn(std::future::poll_fn(move |cx| {
            *s.lock().unwrap() = Some(cx.waker().clone());
            Poll::<()>::Pending
        })));
        rt.block_on(spool::yield_now());
    }
    let w = stash.lock().unwrap().take().unwrap();
    thread::spawn(move || w.wake()).join().unwrap();
}

#[test]
fn waker_woken_on_runtime_thread_while_not_running() {
    // A waker from a runtime that isn't currently running, woken on its own
    // thread, takes the remote path and is delivered on the next block_on.
    let rt = spool::Runtime::new();
    let stash: std::rc::Rc<std::cell::RefCell<Option<Waker>>> = Default::default();
    let s = stash.clone();
    let mut woken = false;
    let h = rt.spawn(std::future::poll_fn(move |cx| {
        if woken {
            Poll::Ready(3)
        } else {
            woken = true;
            *s.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        }
    }));
    rt.block_on(spool::yield_now());
    stash.borrow_mut().take().unwrap().wake();
    assert_eq!(rt.block_on(h), 3);
}

#[test]
fn waker_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    spool::block_on(std::future::poll_fn(|cx| {
        assert_send_sync(cx.waker());
        Poll::Ready(())
    }));
}
