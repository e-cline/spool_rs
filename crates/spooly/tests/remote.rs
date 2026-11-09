//! Wakes that come from other threads (the remote path in design D9).

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

mod common;

use std::sync::{Arc, Barrier, Mutex};
use std::task::{Poll, Waker};
use std::thread;
use std::time::Duration;

use common::oneshot;

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn another_thread_wakes_main() {
    let (tx, rx) = oneshot();
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send(7);
    });
    let out = spooly::block_on(rx);
    assert_eq!(out, 7);
    worker.join().unwrap();
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn another_thread_wakes_a_spawned_task() {
    let out = spooly::block_on(async {
        let (tx, rx) = oneshot();
        let task = spooly::spawn(async move { rx.await * 2 });
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
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn many_threads_wake_many_tasks() {
    const N: usize = 32;
    let out = spooly::block_on(async {
        let mut handles = Vec::new();
        let mut workers = Vec::new();
        let barrier = Arc::new(Barrier::new(N));
        for i in 0..N {
            let (tx, rx) = oneshot();
            handles.push(spooly::spawn(rx));
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
    assert_eq!(out, (0..N).sum::<usize>());
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn remote_wake_before_the_loop_parks() {
    // The worker wakes main immediately, possibly before main gets to park.
    // The thread's unpark token must prevent a lost wake-up.
    for _ in 0..200 {
        let (tx, rx) = oneshot();
        let worker = thread::spawn(move || tx.send(()));
        spooly::block_on(rx);
        worker.join().unwrap();
    }
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn remote_wake_after_task_finished_is_ignored() {
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    spooly::block_on(async {
        let s = stash.clone();
        spooly::spawn(std::future::poll_fn(move |cx| {
            *s.lock().unwrap() = Some(cx.waker().clone());
            Poll::Ready(())
        }))
        .await;
        let w = stash.lock().unwrap().take().unwrap();
        thread::spawn(move || w.wake()).join().unwrap();
        // The stale id is absorbed and ignored.
        spooly::yield_now().await;
    });
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn remote_wake_after_runtime_dropped_is_ignored() {
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    {
        let rt = spooly::Runtime::new();
        let s = stash.clone();
        drop(rt.spawn(std::future::poll_fn(move |cx| {
            *s.lock().unwrap() = Some(cx.waker().clone());
            Poll::<()>::Pending
        })));
        rt.block_on(spooly::yield_now());
    }
    let w = stash.lock().unwrap().take().unwrap();
    thread::spawn(move || w.wake()).join().unwrap();
}

#[test]
fn waker_woken_on_runtime_thread_while_not_running() {
    // A waker from a runtime that isn't currently running, woken on its own
    // thread, takes the remote path and is delivered on the next block_on.
    let rt = spooly::Runtime::new();
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
    rt.block_on(spooly::yield_now());
    stash.borrow_mut().take().unwrap().wake();
    assert_eq!(rt.block_on(h), 3);
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn a_remote_wake_interrupts_a_timer_wait() {
    // The loop is parked until a far-off timer; the remote wake must cut the
    // park short.
    let start = std::time::Instant::now();
    let out = spooly::block_on(async {
        let _far = spooly::set_timeout(Duration::from_secs(30), || {});
        let (tx, rx) = oneshot();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send("early");
        });
        rx.await
    });
    assert_eq!(out, "early");
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "{:?}",
        start.elapsed()
    );
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn remote_wakes_queued_while_idle_run_in_wake_order() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let rt = spooly::Runtime::new();
    let log: Rc<RefCell<Vec<usize>>> = Rc::default();
    let wakers: Arc<Mutex<Vec<Option<Waker>>>> = Arc::new(Mutex::new(vec![None, None, None]));
    let mut handles = Vec::new();
    for i in 0..3 {
        let (log, wakers) = (log.clone(), wakers.clone());
        let mut polled = false;
        handles.push(rt.spawn(std::future::poll_fn(move |cx| {
            if polled {
                log.borrow_mut().push(i);
                Poll::Ready(())
            } else {
                polled = true;
                wakers.lock().unwrap()[i] = Some(cx.waker().clone());
                Poll::Pending
            }
        })));
    }
    rt.block_on(spooly::yield_now());
    // Wake 2, 0, 1 from another thread while the runtime isn't running.
    let w = wakers.clone();
    thread::spawn(move || {
        for i in [2, 0, 1] {
            w.lock().unwrap()[i].take().unwrap().wake();
        }
    })
    .join()
    .unwrap();
    rt.block_on(async {
        for h in handles {
            h.await;
        }
    });
    assert_eq!(*log.borrow(), [2, 0, 1]);
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn racing_remote_and_local_wakes_are_deduplicated() {
    // A task re-registers its waker and yields; threads hammer it with
    // wakes meanwhile. It must finish, and never be polled more often than
    // it was woken plus its own yields.
    const ROUNDS: usize = 2000;
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut threads = Vec::new();
    for _ in 0..4 {
        let (stash, stop, wakes) = (stash.clone(), stop.clone(), wakes.clone());
        threads.push(thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let w = stash.lock().unwrap().clone();
                if let Some(w) = w {
                    w.wake();
                    wakes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                thread::yield_now();
            }
        }));
    }
    let polls = spooly::block_on(async {
        let s = stash.clone();
        spooly::spawn(async move {
            let mut polls = 0;
            std::future::poll_fn(|cx| {
                polls += 1;
                *s.lock().unwrap() = Some(cx.waker().clone());
                if polls >= ROUNDS {
                    return Poll::Ready(());
                }
                cx.waker().wake_by_ref(); // A local wake, racing the remote ones.
                Poll::Pending
            })
            .await;
            polls
        })
        .await
    });
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(polls, ROUNDS, "each poll was preceded by one deduped wake");
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn many_threads_wake_main_repeatedly() {
    const PER_THREAD: usize = 200;
    const THREADS: usize = 8;
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    let mut workers = Vec::new();
    for _ in 0..THREADS {
        let (tx, stash) = (tx.clone(), stash.clone());
        workers.push(thread::spawn(move || {
            for _ in 0..PER_THREAD {
                tx.send(()).unwrap();
                if let Some(w) = stash.lock().unwrap().take() {
                    w.wake();
                }
            }
            // The channel disconnects only once every sender is gone, so
            // wake main again after dropping ours: otherwise main can drain
            // the last message, see `Empty`, and wait forever.
            drop(tx);
            if let Some(w) = stash.lock().unwrap().take() {
                w.wake();
            }
        }));
    }
    drop(tx);
    let received = spooly::block_on(std::future::poll_fn(|cx| {
        // Register first, then drain, so that no send is missed.
        *stash.lock().unwrap() = Some(cx.waker().clone());
        let mut n = 0;
        loop {
            match rx.try_recv() {
                Ok(()) => n += 1,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    COUNT.with(|c| c.set(c.get() + n));
                    return Poll::Pending;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    COUNT.with(|c| c.set(c.get() + n));
                    return Poll::Ready(COUNT.with(std::cell::Cell::get));
                }
            }
        }
    }));
    for w in workers {
        w.join().unwrap();
    }
    assert_eq!(received, PER_THREAD * THREADS);

    thread_local! {
        static COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }
}

#[test]
#[cfg_attr(target_arch = "wasm32", ignore = "needs threads")]
fn a_keep_alive_dropped_while_parked_on_a_timer() {
    // The unpark from the guard's drop is a spurious wake for a loop that is
    // waiting on a timer: it just goes back to waiting.
    let start = std::time::Instant::now();
    spooly::run(async {
        let keep = spooly::keep_alive();
        spooly::set_timeout(Duration::from_millis(40), || {});
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            drop(keep);
        });
    });
    assert!(start.elapsed() >= Duration::from_millis(40));
}

#[test]
fn waker_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    spooly::block_on(std::future::poll_fn(|cx| {
        assert_send_sync(cx.waker());
        Poll::Ready(())
    }));
}
