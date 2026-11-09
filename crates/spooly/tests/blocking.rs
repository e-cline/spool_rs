//! `spawn_blocking` and the blocking thread pool.
#![cfg(not(target_family = "wasm"))]

use std::cell::Cell;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use spooly::{MockClock, Runtime, spawn_blocking};

/// Sets its flag when dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn returns_the_result_from_another_thread() {
    let main = thread::current().id();
    let (value, worker) =
        spooly::block_on(async { spawn_blocking(|| (6 * 7, thread::current().id())).await });
    assert_eq!(value, 42);
    assert_ne!(worker, main);
}

#[test]
fn the_loop_runs_tasks_while_a_job_blocks() {
    let log = spooly::block_on(async {
        let (tx, rx) = mpsc::channel::<()>();
        let job = spawn_blocking(move || rx.recv().unwrap());
        // The job waits for this task, which only runs if the loop isn't
        // blocked on the job.
        let ticks = Rc::new(Cell::new(0));
        let t = ticks.clone();
        let task = spooly::spawn(async move {
            spooly::sleep(Duration::from_millis(5)).await;
            t.set(t.get() + 1);
            tx.send(()).unwrap();
        });
        job.await;
        task.await;
        ticks.get()
    });
    assert_eq!(log, 1);
}

#[test]
fn run_waits_for_a_detached_job() {
    let done = Arc::new(AtomicBool::new(false));
    let d = done.clone();
    spooly::run(async move {
        drop(spawn_blocking(move || {
            thread::sleep(Duration::from_millis(30));
            d.store(true, Ordering::SeqCst);
        }));
    });
    assert!(done.load(Ordering::SeqCst));
}

#[test]
fn block_on_does_not_wait_for_a_detached_job() {
    let (tx, rx) = mpsc::channel::<()>();
    let ran = Arc::new(AtomicBool::new(false));
    let r = ran.clone();
    spooly::block_on(async move {
        drop(spawn_blocking(move || {
            rx.recv().unwrap();
            r.store(true, Ordering::SeqCst);
        }));
    });
    assert!(!ran.load(Ordering::SeqCst));
    // `block_on`'s runtime is gone: the job was dropped unrun if it hadn't
    // started, or finishes detached once this lets it.
    let _ = tx.send(());
}

#[test]
fn a_panic_is_re_raised_in_the_awaiting_task() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        spooly::block_on(async { spawn_blocking(|| -> u32 { panic!("boom in the pool") }).await })
    }));
    let payload = result.unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom in the pool"));
}

#[test]
fn a_detached_panic_is_only_printed() {
    let after = Arc::new(AtomicBool::new(false));
    let a = after.clone();
    let rt = Runtime::builder().max_blocking_threads(1).build();
    rt.run(async move {
        drop(spawn_blocking(|| panic!("detached boom")));
        // The same (only) thread survives to run the next job.
        spawn_blocking(move || a.store(true, Ordering::SeqCst)).await;
    });
    assert!(after.load(Ordering::SeqCst));
}

#[test]
fn cancel_stops_a_queued_job_but_not_a_running_one() {
    let rt = Runtime::builder().max_blocking_threads(1).build();
    let dropped = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicBool::new(false));
    let (d, r) = (dropped.clone(), ran.clone());
    rt.run(async move {
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let running = spawn_blocking(move || {
            started_tx.send(()).unwrap();
            go_rx.recv().unwrap();
        });
        let flag = DropFlag(d);
        let queued = spawn_blocking(move || {
            let _flag = flag;
            r.store(true, Ordering::SeqCst);
        });
        assert!(!queued.is_finished());
        assert!(queued.cancel());
        started_rx.recv().unwrap();
        assert!(!running.is_finished());
        // Already running: cancel detaches it, and `run` still waits.
        assert!(!running.cancel());
        go_tx.send(()).unwrap();
    });
    assert!(!ran.load(Ordering::SeqCst));
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn is_finished_once_the_job_returns() {
    spooly::block_on(async {
        let job = spawn_blocking(|| 1);
        while !job.is_finished() {
            spooly::sleep(Duration::from_millis(1)).await;
        }
        assert!(format!("{job:?}").contains("Done"));
        assert_eq!(job.await, 1);
    });
}

#[test]
fn the_pool_never_exceeds_its_thread_cap() {
    const JOBS: usize = 12;
    let rt = Runtime::builder().max_blocking_threads(3).build();
    let (now, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let threads = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let (n, p, t) = (now.clone(), peak.clone(), threads.clone());
    let total = rt.block_on(async move {
        let jobs: Vec<_> = (0..JOBS)
            .map(|i| {
                let (n, p, t) = (n.clone(), p.clone(), t.clone());
                spawn_blocking(move || {
                    let running = n.fetch_add(1, Ordering::SeqCst) + 1;
                    p.fetch_max(running, Ordering::SeqCst);
                    t.lock().unwrap().insert(thread::current().id());
                    thread::sleep(Duration::from_millis(10));
                    n.fetch_sub(1, Ordering::SeqCst);
                    i
                })
            })
            .collect();
        let mut total = 0;
        for job in jobs {
            total += job.await;
        }
        total
    });
    assert_eq!(total, (0..JOBS).sum::<usize>());
    assert!(peak.load(Ordering::SeqCst) <= 3);
    assert!(threads.lock().unwrap().len() <= 3);
}

#[test]
fn many_jobs_complete() {
    // Fewer under Miri, where each thread is slow.
    const JOBS: u64 = if cfg!(miri) { 20 } else { 200 };
    let sum = spooly::block_on(async {
        let jobs: Vec<_> = (0..JOBS).map(|i| spawn_blocking(move || i * 2)).collect();
        let mut sum = 0;
        for job in jobs {
            sum += job.await;
        }
        sum
    });
    assert_eq!(sum, (0..JOBS).map(|i| i * 2).sum::<u64>());
}

#[test]
fn an_idle_thread_is_reused_then_exits() {
    let rt = Runtime::builder()
        .max_blocking_threads(1)
        .blocking_idle_timeout(Duration::from_millis(50))
        .build();
    let id = || rt.block_on(async { spawn_blocking(|| thread::current().id()).await });
    let first = id();
    // Within the idle timeout: the same thread takes the next job.
    assert_eq!(id(), first);
    // After it: that thread has exited, and a new one runs the job.
    thread::sleep(Duration::from_millis(300));
    assert_ne!(id(), first);
}

#[test]
fn dropping_the_runtime_drops_queued_jobs() {
    let dropped = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicBool::new(false));
    let (go_tx, go_rx) = mpsc::channel::<()>();
    {
        let rt = Runtime::builder().max_blocking_threads(1).build();
        let (d, r) = (dropped.clone(), ran.clone());
        rt.block_on(async move {
            let (started_tx, started_rx) = mpsc::channel::<()>();
            drop(spawn_blocking(move || {
                started_tx.send(()).unwrap();
                go_rx.recv().unwrap();
            }));
            started_rx.recv().unwrap();
            let flag = DropFlag(d);
            drop(spawn_blocking(move || {
                let _flag = flag;
                r.store(true, Ordering::SeqCst);
            }));
        });
    }
    // The runtime is gone: its queued job was dropped unrun.
    assert!(dropped.load(Ordering::SeqCst));
    go_tx.send(()).unwrap(); // The running job finishes on its own.
    thread::sleep(Duration::from_millis(20));
    assert!(!ran.load(Ordering::SeqCst));
}

#[test]
fn a_mock_clock_waits_for_a_running_job() {
    let clock = MockClock::new();
    let rt = Runtime::builder().clock(clock.clone()).build();
    let result = rt.block_on(async {
        spooly::timeout(
            Duration::from_millis(1),
            spawn_blocking(|| {
                thread::sleep(Duration::from_millis(50));
                7
            }),
        )
        .await
    });
    // Without the in-flight check, the clock would have jumped to the
    // timeout at once.
    assert_eq!(result, Ok(7));
    assert_eq!(clock.elapsed(), Duration::ZERO);
}

#[test]
fn a_mock_clock_jumps_once_the_job_is_done() {
    let clock = MockClock::new();
    let rt = Runtime::builder().clock(clock.clone()).build();
    let c = clock.clone();
    rt.run(async move {
        let sleeper = spooly::spawn(spooly::sleep(Duration::from_secs(60)));
        spawn_blocking(|| thread::sleep(Duration::from_millis(20))).await;
        // The job held the clock still for its 20ms of real time.
        assert_eq!(c.elapsed(), Duration::ZERO);
        sleeper.await;
    });
    assert_eq!(clock.elapsed(), Duration::from_secs(60));
}

#[test]
fn a_mock_clock_jumps_after_a_detached_job_despite_other_guards() {
    // Run on a thread, so that a lost wake fails the test instead of
    // hanging it for the timer's hour of real time.
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let clock = MockClock::new();
        let rt = Runtime::builder().clock(clock.clone()).build();
        rt.block_on(async {
            // Another guard: the job's own `KeepAlive` isn't the last one,
            // so dropping it wouldn't wake the loop.
            let keep = spooly::keep_alive();
            drop(spawn_blocking(|| thread::sleep(Duration::from_millis(20))));
            spooly::sleep(Duration::from_secs(3600)).await;
            drop(keep);
        });
        tx.send(clock.elapsed()).unwrap();
    });
    let elapsed = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the loop missed the job's end");
    assert_eq!(elapsed, Duration::from_secs(3600));
}

#[test]
#[should_panic(expected = "`spawn_blocking` called outside of a Spooly runtime")]
fn spawning_outside_a_runtime_panics() {
    drop(spawn_blocking(|| ()));
}

#[test]
#[should_panic(expected = "must be at least 1")]
fn a_zero_thread_cap_is_rejected() {
    drop(Runtime::builder().max_blocking_threads(0));
}

#[test]
fn the_builder_shows_the_pool_settings() {
    let builder = Runtime::builder().max_blocking_threads(5);
    assert!(format!("{builder:?}").contains("max_blocking_threads: 5"));
}
