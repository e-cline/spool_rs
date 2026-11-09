//! Shutting down in phases: stop accepting work, let in-flight work finish
//! within a grace period, then abort whatever is left.
//!
//! Three workers pull jobs from a queue. One job hangs forever. A thread that
//! stands in for a signal handler (a real program would use a crate such as
//! `ctrlc` or `signal-hook`) asks for shutdown after 110ms. Then:
//!
//! 1. Workers stop taking new jobs, but finish the one they are on.
//! 2. `main` gives them a 200ms grace period (`timeout_at` on a shared
//!    deadline).
//! 3. A worker that is still busy is aborted. Aborting drops its future, so
//!    its destructors run.
//!
//! What it uses: a cross-thread wake (`Waker`s are `Send`), `KeepAlive` so
//! that `run` waits for the signal thread, `AbortHandle`, and `timeout_at`.
//!
//! Run with: `cargo run --example graceful_shutdown`

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::thread;
use std::time::Duration;

const MS: Duration = Duration::from_millis(1);
const JOBS: u32 = 12;
const HANGING_JOB: u32 = 5;

// --- A shutdown signal that any thread can raise ------------------------------

#[derive(Clone, Default)]
struct Shutdown {
    requested: Arc<AtomicBool>,
    wakers: Arc<Mutex<Vec<Waker>>>,
}

impl Shutdown {
    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    /// Callable from any thread (a signal handler, say).
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap());
        wakers.into_iter().for_each(Waker::wake);
    }

    /// Completes once shutdown has been requested.
    async fn requested(&self) {
        std::future::poll_fn(|cx| {
            // Register before checking, so a request in between isn't missed.
            self.wakers.lock().unwrap().push(cx.waker().clone());
            if self.is_requested() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await
    }
}

// --- The workers -------------------------------------------------------------------

/// Prints when the worker's future is dropped, whether it finished or was
/// aborted: this is where a real worker would flush or release resources.
struct Cleanup(u32);

impl Drop for Cleanup {
    fn drop(&mut self) {
        println!("  worker {}: cleaned up", self.0);
    }
}

#[derive(Default)]
struct Jobs {
    queue: VecDeque<u32>,
    done: Vec<u32>,
}

async fn worker(id: u32, jobs: Rc<RefCell<Jobs>>, shutdown: Shutdown) {
    let _cleanup = Cleanup(id);
    // Phase 1: take no new jobs once shutdown is requested.
    while !shutdown.is_requested() {
        let Some(job) = jobs.borrow_mut().queue.pop_front() else {
            return;
        };
        println!("  worker {id}: starting job {job}");
        if job == HANGING_JOB {
            std::future::pending::<()>().await; // Never finishes.
        }
        spooly::sleep(30 * MS).await;
        jobs.borrow_mut().done.push(job);
        println!("  worker {id}: finished job {job}");
    }
    println!("  worker {id}: shutdown requested, stopping");
}

fn main() {
    let jobs = Rc::new(RefCell::new(Jobs {
        queue: (1..=JOBS).collect(),
        done: Vec::new(),
    }));
    let shutdown = Shutdown::default();

    spooly::run(async {
        // The "signal handler". `run` would exit as soon as it had no work,
        // since nothing it knows about is waiting for this thread; the
        // `KeepAlive` makes it wait.
        let keep = spooly::keep_alive();
        let signal = shutdown.clone();
        thread::spawn(move || {
            thread::sleep(110 * MS);
            println!("-- signal: shutdown requested --");
            signal.request();
            drop(keep); // After the wake, so the loop sees it first.
        });

        println!("starting 3 workers on {JOBS} jobs");
        let workers: Vec<_> = (0..3)
            .map(|id| spooly::spawn(worker(id, jobs.clone(), shutdown.clone())))
            .collect();
        let aborts: Vec<_> = workers.iter().map(|w| w.abort_handle()).collect();

        shutdown.requested().await;

        // Phase 2: one deadline for everyone, not a fresh timeout per worker.
        let deadline = spooly::now() + 200 * MS;
        let mut aborted = 0;
        for (worker, abort) in workers.into_iter().zip(aborts) {
            if spooly::timeout_at(deadline, worker).await.is_err() {
                // Phase 3: out of time. Dropping the future (via abort) runs
                // its destructors.
                println!("-- grace period over: aborting a worker --");
                abort.abort();
                aborted += 1;
            }
        }

        let jobs = jobs.borrow();
        let not_started = jobs.queue.len();
        println!(
            "finished {} jobs, left {not_started} in the queue, aborted {aborted} worker",
            jobs.done.len(),
        );
        // Every job is accounted for: done, never started, or lost in the
        // hung worker.
        assert_eq!(aborted, 1);
        assert_eq!(jobs.done.len() + not_started + 1, JOBS as usize);
    });
    println!("exited cleanly");
}
