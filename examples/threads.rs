//! Running blocking work on other threads, and waking `!Send` tasks when it
//! is done. Wakers are `Send`, so a thread can wake a task; the task itself
//! never leaves the runtime's thread.
//!
//! Run with: `cargo run --example threads`

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

/// A result slot shared with a worker thread.
type Slot<T> = Arc<Mutex<(Option<T>, Option<Waker>)>>;

/// Runs `f` on a new thread, and returns a future for its result.
fn on_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> ThreadResult<T> {
    let slot: Slot<T> = Arc::default();
    let worker = slot.clone();
    thread::spawn(move || {
        let value = f();
        let waker = {
            let mut s = worker.lock().unwrap();
            s.0 = Some(value);
            s.1.take()
        };
        if let Some(waker) = waker {
            waker.wake(); // Queued for the runtime's thread.
        }
    });
    ThreadResult(slot)
}

struct ThreadResult<T>(Slot<T>);

impl<T> Future for ThreadResult<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut s = self.0.lock().unwrap();
        match s.0.take() {
            Some(value) => Poll::Ready(value),
            None => {
                s.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

fn slow_square(n: u64) -> u64 {
    thread::sleep(Duration::from_millis(30));
    n * n
}

fn main() {
    // Shared, `!Send` state: tasks write to it after each thread finishes.
    let results = Rc::new(RefCell::new(Vec::new()));

    let r = results.clone();
    spool::run(async move {
        // Await the threads from `main`: the loop parks while they work.
        let (a, b) = (on_thread(|| slow_square(3)), on_thread(|| slow_square(4)));
        println!("3² + 4² = {}", a.await + b.await);

        // Detached tasks waiting on threads don't keep `run` alive by
        // themselves. A `KeepAlive` per task does.
        for n in 5..8 {
            let keep = spool::keep_alive();
            let r = r.clone();
            drop(spool::spawn(async move {
                let squared = on_thread(move || slow_square(n)).await;
                r.borrow_mut().push((n, squared));
                drop(keep);
            }));
        }
        println!("main is done; `run` waits for the kept-alive tasks");
    });

    let mut results = results.take();
    results.sort();
    println!("detached results: {results:?}");
}
