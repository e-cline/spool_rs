//! Stress tests (plan §8.1): large task counts, many timers with random
//! cancellation, very long tick/microtask/join chains, and heavy cross-thread
//! waking. They check that nothing overflows the stack, nothing is lost, and
//! time grows roughly linearly.
//!
//! They are `#[ignore]`d by default; CI runs them weekly with:
//!
//! ```sh
//! cargo test --release --test stress -- --ignored
//! ```

mod common;

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// A tiny deterministic PRNG (xorshift64*), to keep the tests dependency-free
/// and reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn mock() -> spool::Runtime {
    spool::Runtime::builder()
        .clock(spool::MockClock::new())
        .build()
}

fn timed(f: impl FnOnce()) -> Duration {
    let start = Instant::now();
    f();
    start.elapsed()
}

#[test]
#[ignore = "stress test"]
fn a_million_spawned_tasks() {
    const N: usize = 1_000_000;
    let done = Rc::new(Cell::new(0));
    let d = done.clone();
    spool::run(async move {
        for _ in 0..N {
            let d = d.clone();
            drop(spool::spawn(async move { d.set(d.get() + 1) }));
        }
    });
    assert_eq!(done.get(), N);
}

#[test]
#[ignore = "stress test"]
fn a_million_joined_tasks() {
    const N: u64 = 1_000_000;
    let sum = spool::block_on(async {
        let handles: Vec<_> = (0..N).map(|i| spool::spawn(async move { i })).collect();
        let mut sum = 0;
        for h in handles {
            sum += h.await;
        }
        sum
    });
    assert_eq!(sum, N * (N - 1) / 2);
}

#[test]
#[ignore = "stress test"]
fn many_tasks_yielding_many_times() {
    const TASKS: usize = 10_000;
    const YIELDS: usize = 100;
    let polls = Rc::new(Cell::new(0));
    let p = polls.clone();
    spool::run(async move {
        for _ in 0..TASKS {
            let p = p.clone();
            drop(spool::spawn(async move {
                for _ in 0..YIELDS {
                    p.set(p.get() + 1);
                    spool::yield_now().await;
                }
            }));
        }
    });
    assert_eq!(polls.get(), TASKS * YIELDS);
}

#[test]
#[ignore = "stress test"]
fn a_hundred_thousand_timers_with_random_cancellation() {
    const N: usize = 100_000;
    let rt = mock();
    let mut rng = Rng(0x5EED);
    // (deadline ms, index) of each timer that fired, in firing order.
    let fired: Rc<RefCell<Vec<(u64, usize)>>> = Rc::default();
    let mut expected = 0;
    rt.run(async {
        let mut handles = Vec::with_capacity(N);
        for i in 0..N {
            let ms = rng.below(10_000);
            let f = fired.clone();
            handles.push(spool::set_timeout(Duration::from_millis(ms), move || {
                f.borrow_mut().push((ms, i));
            }));
        }
        for h in &handles {
            if rng.below(3) == 0 {
                h.cancel();
            } else {
                expected += 1;
            }
        }
    });
    let fired = fired.borrow();
    assert_eq!(fired.len(), expected, "every live timer fired exactly once");
    // Deadline order, and creation order among equal deadlines.
    assert!(
        fired.windows(2).all(|w| w[0] <= w[1]),
        "timers fired out of order"
    );
}

#[test]
#[ignore = "stress test"]
fn timers_cancelled_while_their_phase_runs() {
    // Every timer is due at the same instant; each one cancels a random
    // other one. Cancelled timers must not fire, whether or not the phase
    // had already reached them.
    const N: usize = 50_000;
    let rt = mock();
    let handles: Rc<RefCell<Vec<spool::TimerHandle>>> = Rc::default();
    let fired = Rc::new(Cell::new(0));
    let cancelled_before_firing = Rc::new(Cell::new(0));
    rt.run(async {
        let rng = Rc::new(RefCell::new(Rng(42)));
        for _ in 0..N {
            let (all, fired, rng, cbf) = (
                handles.clone(),
                fired.clone(),
                rng.clone(),
                cancelled_before_firing.clone(),
            );
            let h = spool::set_timeout(Duration::from_millis(5), move || {
                fired.set(fired.get() + 1);
                let victim = rng.borrow_mut().below(N as u64) as usize;
                let victim = all.borrow()[victim].clone();
                if victim.is_active() {
                    cbf.set(cbf.get() + 1);
                    victim.cancel();
                }
            });
            handles.borrow_mut().push(h);
        }
    });
    assert_eq!(fired.get() + cancelled_before_firing.get(), N);
}

#[test]
#[ignore = "stress test"]
fn ten_thousand_intervals() {
    const N: usize = 10_000;
    const TICKS: usize = 10;
    let rt = mock();
    let total = Rc::new(Cell::new(0));
    rt.run(async {
        for i in 0..N {
            let total = total.clone();
            let mut n = 0;
            let period = Duration::from_millis(1 + (i % 7) as u64);
            spool::set_interval(period, move |me| {
                total.set(total.get() + 1);
                n += 1;
                if n == TICKS {
                    me.cancel();
                }
            });
        }
    });
    assert_eq!(total.get(), N * TICKS);
}

#[test]
#[ignore = "stress test"]
fn a_million_chained_ticks_and_microtasks() {
    // Each callback queues the next: the queues must not recurse.
    const N: usize = 1_000_000;
    fn chain_tick(left: usize, count: Rc<Cell<usize>>) {
        count.set(count.get() + 1);
        if left > 0 {
            spool::next_tick(move || chain_tick(left - 1, count));
        }
    }
    fn chain_micro(left: usize, count: Rc<Cell<usize>>) {
        count.set(count.get() + 1);
        if left > 0 {
            spool::queue_microtask(move || chain_micro(left - 1, count));
        }
    }
    let ticks = Rc::new(Cell::new(0));
    let micros = Rc::new(Cell::new(0));
    let (t, m) = (ticks.clone(), micros.clone());
    spool::run(async move {
        spool::next_tick(move || chain_tick(N - 1, t));
        spool::queue_microtask(move || chain_micro(N - 1, m));
    });
    assert_eq!((ticks.get(), micros.get()), (N, N));
}

#[test]
#[ignore = "stress test"]
fn a_hundred_thousand_deep_join_chain() {
    // Task i awaits task i - 1. Completion ripples through the chain one
    // microtask at a time, never recursively.
    const N: u64 = 100_000;
    let out = spool::block_on(async {
        let mut prev = spool::spawn(async { 0_u64 });
        for _ in 0..N {
            let p = prev;
            prev = spool::spawn(async move { p.await + 1 });
        }
        prev.await
    });
    assert_eq!(out, N);
}

#[test]
#[ignore = "stress test"]
fn a_hundred_thousand_aborts() {
    const N: usize = 100_000;
    let dropped = common::DropCounter::new();
    let d = dropped.clone();
    spool::block_on(async move {
        let handles: Vec<_> = (0..N)
            .map(|_| {
                let tok = d.token();
                spool::spawn(async move {
                    let _tok = tok;
                    std::future::pending::<()>().await;
                })
            })
            .collect();
        spool::yield_now().await; // Every task is now pending.
        for h in &handles {
            h.abort();
        }
        assert_eq!(d.count(), N);
    });
}

#[test]
#[ignore = "stress test"]
fn heavy_cross_thread_waking() {
    // 8 threads each complete 10k one-shot results for 10k tasks.
    const THREADS: usize = 8;
    const PER_THREAD: usize = 10_000;
    let received = Arc::new(AtomicUsize::new(0));
    let sum = spool::block_on(async {
        let mut handles = Vec::new();
        let mut senders = Vec::new();
        for _ in 0..THREADS * PER_THREAD {
            let (tx, rx) = common::oneshot::<usize>();
            senders.push(tx);
            handles.push(spool::spawn(rx));
        }
        let mut workers = Vec::new();
        let mut senders = senders.into_iter();
        for _ in 0..THREADS {
            let batch: Vec<_> = senders.by_ref().take(PER_THREAD).collect();
            let received = received.clone();
            workers.push(std::thread::spawn(move || {
                for (j, tx) in batch.into_iter().enumerate() {
                    tx.send(j);
                    received.fetch_add(1, Ordering::Relaxed);
                }
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
    assert_eq!(received.load(Ordering::Relaxed), THREADS * PER_THREAD);
    assert_eq!(sum, THREADS * (PER_THREAD * (PER_THREAD - 1) / 2));
}

#[test]
#[ignore = "stress test"]
fn time_grows_roughly_linearly() {
    // 4x the work should take well under 16x the time (it is O(n), not
    // O(n²)). The bound is generous, since CI machines are noisy.
    fn workload(n: usize) {
        let rt = mock();
        rt.run(async move {
            for i in 0..n {
                drop(spool::spawn(async move {
                    spool::yield_now().await;
                    spool::sleep(Duration::from_millis((i % 100) as u64)).await;
                }));
            }
        });
    }
    workload(10_000); // Warm up.
    let small = timed(|| workload(50_000));
    let large = timed(|| workload(200_000));
    let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-6);
    eprintln!("50k: {small:?}, 200k: {large:?}, ratio {ratio:.1}");
    assert!(ratio < 12.0, "4x the work took {ratio:.1}x the time");
}
