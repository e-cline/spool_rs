//! Throughput benchmarks: spawning, waking, the microtask queue, and timers.
//! They give the planned unsafe-performance phase (plan §9.6) a baseline to
//! beat.
//!
//! A small built-in harness (no criterion): each benchmark runs a batch of
//! `n` operations several times, and reports the median and best time per
//! operation.
//!
//! ```sh
//! cargo bench                  # everything
//! cargo bench -- timer         # only benchmarks whose name contains "timer"
//! ```
//!
//! Under `cargo test --all-targets` (no `--bench` flag), every benchmark runs
//! once with a tiny batch, as a smoke test.

use std::cell::Cell;
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

struct Bench {
    /// Run for real (`cargo bench`), or as a quick smoke test.
    full: bool,
    filters: Vec<String>,
}

impl Bench {
    fn from_args() -> Self {
        let mut full = false;
        let mut filters = Vec::new();
        for arg in std::env::args().skip(1) {
            match arg.as_str() {
                "--bench" => full = true,
                a if a.starts_with('-') => {}
                a => filters.push(a.to_owned()),
            }
        }
        Bench { full, filters }
    }

    /// Times `f(n)`, which performs `n` operations, and prints the result.
    fn run(&self, name: &str, n: usize, mut f: impl FnMut(usize)) {
        if !self.filters.is_empty() && !self.filters.iter().any(|p| name.contains(p.as_str())) {
            return;
        }
        if !self.full {
            f(10);
            println!("{name}: ok");
            return;
        }
        const SAMPLES: usize = 11;
        f(n / 10); // Warm up.
        let mut per_op: Vec<f64> = (0..SAMPLES)
            .map(|_| {
                let start = Instant::now();
                f(n);
                start.elapsed().as_nanos() as f64 / n as f64
            })
            .collect();
        per_op.sort_by(f64::total_cmp);
        let median = per_op[SAMPLES / 2];
        println!(
            "{name:<34} {median:>9.1} ns/op  (best {:>7.1})  {:>7.2} M ops/s",
            per_op[0],
            1e3 / median,
        );
    }
}

fn mock() -> spooly::Runtime {
    spooly::Runtime::builder()
        .clock(spooly::MockClock::new())
        .build()
}

/// A flag set from another thread, that wakes the task waiting on it.
#[derive(Clone, Default)]
struct Flag(Arc<(AtomicBool, Mutex<Option<Waker>>)>);

impl Flag {
    fn set(&self) {
        self.0.0.store(true, Ordering::Release);
        if let Some(waker) = self.0.1.lock().unwrap().take() {
            waker.wake();
        }
    }
}

impl Future for Flag {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.0.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *self.0.1.lock().unwrap() = Some(cx.waker().clone());
        if self.0.0.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

fn main() {
    let b = Bench::from_args();
    if b.full {
        println!(
            "{:<34} {:>15}  {:>14}  {:>14}",
            "benchmark", "median", "best", "throughput"
        );
    }

    // --- Spawning ---------------------------------------------------------

    b.run("spawn/detached", 1_000_000, |n| {
        let rt = spooly::Runtime::new();
        rt.block_on(async {
            for i in 0..n {
                drop(spooly::spawn(async move {
                    black_box(i);
                }));
            }
            spooly::yield_now().await;
        });
    });

    b.run("spawn/join", 1_000_000, |n| {
        let rt = spooly::Runtime::new();
        rt.block_on(async {
            let handles: Vec<_> = (0..n).map(|i| spooly::spawn(async move { i })).collect();
            for h in handles {
                black_box(h.await);
            }
        });
    });

    b.run("spawn/block_on (new runtime each)", 100_000, |n| {
        for i in 0..n {
            black_box(spooly::block_on(async move { i }));
        }
    });

    // --- Waking -----------------------------------------------------------

    b.run("wake/yield_now (one task)", 1_000_000, |n| {
        spooly::Runtime::new().block_on(async {
            for _ in 0..n {
                spooly::yield_now().await;
            }
        });
    });

    b.run("wake/yield_now (1k tasks)", 1_000_000, |n| {
        spooly::Runtime::new().block_on(async {
            let handles: Vec<_> = (0..1_000)
                .map(|_| {
                    spooly::spawn(async move {
                        for _ in 0..n / 1_000 {
                            spooly::yield_now().await;
                        }
                    })
                })
                .collect();
            for h in handles {
                h.await;
            }
        });
    });

    b.run("wake/remote (from another thread)", 100_000, |n| {
        spooly::Runtime::new().block_on(async {
            let flags: Vec<Flag> = (0..n).map(|_| Flag::default()).collect();
            let handles: Vec<_> = flags.iter().cloned().map(spooly::spawn).collect();
            spooly::yield_now().await; // Every task is now waiting on its flag.
            let setter = std::thread::spawn(move || flags.iter().for_each(Flag::set));
            for h in handles {
                h.await;
            }
            setter.join().unwrap();
        });
    });

    // --- Queues -----------------------------------------------------------

    b.run("queue/queue_microtask", 1_000_000, |n| {
        let count = Rc::new(Cell::new(0));
        spooly::Runtime::new().block_on(async {
            for _ in 0..n {
                let count = count.clone();
                spooly::queue_microtask(move || count.set(count.get() + 1));
            }
            spooly::yield_now().await;
        });
        assert_eq!(count.get(), n);
    });

    // --- Timers (on a MockClock, so no real waiting) ------------------------

    b.run("timer/set_timeout + fire", 1_000_000, |n| {
        let count = Rc::new(Cell::new(0));
        mock().run(async {
            for i in 0..n {
                let count = count.clone();
                let delay = Duration::from_millis((i % 1_000) as u64);
                spooly::set_timeout(delay, move || count.set(count.get() + 1));
            }
        });
        assert_eq!(count.get(), n);
    });

    b.run("timer/set_timeout + cancel", 1_000_000, |n| {
        mock().run(async {
            for i in 0..n {
                let delay = Duration::from_millis((i % 1_000) as u64);
                spooly::set_timeout(delay, || unreachable!()).cancel();
            }
        });
    });

    b.run("timer/sleep (1k tasks)", 1_000_000, |n| {
        mock().run(async {
            for t in 0..1_000_u64 {
                drop(spooly::spawn(async move {
                    for _ in 0..n / 1_000 {
                        spooly::sleep(Duration::from_millis(1 + t % 10)).await;
                    }
                }));
            }
        });
    });

    b.run("timer/interval ticks", 1_000_000, |n| {
        mock().block_on(async {
            let mut every = spooly::interval(Duration::from_millis(1));
            for _ in 0..n {
                every.tick().await;
            }
        });
    });

    b.run("timer/timeout (completes first)", 1_000_000, |n| {
        spooly::Runtime::new().block_on(async {
            for i in 0..n {
                let out = spooly::timeout(Duration::from_secs(1), async move { i }).await;
                black_box(out.unwrap());
            }
        });
    });
}
