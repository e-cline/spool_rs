//! `spawn_blocking`: moving blocking and CPU-heavy work off the event loop.
//!
//! A heartbeat task ticks every 5ms. While a long computation runs *on the
//! loop*, the heartbeat stops. Run it on the blocking pool instead, and the
//! heartbeat keeps going while the pool's threads do the work, in parallel.
//!
//! Also shown: results flow back as ordinary values (the closure and its
//! result must be `Send`; the task awaiting them need not be), a runtime
//! configured with a small pool (`Builder::max_blocking_threads`), and
//! cancelling a job that is still queued.
//!
//! `spawn_blocking` isn't available on WASM targets, which have no threads.
//!
//! Run with: `cargo run --example blocking_pool`

// `spawn_blocking` doesn't exist on WASM targets, but CI compiles every
// example for them, so the program is gated and WASM gets a stub `main`.
#[cfg(not(target_family = "wasm"))]
fn main() {
    pool::main();
}

#[cfg(target_family = "wasm")]
fn main() {
    println!("spawn_blocking is not available on WASM targets");
}

#[cfg(not(target_family = "wasm"))]
mod pool {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::time::Duration;

    const MS: Duration = Duration::from_millis(1);

    /// The number in `from..to` with the longest Collatz sequence: pure CPU work.
    fn longest_collatz(from: u64, to: u64) -> (u64, u32) {
        (from..to)
            .map(|start| {
                let (mut n, mut steps) = (start, 0);
                while n != 1 {
                    n = if n % 2 == 0 { n / 2 } else { 3 * n + 1 };
                    steps += 1;
                }
                (start, steps)
            })
            .max_by_key(|&(_, steps)| steps)
            .unwrap()
    }

    /// Ticks every 5ms, counting into `ticks`, until it is aborted.
    fn heartbeat(ticks: &Rc<Cell<u32>>) -> spooly::AbortOnDrop<()> {
        let ticks = ticks.clone();
        spooly::spawn(async move {
            let mut interval = spooly::interval(5 * MS);
            loop {
                interval.tick().await;
                ticks.set(ticks.get() + 1);
            }
        })
        .abort_on_drop()
    }

    const CHUNK: u64 = 800_000;

    pub fn main() {
        let rt = spooly::Runtime::builder().max_blocking_threads(4).build();

        rt.block_on(async {
            let ticks = Rc::new(Cell::new(0));
            let _heartbeat = heartbeat(&ticks);
            spooly::sleep(20 * MS).await; // Let it get going.

            // 1. The computation on the loop: nothing else runs until it's done.
            let before = ticks.get();
            let start = std::time::Instant::now();
            let inline = longest_collatz(1, CHUNK);
            let inline_time = start.elapsed();
            spooly::yield_now().await; // Let the heartbeat catch up, if it can.
            let inline_ticks = ticks.get() - before;
            println!("on the loop:   {inline:?} in {inline_time:?}, heartbeat ticks: {inline_ticks}");

            // 2. Four chunks on the pool: parallel, and the loop stays free.
            let before = ticks.get();
            let start = std::time::Instant::now();
            let jobs: Vec<_> = (0..4)
                .map(|i| {
                    let (from, to) = (1 + i * CHUNK, 1 + (i + 1) * CHUNK);
                    spooly::spawn_blocking(move || longest_collatz(from, to))
                })
                .collect();
            let mut best = (0, 0);
            for job in jobs {
                let (n, steps) = job.await;
                if steps > best.1 {
                    best = (n, steps);
                }
            }
            let pool_time = start.elapsed();
            let pool_ticks = ticks.get() - before;
            println!(
                "on the pool:   4x the work: {best:?} in {pool_time:?}, heartbeat ticks: {pool_ticks}"
            );
            // The first chunk's answer is the same wherever it ran.
            assert_eq!(
                inline,
                spooly::spawn_blocking(|| longest_collatz(1, CHUNK)).await
            );

            // The loop was blocked for the first run and free for the second.
            assert!(inline_ticks <= 1, "the heartbeat should have stalled");
            assert!(pool_ticks >= 3, "the heartbeat should have kept going");

            // 3. Blocking I/O belongs on the pool too. Results land in `Rc` state.
            let seen = Rc::new(RefCell::new(Vec::new()));
            let text = spooly::spawn_blocking(|| std::fs::read_to_string("Cargo.toml")).await;
            seen.borrow_mut()
                .push(text.map(|t| t.lines().count()).unwrap_or(0));
            println!("read Cargo.toml on the pool: {} lines", seen.borrow()[0]);
        });

        // 4. A pool of one thread: a second job has to wait for the first, and
        //    can be cancelled while it waits. (A running job can't be stopped.)
        let rt = spooly::Runtime::builder().max_blocking_threads(1).build();
        rt.block_on(async {
            let (release, wait) = std::sync::mpsc::channel::<()>();
            let running = spooly::spawn_blocking(move || {
                wait.recv().unwrap(); // Occupies the only thread.
                "first job"
            });
            let queued = spooly::spawn_blocking(|| unreachable!("cancelled before it started"));
            println!("queued job cancelled before starting: {}", queued.cancel());
            release.send(()).unwrap();
            println!("{}", running.await);
        });
    }
}
