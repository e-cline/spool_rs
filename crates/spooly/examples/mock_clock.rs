//! Deterministic time: a `MockClock` makes timers instant, so code that waits
//! for minutes or hours can be tested in microseconds, and always in the same
//! order.
//!
//! The scenario is a retry loop with exponential backoff and a per-attempt
//! timeout: the kind of code that is painful to test against a real clock.
//!
//! Run with: `cargo run --example mock_clock`

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

const SECOND: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq)]
enum Failure {
    /// Every attempt failed or timed out.
    GaveUp { attempts: u32 },
}

/// Calls `op` until it succeeds. Each attempt gets 5 seconds; after a failed
/// one, waits 1s, 2s, 4s, ... before the next. Gives up after `max_attempts`.
async fn retry<T, Fut>(max_attempts: u32, mut op: impl FnMut(u32) -> Fut) -> Result<T, Failure>
where
    Fut: Future<Output = Result<T, ()>>,
{
    let mut backoff = SECOND;
    for attempt in 1..=max_attempts {
        // `Ok(Ok(_))`: success. `Ok(Err(_))`: the attempt failed. `Err(_)`: it
        // timed out, and `timeout` dropped it.
        if let Ok(Ok(value)) = spooly::timeout(5 * SECOND, op(attempt)).await {
            return Ok(value);
        }
        if attempt < max_attempts {
            spooly::sleep(backoff).await;
            backoff *= 2;
        }
    }
    Err(Failure::GaveUp {
        attempts: max_attempts,
    })
}

/// A fake service. Attempt 1 fails at once, attempt 2 hangs, attempt 3 is
/// slow but works.
async fn flaky_service(attempt: u32) -> Result<&'static str, ()> {
    match attempt {
        1 => Err(()),
        2 => std::future::pending().await,
        _ => {
            spooly::sleep(2 * SECOND).await;
            Ok("payload")
        }
    }
}

fn main() {
    let clock = spooly::MockClock::new();
    let rt = spooly::Runtime::builder().clock(clock.clone()).build();
    let wall = std::time::Instant::now();

    // 1. Auto-advance. Whenever the loop would sleep until the next timer,
    //    the clock jumps there instead.
    let result = rt.block_on(retry(5, flaky_service));
    println!("retry result: {result:?}");
    // 0s: attempt 1 fails. +1s backoff. Attempt 2 hangs until its 5s timeout
    // (t=6s). +2s backoff (t=8s). Attempt 3 takes 2s (t=10s).
    assert_eq!(result, Ok("payload"));
    assert_eq!(clock.elapsed(), 10 * SECOND);
    println!("virtual time: {:?}", clock.elapsed());

    // 2. Giving up: with an op that never works, the total is the sum of all
    //    timeouts and backoffs: 5 + 1 + 5 + 2 + 5 + 4 + 5 = 27s.
    let before = clock.elapsed();
    let result = rt.block_on(retry(4, |_| std::future::pending::<Result<(), ()>>()));
    assert_eq!(result, Err(Failure::GaveUp { attempts: 4 }));
    assert_eq!(clock.elapsed() - before, 27 * SECOND);
    println!(
        "gave up after {:?} of virtual time",
        clock.elapsed() - before
    );

    // 3. Manual control. `advance` moves time by hand, to check what is (and
    //    isn't) due at a given moment.
    let fired = Rc::new(Cell::new(false));
    rt.block_on({
        let fired = fired.clone();
        async move {
            let f = fired.clone();
            spooly::set_timeout(60 * SECOND, move || f.set(true));

            clock.advance(59 * SECOND);
            spooly::yield_now().await; // Let the loop look at its timers.
            println!("t+59s: fired = {}", fired.get());
            assert!(!fired.get());

            clock.advance(SECOND);
            // The timer is due; it fires in the next timers phase. A short
            // sleep waits for that phase (it is queued after the timer).
            spooly::sleep(Duration::ZERO).await;
            println!("t+60s: fired = {}", fired.get());
            assert!(fired.get());
        }
    });

    println!("real time spent: {:?}", wall.elapsed());
    assert!(wall.elapsed() < Duration::from_secs(1));
}
