//! Combining futures. Spooly's core has no `select!` or `join_all`; a future
//! is just something to poll, so both are short to write (and `futures` or
//! `futures-lite` have polished versions if you'd rather depend on one).
//!
//! - `race(a, b)` polls both, returns whichever finishes first, and **drops**
//!   the other. Dropping is cancelling: the loser's timers go with it.
//! - `join_all(futures)` polls all of them in one task and returns the
//!   outputs in input order.
//!
//! The demo uses a `MockClock`, so the "network" delays are virtual: it runs
//! instantly, and the elapsed-time assertions are exact.
//!
//! Run with: `cargo run --example race_and_join`

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

const MS: Duration = Duration::from_millis(1);

// --- race -----------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum Either<A, B> {
    Left(A),
    Right(B),
}

struct Race<A, B> {
    a: Pin<Box<A>>,
    b: Pin<Box<B>>,
}

/// Completes with the first of `a` and `b` to finish (`a` wins a tie).
fn race<A: Future, B: Future>(a: A, b: B) -> Race<A, B> {
    Race {
        a: Box::pin(a),
        b: Box::pin(b),
    }
}

impl<A: Future, B: Future> Future for Race<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `Race` is `Unpin` (it only holds boxes), so this is safe code.
        if let Poll::Ready(v) = self.a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = self.b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    }
}

// --- join_all ---------------------------------------------------------------

struct JoinAll<F: Future> {
    futures: Vec<Pin<Box<F>>>,
    outputs: Vec<Option<F::Output>>,
}

fn join_all<F: Future>(futures: impl IntoIterator<Item = F>) -> JoinAll<F> {
    let futures: Vec<_> = futures.into_iter().map(Box::pin).collect();
    let outputs = futures.iter().map(|_| None).collect();
    JoinAll { futures, outputs }
}

// Safe to say: the futures are pinned by their boxes, and the outputs are
// plain values that are never pinned.
impl<F: Future> Unpin for JoinAll<F> {}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut pending = false;
        for (future, output) in this.futures.iter_mut().zip(&mut this.outputs) {
            if output.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(v) => *output = Some(v),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            return Poll::Pending;
        }
        Poll::Ready(this.outputs.iter_mut().map(|o| o.take().unwrap()).collect())
    }
}

// --- the demo ----------------------------------------------------------------

/// Announces when it is dropped, to show what happens to the loser of a race.
struct Request(&'static str);

impl Drop for Request {
    fn drop(&mut self) {
        println!("    ({} request dropped)", self.0);
    }
}

async fn fetch(server: &'static str, latency: Duration) -> &'static str {
    let _guard = Request(server);
    spooly::sleep(latency).await;
    server
}

fn main() {
    let clock = spooly::MockClock::new();
    let rt = spooly::Runtime::builder().clock(clock.clone()).build();

    rt.block_on(async {
        // 1. A hedged request: ask two servers, use the first answer.
        println!("race two servers:");
        let winner = race(fetch("east", 80 * MS), fetch("west", 30 * MS)).await;
        println!("  winner: {winner:?} after {:?}", clock.elapsed());
        assert_eq!(winner, Either::Right("west"));
        assert_eq!(clock.elapsed(), 30 * MS); // Not 80: `east` was dropped.

        // 2. A deadline is a race against a timer: this is `spooly::timeout`.
        println!("race a request against a deadline:");
        let start = clock.elapsed();
        let result = race(fetch("slow", 500 * MS), spooly::sleep(100 * MS)).await;
        println!("  {result:?} after {:?}", clock.elapsed() - start);
        assert_eq!(result, Either::Right(()));
        assert_eq!(clock.elapsed() - start, 100 * MS);

        // 3. `join_all`: concurrent, ordered by input. Total time is the
        //    slowest one (60ms), not the sum (110ms).
        println!("join three servers:");
        let start = clock.elapsed();
        let all = join_all([
            fetch("a", 50 * MS),
            fetch("b", 60 * MS),
            fetch("c", Duration::ZERO),
        ])
        .await;
        println!("  {all:?} after {:?}", clock.elapsed() - start);
        assert_eq!(all, ["a", "b", "c"]);
        assert_eq!(clock.elapsed() - start, 60 * MS);

        // 4. With `spawn`, each future is its own task: it keeps running if
        //    nobody is polling the group, and it can be aborted on its own.
        //    `join_all` is lighter; `spawn` is more independent.
        println!("spawned instead:");
        let start = clock.elapsed();
        let handles: Vec<_> = [("x", 20), ("y", 10)]
            .into_iter()
            .map(|(name, ms)| spooly::spawn(fetch(name, ms * MS)))
            .collect();
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await);
        }
        println!("  {results:?} after {:?}", clock.elapsed() - start);
        assert_eq!(results, ["x", "y"]);
        assert_eq!(clock.elapsed() - start, 20 * MS);
    });
}
