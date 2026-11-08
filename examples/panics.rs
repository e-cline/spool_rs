//! Errors and panics: a task that can fail returns a `Result`, and its
//! joiner handles it. A panic is a bug: Spooly doesn't catch it, so it stops
//! the loop (natively it unwinds out of `run`; on WASM it aborts).
//!
//! Run with: `cargo run --example panics`

use std::num::ParseIntError;
use std::panic;

fn parse(input: &'static str) -> spooly::JoinHandle<Result<i32, ParseIntError>> {
    spooly::spawn(async move {
        spooly::yield_now().await;
        input.parse::<i32>()
    })
}

fn main() {
    spooly::run(async {
        // Recoverable errors are values.
        for input in ["42", "forty-two"] {
            match parse(input).await {
                Ok(n) => println!("{input:?} parsed: {n}"),
                Err(err) => println!("{input:?} failed: {err}"),
            }
        }

        // `try_join` reports an abort instead of panicking.
        let task = spooly::spawn(std::future::pending::<()>());
        task.abort();
        println!("aborted task: {}", task.try_join().await.unwrap_err());
    });

    // A panic in any task stops the loop at once, even if the task is
    // awaited. Natively it unwinds out of `run`, where it can be caught.
    panic::set_hook(Box::new(|_| {})); // Keep the demo output readable.
    let rt = spooly::Runtime::new();
    let crashed = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        rt.run(async {
            let bad = spooly::spawn(async { panic!("a bug") });
            bad.await;
            println!("never printed");
        })
    }));
    let _ = panic::take_hook();
    println!("run crashed: {}", crashed.is_err());

    // Don't drive that runtime again: drop it, and use a new one.
    drop(rt);
    println!(
        "a new runtime is fine: {}",
        spooly::block_on(async { 1 + 1 })
    );
}
