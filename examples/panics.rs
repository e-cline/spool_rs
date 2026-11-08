//! Panics behave like exceptions in JS async code: re-thrown into whoever
//! awaits the task, catchable with `try_join`, and fatal when nobody can
//! observe them.
//!
//! Run with: `cargo run --example panics`

use std::panic;

fn parse(input: &'static str) -> spool::JoinHandle<i32> {
    spool::spawn(async move {
        spool::yield_now().await;
        input.parse::<i32>().expect("not a number")
    })
}

fn main() {
    // Keep the demo output readable: don't print the expected panics.
    panic::set_hook(Box::new(|_| {}));

    spool::run(async {
        // `try_join` is `try { await p } catch (e) { ... }`.
        for input in ["42", "forty-two"] {
            match parse(input).try_join().await {
                Ok(n) => println!("{input:?} parsed: {n}"),
                Err(err) => println!("{input:?} failed: {err}"),
            }
        }

        // A plain `.await` re-throws, and the panic travels up a chain of
        // awaits until something observes it.
        let outer = spool::spawn(async { parse("x").await + 1 });
        println!("outer: {}", outer.try_join().await.unwrap_err());
    });

    // A panic nobody observes (like an unhandled rejection) unwinds out of
    // `run`, and poisons the runtime.
    let rt = spool::Runtime::new();
    let crashed = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        rt.run(async {
            drop(parse("oops")); // Detached: nobody will see its panic.
            spool::yield_now().await;
            spool::yield_now().await;
            println!("never printed");
        })
    }));
    println!("run crashed: {}", crashed.is_err());
    let reused = panic::catch_unwind(panic::AssertUnwindSafe(|| rt.block_on(async {})));
    println!(
        "reusing the poisoned runtime panics too: {}",
        reused.is_err()
    );
    println!(
        "a new runtime is fine: {}",
        spool::block_on(async { 1 + 1 })
    );
}
