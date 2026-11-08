//! The ways to cancel things: drop a future, time it out, abort a task, tie a
//! task to a scope, and cancel a callback timer.
//!
//! Run with: `cargo run --example cancellation`

use std::time::Duration;

const MS: Duration = Duration::from_millis(1);

async fn slow_lookup(id: u32) -> String {
    spooly::sleep(50 * MS).await;
    format!("record {id}")
}

fn main() {
    spooly::run(async {
        // 1. `timeout` drops the inner future when time runs out.
        match spooly::timeout(20 * MS, slow_lookup(1)).await {
            Ok(record) => println!("got {record}"),
            Err(elapsed) => println!("lookup 1: {elapsed}"),
        }
        println!(
            "lookup 2: {:?}",
            spooly::timeout(80 * MS, slow_lookup(2)).await
        );

        // 2. Abort a task from elsewhere, here from a watchdog timer.
        let worker = spooly::spawn(async {
            loop {
                spooly::sleep(10 * MS).await;
                println!("  worker: still working...");
            }
        });
        let abort = worker.abort_handle();
        spooly::set_timeout(35 * MS, move || {
            println!("  watchdog: aborting the worker");
            abort.abort();
        });
        match worker.try_join().await {
            Ok(()) => unreachable!(),
            Err(err) => println!("worker: {err}"),
        }

        // 3. A task that lives exactly as long as a scope.
        {
            let _spinner = spooly::spawn(async {
                for frame in ["|", "/", "-", "\\"].iter().cycle() {
                    println!("  spinner {frame}");
                    spooly::sleep(10 * MS).await;
                }
            })
            .abort_on_drop();
            spooly::sleep(25 * MS).await;
            println!("leaving the scope: the spinner stops");
        }
        spooly::sleep(30 * MS).await;

        // 4. Callback timers are cancelled through their handle.
        let reminder = spooly::set_timeout(10 * MS, || println!("never printed"));
        reminder.cancel();
        let ticker = spooly::set_interval(10 * MS, {
            let mut n = 0;
            move |me| {
                n += 1;
                println!("  interval tick {n}");
                if n == 3 {
                    me.cancel(); // An interval can cancel itself.
                }
            }
        });
        // Handles can be cloned; dropping one does *not* cancel the timer.
        drop(ticker.clone());
    });
    println!("done: nothing is left running");
}
