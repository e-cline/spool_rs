//! The example from the README: how microtasks, tasks, and timers are
//! ordered.
//!
//! Run with: `cargo run --example timers`

use std::time::Duration;

fn main() {
    spooly::run(async {
        spooly::set_timeout(Duration::from_millis(40), || println!("timeout 40ms"));
        spooly::queue_microtask(|| println!("microtask"));
        println!("sync");

        let task = spooly::spawn(async {
            spooly::sleep(Duration::from_millis(10)).await;
            42
        });
        println!("joined: {}", task.await);
    });
}
