//! The example from the README: how microtasks, tasks, and timers are
//! ordered, and an interval on its fixed schedule.
//!
//! Run with: `cargo run --example timers`

use std::time::Duration;

fn main() {
    spool::run(async {
        spool::set_timeout(Duration::from_millis(40), || println!("timeout 40ms"));
        spool::queue_microtask(|| println!("microtask"));
        println!("sync");

        let task = spool::spawn(async {
            spool::sleep(Duration::from_millis(10)).await;
            42
        });
        println!("joined: {}", task.await);

        let mut every_5ms = spool::interval(Duration::from_millis(5));
        for i in 0..3 {
            let at = every_5ms.tick().await;
            println!("interval tick {i} at {:?}", at.epoch_offset());
        }
    });
}
