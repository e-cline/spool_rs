//! Intervals on a fixed schedule: tasks sharing state through `Rc<Cell<_>>`.
//!
//! Run with: `cargo run --example intervals`

use std::{cell::Cell, rc::Rc, time::Duration};

fn main() {
    spooly::run(async {
        // Tasks share state through `Rc<Cell<_>>`. Everything runs on one
        // thread, so no locks or atomics are needed.
        let count = Rc::new(Cell::new(0));
        let is_done = Rc::new(Cell::new(false));

        // Bumps the counter every second until `is_done` is set.
        let counter = {
            let (count, is_done) = (count.clone(), is_done.clone());
            spooly::spawn(async move {
                let mut every_second = spooly::interval(Duration::from_secs(1));
                while !is_done.get() {
                    every_second.tick().await;
                    count.set(count.get() + 1);
                }
            })
        };

        // Reads it every 1.5 seconds until `is_done` is set.
        let reporter = {
            let (count, is_done) = (count.clone(), is_done.clone());
            spooly::spawn(async move {
                let mut every_1500ms = spooly::interval(Duration::from_millis(1500));
                while !is_done.get() {
                    let at = every_1500ms.tick().await;
                    println!("at {:?}: count = {}", at.epoch_offset(), count.get());
                }
            })
        };

        // Flips `is_done` after 5 seconds.
        let stopper = {
            let is_done = is_done.clone();
            spooly::spawn(async move {
                spooly::sleep(Duration::from_secs(5)).await;
                is_done.set(true);
            })
        };

        futures::join!(counter, reporter, stopper);
        println!("final count: {}", count.get());
    });
}
