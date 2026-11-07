//! Tasks interleave at every `.await`, in FIFO order, like JS async functions.
//!
//! Run with: `cargo run --example hello`

use std::cell::Cell;
use std::rc::Rc;

fn main() {
    // `Rc` and `Cell` are fine: Spool never requires `Send`.
    let counter = Rc::new(Cell::new(0));

    let total = spool::block_on(async {
        let workers: Vec<_> = ["alpha", "beta", "gamma"]
            .into_iter()
            .map(|name| {
                let counter = counter.clone();
                spool::spawn(async move {
                    for step in 1..=2 {
                        counter.set(counter.get() + 1);
                        println!("{name}: step {step} (counter = {})", counter.get());
                        spool::yield_now().await;
                    }
                    name.len()
                })
            })
            .collect();

        let mut total = 0;
        for worker in workers {
            total += worker.await;
        }
        total
    });

    println!("total name length: {total}, counter: {}", counter.get());
}
