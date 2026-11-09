//! The order things run in. Each line below is logged from a different kind
//! of work; the output shows which goes first, and why.
//!
//! The rules (see "The event loop" in the crate docs):
//! 1. The code running right now finishes first.
//! 2. Then the microtask queue drains, in FIFO order. Woken tasks and
//!    `queue_microtask` callbacks are both microtasks, and microtasks queued
//!    meanwhile run in the same drain.
//! 3. Then timers: due ones fire one at a time, and the microtask queue drains
//!    after each. A timer armed during the timers phase waits for the next one.
//!
//! The browser page (`examples/browser`) logs the same kind of ordering.
//!
//! Run with: `cargo run --example event_loop_order`

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

type Log = Rc<RefCell<Vec<&'static str>>>;

fn log(log: &Log, line: &'static str) {
    println!("  {line}");
    log.borrow_mut().push(line);
}

fn main() {
    let events: Log = Rc::default();

    println!("order of execution:");
    let e = events.clone();
    spooly::run(async move {
        // Timers: both are due on the next timers phase, in creation order.
        let l = e.clone();
        spooly::set_timeout(Duration::ZERO, move || log(&l, "5. timer A"));
        let l = e.clone();
        spooly::set_timeout(Duration::ZERO, move || {
            log(&l, "6. timer B");
            // Queued from a timer: runs before timer C, since microtasks
            // drain after each timer.
            let l2 = l.clone();
            spooly::queue_microtask(move || log(&l2, "7. microtask queued by timer B"));
        });
        let l = e.clone();
        spooly::set_timeout(Duration::ZERO, move || log(&l, "8. timer C"));

        // Microtasks run in the order they were queued (FIFO).
        let l = e.clone();
        spooly::queue_microtask(move || log(&l, "2. microtask"));

        // A spawned task starts as a microtask too, queued after the one above.
        let l = e.clone();
        drop(spooly::spawn(async move {
            log(&l, "3. spawned task, first poll");
            // `yield_now` puts the task at the back of the microtask queue.
            spooly::yield_now().await;
            log(&l, "4. spawned task, after yield_now");
        }));

        log(&e, "1. synchronous code (the end of `main`'s first poll)");
    });

    // The same order every run, on every target.
    let expected = [
        "1. synchronous code (the end of `main`'s first poll)",
        "2. microtask",
        "3. spawned task, first poll",
        "4. spawned task, after yield_now",
        "5. timer A",
        "6. timer B",
        "7. microtask queued by timer B",
        "8. timer C",
    ];
    assert_eq!(*events.borrow(), expected);
}
