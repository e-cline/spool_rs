//! Spooly in a browser page: a ticking clock, the event loop's ordering, and
//! a task woken by button clicks (wakes from JS, outside any Spooly step).
//!
//! Build and serve it (`wasm-bindgen` is the CLI, the same version as the
//! `wasm-bindgen` crate in `Cargo.lock`):
//!
//! ```text
//! cargo build --release --example browser --target wasm32-unknown-unknown --features browser
//! wasm-bindgen --target web --out-dir examples/browser/pkg \
//!     target/wasm32-unknown-unknown/release/examples/browser.wasm
//! python3 -m http.server -d examples/browser
//! ```
//!
//! Then open <http://localhost:8000>.
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::wasm_bindgen;

// A few DOM helpers, written in JS to avoid depending on `web-sys`.
#[wasm_bindgen(inline_js = r#"
export function set_text(id, text) {
    document.getElementById(id).textContent = text;
}
export function append_line(id, text) {
    const li = document.createElement("li");
    li.textContent = text;
    document.getElementById(id).appendChild(li);
}
export function on_click(id, callback) {
    document.getElementById(id).addEventListener("click", () => callback());
}
"#)]
extern "C" {
    fn set_text(id: &str, text: &str);
    fn append_line(id: &str, text: &str);
    fn on_click(id: &str, callback: &Closure<dyn FnMut()>);
}

/// Runs when the module is loaded.
#[wasm_bindgen(start)]
pub fn main() {
    spooly::browser::start(async {
        ordering();
        drop(spooly::spawn(clicks()));
        clock().await;
    });
}

/// The event loop's order: synchronous code, then microtasks, then timers.
fn ordering() {
    spooly::set_timeout(Duration::ZERO, || append_line("order", "3. a timer"));
    spooly::queue_microtask(|| append_line("order", "2. a microtask"));
    append_line("order", "1. synchronous code");
}

/// Counts seconds since the page loaded, on an interval.
async fn clock() {
    let start = spooly::now();
    let mut interval = spooly::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let seconds = start.elapsed().as_secs();
        set_text("clock", &format!("{seconds}s"));
    }
}

/// Waits for button clicks. Each click wakes this task from a JS event
/// handler, which runs outside the event loop's steps.
async fn clicks() {
    let clicks = Clicks::default();
    let c = clicks.clone();
    let callback = Closure::<dyn FnMut()>::new(move || c.push());
    on_click("button", &callback);
    callback.forget(); // The handler lives as long as the page.

    let mut count = 0;
    loop {
        clicks.next().await;
        count += 1;
        // A little async work per click, to show it isn't run by JS directly.
        spooly::sleep(Duration::from_millis(100)).await;
        set_text(
            "clicks",
            &format!("{count} (each counted 100ms after the click)"),
        );
    }
}

/// A queue of clicks, and the waker of the task waiting for the next one.
#[derive(Clone, Default)]
struct Clicks(Rc<RefCell<(VecDeque<()>, Option<Waker>)>>);

impl Clicks {
    fn push(&self) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.0.push_back(());
            state.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    async fn next(&self) {
        poll_fn(|cx| {
            let mut state = self.0.borrow_mut();
            match state.0.pop_front() {
                Some(()) => Poll::Ready(()),
                None => {
                    state.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await
    }
}
