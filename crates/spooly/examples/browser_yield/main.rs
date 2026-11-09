//! Keeping a page responsive during a long computation. The same job (count
//! the primes below 3 million, in 5ms slices) runs three ways, and the page
//! measures the longest time the browser went without drawing a frame.
//!
//! - **No yielding**: the page freezes for the whole job.
//! - **`yield_now`**: also freezes! It is a *microtask* yield: other tasks
//!   run, but the browser can't draw or handle input until the microtask
//!   queue is empty, and a loop of `yield_now` keeps it from emptying.
//! - **`sleep(1ms)`**: the browser gets a turn between slices. A timer is a
//!   macrotask boundary; a zero delay isn't enough (Spooly runs a due timer
//!   without leaving the JS callback), so use a delay of at least 1ms.
//!
//! Build and serve it (see `examples/browser/main.rs` for the details):
//!
//! ```text
//! cargo build --release -p spooly --example browser_yield --target wasm32-unknown-unknown --features browser
//! wasm-bindgen --target web --out-dir crates/spooly/examples/browser_yield/pkg \
//!     target/wasm32-unknown-unknown/release/examples/browser_yield.wasm
//! python3 -m http.server -d crates/spooly/examples/browser_yield
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

#[wasm_bindgen(inline_js = r#"
export function set_text(id, text) {
    document.getElementById(id).textContent = text;
}
export function set_busy(busy) {
    for (const b of document.querySelectorAll("button")) b.disabled = busy;
}
export function on_click(id, callback) {
    document.getElementById(id).addEventListener("click", () => callback());
}

// Measures the longest gap between animation frames while it runs. A frame
// that never comes (a frozen page) shows up as a gap at `meter_stop`.
let running = false, last = 0, longest = 0;
export function meter_start() {
    running = true;
    longest = 0;
    last = performance.now();
    const frame = () => {
        if (!running) return;
        const now = performance.now();
        longest = Math.max(longest, now - last);
        last = now;
        requestAnimationFrame(frame);
    };
    requestAnimationFrame(frame);
}
export function meter_stop() {
    running = false;
    return Math.max(longest, performance.now() - last);
}
"#)]
extern "C" {
    fn set_text(id: &str, text: &str);
    fn set_busy(busy: bool);
    fn on_click(id: &str, callback: &Closure<dyn FnMut()>);
    fn meter_start();
    fn meter_stop() -> f64;
}

/// Count the primes below this.
const LIMIT: u32 = 3_000_000;
/// How long one slice of work may run.
const SLICE: Duration = Duration::from_millis(5);

#[derive(Clone, Copy)]
enum Mode {
    Blocking,
    YieldNow,
    Sleep,
}

impl Mode {
    fn id(self) -> &'static str {
        match self {
            Mode::Blocking => "blocking",
            Mode::YieldNow => "yield",
            Mode::Sleep => "sleep",
        }
    }
}

/// Counts primes by trial division, resumable between slices.
struct Primes {
    next: u32,
    count: u32,
}

impl Primes {
    /// Works until `budget` is spent (or forever, with `None`). Returns true
    /// when done.
    fn work(&mut self, budget: Option<Duration>) -> bool {
        let start = spooly::now();
        while self.next < LIMIT {
            for n in self.next..(self.next + 512).min(LIMIT) {
                self.count += is_prime(n) as u32;
            }
            self.next = (self.next + 512).min(LIMIT);
            if budget.is_some_and(|b| start.elapsed() >= b) {
                break;
            }
        }
        self.next >= LIMIT
    }
}

fn is_prime(n: u32) -> bool {
    n >= 2
        && (2..)
            .take_while(|d| d * d <= n)
            .all(|d| !n.is_multiple_of(d))
}

/// Runs when the module is loaded.
#[wasm_bindgen(start)]
pub fn main() {
    spooly::browser::start(buttons());
}

/// Clicks, queued by JS handlers, and the waker of the task waiting for them.
type Clicks = Rc<RefCell<(VecDeque<Mode>, Option<Waker>)>>;

async fn buttons() {
    let clicks: Clicks = Rc::default();
    for mode in [Mode::Blocking, Mode::YieldNow, Mode::Sleep] {
        let clicks = clicks.clone();
        let callback = Closure::<dyn FnMut()>::new(move || {
            let waker = {
                let mut c = clicks.borrow_mut();
                c.0.push_back(mode);
                c.1.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        });
        on_click(&format!("btn-{}", mode.id()), &callback);
        callback.forget();
    }

    loop {
        let mode = poll_fn(|cx| {
            let mut c = clicks.borrow_mut();
            match c.0.pop_front() {
                Some(mode) => Poll::Ready(mode),
                None => {
                    c.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await;
        run(mode).await;
    }
}

async fn run(mode: Mode) {
    set_busy(true);
    set_text(mode.id(), "running...");
    // Give the browser a moment to draw "running" before a blocking job.
    spooly::sleep(Duration::from_millis(50)).await;

    meter_start();
    let start = spooly::now();
    let mut primes = Primes { next: 0, count: 0 };
    match mode {
        Mode::Blocking => {
            primes.work(None);
        }
        Mode::YieldNow => {
            while !primes.work(Some(SLICE)) {
                spooly::yield_now().await;
            }
        }
        Mode::Sleep => {
            while !primes.work(Some(SLICE)) {
                spooly::sleep(Duration::from_millis(1)).await;
            }
        }
    }
    let total = start.elapsed();
    let longest = meter_stop();

    set_text(
        mode.id(),
        &format!(
            "{} primes in {:.0}ms; longest gap between frames: {:.0}ms",
            primes.count,
            total.as_secs_f64() * 1000.0,
            longest,
        ),
    );
    set_busy(false);
}
