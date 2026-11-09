//! A test runner for time-based code, running in the browser on a
//! `MockClock`. Each test waits for seconds, hours or a whole day of virtual
//! time, and the page shows how long that took for real: milliseconds.
//!
//! The only browser-specific lines are in `main`: `browser::start_on` takes a
//! runtime built with `Builder::clock(MockClock)`. When the event loop would
//! wait for the next timer, the mock clock jumps there, and the browser driver
//! runs the next step at once instead of calling `setTimeout`. A page can
//! therefore test its timing logic without waiting, in the same wasm module it
//! ships.
//!
//! Build and serve it (see `examples/browser/main.rs` for the details):
//!
//! ```text
//! cargo build --release -p spooly --example browser_mock_clock --target wasm32-unknown-unknown --features browser
//! wasm-bindgen --target web --out-dir crates/spooly/examples/browser_mock_clock/pkg \
//!     target/wasm32-unknown-unknown/release/examples/browser_mock_clock.wasm
//! python3 -m http.server -d crates/spooly/examples/browser_mock_clock
//! ```
//!
//! Then open <http://localhost:8000>.
#![cfg(target_arch = "wasm32")]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen(inline_js = r#"
export function clear_rows(id) {
    document.getElementById(id).replaceChildren();
}
export function add_row(id, status, name, detail) {
    const tr = document.createElement("tr");
    tr.className = status;
    for (const text of [status, name, detail]) {
        const td = document.createElement("td");
        td.textContent = text;
        tr.appendChild(td);
    }
    document.getElementById(id).appendChild(tr);
}
export function set_text(id, text) {
    document.getElementById(id).textContent = text;
}
export function real_now() {
    return performance.now();
}
export function on_click(id, callback) {
    document.getElementById(id).addEventListener("click", () => callback());
}
"#)]
extern "C" {
    fn clear_rows(id: &str);
    fn add_row(id: &str, status: &str, name: &str, detail: &str);
    fn set_text(id: &str, text: &str);
    fn real_now() -> f64;
    fn on_click(id: &str, callback: &Closure<dyn FnMut()>);
}

const SECOND: Duration = Duration::from_secs(1);
const HOUR: Duration = Duration::from_secs(3600);
const DAY: Duration = Duration::from_secs(24 * 3600);

type TestResult = Result<(), String>;
type Test = Pin<Box<dyn Future<Output = TestResult>>>;

fn check<T: PartialEq + std::fmt::Debug>(what: &str, got: T, want: T) -> TestResult {
    if got == want {
        Ok(())
    } else {
        Err(format!("{what}: got {got:?}, wanted {want:?}"))
    }
}

/// The tests. Each is a plain async block; none of them mentions the browser.
fn tests(clock: spooly::MockClock) -> Vec<(&'static str, Test)> {
    let c = clock.clone();
    let timeout_test: Test = Box::pin(async move {
        let start = c.elapsed();
        let result = spooly::timeout(30 * SECOND, std::future::pending::<()>()).await;
        check("timed out", result.is_err(), true)?;
        check("elapsed", c.elapsed() - start, 30 * SECOND)
    });

    let c = clock.clone();
    let hourly: Test = Box::pin(async move {
        let runs = Rc::new(Cell::new(0));
        let r = runs.clone();
        let job = spooly::set_interval(HOUR, move |_| r.set(r.get() + 1));
        // Wait a minute past the day. Stopping at exactly 24h is a tie: the
        // 24th run is armed (at 23h) after this sleep (at 0h), and timers
        // with equal deadlines fire in the order they were armed, so the
        // sleep would end first and the test would count 23. (The mock clock
        // made that easy to find.)
        spooly::sleep(DAY + 60 * SECOND).await;
        job.cancel();
        check("runs in a day", runs.get(), 24)?;
        check("clock", c.elapsed() >= DAY, true)
    });

    let heartbeat: Test = Box::pin(async move {
        let beats = Rc::new(Cell::new(0u32));
        let b = beats.clone();
        let job = spooly::set_interval(SECOND, move |_| b.set(b.get() + 1));
        spooly::sleep(DAY + SECOND / 2).await; // Not exactly 24h: see above.
        job.cancel();
        check("beats in a day", beats.get(), 86_400)
    });

    let c = clock.clone();
    let backoff: Test = Box::pin(async move {
        let start = c.elapsed();
        let mut delay = SECOND;
        for _ in 0..4 {
            spooly::sleep(delay).await;
            delay *= 2; // 1s, 2s, 4s, 8s.
        }
        check("total backoff", c.elapsed() - start, 15 * SECOND)
    });

    let c = clock.clone();
    let debounce: Test = Box::pin(async move {
        // Three events 100ms apart, each restarting a 250ms quiet timer: the
        // timer fires 250ms after the last event, at 550ms.
        let start = c.elapsed();
        let mut quiet = spooly::sleep(Duration::from_millis(250));
        for _ in 0..3 {
            spooly::sleep(Duration::from_millis(100)).await;
            quiet.reset(spooly::now() + Duration::from_millis(250));
        }
        quiet.await;
        check("fired at", c.elapsed() - start, Duration::from_millis(550))
    });

    let by_hand: Test = Box::pin(async move {
        let fired = Rc::new(Cell::new(false));
        let f = fired.clone();
        spooly::set_timeout(60 * SECOND, move || f.set(true));
        clock.advance(59 * SECOND);
        spooly::yield_now().await;
        check("fired at 59s", fired.get(), false)?;
        clock.advance(SECOND);
        spooly::sleep(Duration::ZERO).await; // Wait for the timers phase.
        check("fired at 60s", fired.get(), true)
    });

    vec![
        ("a 30s timeout fires at exactly 30s", timeout_test),
        ("an hourly job runs 24 times in a day", hourly),
        ("a 1s heartbeat beats 86,400 times in a day", heartbeat),
        ("exponential backoff waits 1+2+4+8 = 15s", backoff),
        ("a debounce restarts its timer on each event", debounce),
        ("advancing the clock by hand", by_hand),
    ]
}

async fn run_all(clock: &spooly::MockClock) {
    clear_rows("rows");
    set_text("summary", "running...");
    let (mut passed, mut virtual_total, mut real_total) = (0, Duration::ZERO, 0.0);
    let all = tests(clock.clone());
    let count = all.len();
    for (name, test) in all {
        let (v0, r0) = (clock.elapsed(), real_now());
        let result = test.await;
        let (virtual_time, real_ms) = (clock.elapsed() - v0, real_now() - r0);
        virtual_total += virtual_time;
        real_total += real_ms;
        let detail = format!("{virtual_time:?} of virtual time, in {real_ms:.1}ms");
        match result {
            Ok(()) => {
                passed += 1;
                add_row("rows", "pass", name, &detail);
            }
            Err(why) => add_row("rows", "FAIL", name, &format!("{why} ({detail})")),
        }
        // Let the page draw each row as it comes in. The mock clock makes
        // timers instant, so this is just a yield to the browser.
        yield_to_browser().await;
    }
    set_text(
        "summary",
        &format!(
            "{passed}/{count} passed. {:.1} hours of virtual time in {real_total:.0}ms.",
            virtual_total.as_secs_f64() / 3600.0,
        ),
    );
}

/// Under a mock clock a sleep never reaches the browser's `setTimeout`, so
/// the page would not repaint between tests. A JS promise does yield: it is
/// resolved from a callback, which wakes the task from outside.
async fn yield_to_browser() {
    let state: Rc<RefCell<(bool, Option<Waker>)>> = Rc::default();
    let s = state.clone();
    let callback = Closure::once_into_js(move || {
        let waker = {
            let mut s = s.borrow_mut();
            s.0 = true;
            s.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    });
    set_js_timeout(&callback);
    poll_fn(|cx| {
        let mut s = state.borrow_mut();
        if s.0 {
            Poll::Ready(())
        } else {
            s.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    })
    .await
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = setTimeout)]
    fn set_js_timeout(callback: &wasm_bindgen::JsValue);
}

/// Runs when the module is loaded.
#[wasm_bindgen(start)]
pub fn main() {
    let clock = spooly::MockClock::new();
    let runtime = spooly::Runtime::builder().clock(clock.clone()).build();
    spooly::browser::start_on(runtime, async move {
        // "Run again" clicks, from a JS handler: wake this task.
        let clicks: Rc<RefCell<(VecDeque<()>, Option<Waker>)>> = Rc::default();
        let c = clicks.clone();
        let callback = Closure::<dyn FnMut()>::new(move || {
            let waker = {
                let mut c = c.borrow_mut();
                c.0.push_back(());
                c.1.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        });
        on_click("again", &callback);
        callback.forget();

        loop {
            run_all(&clock).await;
            poll_fn(|cx| {
                let mut c = clicks.borrow_mut();
                match c.0.pop_front() {
                    Some(()) => Poll::Ready(()),
                    None => {
                        c.1 = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            })
            .await;
        }
    });
}
