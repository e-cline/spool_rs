//! Bridging a JS `Promise` into a Spooly future, with cancel-on-drop.
//!
//! `fetch_text` starts a JS `fetch` and returns a future for its body. The
//! future owns the request: **dropping it aborts the request**
//! (`AbortController`), so everything Spooly already does by dropping,
//! `timeout`, `AbortOnDrop` and a task going out of scope, now cancels real
//! network work too. No `wasm-bindgen-futures`, no `web-sys`: one callback
//! and a waker.
//!
//! The page runs three requests (one succeeds, one times out and is
//! aborted, one is a 404), then lets you start and cancel a slow one by hand.
//! To make cancellation visible against a fast local server, the JS side holds
//! each response back for a simulated latency; aborting cancels that wait,
//! or the request itself if it is still in flight.
//!
//! Build and serve it (see `examples/browser/main.rs` for the details):
//!
//! ```text
//! cargo build --release -p spooly --example browser_fetch --target wasm32-unknown-unknown --features browser
//! wasm-bindgen --target web --out-dir crates/spooly/examples/browser_fetch/pkg \
//!     target/wasm32-unknown-unknown/release/examples/browser_fetch.wasm
//! python3 -m http.server -d crates/spooly/examples/browser_fetch
//! ```
//!
//! Then open <http://localhost:8000>.
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen(inline_js = r#"
export function append_line(id, text) {
    const li = document.createElement("li");
    li.textContent = text;
    document.getElementById(id).appendChild(li);
}
export function on_click(id, callback) {
    document.getElementById(id).addEventListener("click", () => callback());
}

// Starts a request. `callback(ok, text)` is called once with the body (or the
// error) after `latency_ms` of simulated delay, and never after `abort_fetch`.
export function start_fetch(url, latency_ms, callback) {
    const controller = new AbortController();
    let timer;
    const settle = (ok, text) => { if (!controller.signal.aborted) callback(ok, text); };
    fetch(url, { signal: controller.signal })
        .then((r) => r.ok ? r.text() : Promise.reject(new Error("HTTP " + r.status)))
        .then((text) => { timer = setTimeout(() => settle(true, text), latency_ms); })
        .catch((e) => { timer = setTimeout(() => settle(false, String(e)), latency_ms); });
    controller.signal.addEventListener("abort", () => clearTimeout(timer));
    return controller;
}
export function abort_fetch(controller) {
    controller.abort();
}
"#)]
extern "C" {
    fn append_line(id: &str, text: &str);
    fn on_click(id: &str, callback: &Closure<dyn FnMut()>);
    fn start_fetch(
        url: &str,
        latency_ms: f64,
        callback: &Closure<dyn FnMut(bool, String)>,
    ) -> JsValue;
    fn abort_fetch(controller: &JsValue);
}

// --- The bridge --------------------------------------------------------------------

#[derive(Default)]
struct State {
    /// `Ok(body)` or `Err(message)`, once the request has settled.
    result: Option<Result<String, String>>,
    waker: Option<Waker>,
    settled: bool,
}

/// A request in flight. Dropping it before it settles aborts the request.
struct Fetch {
    state: Rc<RefCell<State>>,
    controller: JsValue,
    // Keeps the JS callback alive for as long as JS might call it. (After an
    // abort JS never calls it, so dropping it then is fine.)
    _callback: Closure<dyn FnMut(bool, String)>,
}

/// Starts fetching `url` at once (unlike most futures, this one does not wait
/// to be polled), with `latency` of simulated delay added to the response.
fn fetch_text(url: &str, latency: Duration) -> Fetch {
    let state = Rc::new(RefCell::new(State::default()));
    let s = state.clone();
    // Runs from a JS promise callback, outside any Spooly step: store the
    // result and wake the task. This is the whole Promise-to-future bridge.
    let callback = Closure::<dyn FnMut(bool, String)>::new(move |ok, text| {
        let waker = {
            let mut s = s.borrow_mut();
            s.result = Some(if ok { Ok(text) } else { Err(text) });
            s.settled = true;
            s.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    });
    let controller = start_fetch(url, latency.as_secs_f64() * 1000.0, &callback);
    Fetch {
        state,
        controller,
        _callback: callback,
    }
}

impl Future for Fetch {
    type Output = Result<String, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();
        match state.result.take() {
            Some(result) => Poll::Ready(result),
            None => {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl Drop for Fetch {
    fn drop(&mut self) {
        if !self.state.borrow().settled {
            abort_fetch(&self.controller);
            log("  (request dropped: aborted)");
        }
    }
}

// --- The page ---------------------------------------------------------------------------

fn log(line: &str) {
    append_line(
        "log",
        &format!("{:>5}ms  {line}", spooly::now().epoch_offset().as_millis()),
    );
}

/// Runs when the module is loaded.
#[wasm_bindgen(start)]
pub fn main() {
    spooly::browser::start(async {
        demo().await;
        drop(spooly::spawn(slow_button()));
    });
}

async fn demo() {
    // 1. A request that succeeds.
    log("1. GET data.json (100ms latency)");
    match fetch_text("data.json", Duration::from_millis(100)).await {
        Ok(body) => log(&format!("  ok: {}", body.trim())),
        Err(e) => log(&format!("  failed: {e}")),
    }

    // 2. A request that takes too long. `timeout` drops the future, and the
    //    drop aborts the request.
    log("2. GET data.json (2s latency) with a 300ms timeout");
    match spooly::timeout(
        Duration::from_millis(300),
        fetch_text("data.json", Duration::from_secs(2)),
    )
    .await
    {
        Ok(_) => log("  finished (unexpected)"),
        Err(elapsed) => log(&format!("  {elapsed}")),
    }

    // 3. An error response.
    log("3. GET missing.json");
    match fetch_text("missing.json", Duration::from_millis(50)).await {
        Ok(body) => log(&format!("  ok: {body}")),
        Err(e) => log(&format!("  failed: {e}")),
    }
    log("done. Try the buttons below.");
}

/// Start and cancel a slow request by hand. The request lives in a task; the
/// task is held by an `AbortOnDrop`, so dropping that cancels the task and
/// with it the request.
async fn slow_button() {
    enum Command {
        Start,
        Cancel,
    }
    let queue: Rc<RefCell<(VecDeque<Command>, Option<Waker>)>> = Rc::default();
    for (id, make) in [
        ("start", (|| Command::Start) as fn() -> Command),
        ("cancel", || Command::Cancel),
    ] {
        let queue = queue.clone();
        let callback = Closure::<dyn FnMut()>::new(move || {
            let waker = {
                let mut q = queue.borrow_mut();
                q.0.push_back(make());
                q.1.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        });
        on_click(id, &callback);
        callback.forget();
    }

    let mut request: Option<spooly::AbortOnDrop<()>> = None;
    loop {
        let command = poll_fn(|cx| {
            let mut q = queue.borrow_mut();
            match q.0.pop_front() {
                Some(c) => Poll::Ready(c),
                None => {
                    q.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await;
        match command {
            Command::Start => {
                log("GET data.json (3s latency)");
                // Replacing the old guard (if any) drops it, which aborts
                // that request.
                drop(
                    request.replace(
                        spooly::spawn(async {
                            match fetch_text("data.json", Duration::from_secs(3)).await {
                                Ok(body) => log(&format!("  ok: {}", body.trim())),
                                Err(e) => log(&format!("  failed: {e}")),
                            }
                        })
                        .abort_on_drop(),
                    ),
                );
            }
            Command::Cancel => {
                log("cancel");
                drop(request.take()); // Aborts the task, which drops the `Fetch`.
            }
        }
    }
}
