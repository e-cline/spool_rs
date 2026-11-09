//! A debounced search box. Typing wakes a task from a JS event handler; the
//! task waits for a pause in typing (`Sleep::reset` restarts the timer on
//! every keystroke), then starts a fake search that takes 400ms. Typing again
//! drops the search that is still running.
//!
//! This is the classic event-loop pattern, in a `!Send` task with plain
//! `Rc`/`RefCell` state and no JS glue beyond three DOM helpers.
//!
//! Build and serve it (see `examples/browser/main.rs` for the details):
//!
//! ```text
//! cargo build --release -p spooly --example browser_debounce --target wasm32-unknown-unknown --features browser
//! wasm-bindgen --target web --out-dir crates/spooly/examples/browser_debounce/pkg \
//!     target/wasm32-unknown-unknown/release/examples/browser_debounce.wasm
//! python3 -m http.server -d crates/spooly/examples/browser_debounce
//! ```
//!
//! Then open <http://localhost:8000>.
#![cfg(target_arch = "wasm32")]

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen(inline_js = r#"
export function set_text(id, text) {
    document.getElementById(id).textContent = text;
}
export function on_input(id, callback) {
    document.getElementById(id).addEventListener("input", (e) => callback(e.target.value));
}
"#)]
extern "C" {
    fn set_text(id: &str, text: &str);
    fn on_input(id: &str, callback: &Closure<dyn FnMut(String)>);
}

/// How long typing must pause before a search starts.
const DEBOUNCE: Duration = Duration::from_millis(250);
/// How long the fake search takes.
const SEARCH_LATENCY: Duration = Duration::from_millis(400);

const WORDS: &[&str] = &[
    "apple",
    "apricot",
    "avocado",
    "banana",
    "blackberry",
    "blueberry",
    "cherry",
    "clementine",
    "coconut",
    "cranberry",
    "date",
    "dragonfruit",
    "fig",
    "grape",
    "grapefruit",
    "guava",
    "kiwi",
    "lemon",
    "lime",
    "lychee",
    "mango",
    "melon",
    "nectarine",
    "olive",
    "orange",
    "papaya",
    "peach",
    "pear",
    "persimmon",
    "pineapple",
    "plum",
    "pomegranate",
    "raspberry",
    "strawberry",
    "tangerine",
    "watermelon",
];

/// What the JS event handler hands to the task: the newest text in the box.
#[derive(Default)]
struct Input {
    latest: Option<String>,
    waker: Option<Waker>,
}

#[derive(Default)]
struct Counts {
    keystrokes: Cell<u32>,
    started: Cell<u32>,
    finished: Cell<u32>,
    cancelled: Cell<u32>,
}

impl Counts {
    fn show(&self) {
        set_text(
            "counts",
            &format!(
                "{} keystrokes, {} searches started, {} finished, {} cancelled",
                self.keystrokes.get(),
                self.started.get(),
                self.finished.get(),
                self.cancelled.get(),
            ),
        );
    }
}

/// Runs when the module is loaded.
#[wasm_bindgen(start)]
pub fn main() {
    spooly::browser::start(debounced_search());
}

async fn debounced_search() {
    let input = Rc::new(RefCell::new(Input::default()));
    let counts = Rc::new(Counts::default());

    let (i, c) = (input.clone(), counts.clone());
    let callback = Closure::<dyn FnMut(String)>::new(move |text: String| {
        c.keystrokes.set(c.keystrokes.get() + 1);
        c.show();
        // Runs outside any Spooly step: store the text and wake the task.
        let waker = {
            let mut i = i.borrow_mut();
            i.latest = Some(text);
            i.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    });
    on_input("box", &callback);
    callback.forget(); // The handler lives as long as the page.
    counts.show();

    enum Event {
        Typed(String),
        Quiet,
    }

    // `Some` while waiting for typing to pause.
    let mut quiet: Option<spooly::Sleep> = None;
    // The search in flight. Dropping the guard aborts the task.
    let mut search: Option<spooly::AbortOnDrop<()>> = None;
    let mut pending_text = String::new();

    loop {
        // Wait for either more typing or the end of the pause. (Spooly has
        // no `select!`, so this polls both by hand.)
        let event = poll_fn(|cx| {
            let typed = {
                let mut i = input.borrow_mut();
                let typed = i.latest.take();
                if typed.is_none() {
                    i.waker = Some(cx.waker().clone());
                }
                typed
            };
            if let Some(text) = typed {
                return Poll::Ready(Event::Typed(text));
            }
            match &mut quiet {
                Some(sleep) => Pin::new(sleep).poll(cx).map(|()| Event::Quiet),
                None => Poll::Pending,
            }
        })
        .await;

        match event {
            Event::Typed(text) => {
                // A new keystroke makes the running search stale.
                if let Some(running) = search.take()
                    && !running.is_finished()
                {
                    counts.cancelled.set(counts.cancelled.get() + 1);
                    counts.show();
                }
                set_text("status", "typing...");
                pending_text = text;
                // Restart the pause.
                let deadline = spooly::now() + DEBOUNCE;
                match &mut quiet {
                    Some(sleep) => sleep.reset(deadline),
                    None => quiet = Some(spooly::sleep_until(deadline)),
                }
            }
            Event::Quiet => {
                quiet = None;
                counts.started.set(counts.started.get() + 1);
                counts.show();
                set_text("status", &format!("searching for {pending_text:?}..."));
                let (query, counts) = (pending_text.clone(), counts.clone());
                search = Some(
                    spooly::spawn(async move {
                        spooly::sleep(SEARCH_LATENCY).await; // The "network".
                        counts.finished.set(counts.finished.get() + 1);
                        counts.show();
                        let query = query.trim().to_lowercase();
                        let hits: Vec<_> = WORDS
                            .iter()
                            .filter(|w| !query.is_empty() && w.contains(&query))
                            .copied()
                            .collect();
                        set_text("status", &format!("{} result(s) for {query:?}", hits.len()));
                        set_text("results", &hits.join(", "));
                    })
                    .abort_on_drop(),
                );
            }
        }
    }
}
