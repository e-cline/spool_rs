//! The per-target primitives the event loop is built on (plan §7).
//!
//! Everything in the core that differs between targets, apart from the
//! browser's callback driver, comes from here: what the default clock is,
//! and how the parking driver (behind `run` and `block_on`) waits between
//! steps and is woken from outside. The rest of the core is the same on
//! every target and uses these through the names re-exported below, so it
//! needs no `cfg` of its own.
//!
//! Exactly one implementation is compiled, chosen by target:
//!
//! | Module   | Targets | Waits by | Woken from outside by |
//! |----------|---------|----------|-----------------------|
//! | `native` | everything with threads (Unix, Windows, ...) | `thread::park` | `Thread::unpark` |
//! | `wasi`   | `wasm32-wasip2` | `thread::sleep` (a `monotonic-clock` poll) | nothing: there are no threads |
//! | `web`    | `wasm32-unknown-unknown` | doesn't (returns at once) | queueing a step of the browser driver |
//!
//! Each must provide:
//!
//! - `default_clock() -> Rc<dyn Clock>`: the target's default clock (D13).
//! - `park(timeout: Option<Duration>)`: blocks the calling thread (the
//!   runtime's) until `timeout` passes (`None`: until unparked), or until
//!   an [`Unparker`] is called. Only used when there's no I/O driver, which
//!   waits in its own way instead. It may return early.
//! - `Unparker`: `Send + Sync`, made with `Unparker::current()` on the
//!   runtime's thread when the runtime is built; `unpark(&self)` ends a
//!   `park` (or makes the next one return at once).
//! - `notify_remote_wake(runtime_id: u64)`: called after every remote wake
//!   (§4.3), after the unparker, whether or not there is an I/O driver. A
//!   driver that doesn't block (the browser's) uses it to schedule a step.
//!
//! These are plain functions and types rather than a trait: only one
//! implementation exists per build, and nothing chooses between them at run
//! time. (The extension points that do have several implementations,
//! [`Clock`](crate::Clock) and [`IoDriver`](crate::driver::IoDriver), are
//! traits.)

cfg_select! {
    target_os = "wasi" => {
        mod wasi;
        use wasi as imp;
    }
    all(target_arch = "wasm32", target_os = "unknown") => {
        mod web;
        use web as imp;
    }
    _ => {
        mod native;
        use native as imp;
    }
}

pub(crate) use imp::{Unparker, default_clock, notify_remote_wake, park};
