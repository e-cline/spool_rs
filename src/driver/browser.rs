//! The browser driver (D14): runs each step of the loop from a JS callback,
//! and never blocks.
//!
//! - `Busy` queues the next step as a JS microtask.
//! - `WaitUntil` sets a JS timeout for the deadline (or, with a clock that
//!   can jump, such as `MockClock`, queues the next step at once).
//! - `WaitForWake` and `Idle` schedule nothing. A wake from outside a step
//!   (a JS promise callback, say) takes the remote path, which calls
//!   [`wake`] to queue a step.
//!
//! A started runtime lives for the rest of the page, in a thread-local
//! registry: the browser driver never exits (§3.5).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::clock::Instant;
use crate::context;
use crate::driver::MainTask;
use crate::runtime::{Inner, Main, Runtime, Step};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = performance, js_name = now)]
    pub(crate) fn performance_now() -> f64;

    #[wasm_bindgen(js_name = queueMicrotask)]
    fn queue_js_microtask(callback: &JsValue);

    #[wasm_bindgen(js_name = setTimeout)]
    fn set_js_timeout(callback: &JsValue, ms: f64) -> JsValue;
}

/// Main, held as a `Pin<Box<dyn Future>>`.
type BoxedMain = MainTask<Box<dyn Future<Output = ()>>>;

thread_local! {
    /// Every started runtime, by id. Never removed: a started runtime runs
    /// for the rest of the page.
    static DRIVERS: RefCell<HashMap<u64, Rc<Driver>>> = RefCell::new(HashMap::new());
}

struct Driver {
    /// Owns the runtime. Never dropped (see `DRIVERS`).
    runtime: Runtime,
    main: RefCell<BoxedMain>,
    /// A step is queued as a JS microtask.
    step_queued: Cell<bool>,
    /// The earliest deadline a JS timeout is already set for.
    timeout_at: Cell<Option<Instant>>,
}

/// Starts `main` on `runtime`. Nothing runs until the current JS code
/// returns: the first step is a JS microtask.
pub(crate) fn start(runtime: Runtime, main: Pin<Box<dyn Future<Output = ()>>>) {
    let inner = runtime.inner();
    let id = inner.id();
    // Main isn't driven to completion by anyone waiting for it.
    let main = MainTask::new(main, inner.main_header(), false);
    let driver = Rc::new(Driver {
        runtime,
        main: RefCell::new(main),
        step_queued: Cell::new(false),
        timeout_at: Cell::new(None),
    });
    DRIVERS.with(|drivers| drivers.borrow_mut().insert(id, Rc::clone(&driver)));
    queue_microtask(move || driver.begin());
}

/// A wake arrived for runtime `runtime_id` from outside its step: queue a
/// step, if that runtime was started by this driver on this thread.
pub(crate) fn wake(runtime_id: u64) {
    let driver = DRIVERS
        .try_with(|drivers| drivers.borrow().get(&runtime_id).cloned())
        .ok()
        .flatten();
    if let Some(driver) = driver {
        driver.queue_step();
    }
}

impl Driver {
    fn inner(&self) -> &Rc<Inner> {
        self.runtime.inner()
    }

    /// The first callback: flushes work queued before `start`, then polls
    /// main for the first time, as a macrotask (D4a), then steps.
    fn begin(self: Rc<Self>) {
        {
            let _enter = context::enter(self.inner());
            self.inner().drain(&mut ());
            self.main.borrow_mut().poll();
        }
        self.step();
    }

    fn step(self: Rc<Self>) {
        let step = {
            let _enter = context::enter(self.inner());
            self.inner().step(&mut *self.main.borrow_mut())
        };
        match step {
            Step::Busy => self.queue_step(),
            Step::WaitUntil(deadline) => self.wait_until(deadline),
            Step::WaitForWake | Step::Idle => {}
        }
    }

    fn queue_step(self: Rc<Self>) {
        if self.step_queued.replace(true) {
            return;
        }
        queue_microtask(move || {
            self.step_queued.set(false);
            self.step();
        });
    }

    fn wait_until(self: Rc<Self>, deadline: Instant) {
        let clock = self.inner().clock();
        let now = clock.now();
        if deadline <= now || clock.advance_when_idle(deadline) {
            return self.queue_step();
        }
        if self.timeout_at.get().is_some_and(|at| at <= deadline) {
            return; // An earlier timeout will step, and re-check then.
        }
        // A later timeout that is already set still fires; its step just
        // finds nothing to do.
        self.timeout_at.set(Some(deadline));
        let ms = (deadline - now).as_secs_f64() * 1000.0;
        let callback = Closure::once_into_js(move || {
            if self.timeout_at.get() == Some(deadline) {
                self.timeout_at.set(None);
            }
            self.step();
        });
        set_js_timeout(&callback, ms.ceil());
    }
}

fn queue_microtask(f: impl FnOnce() + 'static) {
    queue_js_microtask(&Closure::once_into_js(f));
}
