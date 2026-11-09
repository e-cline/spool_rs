//! The I/O driver hook (`spooly::driver`, M12): the I/O phase, liveness,
//! waiting through the driver, and remote wakes interrupting that wait.
//!
//! The tests use `Fake`, a small reactor: "I/O" is a numbered key that
//! another thread (or the test itself) marks ready, and a task awaits with
//! `fake.ready(key)`. Its `wait` blocks on a condition variable, so it
//! behaves like a real reactor, including needing the unparker.
// Needs threads, which the browser doesn't have.
#![cfg(not(target_arch = "wasm32"))]

mod common;

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::future::{Future, poll_fn};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Poll, Waker};
use std::thread;
use std::time::Duration;

use common::{Trace, oneshot};
use spooly::driver::{IoDriver, current_io_driver};
use spooly::{Clock, Instant, MockClock, Runtime};

/// A call the loop made to the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    PollReady,
    Wait(Option<Duration>),
}

/// The thread-safe part: keys marked ready, and whether the unparker ran.
#[derive(Default)]
struct Signal {
    state: Mutex<SignalState>,
    changed: Condvar,
    unparks: AtomicUsize,
}

#[derive(Default)]
struct SignalState {
    ready: Vec<u32>,
    unparked: bool,
}

/// Marks keys ready from any thread.
#[derive(Clone)]
struct Remote(Arc<Signal>);

impl Remote {
    fn make_ready(&self, key: u32) {
        self.0.state.lock().unwrap().ready.push(key);
        self.0.changed.notify_all();
    }
}

#[derive(Clone, Default)]
struct Fake {
    signal: Arc<Signal>,
    /// Tasks waiting on a key.
    waiting: Rc<RefCell<Vec<(u32, Waker)>>>,
    /// Keys whose readiness was delivered, not yet consumed by `ready`.
    fired: Rc<RefCell<HashSet<u32>>>,
    /// Keys that become ready during the next `wait` (not before).
    during_wait: Rc<RefCell<Vec<u32>>>,
    calls: Rc<RefCell<Vec<Call>>>,
    /// If set, `wait` doesn't block: it advances this virtual time by its
    /// timeout instead (see `VirtualClock`).
    virtual_time: Option<Rc<Cell<Duration>>>,
}

impl Fake {
    fn remote(&self) -> Remote {
        Remote(Arc::clone(&self.signal))
    }

    fn make_ready_during_wait(&self, key: u32) {
        self.during_wait.borrow_mut().push(key);
    }

    /// Completes once `key` is ready and the I/O phase delivered it.
    fn ready(&self, key: u32) -> impl Future<Output = ()> + 'static {
        let fake = self.clone();
        poll_fn(move |cx| {
            if fake.fired.borrow_mut().remove(&key) {
                return Poll::Ready(());
            }
            let mut waiting = fake.waiting.borrow_mut();
            waiting.retain(|(k, _)| *k != key);
            waiting.push((key, cx.waker().clone()));
            Poll::Pending
        })
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.borrow().clone()
    }

    fn unparks(&self) -> usize {
        self.signal.unparks.load(Ordering::SeqCst)
    }
}

impl IoDriver for Fake {
    fn poll_ready(&self) -> bool {
        self.calls.borrow_mut().push(Call::PollReady);
        let keys = std::mem::take(&mut self.signal.state.lock().unwrap().ready);
        let mut wakers = Vec::new();
        for key in keys {
            self.fired.borrow_mut().insert(key);
            self.waiting.borrow_mut().retain(|(k, waker)| {
                if *k == key {
                    wakers.push(waker.clone());
                }
                *k != key
            });
        }
        // Wake outside the borrows: wakes queue microtasks right away.
        let woke = !wakers.is_empty();
        wakers.into_iter().for_each(Waker::wake);
        woke
    }

    fn wait(&self, timeout: Option<Duration>) -> bool {
        self.calls.borrow_mut().push(Call::Wait(timeout));
        if let (Some(time), Some(timeout)) = (&self.virtual_time, timeout) {
            time.set(time.get() + timeout);
            return false;
        }
        let deadline = timeout.map(|t| std::time::Instant::now() + t);
        let mut state = self.signal.state.lock().unwrap();
        state.ready.append(&mut self.during_wait.borrow_mut());
        loop {
            if !state.ready.is_empty() {
                return true;
            }
            if std::mem::take(&mut state.unparked) {
                return false;
            }
            state = match deadline {
                None => self.signal.changed.wait(state).unwrap(),
                Some(deadline) => {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return false;
                    }
                    self.signal
                        .changed
                        .wait_timeout(state, deadline - now)
                        .unwrap()
                        .0
                }
            };
        }
    }

    fn unparker(&self) -> Arc<dyn Fn() + Send + Sync> {
        let signal = Arc::clone(&self.signal);
        Arc::new(move || {
            signal.unparks.fetch_add(1, Ordering::SeqCst);
            signal.state.lock().unwrap().unparked = true;
            signal.changed.notify_all();
        })
    }

    fn is_active(&self) -> bool {
        !self.waiting.borrow().is_empty()
    }
}

/// A clock that stands still until the fake driver's `wait` advances it:
/// waits happen in virtual time, whatever the real speed (under Miri, say).
/// Unlike `MockClock`, it never jumps to a deadline by itself, so the loop
/// has to wait through the driver.
struct VirtualClock(Rc<Cell<Duration>>);

impl Clock for VirtualClock {
    fn now(&self) -> Instant {
        Instant::from_epoch_offset(self.0.get())
    }
}

fn runtime(fake: &Fake) -> Runtime {
    Runtime::builder().io_driver(fake.clone()).build()
}

fn mock_runtime(fake: &Fake) -> (Runtime, MockClock) {
    let clock = MockClock::new();
    let rt = Runtime::builder()
        .clock(clock.clone())
        .io_driver(fake.clone())
        .build();
    (rt, clock)
}

/// Makes `key` ready from another thread after `delay`.
fn ready_later(remote: Remote, key: u32, delay: Duration) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        thread::sleep(delay);
        remote.make_ready(key);
    })
}

// --- The I/O phase -------------------------------------------------------

#[test]
fn io_from_another_thread_wakes_main() {
    let fake = Fake::default();
    let worker = ready_later(fake.remote(), 1, Duration::from_millis(20));
    let f = fake.clone();
    runtime(&fake).block_on(async move { f.ready(1).await });
    worker.join().unwrap();
    // Nothing else to wait for, so the loop waited on the driver, with no
    // timeout, and the I/O phase delivered the readiness.
    assert!(fake.calls().contains(&Call::Wait(None)));
    assert!(fake.calls().contains(&Call::PollReady));
}

#[test]
fn io_phase_runs_after_the_timers_phase() {
    let fake = Fake::default();
    let (rt, _clock) = mock_runtime(&fake);
    let trace = Trace::new();
    let (t, f) = (trace.clone(), fake.clone());
    rt.run(async move {
        let t2 = t.clone();
        let f2 = f.clone();
        drop(spooly::spawn(async move {
            f2.ready(1).await;
            t2.log("io");
        }));
        spooly::yield_now().await; // The task registers its interest.
        let t3 = t.clone();
        spooly::set_timeout(Duration::ZERO, move || t3.log("timer"));
        f.remote().make_ready(1);
        t.log("main");
    });
    trace.assert(&["main", "timer", "io"]);
}

#[test]
fn io_wakes_are_microtasks_drained_in_the_io_phase() {
    let fake = Fake::default();
    let (rt, _clock) = mock_runtime(&fake);
    let trace = Trace::new();
    let (t, f) = (trace.clone(), fake.clone());
    rt.run(async move {
        for key in [1, 2] {
            let (t, f) = (t.clone(), f.clone());
            drop(spooly::spawn(async move {
                f.ready(key).await;
                t.log(format!("io {key}"));
                let t2 = t.clone();
                spooly::queue_microtask(move || t2.log(format!("micro {key}")));
            }));
        }
        spooly::yield_now().await;
        f.remote().make_ready(1);
        f.remote().make_ready(2);
        // Armed before the I/O phase, but due after it: the next step.
        let t2 = t.clone();
        spooly::set_timeout(Duration::from_millis(1), move || t2.log("timer"));
    });
    // Woken tasks are FIFO microtasks; the queue drains before the next
    // step's timers phase.
    trace.assert(&["io 1", "io 2", "micro 1", "micro 2", "timer"]);
}

#[test]
fn a_rearming_timer_does_not_starve_io() {
    fn rearm(done: Rc<Cell<bool>>, fired: Rc<Cell<u32>>) {
        spooly::set_timeout(Duration::ZERO, move || {
            fired.set(fired.get() + 1);
            assert!(fired.get() < 1000, "the I/O phase never ran");
            if !done.get() {
                rearm(done, fired);
            }
        });
    }

    let fake = Fake::default();
    let (rt, _clock) = mock_runtime(&fake);
    let fired = Rc::new(Cell::new(0));
    let (f, d) = (fake.clone(), fired.clone());
    rt.run(async move {
        let done = Rc::new(Cell::new(false));
        rearm(done.clone(), d);
        let f2 = f.clone();
        drop(spooly::spawn(async move {
            f2.ready(1).await;
            done.set(true);
        }));
        spooly::yield_now().await;
        f.remote().make_ready(1);
    });
    // The timer fires once per step; the I/O phase of the first step
    // already delivers the readiness.
    assert!(fired.get() <= 2, "fired {} times", fired.get());
}

// --- Liveness and stalls --------------------------------------------------

#[test]
fn a_detached_task_waiting_on_io_keeps_run_alive() {
    let fake = Fake::default();
    let done = Rc::new(Cell::new(false));
    let worker = ready_later(fake.remote(), 1, Duration::from_millis(20));
    let (f, d) = (fake.clone(), done.clone());
    runtime(&fake).run(async move {
        drop(spooly::spawn(async move {
            f.ready(1).await;
            d.set(true);
        }));
    });
    worker.join().unwrap();
    assert!(done.get());
}

#[test]
fn an_idle_driver_does_not_keep_run_alive() {
    let fake = Fake::default();
    runtime(&fake).run(async {
        // A task pending on something that can't wake it, and no I/O
        // waits: nothing keeps the loop alive.
        drop(spooly::spawn(std::future::pending::<()>()));
    });
    assert!(!fake.calls().iter().any(|c| matches!(c, Call::Wait(_))));
}

#[test]
#[should_panic(expected = "main future stalled")]
fn a_stalled_main_still_panics_with_an_idle_driver() {
    let fake = Fake::default();
    runtime(&fake).block_on(std::future::pending::<()>());
}

#[test]
fn block_on_does_not_wait_for_io() {
    let fake = Fake::default();
    let f = fake.clone();
    let out = runtime(&fake).block_on(async move {
        drop(spooly::spawn(f.ready(1)));
        spooly::yield_now().await;
        5
    });
    assert_eq!(out, 5);
    assert!(!fake.calls().iter().any(|c| matches!(c, Call::Wait(_))));
}

// --- Waiting: remote wakes and the clock ---------------------------------

#[test]
fn a_remote_wake_interrupts_the_wait() {
    let fake = Fake::default();
    let (tx, rx) = oneshot();
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send(7);
    });
    // Main waits on another thread, not on I/O: the driver's `wait` blocks
    // until the remote wake calls its unparker.
    assert_eq!(runtime(&fake).block_on(rx), 7);
    worker.join().unwrap();
    assert!(fake.unparks() >= 1);
}

#[test]
fn dropping_the_last_keep_alive_interrupts_the_wait() {
    let fake = Fake::default();
    let rt = runtime(&fake);
    let keep = rt.keep_alive();
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        drop(keep);
    });
    rt.run(async {});
    worker.join().unwrap();
    assert!(fake.unparks() >= 1);
}

#[test]
fn ready_io_comes_before_a_mock_clock_jump() {
    let fake = Fake::default();
    let (rt, clock) = mock_runtime(&fake);
    let trace = Trace::new();
    let (t, f, c) = (trace.clone(), fake.clone(), clock.clone());
    // `block_on`: set up the tasks, and return before the loop waits.
    rt.block_on(async move {
        let (t2, c2) = (t.clone(), c.clone());
        drop(spooly::spawn(async move {
            spooly::sleep(Duration::from_secs(30)).await;
            t2.log(format!("timer at {:?}", c2.elapsed()));
        }));
        drop(spooly::spawn(async move {
            f.ready(1).await;
            t.log(format!("io at {:?}", c.elapsed()));
        }));
        spooly::yield_now().await;
    });
    // Hidden until the loop waits: then the zero-timeout wait sees it, and
    // the clock doesn't jump (IO4).
    fake.make_ready_during_wait(1);
    assert!(trace.events().is_empty());
    rt.run(async {});
    trace.assert(&["io at 0ns", "timer at 30s"]);
    assert!(fake.calls().contains(&Call::Wait(Some(Duration::ZERO))));
    assert_eq!(clock.elapsed(), Duration::from_secs(30));
}

#[test]
fn the_mock_clock_jumps_when_no_io_is_ready() {
    let fake = Fake::default();
    let (rt, clock) = mock_runtime(&fake);
    let f = fake.clone();
    let out =
        rt.block_on(async move { spooly::timeout(Duration::from_secs(30), f.ready(1)).await });
    assert!(out.is_err());
    assert_eq!(clock.elapsed(), Duration::from_secs(30));
}

#[test]
fn waits_for_a_timer_through_the_driver() {
    let time = Rc::new(Cell::new(Duration::ZERO));
    let fake = Fake {
        virtual_time: Some(time.clone()),
        ..Fake::default()
    };
    let rt = Runtime::builder()
        .clock(VirtualClock(time.clone()))
        .io_driver(fake.clone())
        .build();
    rt.block_on(async { spooly::sleep(Duration::from_millis(10)).await });
    // The loop first checks for ready I/O without blocking, then waits
    // through the driver until the deadline.
    let waits: Vec<_> = fake
        .calls()
        .into_iter()
        .filter(|c| matches!(c, Call::Wait(_)))
        .collect();
    assert_eq!(
        waits,
        [
            Call::Wait(Some(Duration::ZERO)),
            Call::Wait(Some(Duration::from_millis(10)))
        ]
    );
    assert_eq!(time.get(), Duration::from_millis(10));
}

// --- Finding the driver ---------------------------------------------------

#[test]
fn current_io_driver_finds_the_installed_driver() {
    struct Other;
    impl IoDriver for Other {
        fn poll_ready(&self) -> bool {
            false
        }
        fn wait(&self, _: Option<Duration>) -> bool {
            false
        }
        fn unparker(&self) -> Arc<dyn Fn() + Send + Sync> {
            Arc::new(|| {})
        }
        fn is_active(&self) -> bool {
            false
        }
    }

    let fake = Fake::default();
    runtime(&fake).block_on(async {
        let found = current_io_driver::<Fake>().expect("the installed driver");
        assert!(Rc::ptr_eq(&found.calls, &fake.calls));
        assert!(current_io_driver::<Other>().is_none());
    });
    Runtime::new().block_on(async {
        assert!(current_io_driver::<Fake>().is_none());
    });
}

#[test]
#[should_panic(expected = "outside of a Spooly runtime")]
fn current_io_driver_outside_a_runtime_panics() {
    let _ = current_io_driver::<Fake>();
}
