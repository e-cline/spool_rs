//! Reference-model tests: `proptest` generates random scheduling programs,
//! and each one is run (a) on Spooly and (b) on a tiny, obviously-correct
//! simulator of the event-loop semantics in plan §3. The traces must match.
//! The model is Spooly's correctness oracle (plan D15).
//!
//! Programs mix microtasks, one-shot timers, intervals, timer cancellation,
//! slow callbacks (which move the clock forward), spawned tasks, yields,
//! sleeps, joins, aborts, and panics. Spooly runs them on a `MockClock`, and
//! the model keeps its own virtual time, so timers are deterministic.
//!
//! Spooly doesn't catch panics (plan §6): any panic crashes the run at
//! once, and the traces must match up to the crash.

// Native only: needs proptest, and catching panics.
#![cfg(not(target_arch = "wasm32"))]

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use proptest::prelude::*;

// ── Programs ────────────────────────────────────────────────────────────────

/// A synchronous operation.
#[derive(Debug, Clone)]
enum Op {
    /// Record an event. Ids are assigned after generation, in tree order.
    Log(u32),
    /// `queue_microtask` a callback that runs these ops.
    Micro(Vec<Op>),
    /// `set_timeout` (delay in ms) a callback that runs these ops. The timer
    /// slot (assigned after generation, in tree order) lets `Cancel` refer
    /// to it.
    Timer(u32, u64, Vec<Op>),
    /// `set_interval` (period in ms) a callback that runs these ops, and
    /// cancels itself after this many runs.
    Interval(u32, u64, u32, Vec<Op>),
    /// Cancel the timer or interval created by `Timer(slot, ..)` /
    /// `Interval(slot, ..)`, if it has been created.
    Cancel(u32),
    /// Be "slow": advance the clock by this many ms, as if this code took
    /// that long. Timers that become due wait for the next timers phase.
    Busy(u64),
    /// `spawn` a detached task. The slot number (assigned after generation,
    /// in tree order) lets `Abort` refer to it.
    Spawn(u32, Body),
    /// Abort the task spawned by `Spawn(slot, _)`, if it has been spawned.
    Abort(u32),
}

/// One step of an async body.
#[derive(Debug, Clone)]
enum Step {
    Ops(Vec<Op>),
    /// `yield_now().await`
    Yield,
    /// `sleep(ms).await`
    Sleep(u64),
    /// `sleep_until(now - ms).await`: a deadline in the past (clamped at the
    /// epoch). It still waits for a timers phase, and it sorts before
    /// timers that are due later.
    SleepUntilPast(u64),
    /// `spawn(body).try_join().await`: run a child task and wait for it.
    Join(Body),
    /// `panic!()`.
    Panic,
}

/// The body of a task (or of main).
#[derive(Debug, Clone)]
struct Body(Rc<Vec<Step>>);

/// Counters for numbering a generated program.
#[derive(Default)]
struct Numbering {
    log: u32,
    slot: u32,
    timer: u32,
}

fn number_ops(ops: &mut [Op], next: &mut Numbering) {
    for op in ops {
        match op {
            Op::Log(id) => {
                *id = next.log;
                next.log += 1;
            }
            Op::Micro(ops) => number_ops(ops, next),
            Op::Timer(slot, _, ops) | Op::Interval(slot, _, _, ops) => {
                *slot = next.timer;
                next.timer += 1;
                number_ops(ops, next);
            }
            Op::Spawn(slot, body) => {
                *slot = next.slot;
                next.slot += 1;
                number_body(body, next);
            }
            Op::Cancel(_) | Op::Busy(_) | Op::Abort(_) => {}
        }
    }
}

fn number_body(body: &mut Body, next: &mut Numbering) {
    for step in Rc::make_mut(&mut body.0) {
        match step {
            Step::Ops(ops) => number_ops(ops, next),
            Step::Yield | Step::Sleep(_) | Step::SleepUntilPast(_) | Step::Panic => {}
            Step::Join(child) => number_body(child, next),
        }
    }
}

// ── Strategies ──────────────────────────────────────────────────────────────

/// Small delays, so that equal deadlines (FIFO ordering) are common.
fn delay() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0), Just(1), Just(2), Just(5)]
}

/// Interval periods: non-zero, and small enough to interleave with delays.
fn period() -> impl Strategy<Value = u64> {
    prop_oneof![Just(1), Just(2), Just(5)]
}

fn body_strategy(op: BoxedStrategy<Op>, join_depth: u32) -> BoxedStrategy<Body> {
    let ops = prop::collection::vec(op.clone(), 0..4).prop_map(Step::Ops);
    let simple = prop_oneof![
        8 => ops,
        2 => Just(Step::Yield),
        2 => delay().prop_map(Step::Sleep),
        2 => prop_oneof![Just(1), Just(3)].prop_map(Step::SleepUntilPast),
        1 => Just(Step::Panic),
    ];
    let step = if join_depth == 0 {
        simple.boxed()
    } else {
        let child = body_strategy(op, join_depth - 1);
        prop_oneof![5 => simple, 1 => child.prop_map(Step::Join)].boxed()
    };
    prop::collection::vec(step, 0..5)
        .prop_map(|steps| Body(Rc::new(steps)))
        .boxed()
}

fn op_strategy() -> BoxedStrategy<Op> {
    Just(Op::Log(0))
        .prop_recursive(4, 48, 4, |inner| {
            let inner = inner.boxed();
            let ops = || prop::collection::vec(inner.clone(), 0..4);
            prop_oneof![
                3 => Just(Op::Log(0)),
                3 => ops().prop_map(Op::Micro),
                3 => (delay(), ops()).prop_map(|(ms, ops)| Op::Timer(0, ms, ops)),
                2 => (period(), 1..4_u32, ops())
                    .prop_map(|(p, runs, ops)| Op::Interval(0, p, runs, ops)),
                1 => (0..4_u32).prop_map(Op::Cancel),
                1 => prop_oneof![Just(1), Just(3), Just(7)].prop_map(Op::Busy),
                1 => body_strategy(inner.clone(), 1).prop_map(|body| Op::Spawn(0, body)),
                1 => (0..4_u32).prop_map(Op::Abort),
            ]
        })
        .boxed()
}

fn program_strategy() -> impl Strategy<Value = Body> {
    body_strategy(op_strategy(), 2).prop_map(|mut body| {
        number_body(&mut body, &mut Numbering::default());
        body
    })
}

// ── Running on Spooly ────────────────────────────────────────────────────────

type Log = Rc<RefCell<Vec<u32>>>;

/// What a running program shares: its trace, the clock, and the handles of
/// its spawned tasks and timers by slot.
#[derive(Clone)]
struct Ctx {
    log: Log,
    clock: spooly::MockClock,
    tasks: Rc<RefCell<HashMap<u32, spooly::AbortHandle>>>,
    timers: Rc<RefCell<HashMap<u32, spooly::TimerHandle>>>,
}

const MODEL_PANIC: &str = "model panic";

/// The outcome of a run: the trace, and whether a panic crashed it.
type Outcome = (Vec<u32>, bool);

/// Silences the (many, expected) panic messages of `Step::Panic`.
fn quiet_model_panics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if info.payload().downcast_ref::<&str>() != Some(&MODEL_PANIC) {
                previous(info);
            }
        }));
    });
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn spooly_ops(ops: &[Op], ctx: &Ctx) {
    for op in ops {
        match op {
            Op::Log(id) => ctx.log.borrow_mut().push(*id),
            Op::Micro(ops) => {
                let (ops, ctx) = (ops.clone(), ctx.clone());
                spooly::queue_microtask(move || spooly_ops(&ops, &ctx));
            }
            Op::Timer(slot, delay, ops) => {
                let (ops, c) = (ops.clone(), ctx.clone());
                let handle = spooly::set_timeout(ms(*delay), move || spooly_ops(&ops, &c));
                ctx.timers.borrow_mut().insert(*slot, handle);
            }
            Op::Interval(slot, period, runs, ops) => {
                let (ops, c) = (ops.clone(), ctx.clone());
                let mut left = *runs;
                let handle = spooly::set_interval(ms(*period), move |me| {
                    spooly_ops(&ops, &c);
                    left -= 1;
                    if left == 0 {
                        me.cancel();
                    }
                });
                ctx.timers.borrow_mut().insert(*slot, handle);
            }
            Op::Cancel(slot) => {
                let handle = ctx.timers.borrow().get(slot).cloned();
                if let Some(handle) = handle {
                    handle.cancel();
                }
            }
            Op::Busy(n) => ctx.clock.advance(ms(*n)),
            Op::Spawn(slot, body) => {
                let handle = spooly::spawn(spooly_body(body.clone(), ctx.clone()));
                ctx.tasks.borrow_mut().insert(*slot, handle.abort_handle());
                drop(handle); // Detached.
            }
            Op::Abort(slot) => {
                let handle = ctx.tasks.borrow().get(slot).cloned();
                if let Some(handle) = handle {
                    handle.abort();
                }
            }
        }
    }
}

fn spooly_body(body: Body, ctx: Ctx) -> Pin<Box<dyn Future<Output = ()>>> {
    Box::pin(async move {
        for step in body.0.iter() {
            match step {
                Step::Ops(ops) => spooly_ops(ops, &ctx),
                Step::Yield => spooly::yield_now().await,
                Step::Sleep(delay) => spooly::sleep(ms(*delay)).await,
                Step::SleepUntilPast(back) => {
                    let deadline = spooly::now()
                        .checked_sub(ms(*back))
                        .unwrap_or(spooly::Instant::ZERO);
                    spooly::sleep_until(deadline).await;
                }
                Step::Join(child) => {
                    let child = spooly::spawn(spooly_body(child.clone(), ctx.clone()));
                    // `Err` only if the child was aborted.
                    let _ = child.try_join().await;
                }
                Step::Panic => std::panic::panic_any(MODEL_PANIC),
            }
        }
    })
}

fn run_on_spooly(program: &Body) -> Outcome {
    quiet_model_panics();
    let clock = spooly::MockClock::new();
    let ctx = Ctx {
        log: Log::default(),
        clock: clock.clone(),
        tasks: Rc::default(),
        timers: Rc::default(),
    };
    let rt = spooly::Runtime::builder().clock(clock).build();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.run(spooly_body(program.clone(), ctx.clone()));
    }));
    if let Err(payload) = &result {
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&MODEL_PANIC),
            "an unexpected panic"
        );
    }
    drop(rt);
    (ctx.log.take(), result.is_err())
}

// ── The reference model ─────────────────────────────────────────────────────

/// A task in the model: a body, a program counter, and (for joined tasks)
/// the task waiting on it.
struct Task {
    body: Body,
    pc: usize,
    joiner: Option<usize>,
    finished: bool,
    /// Aborted: it never runs again, and its handle on a joined child is
    /// gone.
    aborted: bool,
}

/// A microtask.
enum Job {
    Run(Vec<Op>),
    Resume(usize),
}

/// What a timer does when it fires. Timers have a model-wide id, so that a
/// cancel reaches the right one.
enum TimerJob {
    /// A `set_timeout` callback.
    Once(usize, Vec<Op>),
    /// A `set_interval` (its state is in `Model::intervals`).
    Interval(usize),
    /// A sleeping task's wake-up.
    Wake(usize),
}

struct IntervalState {
    period: u64,
    runs_left: u32,
    ops: Vec<Op>,
    /// The deadline it last fired for: the next is on the same schedule.
    deadline: u64,
}

/// The next deadline on an interval's fixed schedule (`prev + k·period`):
/// the first that isn't before `now`. Missed runs are skipped (plan D7).
fn next_interval_deadline(prev: u64, period: u64, now: u64) -> u64 {
    let mut next = prev + period;
    while next < now {
        next += period;
    }
    next
}

#[derive(Default)]
struct Model {
    microtasks: VecDeque<Job>,
    /// Timers keyed by `(deadline in ms, seq)`, so iteration order is firing
    /// order.
    timers: BTreeMap<(u64, u64), TimerJob>,
    next_seq: u64,
    now: u64,
    tasks: Vec<Task>,
    /// Spawn slot → task id (the latest spawn for that slot).
    slots: HashMap<u32, usize>,
    /// Timer slot → timer id (the latest timer for that slot).
    timer_slots: HashMap<u32, usize>,
    next_timer: usize,
    /// Live intervals, by timer id.
    intervals: HashMap<usize, IntervalState>,
    /// The task being resumed right now.
    current: Option<usize>,
    /// A panic: the run stops at once.
    crashed: bool,
    log: Vec<u32>,
}

impl Model {
    fn spawn(&mut self, body: Body, joiner: Option<usize>) -> usize {
        self.tasks.push(Task {
            body,
            pc: 0,
            joiner,
            finished: false,
            aborted: false,
        });
        self.tasks.len() - 1
    }

    fn add_timer_at(&mut self, deadline: u64, job: TimerJob) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.timers.insert((deadline, seq), job);
    }

    fn new_timer(&mut self, slot: u32) -> usize {
        let id = self.next_timer;
        self.next_timer += 1;
        self.timer_slots.insert(slot, id);
        id
    }

    fn ops(&mut self, ops: &[Op]) {
        for op in ops {
            match op {
                Op::Log(id) => self.log.push(*id),
                Op::Micro(ops) => self.microtasks.push_back(Job::Run(ops.clone())),
                Op::Timer(slot, delay, ops) => {
                    let id = self.new_timer(*slot);
                    self.add_timer_at(self.now + delay, TimerJob::Once(id, ops.clone()));
                }
                Op::Interval(slot, period, runs, ops) => {
                    let id = self.new_timer(*slot);
                    let deadline = self.now + period;
                    self.intervals.insert(
                        id,
                        IntervalState {
                            period: *period,
                            runs_left: *runs,
                            ops: ops.clone(),
                            deadline,
                        },
                    );
                    self.add_timer_at(deadline, TimerJob::Interval(id));
                }
                Op::Cancel(slot) => {
                    if let Some(&id) = self.timer_slots.get(slot) {
                        self.cancel_timer(id);
                    }
                }
                Op::Busy(n) => self.now += n,
                Op::Spawn(slot, body) => {
                    let id = self.spawn(body.clone(), None);
                    self.slots.insert(*slot, id);
                    self.microtasks.push_back(Job::Resume(id));
                }
                Op::Abort(slot) => {
                    if let Some(&id) = self.slots.get(slot) {
                        self.abort(id);
                    }
                }
            }
        }
    }

    fn cancel_timer(&mut self, id: usize) {
        self.intervals.remove(&id);
        self.timers.retain(|_, job| match job {
            TimerJob::Once(t, _) | TimerJob::Interval(t) => *t != id,
            TimerJob::Wake(_) => true,
        });
    }

    fn abort(&mut self, id: usize) {
        let task = &mut self.tasks[id];
        if task.finished || task.aborted {
            return;
        }
        task.aborted = true;
        if self.current != Some(id) {
            self.kill(id);
        }
        // Otherwise it is aborting itself: killed once it suspends.
    }

    /// Drops an aborted task's future: its sleep is cancelled.
    fn kill(&mut self, id: usize) {
        self.timers
            .retain(|_, job| !matches!(job, TimerJob::Wake(t) if *t == id));
    }

    /// Whether `joiner` can still observe its child's result.
    fn live_joiner(&self, joiner: Option<usize>) -> Option<usize> {
        joiner.filter(|&j| !self.tasks[j].aborted)
    }

    /// Runs task `id` until it suspends or finishes.
    fn resume(&mut self, id: usize) {
        if self.tasks[id].aborted || self.tasks[id].finished {
            return; // A stale wake.
        }
        self.current = Some(id);
        let suspended = self.step(id);
        self.current = None;
        if suspended && self.tasks[id].aborted {
            self.kill(id); // It aborted itself.
        }
    }

    /// Runs task `id`'s steps. Returns `true` if it suspended, `false` if it
    /// finished or panicked.
    fn step(&mut self, id: usize) -> bool {
        loop {
            let body = self.tasks[id].body.clone();
            let Some(step) = body.0.get(self.tasks[id].pc) else {
                // Finished: wake whoever joined it (a microtask hop).
                self.tasks[id].finished = true;
                if let Some(joiner) = self.live_joiner(self.tasks[id].joiner) {
                    self.microtasks.push_back(Job::Resume(joiner));
                }
                return false;
            };
            self.tasks[id].pc += 1;
            match step {
                Step::Ops(ops) => self.ops(ops),
                Step::Yield => {
                    self.microtasks.push_back(Job::Resume(id));
                    return true;
                }
                Step::Sleep(delay) => {
                    self.add_timer_at(self.now + delay, TimerJob::Wake(id));
                    return true;
                }
                Step::SleepUntilPast(back) => {
                    self.add_timer_at(self.now.saturating_sub(*back), TimerJob::Wake(id));
                    return true;
                }
                Step::Join(child) => {
                    let child = self.spawn(child.clone(), Some(id));
                    self.microtasks.push_back(Job::Resume(child));
                    return true;
                }
                Step::Panic => {
                    self.crashed = true;
                    return false;
                }
            }
        }
    }

    /// Runs every microtask, including ones queued meanwhile. Stops at a
    /// crash.
    fn drain(&mut self) {
        while let Some(job) = self.microtasks.pop_front() {
            match job {
                Job::Run(ops) => self.ops(&ops),
                Job::Resume(id) => self.resume(id),
            }
            if self.crashed {
                return;
            }
        }
    }

    /// Runs interval `id`'s callback, then re-arms it on its schedule unless
    /// it was cancelled (by itself, or because its runs are used up).
    fn fire_interval(&mut self, id: usize) {
        let Some(state) = self.intervals.get(&id) else {
            return;
        };
        let ops = state.ops.clone();
        self.ops(&ops);
        let Some(state) = self.intervals.get_mut(&id) else {
            return; // It cancelled itself.
        };
        state.runs_left -= 1;
        if state.runs_left == 0 {
            self.intervals.remove(&id);
            return;
        }
        state.deadline = next_interval_deadline(state.deadline, state.period, self.now);
        let deadline = state.deadline;
        self.add_timer_at(deadline, TimerJob::Interval(id));
    }

    /// Fires every timer due at the phase's start time that existed when the
    /// phase started, draining after each. Returns whether any fired.
    fn timers_phase(&mut self) -> bool {
        let now = self.now;
        let cutoff = self.next_seq;
        let mut fired = false;
        loop {
            let due = self
                .timers
                .keys()
                .take_while(|(deadline, _)| *deadline <= now)
                .find(|(_, seq)| *seq < cutoff)
                .copied();
            let Some(key) = due else {
                return fired;
            };
            fired = true;
            match self.timers.remove(&key).unwrap() {
                TimerJob::Once(_, ops) => self.ops(&ops),
                TimerJob::Interval(id) => self.fire_interval(id),
                // A sleep's timer wakes its task: a microtask.
                TimerJob::Wake(id) => self.microtasks.push_back(Job::Resume(id)),
            }
            if self.crashed {
                return true;
            }
            self.drain();
            if self.crashed {
                return true;
            }
        }
    }

    fn run(program: &Body) -> Outcome {
        let mut model = Model::default();
        // Main's first poll runs directly, as a macrotask.
        let main = model.spawn(program.clone(), None);
        model.resume(main);
        loop {
            if !model.crashed {
                model.drain();
            }
            if model.crashed {
                return (model.log, true);
            }
            if model.timers_phase() {
                continue;
            }
            // Idle: jump to the next timer, or finish.
            match model.timers.keys().next() {
                Some(&(deadline, _)) => model.now = model.now.max(deadline),
                None => return (model.log, false),
            }
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 8 } else { 2048 },
        ..ProptestConfig::default()
    })]

    #[test]
    fn spooly_matches_the_reference_model(program in program_strategy()) {
        let expected = Model::run(&program);
        let actual = run_on_spooly(&program);
        prop_assert_eq!(actual, expected);
    }
}

/// Sanity checks of the model itself, against hand-computed traces.
mod model_sanity {
    use super::*;

    fn body(steps: Vec<Step>) -> Body {
        Body(Rc::new(steps))
    }

    fn check(program: &Body, expected: &[u32]) {
        check_outcome(program, expected, false);
    }

    fn check_outcome(program: &Body, expected: &[u32], crashed: bool) {
        let expected = (expected.to_vec(), crashed);
        assert_eq!(Model::run(program), expected, "model");
        assert_eq!(run_on_spooly(program), expected, "spooly");
    }

    #[test]
    fn microtask_queued_by_a_microtask_runs_after_earlier_ones() {
        // micro(log 0; micro(log 1)); micro(log 2)
        let program = body(vec![Step::Ops(vec![
            Op::Micro(vec![Op::Log(0), Op::Micro(vec![Op::Log(1)])]),
            Op::Micro(vec![Op::Log(2)]),
        ])]);
        check(&program, &[0, 2, 1]);
    }

    #[test]
    fn join_hops_interleave_with_a_yielding_sibling() {
        let sibling = body(vec![
            Step::Ops(vec![Op::Log(10)]),
            Step::Yield,
            Step::Ops(vec![Op::Log(11)]),
            Step::Yield,
            Step::Ops(vec![Op::Log(12)]),
        ]);
        let child = body(vec![Step::Ops(vec![Op::Log(1)])]);
        let program = body(vec![
            Step::Ops(vec![Op::Spawn(0, sibling)]),
            Step::Join(child),
            Step::Ops(vec![Op::Log(2)]),
        ]);
        // sibling 10 → child 1 → sibling 11 → main 2 → sibling 12
        check(&program, &[10, 1, 11, 2, 12]);
    }

    #[test]
    fn microtasks_drain_between_timers() {
        // timer(0: log 0; micro(log 1)); timer(0: log 2)
        let program = body(vec![Step::Ops(vec![
            Op::Timer(0, 0, vec![Op::Log(0), Op::Micro(vec![Op::Log(1)])]),
            Op::Timer(1, 0, vec![Op::Log(2)]),
        ])]);
        check(&program, &[0, 1, 2]);
    }

    #[test]
    fn timer_armed_mid_phase_waits_for_the_next_phase() {
        // timer(0: log 0; timer(0: log 1)); timer(0: log 2)
        let program = body(vec![Step::Ops(vec![
            Op::Timer(0, 0, vec![Op::Log(0), Op::Timer(1, 0, vec![Op::Log(1)])]),
            Op::Timer(2, 0, vec![Op::Log(2)]),
        ])]);
        check(&program, &[0, 2, 1]);
    }

    #[test]
    fn sleep_resumes_between_timers() {
        // main: timer(5: log 0); sleep(5); log 1; with another timer(5: log 2)
        // registered after the sleep by a microtask.
        let program = body(vec![
            Step::Ops(vec![
                Op::Timer(0, 5, vec![Op::Log(0)]),
                Op::Micro(vec![Op::Timer(1, 5, vec![Op::Log(2)])]),
            ]),
            Step::Sleep(5),
            Step::Ops(vec![Op::Log(1)]),
        ]);
        // The sleep is registered (during main's first poll) before the
        // microtask's timer.
        check(&program, &[0, 1, 2]);
    }

    #[test]
    fn empty_program() {
        check(&body(vec![]), &[]);
    }

    #[test]
    fn intervals_interleave_with_timeouts() {
        // interval(2, 3 runs: log 0); timer(3: log 1); timer(5: log 2)
        // Runs at 2, 4, 6; timeouts at 3 and 5.
        let program = body(vec![Step::Ops(vec![
            Op::Interval(0, 2, 3, vec![Op::Log(0)]),
            Op::Timer(1, 3, vec![Op::Log(1)]),
            Op::Timer(2, 5, vec![Op::Log(2)]),
        ])]);
        check(&program, &[0, 1, 0, 2, 0]);
    }

    #[test]
    fn a_slow_interval_skips_runs_and_keeps_its_schedule() {
        // interval(2, 3 runs: log 0; busy 5): runs at 2 (until 7), then the
        // 4 and 6 runs are skipped: 8 (until 13), then 14. A timer at 9
        // shows where the schedule is: it fires after the 8 run.
        let program = body(vec![Step::Ops(vec![
            Op::Interval(0, 2, 3, vec![Op::Log(0), Op::Busy(5)]),
            Op::Timer(1, 9, vec![Op::Log(1)]),
        ])]);
        check(&program, &[0, 0, 1, 0]);
    }

    #[test]
    fn cancelling_timers_and_intervals() {
        // timer#0(1: log 0); interval#1(1, 3 runs: log 1); timer#2(2: cancel
        // #0 (already fired), cancel #1 (after one run))
        let program = body(vec![Step::Ops(vec![
            Op::Timer(0, 1, vec![Op::Log(0)]),
            Op::Interval(1, 1, 3, vec![Op::Log(1)]),
            Op::Timer(2, 2, vec![Op::Cancel(0), Op::Cancel(1), Op::Log(2)]),
        ])]);
        // At 1: log 0, log 1. At 2: the interval was re-armed for 2 after
        // timer#2, so timer#2 fires first and cancels it.
        check(&program, &[0, 1, 2]);
    }

    #[test]
    fn an_interval_cancelling_itself() {
        let program = body(vec![Step::Ops(vec![Op::Interval(
            0,
            1,
            3,
            vec![Op::Log(0), Op::Cancel(0)],
        )])]);
        check(&program, &[0]);
    }

    #[test]
    fn abort_cancels_a_sleeping_task_and_its_timer() {
        // spawn#0(sleep 5; log 0); timer(1: log 1); yield; abort#0
        // log 0 never happens.
        let victim = body(vec![Step::Sleep(5), Step::Ops(vec![Op::Log(0)])]);
        let program = body(vec![
            Step::Ops(vec![
                Op::Spawn(0, victim),
                Op::Timer(0, 1, vec![Op::Log(1)]),
            ]),
            Step::Yield,
            Step::Ops(vec![Op::Abort(0)]),
        ]);
        check(&program, &[1]);
    }

    #[test]
    fn a_task_aborting_itself_runs_until_it_suspends() {
        let victim = body(vec![
            Step::Ops(vec![Op::Abort(0), Op::Log(0)]),
            Step::Yield,
            Step::Ops(vec![Op::Log(1)]),
        ]);
        let program = body(vec![Step::Ops(vec![Op::Spawn(0, victim)])]);
        check(&program, &[0]);
    }

    #[test]
    fn a_joined_panic_crashes() {
        let child = body(vec![Step::Ops(vec![Op::Log(0)]), Step::Panic]);
        let program = body(vec![Step::Join(child), Step::Ops(vec![Op::Log(1)])]);
        check_outcome(&program, &[0], true);
    }

    #[test]
    fn a_detached_panic_crashes_at_once() {
        let bad = body(vec![Step::Ops(vec![Op::Log(1)]), Step::Panic]);
        let after = body(vec![Step::Ops(vec![Op::Log(2)])]);
        let program = body(vec![Step::Ops(vec![
            Op::Spawn(0, bad),
            Op::Spawn(1, after),
            Op::Log(0),
        ])]);
        check_outcome(&program, &[0, 1], true);
    }

    #[test]
    fn a_panic_in_main_crashes_before_queued_work() {
        let program = body(vec![
            Step::Ops(vec![Op::Micro(vec![Op::Log(1)]), Op::Log(0)]),
            Step::Panic,
        ]);
        check_outcome(&program, &[0], true);
    }

    #[test]
    fn aborting_a_parent_detaches_its_joined_child() {
        // The child outlives its aborted parent, and its later panic still
        // crashes the run.
        let child = body(vec![Step::Yield, Step::Ops(vec![Op::Log(1)]), Step::Panic]);
        let parent = body(vec![Step::Join(child), Step::Ops(vec![Op::Log(9)])]);
        let program = body(vec![
            Step::Ops(vec![Op::Spawn(0, parent)]),
            Step::Yield,
            Step::Ops(vec![Op::Abort(0), Op::Log(0)]),
        ]);
        check_outcome(&program, &[0, 1], true);
    }
}
