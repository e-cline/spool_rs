//! Reference-model tests: `proptest` generates random scheduling programs,
//! and each one is run (a) on Spool and (b) on a tiny, obviously-correct
//! simulator of the event-loop semantics in plan §3. The traces must match.
//!
//! Programs mix ticks, microtasks, timers, spawned tasks, yields, sleeps,
//! joins, aborts, and panics. Spool runs them on a `MockClock`, and the model
//! keeps its own virtual time, so timers are deterministic.
//!
//! A panic in a joined child is observed by its parent (`try_join`); any
//! other panic crashes the run (plan §6), and the traces must match up to
//! the crash.
//!
//! This runs without Node, so it covers machines where Node isn't installed.
//! (Randomized differential testing against real Node is milestone M5.)

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
    /// `next_tick` a callback that runs these ops.
    Tick(Vec<Op>),
    /// `queue_microtask` a callback that runs these ops.
    Micro(Vec<Op>),
    /// `set_timeout` (delay in ms) a callback that runs these ops.
    Timer(u64, Vec<Op>),
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
}

fn number_ops(ops: &mut [Op], next: &mut Numbering) {
    for op in ops {
        match op {
            Op::Log(id) => {
                *id = next.log;
                next.log += 1;
            }
            Op::Tick(ops) | Op::Micro(ops) | Op::Timer(_, ops) => number_ops(ops, next),
            Op::Spawn(slot, body) => {
                *slot = next.slot;
                next.slot += 1;
                number_body(body, next);
            }
            Op::Abort(_) => {}
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
                2 => Just(Op::Log(0)),
                2 => ops().prop_map(Op::Tick),
                2 => ops().prop_map(Op::Micro),
                2 => (delay(), ops()).prop_map(|(ms, ops)| Op::Timer(ms, ops)),
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

// ── Running on Spool ────────────────────────────────────────────────────────

type Log = Rc<RefCell<Vec<u32>>>;

/// What a running program shares: its trace, and the abort handles of its
/// spawned tasks by slot.
#[derive(Clone, Default)]
struct Ctx {
    log: Log,
    handles: Rc<RefCell<HashMap<u32, spool::AbortHandle>>>,
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

fn spool_ops(ops: &[Op], ctx: &Ctx) {
    for op in ops {
        match op {
            Op::Log(id) => ctx.log.borrow_mut().push(*id),
            Op::Tick(ops) => {
                let (ops, ctx) = (ops.clone(), ctx.clone());
                spool::next_tick(move || spool_ops(&ops, &ctx));
            }
            Op::Micro(ops) => {
                let (ops, ctx) = (ops.clone(), ctx.clone());
                spool::queue_microtask(move || spool_ops(&ops, &ctx));
            }
            Op::Timer(delay, ops) => {
                let (ops, ctx) = (ops.clone(), ctx.clone());
                spool::set_timeout(ms(*delay), move || spool_ops(&ops, &ctx));
            }
            Op::Spawn(slot, body) => {
                let handle = spool::spawn(spool_body(body.clone(), ctx.clone()));
                ctx.handles
                    .borrow_mut()
                    .insert(*slot, handle.abort_handle());
                drop(handle); // Detached.
            }
            Op::Abort(slot) => {
                let handle = ctx.handles.borrow().get(slot).cloned();
                if let Some(handle) = handle {
                    handle.abort();
                }
            }
        }
    }
}

fn spool_body(body: Body, ctx: Ctx) -> Pin<Box<dyn Future<Output = ()>>> {
    Box::pin(async move {
        for step in body.0.iter() {
            match step {
                Step::Ops(ops) => spool_ops(ops, &ctx),
                Step::Yield => spool::yield_now().await,
                Step::Sleep(delay) => spool::sleep(ms(*delay)).await,
                Step::SleepUntilPast(back) => {
                    let deadline = spool::now()
                        .checked_sub(ms(*back))
                        .unwrap_or(spool::Instant::ZERO);
                    spool::sleep_until(deadline).await;
                }
                Step::Join(child) => {
                    let child = spool::spawn(spool_body(child.clone(), ctx.clone()));
                    // A panic in the child is observed here, and ignored.
                    let _ = child.try_join().await;
                }
                Step::Panic => std::panic::panic_any(MODEL_PANIC),
            }
        }
    })
}

fn run_on_spool(program: &Body) -> Outcome {
    quiet_model_panics();
    let ctx = Ctx::default();
    let rt = spool::Runtime::builder()
        .clock(spool::MockClock::new())
        .build();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.run(spool_body(program.clone(), ctx.clone()));
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
    /// Completed or panicked.
    finished: bool,
    /// Aborted: it never runs again, and its handle on a joined child is
    /// gone.
    aborted: bool,
    /// A joined child panicked, and this task hasn't resumed to observe it
    /// yet. Aborting it now leaves the panic unobserved.
    holds_panic: bool,
}

enum Job {
    Run(Vec<Op>),
    Resume(usize),
}

#[derive(Default)]
struct Model {
    ticks: VecDeque<Vec<Op>>,
    microtasks: VecDeque<Job>,
    /// Timers keyed by `(deadline in ms, seq)`, so iteration order is firing
    /// order.
    timers: BTreeMap<(u64, u64), Job>,
    next_seq: u64,
    now: u64,
    tasks: Vec<Task>,
    /// Spawn slot → task id.
    slots: HashMap<u32, usize>,
    /// The task being resumed right now.
    current: Option<usize>,
    /// An unobserved panic: the run stops after the current job.
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
            holds_panic: false,
        });
        self.tasks.len() - 1
    }

    fn add_timer(&mut self, delay: u64, job: Job) {
        self.add_timer_at(self.now + delay, job);
    }

    fn add_timer_at(&mut self, deadline: u64, job: Job) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.timers.insert((deadline, seq), job);
    }

    fn ops(&mut self, ops: &[Op]) {
        for op in ops {
            match op {
                Op::Log(id) => self.log.push(*id),
                Op::Tick(ops) => self.ticks.push_back(ops.clone()),
                Op::Micro(ops) => self.microtasks.push_back(Job::Run(ops.clone())),
                Op::Timer(delay, ops) => self.add_timer(*delay, Job::Run(ops.clone())),
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

    /// Drops an aborted task's future: its sleep is cancelled, and a panic
    /// it was holding becomes unobserved.
    fn kill(&mut self, id: usize) {
        self.timers
            .retain(|_, job| !matches!(job, Job::Resume(t) if *t == id));
        if self.tasks[id].holds_panic {
            self.crashed = true;
        }
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
        self.tasks[id].holds_panic = false; // Observed by `try_join`.
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
                    self.add_timer(*delay, Job::Resume(id));
                    return true;
                }
                Step::SleepUntilPast(back) => {
                    self.add_timer_at(self.now.saturating_sub(*back), Job::Resume(id));
                    return true;
                }
                Step::Join(child) => {
                    let child = self.spawn(child.clone(), Some(id));
                    self.microtasks.push_back(Job::Resume(child));
                    return true;
                }
                Step::Panic => {
                    // Like a rejection: delivered to a live joiner (a
                    // microtask hop), or else unobserved.
                    self.tasks[id].finished = true;
                    match self.live_joiner(self.tasks[id].joiner) {
                        Some(joiner) => {
                            self.tasks[joiner].holds_panic = true;
                            self.microtasks.push_back(Job::Resume(joiner));
                        }
                        None => self.crashed = true,
                    }
                    return false;
                }
            }
        }
    }

    /// Node's `processTicksAndRejections`. Stops after the job that crashed.
    fn drain(&mut self) {
        loop {
            while let Some(ops) = self.ticks.pop_front() {
                self.ops(&ops);
                if self.crashed {
                    return;
                }
            }
            while let Some(job) = self.microtasks.pop_front() {
                match job {
                    Job::Run(ops) => self.ops(&ops),
                    Job::Resume(id) => self.resume(id),
                }
                if self.crashed {
                    return;
                }
            }
            if self.ticks.is_empty() {
                return;
            }
        }
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
                Job::Run(ops) => self.ops(&ops),
                // A sleep's timer wakes its task: a microtask.
                Job::Resume(id) => self.microtasks.push_back(Job::Resume(id)),
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
        // Main's first poll is the "script", run synchronously. Main has no
        // joiner, so a panic in main crashes the run, as in Spool.
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
    fn spool_matches_the_reference_model(program in program_strategy()) {
        let expected = Model::run(&program);
        let actual = run_on_spool(&program);
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
        assert_eq!(run_on_spool(program), expected, "spool");
    }

    #[test]
    fn tick_inside_microtask() {
        // micro(m: log 0; tick(log 1)); micro(log 2); tick(log 3)
        let program = body(vec![Step::Ops(vec![
            Op::Micro(vec![Op::Log(0), Op::Tick(vec![Op::Log(1)])]),
            Op::Micro(vec![Op::Log(2)]),
            Op::Tick(vec![Op::Log(3)]),
        ])]);
        check(&program, &[3, 0, 2, 1]);
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
    fn queues_drain_between_timers() {
        // timer(0: log 0; micro(log 1)); timer(0: log 2)
        let program = body(vec![Step::Ops(vec![
            Op::Timer(0, vec![Op::Log(0), Op::Micro(vec![Op::Log(1)])]),
            Op::Timer(0, vec![Op::Log(2)]),
        ])]);
        check(&program, &[0, 1, 2]);
    }

    #[test]
    fn timer_armed_mid_phase_waits_for_the_next_phase() {
        // timer(0: log 0; timer(0: log 1)); timer(0: log 2)
        let program = body(vec![Step::Ops(vec![
            Op::Timer(0, vec![Op::Log(0), Op::Timer(0, vec![Op::Log(1)])]),
            Op::Timer(0, vec![Op::Log(2)]),
        ])]);
        check(&program, &[0, 2, 1]);
    }

    #[test]
    fn sleep_resumes_between_timers() {
        // main: timer(5: log 0); sleep(5); log 1; with another timer(5: log 2)
        // registered after the sleep by a microtask.
        let program = body(vec![
            Step::Ops(vec![
                Op::Timer(5, vec![Op::Log(0)]),
                Op::Micro(vec![Op::Timer(5, vec![Op::Log(2)])]),
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
    fn abort_cancels_a_sleeping_task_and_its_timer() {
        // spawn#0(sleep 5; log 0); timer(1: log 1); yield; abort#0
        // log 0 never happens.
        let victim = body(vec![Step::Sleep(5), Step::Ops(vec![Op::Log(0)])]);
        let program = body(vec![
            Step::Ops(vec![Op::Spawn(0, victim), Op::Timer(1, vec![Op::Log(1)])]),
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
    fn a_joined_panic_is_observed() {
        let child = body(vec![Step::Ops(vec![Op::Log(0)]), Step::Panic]);
        let program = body(vec![Step::Join(child), Step::Ops(vec![Op::Log(1)])]);
        check(&program, &[0, 1]);
    }

    #[test]
    fn a_detached_panic_crashes_after_its_job() {
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
    fn aborting_a_parent_holding_a_childs_panic_crashes() {
        // The parent joins a child that panics; before the parent resumes
        // to observe it, main aborts the parent.
        let child = body(vec![Step::Panic]);
        let parent = body(vec![Step::Join(child), Step::Ops(vec![Op::Log(9)])]);
        let program = body(vec![
            Step::Ops(vec![Op::Spawn(0, parent)]),
            Step::Yield, // parent runs and spawns child; main is re-queued
            Step::Yield, // child panics; parent is queued after main
            Step::Ops(vec![Op::Abort(0), Op::Log(0)]),
        ]);
        check_outcome(&program, &[0], true);
    }

    #[test]
    fn aborting_a_parent_detaches_its_joined_child() {
        // The child outlives its aborted parent; its later panic is then
        // unobserved.
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
