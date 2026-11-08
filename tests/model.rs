//! Reference-model tests: `proptest` generates random scheduling programs,
//! and each one is run (a) on Spool and (b) on a tiny, obviously-correct
//! simulator of the event-loop semantics in plan §3. The traces must match.
//!
//! Programs mix ticks, microtasks, timers, spawned tasks, yields, sleeps, and
//! joins. Spool runs them on a `MockClock`, and the model keeps its own
//! virtual time, so timers are deterministic.
//!
//! This runs without Node, so it covers machines where Node isn't installed.
//! (Randomized differential testing against real Node is milestone M5.)

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
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
    /// `spawn` a detached task.
    Spawn(Body),
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
    /// `spawn(body).await`: run a child task and wait for it.
    Join(Body),
}

/// The body of a task (or of main).
#[derive(Debug, Clone)]
struct Body(Rc<Vec<Step>>);

fn number_ops(ops: &mut [Op], next: &mut u32) {
    for op in ops {
        match op {
            Op::Log(id) => {
                *id = *next;
                *next += 1;
            }
            Op::Tick(ops) | Op::Micro(ops) | Op::Timer(_, ops) => number_ops(ops, next),
            Op::Spawn(body) => number_body(body, next),
        }
    }
}

fn number_body(body: &mut Body, next: &mut u32) {
    for step in Rc::make_mut(&mut body.0) {
        match step {
            Step::Ops(ops) => number_ops(ops, next),
            Step::Yield | Step::Sleep(_) | Step::SleepUntilPast(_) => {}
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
        4 => ops,
        1 => Just(Step::Yield),
        1 => delay().prop_map(Step::Sleep),
        1 => prop_oneof![Just(1), Just(3)].prop_map(Step::SleepUntilPast),
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
                1 => body_strategy(inner.clone(), 1).prop_map(Op::Spawn),
            ]
        })
        .boxed()
}

fn program_strategy() -> impl Strategy<Value = Body> {
    body_strategy(op_strategy(), 2).prop_map(|mut body| {
        number_body(&mut body, &mut 0);
        body
    })
}

// ── Running on Spool ────────────────────────────────────────────────────────

type Log = Rc<RefCell<Vec<u32>>>;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn spool_ops(ops: &[Op], log: &Log) {
    for op in ops {
        match op {
            Op::Log(id) => log.borrow_mut().push(*id),
            Op::Tick(ops) => {
                let (ops, log) = (ops.clone(), log.clone());
                spool::next_tick(move || spool_ops(&ops, &log));
            }
            Op::Micro(ops) => {
                let (ops, log) = (ops.clone(), log.clone());
                spool::queue_microtask(move || spool_ops(&ops, &log));
            }
            Op::Timer(delay, ops) => {
                let (ops, log) = (ops.clone(), log.clone());
                spool::set_timeout(ms(*delay), move || spool_ops(&ops, &log));
            }
            Op::Spawn(body) => drop(spool::spawn(spool_body(body.clone(), log.clone()))),
        }
    }
}

fn spool_body(body: Body, log: Log) -> Pin<Box<dyn Future<Output = ()>>> {
    Box::pin(async move {
        for step in body.0.iter() {
            match step {
                Step::Ops(ops) => spool_ops(ops, &log),
                Step::Yield => spool::yield_now().await,
                Step::Sleep(delay) => spool::sleep(ms(*delay)).await,
                Step::SleepUntilPast(back) => {
                    let deadline = spool::now()
                        .checked_sub(ms(*back))
                        .unwrap_or(spool::Instant::ZERO);
                    spool::sleep_until(deadline).await;
                }
                Step::Join(child) => spool::spawn(spool_body(child.clone(), log.clone())).await,
            }
        }
    })
}

fn run_on_spool(program: &Body) -> Vec<u32> {
    let log = Log::default();
    let rt = spool::Runtime::builder()
        .clock(spool::MockClock::new())
        .build();
    rt.run(spool_body(program.clone(), log.clone()));
    log.take()
}

// ── The reference model ─────────────────────────────────────────────────────

/// A task in the model: a body, a program counter, and (for joined tasks)
/// the task waiting on it.
struct Task {
    body: Body,
    pc: usize,
    joiner: Option<usize>,
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
    log: Vec<u32>,
}

impl Model {
    fn spawn(&mut self, body: Body, joiner: Option<usize>) -> usize {
        self.tasks.push(Task {
            body,
            pc: 0,
            joiner,
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
                Op::Spawn(body) => {
                    let id = self.spawn(body.clone(), None);
                    self.microtasks.push_back(Job::Resume(id));
                }
            }
        }
    }

    /// Runs task `id` until it suspends or finishes.
    fn resume(&mut self, id: usize) {
        loop {
            let body = self.tasks[id].body.clone();
            let Some(step) = body.0.get(self.tasks[id].pc) else {
                // Finished: wake whoever joined it (a microtask hop).
                if let Some(joiner) = self.tasks[id].joiner {
                    self.microtasks.push_back(Job::Resume(joiner));
                }
                return;
            };
            self.tasks[id].pc += 1;
            match step {
                Step::Ops(ops) => self.ops(ops),
                Step::Yield => {
                    self.microtasks.push_back(Job::Resume(id));
                    return;
                }
                Step::Sleep(delay) => {
                    self.add_timer(*delay, Job::Resume(id));
                    return;
                }
                Step::SleepUntilPast(back) => {
                    self.add_timer_at(self.now.saturating_sub(*back), Job::Resume(id));
                    return;
                }
                Step::Join(child) => {
                    let child = self.spawn(child.clone(), Some(id));
                    self.microtasks.push_back(Job::Resume(child));
                    return;
                }
            }
        }
    }

    /// Node's `processTicksAndRejections`.
    fn drain(&mut self) {
        loop {
            while let Some(ops) = self.ticks.pop_front() {
                self.ops(&ops);
            }
            while let Some(job) = self.microtasks.pop_front() {
                match job {
                    Job::Run(ops) => self.ops(&ops),
                    Job::Resume(id) => self.resume(id),
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
            self.drain();
        }
    }

    fn run(program: &Body) -> Vec<u32> {
        let mut model = Model::default();
        // Main's first poll is the "script", run synchronously.
        let main = model.spawn(program.clone(), None);
        model.resume(main);
        loop {
            model.drain();
            if model.timers_phase() {
                continue;
            }
            // Idle: jump to the next timer, or finish.
            match model.timers.keys().next() {
                Some(&(deadline, _)) => model.now = model.now.max(deadline),
                None => return model.log,
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
            Step::Ops(vec![Op::Spawn(sibling)]),
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
}
