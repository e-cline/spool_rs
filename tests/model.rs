//! Reference-model tests: `proptest` generates random scheduling programs,
//! and each one is run (a) on Spool and (b) on a tiny, obviously-correct
//! simulator of the queue semantics in plan §3. The traces must match.
//!
//! This runs without Node, so it covers machines where Node isn't installed.
//! (Randomized differential testing against real Node is milestone M5.)

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

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
    /// `spawn` a detached task.
    Spawn(Body),
}

/// One step of an async body.
#[derive(Debug, Clone)]
enum Step {
    Ops(Vec<Op>),
    /// `yield_now().await`
    Yield,
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
            Op::Tick(ops) | Op::Micro(ops) => number_ops(ops, next),
            Op::Spawn(body) => number_body(body, next),
        }
    }
}

fn number_body(body: &mut Body, next: &mut u32) {
    for step in Rc::make_mut(&mut body.0) {
        match step {
            Step::Ops(ops) => number_ops(ops, next),
            Step::Yield => {}
            Step::Join(child) => number_body(child, next),
        }
    }
}

// ── Strategies ──────────────────────────────────────────────────────────────

fn body_strategy(op: BoxedStrategy<Op>, join_depth: u32) -> BoxedStrategy<Body> {
    let ops = prop::collection::vec(op.clone(), 0..4).prop_map(Step::Ops);
    let step = if join_depth == 0 {
        prop_oneof![3 => ops, 1 => Just(Step::Yield)].boxed()
    } else {
        let child = body_strategy(op, join_depth - 1);
        prop_oneof![3 => ops, 1 => Just(Step::Yield), 1 => child.prop_map(Step::Join)].boxed()
    };
    prop::collection::vec(step, 0..5)
        .prop_map(|steps| Body(Rc::new(steps)))
        .boxed()
}

fn op_strategy() -> BoxedStrategy<Op> {
    Just(Op::Log(0))
        .prop_recursive(4, 48, 4, |inner| {
            let inner = inner.boxed();
            prop_oneof![
                2 => Just(Op::Log(0)),
                2 => prop::collection::vec(inner.clone(), 0..4).prop_map(Op::Tick),
                2 => prop::collection::vec(inner.clone(), 0..4).prop_map(Op::Micro),
                1 => body_strategy(inner, 1).prop_map(Op::Spawn),
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
                Step::Join(child) => spool::spawn(spool_body(child.clone(), log.clone())).await,
            }
        }
    })
}

fn run_on_spool(program: &Body) -> Vec<u32> {
    let log = Log::default();
    spool::run(spool_body(program.clone(), log.clone()));
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

    fn ops(&mut self, ops: &[Op]) {
        for op in ops {
            match op {
                Op::Log(id) => self.log.push(*id),
                Op::Tick(ops) => self.ticks.push_back(ops.clone()),
                Op::Micro(ops) => self.microtasks.push_back(Job::Run(ops.clone())),
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
                Step::Join(child) => {
                    let child = self.spawn(child.clone(), Some(id));
                    self.microtasks.push_back(Job::Resume(child));
                    return;
                }
            }
        }
    }

    fn run(program: &Body) -> Vec<u32> {
        let mut model = Model::default();
        // Main's first poll is the "script", run synchronously.
        let main = model.spawn(program.clone(), None);
        model.resume(main);
        // Node's processTicksAndRejections.
        loop {
            while let Some(ops) = model.ticks.pop_front() {
                model.ops(&ops);
            }
            while let Some(job) = model.microtasks.pop_front() {
                match job {
                    Job::Run(ops) => model.ops(&ops),
                    Job::Resume(id) => model.resume(id),
                }
            }
            if model.ticks.is_empty() {
                return model.log;
            }
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 8 } else { 1024 },
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

    #[test]
    fn tick_inside_microtask() {
        // micro(m: log 0; tick(log 1)); micro(log 2); tick(log 3)
        let program = body(vec![Step::Ops(vec![
            Op::Micro(vec![Op::Log(0), Op::Tick(vec![Op::Log(1)])]),
            Op::Micro(vec![Op::Log(2)]),
            Op::Tick(vec![Op::Log(3)]),
        ])]);
        let expected = [3, 0, 2, 1];
        assert_eq!(Model::run(&program), expected);
        assert_eq!(run_on_spool(&program), expected);
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
        let expected = [10, 1, 11, 2, 12];
        assert_eq!(Model::run(&program), expected);
        assert_eq!(run_on_spool(&program), expected);
    }

    #[test]
    fn empty_program() {
        let program = body(vec![]);
        assert!(Model::run(&program).is_empty());
        assert!(run_on_spool(&program).is_empty());
    }
}
