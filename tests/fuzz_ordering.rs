//! Randomized differential tests against real Node (§8.1).
//!
//! `proptest` generates random programs of `next_tick`, `queue_microtask`,
//! `set_timeout`, `set_interval`, timer cancellation, `spawn`, `yield_now`,
//! `sleep`, and joins. Each program runs (a) in Spool, on a `MockClock`, and
//! (b) emitted as JS, in Node, each program in its own `worker_threads`
//! worker (a fresh event loop, which exits when empty, like `run`). The
//! traces must match.
//!
//! Node runs in real time, so the generator only uses delays that are
//! multiples of 20ms, far apart compared to timer jitter. One Node detail is
//! deliberately avoided rather than matched: when two timers of *different*
//! durations share a deadline, Node fires them in the order their
//! per-duration timer lists were created, not in timer creation order (as
//! Spool does). Programs where that can happen are detected while running on
//! Spool and skipped. A mismatch is re-run in Node twice more before it
//! counts, since a heavily loaded machine can still delay a timer by 20ms.
//!
//! Environment:
//! - `SPOOL_FUZZ_CASES`: how many programs to compare (default 160).
//! - `SPOOL_FUZZ_SEED`: the generator seed (default 0), to explore more.
//! - `SPOOL_REQUIRE_NODE=1`: fail instead of skipping if `node` is missing.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::future::Future;
use std::io::Write as _;
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::rc::Rc;
use std::time::Duration;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

// ── Programs ────────────────────────────────────────────────────────────────

/// A synchronous operation.
#[derive(Debug, Clone)]
enum Op {
    /// Record an event. Ids are assigned after generation, in tree order.
    Log(u32),
    /// `next_tick` / `process.nextTick`.
    Tick(Vec<Op>),
    /// `queue_microtask` / `queueMicrotask`.
    Micro(Vec<Op>),
    /// `set_timeout` / `setTimeout`, stored in a timer slot.
    Timer(u32, u64, Vec<Op>),
    /// `set_interval` / `setInterval` that cancels itself after its Nth tick,
    /// stored in a timer slot.
    Interval(u32, u64, u32, Vec<Op>),
    /// Cancel the timer in a slot (`TimerHandle::cancel` / `clearTimeout`),
    /// if it exists.
    Cancel(u32),
    /// `spawn` a detached task / `queueMicrotask(async () => ..)`.
    Spawn(Body),
}

/// One step of an async body.
#[derive(Debug, Clone)]
enum Step {
    Ops(Vec<Op>),
    /// `yield_now().await` / `await null`.
    Yield,
    /// `sleep(ms).await` / `await new Promise(r => setTimeout(r, ms))`.
    Sleep(u64),
    /// `spawn(body).await` / a deferred promise resolved at the end of the
    /// child.
    Join(Body),
}

#[derive(Debug, Clone)]
struct Body(Rc<Vec<Step>>);

#[derive(Default)]
struct Numbering {
    log: u32,
    slot: u32,
}

fn number_ops(ops: &mut [Op], n: &mut Numbering) {
    for op in ops {
        match op {
            Op::Log(id) => {
                *id = n.log;
                n.log += 1;
            }
            Op::Tick(ops) | Op::Micro(ops) => number_ops(ops, n),
            Op::Timer(slot, _, ops) | Op::Interval(slot, _, _, ops) => {
                *slot = n.slot;
                n.slot += 1;
                number_ops(ops, n);
            }
            Op::Cancel(_) => {}
            Op::Spawn(body) => number_body(body, n),
        }
    }
}

fn number_body(body: &mut Body, n: &mut Numbering) {
    for step in Rc::make_mut(&mut body.0) {
        match step {
            Step::Ops(ops) => number_ops(ops, n),
            Step::Yield | Step::Sleep(_) => {}
            Step::Join(child) => number_body(child, n),
        }
    }
}

// ── Strategies ──────────────────────────────────────────────────────────────

fn delay() -> impl Strategy<Value = u64> {
    prop_oneof![Just(20), Just(40), Just(60)]
}

fn body_strategy(op: BoxedStrategy<Op>, join_depth: u32) -> BoxedStrategy<Body> {
    let ops = prop::collection::vec(op.clone(), 0..4).prop_map(Step::Ops);
    let simple = prop_oneof![
        4 => ops,
        1 => Just(Step::Yield),
        1 => delay().prop_map(Step::Sleep),
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
        .prop_recursive(3, 32, 4, |inner| {
            let inner = inner.boxed();
            let ops = || prop::collection::vec(inner.clone(), 0..3);
            prop_oneof![
                3 => Just(Op::Log(0)),
                2 => ops().prop_map(Op::Tick),
                2 => ops().prop_map(Op::Micro),
                2 => (delay(), ops()).prop_map(|(ms, ops)| Op::Timer(0, ms, ops)),
                1 => (prop_oneof![Just(20), Just(40)], 1..=3_u32, ops())
                    .prop_map(|(ms, n, ops)| Op::Interval(0, ms, n, ops)),
                1 => (0..6_u32).prop_map(Op::Cancel),
                1 => body_strategy(inner.clone(), 1).prop_map(Op::Spawn),
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

#[derive(Clone, Default)]
struct Ctx {
    log: Rc<RefCell<Vec<u32>>>,
    timers: Rc<RefCell<HashMap<u32, spool::TimerHandle>>>,
    /// Every armed timer, as `(deadline ms, duration ms)`.
    armed: Rc<RefCell<Vec<(u64, u64)>>>,
}

impl Ctx {
    fn arm(&self, duration: u64) {
        let now = spool::now().epoch_offset().as_millis() as u64;
        self.armed.borrow_mut().push((now + duration, duration));
    }

    /// Two timers of different durations sharing a deadline: Node's order
    /// for them is an artifact of its timer lists (see the module docs).
    fn is_ambiguous(&self) -> bool {
        let mut by_deadline: HashMap<u64, u64> = HashMap::new();
        self.armed.borrow().iter().any(|&(deadline, duration)| {
            *by_deadline.entry(deadline).or_insert(duration) != duration
        })
    }
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
            Op::Timer(slot, delay, ops) => {
                ctx.arm(*delay);
                let (ops, c) = (ops.clone(), ctx.clone());
                let handle = spool::set_timeout(ms(*delay), move || spool_ops(&ops, &c));
                ctx.timers.borrow_mut().insert(*slot, handle);
            }
            Op::Interval(slot, period, times, ops) => {
                ctx.arm(*period);
                let (ops, c, period, times) = (ops.clone(), ctx.clone(), *period, *times);
                let mut ticks = 0;
                let handle = spool::set_interval(ms(period), move |handle| {
                    spool_ops(&ops, &c);
                    ticks += 1;
                    if ticks == times {
                        handle.cancel();
                    }
                    if handle.is_active() {
                        c.arm(period); // Re-armed from this phase's time.
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
            Op::Spawn(body) => drop(spool::spawn(spool_body(body.clone(), ctx.clone()))),
        }
    }
}

fn spool_body(body: Body, ctx: Ctx) -> Pin<Box<dyn Future<Output = ()>>> {
    Box::pin(async move {
        for step in body.0.iter() {
            match step {
                Step::Ops(ops) => spool_ops(ops, &ctx),
                Step::Yield => spool::yield_now().await,
                Step::Sleep(delay) => {
                    ctx.arm(*delay);
                    spool::sleep(ms(*delay)).await;
                }
                Step::Join(child) => spool::spawn(spool_body(child.clone(), ctx.clone())).await,
            }
        }
    })
}

/// Runs a program on Spool. Returns its trace, or `None` if the program is
/// ambiguous in Node.
fn run_on_spool(program: &Body) -> Option<Vec<u32>> {
    let ctx = Ctx::default();
    let rt = spool::Runtime::builder()
        .clock(spool::MockClock::new())
        .build();
    rt.run(spool_body(program.clone(), ctx.clone()));
    drop(rt);
    if ctx.is_ambiguous() {
        return None;
    }
    Some(ctx.log.take())
}

// ── Emitting JS ─────────────────────────────────────────────────────────────

fn js_ops(ops: &[Op], out: &mut String) {
    for op in ops {
        match op {
            Op::Log(id) => write!(out, "L({id});").unwrap(),
            Op::Tick(ops) => {
                out.push_str("process.nextTick(() => {");
                js_ops(ops, out);
                out.push_str("});");
            }
            Op::Micro(ops) => {
                out.push_str("queueMicrotask(() => {");
                js_ops(ops, out);
                out.push_str("});");
            }
            Op::Timer(slot, delay, ops) => {
                write!(out, "T[{slot}] = setTimeout(() => {{").unwrap();
                js_ops(ops, out);
                write!(out, "}}, {delay});").unwrap();
            }
            Op::Interval(slot, period, times, ops) => {
                write!(out, "{{ let k = 0; const h = setInterval(() => {{").unwrap();
                js_ops(ops, out);
                write!(
                    out,
                    "if (++k === {times}) clearInterval(h); }}, {period}); T[{slot}] = h; }}"
                )
                .unwrap();
            }
            Op::Cancel(slot) => write!(out, "clearTimeout(T[{slot}]);").unwrap(),
            Op::Spawn(body) => {
                out.push_str("queueMicrotask(async () => {");
                js_body(body, out);
                out.push_str("});");
            }
        }
    }
}

fn js_body(body: &Body, out: &mut String) {
    for step in body.0.iter() {
        match step {
            Step::Ops(ops) => js_ops(ops, out),
            Step::Yield => out.push_str("await null;"),
            Step::Sleep(delay) => write!(out, "await sleep({delay});").unwrap(),
            Step::Join(child) => {
                out.push_str("{ const d = deferred(); queueMicrotask(async () => {");
                js_body(child, out);
                out.push_str("d.resolve(); }); await d.promise; }");
            }
        }
    }
}

/// The worker script for one program. Events go into a shared buffer, which
/// is read once the worker exits (messages could be lost at exit).
fn js_program(program: &Body) -> String {
    let mut out = String::from(
        "const { workerData } = require('node:worker_threads');\
         const B = new Int32Array(workerData);\
         function L(id) { const i = Atomics.add(B, 0, 1); if (i < B.length - 1) B[1 + i] = id; }\
         const T = [];\
         const sleep = (ms) => new Promise((r) => setTimeout(r, ms));\
         function deferred() { let resolve; const promise = new Promise((r) => (resolve = r)); \
         return { promise, resolve }; }\
         async function main() {",
    );
    js_body(program, &mut out);
    out.push_str("} main();");
    out
}

/// A JS string literal holding `s`.
fn js_string(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

const HARNESS: &str = r#"
const { Worker } = require('node:worker_threads');
const MAX = 8192;
function runOne(code) {
  const sab = new SharedArrayBuffer(4 * (MAX + 1));
  const worker = new Worker(code, { eval: true, workerData: sab });
  return new Promise((resolve) => {
    let error = null;
    worker.on('error', (e) => (error = e));
    worker.on('exit', () => {
      if (error) return resolve('ERR ' + String(error).replace(/\n/g, ' '));
      const b = new Int32Array(sab);
      if (b[0] > MAX) return resolve('ERR trace overflow');
      resolve(Array.from(b.subarray(1, 1 + b[0])).join(','));
    });
  });
}
(async () => {
  const results = new Array(PROGRAMS.length);
  let next = 0;
  async function lane() {
    while (next < PROGRAMS.length) {
      const i = next++;
      results[i] = await runOne(PROGRAMS[i]);
    }
  }
  await Promise.all(Array.from({ length: LANES }, lane));
  for (let i = 0; i < results.length; i++) console.log(i + ':' + results[i]);
})();
"#;

/// Runs each program in Node. `Ok(None)` if `node` isn't available.
fn run_on_node(programs: &[String], lanes: usize) -> Option<Vec<Result<Vec<u32>, String>>> {
    let mut script = String::from("const PROGRAMS = [");
    for p in programs {
        script.push_str(&js_string(p));
        script.push(',');
    }
    write!(script, "];\nconst LANES = {lanes};\n{HARNESS}").unwrap();

    let child = Command::new("node")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    let mut child = match child {
        Ok(child) => child,
        Err(err) => {
            if std::env::var_os("SPOOL_REQUIRE_NODE").is_some() {
                panic!("SPOOL_REQUIRE_NODE is set but `node` could not run: {err}");
            }
            eprintln!(
                "skipping the randomized Node differential test: `node` not available ({err})"
            );
            return None;
        }
    };
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "node failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("node output is UTF-8");
    let mut results = vec![Err("missing".to_string()); programs.len()];
    for line in stdout.lines() {
        let (index, rest) = line.split_once(':').expect("an `i:trace` line");
        let index: usize = index.parse().unwrap();
        results[index] = if let Some(err) = rest.strip_prefix("ERR ") {
            Err(err.to_string())
        } else if rest.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(rest.split(',').map(|id| id.parse().unwrap()).collect())
        };
    }
    Some(results)
}

// ── The test ────────────────────────────────────────────────────────────────

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map(|v| {
            v.parse()
                .unwrap_or_else(|_| panic!("{name} must be a number"))
        })
        .unwrap_or(default)
}

#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn spool_matches_node_on_random_programs() {
    let cases = env_u64("SPOOL_FUZZ_CASES", 160) as usize;
    let seed = env_u64("SPOOL_FUZZ_SEED", 0);
    let mut seed_bytes = [0u8; 32];
    seed_bytes[..8].copy_from_slice(&seed.to_le_bytes());
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes),
    );
    let strategy = program_strategy();

    // Generate programs, keeping the unambiguous ones.
    let mut programs = Vec::new();
    let mut skipped = 0;
    while programs.len() < cases {
        let program = strategy.new_tree(&mut runner).unwrap().current();
        match run_on_spool(&program) {
            Some(trace) => programs.push((program, trace)),
            None => skipped += 1,
        }
        assert!(
            skipped < 20 * cases,
            "the generator makes too many ambiguous programs"
        );
    }
    eprintln!(
        "fuzz_ordering: seed {seed}, {} programs ({skipped} ambiguous ones skipped)",
        programs.len()
    );

    let js: Vec<String> = programs.iter().map(|(p, _)| js_program(p)).collect();
    let lanes = std::thread::available_parallelism().map_or(4, |n| n.get().clamp(2, 8));
    let Some(node) = run_on_node(&js, lanes) else {
        return;
    };

    let mut failures = Vec::new();
    for (i, ((program, spool_trace), node_result)) in programs.iter().zip(node).enumerate() {
        if node_result.as_ref() == Ok(spool_trace) {
            continue;
        }
        // Re-run alone, twice, to rule out timer jitter under load.
        let retries_match = (0..2).any(|_| {
            run_on_node(std::slice::from_ref(&js[i]), 1)
                .is_some_and(|r| r[0].as_ref() == Ok(spool_trace))
        });
        if retries_match {
            eprintln!("fuzz_ordering: program {i} matched on a re-run (timer jitter)");
            continue;
        }
        failures.push(format!(
            "program {i}:\n  spool: {spool_trace:?}\n  node:  {node_result:?}\n  \
             program: {program:?}\n  js: {}\n",
            js[i]
        ));
    }
    assert!(
        failures.is_empty(),
        "{} of {} programs differ between Spool and Node (seed {seed}):\n{}",
        failures.len(),
        programs.len(),
        failures.join("\n")
    );
}

/// The emitter and the harness, on a fixed program with a known trace.
#[test]
#[cfg_attr(miri, ignore = "spawns a process")]
fn harness_sanity() {
    // main: tick(log 0); micro(log 1); timer(20: log 2); spawn(log 3; yield;
    // log 4); join(log 5); log 6
    let program = Body(Rc::new(vec![
        Step::Ops(vec![
            Op::Tick(vec![Op::Log(0)]),
            Op::Micro(vec![Op::Log(1)]),
            Op::Timer(0, 20, vec![Op::Log(2)]),
            Op::Spawn(Body(Rc::new(vec![
                Step::Ops(vec![Op::Log(3)]),
                Step::Yield,
                Step::Ops(vec![Op::Log(4)]),
            ]))),
        ]),
        Step::Join(Body(Rc::new(vec![Step::Ops(vec![Op::Log(5)])]))),
        Step::Ops(vec![Op::Log(6)]),
    ]));
    let expected = vec![0, 1, 3, 5, 4, 6, 2];
    assert_eq!(run_on_spool(&program), Some(expected.clone()));
    if let Some(node) = run_on_node(&[js_program(&program)], 1) {
        assert_eq!(node[0], Ok(expected));
    }
}
