//! An actor: one task owns some state, and everyone else talks to it through
//! a mailbox. Requests carry a one-shot reply slot.
//!
//! In a multi-threaded runtime the state would need a `Mutex`, or the handle
//! would need `Send + Sync`. Here the state is a plain `HashMap` of `Rc`s:
//! only the actor's task touches it, so there is nothing to lock. The actor
//! stops by itself when the last handle is dropped, and returns its state.
//!
//! The mailbox and reply slot are tiny `Rc<RefCell<..>>` types (see the
//! `channel` example for how this kind of primitive works).
//!
//! Run with: `cargo run --example actor`

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

// --- A one-shot reply slot -------------------------------------------------

type Slot<T> = Rc<RefCell<(Option<T>, Option<Waker>)>>;

struct Reply<T>(Slot<T>);

impl<T> Reply<T> {
    fn send(self, value: T) {
        let waker = {
            let mut s = self.0.borrow_mut();
            s.0 = Some(value);
            s.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

async fn wait_for<T>(slot: &Slot<T>) -> T {
    poll_fn(|cx| {
        let mut s = slot.borrow_mut();
        match s.0.take() {
            Some(value) => Poll::Ready(value),
            None => {
                s.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    })
    .await
}

// --- The mailbox -------------------------------------------------------------

struct Mailbox<M> {
    queue: VecDeque<M>,
    handles: usize,
    waker: Option<Waker>,
}

// --- The actor: a shared score board ------------------------------------

/// Requests the actor understands.
enum Request {
    Add {
        team: Rc<str>,
        points: u32,
    },
    Score {
        team: Rc<str>,
        reply: Reply<u32>,
    },
    Leader {
        reply: Reply<Option<(Rc<str>, u32)>>,
    },
}

/// A cloneable handle to the actor. Sending a request is a synchronous push;
/// only waiting for a reply is async.
struct Board(Rc<RefCell<Mailbox<Request>>>);

impl Board {
    fn start() -> (Board, spooly::JoinHandle<HashMap<Rc<str>, u32>>) {
        let mailbox = Rc::new(RefCell::new(Mailbox {
            queue: VecDeque::new(),
            handles: 1,
            waker: None,
        }));
        let task = spooly::spawn(run_actor(mailbox.clone()));
        (Board(mailbox), task)
    }

    fn push(&self, request: Request) {
        let waker = {
            let mut m = self.0.borrow_mut();
            m.queue.push_back(request);
            m.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn add(&self, team: &str, points: u32) {
        self.push(Request::Add {
            team: team.into(),
            points,
        });
    }

    async fn score(&self, team: &str) -> u32 {
        let slot = Slot::default();
        self.push(Request::Score {
            team: team.into(),
            reply: Reply(slot.clone()),
        });
        wait_for(&slot).await
    }

    async fn leader(&self) -> Option<(Rc<str>, u32)> {
        let slot = Slot::default();
        self.push(Request::Leader {
            reply: Reply(slot.clone()),
        });
        wait_for(&slot).await
    }
}

impl Clone for Board {
    fn clone(&self) -> Self {
        self.0.borrow_mut().handles += 1;
        Board(self.0.clone())
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        let waker = {
            let mut m = self.0.borrow_mut();
            m.handles -= 1;
            if m.handles == 0 { m.waker.take() } else { None }
        };
        if let Some(waker) = waker {
            waker.wake(); // The actor can now see there is nobody left.
        }
    }
}

/// The actor's loop. `scores` is plain data, owned by this task alone.
async fn run_actor(mailbox: Rc<RefCell<Mailbox<Request>>>) -> HashMap<Rc<str>, u32> {
    let mut scores: HashMap<Rc<str>, u32> = HashMap::new();
    loop {
        let request = poll_fn(|cx| {
            let mut m = mailbox.borrow_mut();
            if let Some(request) = m.queue.pop_front() {
                Poll::Ready(Some(request))
            } else if m.handles == 0 {
                Poll::Ready(None)
            } else {
                m.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;

        match request {
            None => return scores,
            Some(Request::Add { team, points }) => *scores.entry(team).or_default() += points,
            Some(Request::Score { team, reply }) => {
                reply.send(scores.get(&team).copied().unwrap_or(0));
            }
            Some(Request::Leader { reply }) => {
                let leader = scores
                    .iter()
                    .max_by_key(|(team, points)| (**points, std::cmp::Reverse((*team).clone())))
                    .map(|(team, points)| (team.clone(), *points));
                reply.send(leader);
            }
        }
    }
}

fn main() {
    spooly::block_on(async {
        let (board, actor) = Board::start();

        // Three clients update the board at different paces. No locks:
        // each request is handled one at a time, in arrival order.
        let clients: Vec<_> = [("red", 10, 3u32), ("blue", 15, 2), ("green", 25, 1)]
            .into_iter()
            .map(|(team, every_ms, points)| {
                let board = board.clone();
                spooly::spawn(async move {
                    for _ in 0..5 {
                        spooly::sleep(Duration::from_millis(every_ms)).await;
                        board.add(team, points);
                    }
                    board.score(team).await // Ask for the final score.
                })
            })
            .collect();

        for client in clients {
            println!("a client saw a final score of {}", client.await);
        }

        let (team, points) = board.leader().await.expect("someone scored");
        println!("leader: {team} with {points}");
        assert_eq!((&*team, points), ("red", 15));

        // Drop the last handle: the actor stops and hands back its state.
        drop(board);
        let mut final_scores: Vec<_> = actor.await.into_iter().collect();
        final_scores.sort();
        println!("final: {final_scores:?}");
        assert_eq!(final_scores.len(), 3);
    });
}
