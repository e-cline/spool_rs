//! A bounded channel that is not `Send`, written from scratch in about 80
//! lines. Because Spooly never moves a task off its thread, a channel is just
//! an `Rc<RefCell<..>>`: no atomics, no locks, and the messages themselves can
//! be `Rc`s.
//!
//! The pattern for any waker-based primitive is the same:
//! - to wait: check the state; if not ready, store `cx.waker()` and return
//!   `Poll::Pending`;
//! - to signal: change the state, then wake the stored waker (after releasing
//!   the `RefCell` borrow).
//!
//! The demo has three producers and one slow consumer. The channel holds two
//! messages, so the producers wait for room (backpressure).
//!
//! Run with: `cargo run --example channel`

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

struct Shared<T> {
    queue: VecDeque<T>,
    capacity: usize,
    senders: usize,
    receiver_alive: bool,
    /// The receiver, waiting for a message.
    recv_waker: Option<Waker>,
    /// Senders, waiting for room.
    send_wakers: Vec<Waker>,
}

struct Sender<T>(Rc<RefCell<Shared<T>>>);
struct Receiver<T>(Rc<RefCell<Shared<T>>>);

/// The message comes back if the receiver is gone.
#[derive(Debug)]
struct SendError<T>(T);

fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(Shared {
        queue: VecDeque::new(),
        capacity,
        senders: 1,
        receiver_alive: true,
        recv_waker: None,
        send_wakers: Vec::new(),
    }));
    (Sender(shared.clone()), Receiver(shared))
}

impl<T> Sender<T> {
    /// Waits for room, then queues `value`.
    async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut value = Some(value);
        poll_fn(|cx| {
            let mut s = self.0.borrow_mut();
            if !s.receiver_alive {
                return Poll::Ready(Err(SendError(value.take().unwrap())));
            }
            if s.queue.len() >= s.capacity {
                s.send_wakers.push(cx.waker().clone());
                return Poll::Pending;
            }
            s.queue.push_back(value.take().unwrap());
            let waker = s.recv_waker.take();
            drop(s); // Release the borrow before waking.
            if let Some(waker) = waker {
                waker.wake();
            }
            Poll::Ready(Ok(()))
        })
        .await
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.borrow_mut().senders += 1;
        Sender(self.0.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut s = self.0.borrow_mut();
        s.senders -= 1;
        let waker = if s.senders == 0 {
            s.recv_waker.take()
        } else {
            None
        };
        drop(s);
        if let Some(waker) = waker {
            waker.wake(); // Let the receiver see that the channel closed.
        }
    }
}

impl<T> Receiver<T> {
    /// The next message, or `None` once every sender is gone and the queue is
    /// empty.
    async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| {
            let mut s = self.0.borrow_mut();
            if let Some(value) = s.queue.pop_front() {
                // Room for one more. Waking every waiting sender is simple;
                // the ones that lose the race just wait again.
                let wakers = std::mem::take(&mut s.send_wakers);
                drop(s);
                wakers.into_iter().for_each(Waker::wake);
                return Poll::Ready(Some(value));
            }
            if s.senders == 0 {
                return Poll::Ready(None);
            }
            s.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut s = self.0.borrow_mut();
        s.receiver_alive = false;
        let wakers = std::mem::take(&mut s.send_wakers);
        drop(s);
        wakers.into_iter().for_each(Waker::wake); // Their sends now fail.
    }
}

fn main() {
    // The messages are `Rc<String>`: not `Send`, and fine here.
    let (tx, mut rx) = channel::<(u32, Rc<String>)>(2);
    let log = Rc::new(RefCell::new(Vec::new()));

    spooly::run(async {
        for id in 1..=3 {
            let tx = tx.clone();
            let log = log.clone();
            drop(spooly::spawn(async move {
                for n in 1..=3 {
                    let msg = Rc::new(format!("producer {id}, message {n}"));
                    tx.send((id, msg)).await.unwrap();
                    println!("  sent:     producer {id}, message {n}");
                    log.borrow_mut().push((id, n));
                }
            }));
        }
        // Only the producers' clones should keep the channel open.
        drop(tx);

        let mut received = Vec::new();
        while let Some((id, msg)) = rx.recv().await {
            println!("received: {msg}");
            received.push(id);
            spooly::sleep(Duration::from_millis(10)).await; // A slow consumer.
        }
        println!("channel closed after {} messages", received.len());
        assert_eq!(received.len(), 9);

        // Each producer's own messages arrive in order.
        let log = log.take();
        for id in 1..=3 {
            let sent: Vec<_> = log
                .iter()
                .filter(|(p, _)| *p == id)
                .map(|(_, n)| *n)
                .collect();
            assert_eq!(sent, [1, 2, 3]);
        }

        // A send to a dropped receiver gives the message back.
        let (tx, rx) = channel::<&str>(1);
        drop(rx);
        let err = tx.send("hello?").await.unwrap_err();
        println!("send after the receiver dropped: {err:?}");
    });
}
