//! Shared helpers for the integration tests.
#![allow(dead_code)] // Not every test binary uses every helper.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use spooly_io::net::{TcpListener, TcpStream};

/// Aborts the test process if the guard isn't dropped within `limit`, so a
/// hang fails fast (with a message) instead of stalling CI.
pub struct Watchdog(Option<mpsc::Sender<()>>);

pub fn watchdog(name: &'static str) -> Watchdog {
    let (tx, rx) = mpsc::channel::<()>();
    thread::spawn(move || {
        if let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(Duration::from_secs(30)) {
            eprintln!("test `{name}` hung for 30s; aborting");
            std::process::abort();
        }
    });
    Watchdog(Some(tx))
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// A connected pair of TCP streams on loopback: (client, server).
pub async fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = TcpStream::connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;
    Ok((client, server))
}

/// A one-shot channel whose sender works from any thread.
pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let slot = Arc::new(Mutex::new((None, None)));
    (Sender(slot.clone()), Receiver(slot))
}

type Slot<T> = Arc<Mutex<(Option<T>, Option<Waker>)>>;

pub struct Sender<T>(Slot<T>);
pub struct Receiver<T>(Slot<T>);

impl<T> Sender<T> {
    pub fn send(self, value: T) {
        let waker = {
            let mut slot = self.0.lock().unwrap();
            slot.0 = Some(value);
            slot.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut slot = self.0.lock().unwrap();
        match slot.0.take() {
            Some(value) => Poll::Ready(value),
            None => {
                slot.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
