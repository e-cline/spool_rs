//! # spooly-io
//!
//! Async network I/O for [Spooly](spooly), the single-threaded executor
//! inspired by the JavaScript event loop. Like the rest of Spooly, nothing
//! here needs `Send`: sockets, futures and tasks can hold `Rc` and
//! `RefCell`.
//!
//! - [`net`]: TCP, UDP, and Unix-domain sockets. Byte streams implement the
//!   `futures-io` traits, so `futures::io::AsyncReadExt` and friends (and
//!   crates built on those traits) work with them. Host names are resolved
//!   on Spooly's blocking pool ([`net::lookup_host`]).
//! - [`fs`]: files and directories, run on Spooly's blocking pool
//!   ([`spooly::spawn_blocking`]), since the OS has no readiness API for
//!   files.
//! - [`Driver`]: the reactor behind them, on the OS's readiness API
//!   (epoll, kqueue, or Windows' AFD) through `mio`.
//!
//! ```
//! use futures::io::{AsyncReadExt, AsyncWriteExt};
//! use spooly_io::net::{TcpListener, TcpStream};
//!
//! spooly_io::run(async {
//!     let listener = TcpListener::bind("127.0.0.1:0").await?;
//!     let addr = listener.local_addr()?;
//!
//!     // An echo server, handling one connection.
//!     drop(spooly::spawn(async move {
//!         let (stream, _) = listener.accept().await?;
//!         let (mut reader, mut writer) = stream.halves();
//!         futures::io::copy(&mut reader, &mut writer).await?;
//!         std::io::Result::Ok(())
//!     }));
//!
//!     let mut client = TcpStream::connect(addr).await?;
//!     client.write_all(b"ping").await?;
//!     client.close().await?; // Done writing: the server sees the end.
//!     let mut reply = String::new();
//!     client.read_to_string(&mut reply).await?;
//!     assert_eq!(reply, "ping");
//!     std::io::Result::Ok(())
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! ## Where I/O fits in the event loop
//!
//! Each step of Spooly's loop drains the microtask queue, runs the timers
//! that are due, and then runs the **I/O phase**: the driver wakes every
//! task whose socket became ready, each as a microtask, and the queue
//! drains again. When there is nothing to do, the loop waits on the OS for
//! I/O, the next timer, or a wake from another thread. A task waiting on a
//! socket keeps [`run`] alive, as an armed timer does (see
//! [`net`](net#liveness)).
//!
//! ## Testing
//!
//! With a [`MockClock`](spooly::MockClock), the loop lets the clock jump to
//! the next timer only when no I/O is ready right now, so a `timeout`
//! around real network I/O usually fires at once. Use real time
//! ([`run`] / [`block_on`]) for tests with real sockets.
//!
//! ## Targets
//!
//! Linux, macOS and other Unix systems, and Windows. On other targets
//! (WASM) the crate compiles, but has no driver and no sockets yet.
//!
//! See `docs/ASYNC_IO_PLAN.md` in the repository for the design.

#![warn(unreachable_pub)]

#[cfg(any(unix, windows))]
pub mod fs;
#[cfg(any(unix, windows))]
pub mod net;
#[cfg(any(unix, windows))]
mod reactor;

#[cfg(any(unix, windows))]
pub use reactor::Driver;

#[cfg(any(unix, windows))]
use std::future::Future;

/// A runtime with a new [`Driver`].
#[cfg(any(unix, windows))]
fn runtime() -> spooly::Runtime {
    let driver = Driver::new().expect("spooly-io: failed to create the I/O driver");
    spooly::Runtime::builder().io_driver(driver).build()
}

/// Runs `future` on a new runtime with an I/O [`Driver`], until it completes
/// **and** the event loop has nothing left to do (no queued work, timers,
/// or tasks waiting on I/O). Returns `future`'s output.
///
/// This is [`spooly::run`] with I/O: see [`spooly::Runtime::run`].
///
/// # Panics
///
/// As [`spooly::Runtime::run`], and if the OS can't create a driver.
#[cfg(any(unix, windows))]
pub fn run<F: Future>(future: F) -> F::Output {
    runtime().run(future)
}

/// Runs `future` to completion on a new runtime with an I/O [`Driver`], and
/// returns its output as soon as it completes.
///
/// This is [`spooly::block_on`] with I/O: see
/// [`spooly::Runtime::block_on`].
///
/// # Panics
///
/// As [`spooly::Runtime::block_on`], and if the OS can't create a driver.
#[cfg(any(unix, windows))]
pub fn block_on<F: Future>(future: F) -> F::Output {
    runtime().block_on(future)
}
