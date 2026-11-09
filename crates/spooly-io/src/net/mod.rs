//! Networking: TCP, UDP, and (on Unix) Unix-domain sockets.
//!
//! Every type here is `!Send`: it belongs to the runtime it was created on,
//! which must have a [`Driver`](crate::Driver) (as [`run`](crate::run) and
//! [`block_on`](crate::block_on) set up). Byte streams implement the
//! `futures-io` traits ([`AsyncRead`](futures_io::AsyncRead) and
//! [`AsyncWrite`](futures_io::AsyncWrite)), both on the stream and on a
//! shared reference to it, so one task can read while another writes.
//!
//! # Cancellation
//!
//! Dropping an I/O future is always safe. A read, `recv` or `accept` that
//! is dropped before it completes consumed nothing, so no data is lost.
//! A partly done write (`write_all`, say) stops at an unknown point, as
//! everywhere in async Rust. Dropping a socket deregisters and closes it.
//!
//! # Liveness
//!
//! A task waiting on a socket (to read, write, accept, …) keeps
//! [`run`](crate::run) alive; a socket nobody waits on doesn't. A detached
//! task running an accept loop therefore keeps `run` going, as a server
//! should. Call `unref()` on a socket whose waits shouldn't keep `run` alive.
//!
//! A wait that is cancelled while pending (a read inside a `timeout` that
//! expires, say) still counts until the socket is next ready, read from, or
//! dropped.
//!
//! # Address resolution
//!
//! Functions that take an address accept anything that implements
//! [`ToSocketAddrs`] and try each address in turn.
//! Resolving a host name (rather than an IP address) still blocks the
//! thread for now; a later version resolves names on a thread pool.

mod split;
mod tcp;
mod udp;
#[cfg(unix)]
mod unix;

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

pub use split::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf};
pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;
#[cfg(unix)]
pub use unix::{UnixDatagram, UnixListener, UnixStream};

/// Resolves `addr` (this may block, for a host name).
fn resolve(addr: impl ToSocketAddrs) -> io::Result<Vec<SocketAddr>> {
    let addrs: Vec<_> = addr.to_socket_addrs()?.collect();
    if addrs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve to any address",
        ));
    }
    Ok(addrs)
}

/// Runs `f` on each address of `addr` until one succeeds; returns the last
/// error otherwise.
fn each_addr<T>(
    addr: impl ToSocketAddrs,
    mut f: impl FnMut(SocketAddr) -> io::Result<T>,
) -> io::Result<T> {
    let mut last = None;
    for addr in resolve(addr)? {
        match f(addr) {
            Ok(t) => return Ok(t),
            Err(e) => last = Some(e),
        }
    }
    Err(last.expect("at least one address"))
}

/// `AsyncRead` and `AsyncWrite` for a stream type and for a reference to it.
/// The type has a `PollEvented` field `io`, and a `shutdown` method.
macro_rules! impl_async_stream {
    ($ty:ty) => {
        impl futures_io::AsyncRead for $ty {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &mut [u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.io.poll_read(cx, buf)
            }

            fn poll_read_vectored(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                bufs: &mut [std::io::IoSliceMut<'_>],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.io.poll_read_vectored(cx, bufs)
            }
        }

        impl futures_io::AsyncRead for &$ty {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &mut [u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.io.poll_read(cx, buf)
            }

            fn poll_read_vectored(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                bufs: &mut [std::io::IoSliceMut<'_>],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.io.poll_read_vectored(cx, bufs)
            }
        }

        impl futures_io::AsyncWrite for $ty {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.io.poll_write(cx, buf)
            }

            fn poll_write_vectored(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                bufs: &[std::io::IoSlice<'_>],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.io.poll_write_vectored(cx, bufs)
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(())) // Nothing is buffered.
            }

            fn poll_close(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready($crate::net::shutdown_write(
                    self.shutdown(std::net::Shutdown::Write),
                ))
            }
        }

        impl futures_io::AsyncWrite for &$ty {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.io.poll_write(cx, buf)
            }

            fn poll_write_vectored(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                bufs: &[std::io::IoSlice<'_>],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.io.poll_write_vectored(cx, bufs)
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }

            fn poll_close(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready($crate::net::shutdown_write(
                    self.shutdown(std::net::Shutdown::Write),
                ))
            }
        }
    };
}
pub(crate) use impl_async_stream;

/// `poll_close`'s result: closing a stream whose peer already went away
/// isn't an error.
pub(crate) fn shutdown_write(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(e) if e.kind() == io::ErrorKind::NotConnected => Ok(()),
        result => result,
    }
}
