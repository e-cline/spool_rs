//! UDP: `UdpSocket`.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::task::{Context, Poll};

use mio::Interest;

use super::{each_addr, resolve};
use crate::reactor::{Direction, PollEvented};

/// A UDP socket.
///
/// ```
/// use spooly_io::net::UdpSocket;
///
/// spooly_io::block_on(async {
///     let a = UdpSocket::bind("127.0.0.1:0").await?;
///     let b = UdpSocket::bind("127.0.0.1:0").await?;
///     a.send_to(b"ping", b.local_addr()?).await?;
///     let mut buf = [0; 16];
///     let (n, from) = b.recv_from(&mut buf).await?;
///     assert_eq!((&buf[..n], from), (&b"ping"[..], a.local_addr()?));
///     std::io::Result::Ok(())
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct UdpSocket {
    io: PollEvented<mio::net::UdpSocket>,
}

impl UdpSocket {
    /// Binds a socket to `addr`, trying each address it resolves to.
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<UdpSocket> {
        each_addr(addr, |addr| Self::new(mio::net::UdpSocket::bind(addr)?))
    }

    fn new(socket: mio::net::UdpSocket) -> io::Result<UdpSocket> {
        Ok(UdpSocket {
            io: PollEvented::new(socket, Interest::READABLE | Interest::WRITABLE)?,
        })
    }

    /// Wraps a socket from the standard library, putting it in
    /// non-blocking mode.
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<UdpSocket> {
        socket.set_nonblocking(true)?;
        Self::new(mio::net::UdpSocket::from_std(socket))
    }

    /// Unwraps the standard library socket. It stays in non-blocking mode.
    pub fn into_std(self) -> io::Result<std::net::UdpSocket> {
        Ok(self.io.into_inner().into())
    }

    /// Connects the socket to `addr`: [`send`](Self::send) goes there, and
    /// [`recv`](Self::recv) only receives from there.
    pub async fn connect(&self, addr: impl ToSocketAddrs) -> io::Result<()> {
        each_addr(addr, |addr| self.io.get_ref().connect(addr))
    }

    /// Sends a datagram to `target` (its first address). Returns the number
    /// of bytes sent.
    pub async fn send_to(&self, buf: &[u8], target: impl ToSocketAddrs) -> io::Result<usize> {
        let target = resolve(target)?[0];
        poll_fn(|cx| self.poll_send_to(cx, buf, target)).await
    }

    /// Polls to send a datagram to `target`.
    pub fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.io
            .poll_io(cx, Direction::Write, |s| s.send_to(buf, target))
    }

    /// Receives a datagram: its length and sender. A datagram longer than
    /// `buf` is truncated.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| self.poll_recv_from(cx, buf)).await
    }

    /// Polls to receive a datagram.
    pub fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        self.io.poll_io(cx, Direction::Read, |s| s.recv_from(buf))
    }

    /// Receives a datagram without removing it from the queue.
    pub async fn peek_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Read, |s| s.peek_from(buf))).await
    }

    /// Sends a datagram to the connected address.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Write, |s| s.send(buf))).await
    }

    /// Receives a datagram from the connected address.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Read, |s| s.recv(buf))).await
    }

    /// The address the socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The connected address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().peer_addr()
    }

    /// Whether the socket may send broadcast datagrams.
    pub fn broadcast(&self) -> io::Result<bool> {
        self.io.get_ref().broadcast()
    }

    /// Allows (or forbids) sending broadcast datagrams.
    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.io.get_ref().set_broadcast(on)
    }

    /// The IP time-to-live.
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.get_ref().ttl()
    }

    /// Sets the IP time-to-live.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.get_ref().set_ttl(ttl)
    }

    /// Stops waits on this socket from keeping [`run`](crate::run) alive.
    pub fn unref(&self) {
        self.io.unref();
    }
}

impl fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.io.fmt(f)
    }
}
