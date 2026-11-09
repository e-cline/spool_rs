//! TCP: `TcpListener` and `TcpStream`.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::net::{Shutdown, SocketAddr};
use std::rc::Rc;
use std::task::{Context, Poll};

use mio::Interest;

use super::split::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf};
use super::{ToSocketAddrs, each_addr, impl_async_stream, resolve};
use crate::reactor::{Direction, PollEvented};

/// A TCP socket listening for connections.
///
/// ```
/// use futures::io::{AsyncReadExt, AsyncWriteExt};
/// use spooly_io::net::{TcpListener, TcpStream};
///
/// spooly_io::block_on(async {
///     let listener = TcpListener::bind("127.0.0.1:0").await?;
///     let addr = listener.local_addr()?;
///     let server = spooly::spawn(async move {
///         let (mut stream, _) = listener.accept().await?;
///         stream.write_all(b"hello").await?;
///         std::io::Result::Ok(())
///     });
///     let mut stream = TcpStream::connect(addr).await?;
///     let mut reply = String::new();
///     stream.read_to_string(&mut reply).await?;
///     assert_eq!(reply, "hello");
///     server.await
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct TcpListener {
    io: PollEvented<mio::net::TcpListener>,
}

impl TcpListener {
    /// Binds a listener to `addr`, trying each address it resolves to.
    /// Port 0 picks a free port; see [`local_addr`](Self::local_addr).
    ///
    /// # Panics
    ///
    /// Outside a runtime with a [`Driver`](crate::Driver), like every
    /// constructor in this module.
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<TcpListener> {
        each_addr(addr, |addr| Self::new(mio::net::TcpListener::bind(addr)?)).await
    }

    fn new(listener: mio::net::TcpListener) -> io::Result<TcpListener> {
        Ok(TcpListener {
            io: PollEvented::new(listener, Interest::READABLE)?,
        })
    }

    /// Accepts a connection: the new stream and the peer's address.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        poll_fn(|cx| self.poll_accept(cx)).await
    }

    /// Polls for a connection: [`accept`](Self::accept) as a poll function.
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        match self.io.poll_io(cx, Direction::Read, |l| l.accept()) {
            Poll::Ready(Ok((stream, addr))) => {
                Poll::Ready(TcpStream::new(stream).map(|s| (s, addr)))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Wraps a listener from the standard library, putting it in
    /// non-blocking mode.
    pub fn from_std(listener: std::net::TcpListener) -> io::Result<TcpListener> {
        listener.set_nonblocking(true)?;
        Self::new(mio::net::TcpListener::from_std(listener))
    }

    /// Unwraps the standard library listener. It stays in non-blocking mode.
    pub fn into_std(self) -> io::Result<std::net::TcpListener> {
        Ok(self.io.into_inner().into())
    }

    /// The address the listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The IP time-to-live of accepted connections.
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.get_ref().ttl()
    }

    /// Sets the IP time-to-live of accepted connections.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.get_ref().set_ttl(ttl)
    }

    /// Stops waits on this listener from keeping [`run`](crate::run) alive:
    /// for a background accept loop that shouldn't keep the program running.
    pub fn unref(&self) {
        self.io.unref();
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.io.fmt(f)
    }
}

/// A TCP connection.
///
/// It implements [`AsyncRead`](futures_io::AsyncRead) and
/// [`AsyncWrite`](futures_io::AsyncWrite), and so does `&TcpStream`: one task
/// can read while another writes, with no lock. Closing it shuts down the
/// write side; dropping it closes the connection.
pub struct TcpStream {
    io: PollEvented<mio::net::TcpStream>,
}

impl TcpStream {
    /// Connects to `addr`, trying each address it resolves to in turn.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<TcpStream> {
        let mut last = None;
        for addr in resolve(addr).await? {
            match Self::connect_addr(addr).await {
                Ok(stream) => return Ok(stream),
                Err(e) => last = Some(e),
            }
        }
        Err(last.expect("at least one address"))
    }

    async fn connect_addr(addr: SocketAddr) -> io::Result<TcpStream> {
        let stream = Self::new(mio::net::TcpStream::connect(addr)?)?;
        // The connection is established, or has failed, once the OS reports
        // the socket writable. A new socket starts out marked ready, so wait
        // for a real event: until then, Windows already reports the peer
        // address, and the stream would look connected.
        stream.io.clear_ready(Direction::Write);
        // A writable event can also be spurious, which a missing peer
        // address reveals.
        loop {
            stream.writable().await?;
            if let Some(e) = stream.io.get_ref().take_error()? {
                return Err(e);
            }
            if stream.io.saw_error() {
                // The OS reported the failure, but not why.
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "the connection attempt failed",
                ));
            }
            match stream.io.get_ref().peer_addr() {
                Ok(_) => return Ok(stream),
                Err(e) if e.kind() == io::ErrorKind::NotConnected => {
                    stream.io.clear_ready(Direction::Write);
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn new(stream: mio::net::TcpStream) -> io::Result<TcpStream> {
        Ok(TcpStream {
            io: PollEvented::new(stream, Interest::READABLE | Interest::WRITABLE)?,
        })
    }

    /// Wraps a connected stream from the standard library, putting it in
    /// non-blocking mode.
    pub fn from_std(stream: std::net::TcpStream) -> io::Result<TcpStream> {
        stream.set_nonblocking(true)?;
        Self::new(mio::net::TcpStream::from_std(stream))
    }

    /// Unwraps the standard library stream. It stays in non-blocking mode.
    pub fn into_std(self) -> io::Result<std::net::TcpStream> {
        Ok(self.io.into_inner().into())
    }

    /// Waits until the stream may be readable. Readiness can be a false
    /// alarm, so follow it with [`try_read`](Self::try_read) and handle
    /// `WouldBlock`.
    pub async fn readable(&self) -> io::Result<()> {
        poll_fn(|cx| self.io.poll_ready(cx, Direction::Read)).await;
        Ok(())
    }

    /// Waits until the stream may be writable; see
    /// [`readable`](Self::readable).
    pub async fn writable(&self) -> io::Result<()> {
        poll_fn(|cx| self.io.poll_ready(cx, Direction::Write)).await;
        Ok(())
    }

    /// Reads without waiting: `WouldBlock` if no data is available.
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.io
            .try_io(Direction::Read, |mut s| io::Read::read(&mut s, buf))
    }

    /// Writes without waiting: `WouldBlock` if the send buffer is full.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.io
            .try_io(Direction::Write, |mut s| io::Write::write(&mut s, buf))
    }

    /// Reads data without removing it from the queue.
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Read, |s| s.peek(buf))).await
    }

    /// Borrows the read and write halves, for use by two tasks (or two
    /// futures in one task) at once. (Not `split`: that name would be
    /// shadowed by `futures::AsyncReadExt::split`, which takes the stream
    /// by value.)
    pub fn halves(&self) -> (ReadHalf<'_, TcpStream>, WriteHalf<'_, TcpStream>) {
        (ReadHalf(self), WriteHalf(self))
    }

    /// Splits the stream into owned read and write halves, which can move
    /// into separate tasks. [`OwnedReadHalf::reunite`] puts it back.
    pub fn into_split(self) -> (OwnedReadHalf<TcpStream>, OwnedWriteHalf<TcpStream>) {
        let stream = Rc::new(self);
        (OwnedReadHalf(Rc::clone(&stream)), OwnedWriteHalf(stream))
    }

    /// Shuts down the read side, the write side, or both.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.io.get_ref().shutdown(how)
    }

    /// The local address of the connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The address of the peer.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().peer_addr()
    }

    /// Whether Nagle's algorithm is disabled (`TCP_NODELAY`).
    pub fn nodelay(&self) -> io::Result<bool> {
        self.io.get_ref().nodelay()
    }

    /// Disables (`true`) or enables Nagle's algorithm (`TCP_NODELAY`).
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.io.get_ref().set_nodelay(nodelay)
    }

    /// The IP time-to-live.
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.get_ref().ttl()
    }

    /// Sets the IP time-to-live.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.get_ref().set_ttl(ttl)
    }

    /// Stops waits on this stream from keeping [`run`](crate::run) alive.
    pub fn unref(&self) {
        self.io.unref();
    }
}

impl_async_stream!(TcpStream);

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.io.fmt(f)
    }
}
