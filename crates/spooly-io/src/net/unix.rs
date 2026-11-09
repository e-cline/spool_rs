//! Unix-domain sockets: `UnixListener`, `UnixStream`, and `UnixDatagram`.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::net::Shutdown;
use std::os::unix::net::SocketAddr;
use std::path::Path;
use std::rc::Rc;
use std::task::{Context, Poll};

use mio::Interest;

use super::split::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf};
use super::{impl_as_handle, impl_async_stream};
use crate::reactor::{Direction, PollEvented};

/// A Unix-domain socket listening for connections.
///
/// ```
/// use futures::io::{AsyncReadExt, AsyncWriteExt};
/// use spooly_io::net::{UnixListener, UnixStream};
///
/// let path = std::env::temp_dir().join(format!("spooly-io-doc-{}.sock", std::process::id()));
/// # let _ = std::fs::remove_file(&path);
/// spooly_io::block_on(async {
///     let listener = UnixListener::bind(&path)?;
///     let mut client = UnixStream::connect(&path).await?;
///     let (mut server, _) = listener.accept().await?;
///     client.write_all(b"hi").await?;
///     let mut buf = [0; 2];
///     server.read_exact(&mut buf).await?;
///     assert_eq!(&buf, b"hi");
///     std::io::Result::Ok(())
/// })?;
/// std::fs::remove_file(&path)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct UnixListener {
    io: PollEvented<mio::net::UnixListener>,
}

impl UnixListener {
    /// Binds a listener to the socket file at `path`, which must not exist.
    ///
    /// # Panics
    ///
    /// Outside a runtime with a [`Driver`](crate::Driver).
    pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixListener> {
        Self::new(mio::net::UnixListener::bind(path)?)
    }

    fn new(listener: mio::net::UnixListener) -> io::Result<UnixListener> {
        Ok(UnixListener {
            io: PollEvented::new(listener, Interest::READABLE)?,
        })
    }

    /// Accepts a connection: the new stream and the peer's address.
    pub async fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
        poll_fn(|cx| self.poll_accept(cx)).await
    }

    /// Polls for a connection: [`accept`](Self::accept) as a poll function.
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(UnixStream, SocketAddr)>> {
        match self.io.poll_io(cx, Direction::Read, |l| l.accept()) {
            Poll::Ready(Ok((stream, addr))) => {
                Poll::Ready(UnixStream::new(stream).map(|s| (s, addr)))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Wraps a listener from the standard library, putting it in
    /// non-blocking mode.
    pub fn from_std(listener: std::os::unix::net::UnixListener) -> io::Result<UnixListener> {
        listener.set_nonblocking(true)?;
        Self::new(mio::net::UnixListener::from_std(listener))
    }

    /// Unwraps the standard library listener. It stays in non-blocking mode.
    pub fn into_std(self) -> io::Result<std::os::unix::net::UnixListener> {
        Ok(self.io.into_inner().into())
    }

    /// The address the listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// Stops waits on this listener from keeping [`run`](crate::run) alive.
    pub fn unref(&self) {
        self.io.unref();
    }
}

impl fmt::Debug for UnixListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.io.fmt(f)
    }
}

/// A Unix-domain stream connection.
///
/// Like [`TcpStream`](super::TcpStream), it and `&UnixStream` implement
/// [`AsyncRead`](futures_io::AsyncRead) and
/// [`AsyncWrite`](futures_io::AsyncWrite).
pub struct UnixStream {
    io: PollEvented<mio::net::UnixStream>,
}

impl UnixStream {
    /// Connects to the socket at `path`.
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<UnixStream> {
        let stream = Self::new(mio::net::UnixStream::connect(path)?)?;
        // Connected (or failed) once the OS reports it writable (a new
        // socket starts out marked ready); a spurious writable event leaves
        // the socket without a peer.
        stream.io.clear_ready(Direction::Write);
        loop {
            stream.writable().await?;
            if let Some(e) = stream.io.get_ref().take_error()? {
                return Err(e);
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

    /// A pair of connected streams.
    pub fn pair() -> io::Result<(UnixStream, UnixStream)> {
        let (a, b) = mio::net::UnixStream::pair()?;
        Ok((Self::new(a)?, Self::new(b)?))
    }

    fn new(stream: mio::net::UnixStream) -> io::Result<UnixStream> {
        Ok(UnixStream {
            io: PollEvented::new(stream, Interest::READABLE | Interest::WRITABLE)?,
        })
    }

    /// Wraps a connected stream from the standard library, putting it in
    /// non-blocking mode.
    pub fn from_std(stream: std::os::unix::net::UnixStream) -> io::Result<UnixStream> {
        stream.set_nonblocking(true)?;
        Self::new(mio::net::UnixStream::from_std(stream))
    }

    /// Unwraps the standard library stream. It stays in non-blocking mode.
    pub fn into_std(self) -> io::Result<std::os::unix::net::UnixStream> {
        Ok(self.io.into_inner().into())
    }

    /// Waits until the stream may be readable; see
    /// [`TcpStream::readable`](super::TcpStream::readable).
    pub async fn readable(&self) -> io::Result<()> {
        poll_fn(|cx| self.io.poll_ready(cx, Direction::Read)).await;
        Ok(())
    }

    /// Waits until the stream may be writable.
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

    /// Borrows the read and write halves, for use by two tasks (or two
    /// futures in one task) at once. (Not `split`: that name would be
    /// shadowed by `futures::AsyncReadExt::split`, which takes the stream
    /// by value.)
    pub fn halves(&self) -> (ReadHalf<'_, UnixStream>, WriteHalf<'_, UnixStream>) {
        (ReadHalf(self), WriteHalf(self))
    }

    /// Splits the stream into owned read and write halves.
    pub fn into_split(self) -> (OwnedReadHalf<UnixStream>, OwnedWriteHalf<UnixStream>) {
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

    /// Stops waits on this stream from keeping [`run`](crate::run) alive.
    pub fn unref(&self) {
        self.io.unref();
    }
}

impl_async_stream!(UnixStream);
impl_as_handle!(UnixListener, UnixStream, UnixDatagram);

impl fmt::Debug for UnixStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.io.fmt(f)
    }
}

/// A Unix-domain datagram socket.
pub struct UnixDatagram {
    io: PollEvented<mio::net::UnixDatagram>,
}

impl UnixDatagram {
    /// Binds a socket to the file at `path`, which must not exist.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixDatagram> {
        Self::new(mio::net::UnixDatagram::bind(path)?)
    }

    /// A socket bound to no address.
    pub fn unbound() -> io::Result<UnixDatagram> {
        Self::new(mio::net::UnixDatagram::unbound()?)
    }

    /// A pair of connected sockets.
    pub fn pair() -> io::Result<(UnixDatagram, UnixDatagram)> {
        let (a, b) = mio::net::UnixDatagram::pair()?;
        Ok((Self::new(a)?, Self::new(b)?))
    }

    fn new(socket: mio::net::UnixDatagram) -> io::Result<UnixDatagram> {
        Ok(UnixDatagram {
            io: PollEvented::new(socket, Interest::READABLE | Interest::WRITABLE)?,
        })
    }

    /// Wraps a socket from the standard library, putting it in
    /// non-blocking mode.
    pub fn from_std(socket: std::os::unix::net::UnixDatagram) -> io::Result<UnixDatagram> {
        socket.set_nonblocking(true)?;
        Self::new(mio::net::UnixDatagram::from_std(socket))
    }

    /// Unwraps the standard library socket. It stays in non-blocking mode.
    pub fn into_std(self) -> io::Result<std::os::unix::net::UnixDatagram> {
        Ok(self.io.into_inner().into())
    }

    /// Connects the socket to the one at `path`.
    pub fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.io.get_ref().connect(path)
    }

    /// Sends a datagram to the socket at `path`.
    pub async fn send_to(&self, buf: &[u8], path: impl AsRef<Path>) -> io::Result<usize> {
        let path = path.as_ref();
        poll_fn(|cx| {
            self.io
                .poll_io(cx, Direction::Write, |s| s.send_to(buf, path))
        })
        .await
    }

    /// Receives a datagram: its length and sender.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Read, |s| s.recv_from(buf))).await
    }

    /// Sends a datagram to the connected socket.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Write, |s| s.send(buf))).await
    }

    /// Receives a datagram from the connected socket.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Read, |s| s.recv(buf))).await
    }

    /// Shuts down the read side, the write side, or both.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.io.get_ref().shutdown(how)
    }

    /// The address the socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().local_addr()
    }

    /// The connected address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.get_ref().peer_addr()
    }

    /// Stops waits on this socket from keeping [`run`](crate::run) alive.
    pub fn unref(&self) {
        self.io.unref();
    }
}

impl fmt::Debug for UnixDatagram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.io.fmt(f)
    }
}
