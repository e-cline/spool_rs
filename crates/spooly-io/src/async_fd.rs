//! `Async<T>`: any non-blocking file descriptor, on the reactor (Unix).

use std::fmt;
use std::future::poll_fn;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::{AsyncRead, AsyncWrite};
use mio::event::Source;
use mio::unix::SourceFd;
use mio::{Interest, Registry, Token};

use crate::reactor::{Direction, PollEvented};

/// A file descriptor registered with mio, through `SourceFd`.
struct FdSource<T>(T);

impl<T: AsFd> Source for FdSource<T> {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        SourceFd(&self.0.as_fd().as_raw_fd()).register(registry, token, interest)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        SourceFd(&self.0.as_fd().as_raw_fd()).reregister(registry, token, interest)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        SourceFd(&self.0.as_fd().as_raw_fd()).deregister(registry)
    }
}

/// Any I/O object with a file descriptor, made async through the reactor:
/// pipes, terminals, `eventfd`, sockets from other crates, and so on.
/// (Unix only: on Windows, the reactor can only register its own socket
/// types; convert other sockets to `std::net` ones and use `from_std`.)
///
/// `Async<T>` waits for the descriptor to be readable or writable, then
/// runs an operation on it, retrying when the operation would block. When
/// `T` implements [`Read`] or [`Write`], `Async<T>` implements
/// [`AsyncRead`] or [`AsyncWrite`]; when `&T` does, so does `&Async<T>`.
///
/// **The descriptor must be in non-blocking mode** before it is wrapped
/// (for `std` types, call their `set_nonblocking(true)`). Spooly can't do
/// it for you without `unsafe` code, and can't tell: an operation on a
/// blocking descriptor would block the runtime's whole thread.
///
/// Not every descriptor works with every OS's readiness API: epoll (Linux)
/// rejects regular files, for one, and [`new`](Self::new) returns that
/// error. Use [`fs`](crate::fs) for files.
///
/// ```
/// use futures::io::{AsyncReadExt, AsyncWriteExt};
/// use spooly_io::Async;
/// use std::os::unix::net::UnixStream;
///
/// spooly_io::block_on(async {
///     let (a, b) = UnixStream::pair()?;
///     a.set_nonblocking(true)?;
///     b.set_nonblocking(true)?;
///     let (mut a, mut b) = (Async::new(a)?, Async::new(b)?);
///     a.write_all(b"through std").await?;
///     let mut buf = [0; 11];
///     b.read_exact(&mut buf).await?;
///     assert_eq!(&buf, b"through std");
///     std::io::Result::Ok(())
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Async<T: AsFd> {
    io: PollEvented<FdSource<T>>,
}

impl<T: AsFd> Async<T> {
    /// Registers `io`, which must be in non-blocking mode, with the
    /// current runtime's driver.
    ///
    /// # Errors
    ///
    /// If the OS can't watch the descriptor for readiness.
    ///
    /// # Panics
    ///
    /// Outside a runtime with a [`Driver`](crate::Driver).
    pub fn new(io: T) -> io::Result<Async<T>> {
        Ok(Async {
            io: PollEvented::new(FdSource(io), Interest::READABLE | Interest::WRITABLE)?,
        })
    }

    /// The wrapped I/O object.
    pub fn get_ref(&self) -> &T {
        &self.io.get_ref().0
    }

    /// The wrapped I/O object, mutably. (Don't make it blocking.)
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.io.get_mut().0
    }

    /// Deregisters the descriptor, and returns the I/O object (still in
    /// non-blocking mode).
    pub fn into_inner(self) -> T {
        self.io.into_inner().0
    }

    /// Waits until the descriptor may be readable. The readiness can be
    /// spurious: the next read may still return `WouldBlock`, which
    /// [`read_with`](Self::read_with) handles.
    pub async fn readable(&self) -> io::Result<()> {
        poll_fn(|cx| self.poll_readable(cx)).await
    }

    /// Waits until the descriptor may be writable; see
    /// [`readable`](Self::readable).
    pub async fn writable(&self) -> io::Result<()> {
        poll_fn(|cx| self.poll_writable(cx)).await
    }

    /// Polls for readability: [`readable`](Self::readable) as a poll
    /// function.
    pub fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.poll_ready(cx, Direction::Read).map(Ok)
    }

    /// Polls for writability: [`writable`](Self::writable) as a poll
    /// function.
    pub fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.poll_ready(cx, Direction::Write).map(Ok)
    }

    /// Runs the read-like operation `op` once the descriptor is readable,
    /// again each time it returns `WouldBlock`, and returns its first other
    /// result.
    ///
    /// ```
    /// use spooly_io::Async;
    /// use std::os::unix::net::UnixDatagram;
    ///
    /// spooly_io::block_on(async {
    ///     let (a, b) = UnixDatagram::pair()?;
    ///     a.set_nonblocking(true)?;
    ///     b.set_nonblocking(true)?;
    ///     let (a, b) = (Async::new(a)?, Async::new(b)?);
    ///     a.write_with(|s| s.send(b"datagram")).await?;
    ///     let mut buf = [0; 16];
    ///     let n = b.read_with(|s| s.recv(&mut buf)).await?;
    ///     assert_eq!(&buf[..n], b"datagram");
    ///     std::io::Result::Ok(())
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub async fn read_with<R>(&self, mut op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Read, |s| op(&s.0))).await
    }

    /// [`read_with`](Self::read_with), for an operation that needs `&mut T`.
    pub async fn read_with_mut<R>(
        &mut self,
        mut op: impl FnMut(&mut T) -> io::Result<R>,
    ) -> io::Result<R> {
        poll_fn(|cx| self.io.poll_io_mut(cx, Direction::Read, |s| op(&mut s.0))).await
    }

    /// Runs the write-like operation `op` once the descriptor is writable;
    /// see [`read_with`](Self::read_with).
    pub async fn write_with<R>(&self, mut op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        poll_fn(|cx| self.io.poll_io(cx, Direction::Write, |s| op(&s.0))).await
    }

    /// [`write_with`](Self::write_with), for an operation that needs
    /// `&mut T`.
    pub async fn write_with_mut<R>(
        &mut self,
        mut op: impl FnMut(&mut T) -> io::Result<R>,
    ) -> io::Result<R> {
        poll_fn(|cx| self.io.poll_io_mut(cx, Direction::Write, |s| op(&mut s.0))).await
    }

    /// Stops waits on this descriptor from keeping [`run`](crate::run)
    /// alive.
    pub fn unref(&self) {
        self.io.unref();
    }
}

// Nothing in an `Async` is pinned: `T` is only ever used through `&` and
// `&mut`.
impl<T: AsFd> Unpin for Async<T> {}

impl<T: AsFd> AsFd for Async<T> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.get_ref().as_fd()
    }
}

impl<T: AsFd + Read> AsyncRead for Async<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .io
            .poll_io_mut(cx, Direction::Read, |s| s.0.read(buf))
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .io
            .poll_io_mut(cx, Direction::Read, |s| s.0.read_vectored(bufs))
    }
}

impl<T: AsFd + Write> AsyncWrite for Async<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .io
            .poll_io_mut(cx, Direction::Write, |s| s.0.write(buf))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .io
            .poll_io_mut(cx, Direction::Write, |s| s.0.write_vectored(bufs))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .io
            .poll_io_mut(cx, Direction::Write, |s| s.0.flush())
    }

    /// Flushes; the descriptor itself closes when the `Async` is dropped.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl<T: AsFd> AsyncRead for &Async<T>
where
    for<'a> &'a T: Read,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.io.poll_io(cx, Direction::Read, |s| (&s.0).read(buf))
    }
}

impl<T: AsFd> AsyncWrite for &Async<T>
where
    for<'a> &'a T: Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.io.poll_io(cx, Direction::Write, |s| (&s.0).write(buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.poll_io(cx, Direction::Write, |s| (&s.0).flush())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl<T: AsFd + fmt::Debug> fmt::Debug for Async<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Async").field(self.get_ref()).finish()
    }
}
