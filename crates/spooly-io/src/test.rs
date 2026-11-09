//! In-memory byte streams for tests: [`pipe`] and [`duplex`].
//!
//! They implement the same [`AsyncRead`] / [`AsyncWrite`] traits as the
//! sockets in [`net`](crate::net), so protocol code written against those
//! traits can be tested without the OS. Nothing here touches a reactor: a
//! write wakes the reader as an ordinary wake on the runtime's thread, so
//! they work on any Spooly runtime (no [`Driver`](crate::Driver) needed),
//! on every target, and deterministically with a
//! [`MockClock`](spooly::MockClock).
//!
//! ```
//! use futures::io::{AsyncReadExt, AsyncWriteExt};
//!
//! spooly::block_on(async {
//!     let (mut client, mut server) = spooly_io::test::duplex(64);
//!     let echo = spooly::spawn(async move {
//!         let mut buf = [0; 4];
//!         server.read_exact(&mut buf).await?;
//!         server.write_all(&buf).await?;
//!         std::io::Result::Ok(())
//!     });
//!     client.write_all(b"ping").await?;
//!     let mut reply = [0; 4];
//!     client.read_exact(&mut reply).await?;
//!     assert_eq!(&reply, b"ping");
//!     echo.await
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Like everything in Spooly they are `!Send`: both ends belong to one
//! runtime's thread.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures_io::{AsyncRead, AsyncWrite};

/// One direction: a bounded byte buffer shared by a reader and a writer.
struct Pipe {
    buf: VecDeque<u8>,
    capacity: usize,
    /// The writer was closed or dropped: once drained, reads return 0.
    write_closed: bool,
    /// The reader was dropped: writes fail with `BrokenPipe`.
    reader_gone: bool,
    reader: Option<Waker>,
    writer: Option<Waker>,
}

fn store(slot: &mut Option<Waker>, waker: &Waker) {
    match slot {
        Some(w) if w.will_wake(waker) => {}
        _ => *slot = Some(waker.clone()),
    }
}

impl Pipe {
    fn poll_read(&mut self, cx: &mut Context<'_>, dst: &mut [u8]) -> Poll<io::Result<usize>> {
        if dst.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.buf.is_empty() {
            if self.write_closed {
                return Poll::Ready(Ok(0));
            }
            store(&mut self.reader, cx.waker());
            return Poll::Pending;
        }
        let n = dst.len().min(self.buf.len());
        for (d, b) in dst.iter_mut().zip(self.buf.drain(..n)) {
            *d = b;
        }
        if let Some(writer) = self.writer.take() {
            writer.wake(); // There is room now.
        }
        Poll::Ready(Ok(n))
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, src: &[u8]) -> Poll<io::Result<usize>> {
        if self.reader_gone {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the reading end was dropped",
            )));
        }
        if self.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the writing end was closed",
            )));
        }
        if src.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let room = self.capacity - self.buf.len();
        if room == 0 {
            store(&mut self.writer, cx.waker());
            return Poll::Pending;
        }
        let n = room.min(src.len());
        self.buf.extend(&src[..n]);
        if let Some(reader) = self.reader.take() {
            reader.wake();
        }
        Poll::Ready(Ok(n))
    }

    fn close_write(&mut self) {
        self.write_closed = true;
        if let Some(reader) = self.reader.take() {
            reader.wake(); // It reads the end now.
        }
    }

    fn drop_reader(&mut self) {
        self.reader_gone = true;
        self.buf.clear();
        if let Some(writer) = self.writer.take() {
            writer.wake(); // It fails now.
        }
    }
}

/// Creates a one-way in-memory pipe that buffers up to `capacity` bytes:
/// writes wait while it is full, and reads wait while it is empty.
///
/// Closing (or dropping) the writer ends the stream: the reader then reads
/// what is left and then 0. Dropping the reader makes further writes fail
/// with [`BrokenPipe`](io::ErrorKind::BrokenPipe).
///
/// ```
/// use futures::io::{AsyncReadExt, AsyncWriteExt};
///
/// spooly::block_on(async {
///     let (mut writer, mut reader) = spooly_io::test::pipe(8);
///     writer.write_all(b"data").await?;
///     writer.close().await?;
///     let mut got = Vec::new();
///     reader.read_to_end(&mut got).await?;
///     assert_eq!(got, b"data");
///     std::io::Result::Ok(())
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
///
/// # Panics
///
/// If `capacity` is 0.
pub fn pipe(capacity: usize) -> (PipeWriter, PipeReader) {
    assert!(
        capacity > 0,
        "spooly-io: a pipe's capacity must be at least 1"
    );
    let pipe = Rc::new(RefCell::new(Pipe {
        buf: VecDeque::new(),
        capacity,
        write_closed: false,
        reader_gone: false,
        reader: None,
        writer: None,
    }));
    (PipeWriter(Rc::clone(&pipe)), PipeReader(pipe))
}

/// The reading end of a [`pipe`].
pub struct PipeReader(Rc<RefCell<Pipe>>);

/// The writing end of a [`pipe`]. Closing it
/// ([`AsyncWriteExt::close`](https://docs.rs/futures/latest/futures/io/trait.AsyncWriteExt.html#method.close))
/// ends the stream.
pub struct PipeWriter(Rc<RefCell<Pipe>>);

impl AsyncRead for PipeReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.0.borrow_mut().poll_read(cx, buf)
    }
}

impl AsyncWrite for PipeWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.borrow_mut().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(())) // Nothing is buffered on this side.
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.borrow_mut().close_write();
        Poll::Ready(Ok(()))
    }
}

impl Drop for PipeReader {
    fn drop(&mut self) {
        self.0.borrow_mut().drop_reader();
    }
}

impl Drop for PipeWriter {
    fn drop(&mut self) {
        self.0.borrow_mut().close_write();
    }
}

impl fmt::Debug for PipeReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pipe = self.0.borrow();
        f.debug_struct("PipeReader")
            .field("buffered", &pipe.buf.len())
            .field("write_closed", &pipe.write_closed)
            .finish()
    }
}

impl fmt::Debug for PipeWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pipe = self.0.borrow();
        f.debug_struct("PipeWriter")
            .field("buffered", &pipe.buf.len())
            .field("capacity", &pipe.capacity)
            .field("reader_gone", &pipe.reader_gone)
            .finish()
    }
}

/// Creates a pair of connected in-memory streams, like a TCP connection:
/// what one end writes, the other reads. Each direction buffers up to
/// `capacity` bytes.
///
/// Closing an end
/// ([`AsyncWriteExt::close`](https://docs.rs/futures/latest/futures/io/trait.AsyncWriteExt.html#method.close))
/// shuts down its writing side: the peer reads the end of the stream, and
/// can still write back. Dropping an end does that and also makes the
/// peer's writes fail with [`BrokenPipe`](io::ErrorKind::BrokenPipe).
///
/// # Panics
///
/// If `capacity` is 0.
pub fn duplex(capacity: usize) -> (DuplexStream, DuplexStream) {
    let (a_writer, b_reader) = pipe(capacity);
    let (b_writer, a_reader) = pipe(capacity);
    (
        DuplexStream {
            reader: a_reader,
            writer: a_writer,
        },
        DuplexStream {
            reader: b_reader,
            writer: b_writer,
        },
    )
}

/// One end of a [`duplex`] pair.
pub struct DuplexStream {
    reader: PipeReader,
    writer: PipeWriter,
}

impl DuplexStream {
    /// Splits the stream into its reading and writing halves, which can be
    /// used by separate tasks.
    pub fn into_split(self) -> (PipeReader, PipeWriter) {
        (self.reader, self.writer)
    }
}

impl AsyncRead for DuplexStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for DuplexStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_close(cx)
    }
}

impl fmt::Debug for DuplexStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuplexStream")
            .field("reader", &self.reader)
            .field("writer", &self.writer)
            .finish()
    }
}
