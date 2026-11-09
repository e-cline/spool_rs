//! The read and write halves of a byte stream (`TcpStream`, `UnixStream`).
//!
//! Everything runs on one thread, so splitting needs no lock: a borrowed
//! half is a shared reference, an owned half an `Rc`.

use std::fmt;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures_io::{AsyncRead, AsyncWrite};

/// The read half of a stream, borrowed with `halves`.
pub struct ReadHalf<'a, T>(pub(crate) &'a T);

/// The write half of a stream, borrowed with `halves`.
///
/// Closing it ([`AsyncWriteExt::close`](https://docs.rs/futures/latest/futures/io/trait.AsyncWriteExt.html#method.close))
/// shuts down the stream's write side.
pub struct WriteHalf<'a, T>(pub(crate) &'a T);

/// The read half of a stream, owned, from `into_split`.
pub struct OwnedReadHalf<T>(pub(crate) Rc<T>);

/// The write half of a stream, owned, from `into_split`.
///
/// Closing it shuts down the stream's write side. Dropping both halves
/// closes the stream.
pub struct OwnedWriteHalf<T>(pub(crate) Rc<T>);

impl<T> OwnedReadHalf<T> {
    /// Puts the stream back together, if `write` came from the same
    /// `into_split`. Returns both halves otherwise.
    pub fn reunite(self, write: OwnedWriteHalf<T>) -> Result<T, (Self, OwnedWriteHalf<T>)> {
        if !Rc::ptr_eq(&self.0, &write.0) {
            return Err((self, write));
        }
        drop(write);
        Ok(Rc::into_inner(self.0).expect("the only two halves"))
    }
}

macro_rules! read_half {
    ($half:ty, $name:literal, $($lt:lifetime)?) => {
        impl<$($lt,)? T> AsyncRead for $half
        where
            for<'r> &'r T: AsyncRead,
        {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut &*self.0).poll_read(cx, buf)
            }

            fn poll_read_vectored(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &mut [io::IoSliceMut<'_>],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut &*self.0).poll_read_vectored(cx, bufs)
            }
        }

        impl<$($lt,)? T: fmt::Debug> fmt::Debug for $half {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple($name).field(&*self.0).finish()
            }
        }
    };
}

macro_rules! write_half {
    ($half:ty, $name:literal, $($lt:lifetime)?) => {
        impl<$($lt,)? T> AsyncWrite for $half
        where
            for<'w> &'w T: AsyncWrite,
        {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut &*self.0).poll_write(cx, buf)
            }

            fn poll_write_vectored(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &[io::IoSlice<'_>],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut &*self.0).poll_write_vectored(cx, bufs)
            }

            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut &*self.0).poll_flush(cx)
            }

            fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut &*self.0).poll_close(cx)
            }
        }

        impl<$($lt,)? T: fmt::Debug> fmt::Debug for $half {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple($name).field(&*self.0).finish()
            }
        }
    };
}

read_half!(ReadHalf<'a, T>, "ReadHalf", 'a);
write_half!(WriteHalf<'a, T>, "WriteHalf", 'a);
read_half!(OwnedReadHalf<T>, "OwnedReadHalf",);
write_half!(OwnedWriteHalf<T>, "OwnedWriteHalf",);
