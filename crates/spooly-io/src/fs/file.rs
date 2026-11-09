//! `File` and `OpenOptions`.

use std::fmt;
use std::future::{Future, poll_fn};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};

use super::{Metadata, Permissions, asyncify};
use crate::blocking::{self, Job};

/// The most a single read or write moves between the task and the pool
/// thread.
const MAX_BUF: usize = 2 * 1024 * 1024;

/// An open file, with async reads, writes and seeks: it implements
/// [`AsyncRead`], [`AsyncWrite`] and [`AsyncSeek`], so
/// `futures::io::AsyncReadExt` and friends work with it.
///
/// Each operation runs on the blocking pool, one at a time, through an
/// internal buffer of up to 2 MiB:
///
/// - **Writes are write-behind.** A write copies the data into the buffer
///   and returns at once; the system call happens on the pool. An error
///   from it comes back from the next write or flush. **Call
///   [`flush`][flush]** (or
///   [`sync_all`](Self::sync_all)) before relying on what was written, as
///   with a `BufWriter`. Dropping a `File` with a write in flight lets that
///   write finish in the background ([`run`](crate::run) waits for it), but
///   loses its error.
/// - **Reads** fetch up to the caller's buffer size (at most 2 MiB) per
///   system call, and hand out what's left over on the next read.
///
/// Like [`std::fs::File`], it has no buffering beyond that: wrap it in
/// `futures::io::BufReader` for many small reads.
///
/// [flush]: https://docs.rs/futures/latest/futures/io/trait.AsyncWriteExt.html#method.flush
///
/// ```
/// use futures::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
/// use std::io::SeekFrom;
/// use spooly_io::fs::File;
///
/// # let path = std::env::temp_dir().join(format!("spooly-io-file-doc-{}", std::process::id()));
/// spooly::block_on(async {
///     let mut file = File::options().read(true).write(true).create(true).truncate(true).open(&path).await?;
///     file.write_all(b"hello, world").await?;
///     file.seek(SeekFrom::Start(7)).await?;
///     let mut word = String::new();
///     file.read_to_string(&mut word).await?;
///     assert_eq!(word, "world");
///     std::io::Result::Ok(())
/// })?;
/// # std::fs::remove_file(&path)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct File {
    std: Arc<std::fs::File>,
    state: State,
    /// An error from a write-behind write, for the next write or flush.
    last_write_err: Option<io::Error>,
}

enum State {
    Idle(Buf),
    /// An operation on the pool. It owns the buffer until it is done.
    Busy(Job<(Op, Buf)>),
}

/// The result of the operation in flight.
enum Op {
    /// A read: the bytes read are the buffer's contents.
    Read(io::Result<usize>),
    Write(io::Result<()>),
    Seek(io::Result<u64>),
}

/// Data read from the OS but not yet handed out (`data[pos..]`), or data
/// being written.
#[derive(Default)]
struct Buf {
    data: Vec<u8>,
    pos: usize,
}

impl Buf {
    fn unread(&self) -> usize {
        self.data.len() - self.pos
    }

    fn copy_to(&mut self, dst: &mut [u8]) -> usize {
        let n = self.unread().min(dst.len());
        dst[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        n
    }

    /// Discards any unread data, returning how far the OS's file position
    /// is ahead of the caller's because of it (as a negative seek).
    fn discard_read(&mut self) -> i64 {
        let ahead = self.unread() as i64;
        self.data.clear();
        self.pos = 0;
        -ahead
    }
}

/// Retries `op` while it is interrupted.
fn retry<T>(mut op: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match op() {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

impl File {
    /// Opens the file at `path` for reading; as [`std::fs::File::open`].
    pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Opens the file at `path` for writing, creating it, or truncating it
    /// if it exists; as [`std::fs::File::create`].
    pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// Creates a new file at `path` for reading and writing, failing if it
    /// exists; as [`std::fs::File::create_new`].
    pub async fn create_new(path: impl AsRef<Path>) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .await
    }

    /// Returns a new [`OpenOptions`], to open a file in other ways.
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    /// Wraps a file from the standard library.
    pub fn from_std(file: std::fs::File) -> File {
        File {
            std: Arc::new(file),
            state: State::Idle(Buf::default()),
            last_write_err: None,
        }
    }

    /// Unwraps the standard library file, once the operation in flight (if
    /// any) is done. Its position is where this file's was: data read ahead
    /// but not yet handed out is given back. Errors of a write in flight are
    /// lost; call [`flush`][flush] first to see
    /// them.
    ///
    /// [flush]: https://docs.rs/futures/latest/futures/io/trait.AsyncWriteExt.html#method.flush
    pub async fn into_std(mut self) -> std::fs::File {
        let _ = self.complete_in_flight().await;
        let ahead = self.idle_buf().discard_read();
        if ahead != 0 {
            let std = Arc::clone(&self.std);
            // Best effort, as for an error from the write in flight.
            let _ = asyncify(move || retry(|| (&*std).seek(SeekFrom::Current(ahead)))).await;
        }
        Arc::into_inner(self.std).expect("no operation in flight holds the file")
    }

    /// Flushes data and metadata to the storage device, after the write in
    /// flight; as [`std::fs::File::sync_all`].
    pub async fn sync_all(&mut self) -> io::Result<()> {
        self.complete_in_flight().await?;
        let std = Arc::clone(&self.std);
        asyncify(move || std.sync_all()).await
    }

    /// Like [`sync_all`](Self::sync_all), but may skip flushing metadata;
    /// as [`std::fs::File::sync_data`].
    pub async fn sync_data(&mut self) -> io::Result<()> {
        self.complete_in_flight().await?;
        let std = Arc::clone(&self.std);
        asyncify(move || std.sync_data()).await
    }

    /// Truncates or extends the file to `size` bytes, after the write in
    /// flight; as [`std::fs::File::set_len`]. The file's position doesn't
    /// change.
    pub async fn set_len(&mut self, size: u64) -> io::Result<()> {
        self.complete_in_flight().await?;
        let ahead = self.idle_buf().discard_read();
        let std = Arc::clone(&self.std);
        asyncify(move || {
            if ahead != 0 {
                retry(|| (&*std).seek(SeekFrom::Current(ahead)))?;
            }
            std.set_len(size)
        })
        .await
    }

    /// The file's metadata; as [`std::fs::File::metadata`].
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let std = Arc::clone(&self.std);
        asyncify(move || std.metadata()).await
    }

    /// Sets the file's permissions; as [`std::fs::File::set_permissions`].
    pub async fn set_permissions(&self, perm: Permissions) -> io::Result<()> {
        let std = Arc::clone(&self.std);
        asyncify(move || std.set_permissions(perm)).await
    }

    /// Waits for the operation in flight, returning a write error (this
    /// one's or an earlier one's).
    async fn complete_in_flight(&mut self) -> io::Result<()> {
        poll_fn(|cx| Pin::new(&mut *self).poll_flush(cx)).await
    }

    /// The buffer, which the file must have (no operation in flight).
    fn idle_buf(&mut self) -> &mut Buf {
        match &mut self.state {
            State::Idle(buf) => buf,
            State::Busy(_) => unreachable!("called with an operation in flight"),
        }
    }

    /// Takes the idle buffer, to send it with a new operation.
    fn take_buf(&mut self) -> Buf {
        std::mem::take(self.idle_buf())
    }

    /// Starts `op` on the pool with the buffer.
    fn start(
        &mut self,
        buf: Buf,
        op: impl FnOnce(&std::fs::File, Buf) -> (Op, Buf) + Send + 'static,
    ) {
        let std = Arc::clone(&self.std);
        self.state = State::Busy(blocking::spawn(move || op(&std, buf)));
    }

    /// Polls the operation in flight, if any, and puts its buffer back.
    /// Returns its result, or `None` if there was none.
    fn poll_in_flight(&mut self, cx: &mut Context<'_>) -> Poll<Option<Op>> {
        let State::Busy(handle) = &mut self.state else {
            return Poll::Ready(None);
        };
        let (op, buf) = ready!(Pin::new(handle).poll(cx));
        self.state = State::Idle(buf);
        Poll::Ready(Some(op))
    }
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match ready!(this.poll_in_flight(cx)) {
                Some(Op::Read(Ok(0))) => return Poll::Ready(Ok(0)), // The end.
                Some(Op::Read(Err(e))) => return Poll::Ready(Err(e)),
                Some(Op::Write(Err(e))) => this.last_write_err = Some(e),
                // A read's data is in the buffer; a seek was abandoned.
                Some(Op::Read(Ok(_)) | Op::Write(Ok(())) | Op::Seek(_)) | None => {}
            }
            let buf = this.idle_buf();
            if buf.unread() > 0 || dst.is_empty() {
                return Poll::Ready(Ok(buf.copy_to(dst)));
            }
            let mut buf = this.take_buf();
            buf.discard_read(); // Empty already: this resets its position.
            let len = dst.len().min(MAX_BUF);
            this.start(buf, move |mut file, mut buf| {
                buf.data.resize(len, 0);
                let result = retry(|| file.read(&mut buf.data));
                buf.data.truncate(*result.as_ref().unwrap_or(&0));
                (Op::Read(result), buf)
            });
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        src: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(e) = this.last_write_err.take() {
            return Poll::Ready(Err(e));
        }
        if let Some(Op::Write(Err(e))) = ready!(this.poll_in_flight(cx)) {
            return Poll::Ready(Err(e));
        }
        if src.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut buf = this.take_buf();
        // Data read ahead isn't the caller's position: write where the
        // caller is.
        let seek = buf.discard_read();
        let n = src.len().min(MAX_BUF);
        buf.data.extend_from_slice(&src[..n]);
        this.start(buf, move |mut file, mut buf| {
            let result = (|| {
                if seek != 0 {
                    retry(|| file.seek(SeekFrom::Current(seek)))?;
                }
                file.write_all(&buf.data)
            })();
            buf.data.clear();
            (Op::Write(result), buf)
        });
        Poll::Ready(Ok(n))
    }

    /// Waits for the write in flight; it doesn't sync to the device (see
    /// [`File::sync_all`]).
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match ready!(this.poll_in_flight(cx)) {
            Some(Op::Write(Err(e))) => Poll::Ready(Err(e)),
            _ => Poll::Ready(this.last_write_err.take().map_or(Ok(()), Err)),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        loop {
            match ready!(this.poll_in_flight(cx)) {
                // The seek this call (or an earlier, abandoned one) started.
                Some(Op::Seek(result)) => return Poll::Ready(result),
                Some(Op::Write(Err(e))) => this.last_write_err = Some(e),
                Some(_) | None => {}
            }
            let mut buf = this.take_buf();
            let pos = match pos {
                // Relative to the caller's position, behind the OS's by
                // what was read ahead.
                SeekFrom::Current(n) => SeekFrom::Current(n + buf.discard_read()),
                pos => {
                    buf.discard_read();
                    pos
                }
            };
            this.start(buf, move |mut file, buf| {
                (Op::Seek(retry(|| file.seek(pos))), buf)
            });
        }
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("std", &self.std)
            .field("busy", &matches!(self.state, State::Busy(_)))
            .finish()
    }
}

/// Options for opening a file, as [`std::fs::OpenOptions`] (and created
/// from one, for platform-specific options such as a Unix `mode`).
///
/// ```
/// # let path = std::env::temp_dir().join(format!("spooly-io-options-doc-{}", std::process::id()));
/// use spooly_io::fs::OpenOptions;
///
/// spooly::block_on(async {
///     let file = OpenOptions::new().append(true).create(true).open(&path).await?;
///     # drop(file);
///     std::io::Result::Ok(())
/// })?;
/// # std::fs::remove_file(&path)?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct OpenOptions(std::fs::OpenOptions);

impl OpenOptions {
    /// Options with everything off; see [`std::fs::OpenOptions::new`].
    #[allow(clippy::new_without_default)] // As `std`'s.
    pub fn new() -> OpenOptions {
        OpenOptions(std::fs::OpenOptions::new())
    }

    /// Opens for reading.
    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        self.0.read(read);
        self
    }

    /// Opens for writing.
    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        self.0.write(write);
        self
    }

    /// Opens for appending: writes go to the end.
    pub fn append(&mut self, append: bool) -> &mut OpenOptions {
        self.0.append(append);
        self
    }

    /// Truncates the file if it exists.
    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        self.0.truncate(truncate);
        self
    }

    /// Creates the file if it doesn't exist.
    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        self.0.create(create);
        self
    }

    /// Creates the file, failing if it exists.
    pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
        self.0.create_new(create_new);
        self
    }

    /// Opens the file at `path` with these options; as
    /// [`std::fs::OpenOptions::open`].
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let (options, path) = (self.0.clone(), path.as_ref().to_owned());
        let file = asyncify(move || options.open(path)).await?;
        Ok(File::from_std(file))
    }
}

impl From<std::fs::OpenOptions> for OpenOptions {
    fn from(options: std::fs::OpenOptions) -> OpenOptions {
        OpenOptions(options)
    }
}
