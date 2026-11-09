//! Files and directories, mirroring [`std::fs`].
//!
//! The OS has no readiness API for regular files, so each operation runs
//! `std::fs` on the runtime's blocking thread pool
//! ([`spooly::spawn_blocking`]), and the runtime's thread keeps running
//! tasks meanwhile. That needs a Spooly runtime, but not a
//! [`Driver`](crate::Driver): `spooly::run` works too.
//!
//! ```
//! # let dir = std::env::temp_dir().join(format!("spooly-io-fs-doc-{}", std::process::id()));
//! # let _ = std::fs::remove_dir_all(&dir);
//! use spooly_io::fs;
//!
//! spooly::block_on(async {
//!     fs::create_dir_all(&dir).await?;
//!     let path = dir.join("hello.txt");
//!     fs::write(&path, "hello").await?;
//!     assert_eq!(fs::read_to_string(&path).await?, "hello");
//!     fs::remove_dir_all(&dir).await
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Each function takes its arguments by value or copies them (a path into
//! a `PathBuf`, say), since the pool thread can't borrow from the task.
//!
//! Like any [`spawn_blocking`](spooly::spawn_blocking) job, an operation
//! whose future is dropped still runs to completion, and
//! [`run`](crate::run) waits for it.

mod file;

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use spooly::BlockingHandle;

pub use file::{File, OpenOptions};
pub use std::fs::{FileType, Metadata, Permissions};

/// Runs `f` on the blocking pool.
async fn asyncify<T: Send + 'static>(
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    spooly::spawn_blocking(f).await
}

/// Reads the whole file at `path`; as [`std::fs::read`].
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::read(path)).await
}

/// Reads the whole file at `path` as UTF-8; as [`std::fs::read_to_string`].
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::read_to_string(path)).await
}

/// Writes `contents` as the whole file at `path`, creating or truncating
/// it; as [`std::fs::write`].
pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let (path, contents) = (path.as_ref().to_owned(), contents.as_ref().to_owned());
    asyncify(move || std::fs::write(path, contents)).await
}

/// Copies the contents and permissions of `from` to `to`, returning the
/// number of bytes copied; as [`std::fs::copy`].
pub async fn copy(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<u64> {
    let (from, to) = (from.as_ref().to_owned(), to.as_ref().to_owned());
    asyncify(move || std::fs::copy(from, to)).await
}

/// Renames `from` to `to`, replacing `to` if it exists; as
/// [`std::fs::rename`].
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let (from, to) = (from.as_ref().to_owned(), to.as_ref().to_owned());
    asyncify(move || std::fs::rename(from, to)).await
}

/// Creates a hard link `link` to `original`; as [`std::fs::hard_link`].
pub async fn hard_link(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    let (original, link) = (original.as_ref().to_owned(), link.as_ref().to_owned());
    asyncify(move || std::fs::hard_link(original, link)).await
}

/// Removes the file at `path`; as [`std::fs::remove_file`].
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::remove_file(path)).await
}

/// Creates the directory `path`; as [`std::fs::create_dir`].
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::create_dir(path)).await
}

/// Creates the directory `path` and any missing parents; as
/// [`std::fs::create_dir_all`].
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::create_dir_all(path)).await
}

/// Removes the empty directory `path`; as [`std::fs::remove_dir`].
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::remove_dir(path)).await
}

/// Removes the directory `path` and everything in it; as
/// [`std::fs::remove_dir_all`].
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::remove_dir_all(path)).await
}

/// The metadata of `path`, following symbolic links; as
/// [`std::fs::metadata`].
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::metadata(path)).await
}

/// The metadata of `path` itself, not following a symbolic link; as
/// [`std::fs::symlink_metadata`].
pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::symlink_metadata(path)).await
}

/// Sets the permissions of `path`; as [`std::fs::set_permissions`].
pub async fn set_permissions(path: impl AsRef<Path>, perm: Permissions) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::set_permissions(path, perm)).await
}

/// The canonical, absolute form of `path`; as [`std::fs::canonicalize`].
pub async fn canonicalize(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::canonicalize(path)).await
}

/// The target of the symbolic link `path`; as [`std::fs::read_link`].
pub async fn read_link(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::read_link(path)).await
}

/// Whether `path` exists (following symbolic links): `Ok(false)` for a
/// missing file or a broken link, an error if that can't be told; as
/// [`std::fs::exists`].
pub async fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    let path = path.as_ref().to_owned();
    asyncify(move || std::fs::exists(path)).await
}

/// Entries fetched per trip to the pool.
const READ_DIR_BATCH: usize = 32;

/// Starts reading the directory `path`; as [`std::fs::read_dir`]. Then
/// call [`ReadDir::next_entry`] for each entry.
///
/// ```
/// # let dir = std::env::temp_dir().join(format!("spooly-io-read-dir-doc-{}", std::process::id()));
/// # let _ = std::fs::remove_dir_all(&dir);
/// # std::fs::create_dir_all(&dir)?;
/// # std::fs::write(dir.join("a"), "")?;
/// spooly::block_on(async {
///     let mut entries = spooly_io::fs::read_dir(&dir).await?;
///     while let Some(entry) = entries.next_entry().await? {
///         println!("{}", entry.path().display());
///     }
///     std::io::Result::Ok(())
/// })?;
/// # std::fs::remove_dir_all(&dir)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    let path = path.as_ref().to_owned();
    let inner = asyncify(move || std::fs::read_dir(path)).await?;
    Ok(ReadDir {
        buf: VecDeque::new(),
        state: ReadDirState::Idle(Some(inner)),
    })
}

/// A batch of entries, and the iterator if it has more.
type Batch = (
    VecDeque<io::Result<std::fs::DirEntry>>,
    Option<std::fs::ReadDir>,
);

/// The entries of a directory, from [`read_dir`]. Entries are fetched from
/// the OS in batches, on the blocking pool.
pub struct ReadDir {
    buf: VecDeque<io::Result<std::fs::DirEntry>>,
    state: ReadDirState,
}

enum ReadDirState {
    /// `None` once the OS has no more entries.
    Idle(Option<std::fs::ReadDir>),
    Busy(BlockingHandle<Batch>),
}

impl ReadDir {
    /// The next entry, or `None` after the last one. Cancelling (dropping)
    /// the future loses no entry.
    pub async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        poll_fn(|cx| self.poll_next_entry(cx)).await
    }

    /// Polls for the next entry: [`next_entry`](Self::next_entry) as a poll
    /// function.
    pub fn poll_next_entry(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<DirEntry>>> {
        loop {
            if let Some(entry) = self.buf.pop_front() {
                return Poll::Ready(entry.map(|e| Some(DirEntry(Arc::new(e)))));
            }
            match &mut self.state {
                ReadDirState::Idle(None) => return Poll::Ready(Ok(None)),
                ReadDirState::Idle(inner) => {
                    let mut inner = inner.take().expect("checked above");
                    self.state = ReadDirState::Busy(spooly::spawn_blocking(move || {
                        let mut batch = VecDeque::with_capacity(READ_DIR_BATCH);
                        for _ in 0..READ_DIR_BATCH {
                            match inner.next() {
                                Some(entry) => batch.push_back(entry),
                                None => return (batch, None),
                            }
                        }
                        (batch, Some(inner))
                    }));
                }
                ReadDirState::Busy(handle) => {
                    let Poll::Ready((batch, inner)) = Pin::new(handle).poll(cx) else {
                        return Poll::Pending;
                    };
                    self.buf = batch;
                    self.state = ReadDirState::Idle(inner);
                }
            }
        }
    }
}

impl fmt::Debug for ReadDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadDir")
            .field("buffered", &self.buf.len())
            .finish_non_exhaustive()
    }
}

/// An entry of a directory, from [`ReadDir::next_entry`].
pub struct DirEntry(Arc<std::fs::DirEntry>);

impl DirEntry {
    /// The entry's full path: the directory's path joined with
    /// [`file_name`](Self::file_name).
    pub fn path(&self) -> PathBuf {
        self.0.path()
    }

    /// The entry's name in its directory.
    pub fn file_name(&self) -> OsString {
        self.0.file_name()
    }

    /// The entry's metadata, not following a symbolic link (on most
    /// platforms); as [`std::fs::DirEntry::metadata`].
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let entry = Arc::clone(&self.0);
        asyncify(move || entry.metadata()).await
    }

    /// The entry's file type; as [`std::fs::DirEntry::file_type`].
    pub async fn file_type(&self) -> io::Result<FileType> {
        let entry = Arc::clone(&self.0);
        asyncify(move || entry.file_type()).await
    }
}

impl fmt::Debug for DirEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DirEntry").field(&self.0.path()).finish()
    }
}
