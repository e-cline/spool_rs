//! `spooly_io::fs`: the free functions, `read_dir`, and `File`.

mod common;

use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::watchdog;
use futures::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use spooly_io::fs::{self, File, OpenOptions};

/// The temp directory. `std::env::temp_dir` panics on WASI, where the test
/// runner preopens `/tmp` (see `.cargo/config.toml`).
fn temp_base() -> PathBuf {
    if cfg!(target_os = "wasi") {
        PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    }
}

/// Unique to this test process: its id, or on WASI (which has no process
/// ids) the time it started.
fn unique() -> u128 {
    if cfg!(target_os = "wasi") {
        static START: std::sync::OnceLock<u128> = std::sync::OnceLock::new();
        *START.get_or_init(|| {
            std::time::SystemTime::UNIX_EPOCH
                .elapsed()
                .map_or(0, |d| d.as_nanos())
        })
    } else {
        u128::from(std::process::id())
    }
}

/// A fresh, empty directory in the temp directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = temp_base().join(format!("spooly-io-fs-{}-{name}-{n}", unique()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn whole_file_functions() -> io::Result<()> {
    let _w = watchdog("whole_file_functions");
    let dir = TempDir::new("whole");
    // No `Driver` needed: plain `spooly` runs `fs`.
    spooly::block_on(async {
        let a = dir.join("a.txt");
        fs::write(&a, "alpha").await?;
        assert_eq!(fs::read(&a).await?, b"alpha");
        assert_eq!(fs::read_to_string(&a).await?, "alpha");
        assert_eq!(fs::metadata(&a).await?.len(), 5);
        assert!(fs::symlink_metadata(&a).await?.is_file());
        assert!(fs::try_exists(&a).await?);

        let b = dir.join("b.txt");
        assert_eq!(fs::copy(&a, &b).await?, 5);
        let c = dir.join("c.txt");
        fs::rename(&b, &c).await?;
        assert!(!fs::try_exists(&b).await?);
        fs::hard_link(&c, dir.join("d.txt")).await?;
        assert_eq!(fs::read_to_string(dir.join("d.txt")).await?, "alpha");

        let canonical = fs::canonicalize(&c).await?;
        assert!(canonical.is_absolute());
        assert!(canonical.ends_with("c.txt"));

        let mut perm = fs::metadata(&c).await?.permissions();
        perm.set_readonly(true);
        if cfg!(target_os = "wasi") {
            // WASI has no way to set permissions.
            let err = fs::set_permissions(&c, perm).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        } else {
            fs::set_permissions(&c, perm.clone()).await?;
            assert!(fs::metadata(&c).await?.permissions().readonly());
            // Writable again (so the directory can be removed on Windows).
            #[allow(clippy::permissions_set_readonly_false)]
            perm.set_readonly(false);
            fs::set_permissions(&c, perm).await?;
        }

        fs::remove_file(&c).await?;
        let err = fs::read(&c).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        Ok(())
    })
}

#[test]
fn directories() -> io::Result<()> {
    let _w = watchdog("directories");
    let dir = TempDir::new("dirs");
    spooly_io::block_on(async {
        let nested = dir.join("x/y/z");
        fs::create_dir_all(&nested).await?;
        fs::create_dir(dir.join("single")).await?;
        assert_eq!(
            fs::create_dir(dir.join("single")).await.unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        fs::remove_dir(dir.join("single")).await?;
        fs::write(nested.join("f"), "").await?;
        fs::remove_dir_all(dir.join("x")).await?;
        assert!(!fs::try_exists(dir.join("x")).await?);
        Ok(())
    })
}

#[test]
fn read_dir_lists_every_entry_across_batches() -> io::Result<()> {
    let _w = watchdog("read_dir_lists_every_entry_across_batches");
    let dir = TempDir::new("listing");
    // More than one batch of 32.
    let names: Vec<String> = (0..75).map(|i| format!("file-{i:02}")).collect();
    for name in &names {
        std::fs::write(dir.join(name), name)?;
    }
    std::fs::create_dir(dir.join("sub"))?;
    spooly::block_on(async {
        let mut entries = fs::read_dir(&dir.0).await?;
        assert!(format!("{entries:?}").contains("ReadDir"));
        let mut found = Vec::new();
        let mut dirs = 0;
        while let Some(entry) = entries.next_entry().await? {
            assert!(format!("{entry:?}").contains("DirEntry"));
            assert_eq!(entry.path(), dir.0.join(entry.file_name()));
            if entry.file_type().await?.is_dir() {
                dirs += 1;
            } else {
                assert_eq!(entry.metadata().await?.len(), 7);
                found.push(entry.file_name().into_string().unwrap());
            }
        }
        // Past the end, it stays at the end.
        assert!(entries.next_entry().await?.is_none());
        found.sort();
        assert_eq!(found, names);
        assert_eq!(dirs, 1);
        Ok(())
    })
}

#[test]
fn read_dir_of_a_missing_directory_fails() {
    let dir = TempDir::new("missing");
    let err = spooly::block_on(fs::read_dir(dir.join("nope"))).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn file_write_then_read_back() -> io::Result<()> {
    let _w = watchdog("file_write_then_read_back");
    let dir = TempDir::new("file");
    let path = dir.join("f");
    spooly::block_on(async {
        let mut file = File::create(&path).await?;
        assert!(format!("{file:?}").contains("File"));
        for chunk in [&b"one "[..], b"two ", b"three"] {
            file.write_all(chunk).await?;
        }
        file.flush().await?;
        // Flushed: visible to anyone now.
        assert_eq!(std::fs::read(&path)?, b"one two three");
        file.close().await?;

        let mut text = String::new();
        File::open(&path).await?.read_to_string(&mut text).await?;
        assert_eq!(text, "one two three");
        Ok(())
    })
}

#[test]
fn seeks() -> io::Result<()> {
    let _w = watchdog("seeks");
    let dir = TempDir::new("plain-seek");
    let path = dir.join("f");
    std::fs::write(&path, b"0123456789")?;
    spooly::block_on(async {
        let mut file = File::open(&path).await?;
        let mut two = [0; 2];
        assert_eq!(file.seek(SeekFrom::Start(3)).await?, 3);
        file.read_exact(&mut two).await?;
        assert_eq!(&two, b"34");
        assert_eq!(file.seek(SeekFrom::Current(-1)).await?, 4);
        assert_eq!(file.seek(SeekFrom::End(-1)).await?, 9);
        assert_eq!(file.read(&mut two).await?, 1);
        assert_eq!(two[0], b'9');
        Ok(())
    })
}

// `one_thread`, `read_ahead` and the tests that use them need the blocking
// pool: on WASI, `fs` runs inline, so no read is ever left in flight.

/// A runtime whose blocking pool has one thread, for [`read_ahead`].
#[cfg(not(target_family = "wasm"))]
fn one_thread() -> spooly::Runtime {
    spooly::Runtime::builder().max_blocking_threads(1).build()
}

#[cfg(not(target_family = "wasm"))]
/// Makes `file` read `ahead` bytes from the OS but hand out only
/// `take.len()` of them, leaving the rest in its buffer: a read of `ahead`
/// is started and abandoned (it can't finish: the pool's only thread is
/// busy), then a smaller read takes its result.
async fn read_ahead(file: &mut File, ahead: usize, take: &mut [u8]) -> io::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let blocker = spooly::spawn_blocking(move || rx.recv().unwrap());
    let mut big = vec![0; ahead];
    assert!(futures::poll!(file.read(&mut big)).is_pending());
    tx.send(()).unwrap();
    blocker.await;
    assert_eq!(file.read(take).await?, take.len());
    Ok(())
}

#[test]
#[cfg(not(target_family = "wasm"))] // Needs `read_ahead`.
fn seeks_account_for_data_read_ahead() -> io::Result<()> {
    let _w = watchdog("seeks_account_for_data_read_ahead");
    let dir = TempDir::new("seek");
    let path = dir.join("f");
    std::fs::write(&path, b"0123456789")?;
    one_thread().block_on(async {
        let mut file = File::open(&path).await?;
        let mut two = [0; 2];
        read_ahead(&mut file, 8, &mut two).await?;
        assert_eq!(&two, b"01");
        // The OS is at 8; the caller at 2.
        assert_eq!(file.seek(SeekFrom::Current(0)).await?, 2);
        read_ahead(&mut file, 6, &mut two).await?;
        assert_eq!(file.seek(SeekFrom::Current(3)).await?, 7);
        assert_eq!(file.read(&mut two).await?, 2);
        assert_eq!(&two, b"78");
        assert_eq!(file.seek(SeekFrom::End(-1)).await?, 9);
        assert_eq!(file.read(&mut two).await?, 1);
        assert_eq!(file.read(&mut two).await?, 0); // The end.
        assert_eq!(file.seek(SeekFrom::Start(1)).await?, 1);
        assert_eq!(file.read(&mut two).await?, 2);
        assert_eq!(&two, b"12");
        Ok(())
    })
}

#[test]
#[cfg(not(target_family = "wasm"))] // Needs `read_ahead`.
fn leftover_data_is_handed_out_first() -> io::Result<()> {
    let _w = watchdog("leftover_data_is_handed_out_first");
    let dir = TempDir::new("leftover");
    let path = dir.join("f");
    std::fs::write(&path, b"abcdefgh")?;
    one_thread().block_on(async {
        let mut file = File::open(&path).await?;
        let mut two = [0; 2];
        read_ahead(&mut file, 8, &mut two).await?;
        let mut rest = Vec::new();
        file.read_to_end(&mut rest).await?;
        assert_eq!(rest, b"cdefgh");
        Ok(())
    })
}

#[test]
#[cfg(not(target_family = "wasm"))] // Needs `read_ahead`.
fn a_write_after_reading_ahead_goes_where_the_caller_is() -> io::Result<()> {
    let _w = watchdog("a_write_after_reading_ahead_goes_where_the_caller_is");
    let dir = TempDir::new("partial");
    let path = dir.join("f");
    std::fs::write(&path, b"abcdefgh")?;
    one_thread().block_on(async {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        let mut four = [0; 4];
        read_ahead(&mut file, 8, &mut four).await?;
        file.write_all(b"XY").await?;
        file.flush().await
    })?;
    assert_eq!(std::fs::read(&path)?, b"abcdXYgh");
    Ok(())
}

#[test]
fn large_files_round_trip() -> io::Result<()> {
    let _w = watchdog("large_files_round_trip");
    let dir = TempDir::new("large");
    let path = dir.join("big");
    // More than the 2 MiB per-operation buffer.
    let data: Vec<u8> = (0..5 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    spooly::block_on(async {
        let mut file = File::create(&path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        assert_eq!(file.metadata().await?.len(), data.len() as u64);
        drop(file);

        let mut back = Vec::new();
        File::open(&path).await?.read_to_end(&mut back).await?;
        assert!(back == data, "the data differs");
        Ok(())
    })
}

#[test]
#[cfg(not(target_family = "wasm"))] // Needs `read_ahead`.
fn set_len_and_sync_data() -> io::Result<()> {
    let _w = watchdog("set_len_and_sync_data");
    let dir = TempDir::new("set-len");
    let path = dir.join("f");
    spooly::block_on(async {
        let mut file = File::create_new(&path).await?;
        assert_eq!(
            File::create_new(&path).await.unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        file.write_all(b"0123456789").await?;
        file.set_len(4).await?;
        file.sync_data().await?;
        assert_eq!(fs::read(&path).await?, b"0123");
        file.set_len(6).await?;
        assert_eq!(fs::read(&path).await?, b"0123\0\0");
        let perm = file.metadata().await?.permissions();
        file.set_permissions(perm).await?;
        Ok::<_, io::Error>(())
    })?;
    // `set_len` after reading ahead keeps the caller's position.
    one_thread().block_on(async {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        let mut one = [0; 1];
        read_ahead(&mut file, 6, &mut one).await?;
        file.set_len(3).await?;
        assert_eq!(file.seek(SeekFrom::Current(0)).await?, 1);
        let mut rest = Vec::new();
        file.read_to_end(&mut rest).await?;
        assert_eq!(rest, b"12");
        Ok(())
    })
}

#[test]
#[cfg(not(target_family = "wasm"))] // Needs `read_ahead`.
fn into_std_gives_back_the_callers_position() -> io::Result<()> {
    let _w = watchdog("into_std_gives_back_the_callers_position");
    let dir = TempDir::new("into-std");
    let path = dir.join("f");
    std::fs::write(&path, b"0123456789")?;
    let mut std_file = one_thread().block_on(async {
        let mut file = File::from_std(std::fs::File::open(&path)?);
        let mut four = [0; 4];
        read_ahead(&mut file, 10, &mut four).await?;
        io::Result::Ok(file.into_std().await)
    })?;
    let mut rest = String::new();
    io::Read::read_to_string(&mut std_file, &mut rest)?;
    assert_eq!(rest, "456789");
    Ok(())
}

#[test]
fn a_failed_write_is_reported_by_flush() -> io::Result<()> {
    let _w = watchdog("a_failed_write_is_reported_by_flush");
    let dir = TempDir::new("read-only");
    let path = dir.join("f");
    std::fs::write(&path, b"x")?;
    spooly::block_on(async {
        // Open for reading only: the OS refuses the write.
        let mut file = File::open(&path).await?;
        assert_eq!(file.write(b"nope").await?, 4); // Write-behind.
        assert!(file.flush().await.is_err());
        // Reported once.
        file.flush().await?;
        // A later write reports its own failure, through the next one.
        file.write_all(b"again").await?;
        assert!(file.write_all(b"and again").await.is_err());
        Ok(())
    })
}

#[test]
fn run_waits_for_a_dropped_files_write() -> io::Result<()> {
    let _w = watchdog("run_waits_for_a_dropped_files_write");
    let dir = TempDir::new("dropped");
    let path = dir.join("f");
    let p = path.clone();
    spooly::run(async move {
        let mut file = File::create(&p).await.unwrap();
        file.write_all(b"written in the background").await.unwrap();
        // Dropped with the write in flight.
    });
    assert_eq!(std::fs::read(&path)?, b"written in the background");
    Ok(())
}

#[test]
fn open_options_convert_from_std() -> io::Result<()> {
    let dir = TempDir::new("options");
    let path = dir.join("f");
    std::fs::write(&path, b"start")?;
    spooly::block_on(async {
        let mut std_options = std::fs::OpenOptions::new();
        std_options.append(true);
        let options = OpenOptions::from(std_options);
        assert!(format!("{options:?}").contains("OpenOptions"));
        let mut file = options.open(&path).await?;
        file.write_all(b"+end").await?;
        file.close().await?;
        assert_eq!(fs::read(&path).await?, b"start+end");
        let mut truncating = File::options();
        truncating.write(true).truncate(true);
        drop(truncating.open(&path).await?);
        assert!(fs::read(&path).await?.is_empty());
        Ok(())
    })
}

/// Unix: symbolic links.
#[cfg(unix)]
#[test]
fn symlinks() -> io::Result<()> {
    let dir = TempDir::new("links");
    std::fs::write(dir.join("target"), "t")?;
    std::os::unix::fs::symlink(dir.join("target"), dir.join("link"))?;
    spooly::block_on(async {
        assert_eq!(fs::read_link(dir.join("link")).await?, dir.join("target"));
        assert!(fs::symlink_metadata(dir.join("link")).await?.is_symlink());
        assert!(fs::metadata(dir.join("link")).await?.is_file());
        std::fs::remove_file(dir.join("target"))?;
        // A broken link doesn't exist.
        assert!(!fs::try_exists(dir.join("link")).await?);
        Ok(())
    })
}
