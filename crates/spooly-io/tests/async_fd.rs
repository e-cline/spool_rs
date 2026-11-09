//! `Async<T>`: arbitrary non-blocking file descriptors (Unix).
#![cfg(unix)]

mod common;

use std::cell::Cell;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::net::{UnixDatagram, UnixStream};
use std::rc::Rc;
use std::time::Duration;

use common::watchdog;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use spooly_io::Async;

fn stream_pair() -> io::Result<(Async<UnixStream>, Async<UnixStream>)> {
    let (a, b) = UnixStream::pair()?;
    a.set_nonblocking(true)?;
    b.set_nonblocking(true)?;
    Ok((Async::new(a)?, Async::new(b)?))
}

#[test]
fn streams_through_the_traits() -> io::Result<()> {
    let _w = watchdog("streams_through_the_traits");
    spooly_io::block_on(async {
        let (mut a, mut b) = stream_pair()?;
        // Owned: `Async<T>` for `T: Read + Write`.
        a.write_all(b"owned").await?;
        a.flush().await?;
        let mut buf = [0; 5];
        b.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"owned");
        // Shared: `&Async<T>` for `&T: Read + Write`.
        (&b).write_all(b"by ref").await?;
        (&b).flush().await?;
        let mut buf = [0; 6];
        (&a).read_exact(&mut buf).await?;
        assert_eq!(&buf, b"by ref");
        // Vectored, and close (a flush).
        let n = a
            .write_vectored(&[io::IoSlice::new(b"v"), io::IoSlice::new(b"w")])
            .await?;
        assert_eq!(n, 2);
        let mut x = [0; 1];
        let mut y = [0; 1];
        let mut got = 0;
        while got < 2 {
            got += b
                .read_vectored(&mut [io::IoSliceMut::new(&mut x), io::IoSliceMut::new(&mut y)][..])
                .await?;
        }
        (&a).close().await?;
        a.close().await?;
        Ok(())
    })
}

#[test]
fn a_read_waits_for_data_and_retries_would_block() -> io::Result<()> {
    let _w = watchdog("a_read_waits_for_data_and_retries_would_block");
    spooly_io::block_on(async {
        let (a, b) = stream_pair()?;
        let calls = Rc::new(Cell::new(0));
        let c = calls.clone();
        let reader = spooly::spawn(async move {
            let mut buf = [0; 4];
            let n = a
                .read_with(|mut s| {
                    c.set(c.get() + 1);
                    s.read(&mut buf)
                })
                .await?;
            io::Result::Ok(buf[..n].to_vec())
        });
        spooly::sleep(Duration::from_millis(10)).await;
        b.write_with(|mut s| s.write(b"late")).await?;
        assert_eq!(reader.await?, b"late");
        // Once before the data (`WouldBlock`), once after.
        assert!(calls.get() >= 2, "called {} times", calls.get());
        Ok(())
    })
}

#[test]
fn datagrams_with_read_with_and_write_with() -> io::Result<()> {
    let _w = watchdog("datagrams_with_read_with_and_write_with");
    spooly_io::block_on(async {
        let (a, b) = UnixDatagram::pair()?;
        a.set_nonblocking(true)?;
        b.set_nonblocking(true)?;
        let (mut a, mut b) = (Async::new(a)?, Async::new(b)?);
        a.writable().await?;
        a.write_with_mut(|s| s.send(b"one")).await?;
        b.readable().await?;
        let mut buf = [0; 8];
        let n = b.read_with_mut(|s| s.recv(&mut buf)).await?;
        assert_eq!(&buf[..n], b"one");
        // Straight through `get_ref` / `get_mut`, without waiting.
        a.get_mut().send(b"two")?;
        b.readable().await?;
        let n = b.get_ref().recv(&mut buf)?;
        assert_eq!(&buf[..n], b"two");
        Ok(())
    })
}

#[test]
fn into_inner_handles_and_debug() -> io::Result<()> {
    spooly_io::block_on(async {
        let (a, b) = stream_pair()?;
        assert_eq!(a.as_fd().as_raw_fd(), a.get_ref().as_raw_fd());
        assert!(format!("{a:?}").starts_with("Async("));
        let mut std_a = a.into_inner();
        // Still non-blocking, and no longer registered.
        let mut buf = [0; 1];
        assert_eq!(
            std_a.read(&mut buf).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(b);
        assert_eq!(std_a.read(&mut buf)?, 0);
        Ok(())
    })
}

#[test]
fn a_task_waiting_on_it_keeps_run_alive_unless_unrefed() -> io::Result<()> {
    let _w = watchdog("a_task_waiting_on_it_keeps_run_alive_unless_unrefed");
    // Waiting keeps `run` alive: the read completes once the peer writes,
    // on another thread.
    let (a, b) = UnixStream::pair()?;
    let got = Rc::new(Cell::new(false));
    let g = got.clone();
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(30));
        (&b).write_all(b"x").unwrap();
        b
    });
    spooly_io::run(async move {
        a.set_nonblocking(true).unwrap();
        let mut a = Async::new(a).unwrap();
        drop(spooly::spawn(async move {
            let mut buf = [0; 1];
            a.read_exact(&mut buf).await.unwrap();
            g.set(true);
        }));
    });
    assert!(got.get());
    let _b = writer.join().unwrap();

    // Unref'd: nothing will ever write, and `run` returns anyway.
    let (a, _b) = UnixStream::pair()?;
    a.set_nonblocking(true)?;
    spooly_io::run(async move {
        let a = Async::new(a).unwrap();
        a.unref();
        drop(spooly::spawn(async move {
            let mut buf = [0; 1];
            let _ = (&a).read(&mut buf).await;
            unreachable!("nothing writes");
        }));
    });
    Ok(())
}

#[test]
#[should_panic(expected = "no I/O driver")]
fn wrapping_outside_a_driver_panics() {
    let (a, _b) = UnixStream::pair().unwrap();
    spooly::block_on(async move {
        let _ = Async::new(a);
    });
}
