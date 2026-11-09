//! How I/O interacts with the event loop: liveness, stalls, cancellation,
//! remote wakes, and timers.

mod common;

use std::cell::Cell;
use std::io::{self, Write};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use common::{oneshot, tcp_pair, watchdog};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use spooly::MockClock;
use spooly_io::Driver;
use spooly_io::net::{TcpListener, TcpStream};

/// A detached server task waiting on `accept` keeps `run` alive until a
/// client (on another thread) connects.
#[test]
fn a_detached_server_keeps_run_alive() -> io::Result<()> {
    let _w = watchdog("a_detached_server_keeps_run_alive");
    let got = Rc::new(Cell::new(0u8));
    let g = got.clone();
    let client = spooly_io::run(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(spooly::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0; 1];
            stream.read_exact(&mut buf).await.unwrap();
            g.set(buf[0]);
        }));
        // Main returns at once; the client connects later, from a thread.
        io::Result::Ok(thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            std::net::TcpStream::connect(addr)?.write_all(&[42])
        }))
    })?;
    client.join().unwrap()?;
    assert_eq!(got.get(), 42);
    Ok(())
}

#[test]
fn an_unrefd_listener_does_not_keep_run_alive() -> io::Result<()> {
    let _w = watchdog("an_unrefd_listener_does_not_keep_run_alive");
    let start = Instant::now();
    spooly_io::run(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        listener.unref();
        drop(spooly::spawn(async move {
            let _ = listener.accept().await;
        }));
        io::Result::Ok(())
    })?;
    assert!(start.elapsed() < Duration::from_secs(5));
    Ok(())
}

#[test]
fn a_socket_nobody_waits_on_does_not_keep_run_alive() -> io::Result<()> {
    let _w = watchdog("a_socket_nobody_waits_on_does_not_keep_run_alive");
    spooly_io::run(async {
        let (client, server) = tcp_pair().await?;
        // Held by a task that waits on something else, which can't wake.
        drop(spooly::spawn(async move {
            let _keep = (client, server);
            std::future::pending::<()>().await;
        }));
        io::Result::Ok(())
    })
}

#[test]
#[should_panic(expected = "main future stalled")]
fn a_stalled_main_still_panics() {
    spooly_io::block_on(std::future::pending::<()>());
}

#[test]
fn a_cancelled_read_loses_no_data() -> io::Result<()> {
    let _w = watchdog("a_cancelled_read_loses_no_data");
    spooly_io::block_on(async {
        let (mut client, mut server) = tcp_pair().await?;
        let mut buf = [0; 4];
        let timed_out = spooly::timeout(Duration::from_millis(20), client.read(&mut buf)).await;
        assert!(timed_out.is_err(), "nothing was sent yet");
        server.write_all(b"late").await?;
        client.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"late");
        Ok(())
    })
}

/// A cancelled wait leaves its waker behind, but dropping the socket
/// deregisters it, so `run` still exits.
#[test]
fn dropping_a_socket_with_a_cancelled_wait_lets_run_exit() -> io::Result<()> {
    let _w = watchdog("dropping_a_socket_with_a_cancelled_wait_lets_run_exit");
    spooly_io::run(async {
        let (mut client, _server) = tcp_pair().await?;
        let mut buf = [0; 4];
        let _ = spooly::timeout(Duration::from_millis(10), client.read(&mut buf)).await;
        io::Result::Ok(())
    })?;
    Ok(())
}

#[test]
fn a_wake_from_another_thread_interrupts_the_wait() -> io::Result<()> {
    let _w = watchdog("a_wake_from_another_thread_interrupts_the_wait");
    spooly_io::block_on(async {
        // A socket, so the driver has a registration but nothing ready.
        let (_client, _server) = tcp_pair().await?;
        let (tx, rx) = oneshot();
        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            tx.send(7);
        });
        assert_eq!(rx.await, 7);
        worker.join().unwrap();
        Ok(())
    })
}

#[test]
fn timers_and_io_together() -> io::Result<()> {
    let _w = watchdog("timers_and_io_together");
    spooly_io::block_on(async {
        let (mut client, mut server) = tcp_pair().await?;
        let writer = spooly::spawn(async move {
            spooly::sleep(Duration::from_millis(30)).await;
            server.write_all(b"after a sleep").await?;
            io::Result::Ok(server)
        });
        let start = Instant::now();
        let mut buf = [0; 13];
        client.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"after a sleep");
        assert!(start.elapsed() >= Duration::from_millis(30));
        writer.await?;
        Ok(())
    })
}

/// With a `MockClock` the clock jumps to the next timer when no I/O is
/// ready, so a timeout around real I/O fires without real waiting.
#[test]
fn a_mock_clock_jumps_past_pending_io() -> io::Result<()> {
    let _w = watchdog("a_mock_clock_jumps_past_pending_io");
    let clock = MockClock::new();
    let rt = spooly::Runtime::builder()
        .clock(clock.clone())
        .io_driver(Driver::new()?)
        .build();
    let start = Instant::now();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let accepted = spooly::timeout(Duration::from_secs(3600), listener.accept()).await;
        assert!(accepted.is_err());
        io::Result::Ok(())
    })?;
    assert_eq!(clock.elapsed(), Duration::from_secs(3600));
    assert!(start.elapsed() < Duration::from_secs(5));
    Ok(())
}

#[test]
fn closed_sockets_are_deregistered() -> io::Result<()> {
    let _w = watchdog("closed_sockets_are_deregistered");
    spooly_io::block_on(async {
        let driver = spooly::driver::current_io_driver::<Driver>().expect("a driver");
        {
            let (_a, _b) = tcp_pair().await?;
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            drop(TcpStream::connect(listener.local_addr()?).await?);
            assert!(format!("{driver:?}").contains("sources: 3"), "{driver:?}");
        }
        assert!(format!("{driver:?}").contains("sources: 0"), "{driver:?}");
        Ok(())
    })
}
