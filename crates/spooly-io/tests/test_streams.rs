//! `spooly_io::test`: in-memory `pipe` and `duplex` streams.

use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::time::Duration;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use spooly::{MockClock, Runtime};
use spooly_io::test::{duplex, pipe};

#[test]
fn duplex_carries_both_directions_and_half_closes() -> io::Result<()> {
    // No `Driver`: the streams need only a Spooly runtime.
    spooly::block_on(async {
        let (mut a, mut b) = duplex(16);
        a.write_all(b"to b").await?;
        b.write_all(b"to a").await?;
        let mut buf = [0; 4];
        b.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"to b");
        a.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"to a");

        // Closing `a` ends what `b` reads; `b` can still write back.
        a.close().await?;
        assert_eq!(b.read(&mut buf).await?, 0);
        b.write_all(b"late").await?;
        a.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"late");
        // `a` can't write after closing.
        assert_eq!(
            a.write(b"x").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        a.flush().await?;
        Ok(())
    })
}

#[test]
fn a_full_pipe_holds_the_writer_back() -> io::Result<()> {
    spooly::block_on(async {
        let (mut writer, mut reader) = pipe(4);
        let data: Vec<u8> = (0..100).collect();
        let sent = data.clone();
        let writer_task = spooly::spawn(async move {
            writer.write_all(&sent).await?;
            writer.close().await
        });
        let mut got = Vec::new();
        let mut buf = [0; 3];
        loop {
            // Each read sees at most the 4 bytes the pipe holds.
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            got.extend_from_slice(&buf[..n]);
        }
        writer_task.await?;
        assert_eq!(got, data);
        Ok(())
    })
}

#[test]
fn a_write_to_a_full_pipe_is_pending() {
    spooly::block_on(async {
        let (mut writer, reader) = pipe(2);
        assert_eq!(writer.write(b"abc").await.unwrap(), 2);
        assert!(futures::poll!(writer.write(b"c")).is_pending());
        assert!(format!("{writer:?}").contains("buffered: 2"));
        drop(reader);
        // The pending writer's reader is gone: the write fails.
        assert_eq!(
            writer.write(b"c").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    });
}

#[test]
fn dropping_ends_wake_the_other_side() -> io::Result<()> {
    spooly::block_on(async {
        // A reader waiting on an empty pipe sees the end when the writer
        // is dropped, after what was left.
        let (mut writer, mut reader) = pipe(8);
        writer.write_all(b"left").await?;
        let read = spooly::spawn(async move {
            let mut got = Vec::new();
            reader.read_to_end(&mut got).await?;
            io::Result::Ok(got)
        });
        spooly::yield_now().await;
        drop(writer);
        assert_eq!(read.await?, b"left");

        // A writer waiting on a full pipe fails when the reader is dropped.
        let (mut writer, reader) = pipe(1);
        writer.write_all(b"x").await?;
        let write = spooly::spawn(async move { writer.write_all(b"y").await });
        spooly::yield_now().await;
        drop(reader);
        assert_eq!(write.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        Ok(())
    })
}

#[test]
fn halves_work_in_separate_tasks() -> io::Result<()> {
    spooly::block_on(async {
        let (client, server) = duplex(8);
        // An echo server on the halves of one end.
        let (mut r, mut w) = server.into_split();
        let echo = spooly::spawn(async move { futures::io::copy(&mut r, &mut w).await });
        let (mut cr, mut cw) = client.into_split();
        let message: Vec<u8> = (0..50).collect();
        let m = message.clone();
        let sender = spooly::spawn(async move {
            cw.write_all(&m).await?;
            cw.close().await
        });
        let mut back = Vec::new();
        cr.read_to_end(&mut back).await?;
        sender.await?;
        assert_eq!(echo.await?, 50);
        assert_eq!(back, message);
        Ok(())
    })
}

#[test]
fn empty_reads_and_writes_return_at_once() -> io::Result<()> {
    spooly::block_on(async {
        let (mut a, b) = duplex(1);
        assert_eq!(a.write(b"").await?, 0);
        assert_eq!(a.read(&mut []).await?, 0);
        assert!(format!("{a:?}").contains("DuplexStream"));
        assert!(format!("{b:?}").contains("PipeReader"));
        Ok(())
    })
}

#[test]
fn deterministic_with_a_mock_clock() {
    // A server that answers after a (mock) delay, and a client with a
    // timeout: the order of events is fixed, in no real time.
    let clock = MockClock::new();
    let rt = Runtime::builder().clock(clock.clone()).build();
    let log = Rc::new(RefCell::new(Vec::new()));
    let (l, c) = (log.clone(), clock.clone());
    rt.block_on(async move {
        let (mut client, mut server) = duplex(64);
        let sl = l.clone();
        drop(spooly::spawn(async move {
            let mut req = [0; 3];
            server.read_exact(&mut req).await.unwrap();
            sl.borrow_mut()
                .push(format!("server got {} at {:?}", req.len(), c.elapsed()));
            spooly::sleep(Duration::from_secs(5)).await;
            server.write_all(b"ok").await.unwrap();
        }));
        client.write_all(b"req").await.unwrap();
        let mut reply = [0; 2];
        // Too short a timeout first, then one long enough.
        let early = spooly::timeout(Duration::from_secs(1), client.read_exact(&mut reply)).await;
        l.borrow_mut()
            .push(format!("first wait timed out: {}", early.is_err()));
        spooly::timeout(Duration::from_secs(10), client.read_exact(&mut reply))
            .await
            .unwrap()
            .unwrap();
        l.borrow_mut().push(format!(
            "client got {:?}",
            std::str::from_utf8(&reply).unwrap()
        ));
    });
    assert_eq!(
        *log.borrow(),
        [
            "server got 3 at 0ns",
            "first wait timed out: true",
            "client got \"ok\""
        ]
    );
    assert_eq!(clock.elapsed(), Duration::from_secs(5));
}

#[test]
#[should_panic(expected = "capacity must be at least 1")]
fn a_zero_capacity_is_rejected() {
    let _ = duplex(0);
}
