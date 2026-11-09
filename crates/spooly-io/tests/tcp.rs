//! TCP over loopback.

mod common;

use std::cell::Cell;
use std::io;
use std::net::Shutdown;
use std::rc::Rc;

use common::{tcp_pair, watchdog};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use spooly_io::net::{TcpListener, TcpStream};

#[test]
fn echo_roundtrip() -> io::Result<()> {
    let _w = watchdog("echo_roundtrip");
    spooly_io::block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = spooly::spawn(async move {
            let (stream, peer) = listener.accept().await?;
            assert_eq!(peer, stream.peer_addr()?);
            let (mut r, mut w) = stream.halves();
            futures::io::copy(&mut r, &mut w).await?;
            io::Result::Ok(())
        });
        let mut client = TcpStream::connect(addr).await?;
        assert_eq!(client.peer_addr()?, addr);
        for chunk in [&b"one "[..], b"two ", b"three"] {
            client.write_all(chunk).await?;
        }
        client.close().await?;
        let mut reply = String::new();
        client.read_to_string(&mut reply).await?;
        assert_eq!(reply, "one two three");
        server.await
    })
}

#[test]
fn connect_resolves_a_host_name() -> io::Result<()> {
    let _w = watchdog("connect_resolves_a_host_name");
    spooly_io::block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let accept = spooly::spawn(async move { listener.accept().await.map(|_| ()) });
        TcpStream::connect(("localhost", port)).await?;
        accept.await
    })
}

/// Megabytes in both directions at once, so writes fill the socket buffers
/// and have to wait for the reader (`WouldBlock` on both sides).
#[test]
fn large_transfer_in_both_directions() -> io::Result<()> {
    let _w = watchdog("large_transfer_in_both_directions");
    const LEN: usize = 8 << 20;
    let data: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
    spooly_io::block_on(async {
        let (client, server) = tcp_pair().await?;
        // The server echoes everything back.
        let echo = spooly::spawn(async move {
            let (mut r, mut w) = server.halves();
            futures::io::copy(&mut r, &mut w).await
        });
        let (mut r, mut w) = client.halves();
        let write = async {
            w.write_all(&data).await?;
            w.close().await
        };
        let read = async {
            let mut back = Vec::with_capacity(LEN);
            r.read_to_end(&mut back).await?;
            io::Result::Ok(back)
        };
        let (wrote, back) = futures::join!(write, read);
        wrote?;
        assert_eq!(back?, data);
        assert_eq!(echo.await?, LEN as u64);
        Ok(())
    })
}

#[test]
fn many_concurrent_connections() -> io::Result<()> {
    let _w = watchdog("many_concurrent_connections");
    const N: usize = 64;
    let served = Rc::new(Cell::new(0));
    let s = served.clone();
    spooly_io::run(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(spooly::spawn(async move {
            for _ in 0..N {
                let (mut stream, _) = listener.accept().await.unwrap();
                let s = s.clone();
                drop(spooly::spawn(async move {
                    let mut buf = [0; 8];
                    stream.read_exact(&mut buf).await.unwrap();
                    stream.write_all(&buf).await.unwrap();
                    s.set(s.get() + 1);
                }));
            }
        }));
        let clients: Vec<_> = (0..N)
            .map(|i| {
                spooly::spawn(async move {
                    let mut stream = TcpStream::connect(addr).await?;
                    let msg = (i as u64).to_le_bytes();
                    stream.write_all(&msg).await?;
                    let mut back = [0; 8];
                    stream.read_exact(&mut back).await?;
                    assert_eq!(back, msg);
                    io::Result::Ok(())
                })
            })
            .collect();
        for client in clients {
            client.await?;
        }
        io::Result::Ok(())
    })?;
    assert_eq!(served.get(), N);
    Ok(())
}

#[test]
fn read_returns_zero_once_the_peer_closes() -> io::Result<()> {
    let _w = watchdog("read_returns_zero_once_the_peer_closes");
    spooly_io::block_on(async {
        let (mut client, server) = tcp_pair().await?;
        drop(server);
        let mut buf = [0; 16];
        assert_eq!(client.read(&mut buf).await?, 0);
        Ok(())
    })
}

#[test]
fn shutdown_write_ends_the_peers_stream() -> io::Result<()> {
    let _w = watchdog("shutdown_write_ends_the_peers_stream");
    spooly_io::block_on(async {
        let (mut client, mut server) = tcp_pair().await?;
        client.write_all(b"bye").await?;
        client.shutdown(Shutdown::Write)?;
        let mut got = Vec::new();
        server.read_to_end(&mut got).await?;
        assert_eq!(got, b"bye");
        // The other direction still works.
        server.write_all(b"ok").await?;
        let mut buf = [0; 2];
        client.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"ok");
        Ok(())
    })
}

#[test]
fn connecting_to_a_closed_port_fails() {
    let _w = watchdog("connecting_to_a_closed_port_fails");
    let err = spooly_io::block_on(async {
        let addr = TcpListener::bind("127.0.0.1:0").await?.local_addr()?;
        // The listener is dropped: nothing listens on `addr` any more.
        TcpStream::connect(addr).await
    })
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
}

#[test]
fn readable_then_try_read() -> io::Result<()> {
    let _w = watchdog("readable_then_try_read");
    spooly_io::block_on(async {
        let (client, mut server) = tcp_pair().await?;
        let mut buf = [0; 16];
        // Nothing sent yet.
        let err = client.try_read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        server.write_all(b"data").await?;
        loop {
            client.readable().await?;
            match client.try_read(&mut buf) {
                Ok(n) => {
                    assert_eq!(&buf[..n], b"data");
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e),
            }
        }
        client.writable().await?;
        assert_eq!(client.try_write(b"x")?, 1);
        Ok(())
    })
}

#[test]
fn peek_does_not_consume() -> io::Result<()> {
    let _w = watchdog("peek_does_not_consume");
    spooly_io::block_on(async {
        let (mut client, mut server) = tcp_pair().await?;
        server.write_all(b"abc").await?;
        let mut buf = [0; 3];
        let n = client.peek(&mut buf).await?;
        assert!(n > 0 && buf[..n] == b"abc"[..n]);
        client.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"abc");
        Ok(())
    })
}

#[test]
fn owned_halves_in_separate_tasks() -> io::Result<()> {
    let _w = watchdog("owned_halves_in_separate_tasks");
    spooly_io::block_on(async {
        let (client, mut server) = tcp_pair().await?;
        let (mut r, mut w) = client.into_split();
        let writer = spooly::spawn(async move {
            w.write_all(b"from the writer").await?;
            io::Result::Ok(w)
        });
        let reader = spooly::spawn(async move {
            let mut buf = [0; 5];
            r.read_exact(&mut buf).await?;
            io::Result::Ok((r, buf))
        });
        let mut got = [0; 15];
        server.read_exact(&mut got).await?;
        assert_eq!(&got, b"from the writer");
        server.write_all(b"hello").await?;
        let (r, buf) = reader.await?;
        assert_eq!(&buf, b"hello");
        let w = writer.await?;
        let stream = r.reunite(w).expect("halves of the same stream");
        assert!(stream.peer_addr().is_ok());
        Ok(())
    })
}

#[test]
fn reunite_rejects_halves_of_different_streams() -> io::Result<()> {
    spooly_io::block_on(async {
        let (a, b) = tcp_pair().await?;
        let (ra, _wa) = a.into_split();
        let (_rb, wb) = b.into_split();
        assert!(ra.reunite(wb).is_err());
        Ok(())
    })
}

#[test]
fn std_conversions_and_options() -> io::Result<()> {
    let _w = watchdog("std_conversions_and_options");
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = std_listener.local_addr()?;
    spooly_io::block_on(async move {
        let listener = TcpListener::from_std(std_listener)?;
        assert_eq!(listener.local_addr()?, addr);
        listener.set_ttl(42)?;
        assert_eq!(listener.ttl()?, 42);

        let client = TcpStream::from_std(std::net::TcpStream::connect(addr)?)?;
        let (server, _) = listener.accept().await?;
        client.set_nodelay(true)?;
        assert!(client.nodelay()?);
        client.set_ttl(17)?;
        assert_eq!(client.ttl()?, 17);
        assert_eq!(client.local_addr()?, server.peer_addr()?);

        // Back to std, and back again: the connection survives.
        let std_client = client.into_std()?;
        let mut client = TcpStream::from_std(std_client)?;
        let mut server = server;
        client.write_all(b"still here").await?;
        let mut buf = [0; 10];
        server.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"still here");
        assert_eq!(listener.into_std()?.local_addr()?, addr);
        Ok(())
    })
}

#[test]
#[should_panic(expected = "no I/O driver")]
fn using_a_socket_without_a_driver_panics() {
    let _ = spooly::block_on(TcpListener::bind("127.0.0.1:0"));
}
