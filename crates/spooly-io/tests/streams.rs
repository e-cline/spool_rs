//! The stream traits and helpers shared by TCP and Unix streams: `&T`
//! impls, vectored I/O, flush and close, the halves, and `Debug`.

mod common;

use std::io::{self, IoSlice, IoSliceMut};

use common::{tcp_pair, watchdog};
use futures::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn shared_references_read_and_write() -> io::Result<()> {
    let _w = watchdog("shared_references_read_and_write");
    spooly_io::block_on(async {
        let (client, server) = tcp_pair().await?;
        // `&TcpStream` implements the traits: no `mut` binding needed.
        (&client).write_all(b"by ref").await?;
        (&client).flush().await?;
        let mut buf = [0; 6];
        (&server).read_exact(&mut buf).await?;
        assert_eq!(&buf, b"by ref");
        (&server).close().await?;
        assert_eq!((&client).read(&mut buf).await?, 0);
        Ok(())
    })
}

#[test]
#[cfg_attr(
    target_os = "wasi",
    ignore = "WASI's `writev` sends only the first buffer"
)]
fn vectored_reads_and_writes() -> io::Result<()> {
    let _w = watchdog("vectored_reads_and_writes");
    spooly_io::block_on(async {
        let (mut client, mut server) = tcp_pair().await?;
        // Round 1 through the stream itself, round 2 through `&stream`.
        for round in 0..2 {
            let parts = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];
            let sent = if round == 0 {
                client.write_vectored(&parts).await?
            } else {
                (&client).write_vectored(&parts).await?
            };
            assert_eq!(sent, 4);
            let mut got = Vec::<u8>::new();
            while got.len() < 4 {
                let (mut a, mut b) = ([0; 1], [0; 3]);
                let mut bufs = [IoSliceMut::new(&mut a), IoSliceMut::new(&mut b)];
                let n = if round == 0 {
                    server.read_vectored(&mut bufs).await?
                } else {
                    (&server).read_vectored(&mut bufs).await?
                };
                assert!(n > 0);
                got.extend(a.iter().chain(&b).take(n));
            }
            assert_eq!(got, b"abcd");
        }
        Ok(())
    })
}

#[test]
fn borrowed_halves_close_and_flush() -> io::Result<()> {
    let _w = watchdog("borrowed_halves_close_and_flush");
    spooly_io::block_on(async {
        let (client, mut server) = tcp_pair().await?;
        let (mut r, mut w) = client.halves();
        assert!(format!("{r:?}").contains("ReadHalf"));
        assert!(format!("{w:?}").contains("WriteHalf"));
        assert_eq!(w.write_vectored(&[IoSlice::new(b"half")]).await?, 4);
        w.flush().await?;
        w.close().await?;
        let mut got = Vec::new();
        server.read_to_end(&mut got).await?;
        assert_eq!(got, b"half");
        server.write_all(b"back").await?;
        let mut buf = [0; 4];
        let mut bufs = [IoSliceMut::new(&mut buf)];
        let n = r.read_vectored(&mut bufs).await?;
        assert!(n > 0);
        Ok(())
    })
}

#[test]
fn owned_halves_close_and_flush() -> io::Result<()> {
    let _w = watchdog("owned_halves_close_and_flush");
    spooly_io::block_on(async {
        let (client, mut server) = tcp_pair().await?;
        let (mut r, mut w) = client.into_split();
        assert!(format!("{r:?}").contains("OwnedReadHalf"));
        assert!(format!("{w:?}").contains("OwnedWriteHalf"));
        assert_eq!(w.write_vectored(&[IoSlice::new(b"owned")]).await?, 5);
        w.flush().await?;
        w.close().await?;
        let mut got = Vec::new();
        server.read_to_end(&mut got).await?;
        assert_eq!(got, b"owned");
        server.write_all(b"x").await?;
        let mut buf = [0; 1];
        assert_eq!(r.read_vectored(&mut [IoSliceMut::new(&mut buf)]).await?, 1);
        assert_eq!(&buf, b"x");
        Ok(())
    })
}

#[test]
fn sockets_and_the_driver_have_debug_output() -> io::Result<()> {
    spooly_io::block_on(async {
        let listener = spooly_io::net::TcpListener::bind("127.0.0.1:0").await?;
        assert!(format!("{listener:?}").contains("TcpListener"));
        let (client, _server) = tcp_pair().await?;
        assert!(format!("{client:?}").contains("TcpStream"));
        let udp = spooly_io::net::UdpSocket::bind("127.0.0.1:0").await?;
        assert!(format!("{udp:?}").contains("UdpSocket"));
        client.unref();
        udp.unref();
        Ok(())
    })
}

#[cfg(unix)]
mod unix {
    use super::*;
    use spooly_io::net::{UnixDatagram, UnixListener, UnixStream};
    use std::net::Shutdown;

    #[test]
    fn unix_stream_readiness_and_options() -> io::Result<()> {
        let _w = watchdog("unix_stream_readiness_and_options");
        spooly_io::block_on(async {
            let (a, b) = UnixStream::pair()?;
            assert!(format!("{a:?}").contains("UnixStream"));
            assert!(a.local_addr()?.is_unnamed());
            a.writable().await?;
            assert_eq!(a.try_write(b"z")?, 1);
            b.readable().await?;
            let mut buf = [0; 1];
            assert_eq!(b.try_read(&mut buf)?, 1);
            let (mut r, mut w) = a.halves();
            w.write_all(b"q").await?;
            w.flush().await?;
            (&b).read_exact(&mut buf).await?;
            assert_eq!((&b).write_vectored(&[IoSlice::new(b"v")]).await?, 1);
            r.read_exact(&mut buf).await?;
            assert_eq!(&buf, b"v");
            a.shutdown(Shutdown::Write)?;
            let mut rest = Vec::new();
            (&b).read_to_end(&mut rest).await?;
            assert!(rest.is_empty());
            (&b).flush().await?;
            (&b).close().await?;
            a.unref();
            Ok(())
        })
    }

    #[test]
    fn unix_listener_and_datagram_details() -> io::Result<()> {
        let _w = watchdog("unix_listener_and_datagram_details");
        let path =
            std::env::temp_dir().join(format!("spooly-io-{}-details.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        spooly_io::block_on(async {
            let listener = UnixListener::bind(&path)?;
            assert!(format!("{listener:?}").contains("UnixListener"));
            listener.unref();
            let (x, y) = UnixDatagram::pair()?;
            assert!(format!("{x:?}").contains("UnixDatagram"));
            assert!(x.peer_addr()?.is_unnamed());
            assert!(y.local_addr()?.is_unnamed());
            x.shutdown(Shutdown::Both)?;
            assert!(x.send(b"after shutdown").await.is_err());
            y.unref();
            io::Result::Ok(())
        })?;
        std::fs::remove_file(&path)
    }
}
