//! Unix-domain sockets.
#![cfg(unix)]

mod common;

use std::io;
use std::path::PathBuf;

use common::watchdog;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use spooly_io::net::{UnixDatagram, UnixListener, UnixStream};

/// A fresh socket path in the temp directory, removed on drop.
struct TempPath(PathBuf);

impl TempPath {
    fn new(name: &str) -> TempPath {
        let path =
            std::env::temp_dir().join(format!("spooly-io-{}-{name}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        TempPath(path)
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn listener_and_stream_echo() -> io::Result<()> {
    let _w = watchdog("listener_and_stream_echo");
    let path = TempPath::new("echo");
    spooly_io::block_on(async {
        let listener = UnixListener::bind(&path.0)?;
        assert_eq!(listener.local_addr()?.as_pathname(), Some(path.0.as_path()));
        let server = spooly::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (mut r, mut w) = stream.halves();
            futures::io::copy(&mut r, &mut w).await
        });
        let mut client = UnixStream::connect(&path.0).await?;
        client.write_all(b"over a unix socket").await?;
        client.close().await?;
        let mut reply = String::new();
        client.read_to_string(&mut reply).await?;
        assert_eq!(reply, "over a unix socket");
        assert_eq!(server.await?, 18);
        Ok(())
    })
}

#[test]
fn connecting_to_a_missing_path_fails() {
    let path = TempPath::new("missing");
    let err = spooly_io::block_on(UnixStream::connect(path.0.clone())).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn stream_pair_and_owned_halves() -> io::Result<()> {
    let _w = watchdog("stream_pair_and_owned_halves");
    spooly_io::block_on(async {
        let (a, mut b) = UnixStream::pair()?;
        let (mut r, mut w) = a.into_split();
        w.write_all(b"ping").await?;
        let mut buf = [0; 4];
        b.read_exact(&mut buf).await?;
        b.write_all(&buf).await?;
        r.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"ping");
        let a = r.reunite(w).expect("same stream");
        assert!(a.peer_addr().is_ok());
        let err = a.try_read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        Ok(())
    })
}

#[test]
fn datagrams_between_paths_and_pairs() -> io::Result<()> {
    let _w = watchdog("datagrams_between_paths_and_pairs");
    let (pa, pb) = (TempPath::new("dgram-a"), TempPath::new("dgram-b"));
    spooly_io::block_on(async {
        let a = UnixDatagram::bind(&pa.0)?;
        let b = UnixDatagram::bind(&pb.0)?;
        a.send_to(b"to b", &pb.0).await?;
        let mut buf = [0; 8];
        let (n, from) = b.recv_from(&mut buf).await?;
        assert_eq!(&buf[..n], b"to b");
        assert_eq!(from.as_pathname(), Some(pa.0.as_path()));

        let (x, y) = UnixDatagram::pair()?;
        x.send(b"paired").await?;
        let n = y.recv(&mut buf).await?;
        assert_eq!(&buf[..n], b"paired");

        let unbound = UnixDatagram::unbound()?;
        unbound.connect(&pb.0)?;
        unbound.send(b"hey").await?;
        let n = b.recv(&mut buf).await?;
        assert_eq!(&buf[..n], b"hey");
        Ok(())
    })
}

#[test]
fn std_conversions() -> io::Result<()> {
    let path = TempPath::new("std");
    spooly_io::block_on(async {
        let listener = UnixListener::from_std(std::os::unix::net::UnixListener::bind(&path.0)?)?;
        let client = UnixStream::from_std(std::os::unix::net::UnixStream::connect(&path.0)?)?;
        let (server, _) = listener.accept().await?;
        let mut server = UnixStream::from_std(server.into_std()?)?;
        let mut client = client;
        client.write_all(b"ok").await?;
        let mut buf = [0; 2];
        server.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"ok");
        listener.into_std()?;
        let (d, _) = std::os::unix::net::UnixDatagram::pair()?;
        UnixDatagram::from_std(d)?.into_std()?;
        Ok(())
    })
}
