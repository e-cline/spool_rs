//! UDP over loopback.

mod common;

use std::io;

use common::watchdog;
use spooly_io::net::UdpSocket;

#[test]
fn send_to_and_recv_from() -> io::Result<()> {
    let _w = watchdog("send_to_and_recv_from");
    spooly_io::block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0").await?;
        let b = UdpSocket::bind("127.0.0.1:0").await?;
        // `b` waits first, so the datagram arrives while it is pending.
        let b_addr = b.local_addr()?;
        let receiver = spooly::spawn(async move {
            let mut buf = [0; 32];
            let (n, from) = b.recv_from(&mut buf).await?;
            io::Result::Ok((buf[..n].to_vec(), from))
        });
        spooly::yield_now().await;
        a.send_to(b"datagram", b_addr).await?;
        let (got, from) = receiver.await?;
        assert_eq!((got.as_slice(), from), (&b"datagram"[..], a.local_addr()?));
        Ok(())
    })
}

#[test]
#[cfg_attr(target_os = "wasi", ignore = "WASI has no `MSG_PEEK`")]
fn connected_send_and_recv() -> io::Result<()> {
    let _w = watchdog("connected_send_and_recv");
    spooly_io::block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0").await?;
        let b = UdpSocket::bind("127.0.0.1:0").await?;
        a.connect(b.local_addr()?).await?;
        b.connect(a.local_addr()?).await?;
        assert_eq!(a.peer_addr()?, b.local_addr()?);
        a.send(b"hi").await?;
        let mut buf = [0; 8];
        let (n, _) = b.peek_from(&mut buf).await?;
        assert_eq!(&buf[..n], b"hi");
        let n = b.recv(&mut buf).await?;
        assert_eq!(&buf[..n], b"hi");
        Ok(())
    })
}

/// More datagrams than one read takes, so the receive side goes from ready
/// to `WouldBlock` and back.
#[test]
fn many_datagrams_in_order() -> io::Result<()> {
    let _w = watchdog("many_datagrams_in_order");
    const N: u32 = 200;
    spooly_io::block_on(async {
        let tx = UdpSocket::bind("127.0.0.1:0").await?;
        let rx = UdpSocket::bind("127.0.0.1:0").await?;
        let to = rx.local_addr()?;
        let receiver = spooly::spawn(async move {
            let mut buf = [0; 4];
            for i in 0..N {
                let (n, _) = rx.recv_from(&mut buf).await?;
                assert_eq!(n, 4);
                assert_eq!(u32::from_le_bytes(buf), i);
            }
            io::Result::Ok(())
        });
        for i in 0..N {
            tx.send_to(&i.to_le_bytes(), to).await?;
            if i % 16 == 0 {
                // Let the receiver drain, so the socket buffer can't fill
                // (loopback UDP drops datagrams when it does).
                spooly::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
        receiver.await
    })
}

#[test]
#[cfg_attr(target_os = "wasi", ignore = "WASI has no `SO_BROADCAST`")]
fn std_conversions_and_options() -> io::Result<()> {
    let _w = watchdog("udp std_conversions_and_options");
    let std_socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let addr = std_socket.local_addr()?;
    spooly_io::block_on(async move {
        let socket = UdpSocket::from_std(std_socket)?;
        assert_eq!(socket.local_addr()?, addr);
        socket.set_broadcast(true)?;
        assert!(socket.broadcast()?);
        socket.set_ttl(9)?;
        assert_eq!(socket.ttl()?, 9);
        assert_eq!(socket.into_std()?.local_addr()?, addr);
        Ok(())
    })
}
