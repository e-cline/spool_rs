//! Address resolution: `lookup_host`, and the address forms the sockets take.

mod common;

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use common::watchdog;
use spooly_io::net::{TcpListener, TcpStream, UdpSocket, lookup_host};

#[test]
fn looks_up_localhost() -> io::Result<()> {
    let _w = watchdog("looks_up_localhost");
    // `lookup_host` needs a runtime (for the pool), not a driver.
    spooly::block_on(async {
        for addrs in [
            lookup_host("localhost:80").await?.collect::<Vec<_>>(),
            lookup_host(("localhost", 80)).await?.collect(),
            lookup_host(String::from("localhost:80")).await?.collect(),
            lookup_host((String::from("localhost"), 80))
                .await?
                .collect(),
        ] {
            assert!(!addrs.is_empty());
            assert!(addrs.iter().all(|a| a.port() == 80 && a.ip().is_loopback()));
        }
        Ok(())
    })
}

#[test]
fn ip_addresses_need_no_lookup_and_no_runtime() -> io::Result<()> {
    // No Spooly runtime at all: an IP address never reaches the pool.
    futures::executor::block_on(async {
        let v4 = SocketAddr::from(([127, 0, 0, 1], 5));
        let v6 = SocketAddr::from((Ipv6Addr::LOCALHOST, 6));
        let one = |addrs: Vec<SocketAddr>| {
            assert_eq!(addrs.len(), 1);
            addrs[0]
        };
        assert_eq!(one(lookup_host("127.0.0.1:5").await?.collect()), v4);
        assert_eq!(one(lookup_host("[::1]:6").await?.collect()), v6);
        assert_eq!(one(lookup_host(("::1", 6)).await?.collect()), v6);
        assert_eq!(one(lookup_host(("127.0.0.1", 5)).await?.collect()), v4);
        assert_eq!(one(lookup_host(v4).await?.collect()), v4);
        assert_eq!(one(lookup_host(&v4).await?.collect()), v4);
        let SocketAddr::V4(v4_only) = v4 else {
            unreachable!()
        };
        assert_eq!(one(lookup_host(v4_only).await?.collect()), v4);
        let v6_only = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6, 0, 0);
        assert_eq!(one(lookup_host(v6_only).await?.collect()), v6);
        assert_eq!(
            one(lookup_host((IpAddr::V4(Ipv4Addr::LOCALHOST), 5))
                .await?
                .collect()),
            v4
        );
        assert_eq!(
            one(lookup_host((Ipv4Addr::LOCALHOST, 5)).await?.collect()),
            v4
        );
        assert_eq!(
            one(lookup_host((Ipv6Addr::LOCALHOST, 6)).await?.collect()),
            v6
        );
        let both: Vec<_> = lookup_host(&[v4, v6][..]).await?.collect();
        assert_eq!(both, [v4, v6]);
        assert_eq!(lookup_host(vec![v6]).await?.collect::<Vec<_>>(), [v6]);
        let _ = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
        Ok(())
    })
}

#[test]
fn bad_addresses_fail() {
    let _w = watchdog("bad_addresses_fail");
    spooly::block_on(async {
        // No addresses at all.
        let err = lookup_host(&[][..]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        // Not an IP address, so it goes to `std`'s resolver, which rejects
        // the port without a lookup.
        assert!(lookup_host("localhost:not-a-port").await.is_err());
        assert!(lookup_host("no port").await.is_err());
    });
}

#[test]
fn sockets_take_host_names() -> io::Result<()> {
    let _w = watchdog("sockets_take_host_names");
    spooly_io::block_on(async {
        // `localhost` may also resolve to `::1`, where nothing listens:
        // `connect` tries each address in turn.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let accept = spooly::spawn(async move {
            for _ in 0..2 {
                listener.accept().await?;
            }
            io::Result::Ok(())
        });
        TcpStream::connect((String::from("localhost"), port)).await?;
        TcpStream::connect(format!("localhost:{port}")).await?;
        accept.await?;

        // `localhost` may resolve to `::1` or `127.0.0.1` first: bind the
        // sender in whichever family the receiver got.
        let a = UdpSocket::bind(("localhost", 0)).await?;
        let a_addr = a.local_addr()?;
        let b = UdpSocket::bind((a_addr.ip(), 0)).await?;
        b.send_to(b"hi", (a_addr.ip().to_string(), a_addr.port()))
            .await?;
        let mut buf = [0; 2];
        let (n, _) = a.recv_from(&mut buf).await?;
        assert_eq!(&buf[..n], b"hi");
        b.connect(&a.local_addr()?).await?;
        b.send(b"yo").await?;
        a.recv(&mut buf).await?;
        assert_eq!(&buf, b"yo");
        Ok(())
    })
}
