//! The OS-handle traits on the socket types, through `socket2`: options
//! that have no method of their own, and sockets configured before use.

mod common;

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use common::{tcp_pair, watchdog};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use spooly_io::net::{TcpListener, TcpStream, UdpSocket};

#[test]
fn options_through_a_sock_ref() -> io::Result<()> {
    let _w = watchdog("options_through_a_sock_ref");
    spooly_io::block_on(async {
        let (client, _server) = tcp_pair().await?;
        let sock = SockRef::from(&client);
        sock.set_keepalive(true)?;
        assert!(sock.keepalive()?);
        sock.set_tcp_nodelay(true)?;
        // The same socket: our own accessor sees it.
        assert!(client.nodelay()?);
        assert_eq!(sock.peer_addr()?.as_socket(), Some(client.peer_addr()?));

        let udp = UdpSocket::bind("127.0.0.1:0").await?;
        SockRef::from(&udp).set_broadcast(true)?;
        assert!(udp.broadcast()?);
        Ok(())
    })
}

#[test]
fn a_listener_configured_with_socket2() -> io::Result<()> {
    let _w = watchdog("a_listener_configured_with_socket2");
    // Set options that must come before `bind`, then hand the socket over.
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&SocketAddr::from(([127, 0, 0, 1], 0)).into())?;
    socket.listen(16)?;
    socket.set_nonblocking(true)?;
    spooly_io::block_on(async move {
        let listener = TcpListener::from_std(socket.into())?;
        assert!(SockRef::from(&listener).reuse_address()?);
        let addr = listener.local_addr()?;
        let accept = spooly::spawn(async move { listener.accept().await });
        let client = TcpStream::connect(addr).await?;
        let (server, _) = accept.await?;
        assert_eq!(server.peer_addr()?, client.local_addr()?);
        SockRef::from(&server).set_linger(Some(Duration::ZERO))?;
        Ok(())
    })
}

#[cfg(unix)]
#[test]
fn raw_fds_match_the_std_socket() -> io::Result<()> {
    use std::os::fd::{AsFd, AsRawFd};
    spooly_io::block_on(async {
        let (client, _server) = tcp_pair().await?;
        let fd = client.as_raw_fd();
        assert_eq!(client.as_fd().as_raw_fd(), fd);
        assert_eq!(client.into_std()?.as_raw_fd(), fd);
        Ok(())
    })
}

#[cfg(windows)]
#[test]
fn raw_sockets_match_the_std_socket() -> io::Result<()> {
    use std::os::windows::io::{AsRawSocket, AsSocket};
    spooly_io::block_on(async {
        let (client, _server) = tcp_pair().await?;
        let raw = client.as_raw_socket();
        assert_eq!(client.as_socket().as_raw_socket(), raw);
        assert_eq!(client.into_std()?.as_raw_socket(), raw);
        Ok(())
    })
}
