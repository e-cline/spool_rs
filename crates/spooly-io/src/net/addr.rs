//! Address resolution: `ToSocketAddrs` and `lookup_host`.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// Types that resolve to socket addresses, for the functions in
/// [`net`](super) that take one: IP socket addresses, `(ip, port)` pairs,
/// slices of addresses, and `"host:port"` strings or `(host, port)` pairs.
///
/// Like [`std::net::ToSocketAddrs`], but a host name is resolved on the
/// runtime's blocking pool ([`spooly::spawn_blocking`]), not on the
/// runtime's thread. An IP address (`"127.0.0.1:80"`, `("::1", 80)`) needs
/// no lookup.
///
/// This trait is sealed: it can't be implemented outside this crate.
pub trait ToSocketAddrs: sealed::Sealed {}

#[allow(unreachable_pub)] // Public in a private module: the sealed pattern.
mod sealed {
    use std::net::SocketAddr;

    /// What an address resolves to without a lookup, or the name to look
    /// up.
    pub enum Addrs {
        Ready(Vec<SocketAddr>),
        /// A `"host:port"` string for `std`'s resolver.
        Lookup(String),
    }

    pub trait Sealed {
        fn to_addrs(self) -> Addrs;
    }
}

use sealed::{Addrs, Sealed};

macro_rules! ready {
    ($($ty:ty => |$v:ident| $addrs:expr),* $(,)?) => {
        $(
            impl ToSocketAddrs for $ty {}
            impl Sealed for $ty {
                fn to_addrs(self) -> Addrs {
                    let $v = self;
                    Addrs::Ready($addrs)
                }
            }
        )*
    };
}

ready! {
    SocketAddr => |a| vec![a],
    SocketAddrV4 => |a| vec![a.into()],
    SocketAddrV6 => |a| vec![a.into()],
    (IpAddr, u16) => |a| vec![a.into()],
    (Ipv4Addr, u16) => |a| vec![a.into()],
    (Ipv6Addr, u16) => |a| vec![a.into()],
    &[SocketAddr] => |a| a.to_vec(),
    Vec<SocketAddr> => |a| a,
}

impl ToSocketAddrs for &str {}
impl Sealed for &str {
    fn to_addrs(self) -> Addrs {
        match self.parse::<SocketAddr>() {
            Ok(addr) => Addrs::Ready(vec![addr]),
            Err(_) => Addrs::Lookup(self.to_owned()),
        }
    }
}

impl ToSocketAddrs for String {}
impl Sealed for String {
    fn to_addrs(self) -> Addrs {
        match self.parse::<SocketAddr>() {
            Ok(addr) => Addrs::Ready(vec![addr]),
            Err(_) => Addrs::Lookup(self),
        }
    }
}

impl ToSocketAddrs for (&str, u16) {}
impl Sealed for (&str, u16) {
    fn to_addrs(self) -> Addrs {
        let (host, port) = self;
        match host.parse::<IpAddr>() {
            Ok(ip) => Addrs::Ready(vec![SocketAddr::new(ip, port)]),
            // `std` takes the pair; as a string, an IPv6-looking name would
            // need brackets, but a name that parses as no IP has no colons
            // `std` would misread.
            Err(_) => Addrs::Lookup(format!("{host}:{port}")),
        }
    }
}

impl ToSocketAddrs for (String, u16) {}
impl Sealed for (String, u16) {
    fn to_addrs(self) -> Addrs {
        (self.0.as_str(), self.1).to_addrs()
    }
}

impl<T: ToSocketAddrs + Copy> ToSocketAddrs for &T {}
impl<T: ToSocketAddrs + Copy> Sealed for &T {
    fn to_addrs(self) -> Addrs {
        (*self).to_addrs()
    }
}

/// Resolves `addr` to one or more addresses: at once for IP addresses, and
/// on the blocking pool for a host name. An empty result is an error.
pub(super) async fn resolve(addr: impl ToSocketAddrs) -> io::Result<Vec<SocketAddr>> {
    let addrs = match addr.to_addrs() {
        Addrs::Ready(addrs) => addrs,
        Addrs::Lookup(name) => {
            spooly::spawn_blocking(move || {
                std::net::ToSocketAddrs::to_socket_addrs(&name).map(Iterator::collect)
            })
            .await?
        }
    };
    if addrs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve to any address",
        ));
    }
    Ok(addrs)
}

/// Looks up the addresses of `host`: a `"host:port"` string, a
/// `(host, port)` pair, or anything else that implements
/// [`ToSocketAddrs`].
///
/// A host name is resolved by the OS (as `std` does), on the runtime's
/// blocking pool, so the runtime's thread keeps running tasks meanwhile.
/// This needs a Spooly runtime, but not a [`Driver`](crate::Driver).
///
/// ```
/// spooly::block_on(async {
///     let addrs: Vec<_> = spooly_io::net::lookup_host(("localhost", 8080)).await?.collect();
///     assert!(addrs.iter().all(|a| a.port() == 8080 && a.ip().is_loopback()));
///     std::io::Result::Ok(())
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
///
/// # Errors
///
/// If the name can't be resolved, or resolves to no address.
pub async fn lookup_host(
    host: impl ToSocketAddrs,
) -> io::Result<impl Iterator<Item = SocketAddr> + std::fmt::Debug> {
    Ok(resolve(host).await?.into_iter())
}
