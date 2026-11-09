//! A TCP port scanner: tries a range of ports concurrently, each with a
//! timeout, on one thread.
//!
//! With no arguments it opens a few listeners on localhost and scans the
//! ports around them, so it always finds something. Scan only hosts you
//! are allowed to.
//!
//! ```text
//! cargo run -p spooly-io --example scan
//! cargo run -p spooly-io --example scan -- <host> <first-port> <last-port>
//! ```

use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::time::Duration;

use spooly_io::net::{TcpListener, TcpStream};

/// How many connection attempts run at once.
const CONCURRENCY: usize = 64;
const TIMEOUT: Duration = Duration::from_millis(500);

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    spooly_io::run(async move {
        // Keep the demo listeners open for the whole scan.
        let mut listeners = Vec::new();
        let (host, first, last) = match args.as_slice() {
            [host, first, last] => (host.clone(), parse(first)?, parse(last)?),
            _ => {
                for _ in 0..3 {
                    let listener = TcpListener::bind("127.0.0.1:0").await?;
                    println!("listening on {}", listener.local_addr()?);
                    listeners.push(listener);
                }
                let ports: Vec<u16> = listeners
                    .iter()
                    .map(|l| l.local_addr().map(|a| a.port()))
                    .collect::<io::Result<_>>()?;
                let (lo, hi) = (*ports.iter().min().unwrap(), *ports.iter().max().unwrap());
                (
                    "127.0.0.1".to_owned(),
                    lo.saturating_sub(5),
                    hi.saturating_add(5),
                )
            }
        };
        println!("scanning {host} ports {first}..={last}");

        // A shared queue of ports, drained by `CONCURRENCY` tasks. Plain
        // `Rc<RefCell<…>>`: every task runs on this thread.
        let ports = Rc::new(RefCell::new(first..=last));
        let open = Rc::new(RefCell::new(Vec::new()));
        let workers: Vec<_> = (0..CONCURRENCY)
            .map(|_| {
                let (ports, open, host) = (ports.clone(), open.clone(), host.clone());
                spooly::spawn(async move {
                    loop {
                        let Some(port) = ports.borrow_mut().next() else {
                            break;
                        };
                        let attempt = TcpStream::connect((host.as_str(), port));
                        if let Ok(Ok(_)) = spooly::timeout(TIMEOUT, attempt).await {
                            open.borrow_mut().push(port);
                        }
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.await;
        }

        let mut open = open.take();
        open.sort_unstable();
        println!("open: {open:?}");
        drop(listeners);
        io::Result::Ok(())
    })
}

fn parse(port: &str) -> io::Result<u16> {
    port.parse().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("bad port {port:?}: {e}"),
        )
    })
}
