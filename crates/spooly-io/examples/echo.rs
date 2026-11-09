//! An echo server and a few clients, on one thread.
//!
//! The server accepts connections in a detached task and echoes each one in
//! a task of its own. Its listener is `unref`'d, so it doesn't keep `run`
//! alive by itself: the program ends once the clients are done.
//!
//! ```text
//! cargo run -p spooly-io --example echo
//! ```

use std::cell::Cell;
use std::io;
use std::rc::Rc;

use futures::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use spooly_io::net::{TcpListener, TcpStream};

fn main() -> io::Result<()> {
    spooly_io::run(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        listener.unref();
        println!("echo server on {addr}");

        // `Rc` and `Cell` in tasks: nothing here needs to be `Send`.
        let connections = Rc::new(Cell::new(0));
        drop(spooly::spawn(server(listener, connections.clone())));

        let clients: Vec<_> = ["alpha", "beta", "gamma"]
            .into_iter()
            .map(|name| spooly::spawn(client(addr, name)))
            .collect();
        for client in clients {
            client.await?;
        }
        println!("served {} connections", connections.get());
        Ok(())
    })
}

async fn server(listener: TcpListener, connections: Rc<Cell<u32>>) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => return eprintln!("accept failed: {e}"),
        };
        connections.set(connections.get() + 1);
        drop(spooly::spawn(async move {
            // Echo line by line, until the client closes its side.
            let (reader, mut writer) = stream.halves();
            let mut lines = BufReader::new(reader).lines();
            while let Some(Ok(line)) = futures::StreamExt::next(&mut lines).await {
                if writer
                    .write_all(format!("{line}\n").as_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
            }
            println!("  server: {peer} disconnected");
        }));
    }
}

async fn client(addr: std::net::SocketAddr, name: &str) -> io::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    for i in 1..=2 {
        stream
            .write_all(format!("{name} #{i}\n").as_bytes())
            .await?;
        // Wait a little between lines: other clients interleave.
        spooly::sleep(std::time::Duration::from_millis(10)).await;
    }
    stream.close().await?;
    let mut echoed = String::new();
    stream.read_to_string(&mut echoed).await?;
    for line in echoed.lines() {
        println!("{name} got back: {line}");
    }
    Ok(())
}
