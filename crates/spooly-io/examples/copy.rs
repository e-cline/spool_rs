//! Copies a file through the async `fs` API, with a progress ticker
//! running on the same thread meanwhile.
//!
//! Each read and write runs on Spooly's blocking pool, so the runtime's
//! thread stays free: the ticker keeps firing while the copy is under way.
//! It needs no I/O driver, so this runs on plain `spooly::run`.
//!
//! ```text
//! cargo run -p spooly-io --example copy -- <from> <to>
//! cargo run -p spooly-io --example copy    # copies a generated 64 MiB file
//! ```

use std::cell::Cell;
use std::io;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use spooly_io::fs::{self, File};

fn main() -> io::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let (from, to, generated) = match (args.next(), args.next()) {
        (Some(from), Some(to)) => (PathBuf::from(from), PathBuf::from(to), false),
        _ => {
            let dir = std::env::temp_dir();
            let pid = std::process::id();
            (
                dir.join(format!("spooly-copy-{pid}.src")),
                dir.join(format!("spooly-copy-{pid}.dst")),
                true,
            )
        }
    };

    spooly::run(async move {
        if generated {
            let data: Vec<u8> = (0..64 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
            fs::write(&from, data).await?;
        }

        let copied = Rc::new(Cell::new(0u64));
        let ticker = {
            let copied = copied.clone();
            spooly::set_interval(Duration::from_millis(50), move |_| {
                println!("  ... {} MiB", copied.get() / (1024 * 1024));
            })
        };

        let mut src = File::open(&from).await?;
        let mut dst = File::create(&to).await?;
        let mut buf = vec![0; 1024 * 1024];
        loop {
            let n = src.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            dst.write_all(&buf[..n]).await?;
            copied.set(copied.get() + n as u64);
        }
        dst.sync_all().await?;
        ticker.cancel();

        let same = fs::metadata(&from).await?.len() == fs::metadata(&to).await?.len();
        println!(
            "copied {} bytes from {} to {} ({})",
            copied.get(),
            from.display(),
            to.display(),
            if same { "sizes match" } else { "SIZES DIFFER" }
        );
        if generated {
            fs::remove_file(&from).await?;
            fs::remove_file(&to).await?;
        }
        io::Result::Ok(())
    })
}
