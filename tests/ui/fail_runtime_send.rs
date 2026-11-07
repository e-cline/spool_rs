// A Runtime can't be sent to another thread.
fn main() {
    let rt = spool::Runtime::new();
    std::thread::spawn(move || rt.block_on(async {}));
}
