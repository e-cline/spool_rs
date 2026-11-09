// A Runtime can't be sent to another thread.
fn main() {
    let rt = spooly::Runtime::new();
    std::thread::spawn(move || rt.block_on(async {}));
}
