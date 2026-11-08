// A JoinHandle can't be sent to another thread.
fn main() {
    spooly::block_on(async {
        let handle = spooly::spawn(async { 1 });
        std::thread::spawn(move || drop(handle));
    });
}
