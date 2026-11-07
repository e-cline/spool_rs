// A JoinHandle can't be sent to another thread.
fn main() {
    spool::block_on(async {
        let handle = spool::spawn(async { 1 });
        std::thread::spawn(move || drop(handle));
    });
}
