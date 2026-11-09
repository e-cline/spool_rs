//! Blocking work (`fs` and host-name lookups), without `cfg`s at the call
//! sites.
//!
//! Where there are threads, a [`Job`] is a [`spooly::spawn_blocking`] job:
//! it runs on the runtime's blocking pool while the loop keeps running
//! tasks. WASI has no threads and no pool, so there the closure runs
//! inline, on the runtime's thread, and the job is a future that is ready
//! at once: the loop waits while it runs (ASYNC_IO_PLAN.md §8.2). Host file
//! operations are short, and a lookup blocks only on the host's resolver.
//!
//! The bounds are the same on every target, so code that builds on one
//! builds on the others.

cfg_select! {
    target_family = "wasm" => {
        /// Work that has already run: ready at once.
        pub(crate) type Job<T> = std::future::Ready<T>;

        /// Runs `f` now, on this thread.
        pub(crate) fn spawn<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Job<T> {
            std::future::ready(f())
        }
    }
    _ => {
        /// Work on the runtime's blocking pool.
        pub(crate) type Job<T> = spooly::BlockingHandle<T>;

        /// Runs `f` on the runtime's blocking pool.
        pub(crate) fn spawn<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Job<T> {
            spooly::spawn_blocking(f)
        }
    }
}
