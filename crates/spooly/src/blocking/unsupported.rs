//! The blocking pool's stand-in on targets without threads (WASM): there
//! is no `spawn_blocking`, so there are never jobs. It has the same
//! internal names as `blocking.rs`, so the runtime needs no `cfg`s for it.

use std::fmt;

/// No settings: there's no pool to configure.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Config;

impl Config {
    pub(crate) fn debug_fields(&self, _f: &mut fmt::DebugStruct<'_, '_>) {}
}

/// A pool that never has jobs.
pub(crate) struct LazyPool;

impl LazyPool {
    pub(crate) fn new(_config: Config) -> Self {
        LazyPool
    }

    pub(crate) fn has_in_flight(&self) -> bool {
        false
    }

    pub(crate) fn shut_down(&self) {}
}
