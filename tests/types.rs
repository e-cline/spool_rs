//! Type-level guarantees: `!Send` futures are accepted, and runtime handles
//! stay on their thread.

use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_not_impl_any!(spool::Runtime: Send, Sync);
assert_not_impl_any!(spool::JoinHandle<i32>: Send, Sync);
assert_impl_all!(spool::JoinHandle<i32>: std::future::Future, std::fmt::Debug, Unpin);
assert_not_impl_any!(spool::AbortHandle: Send, Sync);
assert_impl_all!(spool::AbortHandle: Clone, std::fmt::Debug);
assert_not_impl_any!(spool::AbortOnDrop<i32>: Send, Sync);
assert_impl_all!(spool::AbortOnDrop<i32>: std::future::Future, std::fmt::Debug, Unpin);
assert_not_impl_any!(spool::TryJoin<i32>: Send, Sync);
assert_impl_all!(spool::TryJoin<i32>: std::future::Future, std::fmt::Debug, Unpin);
assert_impl_all!(spool::JoinError: std::error::Error, std::fmt::Display, Send);
assert_impl_all!(spool::KeepAlive: Send, Sync, Clone, std::fmt::Debug);
assert_impl_all!(spool::YieldNow: std::future::Future, Send, Sync, Unpin);
assert_impl_all!(spool::Runtime: Default, std::fmt::Debug);

#[test]
#[cfg_attr(miri, ignore = "trybuild runs rustc, which Miri can't do")]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_*.rs");
    t.compile_fail("tests/ui/fail_*.rs");
}
