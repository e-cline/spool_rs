//! Type-level guarantees: `!Send` futures are accepted, and runtime handles
//! stay on their thread.

use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_not_impl_any!(spool::Runtime: Send, Sync);
assert_not_impl_any!(spool::JoinHandle<i32>: Send, Sync);
assert_impl_all!(spool::JoinHandle<i32>: std::future::Future, std::fmt::Debug, Unpin);
assert_impl_all!(spool::YieldNow: std::future::Future, Send, Sync, Unpin);
assert_impl_all!(spool::Runtime: Default, std::fmt::Debug);

#[test]
#[cfg_attr(miri, ignore = "trybuild runs rustc, which Miri can't do")]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_*.rs");
    t.compile_fail("tests/ui/fail_*.rs");
}
