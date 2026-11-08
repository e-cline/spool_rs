//! Type-level guarantees: `!Send` futures are accepted, and runtime handles
//! stay on their thread.

use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_not_impl_any!(spooly::Runtime: Send, Sync);
assert_not_impl_any!(spooly::JoinHandle<i32>: Send, Sync);
assert_impl_all!(spooly::JoinHandle<i32>: std::future::Future, std::fmt::Debug, Unpin);
assert_not_impl_any!(spooly::AbortHandle: Send, Sync);
assert_impl_all!(spooly::AbortHandle: Clone, std::fmt::Debug);
assert_not_impl_any!(spooly::AbortOnDrop<i32>: Send, Sync);
assert_impl_all!(spooly::AbortOnDrop<i32>: std::future::Future, std::fmt::Debug, Unpin);
assert_not_impl_any!(spooly::TryJoin<i32>: Send, Sync);
assert_impl_all!(spooly::TryJoin<i32>: std::future::Future, std::fmt::Debug, Unpin);
assert_impl_all!(spooly::JoinError: std::error::Error, std::fmt::Display, Send);
assert_impl_all!(spooly::KeepAlive: Send, Sync, Clone, std::fmt::Debug);
assert_impl_all!(spooly::YieldNow: std::future::Future, Send, Sync, Unpin);
assert_impl_all!(spooly::Runtime: Default, std::fmt::Debug);

#[test]
#[cfg_attr(miri, ignore = "trybuild runs rustc, which Miri can't do")]
fn ui() {
    // The .stderr files pin the stable compiler's wording, which beta and
    // nightly often change. CI's beta jobs set this to skip the comparison.
    if std::env::var_os("SPOOLY_SKIP_UI").is_some_and(|v| v == "1") {
        eprintln!("skipping trybuild UI tests: SPOOLY_SKIP_UI=1");
        return;
    }
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_*.rs");
    t.compile_fail("tests/ui/fail_*.rs");
}
