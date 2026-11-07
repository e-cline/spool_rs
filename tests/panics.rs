//! Panic behavior in M1: every panic unwinds out of `block_on`.
//! (The JS-exception model from §6 of the plan lands in M4.)

mod common;

use common::expect_panic;

#[test]
fn panic_in_main_unwinds_out_of_block_on() {
    let msg = expect_panic(|| spool::block_on(async { panic!("main boom") }));
    assert_eq!(msg, "main boom");
}

#[test]
fn panic_in_task_unwinds_out_of_block_on() {
    let msg = expect_panic(|| {
        spool::block_on(async {
            drop(spool::spawn(async { panic!("task boom") }));
            spool::yield_now().await;
            spool::yield_now().await;
        })
    });
    assert_eq!(msg, "task boom");
}

#[test]
fn panic_in_task_after_await_unwinds() {
    let msg = expect_panic(|| {
        spool::block_on(async {
            spool::spawn(async {
                spool::yield_now().await;
                panic!("later boom");
            })
            .await
        })
    });
    assert_eq!(msg, "later boom");
}

#[test]
fn fresh_runtime_works_after_a_panic() {
    let _ = expect_panic(|| {
        spool::block_on(async {
            drop(spool::spawn(async { panic!("x") }));
            spool::yield_now().await;
        })
    });
    assert_eq!(
        spool::block_on(async { spool::spawn(async { 1 }).await }),
        1
    );
}
