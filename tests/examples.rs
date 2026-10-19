//! Runs the programs of `examples/` against the in-process broker imitations.
//!
//! The book (docs/src) includes code from these programs, so this is the test of the code the book
//! shows. Each program has `pub async fn run(...)`, and its `main` does the same for a person at a keyboard.
//! They never touch the network: `BROKAPI_LIVE` is not read here.

#[path = "../examples/quickstart_bcs.rs"]
mod quickstart_bcs;

#[tokio::test(flavor = "multi_thread")]
async fn quickstart_bcs_on_the_mock() {
    quickstart_bcs::run(false)
        .await
        .expect("the quick start runs on the BCS mock");
}

#[path = "../examples/lifecycle.rs"]
mod lifecycle;
#[path = "../examples/recovery.rs"]
mod recovery;

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_on_the_mock() {
    lifecycle::run()
        .await
        .expect("a restart finds the order of the first run");
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_on_the_mock() {
    recovery::run()
        .await
        .expect("an Unknown is resolved with order_state");
}
