//! Time in tests without a real pause.
//!
//! The mock servers listen on real sockets. A test that freezes the whole tokio clock for its whole
//! length (`start_paused`) does not work with them: while a request is on its way through the socket
//! the runtime has nothing to run, so the paused clock jumps to the next timer, and a request timeout
//! fires before the answer is read. So the clock of such a test stays real, and these two helpers
//! replace the pauses that would otherwise be `sleep` calls:
//!
//! * [`jump`] moves the clock forward at once. Rate limits refill, timers that were waiting fire.
//! * [`wait_until`] waits for a condition (a counter of the mock, an event) and fails after 10 seconds. It
//!   does not sleep: it hands the thread to the other tasks until the condition holds.
//!
//! Neither helper is for a test that already runs on a paused clock. There the plain
//! `tokio::time::advance` is the tool.

use std::time::Duration;

/// How long [`wait_until`] waits before it fails the test.
pub const WAIT_LIMIT: Duration = Duration::from_secs(10);

/// Moves the tokio clock forward by `by` at once, then lets the clock run in real time again.
///
/// The connectors read the time from the same clock (`brokapi_runtime::now`), so a rate limiter that
/// refills after a second is full again right after `jump(Duration::from_secs(1))`. The wait of the
/// test is zero. Timers that were due in that span fire, so call it when no request is in flight.
///
/// # Panics
///
/// When the clock is already paused (the test runs with `start_paused`), or the runtime is not
/// `current_thread`, which is the default of `#[tokio::test]`.
///
/// # Examples
///
/// ```
/// use std::time::{Duration, Instant};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let before = brokapi_runtime::now();
/// let started = Instant::now();
/// brokapi_mocks::time::jump(Duration::from_secs(3600)).await;
/// assert!(brokapi_runtime::now() - before >= Duration::from_secs(3600));
/// assert!(started.elapsed() < Duration::from_secs(5), "the hour did not pass for real");
/// # }
/// ```
pub async fn jump(by: Duration) {
    tokio::time::pause();
    tokio::time::advance(by).await;
    tokio::time::resume();
}

/// Waits until `condition` is true. Fails the test after [`WAIT_LIMIT`] with `what` in the message.
///
/// Use it instead of `sleep` to wait for something to happen: the counter of a mock to reach a value,
/// a flag to be set. The condition is checked again and again between runs of the other tasks, with
/// no pause in between.
///
/// # Panics
///
/// When the condition does not hold within [`WAIT_LIMIT`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicU32, Ordering};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let count = Arc::new(AtomicU32::new(0));
/// let worker = Arc::clone(&count);
/// tokio::spawn(async move {
///     worker.store(3, Ordering::SeqCst);
/// });
/// brokapi_mocks::time::wait_until("the worker counted", || count.load(Ordering::SeqCst) == 3).await;
/// # }
/// ```
pub async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let waiting = async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    };
    if tokio::time::timeout(WAIT_LIMIT, waiting).await.is_err() {
        panic!("gave up after {WAIT_LIMIT:?}, still waiting for: {what}");
    }
}

/// Like [`wait_until`], for a condition that has to be asked with an `await`: a call to the connector
/// whose answer changes once the background work is done.
///
/// Each probe is one call, so the call itself paces the loop. Fails the test after [`WAIT_LIMIT`].
///
/// # Panics
///
/// When the probe does not return `true` within [`WAIT_LIMIT`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicU32, Ordering};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let count = Arc::new(AtomicU32::new(0));
/// let worker = Arc::clone(&count);
/// tokio::spawn(async move {
///     worker.store(2, Ordering::SeqCst);
/// });
/// brokapi_mocks::time::wait_until_async("the worker counted", || async {
///     count.load(Ordering::SeqCst) == 2
/// })
/// .await;
/// # }
/// ```
pub async fn wait_until_async<F, Fut>(what: &str, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let waiting = async {
        while !probe().await {
            tokio::task::yield_now().await;
        }
    };
    if tokio::time::timeout(WAIT_LIMIT, waiting).await.is_err() {
        panic!("gave up after {WAIT_LIMIT:?}, still waiting for: {what}");
    }
}

/// Runs `call`, and once `arrived` says the request has reached the mock, moves the clock forward by `by`.
///
/// This is how a test makes a request time out without waiting for the timeout: the timer of the
/// connector (`request_timeout`) is due `by` later, the delay the mock adds to its answer
/// (`post_delay_ms`) is longer than `by`, so the call ends with the timeout while the mock still holds
/// the request. A second `jump` then lets the mock finish it.
///
/// # Panics
///
/// When `arrived` does not become true within [`WAIT_LIMIT`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::time::{Duration, Instant};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let arrived = Arc::new(AtomicBool::new(false));
/// let flag = Arc::clone(&arrived);
/// // A call that waits one minute for an answer which does not come.
/// let call = async move {
///     flag.store(true, Ordering::SeqCst);
///     tokio::time::timeout(Duration::from_secs(60), std::future::pending::<()>()).await
/// };
/// let started = Instant::now();
/// let outcome = brokapi_mocks::time::elapse_when(
///     "the call started",
///     || arrived.load(Ordering::SeqCst),
///     Duration::from_secs(61),
///     call,
/// )
/// .await;
/// assert!(outcome.is_err(), "the minute passed");
/// assert!(started.elapsed() < Duration::from_secs(5), "and no real time with it");
/// # }
/// ```
pub async fn elapse_when<T>(
    what: &str,
    arrived: impl FnMut() -> bool,
    by: Duration,
    call: impl std::future::Future<Output = T>,
) -> T {
    let driver = async {
        wait_until(what, arrived).await;
        jump(by).await;
    };
    let (out, ()) = tokio::join!(call, driver);
    out
}
