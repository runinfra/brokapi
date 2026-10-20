//! The clock the connectors read.
//!
//! The sans-io cores take the current time as an argument (`std::time::Instant`). The connectors read
//! it at the edge, where the network is. They read it here, not from `Instant::now()`, so that
//! a test can freeze and move the time with `tokio::time::pause` and `tokio::time::advance`, and
//! the rate limiter, the command timeouts and the waits of the connector all follow it.
//!
//! Outside a paused test the value is the same as `std::time::Instant::now()`.

use std::time::Instant;

/// The current instant of the runtime clock.
///
/// Equal to `std::time::Instant::now()` in a program. In a test with a paused tokio clock it is the
/// virtual instant, so a rate limit that refills after one second refills when the test calls
/// `tokio::time::advance(Duration::from_secs(1))`, without a real pause.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// let before = brokapi_runtime::now();
/// tokio::time::advance(Duration::from_secs(30)).await;
/// assert!(brokapi_runtime::now() - before >= Duration::from_secs(30));
/// # }
/// ```
#[must_use]
pub fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}
