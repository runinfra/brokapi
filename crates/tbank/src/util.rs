//! Small shared helpers: subscription ids, call wrappers, limiter access.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use brokapi_core::{Error, SubscriptionId};
use tonic::{Response, Status};

use crate::error::{map_command_status, map_status, transport};

/// Hands out subscription ids, each one larger than the last.
///
/// A connector keeps one generator. The user data connector starts at 1000 and the market data
/// connector at 0, so ids of the two kinds do not collide in a log.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::util::SubIdGen;
///
/// let ids = SubIdGen::new(0);
/// let (a, b) = (ids.next(), ids.next());
/// assert_ne!(a, b);
/// assert_eq!(b.value(), a.value() + 1);
/// ```
pub struct SubIdGen(AtomicU64);

impl SubIdGen {
    /// A generator whose first id is `start + 1`.
    pub fn new(start: u64) -> Self {
        Self(AtomicU64::new(start))
    }

    /// The next id. Safe to call from several tasks at once.
    pub fn next(&self) -> SubscriptionId {
        SubscriptionId::new(self.0.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

/// Read-only unary call: transport trouble and timeouts are retryable errors.
///
/// Waits for `fut` at most `timeout`, then maps a gRPC failure with [`map_status`].
///
/// # Errors
///
/// - [`Error::Transport`] when the deadline passes, or for a transport status such as `Unavailable`.
/// - [`Error::RateLimited`], [`Error::Rejected`] and [`Error::Config`] for the matching statuses,
///   see [`map_status`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_core::Error;
/// use brokapi_tbank::util::query;
/// use tonic::{Response, Status};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let ok = query(Duration::from_secs(1), async { Ok::<_, Status>(Response::new(42)) }).await;
/// assert_eq!(ok.unwrap(), 42);
///
/// let slow = query(Duration::from_millis(10), async {
///     tokio::time::sleep(Duration::from_secs(5)).await;
///     Ok::<_, Status>(Response::new(0))
/// })
/// .await;
/// assert!(matches!(slow, Err(Error::Transport(_))));
/// # }
/// ```
pub async fn query<T>(
    timeout: Duration,
    fut: impl Future<Output = Result<Response<T>, Status>>,
) -> Result<T, Error> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(r)) => Ok(r.into_inner()),
        Ok(Err(status)) => Err(map_status(&status)),
        Err(_) => Err(transport(format!("request timed out after {timeout:?}"))),
    }
}

/// Result of a trading command: a definite answer, or an outcome nobody knows.
///
/// The split exists so that a caller cannot treat a timeout as a refusal. Only `Refused` proves
/// that the broker did not act.
pub enum CommandOutcome<T> {
    /// The broker answered and did the command. Holds the reply.
    Done(T),
    /// The broker refused; `status` is its answer.
    Refused(Status, Error),
    /// Timeout, break, server fault: the command may or may not have been executed.
    Unknown,
}

/// Runs a trading command (place, cancel, replace) under a deadline.
///
/// Waits for `fut` at most `timeout`. Statuses that prove the broker did not act become
/// [`CommandOutcome::Refused`] (see [`map_command_status`]). A timeout, a broken connection or a
/// server fault is [`CommandOutcome::Unknown`]: the broker may have acted. The caller must never
/// repeat the command after `Unknown`, because a repeated place could create a second order.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_tbank::util::{command, CommandOutcome};
/// use tonic::{Response, Status};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let done = command(Duration::from_secs(1), async { Ok::<_, Status>(Response::new(1)) }).await;
/// assert!(matches!(done, CommandOutcome::Done(1)));
///
/// let refused = command(Duration::from_secs(1), async {
///     Err::<Response<()>, _>(Status::invalid_argument("30042"))
/// })
/// .await;
/// assert!(matches!(refused, CommandOutcome::Refused(..)));
///
/// // A fault after sending hides what the broker did.
/// let lost = command(Duration::from_secs(1), async { Err::<Response<()>, _>(Status::unavailable("reset")) }).await;
/// assert!(matches!(lost, CommandOutcome::Unknown));
/// # }
/// ```
pub async fn command<T>(
    timeout: Duration,
    fut: impl Future<Output = Result<Response<T>, Status>>,
) -> CommandOutcome<T> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(r)) => CommandOutcome::Done(r.into_inner()),
        Ok(Err(status)) => match map_command_status(&status) {
            Error::Unknown => CommandOutcome::Unknown,
            err => CommandOutcome::Refused(status, err),
        },
        Err(_) => CommandOutcome::Unknown,
    }
}
