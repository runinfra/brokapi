//! Stream supervision: open, read, reconnect with backoff. No protocol logic here.
//!
//! A server-side stream can end at any time: the server closes it, the network breaks, or it goes
//! silent. [`supervise`] reopens it with a growing delay ([`backoff`]) and tells the consumer on every
//! successful open, so the consumer can resend its state. It keeps trying until it is cancelled (or the
//! receiving end of the channel is dropped), except when the broker refuses the stream in a way a repeat
//! cannot cure: a rejected token, no permission, the limit of open streams (80001, see [`fatal_open`]).
//! Then it reports the reason once and stops.
//!
//! # Examples
//!
//! ```
//! use std::time::Duration;
//! use brokapi_tbank::stream::backoff;
//!
//! // 500 ms doubling up to 30 s.
//! assert_eq!(backoff(0), Duration::from_millis(500));
//! assert_eq!(backoff(2), Duration::from_secs(2));
//! assert_eq!(backoff(40), Duration::from_secs(30));
//! ```

use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{Status, Streaming};
use tracing::{debug, warn};

use crate::error::map_status;

/// Delay before reconnect attempt `n` (0-based): 500 ms doubling to 30 s. The same range at BCS and
/// Finam: none of the three brokers documents a reason for a different one.
pub fn backoff(attempt: u32) -> Duration {
    let ms = 500u64.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(ms.min(30_000))
}

/// Whether the refusal to open a stream is final: a repeat will not help, so the supervisor stops and
/// reports it. `Some(reason)` for a rejected token (`Unauthenticated`), no permission
/// (`PermissionDenied`) and the limit of open streams (80001, `streams_limit`). Everything else (the
/// network, `Unavailable`, a rate limit, the lifetime limit 80005) is worth another try.
///
/// The status is read with [`map_status`], as every other answer of the broker.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::stream::fatal_open;
/// use tonic::Status;
///
/// assert!(fatal_open(&Status::unauthenticated("40003")).is_some());
/// assert!(fatal_open(&Status::resource_exhausted("80001")).is_some());
/// assert!(fatal_open(&Status::unavailable("down")).is_none());
/// assert!(fatal_open(&Status::resource_exhausted("80002")).is_none());
/// ```
pub fn fatal_open(status: &Status) -> Option<String> {
    use brokapi_core::Error;
    match map_status(status) {
        Error::Config(e) => Some(format!("unauthenticated: {e}")),
        Error::Rejected { code, message }
            if code == "streams_limit" || code == "PermissionDenied" =>
        {
            Some(format!("{code}: {message}"))
        }
        _ => None,
    }
}

/// The last reason a stream closed, for error messages of calls that wait on a stream.
#[derive(Default)]
pub struct LastError(Mutex<Option<String>>);

impl LastError {
    /// Remembers the reason the stream closed, replacing an earlier one.
    pub fn set(&self, msg: impl Into<String>) {
        *self.0.lock().expect("poisoned") = Some(msg.into());
    }

    /// Forgets the reason, after the stream reopened.
    pub fn clear(&self) {
        *self.0.lock().expect("poisoned") = None;
    }

    /// The last reason, if the stream is down now.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_tbank::stream::LastError;
    ///
    /// let last = LastError::default();
    /// assert_eq!(last.get(), None);
    /// last.set("order_state: closed by the server");
    /// assert_eq!(last.get().as_deref(), Some("order_state: closed by the server"));
    /// last.clear();
    /// assert_eq!(last.get(), None);
    /// ```
    pub fn get(&self) -> Option<String> {
        self.0.lock().expect("poisoned").clone()
    }
}

/// Sleeps `d` unless cancelled. `false` when cancelled.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_tbank::stream::sleep_or_cancel;
/// use tokio_util::sync::CancellationToken;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let cancel = CancellationToken::new();
/// assert!(sleep_or_cancel(Duration::from_millis(1), &cancel).await);
/// cancel.cancel();
/// assert!(!sleep_or_cancel(Duration::from_secs(60), &cancel).await);
/// # }
/// ```
pub async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(d) => true,
    }
}

/// Keeps one server-side stream open. `connected` goes out after every successful open (then the
/// consumer knows a break happened if it is not the first), `wrap` turns each message into a frame.
/// A stream silent for `stale_after` is closed and reopened (pings arrive on a healthy stream).
///
/// The pause before a reopen grows with each failure ([`backoff`]) and goes back to the start only after
/// the stream has delivered a message, not after the open alone: a stream that opens and breaks at once
/// is not tried again at full speed. When the open is refused for good ([`fatal_open`]), `failed(reason)`
/// is sent once and the supervisor ends.
#[allow(clippy::too_many_arguments)]
pub async fn supervise<M, F, Fut, Frame>(
    name: &'static str,
    open: F,
    connected: impl Fn() -> Frame,
    wrap: impl Fn(M) -> Frame,
    failed: impl Fn(String) -> Frame,
    tx: mpsc::Sender<Frame>,
    stale_after: Duration,
    last_error: std::sync::Arc<LastError>,
    cancel: CancellationToken,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Streaming<M>, Status>>,
{
    let mut attempt = 0u32;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let opened = tokio::select! {
            _ = cancel.cancelled() => return,
            r = open() => r,
        };
        let reason = match opened {
            Ok(mut stream) => {
                last_error.clear();
                debug!(stream = name, "stream open");
                if tx.send(connected()).await.is_err() {
                    return;
                }

                loop {
                    let msg = tokio::select! {
                        _ = cancel.cancelled() => return,
                        m = tokio::time::timeout(stale_after, stream.message()) => m,
                    };
                    match msg {
                        Ok(Ok(Some(m))) => {
                            // The stream works: the next break starts from the short pause again.
                            attempt = 0;
                            if tx.send(wrap(m)).await.is_err() {
                                return;
                            }
                        }
                        Ok(Ok(None)) => break "closed by the server".to_string(),
                        Ok(Err(status)) => {
                            break format!("{}: {}", status.code(), status.message());
                        }
                        Err(_) => break format!("silent for {stale_after:?}"),
                    }
                }
            }
            Err(status) => {
                if let Some(reason) = fatal_open(&status) {
                    warn!(stream = name, %reason, "stream refused for good, not reopened");
                    last_error.set(format!("{name}: {reason}"));
                    let _ = tx.send(failed(reason)).await;
                    return;
                }
                format!("open failed: {}: {}", status.code(), status.message())
            }
        };
        warn!(stream = name, %reason, "stream down, reconnecting");
        last_error.set(format!("{name}: {reason}"));
        if !sleep_or_cancel(backoff(attempt), &cancel).await {
            return;
        }
        attempt = attempt.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff(0), Duration::from_millis(500));
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(3), Duration::from_secs(4));
        assert_eq!(backoff(30), Duration::from_secs(30));
    }

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn only_a_final_refusal_stops_a_stream() {
        // A rejected token, no right, the limit of open streams: a repeat will not help.
        assert!(
            fatal_open(&Status::unauthenticated("40003"))
                .unwrap()
                .starts_with("unauthenticated")
        );
        assert!(
            fatal_open(&Status::resource_exhausted("80001"))
                .unwrap()
                .starts_with("streams_limit")
        );
        assert!(
            fatal_open(&Status::permission_denied("no"))
                .unwrap()
                .starts_with("PermissionDenied")
        );
        // The network, a rate limit, the lifetime of a stream, a server fault: try again.
        for retry in [
            Status::unavailable("down"),
            Status::deadline_exceeded("slow"),
            Status::internal("oops"),
            Status::resource_exhausted("80002"),
            Status::resource_exhausted("80005"),
            Status::not_found("x"),
        ] {
            assert!(fatal_open(&retry).is_none(), "{retry:?}");
        }
    }

    type Opened = std::future::Ready<Result<Streaming<u8>, Status>>;

    /// An `open` that always fails with `status`, and the number of its calls.
    fn failing(status: Status) -> (impl Fn() -> Opened, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        (
            move || {
                seen.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err(status.clone()))
            },
            calls,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_for_good_is_reported_once_and_the_supervisor_ends() {
        let (open, calls) = failing(Status::unauthenticated("40003"));
        let (tx, mut rx) = mpsc::channel::<String>(8);
        let last = Arc::new(LastError::default());
        supervise(
            "order_state",
            open,
            || "connected".to_string(),
            |_m: u8| "message".to_string(),
            |reason| format!("failed: {reason}"),
            tx,
            Duration::from_secs(5),
            Arc::clone(&last),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "not tried again");
        let told = rx.recv().await.unwrap();
        assert!(told.starts_with("failed: unauthenticated"), "{told}");
        assert!(rx.recv().await.is_none(), "nothing else was sent");
        assert!(last.get().unwrap().contains("unauthenticated"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_that_may_pass_is_retried_with_a_growing_pause_until_cancelled() {
        let (open, calls) = failing(Status::unavailable("down"));
        let (tx, mut rx) = mpsc::channel::<String>(8);
        let cancel = CancellationToken::new();
        let last = Arc::new(LastError::default());
        let task = tokio::spawn(supervise(
            "trades",
            open,
            || "connected".to_string(),
            |_m: u8| "message".to_string(),
            |reason| format!("failed: {reason}"),
            tx,
            Duration::from_secs(5),
            Arc::clone(&last),
            cancel.clone(),
        ));
        let settle = || async {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
        };
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(last.get().unwrap().contains("open failed"));
        // The first pause is 500 ms, the second 1 s: the open is not tried at full speed.
        tokio::time::advance(Duration::from_millis(499)).await;
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        tokio::time::advance(Duration::from_millis(999)).await;
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        // Nothing was reported as final.
        assert!(rx.try_recv().is_err());
        cancel.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_supervisor_that_is_cancelled_before_it_starts_does_nothing() {
        let (open, calls) = failing(Status::unavailable("down"));
        let (tx, _rx) = mpsc::channel::<String>(1);
        let cancel = CancellationToken::new();
        cancel.cancel();
        supervise(
            "x",
            open,
            || "connected".to_string(),
            |_m: u8| "message".to_string(),
            |reason| reason,
            tx,
            Duration::from_secs(1),
            Arc::new(LastError::default()),
            cancel,
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
