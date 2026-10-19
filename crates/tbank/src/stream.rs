//! Stream supervision: open, read, reconnect with backoff. No protocol logic here.
//!
//! A server-side stream can end at any time: the server closes it, the network breaks, or it goes
//! silent. [`supervise`] reopens it with a growing delay ([`backoff`]) and tells the consumer on every
//! successful open, so the consumer can resend its state. It never gives up until it is cancelled (or the receiving end of the channel is dropped).
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

/// Delay before reconnect attempt `n` (0-based): 500 ms doubling to 30 s. The same range at BCS and
/// Finam: none of the three brokers documents a reason for a different one.
pub fn backoff(attempt: u32) -> Duration {
    let ms = 500u64.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(ms.min(30_000))
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
#[allow(clippy::too_many_arguments)]
pub async fn supervise<M, F, Fut, Frame>(
    name: &'static str,
    open: F,
    connected: impl Fn() -> Frame,
    wrap: impl Fn(M) -> Frame,
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
                attempt = 0;
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
            Err(status) => format!("open failed: {}: {}", status.code(), status.message()),
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
}
