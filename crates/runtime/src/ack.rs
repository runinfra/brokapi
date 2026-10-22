//! Waiting for the broker to confirm a new subscription.
//!
//! A `subscribe_*` call that returns before the stream is open gives the caller a promise the connector
//! cannot keep: an order placed right after it can be filled before the stream is live, and the bot never
//! gets the event. So every connector holds the call until the broker has confirmed the subscription, or the
//! limit has passed ([`wait_ack`]), and a subscription that was not confirmed is taken back, not left half open.

use std::sync::Arc;
use std::time::Duration;

use brokapi_core::{Error, TransportError};
use tokio::sync::{Mutex, Notify};

/// What a connector knows about one new subscription, for [`wait_ack`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckState {
    /// The request is out, the broker has not answered.
    Pending,
    /// The broker confirmed the subscription (or the data of it has started to come, which proves it).
    Confirmed,
    /// The broker refused it. The text is the broker's reason.
    Refused(String),
}

/// The code of the [`Error::Rejected`] that a refused subscription gives. The same at every broker.
pub const SUBSCRIPTION_REFUSED: &str = "subscription_refused";

/// Waits until the broker has confirmed a subscription.
///
/// `state` reads the state of the subscription from `core` and is read again after every wake of `notify`
/// (the core calls `notify_waiters` whenever a subscription changes state). The wait ends with:
///
/// - `Ok(())` when the state is [`AckState::Confirmed`];
/// - [`Error::Rejected`] with the code [`SUBSCRIPTION_REFUSED`] when it is [`AckState::Refused`]: the broker
///   said no, repeating the call unchanged will not help;
/// - [`Error::Transport`] when `limit` passes with the state still [`AckState::Pending`]: the stream did not
///   open in time (the network, or a slow broker). The call may be repeated.
///
/// The limit is counted on the tokio clock, so a test moves it with `tokio::time::advance`.
///
/// The caller owns the cleanup: on `Err` it takes the subscription back (nothing stays half open).
///
/// # Errors
///
/// As listed above.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use brokapi_runtime::{AckState, wait_ack};
/// use tokio::sync::{Mutex, Notify};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let core = Arc::new(Mutex::new(false)); // "the broker confirmed"
/// let notify = Arc::new(Notify::new());
///
/// // The broker confirms a moment later; the core flips the flag and wakes the waiters.
/// let (c, n) = (Arc::clone(&core), Arc::clone(&notify));
/// tokio::spawn(async move {
///     *c.lock().await = true;
///     n.notify_waiters();
/// });
///
/// let state = |confirmed: &bool| if *confirmed { AckState::Confirmed } else { AckState::Pending };
/// wait_ack(&core, notify, Duration::from_secs(3), state).await.unwrap();
/// # }
/// ```
pub async fn wait_ack<C>(
    core: &Mutex<C>,
    notify: Arc<Notify>,
    limit: Duration,
    state: impl Fn(&C) -> AckState,
) -> Result<(), Error> {
    let wait = async {
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            // Registered before the state is read, so a change in between is not missed.
            notified.as_mut().enable();
            match state(&*core.lock().await) {
                AckState::Confirmed => return Ok(()),
                AckState::Refused(reason) => {
                    return Err(Error::Rejected {
                        code: SUBSCRIPTION_REFUSED.into(),
                        message: if reason.is_empty() {
                            "the broker refused the subscription".into()
                        } else {
                            reason
                        },
                    });
                }
                AckState::Pending => {}
            }
            notified.await;
        }
    };
    match tokio::time::timeout(limit, wait).await {
        Ok(result) => result,
        Err(_) => Err(not_confirmed(limit)),
    }
}

/// The error of a subscription that was not confirmed within `limit`: [`Error::Transport`].
#[must_use]
pub fn not_confirmed(limit: Duration) -> Error {
    Error::Transport(TransportError::Message(format!(
        "the subscription was not confirmed by the broker within {limit:?}"
    )))
}
