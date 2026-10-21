//! Shutdown types: how to stop a connector and what the report says.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::order::ClientOrderId;

/// How a connector stops: the argument of `shutdown`.
///
/// Shutdown NEVER cancels or changes orders at the exchange. A working order outlives your
/// process. Cancel orders yourself before shutdown if you want them gone. Shutdown only drops the
/// local wait for an answer.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_core::ShutdownMode;
///
/// let polite = ShutdownMode::Graceful { deadline: Duration::from_secs(5) };
/// assert_ne!(polite, ShutdownMode::Immediate);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ShutdownMode {
    /// Stop taking commands (they get [`Error::ShuttingDown`](crate::Error::ShuttingDown)),
    /// unsubscribe the way the broker's protocol wants where the connector does that, flush the
    /// events still waiting, then stop. After `deadline` it turns into `Immediate`.
    Graceful {
        /// How long to wait for the polite steps.
        deadline: Duration,
    },
    /// Stop now. Skips the unsubscribe and the wait.
    Immediate,
}

/// What a shutdown did: the return value of `shutdown`.
///
/// # Examples
///
/// ```
/// use brokapi_core::ShutdownReport;
///
/// let report = ShutdownReport::empty();
/// assert_eq!(report.unsubscribed, 0);
/// assert!(report.inflight_abandoned.is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ShutdownReport {
    /// Subscriptions closed in the broker's protocol.
    pub unsubscribed: usize,
    /// Subscriptions the connector tried to close and could not.
    pub unsubscribe_failed: usize,
    /// Commands that were sent and had no answer yet when the shutdown came. They are abandoned
    /// locally, which means their outcome is [`Error::Unknown`](crate::Error::Unknown): the
    /// exchange orders are untouched, look at them after the restart with
    /// [`OrderExecutor::reconcile`](crate::OrderExecutor::reconcile).
    pub inflight_abandoned: Vec<ClientOrderId>,
}

impl ShutdownReport {
    /// A report with the given counts. The struct is `#[non_exhaustive]`: build it with this function.
    pub fn new(
        unsubscribed: usize,
        unsubscribe_failed: usize,
        inflight_abandoned: Vec<ClientOrderId>,
    ) -> Self {
        Self {
            unsubscribed,
            unsubscribe_failed,
            inflight_abandoned,
        }
    }

    /// A report of a shutdown that had nothing to do.
    pub fn empty() -> Self {
        Self {
            unsubscribed: 0,
            unsubscribe_failed: 0,
            inflight_abandoned: Vec::new(),
        }
    }
}
