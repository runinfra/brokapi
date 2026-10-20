//! The error type of every connector call and the rule for what to do with each error.
//!
//! One enum, [`enum@Error`], for all brokers. A connector maps everything its broker or network can say
//! into one of nine variants, so the code that handles errors does not depend on the broker.
//! [`Error::class`] gives the coarse answer to "may I try again": see [`ErrorClass`].
//!
//! The one variant that needs care is [`Error::Unknown`]. It says that a command (place, cancel,
//! replace) may or may not have reached the broker. Never repeat the command after it: look at the
//! order first, see [`Error::Unknown`].

use std::borrow::Cow;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A configuration problem: a value that is missing, malformed, or of the wrong kind for this broker.
///
/// Carried by [`Error::Config`]. The text is for a human (a log line), it is not a stable code.
///
/// # Examples
///
/// ```
/// use brokapi_core::{ConfigError, Error};
///
/// let e: Error = ConfigError::Message("token is empty".into()).into();
/// assert_eq!(e.to_string(), "configuration error: token is empty");
/// ```
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum ConfigError {
    /// What is wrong, in words.
    #[error("{0}")]
    Message(String),
}

/// A network-level problem: connect, TLS, timeout of a read, a broken stream, a malformed answer.
///
/// Carried by [`Error::Transport`]. The text is for a human (a log line), it is not a stable code.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Error, TransportError};
///
/// let e: Error = TransportError::Message("connection reset".into()).into();
/// assert_eq!(e.to_string(), "transport error: connection reset");
/// ```
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum TransportError {
    /// What went wrong, in words.
    #[error("{0}")]
    Message(String),
}

/// The coarse answer to "may I try this call again": see [`Error::class`].
///
/// # Examples
///
/// ```
/// use brokapi_core::{Error, ErrorClass};
///
/// assert_eq!(Error::Unknown.class(), ErrorClass::Unknown);
/// assert_eq!(Error::NotReconciled.class(), ErrorClass::Fatal);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorClass {
    /// The call failed for a reason that can pass: the network, a rate limit. Reading calls may be
    /// repeated, after the delay in [`Error::RateLimited`] if there is one. For a command, first find
    /// out what the broker has (see [`Error::Transport`]).
    Retryable,
    /// The same call will fail again until something else changes: a setup, a refused request, a
    /// missing capability, a gate that is closed. Do not repeat it unchanged.
    Fatal,
    /// The outcome of a command is not known: [`Error::Unknown`]. Repeating the command could create
    /// a second order. The library never repeats it and neither should the caller before it knows
    /// the state of the order.
    Unknown,
}

/// What can go wrong in a connector call.
///
/// The texts printed by `Display` are the ones on the errors page of the book (`docs/src/errors.md`),
/// with what they mean and what to do. A test keeps the two in step.
///
/// Each variant says what happened and what the caller does next. [`Error::class`] groups them.
/// Reading calls (order book, history, portfolio) return only the variants that make sense for a
/// read: [`Error::Config`], [`Error::Transport`], [`Error::RateLimited`], [`Error::Rejected`] and
/// [`Error::Unsupported`]. The gate variants ([`Error::NotReconciled`], [`Error::Degraded`],
/// [`Error::ShuttingDown`]) come from trading commands.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_core::{Error, ErrorClass};
///
/// /// What a caller does with an error of a reading call.
/// fn next_step(e: &Error) -> &'static str {
///     match e {
///         Error::RateLimited { retry_after } if *retry_after < Duration::from_secs(5) => "wait and repeat",
///         Error::Transport(_) => "repeat later",
///         Error::Unsupported { .. } => "ask the broker another way, or skip",
///         _ => "give up and report",
///     }
/// }
///
/// let e = Error::RateLimited { retry_after: Duration::from_secs(1) };
/// assert_eq!(e.class(), ErrorClass::Retryable);
/// assert_eq!(next_step(&e), "wait and repeat");
/// ```
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum Error {
    /// The setup is wrong: a bad config value, or an input that cannot work with this broker (for
    /// example an [`InstrumentId`](crate::InstrumentId) whose [`NativeId`](crate::NativeId) is of
    /// another broker's kind).
    ///
    /// Nothing was sent. The call will fail the same way until the config or the input is fixed.
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),

    /// The network or the broker's endpoint failed in a way that is not an answer: connect, TLS,
    /// timeout of a read, a stream cut, an authentication that failed, an unreadable reply.
    ///
    /// For a reading call, repeat it later. For a trading command the connector returns
    /// [`Error::Unknown`] instead whenever the command might have reached the broker, so
    /// `Transport` on a command means the connector has no sign that it did (for example the
    /// authentication failed). Even then, look at the order before you send a command again, and use
    /// a new `cid` for a new place.
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),

    /// A rate limit stopped the call: the local limiter of the connector or the broker's own answer
    /// (HTTP 429 or the equivalent).
    ///
    /// Wait `retry_after`, then repeat. A command refused by the local limiter was not sent. A
    /// connector may still have used up the `cid` (see its docs), so for a place the safe rule is a
    /// new `cid`. The library does not wait and repeat for you.
    #[error("rate limited, retry after {retry_after:?}")]
    RateLimited {
        /// How long to wait before the next try. A hint from the limiter or the broker, not a
        /// promise that the next call passes.
        retry_after: Duration,
    },

    /// The broker (or the connector, before sending) said no to this request, and nothing was done.
    ///
    /// The request is wrong or not allowed: a price outside the band, no money, an unknown order,
    /// an order that is already filled, a missing price on a limit order. Change the request. Do not
    /// repeat it as it is.
    ///
    /// `code` is a short machine-readable label that the connector sets (for example `validation`,
    /// `missing_price`, `not_found`, `already_filled`, `duplicate_place`, `http_400`). `message` is
    /// the broker's text or the connector's explanation, for a human. The set of codes belongs to the
    /// connector and is not fixed across brokers. Do not parse `message`.
    #[error("rejected: {code}: {message}")]
    Rejected {
        /// Short label of the reason, set by the connector. Safe to compare with `==`.
        code: String,
        /// Explanation for a human, usually the broker's own text.
        message: String,
    },

    /// A command (place, cancel, replace, place_stop, cancel_stop) was sent, and no answer says
    /// whether the broker did it: a timeout, a connection cut after sending, an HTTP 5xx, an answer
    /// that cannot be read.
    ///
    /// The order may exist, may be cancelled, or may never have arrived. The library NEVER repeats
    /// the command, because a repeated place could create a second order. Do the same:
    /// 1. Do not send the command again.
    /// 2. Ask the broker: [`OrderExecutor::order_state`](crate::OrderExecutor::order_state) with the
    ///    `cid`, or [`OrderExecutor::active_orders`](crate::OrderExecutor::active_orders), or look for
    ///    the fills in [`ExecutionSource::executions`](crate::ExecutionSource::executions).
    /// 3. Only when the broker has no such order, place a new one under a NEW `cid`.
    ///
    /// Never turned into [`Error::Transport`] or [`Error::Rejected`]: those would say "nothing
    /// happened", and here that is not known.
    #[error("command outcome unknown: do not send it again, ask the broker with order_state(cid)")]
    Unknown,

    /// This connector or this broker does not have the capability, so the call was not made.
    ///
    /// Returned at once, without a network call. `capability` is a short name of what is missing
    /// (for example `stop_order_stream`). The library does not emulate a missing stream by polling.
    /// If you need the data, poll yourself. See also [`UserDataCaps`](crate::UserDataCaps) and
    /// [`MarketDataCaps`](crate::MarketDataCaps), which declare what a connector can do.
    #[error("unsupported capability: {capability}")]
    Unsupported {
        /// Short name of the missing capability. Safe to compare with `==` against a `&str`.
        /// A `Cow` so that the error survives serialization (for example across a process boundary).
        capability: Cow<'static, str>,
    },

    /// The trading gate is closed: this connector has not run [`OrderExecutor::reconcile`](crate::OrderExecutor::reconcile)
    /// successfully yet.
    ///
    /// Trading commands wait for the first reconcile, so that the connector knows which orders are
    /// already at the broker before it puts a new one. Call `reconcile` with your ledger (an empty
    /// slice if you have none), then repeat the command. Nothing was sent.
    #[error("not reconciled: call reconcile() before trading commands")]
    NotReconciled,

    /// The trading gate is closed because the connector lost events it could not afford to lose: a
    /// [`Backpressure::Lossless`](crate::Backpressure::Lossless) subscription overflowed (an
    /// [`Event::Overflow`](crate::Event::Overflow) was sent).
    ///
    /// The local state of orders may have a hole. Drain your event channel, then call
    /// [`OrderExecutor::reconcile`](crate::OrderExecutor::reconcile): a successful reconcile opens the gate again.
    /// Nothing was sent.
    #[error(
        "connector degraded: events may be lost, read the event channel to the end and call reconcile()"
    )]
    Degraded,

    /// The connector is shutting down and takes no new trading commands.
    ///
    /// Final for this connector object. Orders already at the exchange are not touched by a
    /// shutdown. Start a new connector if you need to trade. Nothing was sent.
    #[error("shutting down: the connector takes no new trading commands")]
    ShuttingDown,
}

impl Error {
    /// The coarse class of this error: may the call be tried again, or not.
    ///
    /// | Variant | Class |
    /// |---|---|
    /// | [`Error::Transport`], [`Error::RateLimited`] | [`ErrorClass::Retryable`] |
    /// | [`Error::Unknown`] | [`ErrorClass::Unknown`] |
    /// | all others | [`ErrorClass::Fatal`] |
    ///
    /// The class says what is possible in principle. A trading command is never repeated by the
    /// library, whatever the class.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, ErrorClass, TransportError};
    ///
    /// let e = Error::Transport(TransportError::Message("timeout".into()));
    /// assert_eq!(e.class(), ErrorClass::Retryable);
    /// ```
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::Transport(_) | Self::RateLimited { .. } => ErrorClass::Retryable,
            Self::Unknown => ErrorClass::Unknown,
            Self::Config(_)
            | Self::Rejected { .. }
            | Self::Unsupported { .. }
            | Self::NotReconciled
            | Self::Degraded
            | Self::ShuttingDown => ErrorClass::Fatal,
        }
    }
}
