//! gRPC status -> public connector errors.
//!
//! T-Bank error codes (errors.md): `RESOURCE_EXHAUSTED` 800xx, `NOT_FOUND` 500xx (becomes `not_found`), `UNAUTHENTICATED` 40003,
//! `PERMISSION_DENIED` 4000x, `INVALID_ARGUMENT` 300xx. The broker puts the number in the status
//! message (and may repeat it at the start of the `message` header), see [`broker_code`].
//!
//! `RESOURCE_EXHAUSTED` is not always a rate limit. Only the codes of the table that are about time
//! (80002 requests a minute, 80003 SMS, 80006 errors a minute) become [`Error::RateLimited`]. The codes
//! that a repeat cannot fix become [`Error::Rejected`] with a code of their own: 80001
//! `streams_limit`, 80004 `no_subscriptions`, 80005 `stream_lifetime`, 80007 `stop_orders_limit`.
//! A `RESOURCE_EXHAUSTED` without a known code stays a rate limit, as before.
//!
//! Two functions decide the mapping. [`map_status`] is for reads, [`map_command_status`] is for
//! commands. The difference matters: a failed read may be repeated, a command whose outcome is
//! hidden must never be repeated, so it becomes [`Error::Unknown`].
//!
//! The error codes above are taken from the broker's documentation and the mock. They are not
//! verified against the live API.
//!
//! # Examples
//!
//! ```
//! use brokapi_core::Error;
//! use brokapi_tbank::error::{map_command_status, map_status};
//! use tonic::Status;
//!
//! // A read that timed out is a transport error: repeat it later.
//! assert!(matches!(map_status(&Status::deadline_exceeded("slow")), Error::Transport(_)));
//! // The same status on a command hides what the broker did.
//! assert!(matches!(map_command_status(&Status::deadline_exceeded("slow")), Error::Unknown));
//! // A refusal is a refusal either way.
//! assert!(matches!(map_command_status(&Status::invalid_argument("30042")), Error::Rejected { .. }));
//! ```

use std::time::Duration;

use brokapi_core::{ConfigError, Error, TransportError};
use tonic::{Code, Status};

/// `Error::Rejected` code for the broker's answer 30261: the list of stop orders is too long for one reply.
pub const STOP_ORDERS_RESPONSE_LIMIT: &str = "stop_orders_response_limit";

fn retry_after(status: &Status) -> Duration {
    // x-ratelimit-reset: seconds until the counter resets.
    status
        .metadata()
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(1))
}

fn rejected(status: &Status) -> Error {
    // The number of T-Invest tells the reasons apart (30042 no money for a margin trade, 30014 a too long range of
    // candles): both are INVALID_ARGUMENT. NOT_FOUND has the code every broker uses.
    let code = match (status.code(), broker_code(status)) {
        (Code::NotFound, _) => brokapi_core::NOT_FOUND.to_string(),
        (_, Some(number)) => number.to_string(),
        (other, None) => format!("{other:?}"),
    };
    Error::Rejected {
        code,
        message: status.message().to_string(),
    }
}

/// The five-digit T-Invest error code of a status (`80007`, `30261`), if the broker sent one.
///
/// T-Bank puts the number in the status message and the text in the `message` header. The number is
/// read from the start of either one, so both layouts work.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::error::broker_code;
/// use tonic::Status;
///
/// assert_eq!(broker_code(&Status::resource_exhausted("80007")), Some(80007));
/// assert_eq!(broker_code(&Status::resource_exhausted(" 80007: limit")), Some(80007));
/// assert_eq!(broker_code(&Status::resource_exhausted("mock failure")), None);
/// ```
pub fn broker_code(status: &Status) -> Option<u32> {
    fn lead(text: &str) -> Option<u32> {
        let t = text.trim_start();
        let digits = t.chars().take_while(char::is_ascii_digit).count();
        if digits != 5 {
            return None;
        }
        t[..5].parse().ok()
    }
    lead(status.message()).or_else(|| {
        status
            .metadata()
            .get("message")
            .and_then(|v| v.to_str().ok())
            .and_then(lead)
    })
}

/// Machine code of the T-Bank refusals that came as `RESOURCE_EXHAUSTED` but are not a rate limit.
fn exhausted_refusal(code: u32) -> Option<(&'static str, &'static str)> {
    match code {
        80001 => Some((
            "streams_limit",
            "the limit of open streams is reached: close a stream first, a repeat will not help",
        )),
        80004 => Some(("no_subscriptions", "the stream has no active subscriptions")),
        80005 => Some((
            "stream_lifetime",
            "the stream lived too long and was closed: connect again",
        )),
        80007 => Some((
            "stop_orders_limit",
            "the limit of stop orders for this instrument is reached: cancel one first, a repeat will not help",
        )),
        _ => None,
    }
}

/// How long to wait after a rate limit with no `x-ratelimit-reset` header. The broker's table says
/// "try again in a minute" for 80003 (SMS) and counts 80006 (errors) per minute.
fn exhausted_wait(status: &Status) -> Duration {
    match broker_code(status) {
        Some(80003 | 80006) if status.metadata().get("x-ratelimit-reset").is_none() => {
            Duration::from_secs(60)
        }
        _ => retry_after(status),
    }
}

/// Status of a read-only call (snapshots, reference data).
///
/// - `ResourceExhausted` gives [`Error::RateLimited`], with `retry_after` taken from the
///   `x-ratelimit-reset` header (seconds), else 1 s (60 s for the codes 80003 and 80006, which the
///   broker counts per minute). Except the codes that are not a limit of time: 80001, 80004, 80005
///   and 80007 give [`Error::Rejected`] with the code `streams_limit`, `no_subscriptions`,
///   `stream_lifetime` or `stop_orders_limit`, because a repeat cannot help.
/// - `InvalidArgument` with the code 30261 (too many stop orders in the answer) gives
///   [`Error::Rejected`] with the code `stop_orders_response_limit`.
/// - `NotFound` gives [`Error::Rejected`] with the code `not_found` ([`brokapi_core::NOT_FOUND`], the
///   same at every broker).
/// - `InvalidArgument`, `PermissionDenied`, `FailedPrecondition`, `Unimplemented`,
///   `AlreadyExists`, `OutOfRange` gives [`Error::Rejected`]. Its `code` is the five-digit number of the
///   broker when the status carries one (for example `"30042"`, not enough assets for a margin trade), else the
///   status code name (for example `"InvalidArgument"`). The status message is the `message`.
/// - `Unauthenticated` gives [`Error::Config`]: the token is wrong or expired, fix it.
/// - everything else (`Unavailable`, `DeadlineExceeded`, `Internal`, ...) gives [`Error::Transport`].
///
/// # Examples
///
/// ```
/// use brokapi_core::Error;
/// use brokapi_tbank::error::map_status;
/// use tonic::Status;
///
/// match map_status(&Status::not_found("50002")) {
///     Error::Rejected { code, message } => {
///         assert_eq!(code, "not_found");
///         assert_eq!(message, "50002");
///     }
///     other => panic!("unexpected {other:?}"),
/// }
/// assert!(matches!(map_status(&Status::resource_exhausted("80002")), Error::RateLimited { .. }));
/// // 80007 is a business refusal, not a limit of time.
/// match map_status(&Status::resource_exhausted("80007")) {
///     Error::Rejected { code, .. } => assert_eq!(code, "stop_orders_limit"),
///     other => panic!("unexpected {other:?}"),
/// }
/// assert!(matches!(map_status(&Status::unauthenticated("40003")), Error::Config(_)));
/// ```
pub fn map_status(status: &Status) -> Error {
    match status.code() {
        Code::ResourceExhausted => {
            match broker_code(status).and_then(|c| Some((c, exhausted_refusal(c)?))) {
                Some((num, (code, text))) => Error::Rejected {
                    code: code.to_string(),
                    message: format!("{num}: {text}"),
                },
                None => Error::RateLimited {
                    retry_after: exhausted_wait(status),
                },
            }
        }
        Code::InvalidArgument if broker_code(status) == Some(30261) => Error::Rejected {
            code: STOP_ORDERS_RESPONSE_LIMIT.to_string(),
            message:
                "30261: too many stop orders for one answer: narrow the request with from and to"
                    .to_string(),
        },
        Code::InvalidArgument
        | Code::NotFound
        | Code::PermissionDenied
        | Code::FailedPrecondition
        | Code::Unimplemented
        | Code::AlreadyExists
        | Code::OutOfRange => rejected(status),
        Code::Unauthenticated => Error::Config(ConfigError::Message(format!(
            "unauthenticated: {}",
            status.message()
        ))),
        _ => Error::Transport(TransportError::Message(format!(
            "{:?}: {}",
            status.code(),
            status.message()
        ))),
    }
}

/// Status of a command (place / cancel / replace). Only statuses that prove the broker did not act
/// become `Rejected`; anything that can hide a processed request is `Unknown`.
///
/// The statuses that prove the broker did not act (`ResourceExhausted`, `InvalidArgument`,
/// `NotFound`, `PermissionDenied`, `Unauthenticated`, `FailedPrecondition`, `Unimplemented`,
/// `AlreadyExists`, `OutOfRange`) are mapped like in [`map_status`]. Every other status
/// (`DeadlineExceeded`, `Unavailable`, `Internal`, `Aborted`, `Cancelled`, `Unknown`, ...) is
/// [`Error::Unknown`], because the request may have been processed before the answer was lost.
/// Do not repeat the command after it: ask the broker for the order first.
///
/// # Examples
///
/// ```
/// use brokapi_core::Error;
/// use brokapi_tbank::error::map_command_status;
/// use tonic::Status;
///
/// assert!(matches!(map_command_status(&Status::unavailable("reset")), Error::Unknown));
/// assert!(matches!(map_command_status(&Status::not_found("50005")), Error::Rejected { .. }));
/// ```
pub fn map_command_status(status: &Status) -> Error {
    match status.code() {
        Code::ResourceExhausted
        | Code::InvalidArgument
        | Code::NotFound
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::FailedPrecondition
        | Code::Unimplemented
        | Code::AlreadyExists
        | Code::OutOfRange => map_status(status),
        _ => Error::Unknown,
    }
}

/// `true` for the `NotFound` status: the broker does not know the order or the instrument.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::error::is_not_found;
/// use tonic::Status;
///
/// assert!(is_not_found(&Status::not_found("50005")));
/// assert!(!is_not_found(&Status::unavailable("x")));
/// ```
pub fn is_not_found(status: &Status) -> bool {
    status.code() == Code::NotFound
}

/// Builds a [`Error::Transport`] with a text message.
///
/// # Examples
///
/// ```
/// use brokapi_core::Error;
///
/// assert!(matches!(brokapi_tbank::error::transport("stream closed"), Error::Transport(_)));
/// ```
pub fn transport(msg: impl Into<String>) -> Error {
    Error::Transport(TransportError::Message(msg.into()))
}

/// Builds a [`Error::Config`] with a text message.
///
/// # Examples
///
/// ```
/// use brokapi_core::Error;
///
/// assert!(matches!(brokapi_tbank::error::config("no account"), Error::Config(_)));
/// ```
pub fn config(msg: impl Into<String>) -> Error {
    Error::Config(ConfigError::Message(msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_are_rate_limited() {
        let e = map_status(&Status::resource_exhausted("80002"));
        assert!(matches!(e, Error::RateLimited { .. }));
    }

    fn refusal_code(status: &Status) -> String {
        match map_status(status) {
            Error::Rejected { code, .. } => code,
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn exhausted_codes_that_are_not_a_limit_of_time_are_refusals() {
        // The number can be in the status message or at the start of the `message` header.
        assert_eq!(
            refusal_code(&Status::resource_exhausted("80007")),
            "stop_orders_limit"
        );
        assert_eq!(
            refusal_code(&Status::resource_exhausted("80001")),
            "streams_limit"
        );
        assert_eq!(
            refusal_code(&Status::resource_exhausted("80004")),
            "no_subscriptions"
        );
        assert_eq!(
            refusal_code(&Status::resource_exhausted("80005")),
            "stream_lifetime"
        );
        let mut by_header = Status::resource_exhausted("");
        by_header
            .metadata_mut()
            .insert("message", "80007 Stop-orders limit".parse().unwrap());
        assert_eq!(refusal_code(&by_header), "stop_orders_limit");
        // A command refused with 80007 is a refusal, the broker did not act.
        assert!(matches!(
            map_command_status(&Status::resource_exhausted("80007")),
            Error::Rejected { .. }
        ));
    }

    #[test]
    fn exhausted_codes_about_time_and_unknown_ones_stay_rate_limits() {
        for text in ["80002", "80003", "80006", "mock failure", "80099"] {
            let e = map_status(&Status::resource_exhausted(text));
            assert!(matches!(e, Error::RateLimited { .. }), "{text}: {e:?}");
        }
        let wait = |s: &Status| match map_status(s) {
            Error::RateLimited { retry_after } => retry_after,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            wait(&Status::resource_exhausted("80002")),
            Duration::from_secs(1)
        );
        assert_eq!(
            wait(&Status::resource_exhausted("80003")),
            Duration::from_secs(60)
        );
        assert_eq!(
            wait(&Status::resource_exhausted("80006")),
            Duration::from_secs(60)
        );
        let mut with_header = Status::resource_exhausted("80006");
        with_header
            .metadata_mut()
            .insert("x-ratelimit-reset", "7".parse().unwrap());
        assert_eq!(wait(&with_header), Duration::from_secs(7));
    }

    #[test]
    fn too_many_stop_orders_in_the_answer_is_a_named_refusal() {
        assert_eq!(
            refusal_code(&Status::invalid_argument("30261")),
            STOP_ORDERS_RESPONSE_LIMIT
        );
    }

    #[test]
    fn a_refusal_with_a_number_of_the_broker_has_the_number_as_its_code() {
        // 30042 and 30014 are both INVALID_ARGUMENT: only the number tells them apart.
        assert_eq!(refusal_code(&Status::invalid_argument("30042")), "30042");
        assert_eq!(refusal_code(&Status::invalid_argument("30014")), "30014");
        assert_eq!(refusal_code(&Status::permission_denied("40002")), "40002");
        // Without a number the status name stays, and NOT_FOUND is the same code at every broker.
        assert_eq!(
            refusal_code(&Status::invalid_argument("bad request")),
            "InvalidArgument"
        );
        assert_eq!(
            refusal_code(&Status::not_found("50002")),
            brokapi_core::NOT_FOUND
        );
        // A refused stream stays fatal with the number in the code.
        assert!(crate::stream::fatal_open(&Status::permission_denied("40002")).is_some());
    }

    #[test]
    fn command_timeout_is_unknown_not_rejected() {
        assert!(matches!(
            map_command_status(&Status::deadline_exceeded("t")),
            Error::Unknown
        ));
        assert!(matches!(
            map_command_status(&Status::unavailable("x")),
            Error::Unknown
        ));
        assert!(matches!(
            map_command_status(&Status::internal("x")),
            Error::Unknown
        ));
    }

    #[test]
    fn command_refusal_is_rejected() {
        let e = map_command_status(&Status::invalid_argument("30016"));
        assert!(matches!(e, Error::Rejected { .. }));
    }

    #[test]
    fn query_failure_is_transport() {
        assert!(matches!(
            map_status(&Status::unavailable("x")),
            Error::Transport(_)
        ));
    }
}
