//! Errors of the ALOR protocol layer.
//!
//! [`AlorApiError`] is what every client method returns. The connector crate maps it to the broker-independent
//! error of `brokapi-core`.

use thiserror::Error;

/// A failure of the HTTP or WebSocket protocol layer, in the broker's own terms.
#[derive(Error, Debug)]
pub enum AlorApiError {
    /// The HTTP call failed: no status means no answer (a break, a timeout, a refused connection).
    #[error("HTTP error: {message}")]
    Http {
        /// The HTTP status, if the broker answered.
        status: Option<u16>,
        /// The text of the broker's error body, or the text of the transport error.
        message: String,
    },

    /// The WebSocket failed or the broker sent an error message on it.
    #[error("WebSocket error: {message}")]
    Ws {
        /// The code of the broker's error message (`httpCode`), if any.
        code: Option<u16>,
        /// What happened.
        message: String,
    },

    /// The authorization server refused the refresh token (revoked, expired, or of another contour).
    #[error("authorization error: {message}")]
    Auth {
        /// The text the server gave.
        message: String,
    },

    /// HTTP 404: the broker does not know this order, instrument or portfolio.
    #[error("not found: {message}")]
    NotFound {
        /// The text of the broker's error body.
        message: String,
    },

    /// An answer that is a success but is not the JSON the model expects.
    #[error("parse error: {0}")]
    Parse(String),

    /// HTTP 429: the request limit of the broker is exceeded.
    #[error("rate limit exceeded")]
    RateLimitExceeded,

    /// A subscription cannot be made (a limit of the session, or the session is closed).
    #[error("subscription error: {message}")]
    Subscription {
        /// What happened.
        message: String,
    },
}

/// The error body of the broker: `{"message": "...", "code": "..."}`, with more fields for some errors.
#[derive(Debug, serde::Deserialize)]
pub struct ApiErrorBody {
    /// The text of the error.
    #[serde(default)]
    pub message: Option<String>,
    /// The code of the error, if the broker gives one.
    #[serde(default)]
    pub code: Option<String>,
}

impl From<reqwest::Error> for AlorApiError {
    fn from(err: reqwest::Error) -> Self {
        AlorApiError::Http {
            status: err.status().map(|s| s.as_u16()),
            message: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for AlorApiError {
    fn from(err: serde_json::Error) -> Self {
        AlorApiError::Parse(err.to_string())
    }
}

impl From<url::ParseError> for AlorApiError {
    fn from(err: url::ParseError) -> Self {
        AlorApiError::Parse(err.to_string())
    }
}
