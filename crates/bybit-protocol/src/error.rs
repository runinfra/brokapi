//! Errors of the Bybit protocol layer.
//!
//! [`BybitApiError`] is what every client method returns. The connector crate maps it to the broker-independent error
//! of `brokapi-core`.

use thiserror::Error;

/// An error of a call to Bybit.
#[derive(Error, Debug)]
pub enum BybitApiError {
    /// The transport failed, or the server answered a status that is not a success.
    #[error("HTTP error: {message}")]
    Http {
        /// The HTTP status, when the server answered.
        status: Option<u16>,
        /// What went wrong.
        message: String,
    },

    /// The server answered HTTP 200 with a non-zero `retCode`: a refusal of the request itself.
    #[error("API error {code}: {message}")]
    Api {
        /// The `retCode`.
        code: i64,
        /// The `retMsg`.
        message: String,
    },

    /// The WebSocket failed.
    #[error("WebSocket error: {message}")]
    Ws {
        /// What went wrong.
        message: String,
    },

    /// The key pair was refused (HTTP 401, or a `retCode` of authentication: 10003, 10004, 10005, 10007, 10010).
    #[error("authorization error: {message}")]
    Auth {
        /// The reason the exchange gave.
        message: String,
    },

    /// The answer is a success but is not the JSON that was expected.
    #[error("parse error: {0}")]
    Parse(String),

    /// The request rate limit: HTTP 429, or `retCode` 10006.
    #[error("rate limit exceeded")]
    RateLimitExceeded,

    /// A subscription could not be made.
    #[error("subscription error: {message}")]
    Subscription {
        /// What went wrong.
        message: String,
    },
}

impl BybitApiError {
    /// The `retCode` of a refusal, if this is one.
    pub fn code(&self) -> Option<i64> {
        match self {
            BybitApiError::Api { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for BybitApiError {
    fn from(err: reqwest::Error) -> Self {
        BybitApiError::Http {
            status: err.status().map(|s| s.as_u16()),
            message: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for BybitApiError {
    fn from(err: serde_json::Error) -> Self {
        BybitApiError::Parse(err.to_string())
    }
}

/// The `retCode`s that mean the key pair, its rights or its IP binding are wrong.
pub fn is_auth_code(code: i64) -> bool {
    matches!(code, 10003 | 10004 | 10005 | 10007 | 10010 | 33004)
}
