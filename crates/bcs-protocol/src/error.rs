//! Errors of the BCS protocol layer.
//!
//! [`BcsApiError`] is what every client method returns. The other types in this module describe the
//! error bodies the broker sends. The connector crate maps [`BcsApiError`] to the broker-independent
//! error of `brokapi-core`.

use thiserror::Error;

/// The error of every REST call and of the subscription calls of a WebSocket session.
///
/// Read the variant to decide what to do:
///
/// * [`BcsApiError::ValidationError`] and [`BcsApiError::NotFound`] are answers of the broker. The
///   request reached it and was refused. Retrying the same request gives the same answer.
/// * [`BcsApiError::RateLimitExceeded`] means "slow down". The broker limits REST calls per account,
///   about 10 per second for most services and 3 per second for non-trade operations (broker docs).
/// * [`BcsApiError::AuthError`] means the refresh token was refused. A new token is needed.
/// * [`BcsApiError::HttpError`] with no status is a transport failure (connection, timeout). For a
///   command that creates an order this is an **unknown outcome**: the order may exist. Never repeat
///   such a command blindly, ask for the order by its client order id first.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::error::BcsApiError;
///
/// let err = BcsApiError::NotFound { trace_id: Some("abc".into()) };
/// assert!(matches!(err, BcsApiError::NotFound { .. }));
/// // The messages are for logs, not for parsing.
/// assert!(!err.to_string().is_empty());
/// ```
#[derive(Error, Debug)]
pub enum BcsApiError {
    /// An HTTP-level failure.
    ///
    /// Three sources: a transport failure (`status` is `None`, `message` is the transport text), a
    /// non-success status with the broker's JSON error body (`message` is the broker's error `type`
    /// such as `UNAUTHORIZED`, `FORBIDDEN` or `RESOURCE_EXHAUSTED`, and `trace_id` is set), and a
    /// non-success status with a body that is not that JSON (`message` is the raw body).
    #[error("HTTP ошибка: {message}")]
    HttpError {
        /// HTTP status code, if the broker answered at all.
        status: Option<u16>,
        /// The broker's error type, the raw body, or the transport error text.
        message: String,
        /// The broker's trace id, to quote to the broker support. `None` for transport failures.
        trace_id: Option<String>,
    },

    /// A WebSocket-level failure.
    ///
    /// The session raises it when it cannot open a connection. The session logs it and retries by
    /// itself, so you rarely see it as a return value.
    #[error("WebSocket ошибка: {message}")]
    WsError {
        /// Error code, if the broker gave one.
        code: Option<String>,
        /// Description of the failure.
        message: String,
    },

    /// The token endpoint refused the refresh token (expired, deleted or of a wrong kind).
    ///
    /// `message` is the `error_description` of the token service, or its `error` if there is no
    /// description. Fix the token. Repeating the call does not help.
    #[error("Ошибка авторизации: {message}")]
    AuthError {
        /// The text from the token service.
        message: String,
    },

    /// The broker refused the request body or parameters (`type = VALIDATION_ERROR`).
    ///
    /// For an order this is a refusal: nothing was placed. `errors` names the fields. A price outside
    /// the exchange band is a typical cause (not verified against the live API which field is named).
    #[error("Ошибка валидации: {errors:?}")]
    ValidationError {
        /// The broker's trace id.
        trace_id: Option<String>,
        /// The refused fields, possibly empty.
        errors: Vec<ValidationFieldError>,
    },

    /// The broker has no such object (`type = NOT_FOUND`), for example an unknown order.
    #[error("Не найдено: {trace_id:?}")]
    NotFound {
        /// The broker's trace id.
        trace_id: Option<String>,
    },

    /// A reply or a URL could not be parsed.
    ///
    /// The text holds the parser's message. For a reply, it also holds the first 200 bytes of the
    /// body. The request itself did go through: for an order command this is an **unknown outcome**
    /// of the order, look it up before you repeat it.
    #[error("Ошибка парсинга: {0}")]
    ParseError(String),

    /// HTTP 429 without the broker's JSON error body.
    ///
    /// A 429 that comes with the JSON body is [`BcsApiError::HttpError`] with `status = Some(429)`
    /// and `message = "RESOURCE_EXHAUSTED"`. Handle both, or use the rate limiter in
    /// [`crate::utils::rate_limiter`] to stay below the limit.
    #[error("Превышен лимит запросов")]
    RateLimitExceeded,

    /// A subscription command was refused locally, before anything was sent.
    ///
    /// Causes: the session is not a market data session, the limit of 100 subscriptions per session is
    /// reached, or the session task has ended.
    #[error("Ошибка подписки: {message}")]
    SubscriptionError {
        /// Why the command was refused.
        message: String,
    },
}

/// One refused field in a [`BcsApiError::ValidationError`] (an entry of `errors` in the broker's body).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::error::ValidationFieldError;
///
/// let e: ValidationFieldError =
///     serde_json::from_str(r#"{"type":"INVALID_VALUE","field":"price","payload":{"message":"too low"}}"#)?;
/// assert_eq!(e.error_type, "INVALID_VALUE");
/// assert_eq!(e.field.as_deref(), Some("price"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ValidationFieldError {
    /// Kind of the problem, as the broker names it (JSON field `type`).
    #[serde(rename = "type")]
    pub error_type: String,
    /// Name of the request field the problem is about. `None` if the problem is not about one field.
    #[serde(default)]
    pub field: Option<String>,
    /// Extra data of the broker, free-form JSON. The shape depends on `error_type`.
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

/// The JSON body the broker sends with a non-success status.
///
/// The client reads it to pick the [`BcsApiError`] variant. You see this type only if you parse a raw
/// reply yourself.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::error::ApiErrorResponse;
///
/// let body = r#"{"timestamp":1760860000000,"traceId":"t-1","type":"VALIDATION_ERROR",
///                "errors":[{"type":"INVALID_VALUE","field":"orderQuantity"}]}"#;
/// let e: ApiErrorResponse = serde_json::from_str(body)?;
/// assert_eq!(e.error_type, "VALIDATION_ERROR");
/// assert_eq!(e.trace_id.as_deref(), Some("t-1"));
/// assert_eq!(e.errors.len(), 1);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiErrorResponse {
    /// Time of the error. The docs give an integer and no unit (not verified, looks like epoch
    /// milliseconds).
    #[serde(default)]
    pub timestamp: Option<u64>,
    /// The broker's trace id of the request, to quote to the broker support.
    #[serde(default, rename = "traceId")]
    pub trace_id: Option<String>,
    /// Class of the error (JSON field `type`). The docs list `VALIDATION_ERROR`, `RESOURCE_EXHAUSTED`,
    /// `USER_BLOCKED`, `BAD_REQUEST`, `NOT_FOUND`, `UNAUTHORIZED`, `FORBIDDEN`, `CONFLICT`,
    /// `INTERNAL_SERVER_ERROR`, `SESSION_NOT_FOUND_ERROR`, `SESSION_EXPIRED_ERROR`,
    /// `SESSION_FAILED_ERROR` and `USER_NOT_FOUND`. Only the first and `NOT_FOUND` get their own
    /// [`BcsApiError`] variant.
    #[serde(rename = "type")]
    pub error_type: String,
    /// Per-field details, empty when the error is not about fields.
    #[serde(default)]
    pub errors: Vec<ValidationFieldError>,
}

/// The error body of the token service (it is a Keycloak server).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::error::AuthErrorResponse;
///
/// let e: AuthErrorResponse =
///     serde_json::from_str(r#"{"error":"invalid_grant","error_description":"Token is not active"}"#)?;
/// assert_eq!(e.error, "invalid_grant");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, serde::Deserialize)]
pub struct AuthErrorResponse {
    /// Machine code, for example `invalid_grant`.
    pub error: String,
    /// Human text. The client puts it in [`BcsApiError::AuthError`] when it is present.
    pub error_description: Option<String>,
}

impl From<reqwest::Error> for BcsApiError {
    fn from(err: reqwest::Error) -> Self {
        BcsApiError::HttpError {
            status: err.status().map(|s| s.as_u16()),
            message: err.to_string(),
            trace_id: None,
        }
    }
}

impl From<serde_json::Error> for BcsApiError {
    fn from(err: serde_json::Error) -> Self {
        BcsApiError::ParseError(err.to_string())
    }
}

impl From<url::ParseError> for BcsApiError {
    fn from(err: url::ParseError) -> Self {
        BcsApiError::ParseError(err.to_string())
    }
}
