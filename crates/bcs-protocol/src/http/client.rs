//! The authorized HTTP client: token handling, the 401 retry and the mapping of error replies.
//!
//! The methods in this file are generic ([`BcsHttpClient::get`], [`BcsHttpClient::post`] and so on).
//! The endpoint-specific methods (`get_order_book`, `create_order`, ...) are in [`super::endpoints`].

use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, warn};

use crate::config::BcsConfig;
use crate::error::{ApiErrorResponse, AuthErrorResponse, BcsApiError};
use crate::http::models::{TokenRequest, TokenResponse};

/// Path of the token endpoint on the REST host (`POST`, form-encoded).
///
/// The client calls it to trade the refresh token for an access token.
pub const AUTH_TOKEN_PATH: &str =
    "/trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token";
/// Production base URL of the REST API. [`BcsConfig::new`] and [`BcsConfig::from_env`] use it.
pub const HTTP_BASE_URL: &str = "https://be.broker.ru";
/// Production base URL of the WebSocket API. [`BcsConfig::new`] and [`BcsConfig::from_env`] use it.
pub const WS_BASE_URL: &str = "wss://ws.broker.ru";
/// A request timeout of 30 seconds.
///
/// The client does not read this constant. The timeout of a client is
/// [`BcsConfig::request_timeout`], whose default has the same value.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The access token and the moment after which the client does not trust it any more.
#[derive(Clone)]
struct CachedToken {
    token: String,
    /// 90% of `expires_in` after the token was issued.
    valid_until: Option<tokio::time::Instant>,
}

/// The authorized HTTP client of the BCS Trade API.
///
/// It holds the refresh token and the current access token, adds `Authorization: Bearer ...` to every
/// call, and handles the token life cycle for you:
///
/// * The first call trades the refresh token for an access token. The access token is cached.
/// * The client tracks the expiry time: a token past 90% of its `expires_in` is renewed before the next
///   call.
/// * When the broker answers 401 to a **read**, the client gets a new access token and repeats the
///   **same request once**. If the second answer is 401 again, you get the error of that answer.
/// * A **command** (placing, changing or cancelling an order) is never repeated, not even after a 401:
///   the client renews the token for the next call and returns the 401 as an error, see
///   [`BcsHttpClient::post_command`].
/// * A transport error or a timeout is never repeated. This matters for commands: see
///   [`BcsApiError::HttpError`].
/// * If the token service returns a new refresh token (rotation), the client keeps it in memory. See
///   [`BcsHttpClient::current_refresh_token`].
///
/// The client is cheap to clone. Clones share the tokens and the connection pool. It can be used from
/// many tasks at once.
///
/// The endpoint methods (one per REST call) are documented on this type too, they are in
/// [`super::endpoints`]. The broker limits calls per account (about 10 per second for most services,
/// 3 per second for non-trade operations, per the broker docs). The client does not throttle, see
/// [`crate::utils::rate_limiter::RateLimiter`].
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// use brokapi_bcs_protocol::http::client::BcsHttpClient;
/// use brokapi_bcs_protocol::http::models::OrderBookRequest;
///
/// let client = BcsHttpClient::new(cfg)?;
/// let book = client
///     .get_order_book(&OrderBookRequest { ticker: "SBER".into(), class_code: "TQBR".into(), depth: 5 })
///     .await?;
/// println!("{} levels of bids", book.bids.len());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BcsHttpClient {
    client: Client,
    access_token: Arc<RwLock<Option<CachedToken>>>,
    refresh_token: Arc<RwLock<String>>,
    refresh_mutex: Arc<Mutex<()>>,
    client_id: String,
    http_base_url: Arc<str>,
    ws_base_url: Arc<str>,
}

impl BcsHttpClient {
    /// Creates a client from a configuration. No network call is made.
    ///
    /// The HTTP connection pool is created with the timeout of [`BcsConfig::request_timeout`] and
    /// gzip enabled. The `client_id` of later token requests is `trade-api-write` when
    /// [`BcsConfig::is_write`] is set, otherwise `trade-api-read`.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::HttpError`] (no status) if the HTTP stack cannot be built, for example when the
    /// TLS backend fails to initialise.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(BcsConfig::new("refresh-token".to_string()))?;
    /// assert_eq!(client.base_url(), "https://be.broker.ru");
    /// # Ok::<(), brokapi_bcs_protocol::error::BcsApiError>(())
    /// ```
    pub fn new(config: BcsConfig) -> Result<Self, BcsApiError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .gzip(true)
            .build()?;

        let refresh_token = config.refresh_token.clone();
        let client_id = if config.is_write {
            "trade-api-write".to_string()
        } else {
            "trade-api-read".to_string()
        };
        Ok(Self {
            client,
            access_token: Arc::new(RwLock::new(None)),
            refresh_token: Arc::new(RwLock::new(refresh_token)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client_id,
            http_base_url: Arc::from(config.http_base_url.as_str()),
            ws_base_url: Arc::from(config.ws_base_url.as_str()),
        })
    }

    /// Returns the cached access token, or gets a new one if there is none yet.
    ///
    /// A token that is past 90% of its `expires_in` is renewed here, before it is used, so a command does
    /// not meet a 401 after a day of work. A 401 on a read is still repaired by one refresh and one
    /// repeat. The WebSocket session calls this at every (re)connect, and forces a refresh itself when
    /// the broker refuses the connection.
    ///
    /// # Errors
    ///
    /// As [`BcsHttpClient::refresh_access_token`], and only when there is no cached token or it is
    /// (nearly) expired.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let first = client.get_access_token().await?;
    /// let second = client.get_access_token().await?; // from the cache, no second token request
    /// assert_eq!(first, second);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_access_token(&self) -> Result<String, BcsApiError> {
        {
            let cached = self.access_token.read().await;
            if let Some(c) = cached.as_ref()
                && c.valid_until
                    .is_none_or(|until| tokio::time::Instant::now() < until)
            {
                return Ok(c.token.clone());
            }
        }
        self.refresh_access_token().await
    }

    /// Trades the refresh token for a new access token and stores it. Always calls the token service.
    ///
    /// Sends `grant_type=refresh_token`, the refresh token and the `client_id` as a form to
    /// [`AUTH_TOKEN_PATH`]. On success the new access token is cached. If the reply carries a new
    /// refresh token it replaces the old one in memory.
    ///
    /// Refreshes are serialised by a lock, so two tasks never talk to the token service at the same
    /// moment. The lock does not merge them: each caller still makes its own request, one after the
    /// other.
    ///
    /// # Errors
    ///
    /// * [`BcsApiError::AuthError`]: the token service refused the refresh token. `message` is its
    ///   `error_description` (or `error`).
    /// * [`BcsApiError::HttpError`]: a transport failure, an unreadable success body, or an error
    ///   status whose body is not the token service's JSON error.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let token = client.refresh_access_token().await?;
    /// assert!(!token.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn refresh_access_token(&self) -> Result<String, BcsApiError> {
        let _guard = self.refresh_mutex.lock().await; // только один refresh одновременно!

        let refresh = {
            let rt = self.refresh_token.read().await;
            rt.clone()
        };

        let url = format!("{}{}", self.http_base_url, AUTH_TOKEN_PATH);
        let form = TokenRequest {
            grant_type: "refresh_token".to_string(),
            refresh_token: refresh,
            client_id: self.client_id.clone(),
        };

        debug!("Refreshing access + refresh token...");

        let resp = self.client.post(&url).form(&form).send().await?;

        if resp.status().is_success() {
            let token_resp: TokenResponse = resp.json().await?;
            let access = token_resp.access_token.clone();

            // Обновляем refresh_token если пришел новый (Keycloak rotation)
            if let Some(new_refresh) = token_resp.refresh_token {
                {
                    let mut rt = self.refresh_token.write().await;
                    *rt = new_refresh;
                }
                debug!("✅ New refresh_token received and saved from Keycloak");
            } else {
                debug!("Server did not return new refresh_token (keeping current)");
            }

            {
                let mut at = self.access_token.write().await;
                let lifetime = Duration::from_secs(token_resp.expires_in).mul_f64(0.9);
                *at = Some(CachedToken {
                    token: access.clone(),
                    valid_until: Some(tokio::time::Instant::now() + lifetime),
                });
            }

            debug!("Access token refreshed successfully");
            Ok(access)
        } else {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            error!("Token refresh failed: {} {}", status, body);

            if let Ok(err) = serde_json::from_str::<AuthErrorResponse>(&body) {
                Err(BcsApiError::AuthError {
                    message: err.error_description.unwrap_or(err.error),
                })
            } else {
                Err(BcsApiError::HttpError {
                    status: Some(status),
                    message: body,
                    trace_id: None,
                })
            }
        }
    }

    /// `GET`s a path and parses the JSON reply into `T`.
    ///
    /// `path` starts with `/` and is added to the base URL, for example
    /// `/trade-api-bff-portfolio/api/v1/portfolio`. Use it for an endpoint that has no typed method.
    /// The typed methods of [`super::endpoints`] call it.
    ///
    /// A 401 is answered with one token refresh and one repeat. Other failures are not retried.
    ///
    /// # Errors
    ///
    /// * [`BcsApiError::ValidationError`] and [`BcsApiError::NotFound`]: the broker said
    ///   `VALIDATION_ERROR` or `NOT_FOUND` in its JSON error body.
    /// * [`BcsApiError::RateLimitExceeded`]: HTTP 429 without the JSON error body.
    /// * [`BcsApiError::HttpError`]: any other non-success status, or a transport failure.
    /// * [`BcsApiError::ParseError`]: the reply is a success but is not the JSON that `T` expects.
    /// * [`BcsApiError::AuthError`]: the token refresh after a 401 failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::error::BcsApiError;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let portfolio: serde_json::Value = client.get("/trade-api-bff-portfolio/api/v1/portfolio").await?;
    /// assert!(portfolio.is_array());
    ///
    /// // A path the broker does not know ends in an HttpError that carries the status.
    /// let err = client.get::<serde_json::Value>("/no/such/path").await.unwrap_err();
    /// assert!(matches!(err, BcsApiError::HttpError { status: Some(404), .. }));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, BcsApiError> {
        self.request_with_retry(reqwest::Method::GET, path, None::<&()>)
            .await
    }

    /// `GET`s a path with query parameters and parses the JSON reply into `T`.
    ///
    /// `query` is anything `serde` can turn into a URL query: a struct, a map, or a slice of pairs.
    /// Retry and errors are the same as for [`BcsHttpClient::get`].
    ///
    /// # Errors
    ///
    /// The same as [`BcsHttpClient::get`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let book: serde_json::Value = client
    ///     .get_with_query(
    ///         "/trade-api-market-data-connector/api/v1/order-book",
    ///         &[("ticker", "SBER"), ("classCode", "TQBR"), ("depth", "5")],
    ///     )
    ///     .await?;
    /// assert_eq!(book["ticker"], "SBER");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_with_query<T, Q>(&self, path: &str, query: &Q) -> Result<T, BcsApiError>
    where
        T: serde::de::DeserializeOwned,
        Q: serde::Serialize,
    {
        let url = format!("{}{}", self.http_base_url, path);
        let token = self.get_access_token().await?;

        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .query(query)
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            warn!("Got 401, refreshing token and retrying...");
            let new_token = self.refresh_access_token().await?;
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&new_token)
                .query(query)
                .send()
                .await?;
            self.handle_response(resp).await
        } else {
            self.handle_response(resp).await
        }
    }

    /// `GET`s a path and returns the status and the body text, without parsing and without mapping
    /// errors.
    ///
    /// An error status is **not** an `Err` here: you get it as the first element of the pair. Use it
    /// to look at what the broker really sent. Only a 401 is handled (one refresh, one repeat).
    ///
    /// # Errors
    ///
    /// [`BcsApiError::HttpError`] (no status) on a transport failure, and the errors of
    /// [`BcsHttpClient::refresh_access_token`] when a 401 needs a new token.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let (status, body) = client
    ///     .get_raw("/trade-api-market-data-connector/api/v1/order-book?ticker=SBER&classCode=TQBR")
    ///     .await?;
    /// assert!(status.is_success());
    /// assert!(body.contains("bids"));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_raw(&self, path: &str) -> Result<(reqwest::StatusCode, String), BcsApiError> {
        let url = format!("{}{}", self.http_base_url, path);
        let token = self.get_access_token().await?;

        let resp = self.client.get(&url).bearer_auth(&token).send().await?;

        let status = resp.status();

        // 401 - обновляем токен и пробуем снова
        if status == reqwest::StatusCode::UNAUTHORIZED {
            warn!("Got 401, refreshing token and retrying...");
            let new_token = self.refresh_access_token().await?;
            let resp = self.client.get(&url).bearer_auth(&new_token).send().await?;

            let status = resp.status();
            let body = resp.text().await?;
            return Ok((status, body));
        }

        let body = resp.text().await?;
        Ok((status, body))
    }

    /// `POST`s a JSON body to a path and parses the JSON reply into `T`.
    ///
    /// A 401 is answered with one token refresh and **one repeat of the same body**. Use this for
    /// requests that only read (a `POST` that searches or asks for quotes). For a command use
    /// [`BcsHttpClient::post_command`], which never repeats. A transport error or a timeout is returned
    /// as it is and is never repeated.
    ///
    /// # Errors
    ///
    /// The same as [`BcsHttpClient::get`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let body = serde_json::json!({ "instruments": [{ "ticker": "SBER", "classCode": "TQBR" }] });
    /// let quotes: serde_json::Value = client
    ///     .post("/trade-api-market-data-connector/api/v1/quotes", &body)
    ///     .await?;
    /// assert!(quotes["records"].is_array());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn post<T, B>(&self, path: &str, body: &B) -> Result<T, BcsApiError>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        self.request_with_retry(reqwest::Method::POST, path, Some(body))
            .await
    }

    /// `POST`s a command (place, change or cancel an order) and parses the JSON reply into `T`. **Never
    /// repeats the request.**
    ///
    /// A repeat of a command is the one way to place an order twice, so the client sends it once. If the
    /// broker answers 401, the client renews the access token (so the next call works) and returns the
    /// 401 as [`BcsApiError::HttpError`] with `status: Some(401)`. A 401 means the broker did not act, so
    /// it is safe to send the command again with the same client order id, if you want to. A transport
    /// error or a timeout is an unknown outcome, as for every command.
    ///
    /// # Errors
    ///
    /// The same as [`BcsHttpClient::post`].
    pub async fn post_command<T, B>(&self, path: &str, body: &B) -> Result<T, BcsApiError>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let url = format!("{}{}", self.http_base_url, path);
        let token = self.get_access_token().await?;
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(body)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            warn!("Got 401 on a command: renewing the token, the command is not repeated");
            // The next call needs a fresh token. A failure of the renewal does not hide the 401.
            let _ = self.refresh_access_token().await;
        }
        self.handle_response(resp).await
    }

    /// One request with a single repeat after a 401.
    async fn request_with_retry<T, B>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T, BcsApiError>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let url = format!("{}{}", self.http_base_url, path);
        let token = self.get_access_token().await?;

        let mut req = self
            .client
            .request(method.clone(), &url)
            .bearer_auth(&token);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;

        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            warn!("Got 401, refreshing token and retrying...");
            let new_token = self.refresh_access_token().await?;
            let mut req = self.client.request(method, &url).bearer_auth(&new_token);
            if let Some(b) = body {
                req = req.json(b);
            }
            let resp = req.send().await?;
            self.handle_response(resp).await
        } else {
            self.handle_response(resp).await
        }
    }

    /// Turns a reply into `T` or into the matching [`BcsApiError`].
    async fn handle_response<T: serde::de::DeserializeOwned>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, BcsApiError> {
        let status = resp.status();

        if status.is_success() {
            let body = resp.text().await?;
            serde_json::from_str(&body).map_err(|e| {
                BcsApiError::ParseError(format!(
                    "Failed to parse response: {} | body: {}",
                    e,
                    truncate_utf8(&body, 200)
                ))
            })
        } else {
            let status_code = status.as_u16();
            let body = resp.text().await.unwrap_or_default();

            if let Ok(api_err) = serde_json::from_str::<ApiErrorResponse>(&body) {
                match api_err.error_type.as_str() {
                    "VALIDATION_ERROR" => Err(BcsApiError::ValidationError {
                        trace_id: api_err.trace_id,
                        errors: api_err.errors,
                    }),
                    "NOT_FOUND" => Err(BcsApiError::NotFound {
                        trace_id: api_err.trace_id,
                    }),
                    _ => Err(BcsApiError::HttpError {
                        status: Some(status_code),
                        message: api_err.error_type,
                        trace_id: api_err.trace_id,
                    }),
                }
            } else if status_code == 429 {
                Err(BcsApiError::RateLimitExceeded)
            } else {
                Err(BcsApiError::HttpError {
                    status: Some(status_code),
                    message: body,
                    trace_id: None,
                })
            }
        }
    }

    /// The underlying `reqwest` client, for calls this crate does not wrap.
    ///
    /// It has the timeout and gzip setting of the configuration but carries **no** authorization.
    /// Add the token yourself with [`BcsHttpClient::get_access_token`].
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(BcsConfig::new("t".to_string()))?;
    /// let _http: &reqwest::Client = client.inner_client();
    /// # Ok::<(), brokapi_bcs_protocol::error::BcsApiError>(())
    /// ```
    pub fn inner_client(&self) -> &Client {
        &self.client
    }

    /// The refresh token the client holds now.
    ///
    /// It is the one from the configuration until the token service returns a rotated one, then it is
    /// the new one. Persist it yourself if you want a restart to use the rotated token (the
    /// configuration is not updated). Treat it as a secret.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use brokapi_bcs_protocol::config::BcsConfig;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(BcsConfig::new("my-refresh-token".to_string()))?;
    /// assert_eq!(client.current_refresh_token().await, "my-refresh-token");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn current_refresh_token(&self) -> String {
        let rt = self.refresh_token.read().await;
        rt.clone()
    }

    /// The REST base URL this client calls, without a trailing slash.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(BcsConfig::for_mock("t", "http://127.0.0.1:1"))?;
    /// assert_eq!(client.base_url(), "http://127.0.0.1:1");
    /// # Ok::<(), brokapi_bcs_protocol::error::BcsApiError>(())
    /// ```
    pub fn base_url(&self) -> &str {
        &self.http_base_url
    }

    /// The WebSocket base URL that sessions started from this client connect to.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(BcsConfig::for_mock("t", "http://127.0.0.1:1"))?;
    /// assert_eq!(client.ws_base_url(), "ws://127.0.0.1:1");
    /// # Ok::<(), brokapi_bcs_protocol::error::BcsApiError>(())
    /// ```
    pub fn ws_base_url(&self) -> &str {
        &self.ws_base_url
    }
}

/// The first `max` bytes of `s`, cut back to a character boundary so the cut never splits a character.
fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::truncate_utf8;

    #[test]
    fn truncation_never_splits_a_character() {
        // 150 two-byte characters: byte 200 falls inside a character only if the cut is naive.
        let s = "ж".repeat(150);
        let cut = truncate_utf8(&s, 199);
        assert_eq!(cut.len(), 198);
        assert!(cut.chars().all(|c| c == 'ж'));
        assert_eq!(truncate_utf8("short", 200), "short");
    }
}
