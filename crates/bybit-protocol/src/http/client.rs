//! The authorized HTTP client: signing, the exchange clock offset and the mapping of error replies.
//!
//! The generic methods are here ([`BybitHttpClient::get_public`], [`BybitHttpClient::get_private`],
//! [`BybitHttpClient::post_command`]). The endpoint methods (`get_order_book`, `create_order`, ...) are in
//! [`super::endpoints`].

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::{Client, Method, StatusCode};
use serde::de::DeserializeOwned;
use tracing::debug;

use super::models::Envelope;
use crate::config::BybitConfig;
use crate::error::{BybitApiError, is_auth_code};
use crate::sign::{rest_payload, sign};

struct Inner {
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
    /// The exchange clock minus the local clock, milliseconds.
    offset_ms: AtomicI64,
}

/// The HTTP client of the Bybit API V5.
///
/// It signs every private request with the key pair of the config, and keeps the offset between the clock of the
/// exchange and the local clock ([`sync_time`](BybitHttpClient::sync_time)), so the timestamp is always inside the
/// receive window.
///
/// * A **read** is sent once. The client does not retry: the caller decides.
/// * A **command** (create, amend, cancel) is sent once too, and never repeated: a repeat is the one way to place an
///   order twice. A lost answer is an unknown outcome, as for every command.
///
/// The client is cheap to clone. The limit of the exchange (per second and per endpoint) is not enforced here: the
/// connector throttles through the shared limiter.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bybit_protocol::config::BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
/// use brokapi_bybit_protocol::http::client::BybitHttpClient;
///
/// let client = BybitHttpClient::new(cfg)?;
/// let offset = client.sync_time().await?;
/// assert!(offset.abs() < 5_000, "the clock of the imitation is the local one");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BybitHttpClient {
    client: Client,
    inner: Arc<Inner>,
    http_base_url: Arc<str>,
    config: Arc<BybitConfig>,
}

fn local_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl BybitHttpClient {
    /// Creates a client from a configuration. No network call is made.
    ///
    /// # Errors
    ///
    /// [`BybitApiError::Http`] (no status) if the HTTP stack cannot be built.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bybit_protocol::config::BybitConfig;
    /// use brokapi_bybit_protocol::http::client::BybitHttpClient;
    ///
    /// let client = BybitHttpClient::new(BybitConfig::new("key", "secret"))?;
    /// assert_eq!(client.base_url(), "https://api.bybit.com");
    /// # Ok::<(), brokapi_bybit_protocol::error::BybitApiError>(())
    /// ```
    pub fn new(config: BybitConfig) -> Result<Self, BybitApiError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .gzip(true)
            .build()?;
        Ok(Self {
            client,
            inner: Arc::new(Inner {
                api_key: config.api_key.clone(),
                api_secret: config.api_secret.clone(),
                recv_window_ms: config.recv_window_ms,
                offset_ms: AtomicI64::new(0),
            }),
            http_base_url: Arc::from(config.http_base_url.trim_end_matches('/')),
            config: Arc::new(config),
        })
    }

    /// The base URL requests go to.
    pub fn base_url(&self) -> &str {
        &self.http_base_url
    }

    /// The configuration the client was made from.
    pub fn config(&self) -> &BybitConfig {
        &self.config
    }

    /// The timestamp to put in a request: the local clock moved by the offset of the exchange clock.
    pub fn timestamp_ms(&self) -> i64 {
        local_ms() + self.inner.offset_ms.load(Ordering::Relaxed)
    }

    /// Reads the clock of the exchange and keeps its offset from the local clock. Returns the offset in
    /// milliseconds (positive: the exchange is ahead).
    ///
    /// The offset is taken at the middle of the round trip. A request is signed with the corrected time from then on.
    ///
    /// # Errors
    ///
    /// As [`BybitHttpClient::get_public`].
    pub async fn sync_time(&self) -> Result<i64, BybitApiError> {
        let before = local_ms();
        let t = self.get_server_time().await?;
        let after = local_ms();
        let exchange_ms = t.time_nano / 1_000_000;
        let offset = exchange_ms - (before + after) / 2;
        self.inner.offset_ms.store(offset, Ordering::Relaxed);
        debug!("the exchange clock is {offset} ms from the local one");
        Ok(offset)
    }

    /// `GET`s a public path (no key) and returns the `result` of the reply parsed as `T`.
    ///
    /// # Errors
    ///
    /// * [`BybitApiError::Api`]: the exchange refused the request (`retCode` is not zero).
    /// * [`BybitApiError::RateLimitExceeded`]: HTTP 429 or `retCode` 10006.
    /// * [`BybitApiError::Http`]: another status, a transport failure, or a timeout.
    /// * [`BybitApiError::Parse`]: the reply is a success but not the JSON that `T` expects.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bybit_protocol::config::BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
    /// use brokapi_bybit_protocol::error::BybitApiError;
    /// use brokapi_bybit_protocol::http::client::BybitHttpClient;
    ///
    /// let client = BybitHttpClient::new(cfg)?;
    /// let time: serde_json::Value = client.get_public("/v5/market/time", &[]).await?;
    /// assert!(time["timeSecond"].is_string());
    ///
    /// // A refusal of the exchange keeps its code.
    /// let err = client
    ///     .get_public::<serde_json::Value>("/v5/market/orderbook", &[("category", "linear".into()), ("symbol", "NOPEUSDT".into())])
    ///     .await
    ///     .unwrap_err();
    /// assert!(matches!(err, BybitApiError::Api { code: 10001, .. }));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_public<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, BybitApiError> {
        let url = self.url(path, &encode(query));
        let resp = self.client.get(url).send().await?;
        self.handle(resp).await
    }

    /// `GET`s a private path: signed with the key. Same errors as [`BybitHttpClient::get_public`], and
    /// [`BybitApiError::Auth`] when the key is refused.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bybit_protocol::config::BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
    /// use brokapi_bybit_protocol::http::client::BybitHttpClient;
    ///
    /// let client = BybitHttpClient::new(cfg)?;
    /// let wallet: serde_json::Value = client
    ///     .get_private("/v5/account/wallet-balance", &[("accountType", "UNIFIED".into())])
    ///     .await?;
    /// assert!(wallet["list"].is_array());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_private<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, BybitApiError> {
        let qs = encode(query);
        let req = self.client.get(self.url(path, &qs));
        let resp = self.sign_request(req, &qs).send().await?;
        self.handle(resp).await
    }

    /// `POST`s a command (create, amend, cancel an order) with a JSON body. **Never repeats the request.**
    ///
    /// The body is signed as the exact text that is sent. A transport error or a timeout is an unknown outcome: the
    /// caller must find out with a read.
    ///
    /// # Errors
    ///
    /// The same as [`BybitHttpClient::get_private`].
    pub async fn post_command<T, B>(&self, path: &str, body: &B) -> Result<T, BybitApiError>
    where
        T: DeserializeOwned,
        B: serde::Serialize,
    {
        let text = serde_json::to_string(body)?;
        let req = self
            .client
            .request(Method::POST, self.url(path, ""))
            .header("Content-Type", "application/json");
        let resp = self.sign_request(req, &text).body(text).send().await?;
        self.handle(resp).await
    }

    /// `GET`s a private path and returns the status and the body text, without parsing and without mapping errors.
    /// An error status is not an `Err` here. Only a transport failure is.
    ///
    /// # Errors
    ///
    /// [`BybitApiError::Http`] (no status) on a transport failure.
    pub async fn read_raw(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&serde_json::Value>,
        signed: bool,
    ) -> Result<(StatusCode, String), BybitApiError> {
        let (qs, text) = if method == Method::GET {
            (encode(query), String::new())
        } else {
            (
                String::new(),
                body.map(|b| b.to_string()).unwrap_or_else(|| "{}".into()),
            )
        };
        let mut req = self.client.request(method.clone(), self.url(path, &qs));
        if method != Method::GET {
            req = req.header("Content-Type", "application/json");
        }
        let signed_payload = if method == Method::GET { &qs } else { &text };
        if signed {
            req = self.sign_request(req, signed_payload);
        }
        if method != Method::GET {
            req = req.body(text);
        }
        let resp = req.send().await?;
        let status = resp.status();
        Ok((status, resp.text().await?))
    }

    fn url(&self, path: &str, query: &str) -> String {
        if query.is_empty() {
            format!("{}{}", self.http_base_url, path)
        } else {
            format!("{}{}?{}", self.http_base_url, path, query)
        }
    }

    fn sign_request(&self, req: reqwest::RequestBuilder, payload: &str) -> reqwest::RequestBuilder {
        let ts = self.timestamp_ms();
        let signature = sign(
            &self.inner.api_secret,
            &rest_payload(ts, &self.inner.api_key, self.inner.recv_window_ms, payload),
        );
        req.header("X-BAPI-API-KEY", &self.inner.api_key)
            .header("X-BAPI-TIMESTAMP", ts.to_string())
            .header("X-BAPI-RECV-WINDOW", self.inner.recv_window_ms.to_string())
            .header("X-BAPI-SIGN", signature)
    }

    async fn handle<T: DeserializeOwned>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, BybitApiError> {
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(match status.as_u16() {
                429 => BybitApiError::RateLimitExceeded,
                401 => BybitApiError::Auth {
                    message: "HTTP 401: the key, the signature or the contour (main, demo, test) is wrong".into(),
                },
                s => BybitApiError::Http {
                    status: Some(s),
                    message: error_text(&text),
                },
            });
        }
        let env: Envelope<T> = serde_json::from_str(&text).map_err(|e| {
            BybitApiError::Parse(format!("the reply is not an answer of the API: {e}"))
        })?;
        match env.ret_code {
            0 => env
                .result
                .ok_or_else(|| BybitApiError::Parse("the reply has no result".into())),
            10006 => Err(BybitApiError::RateLimitExceeded),
            code if is_auth_code(code) => Err(BybitApiError::Auth {
                message: format!("{code}: {}", env.ret_msg),
            }),
            code => Err(BybitApiError::Api {
                code,
                message: env.ret_msg,
            }),
        }
    }
}

/// The query as the text that is sent and signed.
///
/// The value of `cursor` is written as it came in the previous answer: the exchange hands the token out already
/// percent-encoded, and encoding it a second time would change it.
fn encode(query: &[(&str, String)]) -> String {
    query
        .iter()
        .map(|(k, v)| {
            if *k == "cursor" {
                format!("cursor={v}")
            } else {
                serde_urlencoded::to_string([(k, v)]).unwrap_or_default()
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn error_text(body: &str) -> String {
    let body = body.trim();
    if body.is_empty() {
        return "empty body".to_string();
    }
    body.chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_query_is_one_text_in_the_order_given() {
        let q = [
            ("category", "linear".to_string()),
            ("symbol", "BTC USDT".to_string()),
        ];
        assert_eq!(encode(&q), "category=linear&symbol=BTC+USDT");
        assert_eq!(encode(&[]), "");
    }

    #[test]
    fn the_timestamp_follows_the_offset() {
        let c = BybitHttpClient::new(BybitConfig::new("k", "s")).unwrap();
        let a = c.timestamp_ms();
        c.inner.offset_ms.store(60_000, Ordering::Relaxed);
        assert!(c.timestamp_ms() >= a + 59_000);
    }
}
