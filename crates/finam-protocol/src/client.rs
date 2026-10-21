//! Channel setup and authorized service clients.
//!
//! [`FinamApi`] is the entry point: one lazily connecting channel plus one shared
//! [`TokenManager`]. Every service client it hands out puts the current JWT
//! into the `authorization` header of each call, so a client never holds an expired token.
//!
//! # Examples
//!
//! ```
//! use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata::QuoteRequest;
//! use brokapi_finam_protocol::{ApiEndpoint, FinamApi, start_token_manager};
//! # use brokapi_mocks::finam::{MockServer, SECRET};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start().await;
//! # let endpoint = ApiEndpoint::new(server.url.clone());
//! # let secret = SECRET;
//! // `endpoint` is `ApiEndpoint::default()` for the real broker, `secret` is your API secret.
//! let tokens = start_token_manager(&endpoint, secret, "my-app").await?;
//! let api = FinamApi::connect(&endpoint, tokens, false)?;
//!
//! let quote = api
//!     .market_data()
//!     .last_quote(QuoteRequest { symbol: "SBER@MISX".into() })
//!     .await?
//!     .into_inner();
//! assert_eq!(quote.symbol, "SBER@MISX");
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::auth::{GrpcTokenApi, TokenManager};
use crate::error::FinamError;
use crate::pb::grpc::tradeapi::v1::accounts::accounts_service_client::AccountsServiceClient;
use crate::pb::grpc::tradeapi::v1::assets::assets_service_client::AssetsServiceClient;
use crate::pb::grpc::tradeapi::v1::auth::auth_service_client::AuthServiceClient;
use crate::pb::grpc::tradeapi::v1::corporateactions::corporate_actions_service_client::CorporateActionsServiceClient;
use crate::pb::grpc::tradeapi::v1::marketdata::market_data_service_client::MarketDataServiceClient;
use crate::pb::grpc::tradeapi::v1::metrics::usage_metrics_service_client::UsageMetricsServiceClient;
use crate::pb::grpc::tradeapi::v1::orders::orders_service_client::OrdersServiceClient;
use crate::pb::grpc::tradeapi::v1::reports::reports_service_client::ReportsServiceClient;

/// Address of the production Trade API (`https://api.finam.ru:443`, TLS).
pub const DEFAULT_ENDPOINT: &str = "https://api.finam.ru:443";

/// Where and how to connect: the address and the timeouts of the channel.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_finam_protocol::{ApiEndpoint, DEFAULT_ENDPOINT};
///
/// // Production: the default.
/// assert_eq!(ApiEndpoint::default().url, DEFAULT_ENDPOINT);
///
/// // A proxy or a local imitation: plain `http://`, a shorter connect timeout.
/// let mut ep = ApiEndpoint::new("http://127.0.0.1:50051");
/// ep.connect_timeout = Duration::from_secs(2);
/// assert_eq!(ep.keepalive_interval, Duration::from_secs(30));
/// ```
#[derive(Debug, Clone)]
pub struct ApiEndpoint {
    /// `https://host:port` (TLS, webpki roots) or `http://host:port` (plaintext, tests/proxies).
    pub url: String,
    /// How long to wait for the TCP and TLS handshake. Default 10 seconds.
    pub connect_timeout: Duration,
    /// HTTP/2 PING interval, detects dead connections under long-lived streams. Default 30 seconds.
    ///
    /// Pings are also sent while the channel is idle.
    pub keepalive_interval: Duration,
    /// How long to wait for the answer to a PING before the connection is declared dead.
    /// Default 10 seconds.
    pub keepalive_timeout: Duration,
}

impl ApiEndpoint {
    /// Endpoint for `url` with the default timeouts (connect 10 s, keepalive 30 s and 10 s).
    ///
    /// The URL is not checked here. A bad URL is reported by [`connect_lazy`] as
    /// [`FinamError::Config`].
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connect_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
        }
    }
}

impl Default for ApiEndpoint {
    fn default() -> Self {
        Self::new(DEFAULT_ENDPOINT)
    }
}

/// Builds a lazily connecting channel to `ep`.
///
/// Nothing is sent here. Connection errors surface on the first call, and tonic reconnects on its
/// own after a drop. TLS (webpki roots) is switched on when the URL starts with `https://`.
///
/// # Errors
///
/// [`FinamError::Config`] when the URL is not a valid endpoint or the TLS settings are refused.
///
/// # Examples
///
/// ```
/// use brokapi_finam_protocol::client::connect_lazy;
/// use brokapi_finam_protocol::{ApiEndpoint, FinamError};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // A valid address gives a channel without touching the network.
/// assert!(connect_lazy(&ApiEndpoint::new("http://127.0.0.1:1")).is_ok());
/// // Garbage is a configuration error.
/// let err = connect_lazy(&ApiEndpoint::new("not a url")).unwrap_err();
/// assert!(matches!(err, FinamError::Config(_)));
/// # }
/// ```
pub fn connect_lazy(ep: &ApiEndpoint) -> Result<Channel, FinamError> {
    let mut endpoint = Endpoint::from_shared(ep.url.clone())
        .map_err(|e| FinamError::Config(format!("bad endpoint {:?}: {e}", ep.url)))?
        .connect_timeout(ep.connect_timeout)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(ep.keepalive_interval)
        .keep_alive_timeout(ep.keepalive_timeout)
        .keep_alive_while_idle(true);
    if ep.url.starts_with("https://") {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_webpki_roots())
            .map_err(|e| FinamError::Config(format!("tls: {e}")))?;
    }
    Ok(endpoint.connect_lazy())
}

/// Starts a [`TokenManager`] for `secret` on a fresh channel to `ep`.
///
/// Logs in once (`AuthService.Auth`) and starts the background refresher. Put the returned `Arc` into
/// every [`FinamApi`] that uses this secret: one secret needs one manager.
///
/// `source_app_id` is sent with `Auth` as the application id. An empty string is allowed.
///
/// # Errors
///
/// - [`FinamError::Config`]: the secret is empty or the endpoint URL is invalid.
/// - [`FinamError::Auth`]: the server returned an empty token.
/// - [`FinamError::Status`]: the server refused the secret (code `UNAUTHENTICATED`) or could not
///   be reached.
///
/// # Examples
///
/// ```
/// use brokapi_finam_protocol::{ApiEndpoint, FinamError, start_token_manager};
/// # use brokapi_mocks::finam::{MockServer, SECRET};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start().await;
/// # let endpoint = ApiEndpoint::new(server.url.clone());
/// # let secret = SECRET;
/// let tokens = start_token_manager(&endpoint, secret, "my-app").await?;
/// assert!(!tokens.current().is_empty());
///
/// // A wrong secret is refused by the server.
/// let err = start_token_manager(&endpoint, "wrong-secret", "my-app").await.unwrap_err();
/// assert!(err.is_unauthenticated());
/// // An empty secret never leaves the process.
/// let err = start_token_manager(&endpoint, " ", "my-app").await.unwrap_err();
/// assert!(matches!(err, FinamError::Config(_)));
/// # Ok(())
/// # }
/// ```
pub async fn start_token_manager(
    ep: &ApiEndpoint,
    secret: &str,
    source_app_id: &str,
) -> Result<Arc<TokenManager>, FinamError> {
    let channel = connect_lazy(ep)?;
    TokenManager::start(Arc::new(GrpcTokenApi::new(channel)), secret, source_app_id).await
}

/// Tonic interceptor that puts the current JWT into the `authorization` metadata of every call.
///
/// Built inside [`FinamApi`], not by hand. It asks the [`TokenManager`] for the token on each call,
/// so a renewed token is used at once. A token that cannot be written as header text fails the
/// call with status `UNAUTHENTICATED` before anything is sent.
#[derive(Clone)]
pub struct AuthInterceptor {
    tokens: Arc<TokenManager>,
    bearer: bool,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let token = self.tokens.current();
        // Official SDKs send the bare JWT; some doc examples show `Bearer <jwt>`.
        let value = if self.bearer {
            format!("Bearer {token}")
        } else {
            token.to_string()
        };
        let mut value = MetadataValue::try_from(value.as_str())
            .map_err(|_| Status::unauthenticated("token is not valid metadata"))?;
        // Sensitive: the JWT does not show in the Debug output of the request or in HTTP/2 traces.
        value.set_sensitive(true);
        req.metadata_mut().insert("authorization", value);
        Ok(req)
    }
}

/// Channel with the [`AuthInterceptor`] on it: the transport type of every client of [`FinamApi`].
pub type Authorized = InterceptedService<Channel, AuthInterceptor>;

/// One channel plus a shared token manager. Hands out service clients that attach the JWT to each call.
///
/// Cheap to clone (the channel is reference counted). Each accessor builds a new client over the same
/// channel, so there is no need to keep clients around.
///
/// A streaming call keeps the JWT it was opened with. Open it again after the token is renewed
/// (the connectors do this for their own streams).
///
/// See the [module docs](self) for a complete example.
#[derive(Clone)]
pub struct FinamApi {
    channel: Channel,
    tokens: Arc<TokenManager>,
    bearer: bool,
}

impl FinamApi {
    /// Opens a lazy channel to `ep` and uses `tokens` for authorization.
    ///
    /// `bearer` selects the header format. `false` sends the bare JWT, which is what the official
    /// SDKs do. `true` sends `Bearer <jwt>`, which some examples in the broker documentation show.
    /// Which of the two the live broker accepts is not verified against the live API here.
    ///
    /// # Errors
    ///
    /// [`FinamError::Config`] when the endpoint URL is invalid (see [`connect_lazy`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_finam_protocol::{ApiEndpoint, FinamApi, FinamError, start_token_manager};
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let endpoint = ApiEndpoint::new(server.url.clone());
    /// # let tokens = start_token_manager(&endpoint, SECRET, "").await?;
    /// let api = FinamApi::connect(&endpoint, tokens.clone(), false)?;
    /// assert!(std::sync::Arc::ptr_eq(api.tokens(), &tokens));
    ///
    /// let bad = FinamApi::connect(&ApiEndpoint::new("not a url"), tokens, false);
    /// assert!(matches!(bad, Err(FinamError::Config(_))));
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect(
        ep: &ApiEndpoint,
        tokens: Arc<TokenManager>,
        bearer: bool,
    ) -> Result<Self, FinamError> {
        Ok(Self {
            channel: connect_lazy(ep)?,
            tokens,
            bearer,
        })
    }

    /// Wraps a channel you built yourself (for example with custom TLS or a proxy).
    ///
    /// Nothing is checked. `bearer` has the same meaning as in [`FinamApi::connect`].
    pub fn from_channel(channel: Channel, tokens: Arc<TokenManager>, bearer: bool) -> Self {
        Self {
            channel,
            tokens,
            bearer,
        }
    }

    /// The token manager used by this client.
    ///
    /// Use it to read the account list ([`TokenManager::account_ids`]) or to ask for a renewal.
    pub fn tokens(&self) -> &Arc<TokenManager> {
        &self.tokens
    }

    fn svc(&self) -> Authorized {
        InterceptedService::new(
            self.channel.clone(),
            AuthInterceptor {
                tokens: Arc::clone(&self.tokens),
                bearer: self.bearer,
            },
        )
    }

    /// A generic gRPC client over the same channel and token, for a call that is not made through a
    /// generated client: the method is named by a path (`/package.Service/Method`) and the messages are
    /// whatever codec you give it. With [`FILE_DESCRIPTOR_SET`](crate::FILE_DESCRIPTOR_SET) it calls any
    /// method of the API by name.
    pub fn grpc(&self) -> tonic::client::Grpc<Authorized> {
        tonic::client::Grpc::new(self.svc())
    }

    /// `OrdersService`: place, cancel and read orders, order and trade streams.
    pub fn orders(&self) -> OrdersServiceClient<Authorized> {
        OrdersServiceClient::new(self.svc())
    }

    /// `AccountsService`: account state, own trades, account stream.
    pub fn accounts(&self) -> AccountsServiceClient<Authorized> {
        AccountsServiceClient::new(self.svc())
    }

    /// `MarketDataService`: quotes, order books, trades, bars and their streams.
    pub fn market_data(&self) -> MarketDataServiceClient<Authorized> {
        MarketDataServiceClient::new(self.svc())
    }

    /// `AssetsService`: instrument directory, schedules, exchange clock.
    pub fn assets(&self) -> AssetsServiceClient<Authorized> {
        AssetsServiceClient::new(self.svc())
    }

    /// `UsageMetricsService`: how much of the request quota is left.
    pub fn usage_metrics(&self) -> UsageMetricsServiceClient<Authorized> {
        UsageMetricsServiceClient::new(self.svc())
    }

    /// `AuthService` with the current JWT attached. `Auth` and `SubscribeJwtRenewal` are driven by the
    /// [`TokenManager`]; `TokenDetails` is reachable here.
    pub fn auth(&self) -> AuthServiceClient<Authorized> {
        AuthServiceClient::new(self.svc())
    }

    /// `ReportsService`: account reports.
    pub fn reports(&self) -> ReportsServiceClient<Authorized> {
        ReportsServiceClient::new(self.svc())
    }

    /// `CorporateActionsService`: splits, dividends and bond events.
    pub fn corporate_actions(&self) -> CorporateActionsServiceClient<Authorized> {
        CorporateActionsServiceClient::new(self.svc())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::auth::{TokenApi, TokenInfo};
    use crate::error::FinamError;

    struct Fixed;

    #[async_trait]
    impl TokenApi for Fixed {
        async fn auth(&self, _s: &str, _a: &str) -> Result<String, FinamError> {
            Ok("the-jwt".into())
        }
        async fn token_details(&self, _t: &str) -> Result<TokenInfo, FinamError> {
            Ok(TokenInfo::default())
        }
    }

    #[tokio::test]
    async fn the_jwt_header_is_sensitive_and_is_sent_bare_or_with_bearer() {
        let tokens = crate::auth::TokenManager::start(Arc::new(Fixed), "secret", "")
            .await
            .unwrap();
        for (bearer, want) in [(false, "the-jwt"), (true, "Bearer the-jwt")] {
            let mut interceptor = AuthInterceptor {
                tokens: Arc::clone(&tokens),
                bearer,
            };
            let req = interceptor.call(Request::new(())).unwrap();
            let value = req.metadata().get("authorization").unwrap();
            assert_eq!(value, want);
            // The value does not show in `Debug` output.
            assert!(value.is_sensitive());
            assert!(!format!("{req:?}").contains("the-jwt"), "{req:?}");
        }
    }
}
