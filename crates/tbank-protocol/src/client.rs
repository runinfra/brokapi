//! gRPC channel with Bearer-token authorization.

use std::time::Duration;

use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::config::TbankConfig;
use crate::error::ProtocolError;
use crate::v1::{
    instruments_service_client::InstrumentsServiceClient,
    market_data_service_client::MarketDataServiceClient,
    market_data_stream_service_client::MarketDataStreamServiceClient,
    operations_service_client::OperationsServiceClient,
    operations_stream_service_client::OperationsStreamServiceClient,
    orders_service_client::OrdersServiceClient,
    orders_stream_service_client::OrdersStreamServiceClient,
    sandbox_service_client::SandboxServiceClient, signal_service_client::SignalServiceClient,
    stop_orders_service_client::StopOrdersServiceClient, users_service_client::UsersServiceClient,
};

/// How often an HTTP/2 PING goes out while a call or a stream is open.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(300);

/// Adds `authorization: Bearer <token>` (and `x-app-name`) to every request.
///
/// The authorization value is marked sensitive, so tonic does not print it in debug output. One
/// interceptor serves one token. [`TbankClient`] builds it from the config, build it yourself only
/// when you wrap your own channel.
///
/// # Examples
///
/// ```
/// use brokapi_tbank_protocol::AuthInterceptor;
/// use tonic::service::Interceptor;
///
/// let mut auth = AuthInterceptor::new("t.my-token", Some("me.my-robot"))?;
/// let request = auth.call(tonic::Request::new(())).expect("the interceptor never refuses");
/// assert_eq!(request.metadata().get("authorization").unwrap(), "Bearer t.my-token");
/// assert_eq!(request.metadata().get("x-app-name").unwrap(), "me.my-robot");
///
/// // A token a header cannot carry is a configuration error, not a panic.
/// assert!(AuthInterceptor::new("line\nbreak", None).is_err());
/// # Ok::<(), brokapi_tbank_protocol::ProtocolError>(())
/// ```
#[derive(Clone)]
pub struct AuthInterceptor {
    auth: MetadataValue<Ascii>,
    app_name: Option<MetadataValue<Ascii>>,
}

impl AuthInterceptor {
    /// Prepares the headers for `token` and the optional `app_name`.
    ///
    /// `app_name` becomes the `x-app-name` header (the T-Bank convention is `<github nick>.<repo>`).
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Config`] when the token or the app name contains characters a header value
    /// cannot carry (control characters, non-ASCII). The message does not repeat the token.
    pub fn new(token: &str, app_name: Option<&str>) -> Result<Self, ProtocolError> {
        let mut auth: MetadataValue<Ascii> = format!("Bearer {token}").parse().map_err(|_| {
            ProtocolError::Config("token contains characters not allowed in a header".into())
        })?;
        auth.set_sensitive(true);
        let app_name = app_name
            .map(|n| {
                n.parse::<MetadataValue<Ascii>>().map_err(|_| {
                    ProtocolError::Config("app_name is not a valid header value".into())
                })
            })
            .transpose()?;
        Ok(Self { auth, app_name })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let md = req.metadata_mut();
        md.insert("authorization", self.auth.clone());
        if let Some(name) = &self.app_name {
            md.insert("x-app-name", name.clone());
        }
        Ok(req)
    }
}

/// Channel type every generated client is parameterised with.
pub type Authed = InterceptedService<Channel, AuthInterceptor>;

/// Shared HTTP/2 channel. Cheap to clone; each accessor returns a fresh generated client.
///
/// All generated clients made from one `TbankClient` share one connection and one token. The
/// channel connects lazily and reconnects on failure, so [`TbankClient::new`] never blocks and never
/// fails because the server is down.
///
/// This type does not limit the request rate and does not apply `request_timeout`. The connectors in
/// `brokapi-tbank` do both. If you call the generated clients directly, you must keep to the broker's
/// rate limits yourself.
///
/// # Examples
///
/// ```
/// use brokapi_tbank_protocol::v1::GetAccountsRequest;
/// use brokapi_tbank_protocol::{TbankClient, TbankConfig};
/// # use brokapi_mocks::tbank::{MockServer, TOKEN};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start(Default::default()).await;
/// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
/// let client = TbankClient::new(&cfg)?;
/// // Any method of the API is reachable through the generated clients.
/// let reply = client.users().get_accounts(GetAccountsRequest { status: None }).await?;
/// assert!(!reply.into_inner().accounts.is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct TbankClient {
    channel: Channel,
    interceptor: AuthInterceptor,
}

impl TbankClient {
    /// Builds the channel without touching the network (connects on first use, reconnects on failure).
    ///
    /// An `https://` endpoint gets TLS with the bundled web PKI roots, and the extra root certificate of
    /// [`TbankConfig::root_certificate_pem`] if there is one. An `http://` endpoint is plain text and meant
    /// for a local mock. HTTP/2 keep-alive pings run every 5 minutes and only while a call or a stream is
    /// open: a server counts pings on an idle connection as abuse and closes it (`too_many_pings`), so none
    /// are sent when nothing is open. A dead stream is found by the silence limit of the connector.
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::Config`] when the token or app name is not a valid header value, or the
    ///   endpoint is not a valid URL.
    /// - [`ProtocolError::Transport`] when the TLS configuration is refused.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_tbank_protocol::{ProtocolError, TbankClient, TbankConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// // The sandbox endpoint is not contacted here: the channel is lazy.
    /// assert!(TbankClient::new(&TbankConfig::sandbox("t.my-token")).is_ok());
    ///
    /// // A bad endpoint is reported at once.
    /// let mut cfg = TbankConfig::sandbox("t.my-token");
    /// cfg.endpoint = "not a url".into();
    /// assert!(matches!(TbankClient::new(&cfg), Err(ProtocolError::Config(_))));
    /// # }
    /// ```
    pub fn new(cfg: &TbankConfig) -> Result<Self, ProtocolError> {
        let interceptor = AuthInterceptor::new(&cfg.token, cfg.app_name.as_deref())?;
        let mut endpoint = Endpoint::from_shared(cfg.endpoint.clone())
            .map_err(|e| ProtocolError::Config(format!("endpoint {}: {e}", cfg.endpoint)))?
            .connect_timeout(cfg.connect_timeout)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(false);
        if cfg.endpoint.starts_with("https://") {
            let mut tls = ClientTlsConfig::new().with_webpki_roots();
            if let Some(pem) = &cfg.root_certificate_pem {
                if !pem.windows(11).any(|w| w == b"-----BEGIN ") {
                    return Err(ProtocolError::Config(
                        "root_certificate_pem holds no PEM certificate".into(),
                    ));
                }
                tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(pem));
            }
            endpoint = endpoint.tls_config(tls)?;
        }
        Ok(Self {
            channel: endpoint.connect_lazy(),
            interceptor,
        })
    }

    fn with_auth(&self) -> Authed {
        InterceptedService::new(self.channel.clone(), self.interceptor.clone())
    }

    /// A generic gRPC client over the same channel and token, for a call that is not made through a
    /// generated client: the method is named by a path (`/package.Service/Method`) and the messages are
    /// whatever codec you give it. With [`FILE_DESCRIPTOR_SET`](crate::FILE_DESCRIPTOR_SET) it calls any
    /// method of the API by name.
    pub fn grpc(&self) -> tonic::client::Grpc<Authed> {
        tonic::client::Grpc::new(self.with_auth())
    }

    /// Client of `MarketDataService`: unary market data: order book snapshot, last trades, candles, trading status.
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn market_data(&self) -> MarketDataServiceClient<Authed> {
        MarketDataServiceClient::new(self.with_auth())
    }

    /// Client of `MarketDataStreamService`: the bidirectional market data stream (order book, trades, candles subscriptions).
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn market_data_stream(&self) -> MarketDataStreamServiceClient<Authed> {
        MarketDataStreamServiceClient::new(self.with_auth())
    }

    /// Client of `OrdersService`: orders: place, cancel, replace, order state, active orders.
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn orders(&self) -> OrdersServiceClient<Authed> {
        OrdersServiceClient::new(self.with_auth())
    }

    /// Client of `OrdersStreamService`: server streams of own order states and own trades.
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn orders_stream(&self) -> OrdersStreamServiceClient<Authed> {
        OrdersStreamServiceClient::new(self.with_auth())
    }

    /// Client of `OperationsService`: portfolio, positions, operation history.
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn operations(&self) -> OperationsServiceClient<Authed> {
        OperationsServiceClient::new(self.with_auth())
    }

    /// Client of `OperationsStreamService`: server streams of portfolio and positions changes.
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn operations_stream(&self) -> OperationsStreamServiceClient<Authed> {
        OperationsStreamServiceClient::new(self.with_auth())
    }

    /// Client of `UsersService`: accounts, user info, tariff and limits.
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn users(&self) -> UsersServiceClient<Authed> {
        UsersServiceClient::new(self.with_auth())
    }

    /// Client of `InstrumentsService`: the instrument directory: search, listings, schedules, reference data.
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn instruments(&self) -> InstrumentsServiceClient<Authed> {
        InstrumentsServiceClient::new(self.with_auth())
    }

    /// Client of `SandboxService`: sandbox methods: open virtual accounts, top up virtual money (meant for the sandbox endpoint).
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn sandbox(&self) -> SandboxServiceClient<Authed> {
        SandboxServiceClient::new(self.with_auth())
    }

    /// Client of `StopOrdersService`: stop orders: place, cancel, list (unary only, no stream).
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn stop_orders(&self) -> StopOrdersServiceClient<Authed> {
        StopOrdersServiceClient::new(self.with_auth())
    }

    /// Client of `SignalService`: signals and strategies.
    ///
    /// Returns a fresh generated client on the shared channel. It is cheap, create one per call.
    pub fn signals(&self) -> SignalServiceClient<Authed> {
        SignalServiceClient::new(self.with_auth())
    }
}

impl TbankClient {
    /// `UsersService/GetAccounts`: every account the token can see (a token covers several accounts;
    /// pick the `id` to put into the connector config as `account_id`).
    ///
    /// No status filter is sent. Whether the broker then returns only open accounts or all of them is
    /// not verified against the live API, so check `status` of each [`Account`](crate::v1::Account)
    /// before you trade on it. The call is not rate limited here.
    ///
    /// # Errors
    ///
    /// The `tonic::Status` of the failed call: `Unauthenticated` for a bad token, `Unavailable` or
    /// `DeadlineExceeded` for network trouble. The call is a read, so repeating it is safe.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_tbank_protocol::{TbankClient, TbankConfig};
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let client = TbankClient::new(&cfg)?;
    /// let ids: Vec<String> = client.list_accounts().await?.into_iter().map(|a| a.id).collect();
    /// // One connector per account: put each id into `TbankConfig::with_account`.
    /// assert!(ids.len() >= 2);
    ///
    /// // A wrong token is refused by the server and shows as a status, not a panic.
    /// let bad = TbankClient::new(&TbankConfig::for_mock("wrong-token", &server.endpoint))?;
    /// let status = bad.list_accounts().await.unwrap_err();
    /// assert_eq!(status.code(), tonic::Code::Unauthenticated);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_accounts(&self) -> Result<Vec<crate::v1::Account>, Status> {
        let resp = self
            .users()
            .get_accounts(crate::v1::GetAccountsRequest { status: None })
            .await?;
        Ok(resp.into_inner().accounts)
    }
}
