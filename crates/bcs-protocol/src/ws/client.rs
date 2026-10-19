//! The WebSocket client: it opens one background session per channel.

use anyhow::Result;
use kanal::AsyncSender;
use serde::Deserialize;

use crate::config::BcsConfig;
use crate::error::BcsApiError;
use crate::http::client::BcsHttpClient;
use crate::models::instrument::InstrumentKey;
use crate::models::messages::IncomingMessage;
use crate::ws::models::MarketDataSubscribe;
use crate::ws::session::{WsOptions, WsSession};

pub use crate::ws::models::WsOutgoing;

/// A WebSocket channel (service) of the BCS API. Each channel is its own URL and its own connection.
///
/// Only [`BcsWsChannel::MarketData`] takes subscription commands. The others push their data as soon as
/// they are connected, with nothing to send.
///
/// The broker limits the number of simultaneous connections per account: 2 for portfolio, limits and
/// margin indicators, 4 for orders, 20 for market data (broker docs). It also limits the client to 10
/// messages per second and 100 instruments per connection.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
///
/// let channel: BcsWsChannel = serde_json::from_str("\"market_data\"")?;
/// assert_eq!(channel, BcsWsChannel::MarketData);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BcsWsChannel {
    /// Account limits: positions, money, futures limits (`limits/ws`). Parsed into
    /// [`IncomingMessage::Limits`].
    Limits,
    /// Portfolio positions with values and P/L (`portfolio/ws`). Parsed into
    /// [`IncomingMessage::Portfolio`].
    Portfolio,
    /// Market data: quotes, order book, trades tape and last candle (`market-data/ws`). The only
    /// channel that takes subscriptions. Parsed into [`IncomingMessage::MarketData`].
    MarketData,
    /// Deprecated order execution channel (`orders/execution/ws`). The broker docs mark it
    /// deprecated: use [`BcsWsChannel::OrderEvents`]. Parsed into [`IncomingMessage::OrderExecution`].
    OrderExecution,
    /// Deprecated order transaction channel (`orders/transaction/ws`). The broker docs mark it
    /// deprecated: use [`BcsWsChannel::OrderEvents`]. Parsed into [`IncomingMessage::OrderTransaction`].
    OrderTransaction,
    /// Current order status channel (`orders/events/ws`), replaces execution-status and
    /// transaction-status, which the docs now mark deprecated. Parsed into
    /// [`IncomingMessage::OrderExecution`].
    OrderEvents,
    /// Margin indicators: portfolio value, margin level, debts (`marginal-indicators/ws`). Parsed into
    /// [`IncomingMessage::MarginalIndicators`].
    MarginalIndicators,
}

impl BcsWsChannel {
    /// The URL path of the channel, added to the WebSocket base URL.
    pub(super) fn path(&self) -> &'static str {
        match self {
            BcsWsChannel::Limits => "/trade-api-bff-limit/api/v1/limits/ws",
            BcsWsChannel::Portfolio => "/trade-api-bff-portfolio/api/v1/portfolio/ws",
            BcsWsChannel::MarketData => "/trade-api-market-data-connector/api/v1/market-data/ws",
            BcsWsChannel::OrderExecution => "/trade-api-bff-operations/api/v1/orders/execution/ws",
            BcsWsChannel::OrderTransaction => {
                "/trade-api-bff-operations/api/v1/orders/transaction/ws"
            }
            BcsWsChannel::OrderEvents => "/trade-api-bff-operations/api/v1/orders/events/ws",
            BcsWsChannel::MarginalIndicators => {
                "/trade-api-bff-marginal-indicators/api/v1/marginal-indicators/ws"
            }
        }
    }
}

/// The WebSocket client of the BCS API: a factory of [`WsSession`]s that share one authorized HTTP
/// client.
///
/// The client holds no connection itself. Each `connect_*` call starts a **background session** for one
/// channel (see [`WsSession`] for what it does for you) and returns at once. Frames arrive in the
/// `kanal` channel you pass in. The sessions of one client share the access token.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// use std::time::Duration;
/// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
/// use brokapi_bcs_protocol::models::messages::IncomingMessage;
/// use brokapi_bcs_protocol::ws::client::BcsWsClient;
/// use brokapi_bcs_protocol::ws::models::MarketDataMessage;
///
/// let ws = BcsWsClient::new(cfg)?;
/// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(64);
/// let session = ws.connect_market_data(tx);
/// ws.subscribe_order_book(&session, vec![InstrumentKey::new("SBER", "TQBR")], 10).await?;
///
/// // Wait for the first book, with a timeout so a broken link cannot hang the program.
/// let book = tokio::time::timeout(Duration::from_secs(5), async {
///     loop {
///         if let IncomingMessage::MarketData(MarketDataMessage::OrderBook(book)) = rx.recv().await? {
///             return Ok::<_, kanal::ReceiveError>(book);
///         }
///     }
/// })
/// .await??;
/// assert_eq!(book.ticker, "SBER");
/// # Ok(())
/// # }
/// ```
pub struct BcsWsClient {
    pub(super) http_client: BcsHttpClient,
    options: WsOptions,
}

impl BcsWsClient {
    /// Creates a client. It builds the HTTP client for tokens and makes no network call.
    ///
    /// The sessions use [`WsOptions::default`] until you call [`BcsWsClient::with_options`].
    ///
    /// # Errors
    ///
    /// The same as [`BcsHttpClient::new`]: [`BcsApiError::HttpError`] if the HTTP stack cannot be built.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    ///
    /// let ws = BcsWsClient::new(BcsConfig::new("refresh-token".to_string()))?;
    /// assert_eq!(ws.http_client().ws_base_url(), "wss://ws.broker.ru");
    /// # Ok::<(), brokapi_bcs_protocol::error::BcsApiError>(())
    /// ```
    pub fn new(config: BcsConfig) -> Result<Self, BcsApiError> {
        let http_client = BcsHttpClient::new(config)?;
        Ok(Self {
            http_client,
            options: WsOptions::default(),
        })
    }

    /// Sets the options of the sessions this client opens from now on. Sessions already open keep the
    /// options they started with.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    /// use brokapi_bcs_protocol::ws::session::WsOptions;
    ///
    /// let ws = BcsWsClient::new(BcsConfig::new("refresh-token".to_string()))?
    ///     .with_options(WsOptions { announce_connects: true, ..WsOptions::default() });
    /// # let _ = ws;
    /// # Ok::<(), brokapi_bcs_protocol::error::BcsApiError>(())
    /// ```
    pub fn with_options(mut self, options: WsOptions) -> Self {
        self.options = options;
        self
    }

    /// Starts a background session for a channel and returns it at once.
    ///
    /// The connection is made in the background: the returned session may not be connected yet
    /// ([`WsSession::is_connected`]). Every parsed frame goes to `event_tx`.
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime, because the session is a Tokio task.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::{BcsWsChannel, BcsWsClient};
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let session = ws.spawn_session(BcsWsChannel::Portfolio, tx);
    /// assert_eq!(session.channel(), BcsWsChannel::Portfolio);
    /// # Ok(())
    /// # }
    /// ```
    pub fn spawn_session(
        &self,
        service: BcsWsChannel,
        event_tx: AsyncSender<IncomingMessage>,
    ) -> WsSession {
        WsSession::spawn_with(service, self.http_client.clone(), event_tx, self.options)
    }

    /// The HTTP client the sessions take their tokens from. It is a normal [`BcsHttpClient`], so you can
    /// make REST calls with the same tokens.
    pub fn http_client(&self) -> &BcsHttpClient {
        &self.http_client
    }

    /// Starts the portfolio session (`portfolio/ws`). The broker sends an array with one record per
    /// position. Frames arrive as [`IncomingMessage::Portfolio`]. The broker docs do not say whether a
    /// first snapshot comes right at connect (the in-process imitation sends one).
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let _session = ws.connect_portfolio(tx);
    /// let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await??;
    /// match first {
    ///     IncomingMessage::Portfolio(positions) => assert!(!positions.is_empty()),
    ///     other => panic!("unexpected: {other:?}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_portfolio(&self, event_tx: AsyncSender<IncomingMessage>) -> WsSession {
        self.spawn_session(BcsWsChannel::Portfolio, event_tx)
    }

    /// Starts the **deprecated** transaction-status session (`orders/transaction/ws`). Prefer
    /// [`BcsWsClient::connect_order_events`]. Frames arrive as [`IncomingMessage::OrderTransaction`].
    /// The in-process imitation does not serve this channel with data, so there is no runnable example.
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime.
    pub fn connect_order_transaction(&self, event_tx: AsyncSender<IncomingMessage>) -> WsSession {
        self.spawn_session(BcsWsChannel::OrderTransaction, event_tx)
    }

    /// Starts the limits session (`limits/ws`). The broker sends one object with the current limits:
    /// positions, money, futures holdings and futures limits. Frames arrive as
    /// [`IncomingMessage::Limits`]. The broker docs do not say whether a first snapshot comes right at
    /// connect (the in-process imitation sends one).
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let _session = ws.connect_limits(tx);
    /// let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await??;
    /// match first {
    ///     IncomingMessage::Limits(limits) => assert!(limits.money_limits.is_some()),
    ///     other => panic!("unexpected: {other:?}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_limits(&self, event_tx: AsyncSender<IncomingMessage>) -> WsSession {
        self.spawn_session(BcsWsChannel::Limits, event_tx)
    }

    /// Starts the order events session (`orders/events/ws`), the current channel for order states and
    /// fills. Frames arrive as [`IncomingMessage::OrderExecution`], in the same shape as the old
    /// execution channel.
    ///
    /// The docs do not describe a replay of history, so treat an event that happened while you were not
    /// connected as lost. After a reconnect ask for the state of your open orders with
    /// [`BcsHttpClient::get_order`] (the session tells you about a reconnect with
    /// [`IncomingMessage::Reconnected`], see [`WsOptions::announce_connects`]).
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let options = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
    /// # let server = brokapi_mocks::bcs::MockServer::start(options.clone()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::http::models::CreateOrderRequest;
    /// use brokapi_bcs_protocol::models::enums::{OrderSide, OrderStatus};
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    /// use brokapi_bcs_protocol::ws::session::WsOptions;
    ///
    /// let ws = BcsWsClient::new(cfg)?
    ///     .with_options(WsOptions { announce_connects: true, ..WsOptions::default() });
    /// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let _session = ws.connect_order_events(tx);
    ///
    /// // The first message is the marker of the connect. Only after it do events follow.
    /// let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await??;
    /// assert!(matches!(first, IncomingMessage::Reconnected));
    /// # // The socket is registered by the imitation a moment after the marker.
    /// # tokio::time::timeout(Duration::from_secs(5), async {
    /// #     while options.event_sockets() == 0 { tokio::time::sleep(Duration::from_millis(10)).await; }
    /// # }).await?;
    ///
    /// // Place an order over REST. Its state arrives on the socket.
    /// let cid = uuid::Uuid::new_v4();
    /// ws.http_client()
    ///     .create_order(CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0)))
    ///     .await?;
    /// let event = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await??;
    /// match event {
    ///     IncomingMessage::OrderExecution(m) => {
    ///         assert_eq!(m.data.unwrap().order_status, OrderStatus::New);
    ///     }
    ///     other => panic!("unexpected: {other:?}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_order_events(&self, event_tx: AsyncSender<IncomingMessage>) -> WsSession {
        self.spawn_session(BcsWsChannel::OrderEvents, event_tx)
    }

    /// Starts the **deprecated** execution-status session (`orders/execution/ws`). Prefer
    /// [`BcsWsClient::connect_order_events`]. Frames arrive as [`IncomingMessage::OrderExecution`].
    /// The in-process imitation does not serve this channel with data, so there is no runnable example.
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime.
    pub fn connect_order_execution(&self, event_tx: AsyncSender<IncomingMessage>) -> WsSession {
        self.spawn_session(BcsWsChannel::OrderExecution, event_tx)
    }

    /// Starts the margin indicators session (`marginal-indicators/ws`): portfolio value, margin level,
    /// debts. The broker sends all of it in one object. Frames arrive as
    /// [`IncomingMessage::MarginalIndicators`]. The docs do not say whether a first snapshot comes right
    /// at connect (the in-process imitation sends one).
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let _session = ws.connect_marginal_indicators(tx);
    /// let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await??;
    /// match first {
    ///     IncomingMessage::MarginalIndicators(m) => assert!(m.portfolio_current_value.is_some()),
    ///     other => panic!("unexpected: {other:?}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_marginal_indicators(&self, event_tx: AsyncSender<IncomingMessage>) -> WsSession {
        self.spawn_session(BcsWsChannel::MarginalIndicators, event_tx)
    }

    /// Starts the market data session (`market-data/ws`). It sends nothing until you subscribe with
    /// [`BcsWsClient::subscribe_quotes`], [`BcsWsClient::subscribe_order_book`],
    /// [`BcsWsClient::subscribe_last_trades`] or [`BcsWsClient::subscribe_last_candle`]. Frames arrive
    /// as [`IncomingMessage::MarketData`].
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime.
    ///
    /// # Examples
    ///
    /// See [`BcsWsClient`] for a full example with a subscription.
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::{BcsWsChannel, BcsWsClient};
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let session = ws.connect_market_data(tx);
    /// assert_eq!(session.channel(), BcsWsChannel::MarketData);
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_market_data(&self, event_tx: AsyncSender<IncomingMessage>) -> WsSession {
        self.spawn_session(BcsWsChannel::MarketData, event_tx)
    }

    /// Subscribes to quotes of the instruments (`dataType` 3).
    ///
    /// The command is kept by the session and sent again after every reconnect. The broker answers
    /// with a subscription confirmation and then with quote frames.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::SubscriptionError`] if the session is not a market data session, if the session
    /// already holds 100 subscribe commands, or if the session task has ended. A refusal by the broker
    /// is not an error here: it arrives later as a frame.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    /// use brokapi_bcs_protocol::ws::models::MarketDataMessage;
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let session = ws.connect_market_data(tx);
    /// ws.subscribe_quotes(&session, vec![InstrumentKey::new("SBER", "TQBR")]).await?;
    ///
    /// let quote = tokio::time::timeout(Duration::from_secs(5), async {
    ///     loop {
    ///         if let IncomingMessage::MarketData(MarketDataMessage::Quotes(q)) = rx.recv().await? {
    ///             return Ok::<_, kanal::ReceiveError>(q);
    ///         }
    ///     }
    /// })
    /// .await??;
    /// assert_eq!(quote.ticker, "SBER");
    ///
    /// // A subscription on a channel that takes none is refused locally.
    /// let (tx2, _rx2) = kanal::bounded_async::<IncomingMessage>(16);
    /// let portfolio = ws.connect_portfolio(tx2);
    /// assert!(ws.subscribe_quotes(&portfolio, vec![]).await.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe_quotes(
        &self,
        session: &WsSession,
        instruments: Vec<InstrumentKey>,
    ) -> Result<(), BcsApiError> {
        session
            .subscribe(MarketDataSubscribe::quotes(instruments))
            .await
    }

    /// Subscribes to the order book of the instruments (`dataType` 0) with `depth` levels per side.
    ///
    /// The broker docs allow `depth` from 1 to 20 (default 20). The value is **not checked here**: a
    /// bad depth comes back as an error frame from the broker. Errors and retention are as for
    /// [`BcsWsClient::subscribe_quotes`].
    ///
    /// # Errors
    ///
    /// The same as [`BcsWsClient::subscribe_quotes`].
    ///
    /// # Examples
    ///
    /// See the example on [`BcsWsClient`].
    pub async fn subscribe_order_book(
        &self,
        session: &WsSession,
        instruments: Vec<InstrumentKey>,
        depth: u32,
    ) -> Result<(), BcsApiError> {
        session
            .subscribe(MarketDataSubscribe::order_book(instruments, depth))
            .await
    }

    /// Subscribes to the anonymous trades tape of the instruments (`dataType` 2). Each trade arrives as
    /// one frame, see [`LastTradesData`](crate::ws::models::LastTradesData).
    ///
    /// # Errors
    ///
    /// The same as [`BcsWsClient::subscribe_quotes`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    /// use brokapi_bcs_protocol::ws::models::MarketDataMessage;
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let session = ws.connect_market_data(tx);
    /// ws.subscribe_last_trades(&session, vec![InstrumentKey::new("SBER", "TQBR")]).await?;
    ///
    /// let trade = tokio::time::timeout(Duration::from_secs(5), async {
    ///     loop {
    ///         if let IncomingMessage::MarketData(MarketDataMessage::LastTrades(t)) = rx.recv().await? {
    ///             return Ok::<_, kanal::ReceiveError>(t);
    ///         }
    ///     }
    /// })
    /// .await??;
    /// assert!(trade.price > dec!(0.0));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe_last_trades(
        &self,
        session: &WsSession,
        instruments: Vec<InstrumentKey>,
    ) -> Result<(), BcsApiError> {
        session
            .subscribe(MarketDataSubscribe::last_trades(instruments))
            .await
    }

    /// Subscribes to the last candle of the instruments (`dataType` 1) for a time frame.
    ///
    /// `time_frame` is one of `M1 M5 M15 M30 H1 H4 D W MN` (broker docs). It is not checked here.
    /// Note `D` and `W`: the names `D1` and `W1` are not in the broker's list.
    ///
    /// # Errors
    ///
    /// The same as [`BcsWsClient::subscribe_quotes`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    /// use brokapi_bcs_protocol::ws::models::MarketDataMessage;
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let session = ws.connect_market_data(tx);
    /// ws.subscribe_last_candle(&session, vec![InstrumentKey::new("SBER", "TQBR")], "M1").await?;
    ///
    /// let candle = tokio::time::timeout(Duration::from_secs(5), async {
    ///     loop {
    ///         if let IncomingMessage::MarketData(MarketDataMessage::LastCandle(c)) = rx.recv().await? {
    ///             return Ok::<_, kanal::ReceiveError>(c);
    ///         }
    ///     }
    /// })
    /// .await??;
    /// assert_eq!(candle.time_frame.as_deref(), Some("M1"));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe_last_candle(
        &self,
        session: &WsSession,
        instruments: Vec<InstrumentKey>,
        time_frame: &str,
    ) -> Result<(), BcsApiError> {
        session
            .subscribe(MarketDataSubscribe::last_candle(instruments, time_frame))
            .await
    }
}
