//! Direct access to any BCS API method (the "back door"): same refresh token and access token, same
//! endpoint, same [`BrokerLimiter`] as the connector it came from.
//!
//! The connectors wrap what the shared traits can express. Everything else (quotes snapshot,
//! non-trade operations, limits, instrument discounts, the limits and marginal-indicators
//! WebSockets, ...) is reachable here. Get a handle with `raw()` on either connector.
//!
//! Rules of a call:
//!
//! 1. The quota is taken first, by the operation name (`"GetOperationHistory"`), from the route in
//!    `configs/bcs_limits.toml`. A name without a route spends the `"*"` default route, so a call is
//!    never free. A refusal is [`Error::RateLimited`] and the request is not sent.
//! 2. The closure gets the connector's [`BcsHttpClient`]: it holds the access token, refreshes it and
//!    repeats a request once after a 401. One closure is one API method.
//! 3. [`call`](BcsRaw::call) is for reads. [`call_command`](BcsRaw::call_command) is for anything that
//!    changes the account: a break, a timeout or a 5xx is [`Error::Unknown`], and it is never retried.
//! 4. [`connect_ws`](BcsRaw::connect_ws) opens one of the WebSocket channels with the same token.
//!
//! The raw layer does not know about the connector's order state: orders placed through it are not
//! tracked (no events, no `reconcile`). Use the shared traits for orders.
//!
//! # Examples
//!
//! A read that no shared trait wraps, the quotes snapshot, through the market data connector:
//!
//! ```
//! use brokapi_bcs_protocol::http::models::{QuoteInstrument, QuotesRequest};
//! use brokapi_bcs::limits::default_limits;
//! use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
//! use brokapi_core::ShutdownMode;
//! use brokapi_runtime::BrokerLimiter;
//! use tokio::sync::mpsc;
//! # use brokapi_bcs_protocol::config::BcsConfig;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
//! # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
//! let (events_tx, _events) = mpsc::channel(256);
//! let limiter = BrokerLimiter::new(&default_limits());
//! let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
//!
//! let raw = md.raw();
//! // "GetQuotes" is the name of the quota route. The closure is one API method.
//! let quotes = raw
//!     .call("GetQuotes", |http| async move {
//!         http.get_quotes(&QuotesRequest { instruments: vec![QuoteInstrument::new("SBER", "TQBR")] }).await
//!     })
//!     .await?;
//! for q in &quotes.records {
//!     println!("{:?} bid {:?} offer {:?}", q.ticker, q.bid, q.offer);
//! }
//! md.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;

use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_bcs_protocol::http::client::BcsHttpClient;
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::session::WsSession;
use brokapi_core::Error;
use brokapi_runtime::BrokerLimiter;
use kanal::AsyncSender;

use crate::error::{map_command_err, map_err};

/// A handle for calling any BCS API method with a connector's token and rate limiter.
///
/// Get it from `raw()` on [`MarketDataConnector`](crate::MarketDataConnector) or
/// [`UserDataConnector`](crate::UserDataConnector). It is cheap to clone, and all clones share the
/// token and the limiter. See the [module docs](self) for the rules of a call.
#[derive(Clone)]
pub struct BcsRaw {
    http: BcsHttpClient,
    limiter: Arc<BrokerLimiter>,
}

impl BcsRaw {
    pub(crate) fn new(http: BcsHttpClient, limiter: Arc<BrokerLimiter>) -> Self {
        Self { http, limiter }
    }

    /// Calls a read-only API method: takes the quota for `method`, then runs `f`.
    ///
    /// `method` is the name of the quota route, not a URL. Take a name from `configs/bcs_limits.toml`
    /// (for example `GetQuotes`). An unknown name spends the default route `"*"`. `f` receives the
    /// connector's [`BcsHttpClient`] and returns the future of one API call. Do not use this for
    /// anything that changes the account, use [`call_command`](Self::call_command).
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`] when the quota is spent. Nothing was sent. Also when the broker
    ///   answers HTTP 429.
    /// - [`Error::Rejected`] with code `validation` when the broker answers HTTP 400 with a
    ///   validation error.
    /// - [`Error::Transport`] for everything else: no connection, a timeout, an unreadable answer, HTTP
    ///   404 (`not found`), a failed token refresh, any other HTTP status. A read can be repeated.
    /// - [`Error::Config`] when the limits file routes the name to a bucket that does not exist.
    ///
    /// The full table is in [`crate::error::map_err`].
    ///
    /// # Examples
    ///
    /// A quota that is spent refuses the call before anything is sent:
    ///
    /// ```
    /// use brokapi_bcs_protocol::http::models::{QuoteInstrument, QuotesRequest};
    /// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_core::{Error, ShutdownMode};
    /// use brokapi_runtime::{BrokerLimiter, BrokerLimits};
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// // One call per minute for GetQuotes.
    /// let limits = BrokerLimits::from_toml_str(
    ///     r#"
    ///     [bcs.buckets.one]
    ///     rate = 1
    ///     per = "60s"
    ///     burst = 1
    ///     [bcs.route]
    ///     "*" = ["one"]
    ///     [bcs.conns]
    ///     max_conns = 4
    ///     max_subs_per_conn = 50
    ///     supports_multiplex = false
    ///     "#,
    /// )?;
    /// # let (events_tx, _events) = mpsc::channel(16);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "bcs-md"),
    ///     BrokerLimiter::new(&limits),
    ///     events_tx,
    /// )
    /// .await?;
    /// let raw = md.raw();
    /// let quotes = || QuotesRequest { instruments: vec![QuoteInstrument::new("SBER", "TQBR")] };
    ///
    /// raw.call("GetQuotes", |http| async move { http.get_quotes(&quotes()).await }).await?;
    /// let second = raw.call("GetQuotes", |http| async move { http.get_quotes(&quotes()).await }).await;
    /// assert!(matches!(second, Err(Error::RateLimited { .. })));
    /// md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(BcsHttpClient) -> Fut,
        Fut: Future<Output = Result<T, BcsApiError>>,
    {
        self.limiter.acquire(method, brokapi_runtime::now())?;
        f(self.http.clone()).await.map_err(map_err)
    }

    /// Calls an API method that changes the account: takes the quota for `method`, then runs `f`.
    ///
    /// Like [`call`](Self::call), but anything that may have reached the broker is
    /// [`Error::Unknown`]. The library never repeats the call: a retry of a lost place request could
    /// create a second order. Find out what happened (read the order, run `reconcile`) before you
    /// decide to send it again. The order is not tracked by the connector (see the module docs).
    ///
    /// # Errors
    ///
    /// - [`Error::Unknown`] when there was no answer, a break or a timeout, HTTP 500 or above, or an
    ///   answer that cannot be read. The command may have been done.
    /// - [`Error::Rejected`] when the broker answered and said no: code `validation` for a validation
    ///   error, code `http_<status>` for another status from 400 to 499 except 429. Nothing was done.
    /// - [`Error::RateLimited`] when the quota is spent (nothing was sent) or the broker answers HTTP 429.
    /// - [`Error::Transport`] for a failed token refresh and for HTTP 404 (`not found`).
    /// - [`Error::Config`] when the limits file routes the name to a bucket that does not exist.
    ///
    /// The full table is in [`crate::error::map_command_err`].
    ///
    /// # Examples
    ///
    /// An order sent through the raw layer, refused by the broker (the mock refuses a limit order at
    /// `REJECT_PRICE`). The refusal is a definite [`Error::Rejected`], not an unknown outcome:
    ///
    /// ```
    /// use brokapi_bcs_protocol::http::models::CreateOrderRequest;
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// use brokapi_conformance::REJECT_PRICE;
    /// use brokapi_core::{ClientOrderId, Error, ShutdownMode};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = mpsc::channel(256);
    /// # let limiter = BrokerLimiter::new(&default_limits());
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    ///
    /// let order = CreateOrderRequest::limit(
    ///     ClientOrderId::new_v4(), "SBER", "TQBR", OrderSide::Buy, 1, REJECT_PRICE.decimal(),
    /// );
    /// let result = ud
    ///     .raw()
    ///     .call_command("PlaceOrder", |http| async move { http.create_order_sync(&order).await })
    ///     .await;
    /// assert!(matches!(result, Err(Error::Rejected { .. })));
    /// ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call_command<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(BcsHttpClient) -> Fut,
        Fut: Future<Output = Result<T, BcsApiError>>,
    {
        self.limiter.acquire(method, brokapi_runtime::now())?;
        f(self.http.clone()).await.map_err(map_command_err)
    }

    /// Opens a WebSocket channel (`Limits`, `MarginalIndicators`, ...) with the connector's token.
    ///
    /// The quota name is `Connect<Channel>` (`ConnectLimits`). The session reconnects and
    /// resubscribes on its own. Messages arrive on `event_tx`, parsed by the protocol layer. The
    /// returned [`WsSession`] connects in the background: it does not wait for the connection.
    /// Read `event_tx`'s receiver, or the session drops messages when the channel is full (see
    /// `WsSession::dropped_events`).
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`] when the quota `Connect<Channel>` is spent. No socket is opened.
    /// - [`Error::Config`] when the limits file routes the name to a bucket that does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = mpsc::channel(256);
    /// # let limiter = BrokerLimiter::new(&default_limits());
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    ///
    /// let (tx, _rx) = kanal::bounded_async(64);
    /// let session = md.raw().connect_ws(BcsWsChannel::Limits, tx)?;
    /// // The session connects in the background: wait for it, with a limit.
    /// tokio::time::timeout(Duration::from_secs(5), async {
    ///     while !session.is_connected() {
    ///         tokio::time::sleep(Duration::from_millis(10)).await;
    ///     }
    /// })
    /// .await?;
    /// md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_ws(
        &self,
        channel: BcsWsChannel,
        event_tx: AsyncSender<IncomingMessage>,
    ) -> Result<WsSession, Error> {
        self.limiter
            .acquire(&format!("Connect{channel:?}"), brokapi_runtime::now())?;
        Ok(WsSession::spawn(channel, self.http.clone(), event_tx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http(status: Option<u16>) -> BcsApiError {
        BcsApiError::HttpError {
            status,
            message: "x".into(),
            trace_id: None,
        }
    }

    #[test]
    fn lost_commands_are_unknown_refused_ones_are_not() {
        assert!(matches!(map_command_err(http(None)), Error::Unknown));
        assert!(matches!(map_command_err(http(Some(503))), Error::Unknown));
        assert!(matches!(
            map_command_err(BcsApiError::ParseError("x".into())),
            Error::Unknown
        ));
        assert!(matches!(
            map_command_err(http(Some(429))),
            Error::RateLimited { .. }
        ));
        assert!(matches!(
            map_command_err(BcsApiError::ValidationError {
                trace_id: None,
                errors: vec![]
            }),
            Error::Rejected { .. }
        ));
    }
}
