//! Direct access to any ALOR API method (the "back door"): same refresh token and access token, same endpoint,
//! same [`BrokerLimiter`] as the connector it came from.
//!
//! The connectors wrap what the shared traits can express. Everything else (quotations of many instruments, the
//! summary and the risks of the portfolio, order groups, estimates, order cancel for all, the legacy stop-loss
//! family, the other subscriptions of the data channel, the trading channel) is reachable here. Get a handle with
//! `raw()` on either connector.
//!
//! Rules of a call:
//!
//! 1. The quota is taken first, by the operation name (`"GetQuotes"`), from the route in `limits.toml`. A name
//!    without a route spends the `"*"` default route, so a call is never free. A refusal is
//!    [`Error::RateLimited`] and the request is not sent.
//! 2. The closure gets the connector's [`AlorHttpClient`]: it holds the access token, refreshes it and repeats a
//!    read once after a 401. One closure is one API method.
//! 3. [`call`](AlorRaw::call) is for reads. [`call_command`](AlorRaw::call_command) is for anything that changes
//!    the account: a break, a timeout or a 5xx is [`Error::Unknown`], and it is never retried.
//! 4. [`connect_ws`](AlorRaw::connect_ws) opens one more session of the data channel and
//!    [`connect_cws`](AlorRaw::connect_cws) one of the trading channel, with the same token.
//!
//! The raw layer does not know about the connector's order state: orders placed through it are not tracked (no
//! events, no `reconcile`). Use the shared traits for orders.
//!
//! # Examples
//!
//! A read that no shared trait wraps, the quotations of several instruments, through the market data connector:
//!
//! ```
//! use brokapi_alor::limits::default_limits;
//! use brokapi_alor::{MarketDataConfig, MarketDataConnector};
//! use brokapi_core::ShutdownMode;
//! use brokapi_runtime::BrokerLimiter;
//! use tokio::sync::mpsc;
//! # use brokapi_alor_protocol::config::AlorConfig;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
//! # let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
//! let (events_tx, _events) = mpsc::channel(256);
//! let limiter = BrokerLimiter::new(&default_limits());
//! let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "alor-md"), limiter, events_tx).await?;
//!
//! let raw = md.raw();
//! // "GetQuotes" is the name of the quota route. The closure is one API method.
//! let quotes = raw
//!     .call("GetQuotes", |http| async move { http.get_quotes(&[("MOEX", "SBER")]).await })
//!     .await?;
//! for q in &quotes {
//!     println!("{} bid {:?} ask {:?}", q.symbol, q.bid, q.ask);
//! }
//! md.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use brokapi_alor_protocol::error::AlorApiError;
use brokapi_alor_protocol::http::client::AlorHttpClient;
use brokapi_alor_protocol::ws::cws::CwsSession;
use brokapi_alor_protocol::ws::models::WsIncoming;
use brokapi_alor_protocol::ws::session::{WsOptions, WsSession};
use brokapi_core::Error;
use brokapi_runtime::BrokerLimiter;
use kanal::AsyncSender;

use crate::error::{map_command_err, map_err};

/// A handle for calling any ALOR API method with a connector's token and rate limiter.
///
/// Get it from `raw()` on [`MarketDataConnector`](crate::MarketDataConnector) or
/// [`UserDataConnector`](crate::UserDataConnector). It is cheap to clone, and all clones share the token and the
/// limiter. See the [module docs](self) for the rules of a call.
#[derive(Clone)]
pub struct AlorRaw {
    http: AlorHttpClient,
    limiter: Arc<BrokerLimiter>,
}

impl AlorRaw {
    pub(crate) fn new(http: AlorHttpClient, limiter: Arc<BrokerLimiter>) -> Self {
        Self { http, limiter }
    }

    /// Calls a read-only API method: takes the quota for `method`, then runs `f`.
    ///
    /// `method` is the name of the quota route, not a URL. Take a name from `limits.toml` (for example
    /// `GetOrders`). An unknown name spends the default route `"*"`. `f` receives the connector's
    /// [`AlorHttpClient`] and returns the future of one API call. Do not use this for anything that changes the
    /// account, use [`call_command`](Self::call_command).
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`] when the quota is spent (nothing was sent), or the broker answers HTTP 429.
    /// - [`Error::Rejected`] with code `not_found` for HTTP 404.
    /// - [`Error::Transport`] for everything else: no connection, a timeout, an unreadable answer, any other HTTP
    ///   status. A read can be repeated.
    /// - [`Error::Config`] when the limits file routes the name to a bucket that does not exist, or the broker
    ///   refuses the token.
    ///
    /// The full table is in [`crate::error::map_err`].
    pub async fn call<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(AlorHttpClient) -> Fut,
        Fut: Future<Output = Result<T, AlorApiError>>,
    {
        self.limiter.acquire(method, brokapi_runtime::now())?;
        f(self.http.clone()).await.map_err(map_err)
    }

    /// Calls an API method that changes the account: takes the quota for `method`, then runs `f`.
    ///
    /// Like [`call`](Self::call), but anything that may have reached the broker is [`Error::Unknown`]. The
    /// library never repeats the call: a retry of a lost place request could create a second order. Find out
    /// what happened (read the order, run `reconcile`) before you decide to send it again. The order is not
    /// tracked by the connector (see the module docs).
    ///
    /// # Errors
    ///
    /// - [`Error::Unknown`] when there was no answer, a break or a timeout, HTTP 500 or above, or an answer that
    ///   cannot be read. The command may have been done.
    /// - [`Error::Rejected`] when the broker answered and said no: code `http_<status>` for a status from 400 to
    ///   499 except 429, `not_found` for 404. Nothing was done.
    /// - [`Error::RateLimited`] when the quota is spent (nothing was sent) or the broker answers HTTP 429.
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
    /// use brokapi_alor_protocol::http::models::{InstrumentRef, LimitOrderRequest, UserRef};
    /// use brokapi_alor::limits::default_limits;
    /// use brokapi_alor::{UserDataConfig, UserDataConnector};
    /// use brokapi_conformance::REJECT_PRICE;
    /// use brokapi_core::{Error, ShutdownMode};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_alor_protocol::config::AlorConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
    /// # let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = mpsc::channel(256);
    /// # let limiter = BrokerLimiter::new(&default_limits());
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "alor-ud"), limiter, events_tx).await?;
    ///
    /// let order = LimitOrderRequest {
    ///     side: "buy".into(),
    ///     quantity: 1,
    ///     price: REJECT_PRICE.decimal(),
    ///     instrument: InstrumentRef { symbol: "SBER".into(), exchange: "MOEX".into(), instrument_group: Some("TQBR".into()) },
    ///     user: UserRef { portfolio: "D00001".into() },
    ///     comment: None,
    ///     time_in_force: None,
    ///     allow_margin: None,
    ///     iceberg_fixed: None,
    ///     iceberg_variance: None,
    /// };
    /// let result = ud
    ///     .raw()
    ///     .call_command("PlaceOrder", |http| async move {
    ///         http.create_limit_order("my-own-request-id-1", &order).await
    ///     })
    ///     .await;
    /// assert!(matches!(result, Err(Error::Rejected { .. })));
    /// ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call_command<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(AlorHttpClient) -> Fut,
        Fut: Future<Output = Result<T, AlorApiError>>,
    {
        self.limiter.acquire(method, brokapi_runtime::now())?;
        f(self.http.clone()).await.map_err(map_command_err)
    }

    /// Opens one more session of the data channel `/ws` (for the subscriptions the connectors do not make:
    /// risks, summaries, instruments, ...) with the connector's token.
    ///
    /// The quota name is `ConnectWs`. The session reconnects and resubscribes on its own. Frames arrive on
    /// `event_tx`, parsed. The returned [`WsSession`] connects in the background: it does not wait for the
    /// connection. Read the receiver of `event_tx`, or the session drops frames when the channel is full (see
    /// `WsSession::dropped_events`). The broker allows 10 connections of a user.
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`] when the quota `ConnectWs` is spent. No socket is opened.
    /// - [`Error::Config`] when the limits file routes the name to a bucket that does not exist.
    pub fn connect_ws(
        &self,
        event_tx: AsyncSender<WsIncoming>,
        options: WsOptions,
    ) -> Result<WsSession, Error> {
        self.limiter.acquire("ConnectWs", brokapi_runtime::now())?;
        Ok(WsSession::spawn_with(self.http.clone(), event_tx, options))
    }

    /// Opens the trading channel `/cws` with the connector's token and authorizes it. `answer_timeout` is how
    /// long a command waits for its answer.
    ///
    /// The quota name is `ConnectCws`. The channel gives the order commands that HTTP gives; the connectors use
    /// HTTP. See [`CwsSession`].
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`] when the quota `ConnectCws` is spent.
    /// - [`Error::Transport`] when the connection or the authorization fails.
    pub async fn connect_cws(&self, answer_timeout: Duration) -> Result<CwsSession, Error> {
        self.limiter.acquire("ConnectCws", brokapi_runtime::now())?;
        CwsSession::connect(self.http.clone(), answer_timeout)
            .await
            .map_err(map_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lost_commands_are_unknown_refused_ones_are_not() {
        let http = |status| AlorApiError::Http {
            status,
            message: "x".into(),
        };
        assert!(matches!(map_command_err(http(None)), Error::Unknown));
        assert!(matches!(map_command_err(http(Some(502))), Error::Unknown));
        assert!(matches!(
            map_command_err(http(Some(400))),
            Error::Rejected { .. }
        ));
    }
}
