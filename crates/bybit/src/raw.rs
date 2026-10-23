//! Direct access to any Bybit API method (the "back door"): same key pair, same endpoint, same [`BrokerLimiter`] as the
//! connector it came from.
//!
//! The connectors wrap what the shared traits can express. Everything else (amending an order in place, cancelling all
//! orders, the batch methods, the take profit and stop loss of a position, leverage and margin mode, the transaction
//! log, the demo funds, the other topics of the streams) is reachable here. Get a handle with `raw()` on either
//! connector.
//!
//! Rules of a call:
//!
//! 1. The quota is taken first, by the operation name (`"CancelAll"`), from the route in `limits.toml`. A name without a
//!    route spends the `"*"` default route, so a call is never free. A refusal is [`Error::RateLimited`] and the
//!    request is not sent.
//! 2. The closure gets the connector's [`BybitHttpClient`]: it signs and sends. One closure is one API method.
//! 3. [`call`](BybitRaw::call) is for reads. [`call_command`](BybitRaw::call_command) is for anything that changes the
//!    account: a break, a timeout or a 5xx is [`Error::Unknown`], and it is never retried.
//! 4. [`connect_ws`](BybitRaw::connect_ws) opens one more session of a stream with the same key.
//!
//! The raw layer does not know about the connector's order state: orders placed or amended through it are not tracked
//! (no events of the connector, no `reconcile`). Use the shared traits for orders.
//!
//! # Examples
//!
//! The amendment of an order in place keeps its queue place and its client id. The shared `replace` is not offered
//! (it would have to give the order a new client id), the exchange method is here:
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
//! # let cfg = brokapi_bybit_protocol::config::BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
//! use brokapi_bybit::limits::default_limits;
//! use brokapi_bybit::{UserDataConfig, UserDataConnector};
//! use brokapi_bybit_protocol::http::models::CreateOrderRequest;
//! use brokapi_core::{OrderExecutor, ShutdownMode};
//! use brokapi_runtime::BrokerLimiter;
//!
//! let (tx, _events) = tokio::sync::mpsc::channel(256);
//! let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bybit-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
//! ud.reconcile(&[]).await?;
//! let raw = ud.raw();
//! let order = raw
//!     .call_command("PlaceOrder", |http| async move {
//!         http.create_order(&CreateOrderRequest {
//!             category: "linear".into(),
//!             symbol: "MOCKUSDT".into(),
//!             side: "Buy".into(),
//!             order_type: "Limit".into(),
//!             qty: "10".parse().unwrap(),
//!             price: Some("1".parse().unwrap()),
//!             ..Default::default()
//!         })
//!         .await
//!     })
//!     .await?;
//! assert!(!order.order_id.is_empty());
//! raw.call_command("CancelAll", |http| async move { http.cancel_all("linear", Some("MOCKUSDT"), "USDT").await })
//!     .await?;
//! ud.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;

use brokapi_bybit_protocol::error::BybitApiError;
use brokapi_bybit_protocol::http::client::BybitHttpClient;
use brokapi_bybit_protocol::ws::models::WsIncoming;
use brokapi_bybit_protocol::ws::session::{WsOptions, WsSession, WsStream};
use brokapi_core::Error;
use brokapi_runtime::BrokerLimiter;
use kanal::AsyncSender;

use crate::error::{map_command_err, map_err};

/// A handle to the whole API with the quota of the connector it came from.
#[derive(Clone)]
pub struct BybitRaw {
    http: BybitHttpClient,
    limiter: Arc<BrokerLimiter>,
}

impl BybitRaw {
    pub(crate) fn new(http: BybitHttpClient, limiter: Arc<BrokerLimiter>) -> Self {
        Self { http, limiter }
    }

    /// Runs one read. The quota of `method` is taken first.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`] when the quota is spent (nothing is sent), else the error of the call.
    pub async fn call<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(BybitHttpClient) -> Fut,
        Fut: Future<Output = Result<T, BybitApiError>>,
    {
        self.limiter.acquire(method, brokapi_runtime::now())?;
        f(self.http.clone()).await.map_err(map_err)
    }

    /// Runs one command that changes the account. A lost answer is [`Error::Unknown`], and the call is never repeated.
    ///
    /// # Errors
    ///
    /// As [`BybitRaw::call`], with the unknown outcome of a command.
    pub async fn call_command<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(BybitHttpClient) -> Fut,
        Fut: Future<Output = Result<T, BybitApiError>>,
    {
        self.limiter.acquire(method, brokapi_runtime::now())?;
        f(self.http.clone()).await.map_err(map_command_err)
    }

    /// Opens one more session of a stream with the same key. The frames arrive in `event_tx`.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`] when the quota for connections is spent.
    pub fn connect_ws(
        &self,
        stream: WsStream,
        event_tx: AsyncSender<WsIncoming>,
        options: WsOptions,
    ) -> Result<WsSession, Error> {
        self.limiter.acquire("ConnectWs", brokapi_runtime::now())?;
        Ok(WsSession::spawn(
            self.http.clone(),
            stream,
            event_tx,
            options,
        ))
    }
}
