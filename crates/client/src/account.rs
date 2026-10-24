//! [`Account`]: what a bot sees of one broker account, whatever stands behind it.

use async_trait::async_trait;
use brokapi::core::{
    ExecutionSource, InstrumentId, InstrumentSource, MarketDataSource, OrderBookSource,
    OrderExecutor, PortfolioSource, StopOrderExecutor,
};
use serde_json::Value;

use crate::{Caps, Client, ClientError};

/// One broker account as a bot uses it: the seven traits of `brokapi-core`, plus the three calls that are not in them
/// (what the connection can do, the instrument of a ticker, any method of the broker's API).
///
/// Two types are accounts. [`Client`] holds the connection to the broker itself. `RemoteClient` of `brokapi-gateway`
/// reaches the same account through a gateway. Everything built on this trait, the call by method name
/// ([`call_by_name`](crate::call_by_name)) and the candle history ([`CandlePager`](crate::CandlePager)), therefore
/// works the same on both, and a bot moves from the broker to a gateway without a change in its calls.
///
/// A clone is the same connection.
#[async_trait]
pub trait Account:
    OrderBookSource
    + MarketDataSource
    + InstrumentSource
    + OrderExecutor
    + ExecutionSource
    + PortfolioSource
    + StopOrderExecutor
    + Clone
    + Send
    + Sync
    + 'static
{
    /// Whether the connection was closed by its owner. After that every call fails.
    fn is_closed(&self) -> bool;

    /// What the connection can do. [`ClientError::Closed`] after close.
    async fn account_caps(&self) -> Result<Caps, ClientError>;

    /// The instrument of a ticker (`"SBER"`, or `"SBER@TQBR"`), in the form the broker wants.
    async fn account_instrument(
        &self,
        ticker: &str,
        board: Option<&str>,
    ) -> Result<InstrumentId, ClientError>;

    /// Any method of the broker's API by its name, arguments and reply as JSON. See [`Client::raw`].
    async fn account_raw(
        &self,
        method: &str,
        args: &Value,
        command: bool,
    ) -> Result<Value, ClientError>;
}

#[async_trait]
impl Account for Client {
    fn is_closed(&self) -> bool {
        Client::is_closed(self)
    }

    async fn account_caps(&self) -> Result<Caps, ClientError> {
        self.caps().await.ok_or(ClientError::Closed)
    }

    async fn account_instrument(
        &self,
        ticker: &str,
        board: Option<&str>,
    ) -> Result<InstrumentId, ClientError> {
        self.instrument(ticker, board).await
    }

    async fn account_raw(
        &self,
        method: &str,
        args: &Value,
        command: bool,
    ) -> Result<Value, ClientError> {
        self.raw(method, args, command).await
    }
}
