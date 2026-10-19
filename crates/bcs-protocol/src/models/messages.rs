//! The result of parsing one WebSocket frame.

use crate::ws::models::{
    LimitsData, MarginalIndicatorsData, MarketDataMessage, OrderMessage, WsError, WsPortfolioData,
};

/// One parsed inbound WebSocket frame.
///
/// A session sends these into the channel you gave it. The variant is chosen by the content of the
/// frame first (an error frame, a market data frame with `responseType`) and then by the channel the
/// session is on. A frame that fits nothing arrives as [`IncomingMessage::Unknown`] with its raw
/// text, so no data is lost silently.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::messages::IncomingMessage;
/// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
/// use brokapi_bcs_protocol::ws::parse_incoming;
///
/// let frame = r#"{"responseType":"OrderBook","ticker":"SBER","classCode":"TQBR","bids":[],"asks":[]}"#;
/// match parse_incoming(frame, BcsWsChannel::MarketData) {
///     IncomingMessage::MarketData(_) => {}
///     other => panic!("unexpected: {other:?}"),
/// }
/// ```
// Boxing the large variant would change the public shape of the enum; the frames are short-lived.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum IncomingMessage {
    /// An error frame of the broker (`type = "error"`).
    Error(WsError),
    /// A market data frame: an answer to a subscription, or data.
    MarketData(MarketDataMessage),
    /// A snapshot of limits (limits channel).
    Limits(LimitsData),
    /// A snapshot of the portfolio (portfolio channel).
    Portfolio(WsPortfolioData),
    /// An order message from the execution or events channel.
    OrderExecution(OrderMessage),
    /// An order message from the transaction channel (deprecated by the broker).
    OrderTransaction(OrderMessage),
    /// Margin indicators (margin channel).
    MarginalIndicators(MarginalIndicatorsData),
    /// A frame that could not be parsed into any of the above. It holds the raw text.
    Unknown(String),
    /// Not a frame of the broker: the session has just (re)connected, sent before it restores the
    /// subscriptions. Only when the session was opened with `WsOptions::announce_connects`. The first
    /// connect sends it too. A reader counts them: from the second on, the connection was lost and
    /// everything that came before is not continued by what follows.
    Reconnected,
}
