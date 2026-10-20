//! Enumerations of the BCS API.
//!
//! The broker writes most of its enums as strings of digits (`"1"` for buy) or as upper-case names.
//! These types serialize to exactly that, so you can use them in requests and read them from replies.
//! Enums that read replies have an `Unknown` variant (through `#[serde(other)]`), so a value the
//! broker adds later does not break parsing of a whole frame.

use serde::{Deserialize, Serialize};

/// Instrument type, as the broker names it in its upper-case form.
///
/// This enum covers seven kinds only. The broker's reference data has more (`FOREIGN_STOCK`, `NOTES`,
/// `DEPOSITARY_RECEIPTS`, `EURO_BONDS`, `GOODS`, `INDICES`) and spells bonds `BONDS`, not `BOND`. The
/// request and response models of [`crate::http::models`] therefore keep the instrument type as a
/// string, and this enum is for code that works with the narrow set. Do not rely on it to parse all
/// broker replies.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::InstrumentType;
///
/// let t: InstrumentType = serde_json::from_str("\"MUTUAL_FUNDS\"")?;
/// assert_eq!(t, InstrumentType::MutualFunds);
/// assert_eq!(serde_json::to_string(&InstrumentType::Etf)?, "\"ETF\"");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InstrumentType {
    /// Share (`STOCK`).
    Stock,
    /// Bond (`BOND`). The broker's own name for bonds in reference data is `BONDS`.
    Bond,
    /// Option (`OPTIONS`).
    Options,
    /// Future (`FUTURES`).
    Futures,
    /// Mutual fund (`MUTUAL_FUNDS`).
    MutualFunds,
    /// Currency (`CURRENCY`).
    Currency,
    /// Exchange-traded fund (`ETF`).
    Etf,
}

/// Exchange code, as the broker writes it in the `exchange` fields.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::Exchange;
///
/// let e: Exchange = serde_json::from_str("\"SPB\"")?;
/// assert_eq!(e, Exchange::Spb);
/// assert_eq!(serde_json::to_string(&Exchange::Moex)?, "\"MOEX\"");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Exchange {
    /// Moscow Exchange (`MOEX`).
    Moex,
    /// SPB Exchange (`SPB`).
    #[serde(rename = "SPB")]
    Spb,
}

/// Top-level kind of a portfolio position (the `upperType` field).
///
/// The broker docs call the field "top-level instrument type" and do not list its values. These four
/// are the ones the code knows, not verified against the live API. The portfolio models keep the field
/// as a string for that reason.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::UpperType;
///
/// let t: UpperType = serde_json::from_str("\"FUTURES\"")?;
/// assert_eq!(t, UpperType::Futures);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UpperType {
    /// Russian securities (`RUSSIA`).
    Russia,
    /// Foreign securities (`FOREIGN`).
    Foreign,
    /// Currency (`CURRENCY`).
    Currency,
    /// Futures (`FUTURES`).
    Futures,
}

/// Status of a trading session: `OPEN` or `CLOSE`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::TradingSessionStatus;
///
/// let s: TradingSessionStatus = serde_json::from_str("\"CLOSE\"")?;
/// assert_eq!(s, TradingSessionStatus::Close);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradingSessionStatus {
    /// Trading is open (`OPEN`).
    Open,
    /// Trading is closed (`CLOSE`).
    Close,
}

/// Side of an order: buy or sell. The wire form is the digit as a string.
///
/// `"1"` is buy, `"2"` is sell. Any other text reads as [`OrderSide::Unknown`]. Never send
/// `Unknown` in a request: it would be written as the text `Unknown`, which the broker does not know.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::OrderSide;
///
/// assert_eq!(serde_json::to_string(&OrderSide::Buy)?, "\"1\"");
/// assert_eq!(serde_json::from_str::<OrderSide>("\"2\"")?, OrderSide::Sell);
/// assert_eq!(serde_json::from_str::<OrderSide>("\"7\"")?, OrderSide::Unknown);
/// assert_eq!(OrderSide::Buy.to_string(), "Buy");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    /// Buy (`"1"`).
    #[serde(rename = "1")]
    Buy,
    /// Sell (`"2"`).
    #[serde(rename = "2")]
    Sell,
    /// A value this library does not know. It appears when reading, never use it when sending.
    #[serde(other)]
    Unknown,
}

impl std::fmt::Display for OrderSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderSide::Buy => write!(f, "Buy"),
            OrderSide::Sell => write!(f, "Sell"),
            OrderSide::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Type of an order as it appears in **order events** of the WebSocket (`orderType` of an
/// order message).
///
/// Only market and limit orders are named. The broker has more types (algorithmic, iceberg,
/// conditional, see [`crate::http::models::OrderType`]), and they read here as [`OrderType::Unknown`].
/// The wire form is the digit as a string.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::OrderType;
///
/// assert_eq!(serde_json::from_str::<OrderType>("\"2\"")?, OrderType::Limit);
/// assert_eq!(serde_json::from_str::<OrderType>("\"5\"")?, OrderType::Unknown);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    /// Market order (`"1"`).
    #[serde(rename = "1")]
    Market,
    /// Limit order (`"2"`).
    #[serde(rename = "2")]
    Limit,
    /// Any other type, or a value this library does not know.
    #[serde(other)]
    Unknown,
}
impl std::fmt::Display for OrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderType::Market => write!(f, "Market"),
            OrderType::Limit => write!(f, "Limit"),
            OrderType::Unknown => write!(f, "Unknown"),
        }
    }
}

/// State of an order in an event or a state reply (`orderStatus`). The wire form is a digit string.
///
/// The final states are `Filled`, `Cancelled`, `Replaced` and `Rejected`. The others are in
/// progress. Do not mix this with the status codes of the **order list** (`orders/search`), which are
/// numbers with other meanings, see [`crate::http::models::OrderRecord::order_status`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::OrderStatus;
///
/// assert_eq!(serde_json::from_str::<OrderStatus>("\"8\"")?, OrderStatus::Rejected);
/// assert_eq!(serde_json::from_str::<OrderStatus>("\"77\"")?, OrderStatus::Unknown);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// New, accepted and working (`"0"`).
    #[serde(rename = "0")]
    New,
    /// Partly filled, the rest is still working (`"1"`).
    #[serde(rename = "1")]
    PartiallyFilled,
    /// Fully filled (`"2"`).
    #[serde(rename = "2")]
    Filled,
    /// Cancelled (`"4"`).
    #[serde(rename = "4")]
    Cancelled,
    /// Replaced by a new order, after an edit (`"5"`).
    #[serde(rename = "5")]
    Replaced,
    /// Being cancelled, the cancel is not done yet (`"6"`).
    #[serde(rename = "6")]
    PendingCancel,
    /// Rejected, see the reject reason of the message (`"8"`).
    #[serde(rename = "8")]
    Rejected,
    /// Being replaced, for example while an edit is processed (`"9"`).
    #[serde(rename = "9")]
    PendingReplace,
    /// Waiting for confirmation of the new order (`"10"`).
    #[serde(rename = "10")]
    PendingConfirmation,
    /// A value this library does not know.
    #[serde(other)]
    Unknown,
}

/// Type of an execution report (`executionType`): why the broker sent this message.
///
/// A fill arrives as `PartialFill` or `Fill`, a trade as `Trade`, and `OrderStatus` is a plain status
/// report with no new fill. The wire form is a digit string.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::ExecutionType;
///
/// assert_eq!(serde_json::from_str::<ExecutionType>("\"11\"")?, ExecutionType::Trade);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionType {
    /// The order is new (`"0"`).
    #[serde(rename = "0")]
    New,
    /// Partly filled (`"1"`).
    #[serde(rename = "1")]
    PartialFill,
    /// Fully filled (`"2"`).
    #[serde(rename = "2")]
    Fill,
    /// Cancelled (`"4"`).
    #[serde(rename = "4")]
    Cancelled,
    /// Replaced (`"5"`).
    #[serde(rename = "5")]
    Replaced,
    /// Waiting for cancel (`"6"`).
    #[serde(rename = "6")]
    PendingCancel,
    /// Rejected (`"8"`).
    #[serde(rename = "8")]
    Rejected,
    /// Suspended (`"9"`).
    #[serde(rename = "9")]
    Suspended,
    /// Waiting for confirmation of the new order (`"10"`).
    #[serde(rename = "10")]
    PendingNew,
    /// A trade (`"11"`).
    #[serde(rename = "11")]
    Trade,
    /// A status report (`"12"`).
    #[serde(rename = "12")]
    OrderStatus,
    /// A corrected report (`"13"`).
    #[serde(rename = "13")]
    Corrected,
    /// A value this library does not know.
    #[serde(other)]
    Unknown,
}

/// Kind of a market data subscription command: subscribe or unsubscribe.
///
/// The wire form of the variants is the digit as a string, but the market data requests in
/// [`crate::ws::models::MarketDataSubscribe`] send `subscribeType` as a number, as the broker docs
/// show. This enum is not used by that struct.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::SubscribeType;
///
/// assert_eq!(SubscribeType::Unsubscribe as u8, 1);
/// assert_eq!(serde_json::to_string(&SubscribeType::Subscribe)?, "\"0\"");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscribeType {
    /// Subscribe (`0`).
    #[serde(rename = "0")]
    Subscribe = 0,
    /// Unsubscribe (`1`).
    #[serde(rename = "1")]
    Unsubscribe = 1,
}

/// Candle time frame, in the names of the broker.
///
/// The names are serialized as written (`M1`, `H4`, `D`, `MN`) and are exactly the list of the broker
/// docs: `M1 M5 M15 M30 H1 H4 D W MN`. There is no ten-minute frame at BCS. The request models of this
/// crate take the time frame as a string, so this enum is a convenience: `TimeFrame::D.name()` is the
/// string to send.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::TimeFrame;
///
/// assert_eq!(serde_json::to_string(&TimeFrame::H1)?, "\"H1\"");
/// assert_eq!(TimeFrame::D.name(), "D");
/// // Names the broker does not know are refused when reading.
/// assert!(serde_json::from_str::<TimeFrame>("\"D1\"").is_err());
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeFrame {
    /// One minute.
    M1,
    /// Five minutes.
    M5,
    /// Fifteen minutes.
    M15,
    /// Thirty minutes.
    M30,
    /// One hour.
    H1,
    /// Four hours.
    H4,
    /// One day (`D`).
    D,
    /// One week (`W`).
    W,
    /// One month.
    MN,
}

impl TimeFrame {
    /// The name the broker uses, the one to put into a request.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::M1 => "M1",
            Self::M5 => "M5",
            Self::M15 => "M15",
            Self::M30 => "M30",
            Self::H1 => "H1",
            Self::H4 => "H4",
            Self::D => "D",
            Self::W => "W",
            Self::MN => "MN",
        }
    }
}

/// Side of a trade on the anonymous trades tape (WebSocket `side` as text).
///
/// The tape sends `BUY` and `SELL` as text. The REST tape sends the digits `"1"` and `"2"` instead, as
/// a string in [`crate::http::models::LastTradeRecord::side`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::TradeSide;
///
/// assert_eq!(serde_json::from_str::<TradeSide>("\"SELL\"")?, TradeSide::Sell);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    /// A buy (`BUY`).
    Buy,
    /// A sell (`SELL`).
    Sell,
}

/// Kind of a portfolio record (`type` of a portfolio position), in camelCase.
///
/// The values the code knows are `moneyLimit` and `depoLimit`. The broker docs describe the field as
/// "position type" and list no values, so the portfolio models keep it as a string.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::PortfolioPositionType;
///
/// let t: PortfolioPositionType = serde_json::from_str("\"depoLimit\"")?;
/// assert_eq!(t, PortfolioPositionType::DepoLimit);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PortfolioPositionType {
    /// Money balance (`moneyLimit`).
    MoneyLimit,
    /// Securities position (`depoLimit`).
    DepoLimit,
}
