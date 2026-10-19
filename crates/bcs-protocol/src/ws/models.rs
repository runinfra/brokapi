//! Frames of the WebSocket channels: the commands you send and the data the broker sends.
//!
//! Market data frames are told apart by their `responseType` ([`MarketDataMessage`]). The other
//! channels send a bare object or array, and the channel decides the type. All times from the broker
//! are UTC. Quantities are in pieces.

use crate::models::enums::{ExecutionType, OrderSide, OrderStatus, OrderType};
use crate::models::instrument::InstrumentKey;
use crate::utils::date_format;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ─── common ──────────────────────────────────────────────────────────────────

/// An error frame of the broker (`"type": "error"`), the server to client direction.
///
/// A frame like this is parsed into [`IncomingMessage::Error`](crate::models::messages::IncomingMessage::Error).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::ws::models::WsError;
///
/// let e: WsError = serde_json::from_str(r#"{"type":"error","code":"UNAUTHORIZED","message":"no token"}"#)?;
/// assert_eq!(e.code, "UNAUTHORIZED");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct WsError {
    /// Kind of the frame (JSON field `type`), `error` for this type.
    #[serde(rename = "type")]
    pub msg_type: String,
    /// Machine code of the error.
    pub code: String,
    /// Text of the error.
    pub message: String,
}

/// The two fields of a frame that the parser looks at first to decide what kind of frame it is.
///
/// Every other field is ignored here. The parser then reads the whole frame into the matching type.
#[derive(Debug, Clone, Deserialize)]
pub struct WsRawMessage {
    /// The `type` of the frame, if there is one. `error` marks an error frame.
    #[serde(default, rename = "type")]
    pub msg_type: Option<String>,
    /// The `responseType` of the frame, if there is one. Market data frames have it.
    #[serde(default, rename = "responseType")]
    pub response_type: Option<String>,
}

// ─── subscription ─────────────────────────────────────────────────────────────

/// A subscribe or unsubscribe command for the market data channel (client to server).
///
/// Build it with the constructors: [`MarketDataSubscribe::quotes`], [`MarketDataSubscribe::order_book`],
/// [`MarketDataSubscribe::last_trades`], [`MarketDataSubscribe::last_candle`] and the `unsubscribe_*` forms.
/// `subscribe_type` and `data_type` are numbers on the wire, as in the broker docs. The broker allows
/// 100 instruments per connection and 10 client messages per second.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
/// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
///
/// let sub = MarketDataSubscribe::order_book(vec![InstrumentKey::new("SBER", "TQBR")], 20);
/// assert_eq!(
///     serde_json::to_value(&sub)?,
///     serde_json::json!({
///         "subscribeType": 0,
///         "dataType": 0,
///         "instruments": [{ "ticker": "SBER", "classCode": "TQBR" }],
///         "depth": 20
///     })
/// );
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketDataSubscribe {
    /// `0` subscribe, `1` unsubscribe.
    pub subscribe_type: u8,
    /// What to subscribe to: `0` order book, `1` last candle, `2` anonymous trades, `3` quotes.
    pub data_type: u8,
    /// The instruments the command is about.
    pub instruments: Vec<InstrumentKey>,
    /// Order book depth, 1 to 20 by the broker docs (default 20). Sent only for the order book. Not checked here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
    /// Candle time frame: `M1 M5 M15 M30 H1 H4 D W MN`. Sent only for the last candle. Not checked here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_frame: Option<String>,
}

impl MarketDataSubscribe {
    /// Turns a subscribe command into the matching unsubscribe: the same data type, instruments, depth
    /// and time frame, with `subscribe_type` set to `1`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let sub = MarketDataSubscribe::order_book(vec![InstrumentKey::new("SBER", "TQBR")], 10);
    /// let unsub = sub.clone().into_unsubscribe();
    /// assert_eq!((sub.subscribe_type, unsub.subscribe_type), (0, 1));
    /// assert_eq!(unsub.depth, Some(10));
    /// ```
    pub fn into_unsubscribe(self) -> Self {
        Self {
            subscribe_type: 1,
            ..self
        }
    }

    /// Subscribes to quotes (`dataType` 3).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let sub = MarketDataSubscribe::quotes(vec![InstrumentKey::new("SBER", "TQBR")]);
    /// assert_eq!((sub.subscribe_type, sub.data_type), (0, 3));
    /// ```
    pub fn quotes(instruments: Vec<InstrumentKey>) -> Self {
        Self {
            subscribe_type: 0,
            data_type: 3,
            instruments,
            depth: None,
            time_frame: None,
        }
    }

    /// Unsubscribes from quotes (`dataType` 3, `subscribeType` 1).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let cmd = MarketDataSubscribe::unsubscribe_quotes(vec![InstrumentKey::new("SBER", "TQBR")]);
    /// assert_eq!((cmd.subscribe_type, cmd.data_type), (1, 3));
    /// ```
    pub fn unsubscribe_quotes(instruments: Vec<InstrumentKey>) -> Self {
        Self {
            subscribe_type: 1,
            data_type: 3,
            instruments,
            depth: None,
            time_frame: None,
        }
    }

    /// Subscribes to the order book (`dataType` 0) with `depth` levels per side.
    ///
    /// The broker docs allow 1 to 20 and default to 20. The depth is not checked here. The broker
    /// computes the volumes in the answers over the levels within `depth`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let sub = MarketDataSubscribe::order_book(vec![InstrumentKey::new("SBER", "TQBR")], 5);
    /// assert_eq!((sub.data_type, sub.depth), (0, Some(5)));
    /// ```
    pub fn order_book(instruments: Vec<InstrumentKey>, depth: u32) -> Self {
        Self {
            subscribe_type: 0,
            data_type: 0,
            instruments,
            depth: Some(depth),
            time_frame: None,
        }
    }

    /// Unsubscribes from the order book (`dataType` 0, `subscribeType` 1). Pass the same `depth` as
    /// at subscribe if you compare it with the restore list of a session.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let cmd = MarketDataSubscribe::unsubscribe_order_book(vec![InstrumentKey::new("SBER", "TQBR")], 5);
    /// assert_eq!((cmd.subscribe_type, cmd.data_type), (1, 0));
    /// ```
    pub fn unsubscribe_order_book(instruments: Vec<InstrumentKey>, depth: u32) -> Self {
        Self {
            subscribe_type: 1,
            data_type: 0,
            instruments,
            depth: Some(depth),
            time_frame: None,
        }
    }

    /// Subscribes to the anonymous trades tape (`dataType` 2).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let sub = MarketDataSubscribe::last_trades(vec![InstrumentKey::new("SBER", "TQBR")]);
    /// assert_eq!(sub.data_type, 2);
    /// ```
    pub fn last_trades(instruments: Vec<InstrumentKey>) -> Self {
        Self {
            subscribe_type: 0,
            data_type: 2,
            instruments,
            depth: None,
            time_frame: None,
        }
    }

    /// Unsubscribes from the anonymous trades tape (`dataType` 2, `subscribeType` 1).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let cmd = MarketDataSubscribe::unsubscribe_last_trades(vec![InstrumentKey::new("SBER", "TQBR")]);
    /// assert_eq!((cmd.subscribe_type, cmd.data_type), (1, 2));
    /// ```
    pub fn unsubscribe_last_trades(instruments: Vec<InstrumentKey>) -> Self {
        Self {
            subscribe_type: 1,
            data_type: 2,
            instruments,
            depth: None,
            time_frame: None,
        }
    }

    /// Subscribes to the last candle (`dataType` 1) for a time frame.
    ///
    /// `time_frame` is one of `M1 M5 M15 M30 H1 H4 D W MN` (broker docs). It is not checked here.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let sub = MarketDataSubscribe::last_candle(vec![InstrumentKey::new("SBER", "TQBR")], "M5");
    /// assert_eq!((sub.data_type, sub.time_frame.as_deref()), (1, Some("M5")));
    /// ```
    pub fn last_candle(instruments: Vec<InstrumentKey>, time_frame: &str) -> Self {
        Self {
            subscribe_type: 0,
            data_type: 1,
            instruments,
            depth: None,
            time_frame: Some(time_frame.to_string()),
        }
    }

    /// Unsubscribes from the last candle (`dataType` 1, `subscribeType` 1) for a time frame.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    ///
    /// let cmd = MarketDataSubscribe::unsubscribe_last_candle(vec![InstrumentKey::new("SBER", "TQBR")], "M5");
    /// assert_eq!(cmd.subscribe_type, 1);
    /// ```
    pub fn unsubscribe_last_candle(instruments: Vec<InstrumentKey>, time_frame: &str) -> Self {
        Self {
            subscribe_type: 1,
            data_type: 1,
            instruments,
            depth: None,
            time_frame: Some(time_frame.to_string()),
        }
    }
}

/// A subscription confirmation in the flat form (server to client).
///
/// The parser does not produce this type: confirmations inside [`MarketDataMessage`] use
/// [`SubscribeSuccessPayload`]. It is kept for code that parses a frame itself.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeSuccess {
    /// Kind of the confirmation, for example `OrderBookSuccess`.
    pub response_type: String,
    /// The instruments the subscription covers, if the broker lists them.
    #[serde(default)]
    pub instruments: Vec<InstrumentKey>,
    /// Order book depth, for an order book subscription.
    #[serde(default)]
    pub depth: Option<u32>,
    /// Candle time frame, for a candle subscription.
    #[serde(default)]
    pub time_frame: Option<String>,
}

// ─── outgoing ─────────────────────────────────────────────────────────────────

/// A command a session sends to the broker. It serializes as the plain command, without a wrapper.
///
/// The two variants hold the same type: the difference is only the purpose, and the session turns an
/// unsubscribe into a command with `subscribeType = 1`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
/// use brokapi_bcs_protocol::ws::models::{MarketDataSubscribe, WsOutgoing};
///
/// let keys = vec![InstrumentKey::new("SBER", "TQBR")];
/// let msg = WsOutgoing::Subscribe(MarketDataSubscribe::quotes(keys));
/// let json = serde_json::to_value(&msg)?;
/// assert_eq!(json["dataType"], 3);
/// assert_eq!(json["subscribeType"], 0);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum WsOutgoing {
    /// A subscribe command.
    Subscribe(MarketDataSubscribe),
    /// An unsubscribe command.
    Unsubscribe(MarketDataSubscribe),
}

// ─── market_data ──────────────────────────────────────────────────────────────

/// A frame of the market data channel, told apart by its `responseType`.
///
/// The success variants confirm a subscription. The data variants carry the data. A value of
/// `responseType` this library does not know reads as [`MarketDataMessage::Undefined`].
///
/// **Error frames of the documented shape do not arrive as `ErrorResponse`.** The docs show an error as
/// a frame with the `responseType` of the data (for example `OrderBook`) and an `errors` list. Such a frame
/// does not fit the data type, so the parser returns it as
/// [`IncomingMessage::Unknown`](crate::models::messages::IncomingMessage::Unknown) with the raw text. The
/// `ErrorResponse` variant is for a frame that really has `responseType = "ErrorResponse"`.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::ws::models::MarketDataMessage;
///
/// let frame = r#"{"responseType":"LastTrades","ticker":"SBER","classCode":"TQBR","side":"BUY","price":300.15,"quantity":100}"#;
/// match serde_json::from_str::<MarketDataMessage>(frame)? {
///     MarketDataMessage::LastTrades(t) => assert_eq!(t.quantity, dec!(100.0)),
///     other => panic!("unexpected: {other:?}"),
/// }
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "responseType")]
pub enum MarketDataMessage {
    /// The subscription to quotes was accepted.
    QuotesSuccess(SubscribeSuccessPayload),
    /// A quote update.
    Quotes(QuotesData),
    /// The subscription to the order book was accepted.
    OrderBookSuccess(SubscribeSuccessPayload),
    /// An order book update.
    OrderBook(OrderBookData),
    /// The subscription to the trades tape was accepted.
    LastTradesSuccess(SubscribeSuccessPayload),
    /// One trade of the tape.
    LastTrades(LastTradesData),
    /// The subscription to the last candle was accepted (`responseType` is `CandleStickSuccess`).
    #[serde(rename = "CandleStickSuccess")]
    LastCandleSuccess(SubscribeSuccessPayload),
    /// An update of the last candle (`responseType` is `CandleStick`).
    #[serde(rename = "CandleStick")]
    LastCandle(LastCandleData),
    /// An error frame with `responseType = "ErrorResponse"`. See the note on the type.
    ErrorResponse(MarketDataError),
    /// A `responseType` this library does not know.
    #[serde(other)]
    Undefined,
}

/// The body of a subscription confirmation.
///
/// The docs show the instrument as flat `ticker` and `classCode` fields in the confirmation. This type
/// reads an `instruments` list. If the broker sends the flat form, `instruments` stays empty. Not verified
/// against the live API which form the broker uses.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeSuccessPayload {
    /// The instruments the subscription covers, if the broker lists them. Empty otherwise.
    #[serde(default)]
    pub instruments: Vec<InstrumentKey>,
    /// Order book depth, for an order book subscription.
    #[serde(default)]
    pub depth: Option<u32>,
    /// Candle time frame, for a candle subscription.
    #[serde(default)]
    pub time_frame: Option<String>,
}

/// A quote update of one instrument.
///
/// The current docs name the best sell price `offer` and the last price `last`. Older samples used `ask`
/// and `lastPrice`. Both spellings are read into [`QuotesData::ask`] and [`QuotesData::last_price`].
/// The docs give no quantity at the best prices.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::ws::models::QuotesData;
///
/// // The current docs form.
/// let q: QuotesData = serde_json::from_str(
///     r#"{"ticker":"SBER","classCode":"TQBR","bid":305.86,"offer":305.96,"last":305.97}"#,
/// )?;
/// assert_eq!((q.bid, q.ask, q.last_price), (Some(dec!(305.86)), Some(dec!(305.96)), Some(dec!(305.97))));
///
/// // The older form.
/// let old: QuotesData = serde_json::from_str(r#"{"ticker":"SBER","classCode":"TQBR","ask":305.96,"lastPrice":305.97}"#)?;
/// assert_eq!(old.ask, Some(dec!(305.96)));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotesData {
    /// Ticker.
    pub ticker: String,
    /// Class code (board).
    pub class_code: String,
    /// Time the data was updated (UTC), kept as the text the broker sends.
    #[serde(default)]
    pub date_time: Option<String>,
    /// Best buy price.
    #[serde(default)]
    pub bid: Option<Decimal>,
    /// Best sell price. The broker docs call the field `offer`, older samples `ask`. Both are read.
    #[serde(default, alias = "offer")]
    pub ask: Option<Decimal>,
    /// Price of the last trade. The broker docs call the field `last`, older samples `lastPrice`. Both are read.
    #[serde(default, alias = "last")]
    pub last_price: Option<Decimal>,
    /// Opening price of the session.
    #[serde(default)]
    pub open: Option<Decimal>,
    /// Closing price of the previous session.
    #[serde(default)]
    pub close: Option<Decimal>,
    /// Highest price of the day.
    #[serde(default)]
    pub high: Option<Decimal>,
    /// Lowest price of the day.
    #[serde(default)]
    pub low: Option<Decimal>,
    /// Price change over the session, in the price currency.
    #[serde(default)]
    pub change: Option<Decimal>,
    /// Price change over the session, in percent.
    #[serde(default)]
    pub change_rate: Option<f64>,
    /// Traded value. Not in the current docs, read if the broker sends it.
    #[serde(default)]
    pub value: Option<Decimal>,
    /// Traded volume. Not in the current docs, read if the broker sends it.
    #[serde(default)]
    pub volume: Option<Decimal>,
    /// Number of trades. Not in the current docs, read if the broker sends it.
    #[serde(default)]
    pub trade_num: Option<u64>,
}

/// One price level of the order book: a price and the quantity at it.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderBookLevel {
    /// Price of the level.
    pub price: Decimal,
    /// Quantity at the level, in pieces.
    pub quantity: Decimal,
}

/// An order book update. It is a full snapshot of the levels within `depth`, not a delta.
///
/// `bids` and `asks` come best price first, as the broker sends them. The code does not sort them.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::ws::models::OrderBookData;
///
/// let book: OrderBookData = serde_json::from_str(
///     r#"{"ticker":"SBER","classCode":"TQBR","depth":2,
///         "bids":[{"price":244.30,"quantity":100}],"asks":[{"price":244.35,"quantity":120}]}"#,
/// )?;
/// assert_eq!(book.bids[0].price, dec!(244.30));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookData {
    /// Ticker.
    pub ticker: String,
    /// Class code (board).
    pub class_code: String,
    /// Depth of the book.
    #[serde(default)]
    pub depth: Option<u32>,
    /// Time the book was updated (UTC), kept as the text the broker sends.
    #[serde(default)]
    pub date_time: Option<String>,
    /// Buy side, best price first.
    #[serde(default)]
    pub bids: Vec<OrderBookLevel>,
    /// Sell side, best price first.
    #[serde(default)]
    pub asks: Vec<OrderBookLevel>,
}

/// One trade of the anonymous trades tape: one trade per frame.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::ws::models::LastTradesData;
///
/// let t: LastTradesData = serde_json::from_str(
///     r#"{"ticker":"SBER","classCode":"TQBR","side":"SELL","volume":30015.0,"price":300.15,"quantity":100}"#,
/// )?;
/// assert_eq!(t.side.as_deref(), Some("SELL"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastTradesData {
    /// Ticker.
    pub ticker: String,
    /// Class code (board).
    pub class_code: String,
    /// Side of the trade as text: `BUY` or `SELL` (the REST tape uses `1` and `2`).
    #[serde(default)]
    pub side: Option<String>,
    /// Trade volume in the instrument currency.
    #[serde(default)]
    pub volume: Option<Decimal>,
    /// Trade price.
    pub price: Decimal,
    /// Quantity in pieces.
    pub quantity: Decimal,
    /// Time of the trade (UTC), kept as the text the broker sends.
    #[serde(default)]
    pub date_time: Option<String>,
}

/// An update of the last candle for a time frame.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::ws::models::LastCandleData;
///
/// let c: LastCandleData = serde_json::from_str(
///     r#"{"ticker":"SBER","classCode":"TQBR","timeFrame":"M1","open":244.2,"close":244.5,
///         "high":244.7,"low":243.9,"volume":3200,"dateTime":"2024-11-10T10:30:00.000Z"}"#,
/// )?;
/// assert_eq!(c.close, Some(dec!(244.5)));
/// assert_eq!(c.timestamp.as_deref(), Some("2024-11-10T10:30:00.000Z"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCandleData {
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(default)]
    pub class_code: Option<String>,
    /// Time frame of the candle: `M1 M5 M15 M30 H1 H4 D W MN`.
    #[serde(default)]
    pub time_frame: Option<String>,
    /// Time of the candle (UTC), kept as text (JSON field `dateTime`).
    #[serde(default, rename = "dateTime")]
    pub timestamp: Option<String>,
    /// Opening price.
    #[serde(default)]
    pub open: Option<Decimal>,
    /// Closing price.
    #[serde(default)]
    pub close: Option<Decimal>,
    /// Highest price.
    #[serde(default)]
    pub high: Option<Decimal>,
    /// Lowest price.
    #[serde(default)]
    pub low: Option<Decimal>,
    /// Traded volume of the candle, in the currency.
    #[serde(default)]
    pub volume: Option<Decimal>,
}

/// The error part of a market data frame.
///
/// Either `error` (one) or `errors` (a list) can be set. The broker docs show the list.
#[derive(Debug, Clone, Deserialize)]
pub struct MarketDataError {
    /// A single error, if the frame has one.
    #[serde(default)]
    pub error: Option<MarketDataErrorDetail>,
    /// The errors of the frame. Empty if there are none.
    #[serde(default)]
    pub errors: Vec<MarketDataErrorDetail>,
}

/// One error of a market data frame.
///
/// The docs list these codes: `NO_DATE` (no data), `NOT_FOUND` (instrument not found), `INCORRECT_JSON`
/// (invalid JSON), `BAD_REQUEST` (execution error), `UNAUTHORIZED` (client not authorized).
#[derive(Debug, Clone, Deserialize)]
pub struct MarketDataErrorDetail {
    /// Error code, see the type docs.
    pub code: String,
    /// Text of the error.
    pub message: String,
}

// ─── orders ───────────────────────────────────────────────────────────────────

fn unknown_side() -> OrderSide {
    OrderSide::Unknown
}

/// A message about an order from the order channels.
///
/// It comes from `orders/events/ws` (the current channel) and from the deprecated execution and transaction
/// channels. It carries the report in [`OrderMessage::data`].
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::models::enums::{ExecutionType, OrderStatus};
/// use brokapi_bcs_protocol::ws::models::OrderMessage;
///
/// let m: OrderMessage = serde_json::from_str(
///     r#"{"originalClientOrderId":"o-1","clientOrderId":"",
///         "data":{"orderStatus":"1","executionType":"1","ticker":"SBER","classCode":"TQBR",
///                 "side":"1","orderType":"2","orderQuantity":10,"executedQuantity":4,
///                 "lastQuantity":4,"remainedQuantity":6,"price":300.0}}"#,
/// )?;
/// let data = m.data.unwrap();
/// assert_eq!(data.order_status, OrderStatus::PartiallyFilled);
/// assert_eq!(data.execution_type, Some(ExecutionType::PartialFill));
/// assert_eq!(data.last_quantity, dec!(4.0));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderMessage {
    /// The `clientOrderId` you gave when you placed the order. For an edit it is the id of the changed order, as the broker docs word it.
    #[serde(default)]
    pub original_client_order_id: Option<String>,
    /// Id of the request that caused the message. Empty text if the broker sent none.
    #[serde(default)]
    pub client_order_id: String,
    /// The report. `None` if the message has none.
    #[serde(default)]
    pub data: Option<OrderExecutionData>,
}

/// The report inside an [`OrderMessage`]: the state of the order and the last fill.
///
/// It is the WebSocket form of [`crate::http::models::OrderExecutionData`], with typed enums. Quantities
/// are in pieces. Fields the broker leaves out in some messages read as `0` or `None`, noted per field.
///
/// * `execution_type` says why the message was sent. A fill has `last_quantity` above 0 and a trade
///   id in `execution_id`.
/// * This struct has no field for a reject reason. The REST report has `rejectReason`, but the events
///   docs do not list it among the fields of `data`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderExecutionData {
    /// Type of the message, as text.
    #[serde(default)]
    pub message_type: Option<String>,
    /// State of the order. Required: a message without it does not parse.
    pub order_status: OrderStatus,
    /// Why the message was sent.
    #[serde(default)]
    pub execution_type: Option<ExecutionType>,
    /// Quantity of the order, in pieces.
    #[serde(default)]
    pub order_quantity: Option<Decimal>,
    /// Executed quantity so far, in pieces.
    #[serde(default)]
    pub executed_quantity: Option<Decimal>,
    /// Absent in pure status messages of the current `orders/events/ws` channel (no fill): `0`.
    #[serde(default)]
    pub last_quantity: Decimal,
    /// Remaining quantity, in pieces. `0` if the message has none.
    #[serde(default)]
    pub remained_quantity: Decimal,
    /// Ticker. Required.
    pub ticker: String,
    /// Class code (board). Empty text if absent.
    #[serde(default)]
    pub class_code: String,
    /// Buy or sell. [`OrderSide::Unknown`] if absent.
    #[serde(default = "unknown_side")]
    pub side: OrderSide,
    /// Type of the order. Types other than market and limit read as [`OrderType::Unknown`].
    #[serde(default)]
    pub order_type: Option<OrderType>,
    /// Average execution price. For repo and spread deals, the price of the first part.
    #[serde(default)]
    pub average_price: Option<Decimal>,
    /// Id of the order at the exchange.
    #[serde(default)]
    pub order_id: Option<String>,
    /// Id of the trade (fill). Set when the message reports a fill.
    #[serde(default)]
    pub execution_id: Option<String>,
    /// Absent in some cancel / reject messages: `0`.
    #[serde(default)]
    pub price: Decimal,
    /// Currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Client code.
    #[serde(default)]
    pub client_code: Option<String>,
    /// Time of the transaction (UTC). `None` if absent or the epoch.
    #[serde(default, with = "date_format::option")]
    pub transaction_time: Option<DateTime<Utc>>,
    /// Trade date, kept as text. For the evening session of the derivatives market it is the date of the next session.
    #[serde(default)]
    pub trade_date: Option<String>,
    /// Order number.
    #[serde(default)]
    pub order_number: Option<String>,
    /// Accrued coupon income.
    #[serde(default)]
    pub accrued_coupon: Option<Decimal>,
    /// Volume of the trade.
    #[serde(default)]
    pub execution_value: Option<Decimal>,
    /// Commission.
    #[serde(default)]
    pub commission: Option<Decimal>,
    /// Exchange id.
    #[serde(default)]
    pub security_exchange: Option<String>,
}

// ─── portfolio ────────────────────────────────────────────────────────────────

/// A portfolio frame: an array of positions, the same records as the REST portfolio.
pub type WsPortfolioData = Vec<WsPortfolioPosition>;

/// One position or balance of a portfolio frame.
///
/// The same fields as [`crate::http::models::PortfolioPosition`], without the deprecated ones the docs
/// mark (sub-account, agreement, logo). All but `type` are optional.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::ws::models::WsPortfolioPosition;
///
/// let p: WsPortfolioPosition = serde_json::from_str(
///     r#"{"type":"depoLimit","ticker":"SBER","quantity":10,"unrealizedPL":40.0}"#,
/// )?;
/// assert_eq!(p.unrealized_pl, Some(dec!(40.0)));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsPortfolioPosition {
    /// Kind of the record (JSON field `type`), for example `depoLimit` for a securities position and `moneyLimit` for a currency balance.
    #[serde(rename = "type")]
    pub position_type: String,
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Name of the instrument for display.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Ticker of the base asset, for derivatives.
    #[serde(default)]
    pub base_asset_ticker: Option<String>,
    /// Price currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Trading account.
    #[serde(default)]
    pub account: Option<String>,
    /// Top-level instrument type, as text. The broker docs list no values.
    #[serde(default)]
    pub upper_type: Option<String>,
    /// Instrument type, as text.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Settlement term code.
    #[serde(default)]
    pub term: Option<String>,
    /// Quantity in pieces.
    #[serde(default)]
    pub quantity: Option<Decimal>,
    /// Quantity held by active orders or by collateral (initial margin).
    #[serde(default)]
    pub locked: Option<Decimal>,
    /// Balance price (the price the position was opened at), in the price currency.
    #[serde(default)]
    pub balance_price: Option<Decimal>,
    /// Current price.
    #[serde(default)]
    pub current_price: Option<Decimal>,
    /// Balance value (at the opening price), in the price currency.
    #[serde(default)]
    pub balance_value: Option<Decimal>,
    /// Balance value in roubles.
    #[serde(default)]
    pub balance_value_rub: Option<Decimal>,
    /// Balance value in dollars.
    #[serde(default)]
    pub balance_value_usd: Option<Decimal>,
    /// Balance value in euros.
    #[serde(default)]
    pub balance_value_eur: Option<Decimal>,
    /// Current value in the price currency.
    #[serde(default)]
    pub current_value: Option<Decimal>,
    /// Current value in roubles.
    #[serde(default)]
    pub current_value_rub: Option<Decimal>,
    /// Current value in dollars.
    #[serde(default)]
    pub current_value_usd: Option<Decimal>,
    /// Current value in euros.
    #[serde(default)]
    pub current_value_eur: Option<Decimal>,
    /// Price change since the position was opened (JSON field `unrealizedPL`, with capital letters).
    #[serde(default, rename = "unrealizedPL", alias = "unrealizedPl")]
    pub unrealized_pl: Option<Decimal>,
    /// The same change in percent (JSON field `unrealizedPercentPL`).
    #[serde(default, rename = "unrealizedPercentPL", alias = "unrealizedPercentPl")]
    pub unrealized_percent_pl: Option<f64>,
    /// Price change over the day (JSON field `dailyPL`). The broker docs word it as "daily change of the price since the position was opened".
    #[serde(default, rename = "dailyPL", alias = "dailyPl")]
    pub daily_pl: Option<Decimal>,
    /// The same change in percent (JSON field `dailyPercentPL`).
    #[serde(default, rename = "dailyPercentPL", alias = "dailyPercentPl")]
    pub daily_percent_pl: Option<f64>,
    /// Share of the portfolio in percent, of its value in roubles.
    #[serde(default)]
    pub portfolio_share: Option<f64>,
    /// Number of decimal places of the price.
    #[serde(default)]
    pub scale: Option<u32>,
    /// Minimum price step.
    #[serde(default)]
    pub minimum_step: Option<Decimal>,
    /// Class code (board) of the instrument.
    #[serde(default)]
    pub board: Option<String>,
    /// Unit of the price.
    #[serde(default)]
    pub price_unit: Option<String>,
    /// Face value, for bonds.
    #[serde(default)]
    pub face_value: Option<Decimal>,
    /// Accrued coupon income, for bonds.
    #[serde(default)]
    pub accrued_income: Option<Decimal>,
    /// `true` if the class is blocked.
    #[serde(default)]
    pub is_blocked: Option<bool>,
    /// `true` if the trading account is blocked.
    #[serde(default)]
    pub is_blocked_trade_account: Option<bool>,
    /// Amount reserved as collateral (initial margin) on futures.
    #[serde(default)]
    pub locked_for_futures: Option<Decimal>,
    /// Quantity in a lot.
    #[serde(default)]
    pub ratio_quantity: Option<Decimal>,
    /// Expiration date, kept as text.
    #[serde(default)]
    pub expire_date: Option<String>,
}

// ─── limits ───────────────────────────────────────────────────────────────────

/// A limits frame: one object with the current limits of the account.
///
/// The four lists are optional, because the broker may leave out a list that is empty.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::ws::models::LimitsData;
///
/// let l: LimitsData = serde_json::from_str(
///     r#"{"moneyLimits":[{"currencyCode":"RUB","locked":10.0,"quantity":{"type":"T0","value":500.0}}]}"#,
/// )?;
/// assert!(l.depo_limit.is_none());
/// assert_eq!(l.money_limits.unwrap()[0].quantity.as_ref().unwrap().value, dec!(500.0));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitsData {
    /// Securities positions.
    #[serde(default)]
    pub depo_limit: Option<Vec<DepoLimitEntry>>,
    /// Futures positions.
    #[serde(default)]
    pub future_holding: Option<Vec<FutureHoldingEntry>>,
    /// Money balances.
    #[serde(default)]
    pub money_limits: Option<Vec<MoneyLimitEntry>>,
    /// Limits of the derivatives market.
    #[serde(default)]
    pub futures_limits: Option<Vec<FuturesLimitEntry>>,
}

/// A securities position in a limits frame (`depoLimit`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepoLimitEntry {
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(default)]
    pub class_code: Option<String>,
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Weighted average price at which the position was opened.
    #[serde(default)]
    pub average_price: Option<Decimal>,
    /// Quantity in pieces, with the settlement mode.
    #[serde(default)]
    pub quantity: Option<Quantity>,
    /// Quantity in lots, with the settlement mode.
    #[serde(default)]
    pub quantity_batch: Option<Quantity>,
    /// Instrument type as the broker names it.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Time the broker updated the data, kept as text.
    #[serde(default)]
    pub load_date: Option<String>,
    /// Money held by active buy orders for this security.
    #[serde(default)]
    pub locked_buy_value: Option<Decimal>,
    /// Money held by active sell orders for this security. The docs word it the same way as for buy.
    #[serde(default)]
    pub locked_sell_value: Option<Decimal>,
}

/// A money balance in a limits frame (`moneyLimits`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoneyLimitEntry {
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Currency code (JSON field `currencyCode`).
    #[serde(default, rename = "currencyCode")]
    pub currency: Option<String>,
    /// Money held by active orders.
    #[serde(default)]
    pub locked: Option<Decimal>,
    /// Weighted average opening price.
    #[serde(default)]
    pub average_price: Option<Decimal>,
    /// Instrument type as the broker names it.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Amount of money, with the settlement mode.
    #[serde(default)]
    pub quantity: Option<Quantity>,
    /// Time the broker updated the data, kept as text.
    #[serde(default)]
    pub load_date: Option<String>,
}

/// A futures position in a limits frame (`futureHolding`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FutureHoldingEntry {
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(default)]
    pub class_code: Option<String>,
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Estimated value of the planned positions after the nearest clearing.
    #[serde(default)]
    pub cbpl_planned: Option<Decimal>,
    /// Estimate of the variation margin on the positions.
    #[serde(default)]
    pub var_margin: Option<Decimal>,
    /// Value of the derivatives market positions.
    #[serde(default)]
    pub position_value: Option<Decimal>,
    /// Current net positions.
    #[serde(default)]
    pub total_net: Option<Decimal>,
    /// Execution date, kept as text.
    #[serde(default)]
    pub execution_date: Option<String>,
    /// Total variation margin of the main clearing, credited on all positions.
    #[serde(default)]
    pub total_var_margin: Option<Decimal>,
    /// Variation margin credited during clearing.
    #[serde(default)]
    pub real_var_margin: Option<Decimal>,
    /// Weighted average opening price.
    #[serde(default)]
    pub average_price: Option<Decimal>,
    /// Instrument type as the broker names it.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Date of the trading session, kept as text.
    #[serde(default)]
    pub trade_date: Option<String>,
}

/// A limit of the derivatives market in a limits frame (`futuresLimits`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesLimitEntry {
    /// Currency code (JSON field `currencyCode`).
    #[serde(default, rename = "currencyCode")]
    pub currency: Option<String>,
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Variation margin for the current day (the JSON field is spelled `accruedint`).
    #[serde(default, rename = "accruedint")]
    pub accrued_int: Option<Decimal>,
    /// Current limit of open positions.
    #[serde(default)]
    pub cbp_limit: Option<Decimal>,
    /// Positions after the last clearing.
    #[serde(default)]
    pub cbpl_used: Option<Decimal>,
    /// Estimated value of the planned positions after the nearest clearing.
    #[serde(default)]
    pub cbpl_planned: Option<Decimal>,
    /// Collateral (initial margin) reserved for the client orders.
    #[serde(default)]
    pub cbpl_used_for_orders: Option<Decimal>,
    /// Collateral (initial margin) reserved for the open client positions.
    #[serde(default)]
    pub cbpl_used_for_positions: Option<Decimal>,
    /// Premiums on options.
    #[serde(default)]
    pub options_premium: Option<Decimal>,
    /// Instrument type as the broker names it.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Time the broker updated the data, kept as text.
    #[serde(default)]
    pub load_date: Option<String>,
    /// Estimate of the variation margin on the positions.
    #[serde(default)]
    pub var_margin: Option<Decimal>,
    /// Variation margin credited during clearing.
    #[serde(default)]
    pub real_var_margin: Option<Decimal>,
}

/// An amount with its settlement mode (`T0`, `T1`, `T365` and so on), in limits frames.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Quantity {
    /// Settlement mode, such as `T0` (JSON field `type`).
    #[serde(rename = "type")]
    pub quantity_type: String,
    /// The amount: pieces, lots or money, depending on the field that holds it.
    pub value: Decimal,
}

// ─── marginal_indicators ──────────────────────────────────────────────────────

/// A margin indicators frame: the margin picture of the account in one object.
///
/// The broker docs describe nine parts. Every part is optional here. **Some parts of this library do not
/// match the docs:** the docs name the fields of `marginParameters` `marginValue`, `collateral`,
/// `marginCall` and `forcedClosure`, and give `futuresParameters` as `futuresValue` and a
/// `futuresPortfolio` list. [`MarginParameters`] and [`FuturesParameters`] read other names, so they stay
/// `None` against a broker that follows the docs. The docs list `fortsMarginCall`, `fortsForcedClosure`
/// and `riskLevel` for the derivatives stability, which [`FortsStability`] does not read either. Not
/// verified against the live API.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::ws::models::MarginalIndicatorsData;
///
/// let m: MarginalIndicatorsData = serde_json::from_str(
///     r#"{"portfolioCurrentValue":{"currentValueRub":512000.0},"cashByCurrency":[{"currency":"RUB","sum":50000.0}]}"#,
/// )?;
/// assert_eq!(m.portfolio_current_value.unwrap().current_value_rub, Some(dec!(512000.0)));
/// assert_eq!(m.cash_by_currency.len(), 1);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginalIndicatorsData {
    /// Data about the account.
    #[serde(default)]
    pub agreement_data: Option<AgreementData>,
    /// Current value of the portfolio and its changes.
    #[serde(default)]
    pub portfolio_current_value: Option<PortfolioCurrentValue>,
    /// Stability (margin level) of the portfolio.
    #[serde(default)]
    pub portfolio_stability: Option<PortfolioStability>,
    /// Stability of the derivatives market account.
    #[serde(default)]
    pub forts_stability: Option<FortsStability>,
    /// Parameters of the margin portfolio. See the note on the type.
    #[serde(default)]
    pub margin_parameters: Option<MarginParameters>,
    /// Free money by currency. Empty if the broker sent none.
    #[serde(default)]
    pub cash_by_currency: Vec<CashByCurrency>,
    /// Debts by currency.
    #[serde(default)]
    pub money_debt: Option<MoneyDebtWrapper>,
    /// Margin positions.
    #[serde(default)]
    pub margin_securities: Option<MarginSecuritiesWrapper>,
    /// Futures positions. See the note on the type.
    #[serde(default)]
    pub futures_parameters: Option<FuturesParameters>,
}

/// Data about the brokerage account in a margin indicators frame (`agreementData`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgreementData {
    /// Account number.
    #[serde(default)]
    pub number: Option<String>,
    /// `true` if the account is an IIA (individual investment account).
    #[serde(default)]
    pub is_iia: Option<bool>,
    /// Name for display.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Id of the sub-account.
    #[serde(default)]
    pub subaccount_id: Option<String>,
    /// Colour of the account as a HEX code.
    #[serde(default)]
    pub color: Option<String>,
}

/// Free money in one currency (`cashByCurrency`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CashByCurrency {
    /// Currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Amount.
    #[serde(default)]
    pub sum: Option<Decimal>,
}

/// Debts of the account (`moneyDebt`): the total and the debt per currency.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoneyDebtWrapper {
    /// Total debt.
    #[serde(default)]
    pub total_money_debt: Option<Decimal>,
    /// Debt per currency.
    #[serde(default)]
    pub debt_by_currency: Vec<DebtByCurrency>,
}

/// Debt in one currency.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebtByCurrency {
    /// Currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Amount of the debt.
    #[serde(default)]
    pub sum: Option<Decimal>,
}

/// Margin positions (`marginSecurities`): the total and the list.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginSecuritiesWrapper {
    /// Total. The docs label this field "total debt".
    #[serde(default)]
    pub total_margin_security: Option<Decimal>,
    /// The margin positions.
    #[serde(default)]
    pub margin_by_security: Vec<MarginBySecurity>,
}

/// One margin position.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginBySecurity {
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Trading mode (board).
    #[serde(default)]
    pub board: Option<String>,
    /// Name for display.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Link to a logo. Deprecated by the broker.
    #[serde(default)]
    pub logo_link: Option<String>,
    /// Quantity.
    #[serde(default)]
    pub quantity: Option<Decimal>,
    /// Current value.
    #[serde(default)]
    pub current_value: Option<Decimal>,
    /// Currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Discount in percent.
    #[serde(default)]
    pub discount: Option<f64>,
}

/// Current value of the portfolio and its changes (`portfolioCurrentValue`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioCurrentValue {
    /// Current value in roubles.
    #[serde(default)]
    pub current_value_rub: Option<Decimal>,
    /// Change over the day, in roubles.
    #[serde(default)]
    pub daily_portfolio_change_rub: Option<Decimal>,
    /// Change over the day, in percent.
    #[serde(default)]
    pub daily_portfolio_change_perc: Option<f64>,
    /// Change over all time, in roubles.
    #[serde(default)]
    pub total_portfolio_change_rub: Option<Decimal>,
    /// Change over all time, in percent.
    #[serde(default)]
    pub total_portfolio_change_perc: Option<f64>,
    /// Collateral available.
    #[serde(default)]
    pub collateral_available: Option<Decimal>,
    /// Variation margin.
    #[serde(default)]
    pub var_margin: Option<Decimal>,
}

/// Stability of the portfolio (`portfolioStability`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioStability {
    /// `true` if margin trading is on.
    #[serde(default)]
    pub is_margin_on: Option<bool>,
    /// All debts, in roubles.
    #[serde(default)]
    pub borrowed_funds: Option<Decimal>,
    /// Id of the status, 0 to 5 by the broker docs.
    #[serde(default)]
    pub stability_status_id: Option<u32>,
    /// Text of the status.
    #[serde(default)]
    pub stability_status_name: Option<String>,
    /// Amount needed to get out of a margin call.
    #[serde(default)]
    pub refill_sum: Option<Decimal>,
}

/// Stability of the derivatives market account (`fortsStability`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FortsStability {
    /// Own funds.
    #[serde(default)]
    pub own_funds: Option<Decimal>,
    /// Free funds.
    #[serde(default)]
    pub free_cash: Option<Decimal>,
    /// Id of the status.
    #[serde(default)]
    pub stability_status_id: Option<u32>,
    /// Text of the status.
    #[serde(default)]
    pub stability_status_name: Option<String>,
}

/// Parameters of the margin portfolio (`marginParameters`).
///
/// The fields here are the names this library reads. The current docs name the fields `marginValue`,
/// `collateral`, `marginCall` and `forcedClosure` instead. See the note on [`MarginalIndicatorsData`].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginParameters {
    /// Portfolio value.
    #[serde(default)]
    pub portfolio_value: Option<Decimal>,
    /// Initial margin (collateral).
    #[serde(default)]
    pub initial_margin: Option<Decimal>,
    /// Minimal margin.
    #[serde(default)]
    pub minimal_margin: Option<Decimal>,
    /// Corrected margin.
    #[serde(default)]
    pub corrected_margin: Option<Decimal>,
    /// Corrected portfolio value.
    #[serde(default)]
    pub corrected_portfolio_value: Option<Decimal>,
    /// Liquid portfolio value.
    #[serde(default)]
    pub liquid_portfolio_value: Option<Decimal>,
    /// Assets without a discount.
    #[serde(default)]
    pub assets_without_discount: Option<Decimal>,
}

/// Futures parameters (`futuresParameters`).
///
/// The fields here are the names this library reads. The current docs give `futuresValue` and a
/// `futuresPortfolio` list instead. See the note on [`MarginalIndicatorsData`].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesParameters {
    /// Initial margin (collateral).
    #[serde(default)]
    pub initial_margin: Option<Decimal>,
    /// Minimal margin.
    #[serde(default)]
    pub minimal_margin: Option<Decimal>,
    /// Variation margin.
    #[serde(default)]
    pub var_margin: Option<Decimal>,
    /// Free funds.
    #[serde(default)]
    pub free_cash: Option<Decimal>,
}
