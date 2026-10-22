//! Request and response models of the REST API, one struct per JSON shape of the broker.
//!
//! The models are the `Simple` format of the broker: the original field names, which the client always asks
//! for. Almost every field of a reply is an `Option` or has a default, because the broker fills only what fits
//! the object, so check before you use a field. Numbers are read as decimals, never through `f64`. All dates
//! and times of the broker are UTC.
//!
//! **Quantities.** The broker counts orders in lots. Its replies carry the quantity both in lots (`qty`,
//! `qty_batch`) and in pieces (`qty_units`), and the connector reads the pieces.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

fn dec_out<S: serde::Serializer>(d: &Decimal, s: S) -> Result<S::Ok, S::Error> {
    rust_decimal::serde::arbitrary_precision::serialize(d, s)
}

fn dec_out_opt<S: serde::Serializer>(d: &Option<Decimal>, s: S) -> Result<S::Ok, S::Error> {
    rust_decimal::serde::arbitrary_precision_option::serialize(d, s)
}

// ─── reference data ───────────────────────────────────────────────────────────

/// An instrument: the answer of `GET /md/v2/Securities/{exchange}/{symbol}` and an item of the list
/// `GET /md/v2/Securities`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SecurityInfo {
    /// The ticker.
    #[serde(default)]
    pub symbol: String,
    /// Short name.
    #[serde(default)]
    pub shortname: Option<String>,
    /// Description in Russian.
    #[serde(default)]
    pub description: Option<String>,
    /// `MOEX` or `SPBX`.
    #[serde(default)]
    pub exchange: String,
    /// The market: `FORTS`, `FOND`, `CURR` or `SPBX`.
    #[serde(default)]
    pub market: Option<String>,
    /// The type of the instrument (`CS` shares, `EUSOV` federal bonds, ... for MOEX).
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    /// Lot size in pieces.
    #[serde(default, rename = "lotsize", serialize_with = "dec_out_opt")]
    pub lot_size: Option<Decimal>,
    /// Face value (shares, bonds), contract size (futures), or the amount a quotation is given for (currency).
    #[serde(default, rename = "facevalue", serialize_with = "dec_out_opt")]
    pub face_value: Option<Decimal>,
    /// ISO 10962 code of the type.
    #[serde(default, rename = "cfiCode")]
    pub cfi_code: Option<String>,
    /// Expiry date of a derivative.
    #[serde(default)]
    pub cancellation: Option<String>,
    /// The price step.
    #[serde(default, rename = "minstep", serialize_with = "dec_out_opt")]
    pub min_step: Option<Decimal>,
    /// The price step in roubles (the value of one step).
    #[serde(default, rename = "pricestep", serialize_with = "dec_out_opt")]
    pub price_step: Option<Decimal>,
    /// The currency of the price.
    #[serde(default)]
    pub currency: Option<String>,
    /// ISIN.
    #[serde(default, rename = "ISIN")]
    pub isin: Option<String>,
    /// The trading mode (board) of this entry.
    #[serde(default)]
    pub board: Option<String>,
    /// The main trading mode of the instrument.
    #[serde(default)]
    pub primary_board: Option<String>,
    /// The trading status code: 17 normal period, 18 closed, 2 break, ...
    #[serde(default, rename = "tradingStatus")]
    pub trading_status: Option<i32>,
    /// The trading status as text.
    #[serde(default, rename = "tradingStatusInfo")]
    pub trading_status_info: Option<String>,
}

/// An item of `GET /md/status/{exchange}/{market}`: the state of the trading session of a market.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MarketStatus {
    /// The market: `FORTS`, `FOND`, `CURR` or `SPBX`.
    #[serde(default)]
    pub code: String,
    /// The status as text (`open`, ...).
    #[serde(default)]
    pub status: String,
}

// ─── market data ──────────────────────────────────────────────────────────────

/// One level of an order book.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BookLevel {
    /// The price.
    #[serde(serialize_with = "dec_out")]
    pub price: Decimal,
    /// The volume at the price, as the broker reports it.
    #[serde(serialize_with = "dec_out")]
    pub volume: Decimal,
}

/// An order book: the answer of `GET /md/v2/orderbooks/{exchange}/{symbol}` and the message of the order book
/// subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct OrderBook {
    /// Bids, best first.
    #[serde(default)]
    pub bids: Vec<BookLevel>,
    /// Asks, best first.
    #[serde(default)]
    pub asks: Vec<BookLevel>,
    /// Time in Unix milliseconds. The REST answer names it `msTimestamp`, the stream `ms_timestamp`.
    #[serde(default, alias = "msTimestamp")]
    pub ms_timestamp: Option<i64>,
    /// `true` for data from the history (the first message of a subscription), `false` for new events.
    #[serde(default)]
    pub existing: Option<bool>,
}

/// A candle of `GET /md/v2/history` and of the candle subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Bar {
    /// Open time, Unix seconds.
    #[serde(default)]
    pub time: i64,
    /// Open.
    #[serde(default, serialize_with = "dec_out")]
    pub open: Decimal,
    /// High.
    #[serde(default, serialize_with = "dec_out")]
    pub high: Decimal,
    /// Low.
    #[serde(default, serialize_with = "dec_out")]
    pub low: Decimal,
    /// Close.
    #[serde(default, serialize_with = "dec_out")]
    pub close: Decimal,
    /// Volume, as the broker reports it.
    #[serde(default, serialize_with = "dec_out")]
    pub volume: Decimal,
}

/// The answer of `GET /md/v2/history`: candles and the Unix times of the neighbouring pages.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct HistoryResponse {
    /// The candles, oldest first.
    #[serde(default)]
    pub history: Vec<Bar>,
    /// Time of the next candle after the range, if there is one.
    #[serde(default)]
    pub next: Option<i64>,
    /// Time of the candle before the range, if there is one.
    #[serde(default)]
    pub prev: Option<i64>,
}

/// A trade of the market: the item of `GET /md/v2/Securities/{exchange}/{symbol}/alltrades` and the message of
/// the all trades subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MarketTradeRecord {
    /// The id of the trade.
    #[serde(default)]
    pub id: Option<i64>,
    /// The number of the order of the trade.
    #[serde(default)]
    pub orderno: Option<String>,
    /// The ticker.
    #[serde(default)]
    pub symbol: Option<String>,
    /// The trading mode.
    #[serde(default)]
    pub board: Option<String>,
    /// The quantity, as the broker reports it.
    #[serde(default, serialize_with = "dec_out")]
    pub qty: Decimal,
    /// The price.
    #[serde(default, serialize_with = "dec_out")]
    pub price: Decimal,
    /// The time of the trade, UTC text.
    #[serde(default)]
    pub time: Option<String>,
    /// The time of the trade, Unix milliseconds.
    #[serde(default)]
    pub timestamp: Option<i64>,
    /// `buy` or `sell`: the side of the aggressor.
    #[serde(default)]
    pub side: Option<String>,
    /// Open interest.
    #[serde(default)]
    pub oi: Option<i64>,
    /// `true` for data from the history, `false` for new events.
    #[serde(default)]
    pub existing: Option<bool>,
}

/// The answer of `GET /md/v2/Securities/{exchange}/{symbol}/alltrades/history`: a page of trades and their total.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AllTradesHistory {
    /// How many trades the query matches in all.
    #[serde(default)]
    pub total: Option<i64>,
    /// The page.
    #[serde(default)]
    pub list: Vec<MarketTradeRecord>,
}

/// A quotation: the item of `GET /md/v2/Securities/{symbols}/quotes` and the message of the quotes
/// subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QuoteRecord {
    /// The ticker.
    #[serde(default)]
    pub symbol: String,
    /// `MOEX` or `SPBX`.
    #[serde(default)]
    pub exchange: Option<String>,
    /// The last price.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub last_price: Option<Decimal>,
    /// The best ask.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub ask: Option<Decimal>,
    /// The best bid.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub bid: Option<Decimal>,
    /// The number of lots at the best ask.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub ask_vol: Option<Decimal>,
    /// The number of lots at the best bid.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub bid_vol: Option<Decimal>,
    /// The time of the order book the bid and ask come from, Unix milliseconds.
    #[serde(default)]
    pub ob_ms_timestamp: Option<i64>,
    /// The size of the lot.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub lotsize: Option<Decimal>,
}

// ─── orders ───────────────────────────────────────────────────────────────────

/// An order of the portfolio: the item of `GET /md/v2/Clients/{exchange}/{portfolio}/orders`, the answer of the
/// call for one order and the message of the orders subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct OrderRecord {
    /// The number of the order at the exchange. Text, though it looks like a number.
    #[serde(default)]
    pub id: String,
    /// The ticker.
    #[serde(default)]
    pub symbol: String,
    /// The trading mode.
    #[serde(default)]
    pub board: Option<String>,
    /// `MOEX` or `SPBX`.
    #[serde(default)]
    pub exchange: Option<String>,
    /// The portfolio.
    #[serde(default)]
    pub portfolio: Option<String>,
    /// The comment of the order (up to 100 characters). The connector keeps the client order id here.
    #[serde(default)]
    pub comment: Option<String>,
    /// `limit` or `market`.
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    /// `buy` or `sell`.
    #[serde(default)]
    pub side: Option<String>,
    /// `working`, `filled`, `canceled` or `rejected`.
    #[serde(default)]
    pub status: Option<String>,
    /// When the order was placed, UTC text.
    #[serde(default, rename = "transTime")]
    pub trans_time: Option<String>,
    /// When the status last changed, UTC text.
    #[serde(default, rename = "updateTime")]
    pub update_time: Option<String>,
    /// The ordered quantity in pieces.
    #[serde(default, rename = "qtyUnits", serialize_with = "dec_out_opt")]
    pub qty_units: Option<Decimal>,
    /// The ordered quantity in lots.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub qty: Option<Decimal>,
    /// The filled quantity in pieces.
    #[serde(default, rename = "filledQtyUnits", serialize_with = "dec_out_opt")]
    pub filled_qty_units: Option<Decimal>,
    /// The filled quantity in lots.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub filled: Option<Decimal>,
    /// The price (empty for a market order).
    #[serde(default, serialize_with = "dec_out_opt")]
    pub price: Option<Decimal>,
    /// The time in force: `oneday`, `immediateorcancel`, `fillorkill`, `attheclose`, `goodtillcancelled`,
    /// `bookorcancel`.
    #[serde(default, rename = "timeInForce")]
    pub time_in_force: Option<String>,
    /// `true` for data from the history (the first messages of a subscription), `false` for new events.
    #[serde(default)]
    pub existing: Option<bool>,
}

/// A stop order of the portfolio: the item of `GET /md/v2/Clients/{exchange}/{portfolio}/stoporders`, the answer
/// of the call for one stop order and the message of the stop orders subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StopOrderRecord {
    /// The number of the stop order. Text.
    #[serde(default)]
    pub id: String,
    /// The number of the order that the stop order made at the exchange when it triggered.
    #[serde(default, rename = "exchangeOrderId")]
    pub exchange_order_id: Option<String>,
    /// The ticker.
    #[serde(default)]
    pub symbol: String,
    /// The trading mode.
    #[serde(default)]
    pub board: Option<String>,
    /// `MOEX` or `SPBX`.
    #[serde(default)]
    pub exchange: Option<String>,
    /// The portfolio.
    #[serde(default)]
    pub portfolio: Option<String>,
    /// The comment (up to 100 characters). The connector keeps the client order id here.
    #[serde(default)]
    pub comment: Option<String>,
    /// `stop` (a market order on trigger) or `stoplimit`.
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    /// `buy` or `sell`.
    #[serde(default)]
    pub side: Option<String>,
    /// The trigger condition: `more`, `less`, `moreorequal` or `lessorequal`.
    #[serde(default)]
    pub condition: Option<String>,
    /// `working`, `filled`, `canceled` or `rejected`.
    #[serde(default)]
    pub status: Option<String>,
    /// When the stop order was placed, UTC text.
    #[serde(default, rename = "transTime")]
    pub trans_time: Option<String>,
    /// When the stop order ends, UTC text.
    #[serde(default, rename = "endTime")]
    pub end_time: Option<String>,
    /// The reason, if the stop order was rejected.
    #[serde(default)]
    pub error: Option<String>,
    /// The ordered quantity in pieces.
    #[serde(default, rename = "qtyUnits", serialize_with = "dec_out_opt")]
    pub qty_units: Option<Decimal>,
    /// The ordered quantity in lots.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub qty: Option<Decimal>,
    /// The limit price of a stop-limit order.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub price: Option<Decimal>,
    /// The trigger price.
    #[serde(default, rename = "stopPrice", serialize_with = "dec_out_opt")]
    pub stop_price: Option<Decimal>,
    /// `true` for data from the history, `false` for new events.
    #[serde(default)]
    pub existing: Option<bool>,
}

/// A trade of the portfolio (a fill of one of its orders): the item of
/// `GET /md/v2/Clients/{exchange}/{portfolio}/trades` and the message of the trades subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TradeRecord {
    /// The id of the trade.
    #[serde(default)]
    pub id: String,
    /// The number of the order.
    #[serde(default)]
    pub orderno: Option<String>,
    /// The comment of the order (the connector keeps the client order id here).
    #[serde(default)]
    pub comment: Option<String>,
    /// The ticker.
    #[serde(default)]
    pub symbol: String,
    /// `MOEX` or `SPBX`.
    #[serde(default)]
    pub exchange: Option<String>,
    /// The time of the trade, UTC text.
    #[serde(default)]
    pub date: Option<String>,
    /// The trading mode.
    #[serde(default)]
    pub board: Option<String>,
    /// The quantity in pieces.
    #[serde(default, rename = "qtyUnits", serialize_with = "dec_out_opt")]
    pub qty_units: Option<Decimal>,
    /// The quantity in lots.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub qty: Option<Decimal>,
    /// The price of one piece.
    #[serde(default, serialize_with = "dec_out")]
    pub price: Decimal,
    /// `buy` or `sell`.
    #[serde(default)]
    pub side: Option<String>,
    /// The commission (empty on the futures market).
    #[serde(default, serialize_with = "dec_out_opt")]
    pub commission: Option<Decimal>,
    /// `true` for data from the history, `false` for new events.
    #[serde(default)]
    pub existing: Option<bool>,
}

/// An instrument or money position: the item of `GET /md/v2/Clients/{exchange}/{portfolio}/positions` and the
/// message of the positions subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PositionRecord {
    /// The ticker (or the currency code of a money position).
    #[serde(default)]
    pub symbol: String,
    /// `MOEX` or `SPBX`.
    #[serde(default)]
    pub exchange: Option<String>,
    /// The portfolio.
    #[serde(default)]
    pub portfolio: Option<String>,
    /// The average price.
    #[serde(default, rename = "avgPrice", serialize_with = "dec_out_opt")]
    pub avg_price: Option<Decimal>,
    /// The quantity in pieces (the amount, for money).
    #[serde(default, rename = "qtyUnits", serialize_with = "dec_out_opt")]
    pub qty_units: Option<Decimal>,
    /// The size of the lot.
    #[serde(default, rename = "lotSize", serialize_with = "dec_out_opt")]
    pub lot_size: Option<Decimal>,
    /// The quantity in lots.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub qty: Option<Decimal>,
    /// `true` for money (a currency balance), `false` for an instrument.
    #[serde(default, rename = "isCurrency")]
    pub is_currency: Option<bool>,
    /// The unrealised profit or loss.
    #[serde(default, rename = "unrealisedPl", serialize_with = "dec_out_opt")]
    pub unrealised_pl: Option<Decimal>,
    /// The value of the position in the settlement currency by its current price.
    #[serde(default, rename = "currentVolume", serialize_with = "dec_out_opt")]
    pub current_volume: Option<Decimal>,
    /// `true` for data from the history, `false` for new events.
    #[serde(default)]
    pub existing: Option<bool>,
}

/// The money of a currency in the summary.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BuyingPowerByCurrency {
    /// The currency.
    #[serde(default)]
    pub currency: String,
    /// The buying power in the currency.
    #[serde(default, rename = "buyingPower", serialize_with = "dec_out_opt")]
    pub buying_power: Option<Decimal>,
}

/// The summary of the portfolio: `GET /md/v2/Clients/{exchange}/{portfolio}/summary` and the message of the
/// summaries subscription.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SummaryRecord {
    /// The buying power.
    #[serde(default, rename = "buyingPower", serialize_with = "dec_out_opt")]
    pub buying_power: Option<Decimal>,
    /// The evaluation of the portfolio.
    #[serde(
        default,
        rename = "portfolioEvaluation",
        serialize_with = "dec_out_opt"
    )]
    pub portfolio_evaluation: Option<Decimal>,
    /// The commission.
    #[serde(default, serialize_with = "dec_out_opt")]
    pub commission: Option<Decimal>,
    /// The buying power by currency.
    #[serde(default, rename = "buyingPowerByCurrency")]
    pub buying_power_by_currency: Vec<BuyingPowerByCurrency>,
}

// ─── commands ─────────────────────────────────────────────────────────────────

/// The instrument of an order command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentRef {
    /// The ticker.
    pub symbol: String,
    /// `MOEX` or `SPBX`.
    pub exchange: String,
    /// The trading mode (board), for example `TQBR`.
    #[serde(rename = "instrumentGroup", skip_serializing_if = "Option::is_none")]
    pub instrument_group: Option<String>,
}

/// The portfolio of an order command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRef {
    /// The portfolio.
    pub portfolio: String,
}

/// The body of a limit order command (`POST .../orders/actions/limit` and the change `PUT .../limit/{orderId}`).
#[derive(Debug, Clone, Serialize)]
pub struct LimitOrderRequest {
    /// `buy` or `sell`.
    pub side: String,
    /// The quantity in lots.
    pub quantity: i64,
    /// The price.
    #[serde(serialize_with = "dec_out")]
    pub price: Decimal,
    /// The instrument.
    pub instrument: InstrumentRef,
    /// The portfolio.
    pub user: UserRef,
    /// The comment of the order (up to 100 characters).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// The time in force: `oneday`, `immediateorcancel`, `fillorkill`, `attheclose`, `goodtillcancelled`,
    /// `bookorcancel`. The broker takes `oneday` when it is left out.
    #[serde(rename = "timeInForce", skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    /// Consent to margin (a risk level of the broker) for this order.
    #[serde(rename = "allowMargin", skip_serializing_if = "Option::is_none")]
    pub allow_margin: Option<bool>,
    /// The visible constant part of an iceberg order, in lots.
    #[serde(rename = "icebergFixed", skip_serializing_if = "Option::is_none")]
    pub iceberg_fixed: Option<i64>,
    /// The deviation of the random addition to the visible part of an iceberg order, in percent of the constant part.
    #[serde(rename = "icebergVariance", skip_serializing_if = "Option::is_none")]
    pub iceberg_variance: Option<i64>,
}

/// The body of a market order command (`POST .../orders/actions/market` and `PUT .../market/{orderId}`).
#[derive(Debug, Clone, Serialize)]
pub struct MarketOrderRequest {
    /// `buy` or `sell`.
    pub side: String,
    /// The quantity in lots.
    pub quantity: i64,
    /// The instrument.
    pub instrument: InstrumentRef,
    /// The portfolio.
    pub user: UserRef,
    /// The comment of the order (up to 100 characters).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// The time in force (see [`LimitOrderRequest::time_in_force`]).
    #[serde(rename = "timeInForce", skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    /// Consent to margin for this order.
    #[serde(rename = "allowMargin", skip_serializing_if = "Option::is_none")]
    pub allow_margin: Option<bool>,
}

/// The body of a stop order command: `POST .../orders/actions/stop` (a market order on trigger) and
/// `.../stopLimit` (a limit order on trigger, with `price`), and the changes `PUT .../stop/{id}` and
/// `.../stopLimit/{id}`.
#[derive(Debug, Clone, Serialize)]
pub struct StopOrderRequest {
    /// `buy` or `sell`.
    pub side: String,
    /// When the stop order triggers: `more`, `less`, `moreorequal` or `lessorequal` than the trigger price.
    pub condition: String,
    /// The trigger price.
    #[serde(rename = "triggerPrice", serialize_with = "dec_out")]
    pub trigger_price: Decimal,
    /// The end of the stop order, Unix seconds.
    #[serde(rename = "stopEndUnixTime", skip_serializing_if = "Option::is_none")]
    pub stop_end_unix_time: Option<i64>,
    /// The price of the order made on trigger (a stop-limit order only).
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "dec_out_opt"
    )]
    pub price: Option<Decimal>,
    /// The quantity in lots.
    pub quantity: i64,
    /// The instrument.
    pub instrument: InstrumentRef,
    /// The portfolio.
    pub user: UserRef,
    /// The comment (up to 100 characters).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// The time in force of the order made on trigger (a stop-limit order only).
    #[serde(rename = "timeInForce", skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    /// Consent to margin for the order made on trigger.
    #[serde(rename = "allowMargin", skip_serializing_if = "Option::is_none")]
    pub allow_margin: Option<bool>,
    /// `true` (the broker default) to start watching the price at once.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activate: Option<bool>,
}

/// The answer to an order command: the number of the order the broker made.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct OrderCommandResponse {
    /// The text of the answer (`success`).
    #[serde(default)]
    pub message: Option<String>,
    /// The number of the order or stop order. Text.
    #[serde(default, rename = "orderNumber")]
    pub order_number: Option<String>,
}
