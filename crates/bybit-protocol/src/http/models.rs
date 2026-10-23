//! Request and response models of the REST endpoints used by the connectors.
//!
//! Bybit writes every number as a text (`"81695.30"`) and an absent number as an empty text. The readers here take
//! either, keep the digits as written (an exact decimal, never a float) and turn the empty text into zero or `None`.
//! Fields the connectors do not use are not modelled: the reply may carry more.

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// A decimal from a JSON text or number. The empty text is zero.
pub fn dec<'de, D: Deserializer<'de>>(d: D) -> Result<Decimal, D::Error> {
    Ok(opt_dec(d)?.unwrap_or(Decimal::ZERO))
}

/// A decimal from a JSON text or number. The empty text, `null` and an absent field are `None`.
pub fn opt_dec<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Decimal>, D::Error> {
    use serde::de::Error;
    let text = match Value::deserialize(d)? {
        Value::Null => return Ok(None),
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        other => {
            return Err(D::Error::custom(format!(
                "a number was expected, got {other}"
            )));
        }
    };
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    text.parse::<Decimal>()
        .or_else(|_| Decimal::from_scientific(text))
        .map(Some)
        .map_err(|e| D::Error::custom(format!("{text:?} is not a number: {e}")))
}

/// An integer from a JSON text or number. The empty text, `null` and an absent field are zero.
pub fn int<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    use serde::de::Error;
    match Value::deserialize(d)? {
        Value::Null => Ok(0),
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .ok_or_else(|| D::Error::custom("an integer was expected")),
        Value::String(s) if s.trim().is_empty() => Ok(0),
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|e| D::Error::custom(format!("{s:?} is not an integer: {e}"))),
        other => Err(D::Error::custom(format!(
            "an integer was expected, got {other}"
        ))),
    }
}

/// Writes a decimal as a JSON text, the way the API wants numbers in a request.
pub fn ser_dec<S: Serializer>(value: &Decimal, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&value.to_string())
}

fn ser_opt_dec<S: Serializer>(value: &Option<Decimal>, s: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(v) => s.serialize_str(&v.to_string()),
        None => s.serialize_none(),
    }
}

/// The envelope of every REST reply.
#[derive(Debug, Clone, Deserialize)]
pub struct Envelope<T> {
    /// `0` is a success.
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    /// The text of the code.
    #[serde(rename = "retMsg", default)]
    pub ret_msg: String,
    /// The payload. Absent or `null` on some refusals.
    pub result: Option<T>,
    /// The time of the exchange, milliseconds.
    #[serde(default)]
    pub time: i64,
}

/// A page of a list with a cursor. The list is empty and the cursor empty at the end.
#[derive(Debug, Clone, Deserialize)]
pub struct Page<T> {
    /// The category of the answer.
    #[serde(default)]
    pub category: String,
    /// The rows of the page.
    #[serde(default = "Vec::new")]
    pub list: Vec<T>,
    /// The cursor of the next page, empty at the end.
    #[serde(rename = "nextPageCursor", default)]
    pub next_page_cursor: String,
}

/// The server time (`/v5/market/time`).
#[derive(Debug, Clone, Deserialize)]
pub struct ServerTime {
    /// Seconds since the epoch.
    #[serde(rename = "timeSecond", deserialize_with = "int")]
    pub time_second: i64,
    /// Nanoseconds since the epoch.
    #[serde(rename = "timeNano", deserialize_with = "int")]
    pub time_nano: i64,
}

/// The price rules of an instrument.
#[derive(Debug, Clone, Deserialize)]
pub struct PriceFilter {
    /// The lowest price.
    #[serde(rename = "minPrice", deserialize_with = "dec", default)]
    pub min_price: Decimal,
    /// The highest price.
    #[serde(rename = "maxPrice", deserialize_with = "dec", default)]
    pub max_price: Decimal,
    /// The price step.
    #[serde(rename = "tickSize", deserialize_with = "dec", default)]
    pub tick_size: Decimal,
}

/// The size rules of an instrument.
#[derive(Debug, Clone, Deserialize)]
pub struct LotSizeFilter {
    /// The largest quantity of a limit order.
    #[serde(rename = "maxOrderQty", deserialize_with = "dec", default)]
    pub max_order_qty: Decimal,
    /// The largest quantity of a market order.
    #[serde(rename = "maxMktOrderQty", deserialize_with = "dec", default)]
    pub max_mkt_order_qty: Decimal,
    /// The smallest quantity of an order.
    #[serde(rename = "minOrderQty", deserialize_with = "dec", default)]
    pub min_order_qty: Decimal,
    /// The quantity step.
    #[serde(rename = "qtyStep", deserialize_with = "dec", default)]
    pub qty_step: Decimal,
    /// The smallest value (price times quantity) of an order.
    #[serde(rename = "minNotionalValue", deserialize_with = "opt_dec", default)]
    pub min_notional_value: Option<Decimal>,
}

/// One instrument of `/v5/market/instruments-info`.
#[derive(Debug, Clone, Deserialize)]
pub struct InstrumentInfo {
    /// The symbol, `BTCUSDT`.
    pub symbol: String,
    /// `LinearPerpetual`, `LinearFutures`, ...
    #[serde(rename = "contractType", default)]
    pub contract_type: String,
    /// `Trading`, `PreLaunch`, `PendingOpen`, `Delivering`, `Closed`.
    #[serde(default)]
    pub status: String,
    /// The base coin, `BTC`.
    #[serde(rename = "baseCoin", default)]
    pub base_coin: String,
    /// The quote coin, `USDT`.
    #[serde(rename = "quoteCoin", default)]
    pub quote_coin: String,
    /// The settle coin, `USDT`.
    #[serde(rename = "settleCoin", default)]
    pub settle_coin: String,
    /// The name shown on the site.
    #[serde(rename = "displayName", default)]
    pub display_name: String,
    /// When trading starts, milliseconds.
    #[serde(rename = "launchTime", deserialize_with = "int", default)]
    pub launch_time: i64,
    /// When a future is delivered (or a perpetual delisted), milliseconds; `0` for a live perpetual.
    #[serde(rename = "deliveryTime", deserialize_with = "int", default)]
    pub delivery_time: i64,
    /// The price rules.
    #[serde(rename = "priceFilter")]
    pub price_filter: PriceFilter,
    /// The size rules.
    #[serde(rename = "lotSizeFilter")]
    pub lot_size_filter: LotSizeFilter,
}

/// One level of an order book: price and size.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub struct Level(
    /// The price.
    #[serde(deserialize_with = "dec")]
    pub Decimal,
    /// The size. Zero in a stream delta removes the level.
    #[serde(deserialize_with = "dec")]
    pub Decimal,
);

/// The order book of `/v5/market/orderbook`.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderBookResult {
    /// The symbol.
    #[serde(rename = "s", default)]
    pub symbol: String,
    /// Bids, best first.
    #[serde(rename = "b", default)]
    pub bids: Vec<Level>,
    /// Asks, best first.
    #[serde(rename = "a", default)]
    pub asks: Vec<Level>,
    /// The time the exchange made the data, milliseconds.
    #[serde(default)]
    pub ts: i64,
    /// The update id.
    #[serde(rename = "u", default)]
    pub update_id: i64,
    /// The cross sequence.
    #[serde(default)]
    pub seq: i64,
    /// The time of the matching engine, milliseconds.
    #[serde(default)]
    pub cts: i64,
}

/// One ticker of `/v5/market/tickers`.
#[derive(Debug, Clone, Deserialize)]
pub struct Ticker {
    /// The symbol.
    pub symbol: String,
    /// The last price.
    #[serde(rename = "lastPrice", deserialize_with = "opt_dec", default)]
    pub last_price: Option<Decimal>,
    /// The mark price.
    #[serde(rename = "markPrice", deserialize_with = "opt_dec", default)]
    pub mark_price: Option<Decimal>,
    /// The best bid.
    #[serde(rename = "bid1Price", deserialize_with = "opt_dec", default)]
    pub bid1_price: Option<Decimal>,
    /// The size at the best bid.
    #[serde(rename = "bid1Size", deserialize_with = "opt_dec", default)]
    pub bid1_size: Option<Decimal>,
    /// The best ask.
    #[serde(rename = "ask1Price", deserialize_with = "opt_dec", default)]
    pub ask1_price: Option<Decimal>,
    /// The size at the best ask.
    #[serde(rename = "ask1Size", deserialize_with = "opt_dec", default)]
    pub ask1_size: Option<Decimal>,
}

/// One trade of `/v5/market/recent-trade`.
#[derive(Debug, Clone, Deserialize)]
pub struct PublicTradeRecord {
    /// The id of the trade.
    #[serde(rename = "execId", default)]
    pub exec_id: String,
    /// The symbol.
    pub symbol: String,
    /// The price.
    #[serde(deserialize_with = "dec")]
    pub price: Decimal,
    /// The size.
    #[serde(deserialize_with = "dec")]
    pub size: Decimal,
    /// The side of the taker, `Buy` or `Sell`.
    pub side: String,
    /// The time, milliseconds.
    #[serde(deserialize_with = "int")]
    pub time: i64,
}

/// One candle: the open time and the prices, from the list of `/v5/market/kline`.
#[derive(Debug, Clone, PartialEq)]
pub struct Kline {
    /// The start of the candle, milliseconds.
    pub start_ms: i64,
    /// The open price.
    pub open: Decimal,
    /// The highest price.
    pub high: Decimal,
    /// The lowest price.
    pub low: Decimal,
    /// The close price.
    pub close: Decimal,
    /// The volume in the base coin.
    pub volume: Decimal,
}

impl Kline {
    /// Reads one row of the list: `[start, open, high, low, close, volume, turnover]`, all texts.
    pub fn from_row(row: &[String]) -> Option<Self> {
        let n = |i: usize| row.get(i).and_then(|s| s.trim().parse::<Decimal>().ok());
        Some(Self {
            start_ms: row.first()?.trim().parse().ok()?,
            open: n(1)?,
            high: n(2)?,
            low: n(3)?,
            close: n(4)?,
            volume: n(5)?,
        })
    }
}

/// The result of `/v5/market/kline`: the rows are the newest first.
#[derive(Debug, Clone, Deserialize)]
pub struct KlineResult {
    /// The symbol.
    #[serde(default)]
    pub symbol: String,
    /// The rows, newest first.
    #[serde(default)]
    pub list: Vec<Vec<String>>,
}

/// An order as the exchange lists it (`/v5/order/realtime`, `/v5/order/history`, and the `order` stream).
#[derive(Debug, Clone, Deserialize)]
pub struct OrderRecord {
    /// The number of the order.
    #[serde(rename = "orderId")]
    pub order_id: String,
    /// The client id (`orderLinkId`), empty when none was given.
    #[serde(rename = "orderLinkId", default)]
    pub order_link_id: String,
    /// The symbol.
    pub symbol: String,
    /// The limit price, `None` for a market order.
    #[serde(deserialize_with = "opt_dec", default)]
    pub price: Option<Decimal>,
    /// The quantity.
    #[serde(deserialize_with = "dec", default)]
    pub qty: Decimal,
    /// `Buy` or `Sell`.
    #[serde(default)]
    pub side: String,
    /// `New`, `PartiallyFilled`, `Untriggered`, `Rejected`, `Filled`, `Cancelled`, `Triggered`, `Deactivated`, ...
    #[serde(rename = "orderStatus", default)]
    pub order_status: String,
    /// `Market` or `Limit`.
    #[serde(rename = "orderType", default)]
    pub order_type: String,
    /// `Stop`, `TakeProfit`, `StopLoss`, ... or `UNKNOWN`/empty for a plain order.
    #[serde(rename = "stopOrderType", default)]
    pub stop_order_type: String,
    /// The quantity executed so far.
    #[serde(rename = "cumExecQty", deserialize_with = "dec", default)]
    pub cum_exec_qty: Decimal,
    /// The quantity still working.
    #[serde(rename = "leavesQty", deserialize_with = "opt_dec", default)]
    pub leaves_qty: Option<Decimal>,
    /// The average price of the executed part.
    #[serde(rename = "avgPrice", deserialize_with = "opt_dec", default)]
    pub avg_price: Option<Decimal>,
    /// The trigger price of a conditional order, `None` (or zero) for a plain one.
    #[serde(rename = "triggerPrice", deserialize_with = "opt_dec", default)]
    pub trigger_price: Option<Decimal>,
    /// `1` the order triggers when the price rises to the trigger price, `2` when it falls, `0` not conditional.
    #[serde(rename = "triggerDirection", deserialize_with = "int", default)]
    pub trigger_direction: i64,
    /// `GTC`, `IOC`, `FOK`, `PostOnly`.
    #[serde(rename = "timeInForce", default)]
    pub time_in_force: String,
    /// Why the exchange cancelled it (`CancelByUser`, ...).
    #[serde(rename = "cancelType", default)]
    pub cancel_type: String,
    /// Why the exchange rejected it (`EC_NoError` when it did not).
    #[serde(rename = "rejectReason", default)]
    pub reject_reason: String,
    /// The product category (the streams carry it, the lists put it on the page).
    #[serde(default)]
    pub category: String,
    /// When the order was made, milliseconds.
    #[serde(rename = "createdTime", deserialize_with = "int", default)]
    pub created_time: i64,
    /// When the order last changed, milliseconds.
    #[serde(rename = "updatedTime", deserialize_with = "int", default)]
    pub updated_time: i64,
}

/// A fill (`/v5/execution/list`, and the `execution` stream).
#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionRecord {
    /// The id of the fill.
    #[serde(rename = "execId")]
    pub exec_id: String,
    /// The number of the order.
    #[serde(rename = "orderId", default)]
    pub order_id: String,
    /// The client id of the order.
    #[serde(rename = "orderLinkId", default)]
    pub order_link_id: String,
    /// The symbol.
    pub symbol: String,
    /// `Buy` or `Sell`.
    #[serde(default)]
    pub side: String,
    /// The price of the fill.
    #[serde(rename = "execPrice", deserialize_with = "dec")]
    pub exec_price: Decimal,
    /// The quantity of the fill.
    #[serde(rename = "execQty", deserialize_with = "dec")]
    pub exec_qty: Decimal,
    /// The fee. Negative is a rebate.
    #[serde(rename = "execFee", deserialize_with = "dec", default)]
    pub exec_fee: Decimal,
    /// The kind: `Trade`, `AdlTrade`, `Funding`, `BustTrade`, `Delivery`, ...
    #[serde(rename = "execType", default)]
    pub exec_type: String,
    /// When the fill happened, milliseconds.
    #[serde(rename = "execTime", deserialize_with = "int", default)]
    pub exec_time: i64,
    /// The product category (the stream carries it).
    #[serde(default)]
    pub category: String,
}

/// A position of `/v5/position/list` or of the `position` stream.
#[derive(Debug, Clone, Deserialize)]
pub struct PositionRecord {
    /// The symbol.
    pub symbol: String,
    /// `Buy` for long, `Sell` for short, empty for no position.
    #[serde(default)]
    pub side: String,
    /// The size, always positive.
    #[serde(deserialize_with = "dec", default)]
    pub size: Decimal,
    /// The average entry price (`avgPrice` in the list, `entryPrice` in the stream).
    #[serde(
        rename = "avgPrice",
        alias = "entryPrice",
        deserialize_with = "opt_dec",
        default
    )]
    pub avg_price: Option<Decimal>,
    /// The mark price.
    #[serde(rename = "markPrice", deserialize_with = "opt_dec", default)]
    pub mark_price: Option<Decimal>,
    /// The unrealised profit.
    #[serde(rename = "unrealisedPnl", deserialize_with = "opt_dec", default)]
    pub unrealised_pnl: Option<Decimal>,
    /// `0` one-way mode, `1` long and `2` short side of hedge mode.
    #[serde(rename = "positionIdx", deserialize_with = "int", default)]
    pub position_idx: i64,
    /// The product category (the stream carries it).
    #[serde(default)]
    pub category: String,
    /// When the position last changed, milliseconds.
    #[serde(rename = "updatedTime", deserialize_with = "int", default)]
    pub updated_time: i64,
}

/// One coin of a wallet.
#[derive(Debug, Clone, Deserialize)]
pub struct CoinBalance {
    /// The coin, `USDT`.
    pub coin: String,
    /// The equity: the wallet balance plus unrealised profit.
    #[serde(deserialize_with = "dec", default)]
    pub equity: Decimal,
    /// The wallet balance.
    #[serde(rename = "walletBalance", deserialize_with = "dec", default)]
    pub wallet_balance: Decimal,
    /// Locked by open spot orders.
    #[serde(deserialize_with = "dec", default)]
    pub locked: Decimal,
    /// Margin taken by open orders (empty in portfolio margin mode).
    #[serde(rename = "totalOrderIM", deserialize_with = "dec", default)]
    pub total_order_im: Decimal,
    /// Margin taken by positions (empty in portfolio margin mode).
    #[serde(rename = "totalPositionIM", deserialize_with = "dec", default)]
    pub total_position_im: Decimal,
    /// The unrealised profit.
    #[serde(rename = "unrealisedPnl", deserialize_with = "dec", default)]
    pub unrealised_pnl: Decimal,
}

/// A wallet: `/v5/account/wallet-balance` and the `wallet` stream.
#[derive(Debug, Clone, Deserialize)]
pub struct WalletRecord {
    /// `UNIFIED`.
    #[serde(rename = "accountType", default)]
    pub account_type: String,
    /// The equity of the whole account in USD.
    #[serde(rename = "totalEquity", deserialize_with = "dec", default)]
    pub total_equity: Decimal,
    /// The coins.
    #[serde(default)]
    pub coin: Vec<CoinBalance>,
}

/// The answer to a command on one order: the number and the client id.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderRef {
    /// The number of the order.
    #[serde(rename = "orderId", default)]
    pub order_id: String,
    /// The client id.
    #[serde(rename = "orderLinkId", default)]
    pub order_link_id: String,
}

/// The answer to `cancel-all`.
#[derive(Debug, Clone, Deserialize)]
pub struct CancelAllResult {
    /// The orders cancelled.
    #[serde(default)]
    pub list: Vec<OrderRef>,
}

/// The body of `POST /v5/order/create`. Only the fields that are set are sent.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CreateOrderRequest {
    /// The category.
    pub category: String,
    /// The symbol.
    pub symbol: String,
    /// `Buy` or `Sell`.
    pub side: String,
    /// `Market` or `Limit`.
    #[serde(rename = "orderType")]
    pub order_type: String,
    /// The quantity.
    #[serde(serialize_with = "ser_dec")]
    pub qty: Decimal,
    /// The limit price.
    #[serde(
        serialize_with = "ser_opt_dec",
        skip_serializing_if = "Option::is_none"
    )]
    pub price: Option<Decimal>,
    /// `GTC`, `IOC`, `FOK` or `PostOnly`.
    #[serde(rename = "timeInForce", skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    /// The client id, at most 36 characters.
    #[serde(rename = "orderLinkId", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
    /// The trigger price of a conditional order.
    #[serde(
        rename = "triggerPrice",
        serialize_with = "ser_opt_dec",
        skip_serializing_if = "Option::is_none"
    )]
    pub trigger_price: Option<Decimal>,
    /// `1` rise, `2` fall.
    #[serde(rename = "triggerDirection", skip_serializing_if = "Option::is_none")]
    pub trigger_direction: Option<i64>,
    /// `LastPrice`, `MarkPrice` or `IndexPrice`.
    #[serde(rename = "triggerBy", skip_serializing_if = "Option::is_none")]
    pub trigger_by: Option<String>,
    /// The order can only reduce the position.
    #[serde(rename = "reduceOnly", skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    /// `0` one-way mode.
    #[serde(rename = "positionIdx", skip_serializing_if = "Option::is_none")]
    pub position_idx: Option<i64>,
}

/// The body of `POST /v5/order/amend`: the order is named by `orderId` or `orderLinkId`, the fields set are changed.
#[derive(Debug, Clone, Default, Serialize)]
pub struct AmendOrderRequest {
    /// The category.
    pub category: String,
    /// The symbol.
    pub symbol: String,
    /// The number of the order.
    #[serde(rename = "orderId", skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    /// The client id of the order.
    #[serde(rename = "orderLinkId", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
    /// The new quantity.
    #[serde(
        serialize_with = "ser_opt_dec",
        skip_serializing_if = "Option::is_none"
    )]
    pub qty: Option<Decimal>,
    /// The new price.
    #[serde(
        serialize_with = "ser_opt_dec",
        skip_serializing_if = "Option::is_none"
    )]
    pub price: Option<Decimal>,
    /// The new trigger price of a conditional order.
    #[serde(
        rename = "triggerPrice",
        serialize_with = "ser_opt_dec",
        skip_serializing_if = "Option::is_none"
    )]
    pub trigger_price: Option<Decimal>,
}

/// The body of `POST /v5/order/cancel`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CancelOrderRequest {
    /// The category.
    pub category: String,
    /// The symbol.
    pub symbol: String,
    /// The number of the order.
    #[serde(rename = "orderId", skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    /// The client id of the order.
    #[serde(rename = "orderLinkId", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_keep_their_digits_and_the_empty_text_is_nothing() {
        let r: OrderRecord = serde_json::from_str(
            r#"{"orderId":"1","symbol":"BTCUSDT","price":"81695.30","qty":"0.10","avgPrice":"","triggerPrice":"0.00","triggerDirection":0,"createdTime":"1684738540559"}"#,
        )
        .unwrap();
        assert_eq!(r.price.unwrap().to_string(), "81695.30");
        assert_eq!(r.qty.to_string(), "0.10");
        assert_eq!(r.avg_price, None);
        assert_eq!(r.created_time, 1_684_738_540_559);
    }

    #[test]
    fn a_request_sends_numbers_as_texts_and_leaves_out_what_is_not_set() {
        let body = CreateOrderRequest {
            category: "linear".into(),
            symbol: "BTCUSDT".into(),
            side: "Buy".into(),
            order_type: "Limit".into(),
            qty: "0.010".parse().unwrap(),
            price: Some("25000.5".parse().unwrap()),
            order_link_id: Some("abc".into()),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            r#"{"category":"linear","symbol":"BTCUSDT","side":"Buy","orderType":"Limit","qty":"0.010","price":"25000.5","orderLinkId":"abc"}"#
        );
    }

    #[test]
    fn a_kline_row_is_read_and_a_short_row_is_not() {
        let row: Vec<String> = ["1792760100000", "1", "2", "0.5", "1.5", "6.3", "9"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let k = Kline::from_row(&row).unwrap();
        assert_eq!(
            (k.start_ms, k.high.to_string()),
            (1_792_760_100_000, "2".into())
        );
        assert!(Kline::from_row(&row[..3]).is_none());
    }
}
