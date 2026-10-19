//! Market data mapping: BCS wire models to brokapi-core domain types.

use brokapi_core::Decimal;
use std::time::Instant;

use brokapi_bcs_protocol::http::models::{CandlesResponse, LastTradeRecord, OrderBookResponse};
use brokapi_bcs_protocol::ws::models::{
    LastCandleData, LastTradesData, MarketDataMessage, OrderBookData, QuotesData,
};
use brokapi_core::{
    BookLevel, Candle, Event, MarketTrade, OrderBook, Price, Qty, Quote, Side, Timeframe,
};

use super::{instrument_id_ticker, parse_broker_ts, timestamps};

/// Maps the candles of `POST /candles-chart` to domain candles of the timeframe `tf` that was asked.
///
/// The API gives one time per bar. It is used for both `open_ts` and `close_ts`. Whether that time is
/// the open or the close of the bar is not verified against the live API, so do not rely on the
/// difference between the two fields.
pub fn map_rest_candles(resp: &CandlesResponse, tf: Timeframe, now: Instant) -> Vec<Candle> {
    let instrument = instrument_id_ticker(&resp.ticker, &resp.class_code);
    resp.bars
        .iter()
        .map(|bar| {
            let close_ts = super::unix_nanos(bar.time);
            Candle {
                instrument: instrument.clone(),
                timeframe: tf,
                open: Price::new(bar.open),
                high: Price::new(bar.high),
                low: Price::new(bar.low),
                close: Price::new(bar.close),
                volume: Qty::new(bar.volume),
                open_ts: close_ts,
                close_ts,
                ts: timestamps(now, Some(close_ts)),
            }
        })
        .collect()
}

/// Maps a REST order book answer to an [`Event::OrderBook`] holding an [`OrderBook::Snapshot`].
///
/// BCS gives full books only, so the result is always a snapshot. Levels keep the broker's order.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use brokapi_bcs_protocol::http::models::OrderBookResponse;
/// use brokapi_bcs::map::market::map_rest_order_book;
/// use brokapi_core::{Event, OrderBook};
///
/// let resp: OrderBookResponse = serde_json::from_str(
///     r#"{"ticker":"SBER","classCode":"TQBR","bids":[{"price":299.9,"quantity":100}],
///         "asks":[{"price":300.1,"quantity":50}]}"#,
/// )
/// .unwrap();
/// let Event::OrderBook(OrderBook::Snapshot { bids, asks, .. }) = map_rest_order_book(&resp, Instant::now())
/// else {
///     panic!("a snapshot")
/// };
/// assert_eq!((bids.len(), asks.len()), (1, 1));
/// ```
pub fn map_rest_order_book(book: &OrderBookResponse, now: Instant) -> Event {
    let instrument = instrument_id_ticker(&book.ticker, &book.class_code);
    let broker_ts = parse_broker_ts(book.date_time.as_deref());
    Event::OrderBook(map_order_book_snapshot(
        &instrument,
        &book.bids,
        &book.asks,
        timestamps(now, broker_ts),
    ))
}

/// Maps a market data WebSocket message to an event.
///
/// `None` for messages that carry no data: subscribe confirmations, error answers, undefined
/// messages, a trade with an unknown side, and a candle message that lacks a ticker, a class code or a price. `sub` is passed
/// into the result unchanged. The core fills it in.
pub fn map_ws_market_data(
    msg: &MarketDataMessage,
    now: Instant,
    sub: Option<brokapi_core::SubscriptionId>,
) -> Option<MappedMarketEvent> {
    let event = match msg {
        MarketDataMessage::OrderBook(data) => Event::OrderBook(map_ws_order_book(data, now)),
        MarketDataMessage::Quotes(data) => Event::Quote(map_quotes(data, now)),
        MarketDataMessage::LastTrades(data) => Event::MarketTrade(map_last_trade(data, now)?),
        MarketDataMessage::LastCandle(data) => map_last_candle(data, now)?,
        MarketDataMessage::QuotesSuccess(_)
        | MarketDataMessage::OrderBookSuccess(_)
        | MarketDataMessage::LastTradesSuccess(_)
        | MarketDataMessage::LastCandleSuccess(_)
        | MarketDataMessage::ErrorResponse(_)
        | MarketDataMessage::Undefined => return None,
    };
    Some(MappedMarketEvent { sub, event })
}

/// An event mapped from a market data message, with the subscription it belongs to if known.
#[derive(Debug, Clone, PartialEq)]
pub struct MappedMarketEvent {
    /// The subscription the event belongs to, when the caller of the mapper knew it.
    pub sub: Option<brokapi_core::SubscriptionId>,
    /// The mapped event.
    pub event: Event,
}

/// Maps a WebSocket order book message to an [`OrderBook::Snapshot`]. Every message of this stream
/// is a full book, not a delta.
pub fn map_ws_order_book(data: &OrderBookData, now: Instant) -> OrderBook {
    let instrument = instrument_id_ticker(&data.ticker, &data.class_code);
    let broker_ts = parse_broker_ts(data.date_time.as_deref());
    map_order_book_snapshot(
        &instrument,
        &data.bids,
        &data.asks,
        timestamps(now, broker_ts),
    )
}

fn map_order_book_snapshot(
    instrument: &brokapi_core::InstrumentId,
    bids: &[impl BookLevelSource],
    asks: &[impl BookLevelSource],
    ts: brokapi_core::Timestamps,
) -> OrderBook {
    OrderBook::Snapshot {
        instrument: instrument.clone(),
        bids: bids
            .iter()
            .map(|l| BookLevel {
                price: Price::new(l.price()),
                qty: Qty::new(l.quantity()),
            })
            .collect(),
        asks: asks
            .iter()
            .map(|l| BookLevel {
                price: Price::new(l.price()),
                qty: Qty::new(l.quantity()),
            })
            .collect(),
        ts,
    }
}

/// A price level of an order book in either wire form (REST or WebSocket), so one function can map both.
pub trait BookLevelSource {
    /// Price of the level.
    fn price(&self) -> Decimal;
    /// Quantity at the level, as the broker sends it.
    fn quantity(&self) -> Decimal;
}

impl BookLevelSource for brokapi_bcs_protocol::http::models::OrderBookLevel {
    fn price(&self) -> Decimal {
        self.price
    }
    fn quantity(&self) -> Decimal {
        self.quantity
    }
}

impl BookLevelSource for brokapi_bcs_protocol::ws::models::OrderBookLevel {
    fn price(&self) -> Decimal {
        self.price
    }
    fn quantity(&self) -> Decimal {
        self.quantity
    }
}

/// Maps a quotes message to a [`Quote`].
///
/// The message has one `volume`, and it is put on both sides (bid and ask). A side the message does
/// not carry is `None`, and a missing volume is `0`. Whether that volume belongs to the bid, the ask
/// or the last trade is not verified against the live API.
pub fn map_quotes(data: &QuotesData, now: Instant) -> Quote {
    let instrument = instrument_id_ticker(&data.ticker, &data.class_code);
    let broker_ts = parse_broker_ts(data.date_time.as_deref());
    Quote {
        instrument,
        bid: data.bid.map(|p| BookLevel {
            price: Price::new(p),
            qty: Qty::new(data.volume.unwrap_or(Decimal::ZERO)),
        }),
        ask: data.ask.map(|p| BookLevel {
            price: Price::new(p),
            qty: Qty::new(data.volume.unwrap_or(Decimal::ZERO)),
        }),
        ts: timestamps(now, broker_ts),
    }
}

/// Maps a market trade message to a [`MarketTrade`].
///
/// The side is `BUY` / `1` or `SELL` / `2`. A trade whose side is none of these is dropped (`None`)
/// rather than guessed, like the REST tape ([`map_rest_last_trade`]): a wrong side would corrupt
/// anything built from the stream.
pub fn map_last_trade(data: &LastTradesData, now: Instant) -> Option<MarketTrade> {
    let instrument = instrument_id_ticker(&data.ticker, &data.class_code);
    let side = match data.side.as_deref() {
        Some("1") | Some("BUY") => Side::Buy,
        Some("2") | Some("SELL") => Side::Sell,
        _ => return None,
    };
    let broker_ts = parse_broker_ts(data.date_time.as_deref());
    Some(MarketTrade {
        instrument,
        price: Price::new(data.price),
        qty: Qty::new(data.quantity),
        side,
        trade_id: None,
        ts: timestamps(now, broker_ts),
    })
}

/// One trade of the tape (`POST /last-trades`) for `instrument`, the one asked for: the records of the
/// docs carry no instrument. Side is `1` buy / `2` sell (older samples: `BUY` / `SELL`); a record whose
/// side is neither is dropped rather than guessed.
pub fn map_rest_last_trade(
    trade: &LastTradeRecord,
    instrument: &brokapi_core::InstrumentId,
    now: Instant,
) -> Option<MarketTrade> {
    let side = match trade.side.as_deref() {
        Some("1") | Some("BUY") => Side::Buy,
        Some("2") | Some("SELL") => Side::Sell,
        _ => return None,
    };
    let broker_ts = parse_broker_ts(trade.date_time.as_deref());
    Some(MarketTrade {
        instrument: instrument.clone(),
        price: Price::new(trade.price),
        qty: Qty::new(trade.quantity),
        side,
        trade_id: None,
        ts: timestamps(now, broker_ts),
    })
}

fn map_last_candle(data: &LastCandleData, now: Instant) -> Option<Event> {
    let ticker = data.ticker.as_deref()?;
    let class_code = data.class_code.as_deref()?;
    let instrument = instrument_id_ticker(ticker, class_code);
    let timeframe = data
        .time_frame
        .as_deref()
        .and_then(parse_timeframe)
        .unwrap_or(Timeframe::M1);
    let broker_ts = parse_broker_ts(data.timestamp.as_deref());
    let close_ts = broker_ts.unwrap_or_else(|| timestamps(now, None).rx_ts);
    Some(Event::Candle(brokapi_core::Candle {
        instrument,
        timeframe,
        open: Price::new(data.open?),
        high: Price::new(data.high?),
        low: Price::new(data.low?),
        close: Price::new(data.close?),
        volume: Qty::new(data.volume.unwrap_or(Decimal::ZERO)),
        open_ts: close_ts,
        close_ts,
        ts: timestamps(now, broker_ts),
    }))
}

fn parse_timeframe(raw: &str) -> Option<Timeframe> {
    match raw {
        "M1" => Some(Timeframe::M1),
        "M5" => Some(Timeframe::M5),
        "M10" => Some(Timeframe::M10),
        "M15" => Some(Timeframe::M15),
        "M30" => Some(Timeframe::M30),
        "H1" => Some(Timeframe::H1),
        "H4" => Some(Timeframe::H4),
        // The docs call them `D` and `W`; the `1` forms are accepted too.
        "D" | "D1" => Some(Timeframe::D1),
        "W" | "W1" => Some(Timeframe::W1),
        "MN" => Some(Timeframe::MN),
        _ => None,
    }
}

/// Detect BCS market-data subscribe error payloads (`errors` array).
///
/// Returns the message of the first error, or a default text when it has none. `None` when the text is
/// not JSON or has no non-empty `errors` array.
///
/// # Examples
///
/// ```
/// use brokapi_bcs::map::market::parse_subscribe_error;
///
/// let text = r#"{"errors":[{"message":"unknown instrument"}]}"#;
/// assert_eq!(parse_subscribe_error(text).as_deref(), Some("unknown instrument"));
/// assert_eq!(parse_subscribe_error(r#"{"data":[]}"#), None);
/// ```
pub fn parse_subscribe_error(text: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    let errors = v.get("errors")?.as_array()?;
    let first = errors.first()?;
    Some(
        first
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("subscription failed")
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(side: Option<&str>) -> LastTradesData {
        serde_json::from_value(serde_json::json!({
            "ticker": "SBER", "classCode": "TQBR", "price": 100.5, "quantity": 3.0, "side": side
        }))
        .unwrap()
    }

    #[test]
    fn stream_trade_side_is_read_and_never_guessed() {
        let now = Instant::now();
        assert_eq!(
            map_last_trade(&trade(Some("BUY")), now).unwrap().side,
            Side::Buy
        );
        assert_eq!(
            map_last_trade(&trade(Some("2")), now).unwrap().side,
            Side::Sell
        );
        assert!(map_last_trade(&trade(None), now).is_none());
        assert!(map_last_trade(&trade(Some("???")), now).is_none());
    }
}
