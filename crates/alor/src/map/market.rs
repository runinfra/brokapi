//! Market data mapping: ALOR wire models to `brokapi-core` domain types.

use std::time::Instant;

use brokapi_alor_protocol::http::models::{
    Bar, BookLevel as WireLevel, HistoryResponse, MarketTradeRecord, OrderBook as WireBook,
    QuoteRecord,
};
use brokapi_core::{
    BookLevel, Candle, InstrumentId, MarketTrade, OrderBook, Price, Qty, Quote, Side, Timeframe,
};

use super::{ms_to_nanos, secs_to_nanos, timestamps};

fn levels(levels: &[WireLevel]) -> Vec<BookLevel> {
    levels
        .iter()
        .map(|l| BookLevel {
            price: Price::new(l.price),
            qty: Qty::new(l.volume),
        })
        .collect()
}

/// An order book, from the REST answer or a stream message: always a whole snapshot (the broker sends the whole
/// book, to the depth asked, in every message). The volume is as the broker reports it.
pub fn order_book(book: &WireBook, instrument: &InstrumentId, now: Instant) -> OrderBook {
    OrderBook::Snapshot {
        instrument: instrument.clone(),
        bids: levels(&book.bids),
        asks: levels(&book.asks),
        ts: timestamps(now, book.ms_timestamp.map(ms_to_nanos)),
    }
}

/// One candle of the timeframe `tf`. The broker gives the open time; the close is the open plus the length of the
/// timeframe.
pub fn candle(bar: &Bar, instrument: &InstrumentId, tf: Timeframe, now: Instant) -> Candle {
    let open_ts = secs_to_nanos(bar.time);
    let close_ts = secs_to_nanos(bar.time.saturating_add(tf.seconds() as i64));
    Candle {
        instrument: instrument.clone(),
        timeframe: tf,
        open: Price::new(bar.open),
        high: Price::new(bar.high),
        low: Price::new(bar.low),
        close: Price::new(bar.close),
        volume: Qty::new(bar.volume),
        open_ts,
        close_ts,
        ts: timestamps(now, Some(open_ts)),
    }
}

/// The candles of a history answer, oldest first.
pub fn candles(
    resp: &HistoryResponse,
    instrument: &InstrumentId,
    tf: Timeframe,
    now: Instant,
) -> Vec<Candle> {
    resp.history
        .iter()
        .map(|b| candle(b, instrument, tf, now))
        .collect()
}

/// The side of a trade of the market is the side of the aggressor (`side` of the record). A trade without a
/// usable side is dropped: the domain type needs one.
pub fn side_of(raw: Option<&str>) -> Option<Side> {
    match raw?.to_ascii_lowercase().as_str() {
        "buy" => Some(Side::Buy),
        "sell" => Some(Side::Sell),
        _ => None,
    }
}

/// A trade of the market. The quantity is as the broker reports it.
pub fn market_trade(
    r: &MarketTradeRecord,
    instrument: &InstrumentId,
    now: Instant,
) -> Option<MarketTrade> {
    let side = side_of(r.side.as_deref())?;
    Some(MarketTrade {
        instrument: instrument.clone(),
        price: Price::new(r.price),
        qty: Qty::new(r.qty),
        side,
        trade_id: r.id.map(|i| i.to_string()),
        ts: timestamps(now, r.timestamp.map(ms_to_nanos)),
    })
}

/// A quotation: the best bid and ask. The broker gives their volumes in lots; with the lot size of the record
/// they become pieces. A side with no price (an empty book side) is `None`.
pub fn quote(q: &QuoteRecord, instrument: &InstrumentId, now: Instant) -> Quote {
    let lot = q.lotsize.unwrap_or(brokapi_core::Decimal::ONE);
    let level = |price: Option<brokapi_core::Decimal>, vol: Option<brokapi_core::Decimal>| {
        price.filter(|p| !p.is_zero()).map(|p| BookLevel {
            price: Price::new(p),
            qty: Qty::new(vol.unwrap_or_default() * lot),
        })
    };
    Quote {
        instrument: instrument.clone(),
        bid: level(q.bid, q.bid_vol),
        ask: level(q.ask, q.ask_vol),
        ts: timestamps(now, q.ob_ms_timestamp.map(ms_to_nanos)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::instrument_id;
    use brokapi_core::{Decimal, dec};

    fn sber() -> InstrumentId {
        instrument_id("SBER", "TQBR")
    }

    #[test]
    fn a_book_keeps_the_digits_of_the_prices() {
        let wire: WireBook = serde_json::from_str(
            r#"{"bids":[{"price":244.30,"volume":157}],"asks":[{"price":244.31,"volume":288}],"msTimestamp":1702631123780,"existing":true}"#,
        )
        .unwrap();
        let OrderBook::Snapshot { bids, asks, ts, .. } = order_book(&wire, &sber(), Instant::now())
        else {
            panic!("a snapshot");
        };
        assert_eq!(bids[0].price.to_string(), "244.30");
        assert_eq!(asks[0].qty, Qty::from(288));
        assert_eq!(ts.broker_ts.unwrap().as_nanos(), 1_702_631_123_780_000_000);
    }

    #[test]
    fn a_candle_closes_one_timeframe_after_it_opens() {
        let bar = Bar {
            time: 1_700_000_000,
            open: dec!(1),
            high: dec!(2),
            low: dec!(1),
            close: dec!(2),
            volume: dec!(5),
        };
        let c = candle(&bar, &sber(), Timeframe::M5, Instant::now());
        assert_eq!(
            c.close_ts.as_nanos() - c.open_ts.as_nanos(),
            300_000_000_000
        );
    }

    #[test]
    fn quote_volumes_are_lots_made_pieces() {
        let q: QuoteRecord = serde_json::from_str(
            r#"{"symbol":"SBER","bid":100.5,"ask":100.6,"bid_vol":3,"ask_vol":2,"lotsize":10}"#,
        )
        .unwrap();
        let out = quote(&q, &sber(), Instant::now());
        assert_eq!(out.bid.unwrap().qty, Qty::from(30));
        assert_eq!(out.ask.unwrap().qty, Qty::from(20));
    }

    #[test]
    fn a_trade_without_a_side_is_dropped() {
        let r: MarketTradeRecord = serde_json::from_str(r#"{"id":1,"qty":1,"price":1}"#).unwrap();
        assert!(market_trade(&r, &sber(), Instant::now()).is_none());
        let r: MarketTradeRecord =
            serde_json::from_str(r#"{"id":1,"qty":1,"price":1,"side":"sell"}"#).unwrap();
        assert_eq!(
            market_trade(&r, &sber(), Instant::now()).unwrap().side,
            Side::Sell
        );
    }
}
