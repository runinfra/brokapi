//! Market data: books, quotes, trades and candles.

use std::time::Instant;

use brokapi_bybit_protocol::http::models::{Kline, Level, PublicTradeRecord};
use brokapi_core::{
    BookLevel, Candle, InstrumentId, MarketTrade, OrderBook, Price, Qty, Quote, Timeframe,
};

use super::{broker_ts, ms_to_nanos, side_of, timestamps};

/// The levels of one side, as the library spells them.
pub fn levels(levels: &[(brokapi_core::Decimal, brokapi_core::Decimal)]) -> Vec<BookLevel> {
    levels
        .iter()
        .map(|(p, q)| BookLevel {
            price: Price::new(*p),
            qty: Qty::new(*q),
        })
        .collect()
}

/// A snapshot of the book from the REST answer.
pub fn order_book(
    bids: &[(brokapi_core::Decimal, brokapi_core::Decimal)],
    asks: &[(brokapi_core::Decimal, brokapi_core::Decimal)],
    ts_ms: i64,
    instrument: &InstrumentId,
    now: Instant,
) -> OrderBook {
    OrderBook::Snapshot {
        instrument: instrument.clone(),
        bids: levels(bids),
        asks: levels(asks),
        ts: timestamps(now, broker_ts(ts_ms)),
    }
}

/// The best bid and ask from the first level of each side.
pub fn quote(
    bid: Option<&Level>,
    ask: Option<&Level>,
    ts_ms: i64,
    instrument: &InstrumentId,
    now: Instant,
) -> Quote {
    let level = |l: Option<&Level>| {
        l.filter(|l| !l.0.is_zero()).map(|l| BookLevel {
            price: Price::new(l.0),
            qty: Qty::new(l.1),
        })
    };
    Quote {
        instrument: instrument.clone(),
        bid: level(bid),
        ask: level(ask),
        ts: timestamps(now, broker_ts(ts_ms)),
    }
}

/// A public trade of the REST answer.
pub fn market_trade(
    r: &PublicTradeRecord,
    instrument: &InstrumentId,
    now: Instant,
) -> Option<MarketTrade> {
    Some(MarketTrade {
        instrument: instrument.clone(),
        price: Price::new(r.price),
        qty: Qty::new(r.size),
        side: side_of(&r.side)?,
        trade_id: (!r.exec_id.is_empty()).then(|| r.exec_id.clone()),
        ts: timestamps(now, broker_ts(r.time)),
    })
}

/// A candle from a row of the history. The candle closes one timeframe after it opens.
pub fn candle(k: &Kline, instrument: &InstrumentId, tf: Timeframe, now: Instant) -> Candle {
    let open_ts = ms_to_nanos(k.start_ms);
    let close_ts = ms_to_nanos(k.start_ms.saturating_add(tf.seconds() as i64 * 1000));
    Candle {
        instrument: instrument.clone(),
        timeframe: tf,
        open: Price::new(k.open),
        high: Price::new(k.high),
        low: Price::new(k.low),
        close: Price::new(k.close),
        volume: Qty::new(k.volume),
        open_ts,
        close_ts,
        ts: timestamps(now, Some(open_ts)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::instrument_id;
    use brokapi_core::{Decimal, dec};

    fn btc() -> InstrumentId {
        instrument_id("BTCUSDT", "linear")
    }

    #[test]
    fn a_book_keeps_the_digits_of_the_prices() {
        let book = order_book(
            &[(dec!(81695.20), dec!(0.699))],
            &[(dec!(81695.30), dec!(7.638))],
            1_792_760_139_559,
            &btc(),
            Instant::now(),
        );
        let OrderBook::Snapshot { bids, asks, ts, .. } = book else {
            panic!("a snapshot");
        };
        assert_eq!(bids[0].price.to_string(), "81695.20");
        assert_eq!(asks[0].qty.to_string(), "7.638");
        assert_eq!(ts.broker_ts.unwrap().as_nanos(), 1_792_760_139_559_000_000);
    }

    #[test]
    fn a_candle_closes_one_timeframe_after_it_opens() {
        let k = Kline {
            start_ms: 1_700_000_000_000,
            open: dec!(1),
            high: dec!(2),
            low: dec!(1),
            close: dec!(2),
            volume: dec!(5),
        };
        let c = candle(&k, &btc(), Timeframe::M5, Instant::now());
        assert_eq!(
            c.close_ts.as_nanos() - c.open_ts.as_nanos(),
            300_000_000_000
        );
    }

    #[test]
    fn an_empty_side_is_no_quote_level() {
        let q = quote(
            None,
            Some(&Level(dec!(2), dec!(1))),
            0,
            &btc(),
            Instant::now(),
        );
        assert!(q.bid.is_none());
        assert_eq!(q.ask.unwrap().price.to_string(), "2");
    }
}
