//! Order books, market trades, candles.

use std::time::Instant;

use brokapi_core::{
    BookLevel, Candle, InstrumentId, MarketTrade, OrderBook, Price, Qty, Quote, Side, Timeframe,
    UnixNanos,
};
use brokapi_tbank_protocol::v1 as pb;

use crate::ids::{price_of_quotation, proto_to_nanos, stamps};

/// Order book depths T-Bank accepts (`SUBSCRIPTION_STATUS_DEPTH_IS_INVALID`: 1, 10, 20, 30, 40, 50).
pub const VALID_DEPTHS: [u32; 6] = [1, 10, 20, 30, 40, 50];
/// The deepest order book T-Bank serves.
pub const MAX_DEPTH: u32 = 50;

/// Smallest valid wire depth that covers `depth` levels.
pub fn wire_depth(depth: u32) -> u32 {
    let depth = depth.max(1);
    VALID_DEPTHS
        .iter()
        .copied()
        .find(|d| *d >= depth)
        .unwrap_or(MAX_DEPTH)
}

fn level(o: &pb::Order) -> BookLevel {
    BookLevel {
        price: price(&o.price),
        qty: Qty::from(o.quantity),
    }
}

/// Levels best-first: bids by price descending, asks ascending, cut to `depth`.
pub fn levels(orders: &[pb::Order], bids: bool, depth: usize) -> Vec<BookLevel> {
    let mut v: Vec<BookLevel> = orders.iter().map(level).collect();
    v.sort_by(|a, b| {
        let o = a.price.cmp(&b.price);
        if bids { o.reverse() } else { o }
    });
    v.truncate(depth);
    v
}

/// An order book snapshot from the two sides of a broker message, cut to `depth` levels.
///
/// Quantities are in lots. The time is the broker's `orderbook_ts` when given.
pub fn book_snapshot(
    instrument: &InstrumentId,
    bids: &[pb::Order],
    asks: &[pb::Order],
    depth: usize,
    broker_ts: Option<&prost_types::Timestamp>,
    now: Instant,
) -> OrderBook {
    OrderBook::Snapshot {
        instrument: instrument.clone(),
        bids: levels(bids, true, depth),
        asks: levels(asks, false, depth),
        ts: stamps(now, broker_ts),
    }
}

/// The best bid and the best ask of a broker order book message.
///
/// T-Bank has no quote stream: a quote is the first level of each side. A side with no orders is
/// `None`.
pub fn quote(
    instrument: &InstrumentId,
    bids: &[pb::Order],
    asks: &[pb::Order],
    broker_ts: Option<&prost_types::Timestamp>,
    now: Instant,
) -> Quote {
    Quote {
        instrument: instrument.clone(),
        bid: levels(bids, true, 1).into_iter().next(),
        ask: levels(asks, false, 1).into_iter().next(),
        ts: stamps(now, broker_ts),
    }
}

/// Trades without a direction cannot be given a `Side` and are skipped.
pub fn trade(instrument: &InstrumentId, t: &pb::Trade, now: Instant) -> Option<MarketTrade> {
    let side = match t.direction() {
        pb::TradeDirection::Buy => Side::Buy,
        pb::TradeDirection::Sell => Side::Sell,
        pb::TradeDirection::Unspecified => return None,
    };
    Some(MarketTrade {
        instrument: instrument.clone(),
        price: price(&t.price),
        qty: Qty::from(t.quantity),
        side,
        trade_id: None,
        ts: stamps(now, t.time.as_ref()),
    })
}

/// Length of one candle of timeframe `tf`, in nanoseconds.
///
/// A month is taken as 30 days, because the broker's candle carries only its open time and the close
/// time has to be derived.
///
/// # Examples
///
/// ```
/// use brokapi_core::Timeframe;
/// use brokapi_tbank::map::market::timeframe_duration_nanos;
///
/// assert_eq!(timeframe_duration_nanos(Timeframe::M5), 300_000_000_000);
/// assert_eq!(timeframe_duration_nanos(Timeframe::MN), 30 * 24 * 3600 * 1_000_000_000);
/// ```
pub fn timeframe_duration_nanos(tf: Timeframe) -> i64 {
    const MIN: i64 = 60 * 1_000_000_000;
    match tf {
        Timeframe::M1 => MIN,
        Timeframe::M5 => 5 * MIN,
        Timeframe::M10 => 10 * MIN,
        Timeframe::M15 => 15 * MIN,
        Timeframe::M30 => 30 * MIN,
        Timeframe::H1 => 60 * MIN,
        Timeframe::H4 => 240 * MIN,
        Timeframe::D1 => 1440 * MIN,
        Timeframe::W1 => 7 * 1440 * MIN,
        Timeframe::MN => 30 * 1440 * MIN,
    }
}

/// Timeframe to the interval name used when subscribing to candles.
pub fn subscription_interval(tf: Timeframe) -> pb::SubscriptionInterval {
    match tf {
        Timeframe::M1 => pb::SubscriptionInterval::OneMinute,
        Timeframe::M5 => pb::SubscriptionInterval::FiveMinutes,
        Timeframe::M10 => pb::SubscriptionInterval::SubscriptionInterval10Min,
        Timeframe::M15 => pb::SubscriptionInterval::FifteenMinutes,
        Timeframe::M30 => pb::SubscriptionInterval::SubscriptionInterval30Min,
        Timeframe::H1 => pb::SubscriptionInterval::OneHour,
        Timeframe::H4 => pb::SubscriptionInterval::SubscriptionInterval4Hour,
        Timeframe::D1 => pb::SubscriptionInterval::OneDay,
        Timeframe::W1 => pb::SubscriptionInterval::Week,
        Timeframe::MN => pb::SubscriptionInterval::Month,
    }
}

/// The interval of a candle stream message to timeframe, or `None` for an interval with no
/// counterpart in the domain model.
pub fn timeframe_from_interval(i: pb::SubscriptionInterval) -> Option<Timeframe> {
    Some(match i {
        pb::SubscriptionInterval::OneMinute => Timeframe::M1,
        pb::SubscriptionInterval::FiveMinutes => Timeframe::M5,
        pb::SubscriptionInterval::SubscriptionInterval10Min => Timeframe::M10,
        pb::SubscriptionInterval::FifteenMinutes => Timeframe::M15,
        pb::SubscriptionInterval::SubscriptionInterval30Min => Timeframe::M30,
        pb::SubscriptionInterval::OneHour => Timeframe::H1,
        pb::SubscriptionInterval::SubscriptionInterval4Hour => Timeframe::H4,
        pb::SubscriptionInterval::OneDay => Timeframe::D1,
        pb::SubscriptionInterval::Week => Timeframe::W1,
        pb::SubscriptionInterval::Month => Timeframe::MN,
        _ => return None,
    })
}

/// Timeframe to the interval name used when requesting historic candles.
pub fn candle_interval(tf: Timeframe) -> pb::CandleInterval {
    match tf {
        Timeframe::M1 => pb::CandleInterval::CandleInterval1Min,
        Timeframe::M5 => pb::CandleInterval::CandleInterval5Min,
        Timeframe::M10 => pb::CandleInterval::CandleInterval10Min,
        Timeframe::M15 => pb::CandleInterval::CandleInterval15Min,
        Timeframe::M30 => pb::CandleInterval::CandleInterval30Min,
        Timeframe::H1 => pb::CandleInterval::Hour,
        Timeframe::H4 => pb::CandleInterval::CandleInterval4Hour,
        Timeframe::D1 => pb::CandleInterval::Day,
        Timeframe::W1 => pb::CandleInterval::Week,
        Timeframe::MN => pb::CandleInterval::Month,
    }
}

fn price(q: &Option<pb::Quotation>) -> Price {
    q.as_ref().map(price_of_quotation).unwrap_or(Price::ZERO)
}

fn candle_times(open: &Option<prost_types::Timestamp>, tf: Timeframe) -> (UnixNanos, UnixNanos) {
    let open_ts = open.as_ref().map(proto_to_nanos).unwrap_or(UnixNanos::ZERO);
    (
        open_ts,
        UnixNanos::from_nanos(open_ts.as_nanos() + timeframe_duration_nanos(tf)),
    )
}

/// A candle of the candle stream to [`Candle`].
///
/// Volume is in lots. `close_ts` is derived from the open time and the timeframe. The candle is the
/// current state of a candle that may still be forming.
pub fn stream_candle(
    instrument: &InstrumentId,
    tf: Timeframe,
    c: &pb::Candle,
    now: Instant,
) -> Candle {
    let (open_ts, close_ts) = candle_times(&c.time, tf);
    Candle {
        instrument: instrument.clone(),
        timeframe: tf,
        open: price(&c.open),
        high: price(&c.high),
        low: price(&c.low),
        close: price(&c.close),
        volume: Qty::from(c.volume),
        open_ts,
        close_ts,
        ts: stamps(now, c.last_trade_ts.as_ref().or(c.time.as_ref())),
    }
}

/// A candle of `GetCandles` to [`Candle`].
///
/// Volume is in lots. `close_ts` is derived from the open time and the timeframe.
pub fn historic_candle(
    instrument: &InstrumentId,
    tf: Timeframe,
    c: &pb::HistoricCandle,
    now: Instant,
) -> Candle {
    let (open_ts, close_ts) = candle_times(&c.time, tf);
    Candle {
        instrument: instrument.clone(),
        timeframe: tf,
        open: price(&c.open),
        high: price(&c.high),
        low: price(&c.low),
        close: price(&c.close),
        volume: Qty::from(c.volume),
        open_ts,
        close_ts,
        ts: stamps(now, c.time.as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{Decimal, dec};
    use brokapi_core::{InstrumentKey, NativeId};
    use brokapi_tbank_protocol::money::quotation;

    fn id() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::Uid("u".into()),
        )
    }

    fn o(p: &str, q: i64) -> pb::Order {
        pb::Order {
            price: Some(quotation(p)),
            quantity: q,
        }
    }

    #[test]
    fn depth_rounds_up_to_valid() {
        assert_eq!(wire_depth(0), 1);
        assert_eq!(wire_depth(1), 1);
        assert_eq!(wire_depth(2), 10);
        assert_eq!(wire_depth(20), 20);
        assert_eq!(wire_depth(21), 30);
        assert_eq!(wire_depth(500), 50);
    }

    #[test]
    fn book_sorted_best_first_and_cut() {
        let bids = [o("99.0", 5), o("100.0", 3), o("98.5", 1)];
        let asks = [o("101.5", 2), o("100.5", 7)];
        let OrderBook::Snapshot {
            bids: b, asks: a, ..
        } = book_snapshot(&id(), &bids, &asks, 2, None, Instant::now())
        else {
            panic!("snapshot expected");
        };
        assert_eq!(
            b.iter().map(|l| l.price.decimal()).collect::<Vec<_>>(),
            vec![dec!(100.0), dec!(99.0)]
        );
        assert_eq!(
            a.iter().map(|l| l.price.decimal()).collect::<Vec<_>>(),
            vec![dec!(100.5), dec!(101.5)]
        );
        assert_eq!(b[0].qty.decimal(), dec!(3.0));
    }

    #[test]
    fn quote_is_best_level_of_each_side() {
        let q = quote(
            &id(),
            &[o("99.0", 5), o("100.0", 3)],
            &[o("100.5", 7)],
            None,
            Instant::now(),
        );
        assert_eq!(q.bid.unwrap().price.decimal(), dec!(100.0));
        assert_eq!(q.ask.unwrap().qty.decimal(), dec!(7.0));
    }

    #[test]
    fn empty_side_gives_no_quote_level() {
        let q = quote(&id(), &[], &[o("100.5", 7)], None, Instant::now());
        assert!(q.bid.is_none());
    }

    #[test]
    fn trade_direction_and_units() {
        let t = pb::Trade {
            direction: pb::TradeDirection::Sell as i32,
            price: Some(pb::Quotation {
                units: 273,
                nano: 450_000_000,
            }),
            quantity: 12,
            ..Default::default()
        };
        let m = trade(&id(), &t, Instant::now()).unwrap();
        assert_eq!(
            (m.side, m.price.decimal(), m.qty.decimal()),
            (Side::Sell, dec!(273.45), dec!(12.0))
        );
        let none = pb::Trade {
            quantity: 1,
            ..Default::default()
        };
        assert!(trade(&id(), &none, Instant::now()).is_none());
    }

    #[test]
    fn timeframe_roundtrip() {
        for tf in [
            Timeframe::M1,
            Timeframe::M5,
            Timeframe::M10,
            Timeframe::M15,
            Timeframe::M30,
            Timeframe::H1,
            Timeframe::H4,
            Timeframe::D1,
            Timeframe::W1,
            Timeframe::MN,
        ] {
            assert_eq!(timeframe_from_interval(subscription_interval(tf)), Some(tf));
        }
    }

    #[test]
    fn candle_close_time_from_timeframe() {
        let c = pb::HistoricCandle {
            open: Some(quotation("10.0")),
            high: Some(quotation("12.0")),
            low: Some(quotation("9.5")),
            close: Some(quotation("11.0")),
            volume: 100,
            time: Some(prost_types::Timestamp {
                seconds: 1_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        let m = historic_candle(&id(), Timeframe::M5, &c, Instant::now());
        assert_eq!(m.open_ts.as_nanos(), 1_000_000_000_000);
        assert_eq!(m.close_ts.as_nanos(), 1_000_000_000_000 + 300_000_000_000);
        assert_eq!(
            (m.high.decimal(), m.volume.decimal()),
            (dec!(12.0), dec!(100.0))
        );
    }
}
