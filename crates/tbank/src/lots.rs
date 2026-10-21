//! Lots. T-Bank counts orders, order book levels, trades and candle volumes in LOTS, the library
//! counts in units of the instrument (see [`Qty`]). This module is the bridge: the lot size of each
//! instrument, learned from the directory once and remembered, and the conversions both ways.
//!
//! The lot size is `Instrument.lot` of `GetInstrumentBy`, and `lot_size` of an order state message of
//! the order stream. A connector looks it up before the first order or subscription on an
//! instrument (one `GetInstrumentBy`, against the quota of that method) and never again.
//!
//! # Examples
//!
//! ```
//! use brokapi_core::Qty;
//! use brokapi_tbank::lots::{lots_to_qty, pieces_to_lots};
//!
//! // A lot of SBER is 10 shares: 30 shares are 3 lots, 3 lots are 30 shares.
//! assert_eq!(pieces_to_lots(Qty::from(30), 10)?, 3);
//! assert_eq!(lots_to_qty(3, 10), Qty::from(30));
//! // Not a whole number of lots: refused, never rounded.
//! assert!(pieces_to_lots(Qty::from(25), 10).is_err());
//! # Ok::<(), brokapi_core::Error>(())
//! ```

use std::collections::HashMap;
use std::sync::Mutex;

use brokapi_core::{
    BookLevel, Candle, Decimal, Error, Event, InstrumentId, MarketTrade, NativeId, OrderBook, Qty,
};
use brokapi_tbank_protocol::v1 as pb;

use crate::ids::instrument_ref;

/// The lot sizes of the instruments this connector has met. Cheap to share behind an `Arc`.
///
/// An instrument is remembered under every name a message can call it: the uid, the figi and the
/// `TICKER_CLASSCODE` form.
#[derive(Debug, Default)]
pub struct LotBook {
    by_name: Mutex<HashMap<String, i64>>,
}

fn names(uid: &str, figi: &str, ticker: &str, class_code: &str) -> Vec<String> {
    let mut v = Vec::new();
    for n in [uid, figi] {
        if !n.is_empty() {
            v.push(n.to_string());
        }
    }
    if !ticker.is_empty() {
        v.push(format!("{ticker}_{class_code}").to_uppercase());
    }
    v
}

impl LotBook {
    /// An empty book.
    pub fn new() -> Self {
        Self::default()
    }

    /// Remembers the lot of an instrument under its uid, figi and ticker with class code. A lot below 1
    /// is ignored.
    pub fn put(&self, uid: &str, figi: &str, ticker: &str, class_code: &str, lot: i64) {
        if lot < 1 {
            return;
        }
        let mut map = self.by_name.lock().expect("lot book poisoned");
        for n in names(uid, figi, ticker, class_code) {
            map.insert(n, lot);
        }
    }

    /// Remembers the lot from a directory record, under the name the caller asked with too.
    pub fn put_instrument(&self, asked: &InstrumentId, i: &pb::Instrument) {
        self.put(&i.uid, &i.figi, &i.ticker, &i.class_code, i64::from(i.lot));
        self.put_id(asked, i64::from(i.lot));
    }

    /// Remembers the lot under the names of an [`InstrumentId`].
    pub fn put_id(&self, id: &InstrumentId, lot: i64) {
        if lot < 1 {
            return;
        }
        let mut map = self.by_name.lock().expect("lot book poisoned");
        map.insert(instrument_ref(id).to_uppercase(), lot);
        match &id.native {
            NativeId::Uid(u) | NativeId::Figi(u) => {
                map.insert(u.clone(), lot);
            }
            _ => {}
        }
        map.insert(
            format!("{}_{}", id.key.symbol, id.key.mic).to_uppercase(),
            lot,
        );
    }

    /// The lot of an instrument by the names a message carries, if it is known.
    pub fn get_parts(&self, uid: &str, figi: &str, ticker: &str, class_code: &str) -> Option<i64> {
        let map = self.by_name.lock().expect("lot book poisoned");
        names(uid, figi, ticker, class_code)
            .iter()
            .find_map(|n| map.get(n).copied())
    }

    /// The lot of an [`InstrumentId`], if it is known.
    pub fn get(&self, id: &InstrumentId) -> Option<i64> {
        let map = self.by_name.lock().expect("lot book poisoned");
        let mut tries = vec![
            instrument_ref(id).to_uppercase(),
            format!("{}_{}", id.key.symbol, id.key.mic).to_uppercase(),
        ];
        if let NativeId::Uid(u) | NativeId::Figi(u) = &id.native {
            tries.push(u.clone());
        }
        tries.iter().find_map(|n| map.get(n).copied())
    }
}

/// Units to lots: the quantity must be a whole number of lots, at least one.
///
/// # Errors
///
/// [`Error::Rejected`] with code `invalid_qty` for zero, a negative number, a fraction, or a number that
/// is not a multiple of `lot`. Nothing is rounded.
pub fn pieces_to_lots(qty: Qty, lot: i64) -> Result<i64, Error> {
    let lot = lot.max(1);
    let bad = |why: String| Error::Rejected {
        code: "invalid_qty".into(),
        message: why,
    };
    let pieces = qty
        .to_whole()
        .filter(|n| *n >= 1)
        .ok_or_else(|| bad(format!("quantity {qty} is not a positive whole number")))?;
    if pieces % lot != 0 {
        return Err(bad(format!(
            "quantity {qty} is not a whole number of lots (one lot is {lot} units)"
        )));
    }
    Ok(pieces / lot)
}

/// Lots to units.
pub fn lots_to_qty(lots: i64, lot: i64) -> Qty {
    Qty::new(Decimal::from(lots) * Decimal::from(lot.max(1)))
}

fn level(l: &mut BookLevel, lot: Decimal) {
    l.qty = Qty::new(l.qty.decimal() * lot);
}

/// Multiplies the quantities of a market data event by the lot size: the levels of a book, the
/// best levels of a quote, the quantity of a trade, the volume of a candle. Other events stay as they are.
pub fn scale_event(ev: &mut Event, lot: i64) {
    let lot = Decimal::from(lot.max(1));
    match ev {
        Event::OrderBook(b) => scale_book(b, lot),
        Event::Quote(q) => {
            q.bid.iter_mut().for_each(|l| level(l, lot));
            q.ask.iter_mut().for_each(|l| level(l, lot));
        }
        Event::MarketTrade(t) => scale_trade(t, lot),
        Event::Candle(c) => scale_candle(c, lot),
        _ => {}
    }
}

/// The levels of a book, in units.
pub fn scale_book(book: &mut OrderBook, lot: Decimal) {
    match book {
        OrderBook::Snapshot { bids, asks, .. } | OrderBook::Increment { bids, asks, .. } => {
            bids.iter_mut()
                .chain(asks.iter_mut())
                .for_each(|l| level(l, lot));
        }
    }
}

/// The quantity of a trade, in units.
pub fn scale_trade(t: &mut MarketTrade, lot: Decimal) {
    t.qty = Qty::new(t.qty.decimal() * lot);
}

/// The volume of a candle, in units.
pub fn scale_candle(c: &mut Candle, lot: Decimal) {
    c.volume = Qty::new(c.volume.decimal() * lot);
}

/// The instrument a market data event is about, if it has one.
pub fn event_instrument(ev: &Event) -> Option<&InstrumentId> {
    match ev {
        Event::OrderBook(OrderBook::Snapshot { instrument, .. })
        | Event::OrderBook(OrderBook::Increment { instrument, .. }) => Some(instrument),
        Event::Quote(q) => Some(&q.instrument),
        Event::MarketTrade(t) => Some(&t.instrument),
        Event::Candle(c) => Some(&c.instrument),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{InstrumentKey, dec};

    fn sber() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::Uid("uid-sber".into()),
        )
    }

    #[test]
    fn conversions_refuse_what_is_not_whole_lots() {
        assert_eq!(pieces_to_lots(Qty::from(10), 10).unwrap(), 1);
        assert_eq!(pieces_to_lots(Qty::from(7), 1).unwrap(), 7);
        for bad in [
            Qty::ZERO,
            Qty::from(-10),
            Qty::new(dec!(5.5)),
            Qty::from(15),
        ] {
            let e = pieces_to_lots(bad, 10).unwrap_err();
            assert!(
                matches!(e, Error::Rejected { ref code, .. } if code == "invalid_qty"),
                "{bad}"
            );
        }
        assert_eq!(lots_to_qty(4, 10), Qty::from(40));
        // A lot below 1 is read as 1, so a broken record cannot divide by zero.
        assert_eq!(lots_to_qty(4, 0), Qty::from(4));
        assert_eq!(pieces_to_lots(Qty::from(3), 0).unwrap(), 3);
    }

    #[test]
    fn the_book_knows_an_instrument_under_every_name() {
        let book = LotBook::new();
        assert_eq!(book.get(&sber()), None);
        book.put("uid-sber", "BBG004730N88", "SBER", "TQBR", 10);
        assert_eq!(book.get(&sber()), Some(10));
        assert_eq!(book.get_parts("", "BBG004730N88", "", ""), Some(10));
        assert_eq!(book.get_parts("", "", "sber", "tqbr"), Some(10));
        assert_eq!(book.get_parts("other", "", "", ""), None);
        // A lot below one is ignored.
        book.put("u2", "", "X", "TQBR", 0);
        assert_eq!(book.get_parts("u2", "", "", ""), None);
        let ticker_id = InstrumentId::new(
            InstrumentKey::new("GAZP", "TQBR"),
            NativeId::TickerClass {
                ticker: "GAZP".into(),
                class_code: "TQBR".into(),
            },
        );
        book.put_id(&ticker_id, 10);
        assert_eq!(book.get(&ticker_id), Some(10));
        book.put_id(&sber(), 0);
        let figi_id = InstrumentId::new(
            InstrumentKey::new("LKOH", "TQBR"),
            NativeId::Figi("BBG004731032".into()),
        );
        book.put_id(&figi_id, 1);
        assert_eq!(book.get(&figi_id), Some(1));
    }

    #[test]
    fn events_are_scaled_by_the_lot() {
        use brokapi_core::{Price, Quote, Side, Timestamps, UnixNanos};
        let ts = Timestamps::new(UnixNanos::ZERO, std::time::Instant::now());
        let level = |q: i64| BookLevel {
            price: Price::from(1),
            qty: Qty::from(q),
        };
        let mut book = Event::OrderBook(OrderBook::Snapshot {
            instrument: sber(),
            bids: vec![level(1), level(2)],
            asks: vec![level(3)],
            ts,
        });
        scale_event(&mut book, 10);
        let Event::OrderBook(OrderBook::Snapshot { bids, asks, .. }) = &book else {
            panic!()
        };
        assert_eq!((bids[1].qty, asks[0].qty), (Qty::from(20), Qty::from(30)));
        assert_eq!(event_instrument(&book), Some(&sber()));

        let mut inc = Event::OrderBook(OrderBook::Increment {
            instrument: sber(),
            seq: 1,
            bids: vec![level(1)],
            asks: vec![],
            ts,
        });
        scale_event(&mut inc, 10);
        assert!(
            matches!(&inc, Event::OrderBook(OrderBook::Increment { bids, .. }) if bids[0].qty == Qty::from(10))
        );
        assert!(event_instrument(&inc).is_some());

        let mut quote = Event::Quote(Quote {
            instrument: sber(),
            bid: Some(level(2)),
            ask: None,
            ts,
        });
        scale_event(&mut quote, 10);
        assert!(
            matches!(&quote, Event::Quote(q) if q.bid.as_ref().unwrap().qty == Qty::from(20) && q.ask.is_none())
        );
        assert!(event_instrument(&quote).is_some());

        let mut trade = Event::MarketTrade(MarketTrade {
            instrument: sber(),
            price: Price::from(1),
            qty: Qty::from(5),
            side: Side::Buy,
            trade_id: None,
            ts,
        });
        scale_event(&mut trade, 10);
        assert!(matches!(&trade, Event::MarketTrade(t) if t.qty == Qty::from(50)));
        assert!(event_instrument(&trade).is_some());

        let mut other = Event::Balances(vec![]);
        scale_event(&mut other, 10);
        assert!(event_instrument(&other).is_none());
    }
}
