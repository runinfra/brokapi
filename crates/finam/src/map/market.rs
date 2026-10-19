//! Order book, quotes, trades, bars.

use std::collections::BTreeMap;
use std::time::Instant;

use brokapi_core::{
    BookLevel, Candle, Error, InstrumentId, MarketTrade, OrderBook, Price, Qty, Quote, Side,
    Timeframe, UnixNanos,
};
use brokapi_finam_protocol::pb::google::r#type::Decimal;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side as PbSide;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pb;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata::{
    TimeFrame, order_book, stream_order_book,
};
use chrono::{DateTime, Datelike, Months, TimeZone, Utc};

use crate::error::unsupported;
use crate::util::{price, price0, qty0, timestamps, ts_nanos};

/// Row action of a stream update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowAction {
    /// The level is removed from the book.
    Remove,
    /// ADD, UPDATE and an unspecified action all mean "set this level to this size".
    Set,
}

/// Side-neutral view of a book row (unary and stream rows are different generated types).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowView {
    /// Price of the level.
    pub price: Price,
    /// `sell_size` rows are asks, `buy_size` rows are bids.
    pub is_ask: bool,
    /// Size of the level in pieces. A zero size on a [`RowAction::Set`] row removes the level.
    pub size: Qty,
    /// What to do with the level.
    pub action: RowAction,
    /// Time of the row at the broker, when it sends one.
    pub ts: Option<UnixNanos>,
}

/// Rows of a unary `OrderBook` answer in the neutral form. Rows with no side or no price are skipped.
pub fn rest_rows(rows: &[order_book::Row]) -> Vec<RowView> {
    rows.iter()
        .filter_map(|r| {
            let (is_ask, size) = match &r.side {
                Some(order_book::row::Side::SellSize(d)) => (true, qty0(&Some(d.clone()))),
                Some(order_book::row::Side::BuySize(d)) => (false, qty0(&Some(d.clone()))),
                None => return None,
            };
            Some(RowView {
                price: price(&r.price)?,
                is_ask,
                size,
                action: if r.action == order_book::row::Action::Remove as i32 {
                    RowAction::Remove
                } else {
                    RowAction::Set
                },
                ts: r.timestamp.as_ref().map(ts_nanos),
            })
        })
        .collect()
}

/// Rows of an order book stream message in the neutral form. Rows with no side or no price are skipped.
pub fn stream_rows(rows: &[stream_order_book::Row]) -> Vec<RowView> {
    rows.iter()
        .filter_map(|r| {
            let (is_ask, size) = match &r.side {
                Some(stream_order_book::row::Side::SellSize(d)) => (true, qty0(&Some(d.clone()))),
                Some(stream_order_book::row::Side::BuySize(d)) => (false, qty0(&Some(d.clone()))),
                None => return None,
            };
            Some(RowView {
                price: price(&r.price)?,
                is_ask,
                size,
                action: if r.action == stream_order_book::row::Action::Remove as i32 {
                    RowAction::Remove
                } else {
                    RowAction::Set
                },
                ts: r.timestamp.as_ref().map(ts_nanos),
            })
        })
        .collect()
}

/// Result of applying rows to a [`BookState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The rows were applied to the book.
    Applied,
    /// An increment arrived while no snapshot is held: the book is unusable until resubscribed.
    NeedSnapshot,
}

/// Local order book rebuilt from Finam's snapshot + action rows.
///
/// # Examples
///
/// ```
/// use brokapi_finam::map::market::{ApplyOutcome, BookState, RowAction, RowView};
///
/// let row = |price: &str, is_ask, size: &str, action| RowView {
///     price: price.parse().unwrap(),
///     is_ask,
///     size: size.parse().unwrap(),
///     action,
///     ts: None,
/// };
/// let mut book = BookState::default();
///
/// // An increment with no snapshot under it is refused: the stream must be reopened.
/// let late = [row("100", false, "5", RowAction::Set)];
/// assert_eq!(book.apply(&late, false), ApplyOutcome::NeedSnapshot);
///
/// // A snapshot replaces the book, then rows change single levels.
/// let snapshot = [row("100", false, "5", RowAction::Set), row("101", true, "3", RowAction::Set)];
/// assert_eq!(book.apply(&snapshot, true), ApplyOutcome::Applied);
/// assert!(book.has_snapshot());
/// book.apply(&[row("100", false, "0", RowAction::Remove), row("99", false, "7", RowAction::Set)], false);
///
/// let (bids, asks) = book.levels(10);
/// assert_eq!(bids.len(), 1);
/// assert_eq!(bids[0].price, "99".parse()?);
/// assert_eq!(asks[0].qty, "3".parse()?);
/// # Ok::<(), brokapi_core::NumberError>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct BookState {
    bids: BTreeMap<Price, Qty>,
    asks: BTreeMap<Price, Qty>,
    have_snapshot: bool,
}

impl BookState {
    /// Forgets the whole book, including the snapshot. The next increment is refused until a new snapshot.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// `true` once a snapshot was applied since creation or the last [`reset`](Self::reset).
    pub fn has_snapshot(&self) -> bool {
        self.have_snapshot
    }

    /// `is_snapshot` replaces the whole book (a snapshot message is assumed to carry all levels).
    ///
    /// Without a snapshot an increment is not applied and the result is [`ApplyOutcome::NeedSnapshot`].
    pub fn apply(&mut self, rows: &[RowView], is_snapshot: bool) -> ApplyOutcome {
        if is_snapshot {
            self.bids.clear();
            self.asks.clear();
            self.have_snapshot = true;
        } else if !self.have_snapshot {
            return ApplyOutcome::NeedSnapshot;
        }
        for r in rows {
            let side = if r.is_ask {
                &mut self.asks
            } else {
                &mut self.bids
            };
            let key = r.price;
            match r.action {
                RowAction::Remove => {
                    side.remove(&key);
                }
                RowAction::Set if r.size.is_positive() => {
                    side.insert(key, r.size);
                }
                // A zero size is a removal in disguise.
                RowAction::Set => {
                    side.remove(&key);
                }
            }
        }
        ApplyOutcome::Applied
    }

    /// Best-first levels, at most `depth` per side.
    pub fn levels(&self, depth: usize) -> (Vec<BookLevel>, Vec<BookLevel>) {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .map(|(p, q)| level(*p, *q))
            .collect();
        let asks = self
            .asks
            .iter()
            .take(depth)
            .map(|(p, q)| level(*p, *q))
            .collect();
        (bids, asks)
    }
}

fn level(price: Price, qty: Qty) -> BookLevel {
    BookLevel { price, qty }
}

/// Full snapshot event from the local book.
pub fn book_snapshot(
    instrument: &InstrumentId,
    state: &BookState,
    depth: usize,
    broker_ts: Option<UnixNanos>,
    now: Instant,
) -> OrderBook {
    let (bids, asks) = state.levels(depth);
    let mut ts = timestamps(None, now);
    ts.broker_ts = broker_ts;
    OrderBook::Snapshot {
        instrument: instrument.clone(),
        bids,
        asks,
        ts,
    }
}

/// Unary `OrderBook` response -> snapshot.
pub fn map_rest_order_book(
    instrument: &InstrumentId,
    resp: &pb::OrderBookResponse,
    depth: u32,
    now: Instant,
) -> OrderBook {
    let rows = resp
        .orderbook
        .as_ref()
        .map(|b| rest_rows(&b.rows))
        .unwrap_or_default();
    let mut state = BookState::default();
    state.apply(&rows, true);
    let broker_ts = rows.iter().filter_map(|r| r.ts).max();
    book_snapshot(instrument, &state, depth as usize, broker_ts, now)
}

/// `Quote` -> core quote. Finam sends 0 for a missing side.
pub fn map_quote(instrument: &InstrumentId, q: &pb::Quote, now: Instant) -> Quote {
    let side = |price: &Option<Decimal>, size: &Option<Decimal>| {
        let p = price0(price);
        p.is_positive().then(|| level(p, qty0(size)))
    };
    Quote {
        instrument: instrument.clone(),
        bid: side(&q.bid, &q.bid_size),
        ask: side(&q.ask, &q.ask_size),
        ts: timestamps(q.timestamp.as_ref(), now),
    }
}

/// `Trade` -> market trade. A trade without a side cannot be expressed (core `Side` is
/// mandatory), so it is dropped by returning `None`.
pub fn map_trade(instrument: &InstrumentId, t: &pb::Trade, now: Instant) -> Option<MarketTrade> {
    let side = match PbSide::try_from(t.side).unwrap_or(PbSide::Unspecified) {
        PbSide::Buy => Side::Buy,
        PbSide::Sell => Side::Sell,
        PbSide::Unspecified => return None,
    };
    Some(MarketTrade {
        instrument: instrument.clone(),
        price: price0(&t.price),
        qty: qty0(&t.size),
        side,
        trade_id: (!t.trade_id.is_empty()).then(|| t.trade_id.clone()),
        ts: timestamps(t.timestamp.as_ref(), now),
    })
}

/// Core timeframe -> Finam. M10 has no Finam equivalent (see `wire_timeframe`).
pub fn timeframe_to_pb(tf: Timeframe) -> Result<TimeFrame, Error> {
    Ok(match tf {
        Timeframe::M1 => TimeFrame::M1,
        Timeframe::M5 => TimeFrame::M5,
        Timeframe::M15 => TimeFrame::M15,
        Timeframe::M30 => TimeFrame::M30,
        Timeframe::H1 => TimeFrame::H1,
        Timeframe::H4 => TimeFrame::H4,
        Timeframe::D1 => TimeFrame::D,
        Timeframe::W1 => TimeFrame::W,
        Timeframe::MN => TimeFrame::Mn,
        Timeframe::M10 => return Err(unsupported("timeframe_m10")),
    })
}

/// Timeframe asked from Finam for a core timeframe: the same one, except M10, which is built from M5
/// candles (`map::m10`).
pub fn wire_timeframe(tf: Timeframe) -> Result<TimeFrame, Error> {
    match tf {
        Timeframe::M10 => Ok(TimeFrame::M5),
        other => timeframe_to_pb(other),
    }
}

/// Finam gives the bar open time only; close is open + frame length (UTC for week/month).
pub fn bar_close(tf: Timeframe, open: UnixNanos) -> UnixNanos {
    const SEC: i64 = 1_000_000_000;
    let fixed = |secs: i64| UnixNanos::from_nanos(open.as_nanos() + secs * SEC);
    match tf {
        Timeframe::M1 => fixed(60),
        Timeframe::M5 => fixed(300),
        Timeframe::M10 => fixed(600),
        Timeframe::M15 => fixed(900),
        Timeframe::M30 => fixed(1800),
        Timeframe::H1 => fixed(3600),
        Timeframe::H4 => fixed(4 * 3600),
        Timeframe::D1 => fixed(86_400),
        Timeframe::W1 => fixed(7 * 86_400),
        Timeframe::MN => {
            let secs = open.as_nanos().div_euclid(SEC);
            let dt: DateTime<Utc> = Utc.timestamp_opt(secs, 0).single().unwrap_or_default();
            let next = dt
                .checked_add_months(Months::new(1))
                .map(|d| d.with_day(dt.day()).unwrap_or(d))
                .unwrap_or(dt);
            UnixNanos::from_nanos(next.timestamp() * SEC)
        }
    }
}

/// `Bar` -> core candle. Absent prices and volume become 0. `close_ts` is computed from the open time
/// and the timeframe (month and longer frames follow the calendar), because the bar carries only
/// the open time.
pub fn map_bar(instrument: &InstrumentId, tf: Timeframe, b: &pb::Bar, now: Instant) -> Candle {
    let open_ts = b
        .timestamp
        .as_ref()
        .map(ts_nanos)
        .unwrap_or(UnixNanos::ZERO);
    Candle {
        instrument: instrument.clone(),
        timeframe: tf,
        open: price0(&b.open),
        high: price0(&b.high),
        low: price0(&b.low),
        close: price0(&b.close),
        volume: qty0(&b.volume),
        open_ts,
        close_ts: bar_close(tf, open_ts),
        ts: timestamps(b.timestamp.as_ref(), now),
    }
}
