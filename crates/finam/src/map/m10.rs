//! 10-minute candles built from 5-minute ones: Finam has no `M10` timeframe.
//!
//! The grid is the epoch: a 10-minute candle opens at a multiple of 600 s. Exchange time is UTC+3
//! (a whole number of 10 minute steps), so these are the usual :00, :10, :20 ... boundaries.
//! A candle is built from the M5 candles that fall in it: the first opens it, the last closes it,
//! high and low are taken over the pair, volume is the sum. A half with no trades has no M5 candle at
//! all, so a lone M5 candle is a legitimate 10-minute candle (and the last pair of a range is often
//! incomplete): it is emitted as it is, not dropped.

use brokapi_core::{Candle, Timeframe, UnixNanos};

const SEC: i64 = 1_000_000_000;
const M10_NANOS: i64 = 600 * SEC;

/// Start of the 10-minute interval that contains `ts`.
pub fn floor_m10(ts: UnixNanos) -> UnixNanos {
    UnixNanos::from_nanos(ts.as_nanos().div_euclid(M10_NANOS) * M10_NANOS)
}

/// Merges the M5 candles of one 10-minute interval into an M10 candle. `parts` must be non-empty,
/// belong to one instrument and one interval; the order does not matter.
fn merge(parts: &[&Candle]) -> Candle {
    let mut sorted: Vec<&Candle> = parts.to_vec();
    sorted.sort_by_key(|c| c.open_ts);
    let first = sorted[0];
    let last = sorted[sorted.len() - 1];
    let open_ts = floor_m10(first.open_ts);
    Candle {
        instrument: first.instrument.clone(),
        timeframe: Timeframe::M10,
        open: first.open,
        high: sorted.iter().map(|c| c.high).max().unwrap_or(first.high),
        low: sorted.iter().map(|c| c.low).min().unwrap_or(first.low),
        close: last.close,
        volume: sorted.iter().map(|c| c.volume).sum(),
        open_ts,
        close_ts: UnixNanos::from_nanos(open_ts.as_nanos() + M10_NANOS),
        ts: last.ts,
    }
}

/// History: M5 candles (in any order) -> M10 candles, oldest first.
pub fn merge_m5(m5: &[Candle]) -> Vec<Candle> {
    let mut groups: std::collections::BTreeMap<UnixNanos, Vec<&Candle>> = Default::default();
    for c in m5 {
        groups.entry(floor_m10(c.open_ts)).or_default().push(c);
    }
    groups.values().map(|g| merge(g)).collect()
}

/// Stream: follows M5 candle updates and keeps the M10 candle that is forming.
///
/// Every M5 update (a forming bar sent again, or a closed one) gives the current M10 candle. A
/// bar of an older interval than the current one is a replay and is ignored.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use brokapi_core::{Candle, Price, Qty, Timeframe, Timestamps, UnixNanos};
/// use brokapi_finam::map::m10::M10Builder;
/// use brokapi_finam::util::instrument_from_symbol;
///
/// let m5 = |open_secs: i64, close: i64, volume: i64| Candle {
///     instrument: instrument_from_symbol("SBER@MISX"),
///     timeframe: Timeframe::M5,
///     open: Price::from(100),
///     high: Price::from(close.max(100)),
///     low: Price::from(close.min(100)),
///     close: Price::from(close),
///     volume: Qty::from(volume),
///     open_ts: UnixNanos::from_nanos(open_secs * 1_000_000_000),
///     close_ts: UnixNanos::from_nanos((open_secs + 300) * 1_000_000_000),
///     ts: Timestamps::new(UnixNanos::from_nanos(0), Instant::now()),
/// };
///
/// let mut builder = M10Builder::new();
/// // The first half of the interval 0..600 s.
/// let part = builder.on_m5(&m5(0, 101, 10)).unwrap();
/// assert_eq!(part.timeframe, Timeframe::M10);
/// // The second half arrives: the same M10 candle, now with both halves.
/// let whole = builder.on_m5(&m5(300, 103, 5)).unwrap();
/// assert_eq!(whole.volume, Qty::from(15));
/// assert_eq!(whole.close, Price::from(103));
/// // A bar of an older interval is a replay and is ignored.
/// let _next = builder.on_m5(&m5(600, 104, 1)).unwrap();
/// assert!(builder.on_m5(&m5(0, 101, 10)).is_none());
/// ```
#[derive(Debug, Default)]
pub struct M10Builder {
    current: Option<UnixNanos>,
    /// The latest version of each M5 candle of the current interval, by open time.
    parts: Vec<Candle>,
}

impl M10Builder {
    /// An empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one M5 update. Returns the current M10 candle, or `None` when the bar belongs to an interval
    /// older than the current one (a replay after a reconnect).
    ///
    /// Call it for every M5 message, including the repeats of a forming bar: each one gives the
    /// up-to-date M10 candle. Prices and volume of the candle are not final until the interval ends.
    pub fn on_m5(&mut self, bar: &Candle) -> Option<Candle> {
        let group = floor_m10(bar.open_ts);
        match self.current {
            Some(cur) if group < cur => return None,
            Some(cur) if group == cur => {}
            _ => {
                self.current = Some(group);
                self.parts.clear();
            }
        }
        match self.parts.iter_mut().find(|p| p.open_ts == bar.open_ts) {
            Some(p) => *p = bar.clone(),
            None => self.parts.push(bar.clone()),
        }
        let refs: Vec<&Candle> = self.parts.iter().collect();
        Some(merge(&refs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{Decimal, dec};
    use brokapi_core::{InstrumentId, InstrumentKey, NativeId, Timestamps};
    use std::time::Instant;

    fn id() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "MISX"),
            NativeId::SymbolMic {
                symbol: "SBER".into(),
                mic: "MISX".into(),
            },
        )
    }

    /// An M5 candle opening `minute` minutes after a 10-minute boundary (the boundary is 1_800_000 s).
    fn m5(minute: i64, o: &str, h: &str, l: &str, c: &str, v: &str) -> Candle {
        let open_ts = UnixNanos::from_nanos(1_800_000 * SEC + minute * 60 * SEC);
        Candle {
            instrument: id(),
            timeframe: Timeframe::M5,
            open: o.parse().unwrap(),
            high: h.parse().unwrap(),
            low: l.parse().unwrap(),
            close: c.parse().unwrap(),
            volume: v.parse().unwrap(),
            open_ts,
            close_ts: UnixNanos::from_nanos(open_ts.as_nanos() + 300 * SEC),
            ts: Timestamps::new(UnixNanos::ZERO, Instant::now()),
        }
    }

    #[test]
    fn floor_is_a_multiple_of_ten_minutes() {
        let t = UnixNanos::from_nanos(1_800_000 * SEC + 7 * 60 * SEC + 13);
        assert_eq!(floor_m10(t).as_nanos(), 1_800_000 * SEC);
        assert_eq!(floor_m10(floor_m10(t)), floor_m10(t));
    }

    #[test]
    fn a_pair_becomes_one_candle() {
        let out = merge_m5(&[
            m5(0, "10.0", "12.0", "9.0", "11.0", "100.0"),
            m5(5, "11.0", "15.0", "10.5", "14.0", "50.0"),
        ]);
        assert_eq!(out.len(), 1);
        let c = &out[0];
        assert_eq!(c.timeframe, Timeframe::M10);
        assert_eq!(
            (
                c.open.decimal(),
                c.high.decimal(),
                c.low.decimal(),
                c.close.decimal()
            ),
            (dec!(10.0), dec!(15.0), dec!(9.0), dec!(14.0))
        );
        assert_eq!(c.volume.decimal(), dec!(150.0));
        assert_eq!(c.open_ts.as_nanos(), 1_800_000 * SEC);
        assert_eq!(c.close_ts.as_nanos(), 1_800_000 * SEC + 600 * SEC);
    }

    #[test]
    fn pairs_are_cut_on_ten_minute_boundaries_not_by_position() {
        // 00:05 is the second half of the first interval, 00:10 opens the next one.
        let out = merge_m5(&[
            m5(5, "11.0", "15.0", "10.5", "14.0", "50.0"),
            m5(10, "20.0", "21.0", "19.0", "20.5", "7.0"),
            m5(15, "20.5", "23.0", "20.0", "22.0", "3.0"),
        ]);
        assert_eq!(out.len(), 2);
        // The first interval has only its second half (no trades in the first): it is kept as it is.
        assert_eq!(
            (
                out[0].open.decimal(),
                out[0].close.decimal(),
                out[0].volume.decimal()
            ),
            (dec!(11.0), dec!(14.0), dec!(50.0))
        );
        assert_eq!(out[0].open_ts.as_nanos(), 1_800_000 * SEC);
        assert_eq!(
            (
                out[1].open.decimal(),
                out[1].high.decimal(),
                out[1].close.decimal()
            ),
            (dec!(20.0), dec!(23.0), dec!(22.0))
        );
        assert_eq!(out[1].open_ts.as_nanos(), 1_800_000 * SEC + 600 * SEC);
    }

    #[test]
    fn incomplete_last_pair_is_emitted() {
        let out = merge_m5(&[
            m5(0, "1.0", "2.0", "0.5", "1.5", "10.0"),
            m5(5, "1.5", "3.0", "1.0", "2.5", "10.0"),
            m5(10, "2.5", "2.6", "2.4", "2.5", "4.0"),
        ]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].volume.decimal(), dec!(4.0));
        assert_eq!(out[1].close.decimal(), dec!(2.5));
        assert_eq!(out[1].open_ts.as_nanos(), 1_800_000 * SEC + 600 * SEC);
    }

    #[test]
    fn input_order_does_not_matter_and_empty_is_empty() {
        let a = merge_m5(&[
            m5(5, "11.0", "15.0", "10.5", "14.0", "50.0"),
            m5(0, "10.0", "12.0", "9.0", "11.0", "100.0"),
        ]);
        assert_eq!(
            (a[0].open.decimal(), a[0].close.decimal()),
            (dec!(10.0), dec!(14.0))
        );
        assert!(merge_m5(&[]).is_empty());
    }

    #[test]
    fn builder_follows_the_forming_candle() {
        let mut b = M10Builder::new();
        // First half forms, then updates.
        let c = b
            .on_m5(&m5(0, "10.0", "11.0", "9.5", "10.5", "5.0"))
            .unwrap();
        assert_eq!(
            (c.open.decimal(), c.close.decimal(), c.volume.decimal()),
            (dec!(10.0), dec!(10.5), dec!(5.0))
        );
        let c = b
            .on_m5(&m5(0, "10.0", "12.0", "9.5", "11.0", "8.0"))
            .unwrap();
        assert_eq!(
            (c.high.decimal(), c.close.decimal(), c.volume.decimal()),
            (dec!(12.0), dec!(11.0), dec!(8.0)),
            "an update replaces, it does not add"
        );
        // Second half joins.
        let c = b
            .on_m5(&m5(5, "11.0", "13.0", "10.0", "12.5", "4.0"))
            .unwrap();
        assert_eq!(
            (
                c.open.decimal(),
                c.high.decimal(),
                c.low.decimal(),
                c.close.decimal()
            ),
            (dec!(10.0), dec!(13.0), dec!(9.5), dec!(12.5))
        );
        assert_eq!(c.volume.decimal(), dec!(12.0));
        // The next interval starts fresh.
        let c = b
            .on_m5(&m5(10, "12.5", "12.6", "12.4", "12.5", "1.0"))
            .unwrap();
        assert_eq!(
            (c.open.decimal(), c.volume.decimal()),
            (dec!(12.5), dec!(1.0))
        );
        assert_eq!(c.open_ts.as_nanos(), 1_800_000 * SEC + 600 * SEC);
        // A replay of the old interval is ignored.
        assert!(
            b.on_m5(&m5(5, "11.0", "13.0", "10.0", "12.5", "4.0"))
                .is_none()
        );
    }
}
