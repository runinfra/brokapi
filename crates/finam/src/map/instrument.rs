//! Instrument directory mapping.

use brokapi_core::{
    Instrument, InstrumentKind, Money, Price, Qty, Schedule, SessionInterval, SessionKind,
    TradingAvailability, TradingParams, UnixNanos,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::assets as pb;
use std::time::Instant;

use crate::util::{instrument_from_symbol, qty, timestamps, ts_nanos};

/// Finam's `type` strings are not enumerated in the docs; matched by substring.
pub fn kind_from_type(t: &str) -> InstrumentKind {
    let t = t.to_ascii_uppercase();
    let has = |s: &str| t.contains(s);
    if has("OPTION") {
        InstrumentKind::Option
    } else if has("FUTURE") {
        InstrumentKind::Future
    } else if has("BOND") {
        InstrumentKind::Bond
    } else if has("ETF") || has("FUND") {
        InstrumentKind::Etf
    } else if has("EQUIT") || has("STOCK") || has("SHARE") {
        InstrumentKind::Stock
    } else if has("CURRENC") {
        InstrumentKind::Currency
    } else {
        InstrumentKind::Other
    }
}

/// A directory entry (`Assets`, `AllAssets`) -> core instrument.
///
/// The list carries no currency, so `currency` is empty. Use `get` or `trading_params` for it.
pub fn map_asset(a: &pb::Asset) -> Instrument {
    let symbol = if a.symbol.is_empty() {
        format!("{}@{}", a.ticker, a.mic)
    } else {
        a.symbol.clone()
    };
    Instrument {
        id: instrument_from_symbol(&symbol),
        name: a.name.clone(),
        isin: (!a.isin.is_empty()).then(|| a.isin.clone()),
        kind: kind_from_type(&a.r#type),
        // The list does not carry a currency; use `get` / `trading_params`.
        currency: String::new(),
        exchange: a.mic.clone(),
    }
}

/// A `GetAsset` answer -> core instrument. `symbol` is the `SYMBOL@MIC` that was asked for.
pub fn map_get_asset(symbol: &str, a: &pb::GetAssetResponse) -> Instrument {
    let id = instrument_from_symbol(symbol);
    Instrument {
        exchange: if a.mic.is_empty() {
            id.key.mic.clone()
        } else {
            a.mic.clone()
        },
        id,
        name: a.name.clone(),
        isin: (!a.isin.is_empty()).then(|| a.isin.clone()),
        kind: kind_from_type(&a.r#type),
        currency: a.quote_currency.clone(),
    }
}

/// Price step = `min_step / 10^decimals`. Finam does not give the money value of a step, so
/// `price_step_value` equals `price_step` (right for stocks, not for futures). `min_qty` is one
/// lot because orders are in pieces and sized in whole lots.
pub fn map_trading_params(symbol: &str, a: &pb::GetAssetResponse) -> TradingParams {
    // min_step counts the smallest units of the price, `decimals` places after the point.
    let places = u32::try_from(a.decimals.clamp(0, 28)).unwrap_or(0);
    let step = Price::from_scaled(a.min_step.max(1), places);
    let lot = qty(&a.lot_size)
        .filter(|l| l.is_positive())
        .unwrap_or(Qty::from(1));
    TradingParams {
        instrument: instrument_from_symbol(symbol),
        lot_size: lot,
        min_qty: lot,
        price_step: step,
        price_step_value: Money::new(step.decimal()),
        currency: a.quote_currency.clone(),
    }
}

/// Session type heuristics: the docs do not list the `type` values.
pub fn session_kind(t: &str) -> SessionKind {
    let t = t.to_ascii_uppercase();
    if t.contains("AUCTION") {
        SessionKind::Auction
    } else if t.contains("EARLY") || t.contains("PRE") || t.contains("MORNING") {
        SessionKind::PreMarket
    } else if t.contains("LATE")
        || t.contains("POST")
        || t.contains("EVENING")
        || t.contains("AFTER")
    {
        SessionKind::PostMarket
    } else if t.contains("CORE")
        || t.contains("MAIN")
        || t.contains("NORMAL")
        || t.contains("REGULAR")
    {
        SessionKind::Main
    } else {
        SessionKind::Other
    }
}

fn is_closed_type(t: &str) -> bool {
    t.to_ascii_uppercase().contains("CLOSED")
}

/// A `Schedule` answer -> core schedule for the range `from..to`.
///
/// Sessions of the closed type are dropped, and so are sessions that do not overlap the range.
/// `symbol` is stored as the schedule's `exchange` field, because Finam schedules are per instrument.
pub fn map_schedule(
    symbol: &str,
    resp: &pb::ScheduleResponse,
    from: UnixNanos,
    to: UnixNanos,
    now: Instant,
) -> Schedule {
    let sessions = resp
        .sessions
        .iter()
        .filter(|s| !is_closed_type(&s.r#type))
        .filter_map(|s| {
            let iv = s.interval.as_ref()?;
            let start = iv.start_time.as_ref().map(ts_nanos)?;
            let end = iv.end_time.as_ref().map(ts_nanos)?;
            // Keep sessions that overlap the requested range.
            (end > from && start < to).then(|| SessionInterval {
                start,
                end,
                kind: session_kind(&s.r#type),
            })
        })
        .collect();
    Schedule {
        exchange: symbol.to_string(),
        sessions,
        ts: timestamps(None, now),
    }
}

/// Availability from the schedule: inside a non-closed session -> open, otherwise closed.
pub fn availability_at(
    resp: &pb::ScheduleResponse,
    at: UnixNanos,
) -> (TradingAvailability, Option<String>) {
    for s in &resp.sessions {
        let Some(iv) = &s.interval else { continue };
        let (Some(a), Some(b)) = (iv.start_time.as_ref(), iv.end_time.as_ref()) else {
            continue;
        };
        if ts_nanos(a) <= at && at < ts_nanos(b) {
            return if is_closed_type(&s.r#type) {
                (TradingAvailability::Closed, Some(s.r#type.clone()))
            } else {
                (TradingAvailability::Open, Some(s.r#type.clone()))
            };
        }
    }
    if resp.sessions.is_empty() {
        (TradingAvailability::Unknown, Some("empty schedule".into()))
    } else {
        (
            TradingAvailability::Closed,
            Some("outside of scheduled sessions".into()),
        )
    }
}
