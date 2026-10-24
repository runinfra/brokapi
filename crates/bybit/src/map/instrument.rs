//! Instruments: the directory entry, the trading rules and the status.

use brokapi_bybit_protocol::http::models::InstrumentInfo;
use brokapi_core::{
    Instrument, InstrumentId, InstrumentKind, Money, Price, Qty, TradingAvailability,
    TradingParams, TradingStatus, UnixNanos,
};

use crate::util::instrument_id;

/// The kind of an instrument by the contract type of the exchange: a perpetual is a [`InstrumentKind::Perpetual`],
/// a dated contract a [`InstrumentKind::Future`].
pub fn kind_of(contract_type: &str) -> InstrumentKind {
    match contract_type {
        "LinearPerpetual" | "InversePerpetual" => InstrumentKind::Perpetual,
        "LinearFutures" | "InverseFutures" => InstrumentKind::Future,
        _ => InstrumentKind::Other,
    }
}

fn id_of(info: &InstrumentInfo, category: &str) -> InstrumentId {
    instrument_id(&info.symbol, category)
}

/// The directory entry.
pub fn instrument(info: &InstrumentInfo, category: &str) -> Instrument {
    Instrument {
        id: id_of(info, category),
        name: if info.display_name.is_empty() {
            info.symbol.clone()
        } else {
            info.display_name.clone()
        },
        isin: None,
        kind: kind_of(&info.contract_type),
        currency: if info.settle_coin.is_empty() {
            info.quote_coin.clone()
        } else {
            info.settle_coin.clone()
        },
        exchange: "BYBIT".to_string(),
    }
}

/// The trading rules.
///
/// The quantity is in units of the base coin (`0.001` BTC). `lot_size` is the quantity step and `min_qty` the smallest
/// order, so a quantity is valid when it is a whole number of steps and not below the minimum. The value of one price
/// step is the step itself: the contract is linear (one unit of the base coin pays one unit of the settle coin per unit
/// of price), so a step of `0.10` moves one unit by `0.10` USDT.
pub fn trading_params(info: &InstrumentInfo, category: &str) -> TradingParams {
    TradingParams {
        instrument: id_of(info, category),
        lot_size: Qty::new(info.lot_size_filter.qty_step),
        min_qty: Qty::new(info.lot_size_filter.min_order_qty),
        price_step: Price::new(info.price_filter.tick_size),
        price_step_value: Money::new(info.price_filter.tick_size),
        currency: if info.settle_coin.is_empty() {
            info.quote_coin.clone()
        } else {
            info.settle_coin.clone()
        },
    }
}

/// The status: `Trading` is open, the contracts that wait to open or are delisted are closed.
pub fn trading_status(
    info: &InstrumentInfo,
    category: &str,
    now_ns: Option<UnixNanos>,
) -> TradingStatus {
    let (availability, reason) = match info.status.as_str() {
        "Trading" => (TradingAvailability::Open, None),
        "PreLaunch" | "PendingOpen" | "Delivering" | "Closed" => {
            (TradingAvailability::Closed, Some(info.status.clone()))
        }
        "" => (TradingAvailability::Unknown, None),
        other => (TradingAvailability::Unknown, Some(other.to_string())),
    };
    TradingStatus {
        instrument: id_of(info, category),
        availability,
        reason,
        ts: now_ns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn btc() -> InstrumentInfo {
        serde_json::from_str(include_str!("../../tests/fixtures/instrument_btcusdt.json"))
            .map(|v: serde_json::Value| {
                serde_json::from_value(v["result"]["list"][0].clone()).unwrap()
            })
            .unwrap()
    }

    #[test]
    fn a_perpetual_is_a_perpetual_and_the_rules_come_from_the_filters() {
        let info = btc();
        let i = instrument(&info, "linear");
        assert_eq!(i.kind, InstrumentKind::Perpetual);
        assert_eq!(i.id.key.display(), "BTCUSDT@linear");
        assert_eq!(
            (i.currency.as_str(), i.exchange.as_str()),
            ("USDT", "BYBIT")
        );
        let p = trading_params(&info, "linear");
        assert_eq!(p.lot_size.to_string(), "0.001");
        assert_eq!(p.min_qty.to_string(), "0.001");
        assert_eq!(p.price_step.to_string(), "0.10");
        assert_eq!(
            trading_status(&info, "linear", None).availability,
            TradingAvailability::Open
        );
    }

    #[test]
    fn contract_types_and_statuses() {
        assert_eq!(kind_of("LinearFutures"), InstrumentKind::Future);
        assert_eq!(kind_of("Strange"), InstrumentKind::Other);
        let mut info = btc();
        info.status = "PreLaunch".into();
        let s = trading_status(&info, "linear", None);
        assert_eq!(
            (s.availability, s.reason.as_deref()),
            (TradingAvailability::Closed, Some("PreLaunch"))
        );
        info.status = "Odd".into();
        assert_eq!(
            trading_status(&info, "linear", None).availability,
            TradingAvailability::Unknown
        );
    }
}
