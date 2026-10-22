//! Reference data mapping: instruments, trading parameters, trading status.

use brokapi_alor_protocol::http::models::SecurityInfo;
use brokapi_core::{
    Decimal, Instrument, InstrumentId, InstrumentKind, Money, Price, Qty, TradingAvailability,
    TradingParams, TradingStatus,
};

use crate::util::instrument_id;

/// The board an entry is about: the board of the entry, else the main one of the instrument.
pub fn board_of(info: &SecurityInfo) -> String {
    info.board
        .clone()
        .filter(|b| !b.is_empty())
        .or_else(|| info.primary_board.clone().filter(|b| !b.is_empty()))
        .unwrap_or_default()
}

/// The kind of an instrument, from its type, its market and the CFI code.
pub fn kind_of(info: &SecurityInfo) -> InstrumentKind {
    let ty = info.kind.as_deref().unwrap_or("");
    match ty {
        "CS" | "PS" | "RDR" => return InstrumentKind::Stock,
        "EUSOV" | "MUNI" | "CORP" => return InstrumentKind::Bond,
        "FOR" => return InstrumentKind::Currency,
        "MF" => return InstrumentKind::MutualFund,
        _ => {}
    }
    if ty.starts_with("Фьючерс") {
        return InstrumentKind::Future;
    }
    if ty.starts_with("Марж") || ty.contains("Call") || ty.contains("Put") {
        return InstrumentKind::Option;
    }
    match info.market.as_deref() {
        Some("CURR") => return InstrumentKind::Currency,
        Some("FORTS") => return InstrumentKind::Future,
        _ => {}
    }
    match info.cfi_code.as_deref().and_then(|c| c.chars().next()) {
        Some('E') => InstrumentKind::Stock,
        Some('D') => InstrumentKind::Bond,
        Some('F') => InstrumentKind::Future,
        Some('O') => InstrumentKind::Option,
        Some('C') => InstrumentKind::Etf,
        _ => InstrumentKind::Other,
    }
}

/// The id of the instrument of an entry.
pub fn id_of(info: &SecurityInfo) -> InstrumentId {
    instrument_id(&info.symbol, &board_of(info))
}

/// An instrument of the reference data.
pub fn instrument(info: &SecurityInfo) -> Instrument {
    Instrument {
        id: id_of(info),
        name: info
            .shortname
            .clone()
            .or_else(|| info.description.clone())
            .unwrap_or_else(|| info.symbol.clone()),
        isin: info.isin.clone().filter(|i| !i.is_empty()),
        kind: kind_of(info),
        currency: info.currency.clone().unwrap_or_default(),
        exchange: info.exchange.clone(),
    }
}

/// The trading parameters of an instrument. The lot is in pieces; the minimal quantity is one lot. The value of a
/// price step is the `pricestep` of the broker (the step in roubles), or the step itself when it gives none.
pub fn trading_params(info: &SecurityInfo) -> TradingParams {
    let lot = info
        .lot_size
        .filter(|l| !l.is_zero())
        .unwrap_or(Decimal::ONE);
    let step = info.min_step.unwrap_or(Decimal::new(1, 2));
    TradingParams {
        instrument: id_of(info),
        lot_size: Qty::new(lot),
        min_qty: Qty::new(lot),
        price_step: Price::new(step),
        price_step_value: Money::new(info.price_step.unwrap_or(step)),
        currency: info.currency.clone().unwrap_or_default(),
    }
}

/// The availability for a trading status code of the broker.
pub fn availability_of(code: i32) -> TradingAvailability {
    match code {
        // 17 normal period, 118 opening period, 103 closing period, auctions (102, 106, 107, 119) and trading at the
        // price of the closing auction (120): orders are accepted.
        17 | 118 | 103 | 102 | 106 | 107 | 119 | 120 => TradingAvailability::Open,
        2 => TradingAvailability::Break,
        18 => TradingAvailability::Closed,
        _ => TradingAvailability::Unknown,
    }
}

/// The trading status of an instrument, from its `tradingStatus` code.
pub fn trading_status(
    info: &SecurityInfo,
    now_ns: Option<brokapi_core::UnixNanos>,
) -> TradingStatus {
    TradingStatus {
        instrument: id_of(info),
        availability: info
            .trading_status
            .map(availability_of)
            .unwrap_or(TradingAvailability::Unknown),
        reason: info.trading_status_info.clone(),
        ts: now_ns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(json: &str) -> SecurityInfo {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn params_come_from_the_lot_and_the_step() {
        let i = info(
            r#"{"symbol":"SBER","exchange":"MOEX","board":"TQBR","type":"CS","lotsize":10,"minstep":0.01,"pricestep":0.01,"currency":"RUB"}"#,
        );
        let p = trading_params(&i);
        assert_eq!(p.lot_size, Qty::from(10));
        assert_eq!(p.min_qty, Qty::from(10));
        assert_eq!(p.price_step.to_string(), "0.01");
        assert_eq!(kind_of(&i), InstrumentKind::Stock);
        assert_eq!(instrument(&i).id.key.display(), "SBER@TQBR");
    }

    #[test]
    fn kinds_follow_the_type_the_market_and_the_cfi() {
        let k = |j: &str| kind_of(&info(j));
        assert_eq!(k(r#"{"type":"CORP"}"#), InstrumentKind::Bond);
        assert_eq!(
            k(r#"{"type":"Фьючерсный контракт SI"}"#),
            InstrumentKind::Future
        );
        assert_eq!(
            k(r#"{"type":"Марж. амер. Call SBER"}"#),
            InstrumentKind::Option
        );
        assert_eq!(k(r#"{"market":"CURR"}"#), InstrumentKind::Currency);
        assert_eq!(k(r#"{"cfiCode":"ESXXXX"}"#), InstrumentKind::Stock);
        assert_eq!(k(r#"{}"#), InstrumentKind::Other);
    }

    #[test]
    fn status_codes_become_availability() {
        assert_eq!(availability_of(17), TradingAvailability::Open);
        assert_eq!(availability_of(18), TradingAvailability::Closed);
        assert_eq!(availability_of(2), TradingAvailability::Break);
        assert_eq!(availability_of(999), TradingAvailability::Unknown);
    }
}
