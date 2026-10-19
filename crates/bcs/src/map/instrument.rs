//! Instrument reference data mapping.

use brokapi_bcs_protocol::http::models::{
    InstrumentInfo, TradingParamsSnapshot, TradingStatusResponse,
};
use brokapi_core::{
    Instrument, InstrumentId, InstrumentKey, InstrumentKind, Money, Price, Qty,
    TradingAvailability, TradingParams, TradingStatus,
};

use super::instrument_id_ticker;

/// Maps trading parameters of one board.
///
/// `min_qty` is the lot size, because BCS gives no separate minimum. Prices and steps are passed
/// through unchanged.
pub fn map_trading_params(snapshot: &TradingParamsSnapshot) -> TradingParams {
    let instrument = instrument_id_ticker(&snapshot.ticker, &snapshot.class_code);
    TradingParams {
        instrument,
        lot_size: Qty::new(snapshot.lot_size),
        min_qty: Qty::new(snapshot.lot_size),
        price_step: Price::new(snapshot.price_step),
        price_step_value: Money::new(snapshot.price_step_value),
        currency: snapshot.currency.clone(),
    }
}

/// Maps the directory record of an instrument, for the board `class_code`.
///
/// - Name: `displayName`, else `shortName`, else the ticker.
/// - Kind: from the BCS type text (contains `STOCK`, `BOND`, `FUTURE`, `OPTION`, `ETF`, `FUND`,
///   `CURRENCY`), anything else is `Other`.
/// - Currency: the trading currency, `RUB` when the record has none (an assumption).
/// - Exchange: the exchange of the board with this class code, `MOEX` when the board is not listed
///   (an assumption).
pub fn map_instrument_info(info: &InstrumentInfo, class_code: &str) -> Instrument {
    let instrument = instrument_id_ticker(&info.ticker, class_code);
    Instrument {
        id: instrument,
        name: info
            .display_name
            .clone()
            .or_else(|| info.short_name.clone())
            .unwrap_or_else(|| info.ticker.clone()),
        isin: info.isin.clone(),
        kind: map_kind(info.instrument_type.as_deref()),
        currency: info
            .trading_currency
            .clone()
            .unwrap_or_else(|| "RUB".into()),
        exchange: info
            .boards
            .iter()
            .find(|b| b.class_code == class_code)
            .map(|b| b.exchange.clone())
            .unwrap_or_else(|| "MOEX".into()),
    }
}

/// Maps the trading status of a board.
///
/// `OPEN` is [`TradingAvailability::Open`], `CLOSE` is `Closed` (any letter case), any other text is
/// `Unknown`. The `reason` is the session type text from the broker. The status carries no timestamp.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::TradingStatusResponse;
/// use brokapi_bcs::map::instrument::map_trading_status;
/// use brokapi_core::TradingAvailability;
///
/// let resp: TradingStatusResponse = serde_json::from_str(
///     r#"{"tradingSessionTypeId":1,"tradingSessionType":"Main","tradingSessionStatus":"OPEN",
///         "nextSessionDate":"2026-10-20T07:00:00Z"}"#,
/// )
/// .unwrap();
/// let status = map_trading_status("SBER", "TQBR", &resp);
/// assert_eq!(status.availability, TradingAvailability::Open);
/// ```
pub fn map_trading_status(
    ticker: &str,
    class_code: &str,
    resp: &TradingStatusResponse,
) -> TradingStatus {
    let instrument = instrument_id_ticker(ticker, class_code);
    let availability = if resp.trading_session_status.eq_ignore_ascii_case("OPEN") {
        TradingAvailability::Open
    } else if resp.trading_session_status.eq_ignore_ascii_case("CLOSE") {
        TradingAvailability::Closed
    } else {
        TradingAvailability::Unknown
    };
    TradingStatus {
        instrument,
        availability,
        reason: Some(resp.trading_session_type.clone()),
        ts: None,
    }
}

fn map_kind(raw: Option<&str>) -> InstrumentKind {
    match raw {
        Some(s) if s.contains("STOCK") => InstrumentKind::Stock,
        Some(s) if s.contains("BOND") => InstrumentKind::Bond,
        Some(s) if s.contains("FUTURE") => InstrumentKind::Future,
        Some(s) if s.contains("OPTION") => InstrumentKind::Option,
        Some(s) if s.contains("ETF") => InstrumentKind::Etf,
        Some(s) if s.contains("FUND") => InstrumentKind::MutualFund,
        Some(s) if s.contains("CURRENCY") => InstrumentKind::Currency,
        _ => InstrumentKind::Other,
    }
}

/// Returns the broker-neutral key of an [`InstrumentId`] (a clone of `id.key`).
pub fn instrument_from_native(id: &InstrumentId) -> InstrumentKey {
    id.key.clone()
}
