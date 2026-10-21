//! Instrument directory and trading status.

use brokapi_core::{
    Instrument, InstrumentId, InstrumentKind, Money, Price, Qty, TradingAvailability,
    TradingParams, TradingStatus,
};
use brokapi_tbank_protocol::v1 as pb;

use crate::ids::{id_from_parts, price_of_quotation, wall_nanos};

/// Instrument kind of a broker message to the domain [`InstrumentKind`].
///
/// Bond, share, currency, ETF, futures and option map one to one. Everything else (indicatives,
/// structured notes, ...) is [`InstrumentKind::Other`].
///
/// # Examples
///
/// ```
/// use brokapi_core::InstrumentKind;
/// use brokapi_tbank::map::instrument::kind;
/// use brokapi_tbank_protocol::v1::InstrumentType;
///
/// assert_eq!(kind(InstrumentType::Share), InstrumentKind::Stock);
/// assert_eq!(kind(InstrumentType::Futures), InstrumentKind::Future);
/// ```
pub fn kind(k: pb::InstrumentType) -> InstrumentKind {
    match k {
        pb::InstrumentType::Bond => InstrumentKind::Bond,
        pb::InstrumentType::Share => InstrumentKind::Stock,
        pb::InstrumentType::Currency => InstrumentKind::Currency,
        pb::InstrumentType::Etf => InstrumentKind::Etf,
        pb::InstrumentType::Futures => InstrumentKind::Future,
        pb::InstrumentType::Option => InstrumentKind::Option,
        _ => InstrumentKind::Other,
    }
}

/// Full record of `GetInstrumentBy` to [`Instrument`].
///
/// `exchange` holds the class code (the board), for example `TQBR`. `isin` is `None` when the broker
/// sends none.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::map::instrument::instrument_from_full;
/// use brokapi_tbank_protocol::v1::Instrument;
///
/// let record = Instrument { uid: "uid-sber".into(), ticker: "SBER".into(), class_code: "TQBR".into(), currency: "rub".into(), ..Default::default() };
/// let instrument = instrument_from_full(&record);
/// assert_eq!(instrument.id.key.symbol, "SBER");
/// assert_eq!(instrument.currency, "rub");
/// assert_eq!(instrument.isin, None);
/// ```
pub fn instrument_from_full(i: &pb::Instrument) -> Instrument {
    Instrument {
        id: id_from_parts(&i.uid, &i.figi, &i.ticker, &i.class_code),
        name: i.name.clone(),
        isin: (!i.isin.is_empty()).then(|| i.isin.clone()),
        kind: kind(i.instrument_kind()),
        currency: i.currency.clone(),
        exchange: i.class_code.clone(),
    }
}

/// Record of a whole-kind listing (`Shares`, `Bonds`, ...): the kind comes from the method, not the record.
/// Futures and options have no ISIN.
#[allow(clippy::too_many_arguments)]
fn listed(
    kind: InstrumentKind,
    uid: &str,
    figi: &str,
    ticker: &str,
    class_code: &str,
    isin: &str,
    name: &str,
    currency: &str,
) -> Instrument {
    Instrument {
        id: id_from_parts(uid, figi, ticker, class_code),
        name: name.to_string(),
        isin: (!isin.is_empty()).then(|| isin.to_string()),
        kind,
        currency: currency.to_string(),
        exchange: class_code.to_string(),
    }
}

/// A share record of the `Shares` listing to [`Instrument`] of kind `Stock`.
pub fn from_share(i: &pb::Share) -> Instrument {
    listed(
        InstrumentKind::Stock,
        &i.uid,
        &i.figi,
        &i.ticker,
        &i.class_code,
        &i.isin,
        &i.name,
        &i.currency,
    )
}

/// A bond record of the `Bonds` listing to [`Instrument`] of kind `Bond`.
pub fn from_bond(i: &pb::Bond) -> Instrument {
    listed(
        InstrumentKind::Bond,
        &i.uid,
        &i.figi,
        &i.ticker,
        &i.class_code,
        &i.isin,
        &i.name,
        &i.currency,
    )
}

/// An ETF record of the `Etfs` listing to [`Instrument`] of kind `Etf`.
pub fn from_etf(i: &pb::Etf) -> Instrument {
    listed(
        InstrumentKind::Etf,
        &i.uid,
        &i.figi,
        &i.ticker,
        &i.class_code,
        &i.isin,
        &i.name,
        &i.currency,
    )
}

/// A futures record of the `Futures` listing to [`Instrument`] of kind `Future`. It has no ISIN.
pub fn from_future(i: &pb::Future) -> Instrument {
    listed(
        InstrumentKind::Future,
        &i.uid,
        &i.figi,
        &i.ticker,
        &i.class_code,
        "",
        &i.name,
        &i.currency,
    )
}

/// An option record of the `Options` listing to [`Instrument`] of kind `Option`. It has no ISIN and no figi.
pub fn from_option(i: &pb::Option) -> Instrument {
    listed(
        InstrumentKind::Option,
        &i.uid,
        "",
        &i.ticker,
        &i.class_code,
        "",
        &i.name,
        &i.currency,
    )
}

/// A currency record of the `Currencies` listing to [`Instrument`] of kind `Currency`.
pub fn from_currency(i: &pb::Currency) -> Instrument {
    listed(
        InstrumentKind::Currency,
        &i.uid,
        &i.figi,
        &i.ticker,
        &i.class_code,
        &i.isin,
        &i.name,
        &i.currency,
    )
}

/// `FindInstrument` returns short records without currency; `currency` stays empty (use `get` for it).
pub fn instrument_from_short(i: &pb::InstrumentShort) -> Instrument {
    Instrument {
        id: id_from_parts(&i.uid, &i.figi, &i.ticker, &i.class_code),
        name: i.name.clone(),
        isin: (!i.isin.is_empty()).then(|| i.isin.clone()),
        kind: kind(i.instrument_kind()),
        currency: String::new(),
        exchange: i.class_code.clone(),
    }
}

/// `lot_size` is units per lot. `min_qty` is one lot: an order is a whole number of lots (and the library
/// counts the quantity of an order in units).
/// `price_step_value` equals the step (price is per one instrument in the settlement currency);
/// futures need `GetFuturesMargin` for the step in money, not requested here.
pub fn trading_params(id: &InstrumentId, i: &pb::Instrument) -> TradingParams {
    let step = i
        .min_price_increment
        .as_ref()
        .map(price_of_quotation)
        .unwrap_or(Price::ZERO);
    TradingParams {
        instrument: id.clone(),
        lot_size: Qty::from(i.lot),
        min_qty: Qty::from(i.lot),
        price_step: step,
        price_step_value: Money::new(step.decimal()),
        currency: i.currency.clone(),
    }
}

/// Broker trading status to [`TradingAvailability`].
///
/// Normal trading and open session are `Open`. A break is `Break`. Not available for trading and a
/// closed session are `Closed`. Opening and closing periods and auctions are `Unknown`, because
/// orders are possible there only with restrictions that this mapping does not model.
///
/// # Examples
///
/// ```
/// use brokapi_core::TradingAvailability;
/// use brokapi_tbank::map::instrument::availability;
/// use brokapi_tbank_protocol::v1::SecurityTradingStatus;
///
/// assert_eq!(availability(SecurityTradingStatus::NormalTrading), TradingAvailability::Open);
/// assert_eq!(availability(SecurityTradingStatus::BreakInTrading), TradingAvailability::Break);
/// ```
pub fn availability(s: pb::SecurityTradingStatus) -> TradingAvailability {
    use pb::SecurityTradingStatus as S;
    match s {
        S::NormalTrading | S::SessionOpen | S::DealerNormalTrading => TradingAvailability::Open,
        S::BreakInTrading | S::DealerBreakInTrading => TradingAvailability::Break,
        S::NotAvailableForTrading
        | S::SessionClose
        | S::DealerNotAvailableForTrading
        | S::SessionAssigned => TradingAvailability::Closed,
        // Opening/closing periods and auctions: orders are possible with restrictions.
        _ => TradingAvailability::Unknown,
    }
}

/// Broker trading status to [`TradingStatus`] of `id`.
///
/// `reason` holds the broker's status name. `ts` is the local clock, since the status has no time of
/// its own.
///
/// # Examples
///
/// ```
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId, TradingAvailability};
/// use brokapi_tbank::map::instrument::trading_status;
/// use brokapi_tbank_protocol::v1::SecurityTradingStatus;
///
/// let id = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
/// let status = trading_status(&id, SecurityTradingStatus::SessionClose);
/// assert_eq!(status.availability, TradingAvailability::Closed);
/// ```
pub fn trading_status(id: &InstrumentId, s: pb::SecurityTradingStatus) -> TradingStatus {
    TradingStatus {
        instrument: id.clone(),
        availability: availability(s),
        reason: Some(s.as_str_name().to_string()),
        ts: Some(wall_nanos()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{Decimal, dec};
    use brokapi_core::{InstrumentKey, NativeId};
    use brokapi_tbank_protocol::money::quotation;

    #[test]
    fn full_instrument_mapping() {
        let i = pb::Instrument {
            uid: "u1".into(),
            figi: "BBG".into(),
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            isin: "RU0009029540".into(),
            lot: 10,
            currency: "rub".into(),
            min_price_increment: Some(quotation("0.01")),
            instrument_kind: pb::InstrumentType::Share as i32,
            name: "Sberbank".into(),
            ..Default::default()
        };
        let m = instrument_from_full(&i);
        assert_eq!(m.id.key, InstrumentKey::new("SBER", "TQBR"));
        assert_eq!(m.id.native, NativeId::Uid("u1".into()));
        assert_eq!(m.kind, InstrumentKind::Stock);
        let p = trading_params(&m.id, &i);
        assert_eq!(
            (
                p.lot_size.decimal(),
                p.price_step.decimal(),
                p.min_qty.decimal()
            ),
            (dec!(10.0), dec!(0.01), dec!(10.0))
        );
    }

    #[test]
    fn trading_status_mapping() {
        use pb::SecurityTradingStatus as S;
        assert_eq!(availability(S::NormalTrading), TradingAvailability::Open);
        assert_eq!(availability(S::BreakInTrading), TradingAvailability::Break);
        assert_eq!(
            availability(S::NotAvailableForTrading),
            TradingAvailability::Closed
        );
        assert_eq!(
            availability(S::ClosingAuction),
            TradingAvailability::Unknown
        );
        assert_eq!(availability(S::Unspecified), TradingAvailability::Unknown);
    }
}
