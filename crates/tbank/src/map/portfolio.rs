//! Portfolio, positions, cash balances.

use std::collections::BTreeMap;
use std::time::Instant;

use brokapi_core::{Balance, InstrumentId, Money, Portfolio, Position, Qty};
use brokapi_tbank_protocol::v1 as pb;

use crate::ids::{id_from_parts, money_of, price_of_money, qty_of_quotation, stamps};

/// Cash lives in `GetPositions.money` / `blocked`; currency instruments in `GetPortfolio` repeat it
/// and are left out of `positions`.
pub fn balances(
    money: &[pb::MoneyValue],
    blocked: &[pb::MoneyValue],
    now: Instant,
) -> Vec<Balance> {
    let mut by_ccy: BTreeMap<String, (Money, Money)> = BTreeMap::new();
    for m in money {
        by_ccy
            .entry(m.currency.to_uppercase())
            .or_insert((Money::ZERO, Money::ZERO))
            .0 += money_of(m);
    }
    for b in blocked {
        by_ccy
            .entry(b.currency.to_uppercase())
            .or_insert((Money::ZERO, Money::ZERO))
            .1 += money_of(b);
    }
    by_ccy
        .into_iter()
        .map(|(currency, (available, blocked))| Balance {
            currency,
            available,
            blocked,
            ts: stamps(now, None),
        })
        .collect()
}

/// Cash of a `GetPositions` answer: money and blocked money by currency.
pub fn balances_from_positions(p: &pb::PositionsResponse, now: Instant) -> Vec<Balance> {
    balances(&p.money, &p.blocked, now)
}

/// Cash of a `PositionsStream` message: available and blocked money by currency.
pub fn balances_from_stream(money: &[pb::PositionsMoney], now: Instant) -> Vec<Balance> {
    let avail: Vec<pb::MoneyValue> = money
        .iter()
        .filter_map(|m| m.available_value.clone())
        .collect();
    let blocked: Vec<pb::MoneyValue> = money
        .iter()
        .filter_map(|m| m.blocked_value.clone())
        .collect();
    balances(&avail, &blocked, now)
}

/// `quantity` is in pieces (the portfolio is not in lots).
pub fn positions_from_portfolio(p: &pb::PortfolioResponse, now: Instant) -> Vec<Position> {
    p.positions
        .iter()
        .filter(|x| x.instrument_type != "currency")
        .map(|x| Position {
            instrument: id_from_parts(&x.instrument_uid, &x.figi, &x.ticker, &x.class_code),
            qty: x
                .quantity
                .as_ref()
                .map(qty_of_quotation)
                .unwrap_or(Qty::ZERO),
            avg_price: x.average_position_price.as_ref().map(price_of_money),
            current_price: x.current_price.as_ref().map(price_of_money),
            ts: stamps(now, None),
        })
        .collect()
}

/// Securities of a positions message (balance + blocked, pieces). No prices in this message.
pub fn positions_from_securities(
    securities: &[pb::PositionsSecurities],
    instruments: Vec<InstrumentId>,
    now: Instant,
) -> Vec<Position> {
    securities
        .iter()
        .zip(instruments)
        .map(|(s, instrument)| Position {
            instrument,
            qty: Qty::from(s.balance + s.blocked),
            avg_price: None,
            current_price: None,
            ts: stamps(now, None),
        })
        .collect()
}

/// A [`Portfolio`] from a `GetPortfolio` answer and the cash you already have.
///
/// Positions come from the answer, with currency holdings left out.
pub fn portfolio(p: &pb::PortfolioResponse, cash: Vec<Balance>, now: Instant) -> Portfolio {
    Portfolio {
        balances: cash,
        positions: positions_from_portfolio(p, now),
        ts: stamps(now, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{Decimal, dec};
    use brokapi_tbank_protocol::money::{money, quotation};

    #[test]
    fn cash_by_currency_available_and_blocked() {
        let b = balances(
            &[money("rub", "1000.5"), money("usd", "10.0")],
            &[money("rub", "200.0")],
            Instant::now(),
        );
        assert_eq!(b.len(), 2);
        let rub = b.iter().find(|x| x.currency == "RUB").unwrap();
        assert_eq!(
            (rub.available.decimal(), rub.blocked.decimal()),
            (dec!(1000.5), dec!(200.0))
        );
    }

    #[test]
    fn currencies_are_not_positions() {
        let p = pb::PortfolioResponse {
            positions: vec![
                pb::PortfolioPosition {
                    instrument_type: "share".into(),
                    ticker: "SBER".into(),
                    class_code: "TQBR".into(),
                    instrument_uid: "u".into(),
                    quantity: Some(quotation("30.0")),
                    average_position_price: Some(money("rub", "250.0")),
                    current_price: Some(money("rub", "260.0")),
                    ..Default::default()
                },
                pb::PortfolioPosition {
                    instrument_type: "currency".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let pos = positions_from_portfolio(&p, Instant::now());
        assert_eq!(pos.len(), 1);
        assert_eq!(pos[0].qty.decimal(), dec!(30.0));
        assert_eq!(pos[0].avg_price.unwrap().decimal(), dec!(250.0));
        assert_eq!(pos[0].instrument.key.symbol, "SBER");
    }
}
