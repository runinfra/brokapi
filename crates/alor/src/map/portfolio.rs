//! Portfolio mapping: positions and money of ALOR to `brokapi-core` positions and balances.

use std::time::Instant;

use brokapi_alor_protocol::http::models::PositionRecord;
use brokapi_core::{Balance, Money, Portfolio, Position, Price, Qty};

use super::timestamps;
use crate::util::instrument_id;

/// A money position (`isCurrency`) is a balance, not a position.
pub fn is_money(r: &PositionRecord) -> bool {
    r.is_currency == Some(true)
}

/// The balance of a money position: the amount of the currency named by the symbol. The broker reports the
/// amount in the quantity; it does not split a part blocked by orders, so `blocked` is zero.
pub fn balance(r: &PositionRecord, now: Instant) -> Balance {
    Balance {
        currency: r.symbol.clone(),
        available: Money::new(r.qty_units.unwrap_or_default()),
        blocked: Money::ZERO,
        ts: timestamps(now, None),
    }
}

/// A position in an instrument. `board` is the trading mode of the instrument (the broker does not name it in a
/// position, the connector looks it up). The quantity is in pieces and signed. The average price is `None` when
/// the broker gives none or zero (futures).
pub fn position(r: &PositionRecord, board: &str, now: Instant) -> Position {
    Position {
        instrument: instrument_id(&r.symbol, board),
        qty: Qty::new(r.qty_units.unwrap_or_default()),
        avg_price: r.avg_price.filter(|p| !p.is_zero()).map(Price::new),
        current_price: None,
        ts: timestamps(now, None),
    }
}

/// The portfolio from the positions of the broker; `board_of` gives the board of a ticker.
pub fn portfolio(
    records: &[PositionRecord],
    board_of: impl Fn(&str) -> String,
    now: Instant,
) -> Portfolio {
    let mut balances = Vec::new();
    let mut positions = Vec::new();
    for r in records {
        if is_money(r) {
            balances.push(balance(r, now));
        } else if r.qty_units.is_some_and(|q| !q.is_zero()) {
            positions.push(position(r, &board_of(&r.symbol), now));
        }
    }
    Portfolio {
        balances,
        positions,
        ts: timestamps(now, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn money_is_a_balance_and_instruments_are_positions() {
        let recs: Vec<PositionRecord> = serde_json::from_str(
            r#"[{"symbol":"RUB","qtyUnits":100000.55,"isCurrency":true},
                {"symbol":"SBER","qtyUnits":30,"avgPrice":250.1,"isCurrency":false},
                {"symbol":"GAZP","qtyUnits":0,"isCurrency":false}]"#,
        )
        .unwrap();
        let p = portfolio(&recs, |_| "TQBR".to_string(), Instant::now());
        assert_eq!(p.balances.len(), 1);
        assert_eq!(p.balances[0].currency, "RUB");
        assert_eq!(p.balances[0].available.to_string(), "100000.55");
        // A closed position (zero) is not listed.
        assert_eq!(p.positions.len(), 1);
        assert_eq!(p.positions[0].instrument.key.display(), "SBER@TQBR");
        assert_eq!(p.positions[0].qty, Qty::from(30));
    }
}
