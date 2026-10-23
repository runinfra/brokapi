//! The portfolio: positions and the balances of the wallet.

use std::time::Instant;

use brokapi_bybit_protocol::http::models::{CoinBalance, PositionRecord, WalletRecord};
use brokapi_core::{Balance, Decimal, Money, Portfolio, Position, Price, Qty};

use super::{broker_ts, timestamps};
use crate::util::instrument_id;

/// The balance of one coin.
///
/// `blocked` is the margin the open orders and positions hold plus what open spot orders lock. `available` is the
/// equity (the wallet balance plus the unrealised profit) less `blocked`, never below zero: what can still open a
/// position. The exchange shows an available figure only for the whole account (in USD), so this per coin figure is a
/// reading of the connector, not the exchange's own.
pub fn balance(c: &CoinBalance, now: Instant) -> Balance {
    let blocked = c.locked + c.total_order_im + c.total_position_im;
    Balance {
        currency: c.coin.clone(),
        available: Money::new((c.equity - blocked).max(Decimal::ZERO)),
        blocked: Money::new(blocked),
        ts: timestamps(now, None),
    }
}

/// The balances of the coins that hold anything.
pub fn balances(wallets: &[WalletRecord], now: Instant) -> Vec<Balance> {
    wallets
        .iter()
        .flat_map(|w| w.coin.iter())
        .filter(|c| !(c.equity.is_zero() && c.wallet_balance.is_zero()))
        .map(|c| balance(c, now))
        .collect()
}

/// A position. The quantity is signed: a short position is negative.
pub fn position(r: &PositionRecord, category: &str, now: Instant) -> Position {
    let size = if r.side == "Sell" { -r.size } else { r.size };
    let category = if r.category.is_empty() {
        category
    } else {
        &r.category
    };
    Position {
        instrument: instrument_id(&r.symbol, category),
        qty: Qty::new(size),
        avg_price: r.avg_price.filter(|p| !p.is_zero()).map(Price::new),
        current_price: r.mark_price.filter(|p| !p.is_zero()).map(Price::new),
        ts: timestamps(now, broker_ts(r.updated_time)),
    }
}

/// The open positions (a size of zero is no position).
pub fn positions(records: &[PositionRecord], category: &str, now: Instant) -> Vec<Position> {
    records
        .iter()
        .filter(|r| !r.size.is_zero())
        .map(|r| position(r, category, now))
        .collect()
}

/// The whole portfolio.
pub fn portfolio(
    wallets: &[WalletRecord],
    records: &[PositionRecord],
    category: &str,
    now: Instant,
) -> Portfolio {
    Portfolio {
        balances: balances(wallets, now),
        positions: positions(records, category, now),
        ts: timestamps(now, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_position_is_negative_and_a_closed_one_is_not_listed() {
        let recs: Vec<PositionRecord> = serde_json::from_str(
            r#"[{"symbol":"BTCUSDT","side":"Sell","size":"0.3","avgPrice":"81000.5","markPrice":"80000"},
                {"symbol":"ETHUSDT","side":"Buy","size":"2"},
                {"symbol":"SOLUSDT","side":"","size":"0"}]"#,
        )
        .unwrap();
        let p = positions(&recs, "linear", Instant::now());
        assert_eq!(p.len(), 2);
        assert_eq!(p[0].qty.to_string(), "-0.3");
        assert_eq!(p[0].avg_price.unwrap().to_string(), "81000.5");
        assert_eq!(p[0].instrument.key.display(), "BTCUSDT@linear");
        assert_eq!(p[1].qty.to_string(), "2");
        assert!(p[1].avg_price.is_none());
    }

    #[test]
    fn the_free_money_is_the_equity_less_what_orders_and_positions_hold() {
        let w: Vec<WalletRecord> = serde_json::from_str(
            r#"[{"accountType":"UNIFIED","coin":[
                {"coin":"USDT","equity":"1000.5","walletBalance":"1000","totalOrderIM":"10","totalPositionIM":"90.25","unrealisedPnl":"0.5","locked":"0"},
                {"coin":"BTC","equity":"0","walletBalance":"0"}]}]"#,
        )
        .unwrap();
        let b = balances(&w, Instant::now());
        assert_eq!(b.len(), 1, "an empty coin is not listed");
        assert_eq!(b[0].currency, "USDT");
        assert_eq!(b[0].blocked.to_string(), "100.25");
        assert_eq!(b[0].available.to_string(), "900.25");
    }
}
