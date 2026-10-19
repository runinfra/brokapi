//! Account / portfolio mapping.

use std::time::Instant;

use brokapi_core::{Balance, Execution, Money, Portfolio, Position};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::accounts::GetAccountResponse;

use crate::map::order::side_from_pb;
use crate::util::{dec0, instrument_from_symbol, money, price, price0, qty0, timestamps};

fn positions(resp: &GetAccountResponse, now: Instant) -> Vec<Position> {
    resp.positions
        .iter()
        .map(|p| Position {
            instrument: instrument_from_symbol(&p.symbol),
            // Signed: long positive, short negative.
            qty: qty0(&p.quantity),
            // Absent for FORTS positions.
            avg_price: price(&p.average_price),
            current_price: price(&p.current_price),
            ts: timestamps(None, now),
        })
        .collect()
}

/// One balance per currency in `cash`. Finam reports no blocked amount per currency, so
/// `blocked` is 0.
fn balances(resp: &GetAccountResponse, now: Instant) -> Vec<Balance> {
    resp.cash
        .iter()
        .map(|m| {
            let (currency, available) = money(m);
            Balance {
                currency,
                available,
                blocked: Money::ZERO,
                ts: timestamps(None, now),
            }
        })
        .collect()
}

/// `GetAccount` answer -> portfolio: one balance per currency of `cash` and one position per
/// holding. Quantity is signed: a short position is negative. `average_price` is `None` when
/// Finam does not send it (futures positions).
pub fn map_account(resp: &GetAccountResponse, now: Instant) -> Portfolio {
    Portfolio {
        balances: balances(resp, now),
        positions: positions(resp, now),
        ts: timestamps(None, now),
    }
}

/// `AccountTrade` -> execution. `cid` is resolved by the caller (comment / order index).
pub fn map_trade(
    t: &v1::AccountTrade,
    cid: Option<brokapi_core::ClientOrderId>,
    now: Instant,
) -> Execution {
    Execution {
        cid,
        broker_order_id: brokapi_core::BrokerOrderId::new(t.order_id.clone()),
        instrument: instrument_from_symbol(&t.symbol),
        side: side_from_pb(t.side),
        price: price0(&t.price),
        qty: qty0(&t.size),
        // Only historical trades carry a commission; its currency is not part of `Execution`.
        commission: t
            .commission
            .as_ref()
            .map(|c| Money::new(dec0(&c.value)))
            .unwrap_or(Money::ZERO),
        trade_id: t.trade_id.clone(),
        ts: timestamps(t.timestamp.as_ref(), now),
    }
}
