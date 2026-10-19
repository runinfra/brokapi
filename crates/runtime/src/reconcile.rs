//! Bootstrap ledger reconciliation.

use std::collections::{HashMap, HashSet};

use brokapi_core::{ClientOrderId, OrderSnapshot, ReconcileReport};

/// Compares the client's ledger of orders with the broker's active orders and sorts them into three
/// groups.
///
/// - `confirmed`: in both. The broker's snapshot is returned.
/// - `closed_offline`: in the ledger, not active at the broker. They filled, were cancelled or
///   were rejected while the client was away. Their cids are returned. The function does not say
///   which of these happened. Ask the broker for the order state.
/// - `foreign`: active at the broker, not in the ledger. They were placed by someone else (another
///   program, the broker's app). They must not be touched. Why: cancelling an order the client
///   never created could damage someone else's strategy.
///
/// Broker orders whose cid is the nil UUID (no client id) are ignored. They cannot be matched with
/// a ledger entry and are not reported as foreign. The order of the returned lists is not defined.
///
/// The function is pure: no I/O and no state.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use brokapi_core::{
///     BrokerOrderId, ClientOrderId, InstrumentId, InstrumentKey, NativeId, OrderSnapshot,
///     OrderState, OrderType, Price, Qty, Side, Timestamps, UnixNanos,
/// };
/// use brokapi_runtime::reconcile_ledger;
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let order = |cid: ClientOrderId| OrderSnapshot {
///     cid,
///     broker_id: None,
///     instrument: sber.clone(),
///     state: OrderState::Acked { cid, broker_id: BrokerOrderId::new("1") },
///     side: Side::Buy,
///     order_type: OrderType::Limit,
///     qty: Qty::from(1),
///     filled: Qty::from(0),
///     price: Some(Price::from(100)),
///     ts: Timestamps::new(UnixNanos::ZERO, Instant::now()),
/// };
/// let (kept, gone, other) = (ClientOrderId::new_v4(), ClientOrderId::new_v4(), ClientOrderId::new_v4());
///
/// // We remember `kept` and `gone`. The broker shows `kept` and one we never placed.
/// let ledger = [order(kept), order(gone)];
/// let at_broker = [order(kept), order(other)];
/// let report = reconcile_ledger(&ledger, &at_broker);
///
/// assert_eq!(report.confirmed.len(), 1);
/// assert_eq!(report.confirmed[0].cid, kept);
/// assert_eq!(report.closed_offline, vec![gone]);
/// assert_eq!(report.foreign[0].cid, other);
/// ```
pub fn reconcile_ledger(
    ledger: &[OrderSnapshot],
    broker_active: &[OrderSnapshot],
) -> ReconcileReport {
    let ledger_map: HashMap<ClientOrderId, &OrderSnapshot> =
        ledger.iter().map(|o| (o.cid, o)).collect();
    let broker_map: HashMap<ClientOrderId, &OrderSnapshot> = broker_active
        .iter()
        .filter(|o| !o.cid.is_nil())
        .map(|o| (o.cid, o))
        .collect();

    let ledger_ids: HashSet<_> = ledger_map.keys().copied().collect();
    let broker_ids: HashSet<_> = broker_map.keys().copied().collect();

    let confirmed = ledger_ids
        .intersection(&broker_ids)
        .filter_map(|cid| broker_map.get(cid).copied().cloned())
        .collect();

    let closed_offline = ledger_ids.difference(&broker_ids).copied().collect();

    let foreign = broker_ids
        .difference(&ledger_ids)
        .filter_map(|cid| broker_map.get(cid).copied().cloned())
        .collect();

    ReconcileReport {
        confirmed,
        closed_offline,
        foreign,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use brokapi_core::{
        InstrumentId, InstrumentKey, NativeId, OrderState, OrderType, Qty, Side, Timestamps,
        UnixNanos,
    };
    use std::time::Instant;

    fn snapshot(cid: u8) -> OrderSnapshot {
        OrderSnapshot {
            cid: ClientOrderId::from_bytes([cid; 16]),
            broker_id: None,
            instrument: InstrumentId::new(
                InstrumentKey::new("SBER", "TQBR"),
                NativeId::TickerClass {
                    ticker: "SBER".into(),
                    class_code: "TQBR".into(),
                },
            ),
            state: OrderState::Acked {
                cid: ClientOrderId::from_bytes([cid; 16]),
                broker_id: brokapi_core::BrokerOrderId::new("1"),
            },
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: Qty::from(1),
            filled: Qty::from(0),
            price: None,
            ts: Timestamps::new(UnixNanos::ZERO, Instant::now()),
        }
    }

    #[test]
    fn splits_confirmed_closed_foreign() {
        let ledger = vec![snapshot(1), snapshot(2)];
        let broker = vec![snapshot(1), snapshot(3)];
        let report = reconcile_ledger(&ledger, &broker);
        assert_eq!(report.confirmed.len(), 1);
        assert_eq!(report.closed_offline.len(), 1);
        assert_eq!(report.foreign.len(), 1);
    }
}
