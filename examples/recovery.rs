//! What a bot does when something went wrong: an order with no answer (`Unknown`) and a channel that
//! overflowed (`Overflow`, `Degraded`).
//!
//! The code of the page "Если что-то пошло не так" of the book (docs/src/rust/recovery.md). `run` makes a real
//! `Unknown` on the T-Bank mock (the mock answers 1.5 s late, the connector gives up after 200 ms) and
//! resolves it with `order_state`. It runs against the in-process imitation: no network, no money.
//!
//! ```text
//! cargo run --example recovery
//! ```
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

// ANCHOR: imports
use brokapi::core::{
    ClientOrderId, Envelope, Error, OrderExecutor, OrderSnapshot, ReconcileReport,
};
use tokio::sync::mpsc;
// ANCHOR_END: imports

// ANCHOR: resolve_unknown
/// `place` answered `Unknown`: ask the broker about the `cid` until it answers or `patience` runs out.
///
/// `Ok(Some(order))` - the order exists, continue with its state. `Ok(None)` - the broker still does not
/// know it after `patience`: it was not placed, and a new order needs a NEW `cid`.
pub async fn resolve_unknown<U: OrderExecutor>(
    ud: &U,
    cid: ClientOrderId,
    patience: Duration,
) -> Result<Option<OrderSnapshot>, Error> {
    let deadline = tokio::time::Instant::now() + patience;
    loop {
        match ud.order_state(cid).await {
            Ok(order) => return Ok(Some(order)),
            // "Not found" right after a cut is not final: the request may still be on its way.
            Err(Error::Rejected { code, .. }) if code.eq_ignore_ascii_case("not_found") => {}
            // A limit or a network error says nothing about the order: wait and ask again.
            Err(Error::RateLimited { .. } | Error::Transport(_)) => {}
            Err(other) => return Err(other),
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
// ANCHOR_END: resolve_unknown

// ANCHOR: after_overflow
/// `Event::Overflow` came, or a command returned `Degraded`: read the channel to the end, then reconcile.
///
/// `handle` gets every event that was waiting, so nothing that did arrive is lost. A successful
/// `reconcile` opens the gate again. After it, restore what was lost from the snapshots: the states in
/// `confirmed`, the fills from `executions(...)` since `lost_from`, the portfolio from `portfolio()`.
pub async fn after_overflow<U: OrderExecutor>(
    ud: &U,
    events: &mut mpsc::Receiver<Envelope>,
    ledger: &[ClientOrderId],
    mut handle: impl FnMut(Envelope),
) -> Result<ReconcileReport, Error> {
    while let Ok(env) = events.try_recv() {
        handle(env);
    }
    ud.reconcile(ledger).await
}
// ANCHOR_END: after_overflow

/// Makes an `Unknown` on the T-Bank mock and resolves it.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    use brokapi::core::{
        InstrumentId, InstrumentKey, NativeId, OrderType, PlaceOrderRequest, Price, Qty,
        ShutdownMode, Side,
    };
    use brokapi::runtime::BrokerLimiter;
    use brokapi::tbank::limits::default_limits;
    use brokapi::tbank::protocol::TbankConfig;
    use brokapi::tbank::{UserDataConfig, UserDataConnector};
    use brokapi_mocks::tbank::{ACCOUNT_A, MockOptions, MockServer, TOKEN};

    // The mock answers a place 1.5 s late, the connector waits 200 ms: the answer is lost.
    let opts = Arc::new(MockOptions::default());
    opts.post_delay_ms.store(1500, Ordering::SeqCst);
    let server = MockServer::start(opts).await;
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint)
        .with_account(ACCOUNT_A)
        .with_request_timeout(Duration::from_millis(200));
    let (tx, mut events) = mpsc::channel(256);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "tbank-ud"),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await?;
    ud.reconcile(&[]).await?;

    let sber = InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    );
    let cid = ClientOrderId::new_v4();
    let request = PlaceOrderRequest {
        cid,
        instrument: sber,
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(1),
        price: Some(Price::from(250)),
    };
    match ud.place(request).await {
        Err(Error::Unknown) => println!("place: Unknown, the order is not sent again"),
        other => return Err(format!("expected Unknown, got {other:?}").into()),
    }

    // The order did reach the exchange late: the broker knows it after a moment.
    let found = resolve_unknown(&ud, cid, Duration::from_secs(10)).await?;
    println!(
        "resolved: {}",
        if found.is_some() {
            "the order exists"
        } else {
            "no such order"
        }
    );
    assert!(found.is_some());

    // A cid that was never placed: after the patience it is "no such order", a new cid is needed.
    let never = resolve_unknown(&ud, ClientOrderId::new_v4(), Duration::from_millis(300)).await?;
    assert!(never.is_none());
    println!("a cid that was never placed: no such order");

    // The channel is read to the end, then the gate is checked by a reconcile.
    let report = after_overflow(&ud, &mut events, &[cid], |_| {}).await?;
    println!("after overflow: {} confirmed", report.confirmed.len());
    ud.shutdown(ShutdownMode::Immediate).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}
