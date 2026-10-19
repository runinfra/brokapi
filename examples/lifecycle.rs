//! Start, restart and stop of a bot on BCS: the order of actions and what each one returns.
//!
//! The code of the page "Запуск, рестарт и остановка бота" of the book (docs/src/lifecycle.md). `run` plays
//! two runs of a bot against the in-process BCS imitation: the first places an order and stops without
//! cancelling it, the second starts, reconciles with the journal and finds the order. No network, no money.
//!
//! ```text
//! cargo run --example lifecycle
//! ```
#![allow(dead_code)]

use std::time::Duration;

use brokapi::bcs::limits::default_limits;
use brokapi::bcs::protocol::config::BcsConfig;
use brokapi::bcs::{UserDataConfig, UserDataConnector};
use brokapi::core::{
    ClientOrderId, Envelope, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType,
    PlaceOrderRequest, Price, Qty, ShutdownMode, Side,
};
use brokapi::runtime::BrokerLimiter;
use tokio::sync::mpsc;

async fn start_ud(
    cfg: BcsConfig,
) -> Result<(UserDataConnector, mpsc::Receiver<Envelope>), Box<dyn std::error::Error>> {
    let (tx, events) = mpsc::channel(1024);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "bcs-ud"),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await?;
    Ok((ud, events))
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    let sber = InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    );

    // First run: one order, and the bot goes away without cancelling it. `journal` is the bot's own file.
    let (ud, _events) = start_ud(cfg.clone()).await?;
    ud.reconcile(&[]).await?;
    let cid = ClientOrderId::new_v4();
    let mut journal: Vec<ClientOrderId> = vec![cid];
    ud.place(PlaceOrderRequest {
        cid,
        instrument: sber,
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(1),
        price: Some(Price::from(200)),
    })
    .await?;
    ud.shutdown(ShutdownMode::Immediate).await;
    println!("first run stopped, the order stays on the exchange");

    // ANCHOR: start
    // Second run, after a restart. The gate is closed after `start`: commands return NotReconciled.
    let (ud, mut events) = start_ud(cfg).await?;
    // Subscribe BEFORE the reconcile: an event between the snapshot and the subscription would be lost.
    ud.subscribe_order_state().await?;
    // ANCHOR_END: start

    // ANCHOR: reconcile
    // The journal says which orders the bot thinks are alive.
    let report = ud.reconcile(&journal).await?;
    for order in &report.confirmed {
        println!(
            "confirmed {}: still at the broker, filled {}",
            order.cid,
            order.filled.decimal()
        );
    }
    for cid in &report.closed_offline {
        // Find out how it ended: order_state(cid) and executions(...), then write it to the journal.
        println!("closed offline {cid}: ask order_state and write the result");
    }
    for order in &report.foreign {
        // Not ours. Do not touch: a person decides.
        println!("foreign {}: shown to a person", order.cid);
    }
    // ANCHOR_END: reconcile

    // ANCHOR: work
    // The gate is open now. The bot trades. Here it takes its own order off.
    ud.cancel(cid).await?;
    journal.clear();
    // ANCHOR_END: work

    // ANCHOR: shutdown
    // Stop: new commands get ShuttingDown, subscriptions are closed, the report says what was left.
    let report = ud
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(5),
        })
        .await;
    println!(
        "stopped: {} subscription(s) closed, {} failed, {} command(s) without an answer",
        report.unsubscribed,
        report.unsubscribe_failed,
        report.inflight_abandoned.len()
    );
    // ANCHOR_END: shutdown
    while events.try_recv().is_ok() {}
    assert_eq!(report.inflight_abandoned.len(), 0);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}
