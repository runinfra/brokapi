//! Smoke run of the main paths of both BCS connectors.
//!
//! It starts the market data connector and the user data connector, and walks the calls a bot makes
//! first: an order book, the instrument directory, the trading status, the portfolio, the active
//! orders. Against the mock it also goes through the order path (reconcile, place, read, cancel) and
//! checks that a refused order is a definite refusal.
//!
//! By default it runs against the in-process imitation of the broker (`brokapi-mocks`): no network, no
//! account, no money. With `BROKAPI_LIVE=1` it talks to the real broker instead, with READS ONLY:
//! the config is forced to read-only and no order is sent. The refresh token is read by
//! `BcsConfig::from_env()` (variable `BCS_REFRESH_TOKEN`, a `.env` file works).
//!
//! ```text
//! cargo run -p brokapi-bcs --example api_smoke
//! BROKAPI_LIVE=1 cargo run -p brokapi-bcs --example api_smoke
//! ```

use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::time::Duration;

use brokapi_bcs::limits::default_limits;
use brokapi_bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_conformance::REJECT_PRICE;
use brokapi_core::{
    ClientOrderId, Error, InstrumentId, InstrumentKey, InstrumentQuery, InstrumentSource, NativeId,
    OrderBook, OrderBookSource, OrderExecutor, OrderState, OrderType, PlaceOrderRequest,
    PortfolioSource, Price, Qty, ShutdownMode, Side,
};
use brokapi_mocks::bcs::MockServer;
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

fn limit_buy(price: Decimal) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(1),
        price: Some(Price::new(price)),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let live = std::env::var("BROKAPI_LIVE").as_deref() == Ok("1");
    // The mock server must live as long as the connectors.
    let (cfg, _mock) = if live {
        println!("mode: LIVE (real broker, reads only)");
        (BcsConfig::from_env()?.with_write(false), None)
    } else {
        println!("mode: mock (set BROKAPI_LIVE=1 for the real broker)");
        let server = MockServer::start(Default::default()).await;
        (
            BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true),
            Some(server),
        )
    };

    // One limiter for the whole account: the limits belong to the token.
    let limiter: Arc<BrokerLimiter> = BrokerLimiter::new(&default_limits());
    let (md_tx, _md_events) = mpsc::channel(256);
    let (ud_tx, _ud_events) = mpsc::channel(256);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "bcs-md"),
        Arc::clone(&limiter),
        md_tx,
    )
    .await?;
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, ud_tx).await?;

    // Market data.
    let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&sber(), 5).await? else {
        return Err("REST answers a snapshot".into());
    };
    println!("order book SBER: {} bids, {} asks", bids.len(), asks.len());
    let found = md
        .find(InstrumentQuery::ByTicker {
            ticker: "SBER".into(),
            exchange: None,
        })
        .await?;
    println!("directory: {} instrument(s) for SBER", found.len());
    let status = md.trading_status(&sber()).await?;
    println!("trading status: {:?}", status.availability);

    // User data, reads.
    let portfolio = ud.portfolio().await?;
    println!(
        "portfolio: {} balance(s), {} position(s)",
        portfolio.balances.len(),
        portfolio.positions.len()
    );
    // Commands wait for the first reconcile. Reconcile only reads.
    ud.reconcile(&[]).await?;
    println!("active orders: {}", ud.active_orders().await?.len());

    // User data, orders: only against the mock, never on the real account.
    if live {
        println!("orders: skipped in live mode");
    } else {
        let req = limit_buy(dec!(100.0));
        let cid = req.cid;
        let ack = ud.place(req).await?;
        println!("placed, broker order id {}", ack.broker_id.as_str());
        ud.cancel(cid).await?;
        let state = tokio::time::timeout(Duration::from_secs(5), ud.order_state(cid)).await??;
        assert!(
            matches!(state.state, OrderState::Cancelled { .. }),
            "cancelled: {state:?}"
        );
        println!("cancelled");
        match ud.place(limit_buy(REJECT_PRICE.decimal())).await {
            Err(Error::Rejected { code, .. }) => println!("refused as expected: {code}"),
            other => return Err(format!("expected a refusal, got {other:?}").into()),
        }
    }

    md.shutdown(ShutdownMode::Immediate).await;
    ud.shutdown(ShutdownMode::Immediate).await;
    println!("ok");
    Ok(())
}
