//! Level-3 tests against the live T-Bank SANDBOX. Never run in CI and never by an agent: they need
//! a sandbox token. Run by hand:
//!
//! ```text
//! # .env (git-ignored)
//! TBANK_TOKEN=t.xxxxxxxx
//! TBANK_SANDBOX=true
//! cargo test -p brokapi-tbank --features integration-live --test live_sandbox -- --nocapture --test-threads=1
//! ```
//!
//! The tests open their own sandbox account, top it up with 1 000 000 rub, trade a far-from-market
//! limit order and close the account. They refuse to run against production (`TBANK_SANDBOX` must be
//! true). Questions these tests are meant to answer are listed on the live runs page of the book (<https://brokapi.runinfra.ru/live.html>).

#![cfg(feature = "integration-live")]

use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::time::Duration;

use brokapi_conformance as conf;
use brokapi_core::{
    ClientOrderId, Envelope, Event, InstrumentId, InstrumentKey, InstrumentQuery, InstrumentSource,
    NativeId, OrderExecutor, OrderState, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode,
    Side,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::default_limits;
use brokapi_tbank::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::money::money;
use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::{TbankClient, TbankConfig};
use tokio::sync::mpsc;

fn config() -> TbankConfig {
    let cfg = TbankConfig::from_env().expect("TBANK_TOKEN in .env");
    assert!(
        cfg.sandbox,
        "live tests run on the sandbox only: set TBANK_SANDBOX=true"
    );
    cfg
}

/// Opens a fresh sandbox account with money on it.
async fn sandbox_account(cfg: &TbankConfig) -> String {
    let client = TbankClient::new(cfg).unwrap();
    let id = client
        .sandbox()
        .open_sandbox_account(pb::OpenSandboxAccountRequest {
            name: Some("connector-live-test".into()),
        })
        .await
        .expect("open sandbox account")
        .into_inner()
        .account_id;
    client
        .sandbox()
        .sandbox_pay_in(pb::SandboxPayInRequest {
            account_id: id.clone(),
            amount: Some(money("rub", "1_000_000.0")),
        })
        .await
        .expect("pay in");
    id
}

async fn close_account(cfg: &TbankConfig, id: &str) {
    let client = TbankClient::new(cfg).unwrap();
    let _ = client
        .sandbox()
        .close_sandbox_account(pb::CloseSandboxAccountRequest {
            account_id: id.to_string(),
        })
        .await;
}

async fn find_sber(md: &MarketDataConnector) -> InstrumentId {
    let found = md
        .find(InstrumentQuery::ByTicker {
            ticker: "SBER".into(),
            exchange: Some("TQBR".into()),
        })
        .await
        .expect("find SBER");
    found.into_iter().next().expect("SBER on TQBR").id
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

#[tokio::test]
async fn market_data_scenarios() {
    let (tx, mut rx) = mpsc::channel(512);
    let md = MarketDataConnector::start(MarketDataConfig::new(config(), "tbank-md"), limiter(), tx)
        .await
        .unwrap();
    let sber = find_sber(&md).await;
    assert_eq!(sber.key, InstrumentKey::new("SBER", "TQBR"));
    conf::instrument_lookup(
        &md,
        InstrumentQuery::ByTicker {
            ticker: "SBER".into(),
            exchange: None,
        },
        &sber,
    )
    .await;
    conf::order_book_snapshot(&md, &sber, 10).await;
    // the stream only moves while the exchange is open
    if std::env::var("TBANK_LIVE_STREAMS").is_ok() {
        let sub = conf::order_book_stream(&md, &mut rx, "tbank-md", &sber).await;
        conf::unsubscribe(&md, sub).await;
    }
    md.shutdown(ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    })
    .await;
}

#[tokio::test]
async fn trading_scenarios() {
    let cfg = config();
    let account = sandbox_account(&cfg).await;
    let (md_tx, _md_rx) = mpsc::channel(64);
    let lim = limiter();
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "tbank-md"),
        Arc::clone(&lim),
        md_tx,
    )
    .await
    .unwrap();
    let sber = find_sber(&md).await;

    let (tx, mut rx) = mpsc::channel::<Envelope>(512);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg.clone().with_account(&account), "tbank-ud"),
        lim,
        tx,
    )
    .await
    .unwrap();
    assert!(ud.caps().sandbox);
    conf::reconcile(&ud).await;
    ud.subscribe_order_state()
        .await
        .expect("order state stream");

    // far from the market: a buy at 1 rub rests in the book
    conf::place_and_cancel(&ud, &sber, 1.0).await;

    // Q1 (open question): what does the broker do with a repeated order_id (idempotency)?
    let cid = ClientOrderId::new_v4();
    let req = PlaceOrderRequest {
        cid,
        instrument: sber.clone(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::new(1.0),
        price: Some(Price::new(1.0)),
    };
    ud.place(req.clone()).await.expect("first place");
    println!(
        "order live: {:?}",
        ud.active_orders()
            .await
            .unwrap()
            .iter()
            .map(|o| o.cid)
            .collect::<Vec<_>>()
    );
    // The library refuses to repeat a place. To measure the broker, send the key again with the raw client:
    let raw = TbankClient::new(&cfg).unwrap();
    let second = raw
        .orders()
        .post_order(pb::PostOrderRequest {
            quantity: 1,
            price: Some(brokapi_tbank_protocol::money::quotation("1.0")),
            direction: pb::OrderDirection::Buy as i32,
            account_id: account.clone(),
            order_type: pb::OrderType::Limit as i32,
            order_id: cid.hyphenated().to_string(),
            instrument_id: match &sber.native {
                NativeId::Uid(u) => u.clone(),
                other => panic!("{other:?}"),
            },
            ..Default::default()
        })
        .await;
    println!("MEASURE repeated order_id -> {second:?}");
    println!(
        "MEASURE live orders after the repeat: {}",
        ud.active_orders().await.unwrap().len()
    );
    ud.cancel(cid).await.ok();

    // Q2: events of the order-state stream carry our cid as order_request_id
    let mut saw_ours = false;
    while let Ok(Some(env)) = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
        if let Event::Order(s) = env.payload {
            println!("order event {:?} {:?}", s.cid, s.state);
            saw_ours |= s.cid == cid
                && matches!(
                    s.state,
                    OrderState::Acked { .. } | OrderState::Cancelled { .. }
                );
        }
    }
    assert!(saw_ours, "no event with our cid");

    ud.shutdown(ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    })
    .await;
    md.shutdown(ShutdownMode::Immediate).await;
    close_account(&cfg, &account).await;
}

/// Stop orders on the sandbox: far from the market, see in the list, cancel. Open questions:
/// does the sandbox endpoint serve `StopOrdersService` (else `SandboxService/PostSandboxStopOrder`
/// through `raw()`), does `GetStopOrders` need `from`/`to`, does the broker keep our `order_id`.
#[tokio::test]
async fn stop_order_scenarios() {
    let cfg = config();
    let account = sandbox_account(&cfg).await;
    let (md_tx, _md_rx) = mpsc::channel(64);
    let lim = limiter();
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "tbank-md"),
        Arc::clone(&lim),
        md_tx,
    )
    .await
    .unwrap();
    let sber = find_sber(&md).await;
    let (tx, _rx) = mpsc::channel::<Envelope>(512);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg.clone().with_account(&account), "tbank-ud"),
        lim,
        tx,
    )
    .await
    .unwrap();
    conf::reconcile(&ud).await;
    assert!(ud.caps().supports_stop_orders);
    // SELL stops at 1 rub are far below any market price and never trigger.
    conf::stop_order_far_from_market(&ud, &sber, 1.0).await;
    close_account(&cfg, &account).await;
}

#[tokio::test]
async fn portfolio_scenarios() {
    let cfg = config();
    let account = sandbox_account(&cfg).await;
    let (tx, mut rx) = mpsc::channel(64);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg.clone().with_account(&account), "tbank-ud"),
        limiter(),
        tx,
    )
    .await
    .unwrap();
    conf::portfolio(&ud, &mut rx, "tbank-ud").await;
    ud.shutdown(ShutdownMode::Immediate).await;
    close_account(&cfg, &account).await;
}

#[tokio::test]
async fn accounts_are_listed_for_the_token() {
    let cfg = config();
    let accounts = TbankClient::new(&cfg)
        .unwrap()
        .list_accounts()
        .await
        .expect("GetAccounts");
    println!(
        "accounts: {:?}",
        accounts
            .iter()
            .map(|a| (&a.id, a.status))
            .collect::<Vec<_>>()
    );
}
