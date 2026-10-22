//! Behaviour of the ALOR connectors that the shared scenarios do not cover: what goes over the wire, the start
//! checks, refusals of subscriptions, positions and fills.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use brokapi_alor::limits::default_limits;
use brokapi_alor::util::instrument_id;
use brokapi_alor::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, MarketDataSource,
    OrderExecutor, OrderType, PlaceOrderRequest, PortfolioSource, Price, Qty, ShutdownMode, Side,
    SubscribeOpts, TimeRange, UnixNanos, dec,
};
use brokapi_runtime::BrokerLimiter;
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "alor-ud"), limiter(), tx)
        .await
        .expect("ud start");
    (ud, rx)
}

fn limit_order(qty: i64, price: Decimal) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: instrument_id("SBER", "TQBR"),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(qty),
        price: Some(Price::new(price)),
    }
}

#[tokio::test]
async fn the_cid_goes_to_the_broker_as_the_request_id_and_as_the_comment_and_pieces_become_lots() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    // 30 pieces of SBER are 3 lots of 10.
    let req = limit_order(30, dec!(100.50));
    let cid = req.cid;
    ud.place(req).await.unwrap();

    assert_eq!(
        *opts.request_ids.lock().unwrap(),
        vec![cid.hyphenated().to_string()]
    );
    let active = ud.active_orders().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(
        active[0].cid, cid,
        "the cid comes back from the comment of the order"
    );
    assert_eq!(
        active[0].qty,
        Qty::from(30),
        "the quantity comes back in pieces"
    );
    assert_eq!(active[0].price.unwrap().to_string(), "100.50");
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_order_of_another_program_is_listed_with_a_made_up_cid_and_can_be_cancelled_by_it() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    // The order is placed by another program of the same account: through the raw layer, with a plain comment.
    use brokapi_alor_protocol::http::models::{InstrumentRef, LimitOrderRequest, UserRef};
    let body = LimitOrderRequest {
        side: "buy".into(),
        quantity: 1,
        price: dec!(100),
        instrument: InstrumentRef {
            symbol: "SBER".into(),
            exchange: "MOEX".into(),
            instrument_group: Some("TQBR".into()),
        },
        user: UserRef {
            portfolio: "D00001".into(),
        },
        comment: Some("placed by hand".into()),
        time_in_force: None,
        allow_margin: None,
        iceberg_fixed: None,
        iceberg_variance: None,
    };
    ud.raw()
        .call_command("PlaceOrder", |http| async move {
            http.create_limit_order("by-hand-1", &body).await
        })
        .await
        .unwrap();
    let report = ud.reconcile(&[]).await.unwrap();
    assert_eq!(report.foreign.len(), 1);
    let foreign = &report.foreign[0];
    assert_eq!(
        foreign.cid,
        brokapi_alor::cid::foreign_cid(foreign.broker_id.as_ref().unwrap().as_str())
    );
    ud.cancel(foreign.cid).await.unwrap();
    assert!(ud.active_orders().await.unwrap().is_empty());
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_market_order_cannot_be_replaced() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let mut req = limit_order(10, dec!(100));
    req.order_type = OrderType::Market;
    req.price = None;
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let err = ud
        .replace(brokapi_core::ReplaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            original_cid: cid,
            qty: None,
            price: Some(Price::new(dec!(101))),
        })
        .await;
    assert!(
        matches!(err, Err(Error::Rejected { ref code, .. }) if code == "not_replaceable"),
        "{err:?}"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_refused_refresh_token_stops_the_start() {
    let opts = Arc::new(MockOptions::default());
    opts.refuse_refresh.store(true, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (tx, _rx) = mpsc::channel(8);
    let cfg = AlorConfig::for_mock("revoked", &server.http_base);
    match MarketDataConnector::start(MarketDataConfig::new(cfg, "alor-md"), limiter(), tx).await {
        Err(Error::Config(e)) => assert!(e.to_string().contains("authentication refused"), "{e}"),
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("started with a revoked token"),
    }
}

#[tokio::test]
async fn without_a_portfolio_in_the_config_and_in_the_token_the_start_is_refused() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (tx, _rx) = mpsc::channel(8);
    let mut cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    cfg.portfolio = None;
    // The access token of the imitation is not a JWT and names no portfolio.
    match UserDataConnector::start(UserDataConfig::new(cfg, "alor-ud"), limiter(), tx).await {
        Err(Error::Config(e)) => assert!(e.to_string().contains("ALOR_PORTFOLIO"), "{e}"),
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("started without a portfolio"),
    }
}

#[tokio::test]
async fn a_subscription_the_broker_refuses_is_an_error_at_once_and_leaves_nothing_behind() {
    let opts = Arc::new(MockOptions::default());
    *opts.fail_subscribe_text.lock().await = Some("GAZP".into());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (tx, mut rx) = mpsc::channel(64);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "alor-md"), limiter(), tx)
        .await
        .unwrap();
    match md
        .subscribe_quotes(&instrument_id("GAZP", "TQBR"), SubscribeOpts::conflate())
        .await
    {
        Err(Error::Rejected { code, message }) => {
            assert_eq!(code, "subscription_refused");
            assert!(message.contains("no such instrument"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    // The other instrument still works, and no event of the refused one comes.
    let sub = md
        .subscribe_quotes(&instrument_id("SBER", "TQBR"), SubscribeOpts::conflate())
        .await
        .unwrap();
    let env = rx.recv().await.unwrap();
    assert_eq!(env.sub, Some(sub));
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn positions_carry_the_main_board_and_money_is_a_balance() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    let portfolio = ud.portfolio().await.unwrap();
    assert_eq!(portfolio.balances.len(), 1);
    assert_eq!(portfolio.balances[0].currency, "RUB");
    assert_eq!(portfolio.balances[0].available.to_string(), "1000000.55");
    assert_eq!(portfolio.positions.len(), 1);
    assert_eq!(portfolio.positions[0].instrument.key.display(), "SBER@TQBR");
    assert_eq!(portfolio.positions[0].qty, Qty::from(30));
    assert_eq!(
        portfolio.positions[0].avg_price.unwrap().to_string(),
        "250.1"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_fill_comes_as_an_order_event_and_an_execution_with_the_cid() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, mut rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let orders = ud.subscribe_order_state().await.unwrap();
    let fills = ud.subscribe_executions().await.unwrap();
    let req = limit_order(20, dec!(100));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    server.fill(
        &cid.hyphenated().to_string(),
        Qty::from(10),
        Price::new(dec!(99.95)),
    );

    let mut seen_partial = false;
    let mut execution = None;
    while !(seen_partial && execution.is_some()) {
        let env = rx.recv().await.expect("the stream ended");
        match env.payload {
            Event::Order(o) if o.cid == cid && o.filled == Qty::from(10) => {
                assert_eq!(env.sub, Some(orders));
                assert!(matches!(
                    o.state,
                    brokapi_core::OrderState::PartiallyFilled { .. }
                ));
                seen_partial = true;
            }
            Event::Execution(e) => {
                assert_eq!(env.sub, Some(fills));
                execution = Some(e);
            }
            _ => {}
        }
    }
    let e = execution.unwrap();
    assert_eq!(e.cid, Some(cid));
    assert_eq!(e.qty, Qty::from(10));
    assert_eq!(e.price.to_string(), "99.95");

    // The same fill is in the list of today.
    let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    let all = ud.executions(range).await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].trade_id, e.trade_id);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_last_trades_are_oldest_first_and_cut_to_the_limit() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (tx, _rx) = mpsc::channel(8);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "alor-md"), limiter(), tx)
        .await
        .unwrap();
    let trades = md
        .last_trades(&instrument_id("SBER", "TQBR"), 2)
        .await
        .unwrap();
    assert_eq!(trades.len(), 2);
    // The mock answers newest first (ids 9000, 8999): the oldest comes first here.
    assert_eq!(trades[0].trade_id.as_deref(), Some("8999"));
    assert_eq!(trades[1].trade_id.as_deref(), Some("9000"));
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_price_off_the_step_is_refused_before_anything_is_sent_and_the_cid_stays_usable() {
    use brokapi_core::InstrumentSource;
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    let (tx, _mrx) = mpsc::channel(8);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "alor-md"), limiter(), tx)
        .await
        .unwrap();
    let id = instrument_id("SBER", "TQBR");
    // The step is taught from the trading parameters of the instrument.
    ud.price_grid()
        .learn(&md.trading_params(&id).await.unwrap());
    ud.reconcile(&[]).await.unwrap();
    let mut req = limit_order(10, dec!(100.005));
    let err = ud.place(req.clone()).await;
    assert!(
        matches!(err, Err(Error::Rejected { ref code, .. }) if code == "price_off_step"),
        "{err:?}"
    );
    assert_eq!(
        opts.order_commands.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
    req.price = Some(Price::new(dec!(100.01)));
    ud.place(req).await.expect("the same cid with a good price");
    md.shutdown(ShutdownMode::Immediate).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}
