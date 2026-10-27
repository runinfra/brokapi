//! Spec 3.1: one token, several accounts. One `UserDataConnector` per account, each sees only its own.

mod mock_server;

use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::time::Duration;

use brokapi_core::{
    ClientOrderId, Envelope, Event, ExecutionSource, InstrumentId, InstrumentKey, NativeId,
    OrderExecutor, OrderState, OrderType, PlaceOrderRequest, PortfolioSource, Price, Qty,
    ShutdownMode, Side, SubscribeOpts, TimeRange, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::default_limits;
use brokapi_tbank::{UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::{TbankClient, TbankConfig};
use mock_server::{ACCOUNT_A, ACCOUNT_B, MockOptions, MockServer, TOKEN};
use tokio::sync::mpsc;
use tokio::time::timeout;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

fn order(cid: ClientOrderId) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid,
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(Price::from(250)),
    }
}

async fn ud_for(
    server: &MockServer,
    account: &str,
    source: &str,
    limiter: &Arc<BrokerLimiter>,
) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = UserDataConfig::new(
        TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(account),
        source,
    );
    let ud = UserDataConnector::start(cfg, Arc::clone(limiter), tx)
        .await
        .expect("ud");
    ud.reconcile(&[]).await.unwrap();
    (ud, rx)
}

#[tokio::test]
async fn two_connectors_one_token_see_only_their_own_account() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    // limits are per user: one limiter for both accounts
    let limiter = BrokerLimiter::new(&default_limits());
    let (a, mut rx_a) = ud_for(&server, ACCOUNT_A, "tbank-ud-a", &limiter).await;
    let (b, mut rx_b) = ud_for(&server, ACCOUNT_B, "tbank-ud-b", &limiter).await;
    assert_eq!((a.account_id(), b.account_id()), (ACCOUNT_A, ACCOUNT_B));

    a.subscribe_order_state().await.unwrap();
    b.subscribe_order_state().await.unwrap();
    a.subscribe_executions().await.unwrap();
    b.subscribe_executions().await.unwrap();

    let (cid_a, cid_b) = (ClientOrderId::new_v4(), ClientOrderId::new_v4());
    a.place(order(cid_a)).await.unwrap();
    b.place(order(cid_b)).await.unwrap();

    // orders
    let live_a = a.active_orders().await.unwrap();
    let live_b = b.active_orders().await.unwrap();
    assert_eq!(
        live_a.iter().map(|o| o.cid).collect::<Vec<_>>(),
        vec![cid_a]
    );
    assert_eq!(
        live_b.iter().map(|o| o.cid).collect::<Vec<_>>(),
        vec![cid_b]
    );
    assert!(
        a.order_state(cid_b).await.is_err(),
        "A cannot read B's order"
    );
    assert!(b.cancel(cid_a).await.is_err(), "B cannot cancel A's order");
    assert_eq!(
        server.orders_of(ACCOUNT_A),
        vec![cid_a.hyphenated().to_string()]
    );

    // reconcile: the ledger of A holds a cid that belongs to B -> unknown to A's account
    let report = a.reconcile(&[cid_a, cid_b]).await.unwrap();
    assert_eq!(
        report.confirmed.iter().map(|o| o.cid).collect::<Vec<_>>(),
        vec![cid_a]
    );
    assert_eq!(report.closed_offline, vec![cid_b]);
    assert!(report.foreign.is_empty());

    // portfolio and cash
    let (pa, pb) = (a.portfolio().await.unwrap(), b.portfolio().await.unwrap());
    assert_eq!(
        (pa.positions[0].qty.decimal(), pb.positions[0].qty.decimal()),
        (dec!(30.0), dec!(7.0))
    );
    assert_eq!(
        (
            pa.balances[0].available.decimal(),
            pb.balances[0].available.decimal()
        ),
        (dec!(100_000.0), dec!(5_000.0))
    );

    // history
    let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    assert_eq!(
        a.executions(range).await.unwrap()[0].qty.decimal(),
        dec!(10.0)
    );
    assert_eq!(
        b.executions(range).await.unwrap()[0].qty.decimal(),
        dec!(3.0)
    );

    // streams: an event of A reaches A's channel with A's source label and never B's
    server.fill(ACCOUNT_A, &cid_a.hyphenated().to_string());
    let mut seen_a = Vec::new();
    timeout(Duration::from_secs(5), async {
        while let Some(env) = rx_a.recv().await {
            assert_eq!(env.source.as_ref(), "tbank-ud-a");
            match env.payload {
                Event::Order(s) => {
                    assert_eq!(s.cid, cid_a);
                    seen_a.push(matches!(s.state, OrderState::Filled { .. }));
                }
                Event::Execution(x) => assert_eq!(x.cid, Some(cid_a)),
                _ => {}
            }
            if seen_a.contains(&true) {
                break;
            }
        }
    })
    .await
    .expect("A got its fill");

    // B must not have seen the fill of A. B's own fill is the fence: events come in the order the broker
    // sent them, so A's fill (sent first) would be in B's channel before B's own.
    server.fill(ACCOUNT_B, &cid_b.hyphenated().to_string());
    timeout(Duration::from_secs(5), async {
        while let Some(env) = rx_b.recv().await {
            assert_eq!(env.source.as_ref(), "tbank-ud-b");
            match env.payload {
                Event::Order(s) => {
                    assert_eq!(s.cid, cid_b, "B must not see A's order");
                    if matches!(s.state, OrderState::Filled { .. }) {
                        break;
                    }
                }
                Event::Execution(x) => {
                    assert_eq!(x.cid, Some(cid_b), "B must not see A's trades");
                }
                _ => {}
            }
        }
    })
    .await
    .expect("B got its own fill");

    // portfolio streams are per account too
    let sub_a = a
        .subscribe_portfolio(SubscribeOpts::conflate())
        .await
        .unwrap();
    let sub_b = b
        .subscribe_portfolio(SubscribeOpts::conflate())
        .await
        .unwrap();
    let first_portfolio = |mut rx: mpsc::Receiver<Envelope>, want: Decimal| async move {
        timeout(Duration::from_secs(5), async {
            while let Some(env) = rx.recv().await {
                if let Event::Portfolio(p) = env.payload {
                    assert_eq!(p.positions[0].qty.decimal(), want);
                    return env.sub;
                }
            }
            None
        })
        .await
        .expect("portfolio event")
    };
    // Each account holds its own fill of 10 on top of what it had (30 and 7), and not the fill of the other.
    assert_eq!(first_portfolio(rx_a, dec!(40.0)).await, Some(sub_a));
    assert_eq!(first_portfolio(rx_b, dec!(17.0)).await, Some(sub_b));

    a.shutdown(ShutdownMode::Immediate).await;
    b.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_client_can_list_its_accounts() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let client = TbankClient::new(&TbankConfig::for_mock(TOKEN, &server.endpoint)).unwrap();
    let accounts = client.list_accounts().await.unwrap();
    let ids: Vec<_> = accounts.iter().map(|a| a.id.as_str()).collect();
    assert_eq!(ids, vec![ACCOUNT_A, ACCOUNT_B]);
}
