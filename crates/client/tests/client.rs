//! `Client` against the imitations of the four brokers: no network, no money.

use std::sync::Arc;

use brokapi::core::{
    ClientOrderId, Error, OrderBook, OrderBookSource, OrderExecutor, OrderType, PlaceOrderRequest,
    Price, Qty, ShutdownMode, Side,
};
use brokapi_client::{Broker, Client, ClientError, Limiter, Settings};
use brokapi_mocks::{alor, bcs, finam, settings, tbank};
use serde_json::{Value, json};

/// A running imitation of one broker and the settings that connect to it.
enum Mock {
    Bcs(bcs::MockServer),
    Tbank(tbank::MockServer),
    Finam(finam::MockServer),
    Alor(alor::MockServer),
}

impl Mock {
    async fn start(broker: Broker) -> Self {
        match broker {
            Broker::Bcs => Mock::Bcs(bcs::MockServer::start(Arc::default()).await),
            Broker::Tbank => Mock::Tbank(tbank::MockServer::start(Arc::default()).await),
            Broker::Finam => Mock::Finam(finam::MockServer::start().await),
            Broker::Alor => Mock::Alor(alor::MockServer::start(Default::default()).await),
        }
    }

    fn settings(&self) -> Settings {
        let json = match self {
            Mock::Bcs(s) => settings::bcs(s),
            Mock::Tbank(s) => settings::tbank(s),
            Mock::Finam(s) => settings::finam(s),
            Mock::Alor(s) => settings::alor(s),
        };
        Settings::parse(&json.to_string()).unwrap()
    }
}

async fn connect(broker: Broker) -> (Mock, Client) {
    let mock = Mock::start(broker).await;
    let client = Client::connect(broker.as_str(), "t", &mock.settings(), None, 256)
        .await
        .expect("connect");
    (mock, client)
}

#[tokio::test]
async fn every_broker_answers_the_same_calls() {
    for broker in Broker::ALL {
        let (_mock, client) = connect(broker).await;
        assert_eq!(client.broker(), broker);
        assert_eq!(client.name(), "t");
        let sber = client.instrument("SBER", None).await.expect("instrument");
        // Typed.
        let book = client.order_book(&sber, 5).await.expect("order book");
        assert!(matches!(book, OrderBook::Snapshot { .. }), "{broker}");
        // By name, JSON in and out: the same book.
        let text = client
            .call("order_book", &json!({"instrument": sber, "depth": 5}))
            .await
            .expect("call");
        let value: Value = serde_json::from_str(&text).unwrap();
        assert!(value.get("Snapshot").is_some(), "{broker}: {text}");
        let caps = client.call("caps", &Value::Null).await.expect("caps");
        assert!(caps.contains("\"md\"") && caps.contains("\"ud\""), "{caps}");
        client.close(ShutdownMode::Immediate).await;
    }
}

#[tokio::test]
async fn trading_is_refused_until_the_first_reconcile() {
    let (_mock, client) = connect(Broker::Bcs).await;
    let sber = client.instrument("SBER@TQBR", None).await.unwrap();
    let request = PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber,
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(Price::from(1)),
    };
    assert!(matches!(
        client.place(request.clone()).await,
        Err(Error::NotReconciled)
    ));
    // By name the refusal is the same error of the library.
    let args = json!({ "request": request });
    assert!(matches!(
        client.call("place", &args).await,
        Err(ClientError::Broker(Error::NotReconciled))
    ));
    client.reconcile(&[]).await.expect("reconcile");
    let ack = client.place(request.clone()).await.expect("place");
    client.cancel(ack.cid).await.expect("cancel");
    client.close(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_closed_client_says_so() {
    let (_mock, client) = connect(Broker::Bcs).await;
    let sber = client.instrument("SBER", None).await.unwrap();
    assert!(client.close(ShutdownMode::Immediate).await.is_some());
    assert!(client.is_closed());
    // A second close has nothing to stop.
    assert!(client.close(ShutdownMode::Immediate).await.is_none());
    assert!(matches!(
        client.order_book(&sber, 1).await,
        Err(Error::ShuttingDown)
    ));
    assert!(matches!(
        client.call("portfolio", &Value::Null).await,
        Err(ClientError::Closed)
    ));
    assert!(client.caps().await.is_none());
    assert!(client.next_event().await.is_none());
}

#[tokio::test]
async fn bad_input_is_named_before_anything_is_sent() {
    let (_mock, client) = connect(Broker::Bcs).await;
    assert!(matches!(
        client.call("no_such_method", &Value::Null).await,
        Err(ClientError::Arg(m)) if m.contains("no_such_method")
    ));
    assert!(matches!(
        client.call("order_book", &json!({})).await,
        Err(ClientError::Arg(m)) if m.contains("instrument")
    ));
    assert!(matches!(
        client.instrument("", None).await,
        Err(ClientError::Arg(_))
    ));
    let err = Client::connect("moex", "t", &Settings::parse("{}").unwrap(), None, 8)
        .await
        .err()
        .expect("unknown broker");
    assert!(
        matches!(err, ClientError::Broker(Error::Config(_))),
        "{err}"
    );
    client.close(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_limiter_is_shared_only_inside_one_broker() {
    let mock = Mock::start(Broker::Bcs).await;
    let wrong = Limiter::new(Broker::Alor);
    let err = Client::connect("bcs", "t", &mock.settings(), Some(wrong), 8)
        .await
        .err()
        .expect("a limiter of another broker");
    assert!(
        err.to_string().contains("shared only inside one broker"),
        "{err}"
    );
    // The same limiter in two clients is one counter.
    let shared = Limiter::new(Broker::Bcs);
    let a = Client::connect("bcs", "a", &mock.settings(), Some(shared.clone()), 8)
        .await
        .unwrap();
    let b = Client::connect("bcs", "b", &mock.settings(), Some(shared.clone()), 8)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(a.limiter().shared(), b.limiter().shared()));
    a.close(ShutdownMode::Immediate).await;
    b.close(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_event_channel_can_be_taken_once() {
    let (_mock, client) = connect(Broker::Bcs).await;
    let sber = client.instrument("SBER", None).await.unwrap();
    let mut events = client.take_events().await.expect("the channel");
    assert!(client.take_events().await.is_none());
    assert!(client.next_event().await.is_none());
    client
        .subscribe_order_book(&sber, brokapi::core::SubscribeOpts::conflate())
        .await
        .unwrap();
    // Events of the account connector may come first: wait for the order book.
    loop {
        let env = events.recv().await.expect("an event");
        if &*env.source == "t-md" {
            break;
        }
    }
    client.close(ShutdownMode::Immediate).await;
}
