//! Events, commands and errors survive serialization unchanged.
//!
//! A process boundary (a gateway service, a recording on disk) must not change what the bot sees:
//! prices keep their exact digits and scale, times keep their nanoseconds, cids stay the same.
//! The one field that is deliberately not serialized is [`Timestamps::rx_mono`]: it is a monotonic
//! clock of the process that received the event and means nothing in another process. After a round
//! trip it is the receiving process's own `Instant::now()`, and `Timestamps` equality ignores it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use brokapi_core::{
    BookLevel, BrokerOrderId, ClientOrderId, ConfigError, Envelope, Error, ErrorClass, Event,
    Execution, InstrumentId, InstrumentKey, Money, NativeId, OrderBook, OrderSnapshot, OrderState,
    OrderType, PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty, ResyncReason, Side,
    StopExpiry, StopOrderKind, SubscriptionId, Timestamps, TransportError, UnixNanos,
};
use serde::{Serialize, de::DeserializeOwned};

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

fn ts() -> Timestamps {
    Timestamps::new(
        UnixNanos::from_nanos(1_760_900_000_123_456_789),
        Instant::now(),
    )
    .with_broker_ts(UnixNanos::from_nanos(1_760_900_000_000_000_001))
}

fn price(s: &str) -> Price {
    s.parse().expect("price")
}

fn qty(s: &str) -> Qty {
    s.parse().expect("qty")
}

/// Serialize, read back, serialize again: both texts must be equal and the value must compare equal.
fn round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: &T) -> T {
    let text = serde_json::to_string(value).expect("serialize");
    let back: T = serde_json::from_str(&text).expect("deserialize");
    assert_eq!(&back, value, "value changed in a round trip: {text}");
    assert_eq!(serde_json::to_string(&back).expect("serialize again"), text);
    back
}

#[test]
fn envelope_with_every_kind_of_money_keeps_digits_times_and_cid() {
    let cid: ClientOrderId = "1c6f7e2a-6b2d-4c51-9a55-6f6a1d8e2b11".parse().unwrap();
    let order = OrderSnapshot {
        cid,
        broker_id: Some(BrokerOrderId::new("98765")),
        instrument: sber(),
        state: OrderState::PartiallyFilled {
            cid,
            filled: qty("3"),
            left: qty("7"),
        },
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: qty("10"),
        filled: qty("3"),
        price: Some(price("244.30")),
        ts: ts(),
    };
    let events = [
        Event::Order(order),
        Event::Execution(Execution {
            cid: Some(cid),
            broker_order_id: BrokerOrderId::new("98765"),
            instrument: sber(),
            side: Side::Sell,
            price: price("0.0001"),
            qty: qty("1"),
            commission: "0.000000001".parse::<Money>().unwrap(),
            trade_id: "t-1".into(),
            ts: ts(),
        }),
        Event::OrderBook(OrderBook::Snapshot {
            instrument: sber(),
            bids: vec![BookLevel {
                price: price("244.30"),
                qty: qty("12"),
            }],
            asks: vec![BookLevel {
                price: price("244.31"),
                qty: qty("5"),
            }],
            ts: ts(),
        }),
        Event::Resync {
            sub: SubscriptionId::new(7),
            instrument: sber(),
            reason: ResyncReason::SubscriptionReset,
        },
        Event::Overflow {
            sub: SubscriptionId::new(7),
            lost_from: UnixNanos::from_nanos(1),
        },
    ];
    for event in events {
        let env = Envelope::new(Arc::from("bcs-ud"), ts(), event).with_sub(SubscriptionId::new(7));
        let back = round_trip(&env);
        assert_eq!(back.ts.broker_ts, env.ts.broker_ts);
        assert_eq!(back.ts.rx_ts, env.ts.rx_ts);
    }

    // The scale of a price is kept: 244.30 stays "244.30", not 244.3.
    let text = serde_json::to_string(&price("244.30")).unwrap();
    assert_eq!(text, "\"244.30\"");
}

#[test]
fn rx_mono_is_not_serialized_and_does_not_count_in_equality() {
    let env = Envelope::new(
        Arc::from("tbank-md"),
        ts(),
        Event::Overflow {
            sub: SubscriptionId::new(1),
            lost_from: UnixNanos::from_nanos(5),
        },
    );
    let text = serde_json::to_string(&env).unwrap();
    assert!(
        !text.contains("rx_mono"),
        "rx_mono must stay local to the process: {text}"
    );
    let back: Envelope = serde_json::from_str(&text).unwrap();
    assert_eq!(back, env);
}

#[test]
fn commands_round_trip() {
    let cid: ClientOrderId = "0b9a3f4e-1111-4c51-9a55-6f6a1d8e2b11".parse().unwrap();
    round_trip(&PlaceOrderRequest {
        cid,
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: qty("1"),
        price: Some(price("101.05")),
    });
    round_trip(&PlaceStopOrderRequest {
        cid,
        instrument: sber(),
        side: Side::Sell,
        kind: StopOrderKind::StopLimit,
        qty: qty("2"),
        stop_price: price("99.50"),
        limit_price: Some(price("99.40")),
        expiry: StopExpiry::GoodTillDate(UnixNanos::from_nanos(1_761_000_000_000_000_000)),
    });
}

#[test]
fn errors_round_trip_with_their_text_and_class() {
    let errors = [
        Error::Config(ConfigError::Message("no token".into())),
        Error::Transport(TransportError::Message("connection reset".into())),
        Error::RateLimited {
            retry_after: Duration::from_millis(1500),
        },
        Error::Rejected {
            code: "price_off_step".into(),
            message: "price 244.305 is not on the step 0.01".into(),
        },
        Error::Unknown,
        Error::Unsupported {
            capability: "stop_order_stream".into(),
        },
        Error::NotReconciled,
        Error::Degraded,
        Error::ShuttingDown,
    ];
    for err in errors {
        let text = serde_json::to_string(&err).expect("serialize error");
        let back: Error = serde_json::from_str(&text).expect("deserialize error");
        assert_eq!(back.to_string(), err.to_string(), "text changed: {text}");
        assert_eq!(back.class(), err.class());
        assert_eq!(serde_json::to_string(&back).unwrap(), text);
    }
    let class: ErrorClass =
        serde_json::from_str(&serde_json::to_string(&ErrorClass::Retryable).unwrap()).unwrap();
    assert_eq!(class, ErrorClass::Retryable);
}
