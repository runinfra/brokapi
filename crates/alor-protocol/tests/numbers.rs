//! Numbers of the ALOR JSON are read and written as decimals, without an `f64` on the way.

use brokapi_alor_protocol::http::models::{BookLevel, OrderBook, OrderRecord, StopOrderRequest};
use proptest::prelude::*;
use rust_decimal::Decimal;

fn d(s: &str) -> Decimal {
    s.parse().unwrap()
}

#[test]
fn numbers_keep_the_digits_the_broker_wrote() {
    // 244.30 is not 244.3 as text, and 0.0001 is not 1e-4. Neither is an f64.
    let level: BookLevel = serde_json::from_str(r#"{"price":244.30,"volume":157}"#).unwrap();
    assert_eq!(level.price.to_string(), "244.30");
    assert_eq!(
        serde_json::to_string(&level).unwrap(),
        r#"{"price":244.30,"volume":157}"#
    );
}

#[test]
fn a_price_that_f64_cannot_hold_survives() {
    let level: BookLevel =
        serde_json::from_str(r#"{"price":12345678.12345678,"volume":1}"#).unwrap();
    assert_eq!(level.price, d("12345678.12345678"));
    let level: BookLevel =
        serde_json::from_str(r#"{"price":0.1234567890123456789,"volume":1}"#).unwrap();
    assert_eq!(level.price, d("0.1234567890123456789"));
}

#[test]
fn the_book_of_the_rest_answer_and_of_the_stream_read_the_same() {
    let rest: OrderBook =
        serde_json::from_str(r#"{"bids":[],"asks":[],"msTimestamp":5,"existing":false}"#).unwrap();
    let stream: OrderBook = serde_json::from_str(
        r#"{"snapshot":true,"bids":[],"asks":[],"ms_timestamp":5,"timestamp":1}"#,
    )
    .unwrap();
    assert_eq!(rest.ms_timestamp, Some(5));
    assert_eq!(stream.ms_timestamp, Some(5));
}

#[test]
fn an_order_record_is_read_with_the_names_of_the_broker() {
    let r: OrderRecord = serde_json::from_str(
        r#"{"id":"18995978560","symbol":"SBER","board":"TQBR","type":"limit","side":"buy","status":"working",
            "qtyUnits":10,"qty":1,"filledQtyUnits":0,"filled":0,"price":142.52,"comment":"x","transTime":"2020-06-16T23:59:59.9990000Z",
            "someNewField":{"a":1}}"#,
    )
    .unwrap();
    assert_eq!(r.id, "18995978560");
    assert_eq!(r.qty_units, Some(d("10")));
    assert_eq!(r.price.unwrap().to_string(), "142.52");
}

#[test]
fn a_stop_order_goes_out_with_the_names_of_the_broker() {
    let body = StopOrderRequest {
        side: "sell".into(),
        condition: "lessorequal".into(),
        trigger_price: d("350.80"),
        stop_end_unix_time: Some(1_735_678_799),
        price: Some(d("350.00")),
        quantity: 3,
        instrument: brokapi_alor_protocol::http::models::InstrumentRef {
            symbol: "SBER".into(),
            exchange: "MOEX".into(),
            instrument_group: None,
        },
        user: brokapi_alor_protocol::http::models::UserRef {
            portfolio: "D1".into(),
        },
        comment: None,
        time_in_force: None,
        allow_margin: None,
        activate: None,
    };
    let text = serde_json::to_string(&body).unwrap();
    assert!(text.contains(r#""triggerPrice":350.80"#), "{text}");
    assert!(text.contains(r#""stopEndUnixTime":1735678799"#), "{text}");
    assert!(text.contains(r#""price":350.00"#), "{text}");
    assert!(!text.contains("instrumentGroup"), "{text}");
    assert!(!text.contains("comment"), "{text}");
}

proptest! {
    #[test]
    fn any_decimal_price_round_trips(units in 1i64..1_000_000_000_000, scale in 0u32..8) {
        let price = Decimal::new(units, scale);
        let level = BookLevel { price, volume: Decimal::ONE };
        let text = serde_json::to_string(&level).unwrap();
        let back: BookLevel = serde_json::from_str(&text).unwrap();
        prop_assert_eq!(back.price.to_string(), price.to_string());
    }
}
