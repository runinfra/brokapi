//! Numbers of the BCS JSON are read and written as decimals, without an `f64` on the way.

use brokapi_bcs_protocol::http::models::{CreateOrderRequest, OrderBookLevel, OrderExtras};
use brokapi_bcs_protocol::models::enums::OrderSide;
use proptest::prelude::*;
use rust_decimal::Decimal;
use uuid::Uuid;

fn d(s: &str) -> Decimal {
    s.parse().unwrap()
}

#[test]
fn numbers_keep_the_digits_the_broker_wrote() {
    // 244.30 is not 244.3 as text, and 0.0001 is not 1e-4. Neither is an f64.
    let level: OrderBookLevel =
        serde_json::from_str(r#"{"price":244.30,"quantity":0.0001}"#).unwrap();
    assert_eq!(level.price.to_string(), "244.30");
    assert_eq!(level.quantity.to_string(), "0.0001");
    // And they go back the same.
    assert_eq!(
        serde_json::to_string(&level).unwrap(),
        r#"{"price":244.30,"quantity":0.0001}"#
    );
}

#[test]
fn a_price_that_f64_cannot_hold_survives() {
    // 17 significant digits: an f64 would turn this into 0.30000000000000004 or cut it.
    let level: OrderBookLevel =
        serde_json::from_str(r#"{"price":12345678.12345678,"quantity":1}"#).unwrap();
    assert_eq!(level.price, d("12345678.12345678"));
    let level: OrderBookLevel =
        serde_json::from_str(r#"{"price":0.1234567890123456789,"quantity":1}"#).unwrap();
    assert_eq!(level.price, d("0.1234567890123456789"));
}

#[test]
fn an_order_goes_out_with_the_number_as_given() {
    let req =
        CreateOrderRequest::limit(Uuid::nil(), "SBER", "TQBR", OrderSide::Buy, 10, d("244.30"));
    let text = serde_json::to_string(&req).unwrap();
    // A JSON number, not a string, and with the trailing zero.
    assert!(text.contains(r#""price":244.30"#), "{text}");
    let extras = OrderExtras {
        stop_price: Some(d("0.0001")),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_string(&extras).unwrap(),
        r#"{"stopPrice":0.0001}"#
    );
}

proptest! {
    // broker -> type -> broker: the text of a number comes back as it was.
    #[test]
    fn json_number_round_trips(m in -1_000_000_000_000_000_000_i64..1_000_000_000_000_000_000, scale in 0u32..=12) {
        let text = Decimal::new(m, scale).to_string();
        let body = format!(r#"{{"price":{text},"quantity":{text}}}"#);
        let level: OrderBookLevel = serde_json::from_str(&body).unwrap();
        prop_assert_eq!(level.price.to_string(), text.clone());
        prop_assert_eq!(serde_json::to_string(&level).unwrap(), body);
    }
}
