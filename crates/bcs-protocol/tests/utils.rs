//! The small helpers of the protocol crate: the date formats of the broker and the rate limiter.

use std::time::Duration;

use brokapi_bcs_protocol::utils::date_format;
use brokapi_bcs_protocol::utils::rate_limiter::RateLimiter;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Plain {
    #[serde(with = "date_format")]
    at: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Moex {
    #[serde(with = "date_format::moex")]
    at: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Maybe {
    #[serde(default, with = "date_format::option")]
    at: Option<DateTime<Utc>>,
}

fn t() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 19, 7, 5, 9).unwrap()
}

#[test]
fn a_date_goes_out_with_milliseconds_and_comes_back_from_either_form() {
    let text = serde_json::to_string(&Plain { at: t() }).unwrap();
    assert_eq!(text, r#"{"at":"2026-10-19T07:05:09.000Z"}"#);
    assert_eq!(serde_json::from_str::<Plain>(&text).unwrap().at, t());
    // RFC 3339 with an offset is read too.
    let shifted: Plain = serde_json::from_str(r#"{"at":"2026-10-19T10:05:09+03:00"}"#).unwrap();
    assert_eq!(shifted.at, t());
    assert!(serde_json::from_str::<Plain>(r#"{"at":"yesterday"}"#).is_err());

    let text = serde_json::to_string(&Moex { at: t() }).unwrap();
    assert_eq!(text, r#"{"at":"2026-10-19T07:05:09.000Z"}"#);
    assert_eq!(serde_json::from_str::<Moex>(&text).unwrap().at, t());
    assert_eq!(
        serde_json::from_str::<Moex>(r#"{"at":"2026-10-19T10:05:09+03:00"}"#)
            .unwrap()
            .at,
        t()
    );
    assert!(serde_json::from_str::<Moex>(r#"{"at":"x"}"#).is_err());
}

#[test]
fn an_optional_date_treats_empty_and_the_epoch_as_absent() {
    assert_eq!(
        serde_json::to_string(&Maybe { at: Some(t()) }).unwrap(),
        r#"{"at":"2026-10-19T07:05:09.000Z"}"#
    );
    assert_eq!(
        serde_json::to_string(&Maybe { at: None }).unwrap(),
        r#"{"at":null}"#
    );
    for absent in [
        r#"{"at":null}"#,
        r#"{"at":""}"#,
        r#"{"at":"1970-01-01T00:00:00.000Z"}"#,
        "{}",
    ] {
        assert_eq!(
            serde_json::from_str::<Maybe>(absent).unwrap().at,
            None,
            "{absent}"
        );
    }
    assert_eq!(
        serde_json::from_str::<Maybe>(r#"{"at":"2026-10-19T07:05:09.000Z"}"#)
            .unwrap()
            .at,
        Some(t())
    );
    assert_eq!(
        serde_json::from_str::<Maybe>(r#"{"at":"2026-10-19T10:05:09+03:00"}"#)
            .unwrap()
            .at,
        Some(t())
    );
    assert!(serde_json::from_str::<Maybe>(r#"{"at":"nonsense"}"#).is_err());
}

#[test]
fn the_wall_clock_is_after_2026() {
    assert!(date_format::now_millis() > 1_790_000_000_000);
}

#[tokio::test(start_paused = true)]
async fn the_limiter_lets_a_second_of_calls_through_and_then_waits() {
    let limiter = RateLimiter::new(4);
    let start = tokio::time::Instant::now();
    for _ in 0..4 {
        limiter.acquire().await;
    }
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "the first second's worth is free"
    );
    // The fifth waits a quarter of a second, the sixth another quarter.
    limiter.acquire().await;
    assert!(
        start.elapsed() >= Duration::from_millis(250)
            && start.elapsed() < Duration::from_millis(300)
    );
    limiter.acquire().await;
    assert!(start.elapsed() >= Duration::from_millis(500));
    // After a long quiet the bucket is full again, not fuller.
    tokio::time::advance(Duration::from_secs(60)).await;
    let before = tokio::time::Instant::now();
    for _ in 0..4 {
        limiter.acquire().await;
    }
    assert_eq!(before.elapsed(), Duration::ZERO);
    // A clone shares the bucket.
    let other = limiter.clone();
    other.acquire().await;
    assert!(before.elapsed() >= Duration::from_millis(250));
}
