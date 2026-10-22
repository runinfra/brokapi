//! Small parts of the core that a user meets: numbers, rounding to the grid of an instrument, the codec of the cid,
//! the length of a timeframe and the words of the states.

use std::time::Duration;

use brokapi_core::{
    CidCodec, ClientOrderId, Decimal, Error, HexUuidCodec, InstrumentId, InstrumentKey, Money,
    NativeId, OrderState, PlaceIdempotency, Price, Qty, RoundMode, ShutdownReport, Timeframe,
    TradingParams, dec,
};

fn params(step: Decimal, lot: i64) -> TradingParams {
    TradingParams {
        instrument: InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        ),
        lot_size: Qty::from(lot),
        min_qty: Qty::from(lot),
        price_step: Price::new(step),
        price_step_value: Money::new(dec!(0.01)),
        currency: "RUB".into(),
    }
}

#[test]
fn a_price_and_a_quantity_are_rounded_to_the_grid_of_the_instrument() {
    let p = params(dec!(0.05), 10);
    assert_eq!(
        p.round_price(Price::new(dec!(100.07)), RoundMode::Down)
            .unwrap()
            .to_string(),
        "100.05"
    );
    assert_eq!(
        p.round_price(Price::new(dec!(100.07)), RoundMode::Up)
            .unwrap()
            .to_string(),
        "100.10"
    );
    assert_eq!(
        p.round_qty(Qty::from(27), RoundMode::Nearest).unwrap(),
        Qty::from(30)
    );
    assert!(p.is_price_on_grid(Price::new(dec!(100.05))));
    assert!(!p.is_price_on_grid(Price::new(dec!(100.07))));
    p.check_price(Price::new(dec!(100.05))).unwrap();
    assert!(matches!(
        p.check_price(Price::new(dec!(100.07))),
        Err(Error::Rejected { ref code, .. }) if code == "price_off_step"
    ));
}

#[test]
fn without_a_usable_step_nothing_is_said_against_a_price_and_rounding_is_an_error() {
    let p = params(dec!(0), 1);
    assert!(p.is_price_on_grid(Price::new(dec!(100.07))));
    assert!(
        p.round_price(Price::new(dec!(100.07)), RoundMode::Down)
            .is_err()
    );
}

#[test]
fn numbers_convert_to_and_from_decimals_and_have_a_sign() {
    let price = Price::from(dec!(-2.5));
    assert!(price.is_negative());
    assert!(!(-price).is_negative());
    assert_eq!(Decimal::from(-price), dec!(2.5));
    assert_eq!(Qty::from(7u64), Qty::from(7));
    assert!(!Qty::from(0).is_negative());
    assert!(Qty::from(0).is_zero());
}

#[test]
fn a_timeframe_is_as_long_as_its_name_says() {
    let got: Vec<_> = [
        Timeframe::M1,
        Timeframe::M5,
        Timeframe::M10,
        Timeframe::M15,
        Timeframe::M30,
        Timeframe::H1,
        Timeframe::H4,
        Timeframe::D1,
        Timeframe::W1,
        Timeframe::MN,
    ]
    .map(Timeframe::seconds)
    .to_vec();
    assert_eq!(
        got,
        [
            60,
            300,
            600,
            900,
            1800,
            3600,
            4 * 3600,
            86_400,
            7 * 86_400,
            30 * 86_400
        ]
    );
}

#[test]
fn the_hex_codec_is_lossless_and_says_so() {
    let caps = HexUuidCodec.caps();
    assert!(caps.lossless);
    assert_eq!(caps.max_len, 32);
    let cid = ClientOrderId::new_v4();
    let native = HexUuidCodec.encode(cid).unwrap();
    assert_eq!(native.as_str().len(), 32);
    assert_eq!(HexUuidCodec.decode(&native), Some(cid));
}

#[test]
fn how_long_a_broker_remembers_a_cid_is_kept_in_seconds_in_json() {
    for idem in [
        PlaceIdempotency::LabelOnly,
        PlaceIdempotency::RejectDuplicate {
            window: Duration::from_secs(90),
        },
        PlaceIdempotency::Idempotent {
            window: Duration::from_secs(3600),
        },
    ] {
        let json = serde_json::to_string(&idem).unwrap();
        assert_eq!(
            serde_json::from_str::<PlaceIdempotency>(&json).unwrap(),
            idem,
            "{json}"
        );
    }
    assert!(
        serde_json::to_string(&PlaceIdempotency::RejectDuplicate {
            window: Duration::from_secs(90)
        })
        .unwrap()
        .contains("90")
    );
}

#[test]
fn an_empty_shutdown_report_has_nothing_in_it_and_every_state_has_a_name() {
    let report = ShutdownReport::empty();
    assert_eq!((report.unsubscribed, report.unsubscribe_failed), (0, 0));
    assert!(report.inflight_abandoned.is_empty());
    let cid = ClientOrderId::new_v4();
    assert_eq!(
        OrderState::InFlight {
            cid,
            sent_at: std::time::Instant::now()
        }
        .name(),
        "InFlight"
    );
}
