//! P5 connector glue tests — no network.

use std::sync::Arc;
use std::time::Duration;

use brokapi_bcs::{MarketDataConfig, UserDataConfig};
use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_core::{
    Backpressure, ConfigError, Error, MarketDataCaps, PlaceIdempotency, StreamingCaps, UserDataCaps,
};

#[test]
fn market_data_config_defaults() {
    let cfg = MarketDataConfig::new(
        brokapi_bcs_protocol::config::BcsConfig::new("test-token".into()),
        "bcs-md",
    );
    assert_eq!(cfg.source.as_ref(), "bcs-md");
}

#[test]
fn user_data_config_order_state_buffer() {
    let mut cfg = UserDataConfig::new(
        brokapi_bcs_protocol::config::BcsConfig::new("test-token".into()),
        "bcs-ud",
    );
    assert_eq!(cfg.order_state_buffer, 1024);
    cfg.order_state_buffer = 512;
    assert_eq!(cfg.order_state_buffer, 512);
}

#[test]
fn market_data_caps_match_spec() {
    let caps = MarketDataCaps {
        max_order_book_depth: 20,
        incremental_order_book: false,
        streaming: StreamingCaps {
            order_book: true,
            market_trades: true,
            quotes: true,
            candles: true,
        },
    };
    assert_eq!(caps.max_order_book_depth, 20);
    assert!(caps.streaming.order_book);
}

#[test]
fn user_data_caps_match_spec() {
    let caps = UserDataCaps {
        place_idempotency: PlaceIdempotency::LabelOnly,
        ws_rpc: false,
        ordered_command_channel: false,
        supports_replace: true,
        supports_stop_orders: false,
        streams_executions: false,
        sandbox: false,
    };
    assert!(!caps.streams_executions);
    assert!(!caps.ws_rpc);
    assert!(caps.supports_replace);
}

#[test]
fn map_rate_limit_error() {
    let err = brokapi_bcs::error::map_err(BcsApiError::RateLimitExceeded);
    assert!(matches!(err, Error::RateLimited { .. }));
}

#[test]
fn map_auth_error_to_transport() {
    let err = brokapi_bcs::error::map_err(BcsApiError::AuthError {
        message: "bad token".into(),
    });
    assert!(matches!(err, Error::Transport(_)));
}

#[test]
fn map_config_error() {
    let err = brokapi_bcs::error::map_config_err(BcsApiError::ParseError("bad url".into()));
    assert!(matches!(err, Error::Config(ConfigError::Message(_))));
}

#[test]
fn backpressure_lossless_default_buffer() {
    let bp = Backpressure::Lossless { cap: 1024 };
    assert_eq!(bp, Backpressure::Lossless { cap: 1024 });
}

#[test]
fn retry_after_is_non_zero() {
    if let Error::RateLimited { retry_after } =
        brokapi_bcs::error::map_err(BcsApiError::RateLimitExceeded)
    {
        assert!(retry_after >= Duration::from_secs(1));
    } else {
        panic!("expected RateLimited");
    }
}

#[test]
fn envelope_source_is_arc_str() {
    let source: Arc<str> = Arc::from("bcs-md");
    assert_eq!(source.as_ref(), "bcs-md");
}
