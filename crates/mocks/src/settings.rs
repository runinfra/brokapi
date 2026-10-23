//! The settings that connect a client to an imitation, as the JSON object of `brokapi-client`'s `Settings`.
//!
//! One function per broker, so that a test or a tool does not repeat the endpoint and the credentials each
//! imitation accepts.

use serde_json::{Value, json};

use crate::{alor, bcs, bybit, finam, tbank};

/// Settings for the BCS imitation.
pub fn bcs(server: &bcs::MockServer) -> Value {
    json!({
        "endpoint": server.http_base,
        "refresh_token": "mock-refresh-token",
        "write": true,
    })
}

/// Settings for the T-Bank imitation (sandbox, account A).
pub fn tbank(server: &tbank::MockServer) -> Value {
    json!({
        "endpoint": server.endpoint,
        "token": tbank::TOKEN,
        "sandbox": true,
        "account_id": tbank::ACCOUNT_A,
    })
}

/// Settings for the Finam imitation (account `ACC1`).
pub fn finam(server: &finam::MockServer) -> Value {
    json!({
        "endpoint": server.url,
        "secret": finam::SECRET,
        "account_id": "ACC1",
    })
}

/// Settings for the ALOR imitation.
pub fn alor(server: &alor::MockServer) -> Value {
    json!({
        "endpoint": server.http_base,
        "refresh_token": "mock-refresh-token",
        "portfolio": alor::PORTFOLIO,
    })
}

/// Settings for the Bybit imitation (the demo contour, the default account).
pub fn bybit(server: &bybit::MockServer) -> Value {
    json!({
        "endpoint": server.http_base,
        "api_key": bybit::DEFAULT_KEY,
        "api_secret": bybit::SECRET,
    })
}
