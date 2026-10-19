//! The Finam mock lives in `brokapi-mocks`; tests reach it through this module, plus connector
//! constructors that tests share.

#![allow(dead_code, unused_imports)]

pub use brokapi_mocks::finam::{MockOptions, MockServer, MockState, SECRET, server_d};

use std::sync::Arc;

use brokapi_core::{Envelope, InstrumentId, InstrumentKey, NativeId};
use brokapi_finam::{
    FinamApiConfig, MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
};
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;

pub fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "MISX"),
        NativeId::SymbolMic {
            symbol: "SBER".into(),
            mic: "MISX".into(),
        },
    )
}

pub fn gazp() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("GAZP", "MISX"),
        NativeId::SymbolMic {
            symbol: "GAZP".into(),
            mic: "MISX".into(),
        },
    )
}

pub fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&brokapi_finam::limits::default_limits())
}

pub fn api(server: &MockServer) -> FinamApiConfig {
    FinamApiConfig::new(SECRET).with_endpoint(server.url.clone())
}

pub async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(api(server), "finam-md"),
        limiter(),
        tx,
    )
    .await
    .expect("md start");
    (md, rx)
}

pub async fn ud_with(
    cfg: UserDataConfig,
    limiter: Arc<BrokerLimiter>,
    cap: usize,
) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(cap);
    let ud = UserDataConnector::start(cfg, limiter, tx)
        .await
        .expect("ud start");
    (ud, rx)
}

pub async fn ud(
    server: &MockServer,
    account: &str,
) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let cfg = UserDataConfig::new(api(server), account, format!("finam-ud-{account}"));
    ud_with(cfg, limiter(), 256).await
}
