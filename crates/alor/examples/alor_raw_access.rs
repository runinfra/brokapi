//! Any ALOR API method through the connector: `connector.raw()`.
//!
//! The library wraps what the shared traits can express. `raw()` reaches everything else with the same
//! refresh token, access token and rate limiter. This program reads the quotations of several instruments and
//! the summary of the portfolio: neither is part of the shared traits. Reads only.
//!
//! By default it runs against the in-process imitation of the broker (`brokapi-mocks`): no network, no
//! account. With `BROKAPI_LIVE=1` it talks to the real broker instead. The refresh token and the portfolio are
//! read by `AlorConfig::from_env()` (variables `ALOR_REFRESH_TOKEN` and `ALOR_PORTFOLIO`, a `.env` file works).
//!
//! ```text
//! cargo run -p brokapi-alor --example alor_raw_access
//! BROKAPI_LIVE=1 cargo run -p brokapi-alor --example alor_raw_access
//! ```

use brokapi_alor::limits::default_limits;
use brokapi_alor::{MarketDataConfig, MarketDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_core::ShutdownMode;
use brokapi_mocks::alor::MockServer;
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;

/// The config for the chosen mode. The mock server is returned too: it must live as long as the connector, so
/// the caller keeps it.
async fn config() -> Result<(AlorConfig, Option<MockServer>), Box<dyn std::error::Error>> {
    if std::env::var("BROKAPI_LIVE").as_deref() == Ok("1") {
        println!("mode: LIVE (real broker, reads only)");
        Ok((AlorConfig::from_env()?, None))
    } else {
        println!("mode: mock (set BROKAPI_LIVE=1 for the real broker)");
        let server = MockServer::start(Default::default()).await;
        let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
        Ok((cfg, Some(server)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (cfg, _mock) = config().await?;
    let portfolio = cfg.portfolio.clone().unwrap_or_default();
    let exchange = cfg.exchange.clone();
    let (tx, _events) = mpsc::channel(64);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg, "alor-md"),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await?;
    let raw = md.raw();

    // The quota is taken by the operation name, then the call is made with the connector's token.
    let quotes = raw
        .call("GetQuotes", |http| async move {
            http.get_quotes(&[("MOEX", "SBER"), ("MOEX", "GAZP")]).await
        })
        .await?;
    for q in quotes {
        println!(
            "{} bid {:?} ask {:?} last {:?}",
            q.symbol, q.bid, q.ask, q.last_price
        );
    }

    // The summary of the portfolio: not part of the shared traits. A real account may refuse it, so a failure
    // is reported, not fatal.
    let summary = raw
        .call("GetSummary", |http| async move {
            http.get_summary(&exchange, &portfolio).await
        })
        .await;
    match summary {
        Ok(s) => println!(
            "evaluation: {:?}, buying power: {:?}",
            s.portfolio_evaluation, s.buying_power
        ),
        Err(e) => println!("summary: not available here ({e})"),
    }

    md.shutdown(ShutdownMode::Immediate).await;
    Ok(())
}
