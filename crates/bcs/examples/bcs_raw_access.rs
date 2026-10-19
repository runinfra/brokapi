//! Any BCS API method through the connector: `connector.raw()`.
//!
//! The library wraps what the shared traits can express. `raw()` reaches everything else with the same
//! refresh token, access token and rate limiter. This program reads the quotes snapshot and the
//! non-trade operations: neither is part of the shared traits. Reads only.
//!
//! By default it runs against the in-process imitation of the broker (`brokapi-mocks`): no network,
//! no account. With `BROKAPI_LIVE=1` it talks to the real broker instead. The refresh token is read
//! by `BcsConfig::from_env()` (variable `BCS_REFRESH_TOKEN`, a `.env` file works), and the config is
//! forced to read-only.
//!
//! ```text
//! cargo run -p brokapi-bcs --example bcs_raw_access
//! BROKAPI_LIVE=1 cargo run -p brokapi-bcs --example bcs_raw_access
//! ```

use brokapi_bcs::limits::default_limits;
use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::http::models::{OperationHistoryRequest, QuoteInstrument, QuotesRequest};
use brokapi_core::ShutdownMode;
use brokapi_mocks::bcs::MockServer;
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;

/// The config for the chosen mode. The mock server is returned too: it must live as long as the
/// connector, so the caller keeps it.
async fn config() -> Result<(BcsConfig, Option<MockServer>), Box<dyn std::error::Error>> {
    if std::env::var("BROKAPI_LIVE").as_deref() == Ok("1") {
        println!("mode: LIVE (real broker, reads only)");
        Ok((BcsConfig::from_env()?.with_write(false), None))
    } else {
        println!("mode: mock (set BROKAPI_LIVE=1 for the real broker)");
        let server = MockServer::start(Default::default()).await;
        let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
        Ok((cfg, Some(server)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (cfg, _mock) = config().await?;
    let (tx, _events) = mpsc::channel(64);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg, "bcs-md"),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await?;
    let raw = md.raw();

    // The quota is taken by the operation name, then the call is made with the connector's token.
    let quotes = raw
        .call("GetQuotes", |http| async move {
            http.get_quotes(&QuotesRequest {
                instruments: vec![QuoteInstrument::new("SBER", "TQBR")],
            })
            .await
        })
        .await?;
    for q in quotes.records {
        println!(
            "{:?} bid {:?} offer {:?} last {:?}",
            q.ticker, q.bid, q.offer, q.last
        );
    }

    // Non-trade operations (deposits, dividends, commissions): not part of the shared traits.
    // A real account may refuse or limit this method, so a failure is reported, not fatal.
    let history = raw
        .call("GetOperationHistory", |http| async move {
            http.search_operations(&OperationHistoryRequest::default(), Some(10), None)
                .await
        })
        .await;
    match history {
        Ok(h) => println!("operations: {}", h.records.len()),
        Err(e) => println!("operations: not available here ({e})"),
    }

    md.shutdown(ShutdownMode::Immediate).await;
    Ok(())
}
