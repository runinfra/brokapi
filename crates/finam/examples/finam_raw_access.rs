//! Any Finam API method through the connector: `connector.raw()`.
//!
//! The library wraps what the shared traits can express. `raw()` reaches everything else with the same
//! secret, JWT and rate limiter. Reads only, no orders.
//!
//! By default the example talks to an in-process imitation of the broker (`brokapi-mocks`): no
//! account, no network, no money.
//!
//! ```text
//! cargo run -p brokapi-finam --example finam_raw_access
//! ```
//!
//! To use the real broker set `BROKAPI_LIVE=1`. The config then comes from the environment (a `.env`
//! file is read too):
//!
//! - `FINAM_SECRET` (required): the API secret
//! - `FINAM_API_URL` (optional): another endpoint instead of the production one
//!
//! ```text
//! BROKAPI_LIVE=1 cargo run -p brokapi-finam --example finam_raw_access
//! ```

use brokapi_core::ShutdownMode;
use brokapi_finam::limits::default_limits;
use brokapi_finam::{FinamApiConfig, MarketDataConfig, MarketDataConnector, api_config_from_env};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::assets as ps;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pm;
use brokapi_mocks::finam::{MockServer, SECRET};
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let live = std::env::var("BROKAPI_LIVE").is_ok_and(|v| v == "1");

    // The mock server must stay alive while the example runs.
    let (api, _mock): (FinamApiConfig, Option<MockServer>) = if live {
        dotenv::dotenv().ok();
        let (api, _account) = api_config_from_env()?;
        (api, None)
    } else {
        let server = MockServer::start().await;
        let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
        (api, Some(server))
    };
    println!(
        "mode: {}",
        if live {
            "LIVE broker"
        } else {
            "mock (set BROKAPI_LIVE=1 for the real broker)"
        }
    );

    let limiter = BrokerLimiter::new(&default_limits());
    let (tx, _events) = mpsc::channel(64);
    let md =
        MarketDataConnector::start(MarketDataConfig::new(api, "finam-md"), limiter, tx).await?;
    let raw = md.raw();

    // The closure gets a client that attaches the CURRENT JWT; the quota is taken by the proto name.
    let quote = raw
        .call("LastQuote", |api| async move {
            api.market_data()
                .last_quote(pm::QuoteRequest {
                    symbol: "SBER@MISX".into(),
                })
                .await
        })
        .await?;
    println!("{} {:?}", quote.symbol, quote.quote);

    // The imitation does not serve the exchange clock, so this call is for the real broker only.
    if live {
        let clock = raw
            .call("Clock", |api| async move {
                api.assets().clock(ps::ClockRequest {}).await
            })
            .await?;
        println!("exchange time: {:?}", clock.timestamp);
    }

    // Anything that changes the account (PlaceSLTPOrder, CreateAccountReport) goes through
    // `call_command`: no retry, an unclear outcome is `Error::Unknown`.
    md.shutdown(ShutdownMode::Immediate).await;
    Ok(())
}
