//! Any T-Bank API method through the connector: `connector.raw()`.
//!
//! The library wraps what the shared traits can express. `raw()` reaches everything else with the same
//! token, endpoint and rate limiter. The example only reads.
//!
//! By default it runs against the in-process mock of the broker (`brokapi_mocks::tbank`): no network,
//! no token, no money.
//!
//! ```text
//! cargo run -p brokapi-tbank --example tbank_raw_access
//! ```
//!
//! With `BROKAPI_LIVE=1` it talks to the real broker instead. The config comes from the environment
//! (a `.env` file is read too). Variables: `TBANK_TOKEN` (required), `TBANK_SANDBOX` (`true` for the
//! sandbox, otherwise production), `TBANK_ENDPOINT`, `TBANK_ACCOUNT_ID` and `TBANK_APP_NAME`
//! (all three optional). Prefer a sandbox or a read-only token.
//!
//! ```text
//! BROKAPI_LIVE=1 cargo run -p brokapi-tbank --example tbank_raw_access
//! ```

use brokapi_core::ShutdownMode;
use brokapi_mocks::tbank::{MockServer, TOKEN};
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::default_limits;
use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
use brokapi_tbank_protocol::TbankConfig;
use brokapi_tbank_protocol::v1 as pb;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let live = std::env::var("BROKAPI_LIVE").is_ok_and(|v| v == "1");

    // The mock server must stay alive for as long as the connector runs.
    let (cfg, _mock) = if live {
        println!("mode: live (config from the environment)");
        (TbankConfig::from_env()?, None)
    } else {
        println!("mode: mock (set BROKAPI_LIVE=1 for the real broker)");
        let server = MockServer::start(Default::default()).await;
        (TbankConfig::for_mock(TOKEN, &server.endpoint), Some(server))
    };

    let limiter = BrokerLimiter::new(&default_limits());
    let (tx, _events) = mpsc::channel(64);
    let md =
        MarketDataConnector::start(MarketDataConfig::new(cfg, "tbank-md"), limiter, tx).await?;
    let raw = md.raw();

    // 1. The quota is taken by the proto method name, then the call is made with the connector's token.
    let accounts = raw
        .call("GetAccounts", |c| async move {
            c.users()
                .get_accounts(pb::GetAccountsRequest { status: None })
                .await
        })
        .await?;
    println!("accounts: {}", accounts.accounts.len());

    // 2. A method no shared trait has: last prices of several instruments at once.
    let prices = raw
        .call("GetLastPrices", |c| async move {
            c.market_data()
                .get_last_prices(pb::GetLastPricesRequest {
                    instrument_id: vec!["SBER_TQBR".into(), "GAZP_TQBR".into()],
                    ..Default::default()
                })
                .await
        })
        .await?;
    for p in prices.last_prices {
        println!("{} {:?}", p.ticker, p.price);
    }

    // 3. The limits of your tariff (a read the mock does not serve, so live only).
    if live {
        let tariff = raw
            .call("GetUserTariff", |c| async move {
                c.users().get_user_tariff(pb::GetUserTariffRequest {}).await
            })
            .await?;
        println!("unary limits: {}", tariff.unary_limits.len());
    }

    // Anything that changes the account goes through `call_command`: an unclear outcome is
    // `Error::Unknown` and is never repeated.
    md.shutdown(ShutdownMode::Immediate).await;
    Ok(())
}
