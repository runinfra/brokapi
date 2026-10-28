//! The two connectors of one broker behind one type.

use std::sync::Arc;

use brokapi::core::{
    ExecutionSource, InstrumentSource, MarketDataSource, OrderBookSource, OrderExecutor,
    PortfolioSource, ShutdownMode, ShutdownReport, StopOrderExecutor, TradingParams,
};
use brokapi::runtime::BrokerLimiter;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::error::ClientError;
use crate::{BrokerConfig, Caps};

/// What the market data side of any connector can do.
pub(crate) trait Md: OrderBookSource + MarketDataSource + InstrumentSource {}
impl<T: OrderBookSource + MarketDataSource + InstrumentSource> Md for T {}

/// What the account side of any connector can do.
pub(crate) trait Ud:
    OrderExecutor + ExecutionSource + PortfolioSource + StopOrderExecutor
{
}
impl<T: OrderExecutor + ExecutionSource + PortfolioSource + StopOrderExecutor> Ud for T {}

/// The two connectors of one broker.
pub(crate) enum Conn {
    Bcs {
        md: brokapi::bcs::MarketDataConnector,
        ud: brokapi::bcs::UserDataConnector,
    },
    Tbank {
        md: brokapi::tbank::MarketDataConnector,
        ud: brokapi::tbank::UserDataConnector,
    },
    Finam {
        md: brokapi::finam::MarketDataConnector,
        ud: brokapi::finam::UserDataConnector,
    },
    Alor {
        md: brokapi::alor::MarketDataConnector,
        ud: brokapi::alor::UserDataConnector,
    },
    Bybit {
        md: brokapi::bybit::MarketDataConnector,
        ud: brokapi::bybit::UserDataConnector,
    },
}

impl Conn {
    pub(crate) fn md(&self) -> &dyn Md {
        match self {
            Conn::Bcs { md, .. } => md,
            Conn::Tbank { md, .. } => md,
            Conn::Finam { md, .. } => md,
            Conn::Alor { md, .. } => md,
            Conn::Bybit { md, .. } => md,
        }
    }

    pub(crate) fn ud(&self) -> &dyn Ud {
        match self {
            Conn::Bcs { ud, .. } => ud,
            Conn::Tbank { ud, .. } => ud,
            Conn::Finam { ud, .. } => ud,
            Conn::Alor { ud, .. } => ud,
            Conn::Bybit { ud, .. } => ud,
        }
    }

    /// Tells the account connector the price step of an instrument: a price off the step is then refused before
    /// anything is sent.
    pub(crate) fn learn_grid(&self, params: &TradingParams) {
        match self {
            Conn::Bcs { ud, .. } => ud.price_grid().learn(params),
            Conn::Tbank { ud, .. } => ud.price_grid().learn(params),
            Conn::Finam { ud, .. } => ud.price_grid().learn(params),
            Conn::Alor { ud, .. } => ud.price_grid().learn(params),
            Conn::Bybit { ud, .. } => ud.price_grid().learn(params),
        }
    }

    /// Whether the account connector knows the price step of an instrument.
    pub(crate) fn knows_step(&self, key: &brokapi::core::InstrumentKey) -> bool {
        match self {
            Conn::Bcs { ud, .. } => ud.price_grid().step(key).is_some(),
            Conn::Tbank { ud, .. } => ud.price_grid().step(key).is_some(),
            Conn::Finam { ud, .. } => ud.price_grid().step(key).is_some(),
            Conn::Alor { ud, .. } => ud.price_grid().step(key).is_some(),
            Conn::Bybit { ud, .. } => ud.price_grid().step(key).is_some(),
        }
    }

    /// Any method of the broker's API: see [`crate::raw`].
    pub(crate) async fn raw(
        &self,
        method: &str,
        args: &Value,
        command: bool,
    ) -> Result<Value, ClientError> {
        match self {
            Conn::Bcs { ud, .. } => crate::raw::bcs(ud.raw(), method, args, command).await,
            Conn::Tbank { ud, .. } => crate::raw::tbank(ud.raw(), method, args, command).await,
            Conn::Finam { ud, .. } => crate::raw::finam(ud.raw(), method, args, command).await,
            Conn::Alor { ud, .. } => crate::raw::alor(ud.raw(), method, args, command).await,
            Conn::Bybit { ud, .. } => crate::raw::bybit(ud.raw(), method, args, command).await,
        }
    }

    pub(crate) fn caps(&self) -> Caps {
        match self {
            Conn::Bcs { md, ud } => Caps {
                md: md.caps(),
                ud: ud.caps(),
            },
            Conn::Tbank { md, ud } => Caps {
                md: md.caps(),
                ud: ud.caps(),
            },
            Conn::Finam { md, ud } => Caps {
                md: md.caps(),
                ud: ud.caps(),
            },
            Conn::Alor { md, ud } => Caps {
                md: md.caps(),
                ud: ud.caps(),
            },
            Conn::Bybit { md, ud } => Caps {
                md: md.caps(),
                ud: ud.caps(),
            },
        }
    }

    /// Stops the account side first, then market data, as the Rust examples do. Returns (market data, account).
    pub(crate) async fn shutdown(self, mode: ShutdownMode) -> (ShutdownReport, ShutdownReport) {
        match self {
            Conn::Bcs { md, ud } => {
                let u = ud.shutdown(mode).await;
                (md.shutdown(mode).await, u)
            }
            Conn::Tbank { md, ud } => {
                let u = ud.shutdown(mode).await;
                (md.shutdown(mode).await, u)
            }
            Conn::Finam { md, ud } => {
                let u = ud.shutdown(mode).await;
                (md.shutdown(mode).await, u)
            }
            Conn::Alor { md, ud } => {
                let u = ud.shutdown(mode).await;
                (md.shutdown(mode).await, u)
            }
            Conn::Bybit { md, ud } => {
                let u = ud.shutdown(mode).await;
                (md.shutdown(mode).await, u)
            }
        }
    }
}

/// Starts both connectors of a broker.
pub(crate) async fn start(
    config: BrokerConfig,
    name: &str,
    limiter: Arc<BrokerLimiter>,
    tx: mpsc::Sender<brokapi::core::Envelope>,
) -> Result<Conn, brokapi::core::Error> {
    let md_source = format!("{name}-md");
    let ud_source = format!("{name}-ud");
    match config {
        BrokerConfig::Bcs(cfg) => {
            use brokapi::bcs::{
                MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
            };
            let md = MarketDataConnector::start(
                MarketDataConfig::new(cfg.clone(), md_source),
                limiter.clone(),
                tx.clone(),
            )
            .await?;
            let ud =
                UserDataConnector::start(UserDataConfig::new(cfg, ud_source), limiter, tx).await?;
            Ok(Conn::Bcs { md, ud })
        }
        BrokerConfig::Tbank(cfg) => {
            use brokapi::tbank::{
                MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
            };
            let md = MarketDataConnector::start(
                MarketDataConfig::new(cfg.clone(), md_source),
                limiter.clone(),
                tx.clone(),
            )
            .await?;
            let ud =
                UserDataConnector::start(UserDataConfig::new(cfg, ud_source), limiter, tx).await?;
            Ok(Conn::Tbank { md, ud })
        }
        BrokerConfig::Alor(cfg) => {
            use brokapi::alor::{
                MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
            };
            let md = MarketDataConnector::start(
                MarketDataConfig::new(cfg.clone(), md_source),
                limiter.clone(),
                tx.clone(),
            )
            .await?;
            let ud =
                UserDataConnector::start(UserDataConfig::new(cfg, ud_source), limiter, tx).await?;
            Ok(Conn::Alor { md, ud })
        }
        BrokerConfig::Bybit(cfg) => {
            use brokapi::bybit::{
                MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
            };
            let md = MarketDataConnector::start(
                MarketDataConfig::new(cfg.clone(), md_source),
                limiter.clone(),
                tx.clone(),
            )
            .await?;
            let ud =
                UserDataConnector::start(UserDataConfig::new(cfg, ud_source), limiter, tx).await?;
            Ok(Conn::Bybit { md, ud })
        }
        BrokerConfig::Finam { api, account } => {
            use brokapi::finam::{
                MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
            };
            // One JWT for both connectors.
            let tokens = api.shared_tokens().await?;
            let account = match account {
                Some(account) => account.clone(),
                None => only_finam_account(&tokens.account_ids())?,
            };
            let api = api.clone().with_tokens(tokens);
            let md = MarketDataConnector::start(
                MarketDataConfig::new(api.clone(), md_source),
                limiter.clone(),
                tx.clone(),
            )
            .await?;
            let ud =
                UserDataConnector::start(UserDataConfig::new(api, account, ud_source), limiter, tx)
                    .await?;
            Ok(Conn::Finam { md, ud })
        }
    }
}

/// The account of a Finam secret when the settings name none: the only one it sees, else a configuration error that
/// lists them, so the person can pick one without the web cabinet.
fn only_finam_account(accounts: &[String]) -> Result<String, brokapi::core::Error> {
    match accounts {
        [one] => Ok(one.clone()),
        [] => Err(crate::error::config_error(
            "FINAM_ACCOUNT_ID is not set and the secret lists no account (TokenDetails): \
             pass account_id=... or set the variable",
        )),
        many => Err(crate::error::config_error(format!(
            "FINAM_ACCOUNT_ID is not set and the secret sees {} accounts: {}. Pass account_id=... or set the variable",
            many.len(),
            many.join(", ")
        ))),
    }
}

#[cfg(test)]
mod finam_account_tests {
    use super::only_finam_account;

    #[test]
    fn without_an_account_id_the_only_account_of_the_secret_is_taken_and_several_are_named() {
        assert_eq!(only_finam_account(&["A1".into()]).unwrap(), "A1");
        let e = only_finam_account(&["A1".into(), "A2".into()])
            .unwrap_err()
            .to_string();
        assert!(e.contains("sees 2 accounts: A1, A2"), "{e}");
        assert!(
            only_finam_account(&[])
                .unwrap_err()
                .to_string()
                .contains("lists no account")
        );
    }
}
