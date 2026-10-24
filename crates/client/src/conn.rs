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
            let api = api.clone().with_tokens(api.shared_tokens().await?);
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
