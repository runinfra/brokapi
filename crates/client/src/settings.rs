//! Settings of a connection: the arguments of the call first, the environment variables of the
//! Rust library (`BCS_REFRESH_TOKEN`, `TBANK_TOKEN`, ...) for everything the caller did not pass.

use brokapi::alor::protocol::config::{AlorConfig, AlorEnv};
use brokapi::bcs::protocol::config::BcsConfig;
use brokapi::core::Error;
use brokapi::finam::FinamApiConfig;
use brokapi::tbank::protocol::TbankConfig;
use serde_json::{Map, Value};

use crate::error::config_error as config;

/// The settings of one connection: a JSON object, read by [`BrokerConfig::from_settings`](crate::BrokerConfig::from_settings).
pub struct Settings(pub Map<String, Value>);

impl Settings {
    /// Reads the settings from JSON text. Anything but an object is a configuration error.
    pub fn parse(text: &str) -> Result<Self, Error> {
        match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(map)) => Ok(Self(map)),
            _ => Err(config("settings must be a JSON object")),
        }
    }

    /// A text setting: the argument, else the variable, else `None`. Empty text counts as not set.
    fn text(&self, key: &str, env: &str) -> Option<String> {
        let from_arg = self.0.get(key).and_then(Value::as_str).map(str::to_owned);
        from_arg
            .or_else(|| std::env::var(env).ok())
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
    }

    /// A flag: the argument, else the variable (`true`, `1`, `yes`), else `false`.
    fn flag(&self, key: &str, env: &str) -> bool {
        match self.0.get(key).and_then(Value::as_bool) {
            Some(b) => b,
            None => std::env::var(env)
                .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
                .unwrap_or(false),
        }
    }

    /// The endpoint: the argument, else the variable of the broker (BCS has none).
    fn endpoint(&self, env: Option<&str>) -> Option<String> {
        let from_arg = self
            .0
            .get("endpoint")
            .and_then(Value::as_str)
            .map(str::to_owned);
        from_arg
            .or_else(|| env.and_then(|name| std::env::var(name).ok()))
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
    }

    /// [`Self::endpoint`], but an address that comes from the environment must be `https://`. Plain
    /// text is for a local imitation, and that address is passed as the `endpoint` argument.
    fn checked_endpoint(&self, env: &str) -> Result<Option<String>, Error> {
        let from_arg = self.0.get("endpoint").and_then(Value::as_str).is_some();
        let url = self.endpoint(Some(env));
        if let Some(u) = &url
            && !from_arg
            && !u.starts_with("https://")
        {
            return Err(config(format!(
                "{env} must start with https:// (for a local imitation pass endpoint=...)"
            )));
        }
        Ok(url)
    }

    /// The BCS connector configuration: `refresh_token`, `write`, `endpoint`.
    pub fn bcs(&self) -> Result<BcsConfig, Error> {
        let token = self
            .text("refresh_token", "BCS_REFRESH_TOKEN")
            .ok_or_else(|| {
                config("BCS_REFRESH_TOKEN is not set (pass refresh_token=... or set the variable)")
            })?;
        let write = self.flag("write", "BCS_IS_WRITE");
        let cfg = match self.endpoint(None) {
            Some(url) => BcsConfig::for_mock(token, url),
            None => BcsConfig::new(token),
        };
        Ok(cfg.with_write(write))
    }

    /// The ALOR connector configuration: `refresh_token`, `portfolio`, `exchange`, `test`, `endpoint`.
    pub fn alor(&self) -> Result<AlorConfig, Error> {
        let token = self
            .text("refresh_token", "ALOR_REFRESH_TOKEN")
            .ok_or_else(|| {
                config("ALOR_REFRESH_TOKEN is not set (pass refresh_token=... or set the variable)")
            })?;
        let mut cfg = match self.endpoint(None) {
            Some(url) => AlorConfig::for_mock(token, url),
            None => {
                let mut cfg = AlorConfig::new(token);
                if self.flag("test", "ALOR_TEST") {
                    cfg = cfg.with_env(AlorEnv::Test);
                }
                cfg
            }
        };
        if let Some(portfolio) = self.text("portfolio", "ALOR_PORTFOLIO") {
            cfg = cfg.with_portfolio(portfolio);
        }
        if let Some(exchange) = self.text("exchange", "ALOR_EXCHANGE") {
            cfg = cfg.with_exchange(exchange);
        }
        Ok(cfg)
    }

    /// The T-Bank connector configuration: `token`, `sandbox`, `account_id`, `app_name`, `endpoint`.
    pub fn tbank(&self) -> Result<TbankConfig, Error> {
        let token = self
            .text("token", "TBANK_TOKEN")
            .ok_or_else(|| config("TBANK_TOKEN is not set (pass token=... or set the variable)"))?;
        let mut cfg = TbankConfig::new(token, self.flag("sandbox", "TBANK_SANDBOX"));
        if let Some(url) = self.checked_endpoint("TBANK_ENDPOINT")? {
            cfg.endpoint = url;
        }
        cfg.account_id = self.text("account_id", "TBANK_ACCOUNT_ID");
        cfg.app_name = self.text("app_name", "TBANK_APP_NAME");
        Ok(cfg)
    }

    /// The Finam connector configuration and the account id: `secret`, `account_id`, `endpoint`.
    pub fn finam(&self) -> Result<(FinamApiConfig, String), Error> {
        let secret = self.text("secret", "FINAM_SECRET").ok_or_else(|| {
            config("FINAM_SECRET is not set (pass secret=... or set the variable)")
        })?;
        let account = self.text("account_id", "FINAM_ACCOUNT_ID").ok_or_else(|| {
            config("FINAM_ACCOUNT_ID is not set (pass account_id=... or set the variable)")
        })?;
        let mut cfg = FinamApiConfig::new(secret);
        if let Some(url) = self.checked_endpoint("FINAM_API_URL")? {
            cfg = cfg.with_endpoint(url);
        }
        Ok((cfg, account))
    }
}
