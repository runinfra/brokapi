//! An account of the T-Invest sandbox, for a new sandbox token.
//!
//! A sandbox token sees no account until one is opened with `SandboxService/OpenSandboxAccount`, and a connector
//! refuses to start on a token without an account. [`open_sandbox_account`] opens one and pays roubles into it, with
//! the token of the config, before any connector is started. A config of the production endpoint is refused before
//! anything is sent: the sandbox service lives only on the sandbox endpoint.
//!
//! # Examples
//!
//! ```
//! use brokapi_core::{Decimal, dec};
//! use brokapi_tbank::sandbox::open_sandbox_account;
//! # use brokapi_mocks::tbank::{MockOptions, MockServer, TOKEN};
//! # use brokapi_tbank_protocol::TbankConfig;
//! # use std::sync::Arc;
//! # use std::sync::atomic::Ordering;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let opts = Arc::new(MockOptions::default());
//! # opts.new_sandbox.store(true, Ordering::Relaxed);
//! # let server = MockServer::start(opts).await;
//! # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
//! // In real code: TbankConfig::sandbox("t.your-sandbox-token").
//! let account = open_sandbox_account(&cfg, Some(dec!(100000))).await?;
//! assert!(!account.is_empty());
//! # Ok(())
//! # }
//! ```

use brokapi_core::{Decimal, Error};
use brokapi_tbank_protocol::money::decimal_to_money;
use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::{TbankClient, TbankConfig};

use crate::error::config;
use crate::util::{CommandOutcome, command};

/// Opens an account in the sandbox and pays `money` roubles into it. Returns the account id.
///
/// # Errors
///
/// [`Error::Config`] for a config that is not a sandbox one, before anything is sent. [`Error::Unknown`] when the
/// answer to the opening is lost: the account may be open, look with `GetSandboxAccounts` before opening another one.
/// A lost answer to the payment is [`Error::Unknown`] too: look at the balance before paying again.
pub async fn open_sandbox_account(
    cfg: &TbankConfig,
    money: Option<Decimal>,
) -> Result<String, Error> {
    if !cfg.sandbox {
        return Err(config(
            "a sandbox account is opened with a sandbox token on the sandbox endpoint (sandbox=true), nothing was sent",
        ));
    }
    let amount = match money {
        Some(m) if m <= Decimal::ZERO => {
            return Err(config(format!(
                "the money to pay into the sandbox account must be above zero, got {m}"
            )));
        }
        Some(m) => Some(decimal_to_money("rub", m).map_err(|e| config(e.to_string()))?),
        None => None,
    };
    let client = TbankClient::new(cfg).map_err(|e| config(e.to_string()))?;
    let mut sandbox = client.sandbox();
    let open = sandbox.open_sandbox_account(pb::OpenSandboxAccountRequest {
        name: Some("brokapi".into()),
    });
    let account_id = done(command(cfg.request_timeout, open).await)?.account_id;
    if let Some(amount) = amount {
        let pay = sandbox.sandbox_pay_in(pb::SandboxPayInRequest {
            account_id: account_id.clone(),
            amount: Some(amount),
        });
        done(command(cfg.request_timeout, pay).await)?;
    }
    Ok(account_id)
}

fn done<T>(outcome: CommandOutcome<T>) -> Result<T, Error> {
    match outcome {
        CommandOutcome::Done(reply) => Ok(reply),
        CommandOutcome::Refused(_, error) => Err(error),
        CommandOutcome::Unknown => Err(Error::Unknown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::dec;
    use brokapi_mocks::tbank::{ACCOUNT_A, MockOptions, MockServer, TOKEN};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn a_new_sandbox_token_sees_the_account_it_opened_with_its_money() {
        let opts = Arc::new(MockOptions::default());
        opts.new_sandbox.store(true, Ordering::Relaxed);
        let server = MockServer::start(Arc::clone(&opts)).await;
        let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
        let client = TbankClient::new(&cfg).unwrap();
        assert!(client.list_accounts().await.unwrap().is_empty());

        let account = open_sandbox_account(&cfg, Some(dec!(100000)))
            .await
            .unwrap();
        assert_eq!(account, ACCOUNT_A);
        let ids: Vec<String> = client
            .list_accounts()
            .await
            .unwrap()
            .into_iter()
            .map(|a| a.id)
            .collect();
        assert_eq!(ids, [ACCOUNT_A]);
        assert_eq!(
            *opts.sandbox_accounts.lock().unwrap(),
            [(ACCOUNT_A.to_owned(), dec!(100000))]
        );
    }

    #[tokio::test]
    async fn a_production_config_is_refused_before_anything_is_sent() {
        let mut cfg = TbankConfig::for_mock(TOKEN, "http://127.0.0.1:9");
        cfg.sandbox = false;
        let e = open_sandbox_account(&cfg, None).await.unwrap_err();
        assert!(
            matches!(e, Error::Config(_)) && e.to_string().contains("nothing was sent"),
            "{e}"
        );
        cfg.sandbox = true;
        let e = open_sandbox_account(&cfg, Some(Decimal::ZERO))
            .await
            .unwrap_err();
        assert!(e.to_string().contains("above zero"), "{e}");
    }
}
