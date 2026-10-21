//! Unary call helper: deadline, one re-auth retry for reads.
//!
//! Internal plumbing of the connectors, public so that tests and tools can use it. Application code
//! calls the broker through the connector traits or [`FinamRaw`](crate::FinamRaw).
//!
//! # Examples
//!
//! The difference between a read and a command, with a call that always answers `UNAUTHENTICATED`
//! the first time:
//!
//! ```
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicU32, Ordering};
//! use std::time::Duration;
//! use brokapi_finam::rpc::unary;
//! use brokapi_finam_protocol::{ApiEndpoint, start_token_manager};
//! # use brokapi_mocks::finam::{MockServer, SECRET};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start().await;
//! # let endpoint = ApiEndpoint::new(server.url.clone());
//! let tokens = start_token_manager(&endpoint, SECRET, "").await?;
//! let calls = Arc::new(AtomicU32::new(0));
//! let call = || {
//!     let calls = Arc::clone(&calls);
//!     async move {
//!         match calls.fetch_add(1, Ordering::SeqCst) {
//!             0 => Err(tonic::Status::unauthenticated("token expired")),
//!             _ => Ok(tonic::Response::new("ok")),
//!         }
//!     }
//! };
//!
//! // A read renews the token and repeats once.
//! let value = unary(&tokens, Duration::from_secs(5), true, call).await?;
//! assert_eq!(value, "ok");
//! assert_eq!(calls.load(Ordering::SeqCst), 2);
//! assert_eq!(tokens.generation(), 2);
//!
//! // A command is sent once, whatever the answer.
//! calls.store(0, Ordering::SeqCst);
//! let err = unary(&tokens, Duration::from_secs(5), false, call).await.unwrap_err();
//! assert!(err.is_unauthenticated());
//! assert_eq!(calls.load(Ordering::SeqCst), 1);
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use brokapi_finam_protocol::{FinamError, TokenManager};
use tonic::Code;

/// Runs `f` with a deadline. On `UNAUTHENTICATED` and `retry_unauth` renews the JWT and calls
/// once more. Never set `retry_unauth` for commands (no automatic retry of trading
/// commands).
///
/// A call that does not answer within `timeout` ends as status `DEADLINE_EXCEEDED`. The request may
/// still be running at the broker, which is why a command with this result is `Unknown`.
///
/// # Errors
///
/// [`FinamError::Status`] with the code of the server, or `DEADLINE_EXCEEDED` on a timeout. When the
/// renewal itself fails, its error is returned instead of the second call.
///
/// # Examples
///
/// See the [module docs](self).
pub async fn unary<T, F, Fut>(
    tokens: &Arc<TokenManager>,
    timeout: Duration,
    retry_unauth: bool,
    f: F,
) -> Result<T, FinamError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match once(timeout, &f).await {
        Err(e) if retry_unauth && e.is_unauthenticated() => {
            tokens.refresh().await?;
            once(timeout, &f).await
        }
        // A command is never repeated, but the next call must not meet the same expired token: the
        // renewal runs in the background.
        Err(e) if e.is_unauthenticated() => {
            tokens.request_refresh();
            Err(e)
        }
        other => other,
    }
}

async fn once<T, F, Fut>(timeout: Duration, f: &F) -> Result<T, FinamError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match tokio::time::timeout(timeout, f()).await {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(status)) => Err(status.into()),
        Err(_) => Err(FinamError::Status {
            code: Code::DeadlineExceeded,
            message: format!("no response within {timeout:?}"),
        }),
    }
}

/// Everything a connector needs to make calls: clients, quota, deadline.
///
/// Cheap to clone. [`FinamRaw`](crate::FinamRaw) wraps it for application code.
#[derive(Clone)]
pub struct Rpc {
    /// Service clients with the shared token manager.
    pub api: brokapi_finam_protocol::FinamApi,
    /// Rate limiter shared by all connectors of one Finam user.
    pub limiter: Arc<brokapi_runtime::BrokerLimiter>,
    /// Deadline of one unary call.
    pub timeout: Duration,
}

impl Rpc {
    /// Takes the quota for the proto method `method`.
    ///
    /// # Errors
    ///
    /// [`brokapi_core::Error::RateLimited`] when a bucket on the route of `method` is empty. Nothing
    /// is consumed in that case and the request must not be sent.
    pub fn acquire(&self, method: &str) -> Result<(), brokapi_core::Error> {
        self.limiter.acquire(method, brokapi_runtime::now())
    }

    /// A read-only call: quota, deadline, one re-auth retry, public error mapping.
    ///
    /// # Errors
    ///
    /// [`brokapi_core::Error::RateLimited`] (local quota or `RESOURCE_EXHAUSTED`),
    /// [`brokapi_core::Error::Rejected`] (the broker said no), [`brokapi_core::Error::Transport`]
    /// (connection failure or timeout). See [`crate::error::map_protocol_err`].
    pub async fn read<T, F, Fut>(&self, method: &str, f: F) -> Result<T, brokapi_core::Error>
    where
        F: Fn(brokapi_finam_protocol::FinamApi) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        self.acquire(method)?;
        unary(self.api.tokens(), self.timeout, true, || {
            f(self.api.clone())
        })
        .await
        .map_err(|e| crate::error::map_protocol_err(&e))
    }

    /// A trading command: quota, deadline, NO retry. The raw protocol error is returned so the
    /// caller can tell "definitely rejected" from "unknown outcome".
    ///
    /// Quota is taken by the caller beforehand (`acquire`) so that a refusal happens before the
    /// command is registered as in flight.
    ///
    /// # Errors
    ///
    /// The raw [`FinamError`]. Map it with [`crate::error::map_command_err`].
    pub async fn send_command<T, F, Fut>(&self, f: F) -> Result<T, FinamError>
    where
        F: Fn(brokapi_finam_protocol::FinamApi) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        unary(self.api.tokens(), self.timeout, false, || {
            f(self.api.clone())
        })
        .await
    }
}
