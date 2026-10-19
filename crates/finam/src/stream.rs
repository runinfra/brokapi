//! One supervised gRPC server-stream: open, read, reopen with backoff.
//!
//! A stream task only reads and forwards frames. Parsing into events happens in the pump.
//! Closing a stream (dropping it) is the protocol-level unsubscribe for gRPC (RST_STREAM).
//!
//! What the supervisor does:
//!
//! - **Opens** the stream after taking quota for it (opening counts as a request) and within a deadline.
//! - **Reopens** it after a break or a normal end, with a pause that doubles from 0.5 s to 30 s. A
//!   stream that lived at least 10 s counts as healthy and the pause starts over.
//! - **Asks for a new token** when the broker answers `UNAUTHENTICATED`.
//! - **Treats some refusals as permanent** (`INVALID_ARGUMENT`, `NOT_FOUND`, `PERMISSION_DENIED`,
//!   `UNIMPLEMENTED`, `FAILED_PRECONDITION`): it reports `Failed` once and keeps trying slowly (every 30 s),
//!   because a permission may be granted later.
//! - **Reports** every step as a [`StreamEvent`] through the `wrap` function.
//!
//! # Examples
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Duration;
//! use brokapi_core::SubscriptionId;
//! use brokapi_finam::core::StreamEvent;
//! use brokapi_finam::limits::default_limits;
//! use brokapi_finam::stream::{OpenFn, StreamParams, spawn_stream};
//! use brokapi_runtime::BrokerLimiter;
//! use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pm;
//! use brokapi_finam_protocol::{ApiEndpoint, FinamApi, start_token_manager};
//! use tokio::sync::mpsc;
//! use tokio_util::sync::CancellationToken;
//! # use brokapi_mocks::finam::{MockServer, SECRET};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start().await;
//! # let endpoint = ApiEndpoint::new(server.url.clone());
//! let tokens = start_token_manager(&endpoint, SECRET, "").await?;
//! let api = FinamApi::connect(&endpoint, Arc::clone(&tokens), false)?;
//!
//! // How to open the stream. The supervisor calls it again after every reconnect.
//! let open: OpenFn<pm::SubscribeOrderBookResponse> = Arc::new(move || {
//!     let api = api.clone();
//!     Box::pin(async move {
//!         let request = pm::SubscribeOrderBookRequest { symbol: "SBER@MISX".into() };
//!         api.market_data().subscribe_order_book(request).await.map(|r| r.into_inner())
//!     })
//! });
//!
//! let (tx, mut frames) = mpsc::channel(16);
//! let handle = spawn_stream(
//!     StreamParams {
//!         sub: SubscriptionId::new(1),
//!         method: "SubscribeOrderBook",
//!         limiter: BrokerLimiter::new(&default_limits()),
//!         tokens,
//!         open,
//!         open_timeout: Duration::from_secs(5),
//!         wrap: Arc::new(|event| event), // keep the events as they are
//!         tx,
//!         first: None,
//!     },
//!     CancellationToken::new(),
//! );
//!
//! let first = tokio::time::timeout(Duration::from_secs(5), frames.recv()).await?.unwrap();
//! assert!(matches!(first, StreamEvent::Connected));
//!
//! assert!(handle.stop(Duration::from_secs(2)).await);
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;

use brokapi_core::SubscriptionId;
use brokapi_finam_protocol::{FinamError, TokenManager};
use brokapi_runtime::BrokerLimiter;
use futures_util::future::BoxFuture;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Streaming};
use tracing::{debug, warn};

use crate::core::StreamEvent;

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A stream that lived this long counts as healthy: the next drop restarts the backoff.
const HEALTHY_AFTER: Duration = Duration::from_secs(10);

/// How to open a stream: a function that returns the future of a new stream. Called again after
/// every drop, so it must build a fresh request each time. It reads the token through the
/// [`FinamApi`](brokapi_finam_protocol::FinamApi) it captured.
pub type OpenFn<M> =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Streaming<M>, tonic::Status>> + Send + Sync>;

/// Result of the first open attempt, reported once to `subscribe_*`.
pub type FirstOutcome = oneshot::Sender<Result<(), FinamError>>;

/// Everything [`spawn_stream`] needs. `M` is the message type of the stream, `F` the frame type the
/// connector wants.
pub struct StreamParams<M, F> {
    /// The subscription this stream serves. Only used for logging here.
    pub sub: SubscriptionId,
    /// Limiter key of the open call (`SubscribeOrders`, ...).
    pub method: &'static str,
    /// Rate limiter. A refused open waits for the time the limiter gives and tries again.
    pub limiter: Arc<BrokerLimiter>,
    /// Token manager. The supervisor asks it for a renewal after `UNAUTHENTICATED`.
    pub tokens: Arc<TokenManager>,
    /// How to open the stream.
    pub open: OpenFn<M>,
    /// How long one open attempt may take before it counts as a failure.
    pub open_timeout: Duration,
    /// Turns a [`StreamEvent`] into the connector's frame.
    pub wrap: Arc<dyn Fn(StreamEvent<M>) -> F + Send + Sync>,
    /// Where the frames go. When the receiver is gone, the supervisor ends.
    pub tx: mpsc::Sender<F>,
    /// Receives the outcome of the FIRST open attempt, once. Used by `subscribe_*` to return a refusal.
    pub first: Option<FirstOutcome>,
}

/// Handle of a running stream supervisor: restart it, stop it, or abort it.
///
/// Dropping the handle does not stop the stream. Call [`stop`](Self::stop) or [`abort`](Self::abort),
/// or cancel the token given to [`spawn_stream`].
pub struct StreamHandle {
    cancel: CancellationToken,
    restart: Arc<Notify>,
    join: JoinHandle<()>,
}

impl StreamHandle {
    /// Drop the current stream and open a fresh one right away (book out of sync).
    ///
    /// The pause of the backoff starts over. The new stream is reported as `Connected` again.
    pub fn restart(&self) {
        self.restart.notify_one();
    }

    /// Cancels the stream (protocol-level unsubscribe) and waits for the task, at most `limit`.
    /// `true` when the task ended in time.
    ///
    /// # Examples
    ///
    /// See the [module docs](self).
    pub async fn stop(self, limit: Duration) -> bool {
        self.cancel.cancel();
        tokio::time::timeout(limit, self.join).await.is_ok()
    }

    /// Cancels the stream and kills the task at once, without waiting for it to finish its step.
    pub fn abort(self) {
        self.cancel.cancel();
        self.join.abort();
    }
}

/// Starts the supervisor task. `cancel` should be a child of the connector's root token.
///
/// Must be called inside a Tokio runtime. See the [module docs](self) for a complete example.
pub fn spawn_stream<M, F>(params: StreamParams<M, F>, cancel: CancellationToken) -> StreamHandle
where
    M: Send + 'static,
    F: Send + 'static,
{
    let restart = Arc::new(Notify::new());
    let join = tokio::spawn(run(params, cancel.clone(), Arc::clone(&restart)));
    StreamHandle {
        cancel,
        restart,
        join,
    }
}

fn is_permanent(code: Code) -> bool {
    matches!(
        code,
        Code::InvalidArgument
            | Code::NotFound
            | Code::PermissionDenied
            | Code::Unimplemented
            | Code::FailedPrecondition
    )
}

async fn forward<M, F>(
    p: &StreamParams<M, F>,
    cancel: &CancellationToken,
    ev: StreamEvent<M>,
) -> bool {
    let frame = (p.wrap)(ev);
    tokio::select! {
        _ = cancel.cancelled() => false,
        r = p.tx.send(frame) => r.is_ok(),
    }
}

async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(d) => true,
    }
}

async fn run<M, F>(mut p: StreamParams<M, F>, cancel: CancellationToken, restart: Arc<Notify>)
where
    M: Send + 'static,
    F: Send + 'static,
{
    let mut backoff = BACKOFF_MIN;
    let mut reported_down = false;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        // Opening a stream spends quota like any call.
        if let Err(e) = p.limiter.acquire(p.method, std::time::Instant::now()) {
            let wait = match e {
                brokapi_core::Error::RateLimited { retry_after } => retry_after,
                _ => BACKOFF_MAX,
            };
            if !sleep_or_cancel(wait, &cancel).await {
                return;
            }
            continue;
        }

        let opened = tokio::select! {
            _ = cancel.cancelled() => return,
            r = tokio::time::timeout(p.open_timeout, (p.open)()) => r,
        };
        let status = match opened {
            Ok(Ok(stream)) => {
                reported_down = false;
                if let Some(first) = p.first.take() {
                    let _ = first.send(Ok(()));
                }
                if !forward(&p, &cancel, StreamEvent::Connected).await {
                    return;
                }
                let started = Instant::now();
                match read(&mut p, stream, &cancel, &restart).await {
                    ReadEnd::Cancelled => return,
                    ReadEnd::Restart => {
                        backoff = BACKOFF_MIN;
                        continue;
                    }
                    ReadEnd::Dropped(why, unauth) => {
                        if started.elapsed() >= HEALTHY_AFTER {
                            backoff = BACKOFF_MIN;
                        }
                        if !forward(&p, &cancel, StreamEvent::Disconnected(why.clone())).await {
                            return;
                        }
                        reported_down = true;
                        debug!(sub = ?p.sub, reason = %why, "stream dropped, reopening");
                        // An unauthenticated drop usually means the JWT lapsed.
                        if unauth {
                            p.tokens.request_refresh();
                        }
                        None
                    }
                }
            }
            Ok(Err(status)) => Some(status),
            Err(_) => Some(tonic::Status::deadline_exceeded(format!(
                "stream not opened within {:?}",
                p.open_timeout
            ))),
        };

        if let Some(status) = status {
            let err: FinamError = status.clone().into();
            if status.code() == Code::Unauthenticated {
                p.tokens.request_refresh();
            }
            if is_permanent(status.code()) {
                warn!(sub = ?p.sub, error = %err, "stream refused");
                if let Some(first) = p.first.take() {
                    let _ = first.send(Err(err.clone()));
                }
                if !forward(&p, &cancel, StreamEvent::Failed(err.to_string())).await {
                    return;
                }
                // The refusal may be lifted later (permissions): keep trying, slowly.
                backoff = BACKOFF_MAX;
            } else if !reported_down {
                reported_down = true;
                if !forward(&p, &cancel, StreamEvent::Disconnected(err.to_string())).await {
                    return;
                }
            }
        }

        if !sleep_or_cancel(backoff, &cancel).await {
            return;
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

enum ReadEnd {
    Cancelled,
    Restart,
    Dropped(String, bool),
}

async fn read<M, F>(
    p: &mut StreamParams<M, F>,
    mut stream: Streaming<M>,
    cancel: &CancellationToken,
    restart: &Notify,
) -> ReadEnd
where
    M: Send + 'static,
    F: Send + 'static,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return ReadEnd::Cancelled,
            _ = restart.notified() => return ReadEnd::Restart,
            msg = stream.message() => match msg {
                Ok(Some(m)) => {
                    if !forward(p, cancel, StreamEvent::Msg(m)).await {
                        return ReadEnd::Cancelled;
                    }
                }
                Ok(None) => return ReadEnd::Dropped("stream ended by server".into(), false),
                Err(status) => {
                    let unauth = status.code() == Code::Unauthenticated;
                    let e: FinamError = status.into();
                    return ReadEnd::Dropped(e.to_string(), unauth);
                }
            },
        }
    }
}
