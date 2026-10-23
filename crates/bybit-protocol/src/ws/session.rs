//! A background WebSocket session that reconnects and restores its subscriptions.
//!
//! One session is one connection: public data, or the private streams (logged in with the key). The session sends a
//! heartbeat every 20 seconds as the exchange asks, reconnects when the link breaks or goes silent, logs in again,
//! and sends its subscriptions again. With `announce_connects` it tells the reader about every connection with
//! [`WsIncoming::Connected`] before the restored data, so the reader can tell a reconnect from the first connect.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use kanal::{AsyncReceiver, AsyncSender};
use tokio::sync::Mutex;
use tokio::time::{self, Instant, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use super::models::{WsIncoming, WsRequest, parse_incoming};
use crate::error::BybitApiError;
use crate::http::client::BybitHttpClient;

/// The heartbeat the exchange asks for.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
/// Without any frame (a heartbeat answer counts) for this long the link is taken for dead.
pub const SILENCE_LIMIT: Duration = Duration::from_secs(50);
/// The first pause before a reconnect.
pub const DEFAULT_RECONNECT_MIN: Duration = Duration::from_millis(500);
/// The longest pause before a reconnect.
pub const DEFAULT_RECONNECT_MAX: Duration = Duration::from_secs(30);
const HEALTHY_AFTER: Duration = Duration::from_secs(10);
const LOGIN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the login stays valid after it is sent, in milliseconds.
const LOGIN_EXPIRES_MS: i64 = 10_000;

/// Which stream the session connects to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsStream {
    /// Public market data of the category of the config. No login.
    Public,
    /// The private streams of the account (`order`, `execution`, `position`, `wallet`). Logs in with the key.
    Private,
}

/// How the session behaves.
#[derive(Debug, Clone, Copy)]
pub struct WsOptions {
    /// Tell the reader about every connection with [`WsIncoming::Connected`].
    pub announce_connects: bool,
    /// The first pause before a reconnect.
    pub reconnect_min: Duration,
    /// The longest pause before a reconnect.
    pub reconnect_max: Duration,
}

impl Default for WsOptions {
    fn default() -> Self {
        Self {
            announce_connects: false,
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
        }
    }
}

struct Queued {
    seq: Option<u64>,
    request: WsRequest,
}

/// One WebSocket connection to Bybit, kept alive in a task of its own.
pub struct WsSession {
    user_tx: AsyncSender<Queued>,
    /// Subscriptions to restore after a reconnect: (number, topic, req_id).
    subscriptions: Arc<Mutex<Vec<(u64, String, String)>>>,
    next_seq: AtomicU64,
    is_connected: Arc<AtomicBool>,
    dropped_events: Arc<AtomicU64>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for WsSession {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl WsSession {
    /// Starts the session. It connects at once, in the background; `event_tx` gets every frame.
    pub fn spawn(
        http: BybitHttpClient,
        stream: WsStream,
        event_tx: AsyncSender<WsIncoming>,
        options: WsOptions,
    ) -> Self {
        let (user_tx, user_rx) = kanal::bounded_async::<Queued>(128);
        let subscriptions = Arc::new(Mutex::new(Vec::new()));
        let is_connected = Arc::new(AtomicBool::new(false));
        let dropped_events = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            http,
            stream,
            event_tx,
            options,
            Arc::clone(&subscriptions),
            user_rx,
            Arc::clone(&is_connected),
            Arc::clone(&dropped_events),
        ));
        Self {
            user_tx,
            subscriptions,
            next_seq: AtomicU64::new(0),
            is_connected,
            dropped_events,
            handle,
        }
    }

    /// How many frames were dropped because the reader's channel was full.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Whether the link is up (and logged in, for the private stream).
    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    /// Subscribes to `topic`. The request is sent when the link is up and again after every reconnect. The answer of
    /// the exchange comes as a [`WsIncoming::Ack`] carrying `req_id`.
    ///
    /// # Errors
    ///
    /// [`BybitApiError::Subscription`] when the session is gone.
    pub async fn subscribe(&self, req_id: &str, topic: &str) -> Result<(), BybitApiError> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.subscriptions
            .lock()
            .await
            .push((seq, topic.to_string(), req_id.to_string()));
        self.user_tx
            .send(Queued {
                seq: Some(seq),
                request: WsRequest::subscribe(req_id, topic),
            })
            .await
            .map_err(|e| BybitApiError::Subscription {
                message: format!("failed to send the subscription: {e}"),
            })
    }

    /// Asks the exchange for `topic` again: it is cancelled and subscribed anew under `req_id`, so an order book
    /// topic sends a fresh snapshot. A reconnect restores it under the new `req_id`.
    ///
    /// # Errors
    ///
    /// [`BybitApiError::Subscription`] when the session is gone.
    pub async fn resubscribe(&self, req_id: &str, topic: &str) -> Result<(), BybitApiError> {
        for (_, t, r) in self.subscriptions.lock().await.iter_mut() {
            if t == topic {
                *r = req_id.to_string();
            }
        }
        for request in [
            WsRequest::unsubscribe("", topic),
            WsRequest::subscribe(req_id, topic),
        ] {
            self.user_tx
                .send(Queued { seq: None, request })
                .await
                .map_err(|e| BybitApiError::Subscription {
                    message: format!("failed to send the request: {e}"),
                })?;
        }
        Ok(())
    }

    /// Cancels the subscription to `topic` and forgets it, so a reconnect does not restore it.
    ///
    /// # Errors
    ///
    /// [`BybitApiError::Subscription`] when the session is gone.
    pub async fn unsubscribe(&self, topic: &str) -> Result<(), BybitApiError> {
        self.subscriptions
            .lock()
            .await
            .retain(|(_, t, _)| t != topic);
        self.user_tx
            .send(Queued {
                seq: None,
                request: WsRequest::unsubscribe("", topic),
            })
            .await
            .map_err(|e| BybitApiError::Subscription {
                message: format!("failed to send the unsubscription: {e}"),
            })
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    http: BybitHttpClient,
    stream: WsStream,
    event_tx: AsyncSender<WsIncoming>,
    options: WsOptions,
    subscriptions: Arc<Mutex<Vec<(u64, String, String)>>>,
    user_rx: AsyncReceiver<Queued>,
    is_connected: Arc<AtomicBool>,
    dropped_events: Arc<AtomicU64>,
) {
    let url = match stream {
        WsStream::Public => http.config().ws_public_url(),
        WsStream::Private => http.config().ws_private_url(),
    };
    let backoff_min = options.reconnect_min.max(Duration::from_millis(10));
    let backoff_max = options.reconnect_max.max(backoff_min);
    let mut backoff = backoff_min;

    'session: loop {
        let ws_stream = match connect_async(url.as_str()).await {
            Ok((s, res)) => {
                info!(
                    "WebSocket connected to {url} with status <{}>",
                    res.status()
                );
                s
            }
            Err(e) => {
                error!("WS connect failed: {e}");
                sleep(backoff).await;
                backoff = (backoff * 2).min(backoff_max);
                continue;
            }
        };
        let connected_at = Instant::now();
        let (mut write, mut read) = ws_stream.split();

        if stream == WsStream::Private {
            let cfg = http.config();
            let login = WsRequest::auth(
                &cfg.api_key,
                &cfg.api_secret,
                http.timestamp_ms() + LOGIN_EXPIRES_MS,
            );
            if write
                .send(Message::Text(login.to_json().into()))
                .await
                .is_err()
            {
                error!("WS: failed to send the login");
                sleep(backoff).await;
                backoff = (backoff * 2).min(backoff_max);
                continue 'session;
            }
            match wait_login(&mut read).await {
                Ok(()) => {}
                Err(reason) => {
                    error!("WS: login failed: {reason}");
                    // The reader is told, so a subscription waiting for the stream fails with the reason.
                    let _ = event_tx
                        .send(WsIncoming::Ack {
                            op: "auth".into(),
                            req_id: None,
                            success: false,
                            message: reason,
                        })
                        .await;
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(backoff_max);
                    continue 'session;
                }
            }
        }

        // The reader is told first: the data of the restored subscriptions comes after this marker.
        if options.announce_connects && event_tx.send(WsIncoming::Connected).await.is_err() {
            break 'session;
        }

        // Restore. The numbers sent here are remembered: the same request still in the queue is not sent twice.
        let mut restored: std::collections::HashSet<u64> = std::collections::HashSet::new();
        {
            let subs = subscriptions.lock().await;
            for (seq, topic, req_id) in subs.iter() {
                let text = WsRequest::subscribe(req_id, topic).to_json();
                if write.send(Message::Text(text.into())).await.is_err() {
                    error!("WS: failed to restore a subscription");
                    continue 'session;
                }
                restored.insert(*seq);
            }
            if !subs.is_empty() {
                info!("Restored {} subscriptions", subs.len());
            }
        }
        is_connected.store(true, Ordering::Release);

        let mut heartbeat = time::interval(HEARTBEAT_INTERVAL);
        heartbeat.tick().await; // the first tick is at once
        let mut last_rx = Instant::now();

        'inner: loop {
            tokio::select! {
                frame = read.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
                        last_rx = Instant::now();
                        let incoming = parse_incoming(&text);
                        if incoming == WsIncoming::Pong {
                            continue 'inner;
                        }
                        match event_tx.try_send(incoming) {
                            Ok(true) => {}
                            Ok(false) => {
                                dropped_events.fetch_add(1, Ordering::Relaxed);
                                warn!("WS: the receiving channel is full, a frame is dropped");
                            }
                            Err(_) => break 'session,
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        last_rx = Instant::now();
                        if write.send(Message::Pong(data)).await.is_err() {
                            break 'inner;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("WS closed by the server");
                        break 'inner;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        error!("WS read error: {e}");
                        break 'inner;
                    }
                    None => break 'inner,
                },
                _ = heartbeat.tick() => {
                    if last_rx.elapsed() > SILENCE_LIMIT {
                        warn!("WS: nothing from the exchange for {SILENCE_LIMIT:?}, reconnecting");
                        break 'inner;
                    }
                    if write.send(Message::Text(WsRequest::ping().to_json().into())).await.is_err() {
                        break 'inner;
                    }
                    debug!("WS heartbeat sent");
                }
                msg = user_rx.recv() => match msg {
                    Ok(Queued { seq, request }) => {
                        if seq.is_some_and(|n| restored.remove(&n)) {
                            continue 'inner;
                        }
                        if write.send(Message::Text(request.to_json().into())).await.is_err() {
                            error!("WS write error");
                            break 'inner;
                        }
                    }
                    Err(_) => break 'session,
                },
            }
        }

        is_connected.store(false, Ordering::Release);
        if connected_at.elapsed() >= HEALTHY_AFTER {
            backoff = backoff_min;
        }
        warn!("WS disconnected, reconnecting in {backoff:?}...");
        sleep(backoff).await;
        backoff = (backoff * 2).min(backoff_max);
    }
    is_connected.store(false, Ordering::Release);
}

/// Reads frames until the answer to the login. Returns the reason when the exchange refused it or the link failed.
async fn wait_login<S>(read: &mut S) -> Result<(), String>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let deadline = Instant::now() + LOGIN_TIMEOUT;
    loop {
        let next = time::timeout_at(deadline, read.next())
            .await
            .map_err(|_| "no answer to the login".to_string())?;
        match next {
            Some(Ok(Message::Text(text))) => {
                if let WsIncoming::Ack {
                    op,
                    success,
                    message,
                    ..
                } = parse_incoming(&text)
                    && op == "auth"
                {
                    return if success {
                        Ok(())
                    } else {
                        Err(if message.is_empty() {
                            "the exchange refused the login".to_string()
                        } else {
                            message
                        })
                    };
                }
            }
            Some(Ok(Message::Close(_))) | None => {
                return Err("the link closed during the login".into());
            }
            Some(Err(e)) => return Err(format!("the link failed during the login: {e}")),
            Some(Ok(_)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pauses_are_the_range_of_the_other_connectors() {
        let o = WsOptions::default();
        assert_eq!(
            (o.reconnect_min, o.reconnect_max),
            (Duration::from_millis(500), Duration::from_secs(30))
        );
        assert!(!o.announce_connects);
    }

    #[test]
    fn the_silence_limit_allows_two_missed_heartbeats() {
        assert!(SILENCE_LIMIT > HEARTBEAT_INTERVAL * 2);
    }
}
