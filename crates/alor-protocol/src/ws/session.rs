//! The background session of the data channel `/ws`: connect, keep alive, reconnect and restore subscriptions.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use kanal::{AsyncReceiver, AsyncSender};
use tokio::sync::Mutex;
use tokio::time::{self, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::error::AlorApiError;
use crate::http::client::AlorHttpClient;
use crate::ws::models::{WS_PATH, WsIncoming, WsSubscribe, parse_incoming};

/// How often a ping frame is sent to keep the connection alive.
const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// Default pause before the first reconnect attempt (the same as in the other connectors): 500 milliseconds.
pub const DEFAULT_RECONNECT_MIN: Duration = Duration::from_millis(500);
/// Default upper bound of the pause between reconnect attempts: 30 seconds.
pub const DEFAULT_RECONNECT_MAX: Duration = Duration::from_secs(30);
/// A connection that lived this long is healthy: the pause goes back to the minimum.
const WS_HEALTHY_AFTER: Duration = Duration::from_secs(10);
/// The most subscriptions one connection may hold (a limit of the broker).
pub const WS_MAX_SUBSCRIPTIONS: usize = 5000;

/// Options of a [`WsSession`].
#[derive(Debug, Clone, Copy)]
pub struct WsOptions {
    /// Put [`WsIncoming::Reconnected`] into the stream at every connect, before the data of the restored
    /// subscriptions. The connector uses it to see a lost connection.
    pub announce_connects: bool,
    /// Pause before the first reconnect, doubling up to `reconnect_max`.
    pub reconnect_min: Duration,
    /// Upper bound of the pause.
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

/// A request on its way from the caller to the session task.
struct Queued {
    /// The number of a subscribe request in the restore list (it is sent with the list when the connection is
    /// new, so the queued copy is not sent a second time).
    seq: Option<u64>,
    sub: WsSubscribe,
}

/// A WebSocket session of the data channel `/ws`.
///
/// It connects in the background, keeps the connection alive with ping frames, and when the connection drops it
/// reconnects with growing pauses and sends the subscriptions again (each with a fresh access token). Frames
/// arrive parsed on the channel given to [`WsSession::spawn`]. Dropping the session closes it.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_alor_protocol::config::AlorConfig::for_mock("mock-refresh-token", &server.http_base);
/// use brokapi_alor_protocol::http::client::AlorHttpClient;
/// use brokapi_alor_protocol::ws::models::{WsIncoming, WsSubscribe};
/// use brokapi_alor_protocol::ws::session::WsSession;
///
/// let http = AlorHttpClient::new(cfg)?;
/// let (tx, rx) = kanal::bounded_async(64);
/// let session = WsSession::spawn(http, tx);
/// session.subscribe(WsSubscribe::quotes("q1", "MOEX", "SBER", Some("TQBR"))).await?;
/// loop {
///     if let WsIncoming::Data { guid, .. } = rx.recv().await? {
///         assert_eq!(guid, "q1");
///         break;
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct WsSession {
    user_tx: AsyncSender<Queued>,
    subscriptions: Arc<Mutex<Vec<(u64, WsSubscribe)>>>,
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
    /// Starts a session with the default options. It returns at once and connects in the background.
    pub fn spawn(http: AlorHttpClient, event_tx: AsyncSender<WsIncoming>) -> Self {
        Self::spawn_with(http, event_tx, WsOptions::default())
    }

    /// Starts a session with the given options.
    pub fn spawn_with(
        http: AlorHttpClient,
        event_tx: AsyncSender<WsIncoming>,
        options: WsOptions,
    ) -> Self {
        let (user_tx, user_rx) = kanal::bounded_async::<Queued>(128);
        let subscriptions = Arc::new(Mutex::new(Vec::new()));
        let is_connected = Arc::new(AtomicBool::new(false));
        let dropped_events = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            http,
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

    /// How many parsed frames were dropped because the receiving channel was full.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// `true` while the connection is up and the subscriptions are sent.
    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    /// Subscribes. The request is remembered and sent again after every reconnect until [`WsSession::unsubscribe`].
    ///
    /// # Errors
    ///
    /// [`AlorApiError::Subscription`] at 5000 subscriptions or when the session is closed.
    pub async fn subscribe(&self, sub: WsSubscribe) -> Result<(), AlorApiError> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut subs = self.subscriptions.lock().await;
            if subs.len() >= WS_MAX_SUBSCRIPTIONS {
                return Err(AlorApiError::Subscription {
                    message: format!(
                        "subscription limit reached ({WS_MAX_SUBSCRIPTIONS} per connection)"
                    ),
                });
            }
            subs.push((seq, sub.clone()));
        }
        self.user_tx
            .send(Queued {
                seq: Some(seq),
                sub,
            })
            .await
            .map_err(|e| AlorApiError::Subscription {
                message: format!("failed to send the subscription: {e}"),
            })
    }

    /// Cancels the subscription named by `guid`: forgets it and sends `unsubscribe`.
    ///
    /// # Errors
    ///
    /// [`AlorApiError::Subscription`] when the session is closed.
    pub async fn unsubscribe(&self, guid: &str) -> Result<(), AlorApiError> {
        self.subscriptions
            .lock()
            .await
            .retain(|(_, s)| s.guid != guid);
        self.user_tx
            .send(Queued {
                seq: None,
                sub: WsSubscribe::unsubscribe(guid),
            })
            .await
            .map_err(|e| AlorApiError::Subscription {
                message: format!("failed to send the unsubscription: {e}"),
            })
    }
}

async fn run_loop(
    http: AlorHttpClient,
    event_tx: AsyncSender<WsIncoming>,
    options: WsOptions,
    subscriptions: Arc<Mutex<Vec<(u64, WsSubscribe)>>>,
    user_rx: AsyncReceiver<Queued>,
    is_connected: Arc<AtomicBool>,
    dropped_events: Arc<AtomicU64>,
) {
    let url = format!("{}{}", http.ws_base_url(), WS_PATH);
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
        let connected_at = tokio::time::Instant::now();
        let (mut write, mut read) = ws_stream.split();

        // The reader is told first: the data of the restored subscriptions comes after this marker.
        if options.announce_connects && event_tx.send(WsIncoming::Reconnected).await.is_err() {
            break 'session;
        }

        // Restore. The numbers sent here are remembered: the same request still in the queue is not sent twice.
        let mut restored: HashMap<u64, ()> = HashMap::new();
        {
            let token = match http.get_access_token().await {
                Ok(t) => t,
                Err(e) => {
                    error!("WS: no access token: {e}");
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(backoff_max);
                    continue 'session;
                }
            };
            let subs = subscriptions.lock().await;
            for (seq, sub) in subs.iter() {
                if write
                    .send(Message::Text(sub.to_json(&token).into()))
                    .await
                    .is_err()
                {
                    error!("WS: failed to restore a subscription");
                    continue 'session;
                }
                restored.insert(*seq, ());
            }
            if !subs.is_empty() {
                info!("Restored {} subscriptions", subs.len());
            }
        }
        is_connected.store(true, Ordering::Release);

        let mut heartbeat = time::interval(WS_HEARTBEAT_INTERVAL);
        heartbeat.tick().await; // the first tick is at once

        'inner: loop {
            tokio::select! {
                frame = read.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
                        let incoming = parse_incoming(&text);
                        // The broker closes the connection after a 401 and the cached token is no good: get a
                        // new one now, the reconnect needs it.
                        if matches!(incoming, WsIncoming::Status { code: 401, .. }) {
                            warn!("WS: the broker refused the token, renewing it");
                            let _ = http.refresh_access_token().await;
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
                    if write.send(Message::Ping(Vec::new().into())).await.is_err() {
                        break 'inner;
                    }
                    debug!("WS ping sent");
                }
                msg = user_rx.recv() => match msg {
                    Ok(Queued { seq, sub }) => {
                        if seq.is_some_and(|n| restored.remove(&n).is_some()) {
                            continue 'inner;
                        }
                        let token = match http.get_access_token().await {
                            Ok(t) => t,
                            Err(e) => {
                                // The subscribe stays in the list, the reconnect sends it with a token.
                                error!("WS: no access token for a request: {e}");
                                break 'inner;
                            }
                        };
                        if write.send(Message::Text(sub.to_json(&token).into())).await.is_err() {
                            error!("WS write error");
                            break 'inner;
                        }
                    }
                    Err(_) => break 'session,
                },
            }
        }

        is_connected.store(false, Ordering::Release);
        if connected_at.elapsed() >= WS_HEALTHY_AFTER {
            backoff = backoff_min;
        }
        warn!("WS disconnected, reconnecting in {backoff:?}...");
        sleep(backoff).await;
        backoff = (backoff * 2).min(backoff_max);
    }
    is_connected.store(false, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pause_is_the_range_of_the_other_connectors() {
        let o = WsOptions::default();
        assert_eq!(
            (o.reconnect_min, o.reconnect_max),
            (Duration::from_millis(500), Duration::from_secs(30))
        );
        assert!(!o.announce_connects);
    }
}
