//! The background WebSocket session: connect, heartbeat, reconnect and restore subscriptions.

use crate::error::BcsApiError;
use crate::http::client::BcsHttpClient;
use crate::models::messages::IncomingMessage;
use crate::utils::date_format::now_millis;
use crate::ws::client::BcsWsChannel;
use crate::ws::models::MarketDataSubscribe;
use crate::ws::models::WsOutgoing;
use crate::ws::parser::parse_incoming;
use futures_util::{SinkExt, StreamExt};
use kanal::{AsyncReceiver, AsyncSender};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{self, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// Default pause before the first reconnect attempt: 500 milliseconds (the same as in the T-Bank and
/// Finam connectors).
pub const DEFAULT_RECONNECT_MIN: Duration = Duration::from_millis(500);
/// Default upper bound of the pause between reconnect attempts: 30 seconds.
pub const DEFAULT_RECONNECT_MAX: Duration = Duration::from_secs(30);
/// A connection that lived this long is healthy: the pause goes back to the minimum.
const WS_HEALTHY_AFTER: Duration = Duration::from_secs(10);
/// How long to wait for a pong or ping before the link is dropped (twice the heartbeat interval).
const WS_PONG_TIMEOUT: Duration = Duration::from_secs(60);
/// The most subscribe commands one session keeps.
const WS_MAX_SUBSCRIPTIONS: usize = 100;

/// A command on its way from the caller to the session task.
///
/// `seq` is set for a subscribe command: it is the number the command has in the restore list. When the
/// session connects, it sends the whole list, and a queued subscribe whose number it has just sent is
/// dropped, so the broker gets every subscribe once.
struct Queued {
    seq: Option<u64>,
    outgoing: WsOutgoing,
}

/// Options of a WebSocket session.
///
/// The default is no announcement, a first pause of 500 ms and an upper bound of 30 s.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_bcs_protocol::ws::session::WsOptions;
///
/// let options = WsOptions { announce_connects: true, ..WsOptions::default() };
/// assert_eq!(options.reconnect_min, Duration::from_millis(500));
/// assert_eq!(options.reconnect_max, Duration::from_secs(30));
/// ```
#[derive(Debug, Clone, Copy)]
pub struct WsOptions {
    /// Send [`IncomingMessage::Reconnected`] to the reader at every connect, before the subscriptions
    /// are restored (also at the first connect).
    ///
    /// That is how the reader learns about a lost link in the same stream as the data. Count the
    /// markers: from the second on, the connection was lost, and what came before is not continued by
    /// what follows. For order events this means: ask for the state of your open orders again.
    pub announce_connects: bool,
    /// Pause before the first reconnect attempt. It doubles after each failed attempt up to
    /// `reconnect_max`. Values below 10 ms are raised to 10 ms.
    pub reconnect_min: Duration,
    /// Upper bound of the pause. The pause returns to `reconnect_min` once a connection has lived
    /// 10 seconds. A value below `reconnect_min` is raised to it.
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

/// A background WebSocket session for one channel. It reconnects by itself.
///
/// What the session does for you:
///
/// * It connects with a bearer token and takes a fresh token at every reconnect. If the broker refuses
///   the connection with 403 or "Unauthorized", it forces a token refresh.
/// * It sends a ping every 30 seconds and answers the broker's pings. If no pong or ping arrives for
///   60 seconds it drops the link and reconnects.
/// * After a lost link it waits (500 ms, doubling up to 30 s, see [`WsOptions`]) and connects again.
///   Market data subscriptions are sent again after each connect.
/// * It parses every text frame and pushes the [`IncomingMessage`] with `try_send` into your channel.
///   **If your channel is full, the frame is dropped** and counted ([`WsSession::dropped_events`]):
///   the session never waits for a slow reader. Size the channel for bursts and read it quickly.
/// * It stops when your channel is closed (the receiver dropped) or when the session value is dropped.
///
/// There is **no explicit close method**. Dropping the `WsSession` stops its background task at once,
/// also when the session is waiting for the next reconnect or has never been able to connect.
///
/// The session never reports a lost link as an error value. Use [`WsSession::is_connected`] and, for
/// an ordered signal, [`WsOptions::announce_connects`].
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// use std::time::Duration;
/// use brokapi_bcs_protocol::http::client::BcsHttpClient;
/// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
/// use brokapi_bcs_protocol::models::messages::IncomingMessage;
/// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
/// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
/// use brokapi_bcs_protocol::ws::session::{WsOptions, WsSession};
///
/// let http = BcsHttpClient::new(cfg)?;
/// let (tx, rx) = kanal::bounded_async::<IncomingMessage>(64);
/// let options = WsOptions { announce_connects: true, ..WsOptions::default() };
/// let session = WsSession::spawn_with(BcsWsChannel::MarketData, http, tx, options);
///
/// let sub = MarketDataSubscribe::order_book(vec![InstrumentKey::new("SBER", "TQBR")], 5);
/// session.subscribe(sub).await?;
///
/// // First the connect marker, then the answers of the broker.
/// let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await??;
/// assert!(matches!(first, IncomingMessage::Reconnected));
/// assert_eq!(session.dropped_events(), 0);
/// # Ok(())
/// # }
/// ```
pub struct WsSession {
    /// The channel of this session.
    channel: BcsWsChannel,
    /// Queue of outgoing commands.
    user_tx: AsyncSender<Queued>,
    /// Subscriptions of this session with their numbers, sent again after each reconnect.
    subscriptions: Arc<RwLock<Vec<(u64, WsOutgoing)>>>,
    /// The number of the next subscribe command.
    next_seq: AtomicU64,
    /// Set while the link is up.
    is_connected: Arc<AtomicBool>,
    /// Frames dropped because the downstream channel was full.
    dropped_events: Arc<AtomicU64>,
    /// Handle of the background task.
    _handle: tokio::task::JoinHandle<()>,
}

impl Drop for WsSession {
    /// Stops the background task, wherever it is: connected, waiting for a reconnect or never connected.
    fn drop(&mut self) {
        self._handle.abort();
    }
}

impl WsSession {
    /// Starts a session with the default [`WsOptions`]. It returns at once, the connection is made in
    /// the background.
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime, because the session is a Tokio task.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
    /// use brokapi_bcs_protocol::ws::session::WsSession;
    ///
    /// let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let session = WsSession::spawn(BcsWsChannel::Limits, BcsHttpClient::new(cfg)?, tx);
    /// assert_eq!(session.channel(), BcsWsChannel::Limits);
    /// # Ok(())
    /// # }
    /// ```
    pub fn spawn(
        channel: BcsWsChannel,
        http_client: BcsHttpClient,
        event_tx: AsyncSender<IncomingMessage>,
    ) -> Self {
        Self::spawn_with(channel, http_client, event_tx, WsOptions::default())
    }

    /// Starts a session with the given options. Otherwise the same as [`WsSession::spawn`].
    ///
    /// # Panics
    ///
    /// Panics if there is no Tokio runtime.
    ///
    /// # Examples
    ///
    /// See the example on [`WsSession`].
    pub fn spawn_with(
        channel: BcsWsChannel,
        http_client: BcsHttpClient,
        event_tx: AsyncSender<IncomingMessage>,
        options: WsOptions,
    ) -> Self {
        let (user_tx, user_rx) = kanal::bounded_async::<Queued>(128);
        let user_rx = Arc::new(user_rx);
        let subscriptions: Arc<RwLock<Vec<(u64, WsOutgoing)>>> = Arc::new(RwLock::new(Vec::new()));

        let is_connected = Arc::new(AtomicBool::new(false));
        let dropped_events = Arc::new(AtomicU64::new(0));

        let _handle = tokio::spawn(Self::run_loop(
            channel,
            http_client,
            event_tx,
            options,
            subscriptions.clone(),
            user_rx,
            is_connected.clone(),
            dropped_events.clone(),
        ));

        Self {
            channel,
            user_tx,
            subscriptions,
            next_seq: AtomicU64::new(0),
            is_connected,
            dropped_events,
            _handle,
        }
    }

    /// The number of parsed frames dropped because your event channel was full.
    ///
    /// A growing number means the reader is too slow or the channel is too small. Frames that were
    /// dropped are gone. For order events that means states you must re-read over REST.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let session = ws.connect_limits(tx);
    /// assert_eq!(session.dropped_events(), 0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// `true` while the link is up and the subscriptions have been sent again.
    ///
    /// It is `false` before the first connect, during a reconnect and after the session has ended.
    /// Do not use it to decide that data is flowing, it only says the socket is open.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use std::time::Duration;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsClient;
    ///
    /// let ws = BcsWsClient::new(cfg)?;
    /// let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let session = ws.connect_limits(tx);
    /// tokio::time::timeout(Duration::from_secs(5), async {
    ///     while !session.is_connected() {
    ///         tokio::time::sleep(Duration::from_millis(10)).await;
    ///     }
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    /// The channel this session is on.
    pub fn channel(&self) -> BcsWsChannel {
        self.channel
    }

    /// Subscribes on the market data channel. The command is kept and sent again after each reconnect.
    ///
    /// The command is put in the restore list first and then queued for sending. Something to know:
    ///
    /// * The session counts **commands**: at most 100 per session. The broker's limit (broker docs) is
    ///   100 **instruments** per connection, so a command with many instruments can pass the local
    ///   check and still be refused by the broker (as an error frame, not as a return value).
    /// * If you subscribe before the first connect, the command waits in the restore list and goes out
    ///   once, when the session connects. The same holds for a command given while the session is
    ///   connecting.
    ///
    /// The answer of the broker (subscription confirmation, data, or an error frame) arrives in your
    /// event channel, not as the return value.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::SubscriptionError`] if the session is not on [`BcsWsChannel::MarketData`], if it
    /// already holds 100 subscribe commands, or if the session task has ended.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::error::BcsApiError;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    /// use brokapi_bcs_protocol::ws::session::WsSession;
    ///
    /// let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let sber = vec![InstrumentKey::new("SBER", "TQBR")];
    ///
    /// // A portfolio session takes no subscriptions.
    /// let portfolio = WsSession::spawn(BcsWsChannel::Portfolio, BcsHttpClient::new(cfg.clone())?, tx.clone());
    /// let err = portfolio.subscribe(MarketDataSubscribe::quotes(sber.clone())).await.unwrap_err();
    /// assert!(matches!(err, BcsApiError::SubscriptionError { .. }));
    ///
    /// // A market data session does.
    /// let md = WsSession::spawn(BcsWsChannel::MarketData, BcsHttpClient::new(cfg)?, tx);
    /// md.subscribe(MarketDataSubscribe::quotes(sber)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe(&self, sub: MarketDataSubscribe) -> Result<(), BcsApiError> {
        if self.channel != BcsWsChannel::MarketData {
            return Err(BcsApiError::SubscriptionError {
                message: format!(
                    "WS {:?} ignoring subscribe message for non-MarketData channel",
                    self.channel
                ),
            });
        }
        let mut subs = self.subscriptions.write().await;
        if subs.len() >= WS_MAX_SUBSCRIPTIONS {
            return Err(BcsApiError::SubscriptionError {
                message: format!(
                    "Subscription limit reached ({} max per session)",
                    WS_MAX_SUBSCRIPTIONS
                ),
            });
        }

        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        subs.push((seq, WsOutgoing::Subscribe(sub.clone())));
        drop(subs);

        self.user_tx
            .send(Queued {
                seq: Some(seq),
                outgoing: WsOutgoing::Subscribe(sub),
            })
            .await
            .map_err(|e| BcsApiError::SubscriptionError {
                message: format!("Failed to send subscription: {}", e),
            })?;

        Ok(())
    }

    /// Unsubscribes on the market data channel. Pass the same [`MarketDataSubscribe`] you subscribed
    /// with: the session turns it into the unsubscribe command itself.
    ///
    /// The matching entry is removed from the restore list only if it is **equal** to the one you pass
    /// (same instruments in the same order, same depth, same time frame). A different grouping of the
    /// same instruments is sent to the broker as an unsubscribe, but the original stays in the restore
    /// list and comes back after a reconnect. The broker docs describe no answer to an unsubscribe.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::SubscriptionError`] if the session is not on [`BcsWsChannel::MarketData`] or if
    /// the session task has ended.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    /// use brokapi_bcs_protocol::models::messages::IncomingMessage;
    /// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
    /// use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
    /// use brokapi_bcs_protocol::ws::session::WsSession;
    ///
    /// let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(16);
    /// let md = WsSession::spawn(BcsWsChannel::MarketData, BcsHttpClient::new(cfg)?, tx);
    ///
    /// let sub = MarketDataSubscribe::order_book(vec![InstrumentKey::new("SBER", "TQBR")], 5);
    /// md.subscribe(sub.clone()).await?;
    /// md.unsubscribe(sub).await?; // the same value as at subscribe
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unsubscribe(&self, sub: MarketDataSubscribe) -> Result<(), BcsApiError> {
        if self.channel != BcsWsChannel::MarketData {
            return Err(BcsApiError::SubscriptionError {
                message: format!(
                    "WS {:?} ignoring unsubscribe message for non-MarketData channel",
                    self.channel
                ),
            });
        }

        {
            let mut subs = self.subscriptions.write().await;
            subs.retain(|(_, s)| s != &WsOutgoing::Subscribe(sub.clone()));
        }

        self.user_tx
            .send(Queued {
                seq: None,
                outgoing: WsOutgoing::Unsubscribe(sub.into_unsubscribe()),
            })
            .await
            .map_err(|e| BcsApiError::SubscriptionError {
                message: format!("Failed to send unsubscription: {}", e),
            })
    }

    /// The main loop: connect, read and write, reconnect.
    #[allow(clippy::too_many_arguments)]
    async fn run_loop(
        channel: BcsWsChannel,
        http_client: BcsHttpClient,
        event_tx: AsyncSender<IncomingMessage>,
        options: WsOptions,
        subscriptions: Arc<RwLock<Vec<(u64, WsOutgoing)>>>,
        user_rx: Arc<AsyncReceiver<Queued>>,
        is_connected: Arc<AtomicBool>,
        dropped_events: Arc<AtomicU64>,
    ) {
        let ws_base = http_client.ws_base_url().to_string();
        let backoff_min = options.reconnect_min.max(Duration::from_millis(10));
        let backoff_max = options.reconnect_max.max(backoff_min);
        let mut backoff = backoff_min;

        'session: loop {
            let url = format!("{}{}", ws_base, channel.path());
            // 1. Свежий токен при каждом reconnect
            let token = match http_client.get_access_token().await {
                Ok(t) => t,
                Err(e) => {
                    error!("Failed to get token for {:?}: {}", channel, e);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(backoff_max);
                    continue;
                }
            };

            // 2. Подключение с обработкой 403
            let ws_stream = match Self::connect(&url, &token).await {
                Ok(s) => s,
                Err(e) => {
                    // Если 403 - принудительно обновляем токен и увеличиваем backoff
                    if e.to_string().contains("403 Forbidden")
                        || e.to_string().contains("Unauthorized")
                    {
                        error!("WS {:?} 403 Forbidden - forcing token refresh", channel);
                        let _ = http_client.refresh_access_token().await;
                    }

                    error!("WS {:?} connect failed: {}", channel, e);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(backoff_max);
                    continue;
                }
            };

            info!("WS {:?} session established", channel);
            let connected_at = tokio::time::Instant::now();
            let (mut write, mut read) = ws_stream.split();

            // 3. Pong watchdog state
            let mut last_pong: u64 = now_millis();

            // Reader is told first: the data of the restored subscriptions comes after this marker.
            if options.announce_connects
                && event_tx.send(IncomingMessage::Reconnected).await.is_err()
            {
                info!("Event channel closed for {:?}", channel);
                break 'session;
            }

            // 4. Восстановить подписки. Номера отправленных запоминаем: такая же команда из очереди уже
            // ушла, второй раз ее не шлем.
            let mut restored: HashSet<u64> = HashSet::new();
            {
                let subs = subscriptions.read().await;
                for (seq, outgoing) in subs.iter() {
                    match serde_json::to_string(outgoing) {
                        Ok(json) => {
                            if write.send(Message::Text(json.into())).await.is_err() {
                                error!("Failed to restore subscription for {:?}", channel);
                                continue 'session;
                            }
                            restored.insert(*seq);
                        }
                        Err(e) => {
                            error!("Failed to serialize subscription for {:?}: {}", channel, e);
                        }
                    }
                }
                if !subs.is_empty() {
                    info!("Restored {} subscriptions for {:?}", subs.len(), channel);
                }
            }

            is_connected.store(true, Ordering::Release); // ← UP: подписки восстановлены, готов к работе

            // 5. Единый event loop: read + write + heartbeat в одном select
            let mut heartbeat = time::interval(WS_HEARTBEAT_INTERVAL);
            heartbeat.tick().await; // пропустить немедленный первый тик

            'inner_session: loop {
                tokio::select! {
                    // Входящие сообщения из WS
                    frame = read.next() => {
                        match frame {
                            Some(Ok(Message::Text(text))) => {
                                // RAW лог для OrderTransaction
                                if channel == BcsWsChannel::OrderTransaction {
                                    info!("[WS_SESSION] OrderTransaction received: {}", text);
                                }

                                let incoming = parse_incoming(&text, channel);
                                match event_tx.try_send(incoming) {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        dropped_events.fetch_add(1, Ordering::Relaxed);
                                        warn!(
                                            "WS {:?} downstream full — dropping parsed frame",
                                            channel
                                        );
                                    }
                                    Err(_) => {
                                        info!("Event channel closed for {:?}", channel);
                                        break 'session;
                                    }
                                }
                            }
                            Some(Ok(Message::Close(_))) => {
                                info!("WS {:?} closed by server", channel);
                                break 'inner_session;
                            }
                            Some(Ok(Message::Pong(_))) => {
                                last_pong = now_millis();
                                info!("WS {:?} received pong", channel);
                            }
                            Some(Ok(Message::Ping(data))) => {
                                last_pong = now_millis();
                                if write.send(Message::Pong(data)).await.is_err() {
                                    error!("WS {:?} failed to send pong", channel);
                                    break 'inner_session;
                                }
                            }
                            Some(Ok(_)) => {
                                warn!("WS {:?} received unknown message", channel);
                            }
                            Some(Err(e)) => {
                                error!("WS {:?} read error: {}", channel, e);
                                break 'inner_session;
                            }
                            None => {
                                info!("WS {:?} stream ended", channel);
                                break 'inner_session;
                            }
                        }
                    }

                    // ИСПРАВЛЕННЫЙ HEARTBEAT - настоящий Ping frame!
                    _ = heartbeat.tick() => {
                        let elapsed = now_millis() - last_pong;
                        if elapsed > WS_PONG_TIMEOUT.as_millis() as u64 {
                            error!("Pong timeout for {:?} ({}ms), reconnect", channel, elapsed);
                            break 'inner_session;
                        }


                        if write.send(Message::Ping(vec![].into())).await.is_err() {
                            error!("Failed to send Ping frame");
                            break 'inner_session;
                        }
                        debug!("WS {:?} → Ping frame sent", channel);
                    }

                    // Исходящие сообщения от юзера (подписки и т.д.)
                    msg = user_rx.recv() => {
                        match msg {
                            Ok(Queued { seq, outgoing }) => {
                                // Фильтрация: Subscribe/Unsubscribe только для MarketData
                                if channel != BcsWsChannel::MarketData {
                                    warn!("WS {:?} ignoring subscribe message for non-MarketData channel", channel);
                                    continue 'inner_session;
                                }
                                // Эта подписка уже ушла вместе со списком при подключении.
                                if seq.is_some_and(|n| restored.remove(&n)) {
                                    continue 'inner_session;
                                }
                                match serde_json::to_string(&outgoing) {
                                    Ok(json) => {
                                        if write.send(Message::Text(json.into())).await.is_err() {
                                            error!("WS {:?} write error", channel);
                                            break 'inner_session;
                                        }
                                    }
                                    Err(e) => {
                                        error!("WS {:?} serialize error: {}", channel, e);
                                    }
                                }
                            }
                            Err(_) => break 'session,
                        }
                    }


                }
            }

            is_connected.store(false, Ordering::Release); // ← DOWN (при любом выходе из inner)
            // Соединение, которое тут же рвется, не должно давить на сервер с минимальной паузой.
            if connected_at.elapsed() >= WS_HEALTHY_AFTER {
                backoff = backoff_min;
            }
            warn!(
                "WS {:?} disconnected, reconnecting in {:?}...",
                channel, backoff
            );
            sleep(backoff).await;
            backoff = (backoff * 2).min(backoff_max);
        }
        is_connected.store(false, Ordering::Release); // ← DOWN (выход из session loop)
    }

    async fn connect(url: &str, access_token: &str) -> Result<WsStream, BcsApiError> {
        let mut request = url
            .into_client_request()
            .map_err(|e| BcsApiError::WsError {
                code: None,
                message: format!("Failed to create WS request: {}", e),
            })?;

        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", access_token)
                .parse()
                .map_err(|e| BcsApiError::WsError {
                    code: None,
                    message: format!("Invalid auth header: {}", e),
                })?,
        );

        let (stream, res) = connect_async(request)
            .await
            .map_err(|e| BcsApiError::WsError {
                code: None,
                message: format!("WebSocket connection failed internally: {}", e),
            })?;

        info!(
            "WebSocket connected to {} with status <{}>",
            url,
            res.status()
        );
        Ok(stream)
    }
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
