//! The gateway: the server side.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brokapi::core::{
    Error, ExecutionSource, InstrumentSource, MarketDataSource, OrderBookSource, OrderExecutor,
    PortfolioSource, ShutdownMode, StopOrderExecutor,
};
use brokapi_client::{Client, ClientError, CloseReport, Limiter};
use futures_util::Stream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::access::{ADDRESS_FILE, Access, Address, BotName, Listen, load_or_create_key};
use crate::convert::{self as cv, Invalid};
use crate::error::{gateway_status, invalid_status, status_of};
use crate::hub::{Attached, Hub, RingConfig};
use crate::journal::Journal;
use crate::metrics::RateMeter;
use crate::pb;
use crate::pb::error_detail::Kind;
use crate::subs::{self, OWN, Subscriptions};

/// The largest message the gateway takes or sends: a year of minute candles is a few megabytes.
const MAX_MESSAGE: usize = 64 * 1024 * 1024;

/// How the gateway runs.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// The name of the gateway. It names the connection to the broker (its events come from `"<name>-md"` and
    /// `"<name>-ud"`) and is shown by `Health`.
    pub name: String,
    /// The directory of the gateway: the journal, the access key, the socket. Made if it is not there.
    pub state_dir: PathBuf,
    /// Where to listen.
    pub listen: Listen,
    /// How much of the past is kept for a bot that reconnects.
    pub ring: RingConfig,
    /// How many events wait for one bot before the bot is cut off as too slow.
    pub bot_buffer: usize,
    /// The size of the channel from the connectors to the gateway.
    pub event_buffer: usize,
    /// How long a bot may be away before its subscriptions are let go. A bot whose stream broke and comes back within
    /// this time keeps them.
    pub bot_grace: Duration,
}

impl GatewayConfig {
    /// The defaults for a gateway of this name and state directory.
    pub fn new(name: impl Into<String>, state_dir: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            state_dir: state_dir.into(),
            listen: Listen::Auto,
            ring: RingConfig::default(),
            bot_buffer: 1024,
            event_buffer: 8192,
            bot_grace: Duration::from_secs(30),
        }
    }
}

/// Why a gateway did not start.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// A file or a socket could not be made or read.
    #[error("{what}: {source}")]
    Io {
        /// What the gateway was doing.
        what: String,
        /// The error of the system.
        source: std::io::Error,
    },
    /// The address is not this machine.
    #[error(
        "the address {0} is not on this machine: the gateway listens on loopback only (it has no TLS yet)"
    )]
    NotLocal(String),
    /// Another gateway listens there.
    #[error("another gateway is already listening at {0}")]
    AlreadyRunning(String),
    /// The name of the gateway cannot be a part of an event source.
    #[error("the name of the gateway {0:?} is not valid: 1 to 64 letters, digits, '.', '_' or '-'")]
    BadName(String),
    /// The connection to the broker did not start.
    #[error(transparent)]
    Broker(#[from] Error),
}

pub(crate) fn io(what: impl Into<String>) -> impl FnOnce(std::io::Error) -> GatewayError {
    let what = what.into();
    move |source| GatewayError::Io { what, source }
}

/// What the gateway is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayState {
    /// Comparing the journal with the broker. Commands are refused.
    Starting,
    /// Serving.
    Ready,
    /// Stopping. Commands are refused.
    Stopping,
}

struct BotEntry {
    first_seen_ns: i64,
    last_seen_ns: i64,
    calls: u64,
    streams: u32,
    lagged: u64,
    /// Grows when a stream of the bot opens: a pending release of its subscriptions checks it.
    generation: u64,
}

struct Shared {
    name: String,
    client: Client,
    hub: Hub,
    journal: Journal,
    bots: Mutex<HashMap<String, BotEntry>>,
    state: watch::Sender<GatewayState>,
    detail: Mutex<String>,
    /// Becomes true when the gateway stops: the open streams end.
    stopping: watch::Sender<bool>,
    bot_buffer: usize,
    bot_grace: Duration,
    subs: Subscriptions,
    started: std::time::Instant,
    /// Events per second, over the last ten seconds.
    rate: Mutex<RateMeter>,
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

impl Shared {
    fn bot_entry<R>(&self, name: &str, f: impl FnOnce(&mut BotEntry) -> R) -> R {
        let mut bots = self.bots.lock().unwrap_or_else(|e| e.into_inner());
        let now = now_ns();
        let entry = bots.entry(name.to_owned()).or_insert(BotEntry {
            first_seen_ns: now,
            last_seen_ns: now,
            calls: 0,
            streams: 0,
            lagged: 0,
            generation: 0,
        });
        f(entry)
    }

    fn health(&self) -> pb::HealthResponse {
        let state = match *self.state.borrow() {
            GatewayState::Starting => pb::GatewayState::Starting,
            GatewayState::Ready => pb::GatewayState::Ready,
            GatewayState::Stopping => pb::GatewayState::Stopping,
        };
        let queued = self.hub.queued_by_bot();
        let mut bots: Vec<pb::BotInfo> = self
            .bots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(name, b)| pb::BotInfo {
                name: name.clone(),
                first_seen_ns: b.first_seen_ns,
                last_seen_ns: b.last_seen_ns,
                calls: b.calls,
                streaming: b.streams > 0,
                queued: queued.get(name).copied().unwrap_or(0),
                lagged: b.lagged,
            })
            .collect();
        bots.sort_by(|a, b| a.name.cmp(&b.name));
        pb::HealthResponse {
            state: state as i32,
            name: self.name.clone(),
            broker: self.client.broker().as_str().to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            epoch: self.hub.epoch(),
            last_seq: self.hub.last_seq(),
            detail: self
                .detail
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            open_orders: self.journal.open_count() as u64,
            bots,
            uptime_ns: crate::metrics::nanos(
                brokapi::runtime::now().saturating_duration_since(self.started),
            ),
            pid: std::process::id(),
            events_total: self.hub.last_seq(),
            events_per_sec: self
                .rate
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .per_second(brokapi::runtime::now()),
            limits: self
                .client
                .limiter()
                .shared()
                .usage(brokapi::runtime::now())
                .into_iter()
                .map(|(bucket, available, capacity)| pb::LimitInfo {
                    bucket,
                    available,
                    capacity,
                })
                .collect(),
            subscriptions: self.subs.list(),
        }
    }

    /// Writes the id of an order to the journal before the order is sent. The write waits for the disk, so it runs where
    /// waiting is allowed.
    async fn journal_open(
        self: &Arc<Self>,
        cid: brokapi::core::ClientOrderId,
        bot: &str,
    ) -> Result<(), Error> {
        let (shared, bot) = (Arc::clone(self), bot.to_owned());
        tokio::task::spawn_blocking(move || shared.journal.open_order(cid, &bot, now_ns()))
            .await
            .map_err(|e| journal_error(std::io::Error::other(e)))?
            .map_err(journal_error)
    }

    /// The subscriptions the gateway holds for itself, whatever the bots do: the states of orders and of stop orders. They
    /// bring the end of every order to the journal, also when no bot is connected. A broker that has no such stream
    /// (`Unsupported`) is not an error.
    async fn subscribe_own(self: &Arc<Self>) {
        let plain = format!("{:?}", pb::SubscribeRequest::default());
        let mut pause = Duration::from_secs(1);
        for kind in ["subscribe_order_state", "subscribe_stop_orders"] {
            for attempt in 0..5 {
                let client = self.client.clone();
                let result = self
                    .subs
                    .subscribe(
                        OWN,
                        subs::key(kind, &plain),
                        kind.trim_start_matches("subscribe_").to_owned(),
                        || async move {
                            if kind == "subscribe_order_state" {
                                client.subscribe_order_state().await
                            } else {
                                client.subscribe_stop_orders().await
                            }
                        },
                    )
                    .await;
                match result {
                    Ok(_) => break,
                    Err(Error::Unsupported { .. }) => break,
                    Err(e) if attempt < 4 && !*self.stopping.borrow() => {
                        tracing::warn!(
                            "the gateway's own subscription ({kind}) failed, trying again: {e}"
                        );
                        tokio::time::sleep(pause).await;
                        pause = (pause * 2).min(Duration::from_secs(8));
                    }
                    Err(e) => {
                        tracing::warn!("the gateway's own subscription ({kind}) failed: {e}");
                        break;
                    }
                }
            }
        }
    }

    /// Lets go of the subscriptions of a bot that has no event stream, once it has stayed away for the grace period.
    fn schedule_release(self: &Arc<Self>, bot: &str) {
        let (shared, bot) = (Arc::clone(self), bot.to_owned());
        let generation = self.bot_entry(&bot, |b| b.generation);
        tokio::spawn(async move {
            tokio::time::sleep(shared.bot_grace).await;
            let gone = shared.bot_entry(&bot, |b| b.streams == 0 && b.generation == generation);
            if gone && !*shared.stopping.borrow() {
                let client = shared.client.clone();
                let closed = shared
                    .subs
                    .release_bot(&bot, |sub| {
                        let client = client.clone();
                        async move { client.unsubscribe(sub).await }
                    })
                    .await;
                if closed > 0 {
                    tracing::info!(%bot, closed, "a bot is gone: its subscriptions are closed");
                }
            }
        });
    }

    fn set_detail(&self, text: impl Into<String>) {
        *self.detail.lock().unwrap_or_else(|e| e.into_inner()) = text.into();
    }

    /// What the gateway learns from an event on its way to the bots: an order that ended is closed in the journal.
    fn observe(&self, env: &brokapi::core::Envelope) {
        if let brokapi::core::Event::Order(o) = &env.payload
            && o.state.is_final()
        {
            self.journal.close_order(o.cid);
        }
    }
}

/// A running gateway.
///
/// It listens at [`Gateway::address`] and holds the connection to the broker. Dropping the handle does not stop it:
/// call [`Gateway::shutdown`].
pub struct Gateway {
    shared: Arc<Shared>,
    address: Address,
    key: String,
    state_dir: PathBuf,
    stop_server: Option<oneshot::Sender<()>>,
    server: JoinHandle<()>,
    pump: JoinHandle<()>,
}

pub(crate) enum BoundSocket {
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
    Tcp(tokio::net::TcpListener),
}

/// Refuses an address that is not this machine.
pub(crate) fn check_listen(listen: &Listen) -> Result<(), GatewayError> {
    match listen {
        Listen::Tcp(addr) if !addr.ip().is_loopback() => {
            Err(GatewayError::NotLocal(addr.to_string()))
        }
        #[cfg(not(unix))]
        Listen::Unix(path) => Err(GatewayError::Io {
            what: format!("listening on {}", path.display()),
            source: std::io::Error::other("Unix sockets are not available on this platform"),
        }),
        _ => Ok(()),
    }
}

pub(crate) async fn bind(
    listen: &Listen,
    state_dir: &std::path::Path,
    socket_name: &str,
) -> Result<(BoundSocket, Address), GatewayError> {
    #[cfg(not(unix))]
    let _ = (state_dir, socket_name); // only a Unix socket lives in the state directory
    let listen = match listen {
        #[cfg(unix)]
        Listen::Auto => Listen::Unix(
            crate::socket::auto_path(state_dir, socket_name)
                .map_err(io("choosing the place of the socket"))?,
        ),
        #[cfg(not(unix))]
        Listen::Auto => Listen::Tcp("127.0.0.1:0".parse().expect("a literal address")),
        other => other.clone(),
    };
    match listen {
        #[cfg(unix)]
        Listen::Unix(path) => {
            if !crate::socket::fits(&path) {
                return Err(GatewayError::Io {
                    what: format!("listening on {}", path.display()),
                    source: std::io::Error::other(format!(
                        "the path has {} bytes and a Unix socket path may have {} at most on this system. \
                         Give a shorter path, or leave `listen` out: the program then chooses the place itself",
                        path.as_os_str().len(),
                        crate::socket::MAX_SOCKET_PATH
                    )),
                });
            }
            if crate::socket::inspect(&path)
                .map_err(io(format!("listening on {}", path.display())))?
            {
                // A socket file is left behind by a gateway that died. One that answers is a gateway that lives.
                if tokio::net::UnixStream::connect(&path).await.is_ok() {
                    return Err(GatewayError::AlreadyRunning(path.display().to_string()));
                }
                std::fs::remove_file(&path).map_err(io(format!("removing {}", path.display())))?;
            }
            let listener = tokio::net::UnixListener::bind(&path)
                .map_err(io(format!("listening on {}", path.display())))?;
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                    .map_err(io(format!("protecting {}", path.display())))?;
            }
            Ok((BoundSocket::Unix(listener), Address::Unix(path)))
        }
        #[cfg(not(unix))]
        Listen::Unix(_) => unreachable!("refused by check_listen"),
        Listen::Tcp(addr) => {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(io(format!("listening on {addr}")))?;
            let local = listener.local_addr().map_err(io("reading the address"))?;
            Ok((BoundSocket::Tcp(listener), Address::Tcp(local)))
        }
        Listen::Auto => unreachable!("resolved above"),
    }
}

impl Gateway {
    /// Starts the connection to the broker and the gateway on it. The gateway answers `Health` at once with
    /// `starting`, compares its journal with the broker's active orders, and only then takes commands.
    pub async fn start(
        config: GatewayConfig,
        broker: brokapi_client::BrokerConfig,
        limiter: Option<Limiter>,
    ) -> Result<Self, GatewayError> {
        check_listen(&config.listen)?;
        if !crate::access::valid_bot_name(&config.name) {
            return Err(GatewayError::BadName(config.name));
        }
        let client =
            Client::start(broker, config.name.clone(), limiter, config.event_buffer).await?;
        Self::start_with_client(config, client).await
    }

    /// Starts the gateway on a connection that is already made.
    pub async fn start_with_client(
        config: GatewayConfig,
        client: Client,
    ) -> Result<Self, GatewayError> {
        let started = Self::prepare(&config, &client).await;
        match started {
            Ok(gateway) => Ok(gateway),
            Err(e) => {
                client.close(ShutdownMode::Immediate).await;
                Err(e)
            }
        }
    }

    async fn prepare(config: &GatewayConfig, client: &Client) -> Result<Self, GatewayError> {
        check_listen(&config.listen)?;
        std::fs::create_dir_all(&config.state_dir)
            .map_err(io(format!("making {}", config.state_dir.display())))?;
        let key = load_or_create_key(&config.state_dir).map_err(io("the access key"))?;
        let journal = Journal::open(&config.state_dir).map_err(io("the journal"))?;
        let (socket, address) = bind(&config.listen, &config.state_dir, "gateway.sock").await?;
        if let Err(e) = std::fs::write(config.state_dir.join(ADDRESS_FILE), address.to_string()) {
            if let Address::Unix(path) = &address {
                let _ = std::fs::remove_file(path);
            }
            return Err(io("writing the address file")(e));
        }

        let epoch = u64::from_le_bytes(
            uuid::Uuid::new_v4().as_bytes()[..8]
                .try_into()
                .expect("8 bytes"),
        ) | 1;
        let (state, _) = watch::channel(GatewayState::Starting);
        let (stopping, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            name: config.name.clone(),
            client: client.clone(),
            hub: Hub::new(config.ring, epoch),
            journal,
            bots: Mutex::new(HashMap::new()),
            state,
            detail: Mutex::new("comparing the journal with the broker".into()),
            stopping,
            bot_buffer: config.bot_buffer,
            bot_grace: config.bot_grace,
            subs: Subscriptions::default(),
            started: brokapi::runtime::now(),
            rate: Mutex::new(RateMeter::new(brokapi::runtime::now())),
        });

        let access = Access::new(&key);
        let server_side = pb::gateway_server::GatewayServer::new(GatewayService {
            shared: Arc::clone(&shared),
        })
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
        let service =
            tonic::service::interceptor::InterceptedService::new(server_side, move |req| {
                access.check(req)
            });
        let (stop_server, stopped) = oneshot::channel::<()>();
        let signal = async move {
            let _ = stopped.await;
        };
        let router = tonic::transport::Server::builder().add_service(service);
        let server = match socket {
            #[cfg(unix)]
            BoundSocket::Unix(listener) => tokio::spawn(async move {
                let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
                if let Err(e) = router.serve_with_incoming_shutdown(incoming, signal).await {
                    tracing::error!("the gateway server ended with an error: {e}");
                }
            }),
            BoundSocket::Tcp(listener) => tokio::spawn(async move {
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                if let Err(e) = router.serve_with_incoming_shutdown(incoming, signal).await {
                    tracing::error!("the gateway server ended with an error: {e}");
                }
            }),
        };

        // One task runs the connection of the gateway to its hub. First the journal is compared with the broker. The
        // events that were already waiting when the comparison ended (a first snapshot of the account, for one) are
        // numbered before the gateway says it is ready, so a bot that connects after that sees exactly the events
        // from its own start. Then the task passes on every event as it comes.
        let mut events = client
            .take_events()
            .await
            .expect("a new connection has its event channel");
        let task_shared = Arc::clone(&shared);
        let pump = tokio::spawn(async move {
            let sh = task_shared;
            let publish = |env: brokapi::core::Envelope| {
                sh.observe(&env);
                match cv::event_to(0, &env) {
                    Some(event) => {
                        sh.rate
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .record(brokapi::runtime::now());
                        let (_, cut) = sh.hub.publish(event);
                        for bot in cut {
                            tracing::warn!(%bot, "a bot reads too slowly: its event stream is closed");
                            sh.bot_entry(&bot, |b| b.lagged += 1);
                        }
                    }
                    None => {
                        tracing::warn!("an event of a kind this version cannot send was dropped")
                    }
                }
            };
            let mut pause = Duration::from_millis(500);
            loop {
                let ledger = sh.journal.open_orders();
                match sh.client.reconcile(&ledger).await {
                    Ok(report) => {
                        for cid in &report.closed_offline {
                            sh.journal.close_order(*cid);
                        }
                        tracing::info!(
                            confirmed = report.confirmed.len(),
                            closed_offline = report.closed_offline.len(),
                            foreign = report.foreign.len(),
                            "the journal is compared with the broker"
                        );
                        while let Ok(env) = events.try_recv() {
                            publish(env);
                        }
                        sh.set_detail("");
                        sh.state.send_replace(GatewayState::Ready);
                        let own = Arc::clone(&sh);
                        tokio::spawn(async move { own.subscribe_own().await });
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("the reconcile at start failed, trying again: {e}");
                        sh.set_detail(format!("the reconcile at start failed: {e}"));
                        tokio::time::sleep(pause).await;
                        pause = (pause * 2).min(Duration::from_secs(30));
                    }
                }
            }
            while let Some(env) = events.recv().await {
                publish(env);
            }
        });

        Ok(Self {
            shared,
            address,
            key,
            state_dir: config.state_dir.clone(),
            stop_server: Some(stop_server),
            server,
            pump,
        })
    }

    /// Where the gateway listens.
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// The access key: the contents of `access.key`.
    pub fn access_key(&self) -> &str {
        &self.key
    }

    /// The state directory.
    pub fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }

    /// The connection to the broker, for code in the same process (tests, a manager).
    pub fn client(&self) -> &Client {
        &self.shared.client
    }

    /// What the gateway is doing now.
    pub fn state(&self) -> GatewayState {
        *self.shared.state.borrow()
    }

    /// What `Health` answers.
    pub fn health(&self) -> pb::HealthResponse {
        self.shared.health()
    }

    /// Waits until the gateway takes commands.
    pub async fn ready(&self) {
        let mut rx = self.shared.state.subscribe();
        let _ = rx.wait_for(|s| *s != GatewayState::Starting).await;
    }

    /// Stops the gateway: refuses new commands, ends the event streams, stops listening and closes the connection to
    /// the broker. Orders at the exchange are not touched.
    pub async fn shutdown(mut self, mode: ShutdownMode) -> Option<CloseReport> {
        self.shared.state.send_replace(GatewayState::Stopping);
        self.shared.stopping.send_replace(true);
        if let Some(stop) = self.stop_server.take() {
            let _ = stop.send(());
        }
        if tokio::time::timeout(Duration::from_secs(5), &mut self.server)
            .await
            .is_err()
        {
            self.server.abort();
        }
        let report = self.shared.client.close(mode).await;
        self.pump.abort();
        self.cleanup();
        report
    }

    /// Ends the gateway the way a crash would: nothing is said to the bots, the journal is left as it is, the
    /// connection to the broker is dropped. For tests of the restart.
    pub async fn kill(self) {
        self.server.abort();
        self.pump.abort();
        self.shared.stopping.send_replace(true);
        self.shared.client.close(ShutdownMode::Immediate).await;
        self.cleanup();
    }

    fn cleanup(&self) {
        if let Address::Unix(path) = &self.address {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_file(self.state_dir.join(ADDRESS_FILE));
    }
}

// ---------------------------------------------------------------------------------------------------- the service

struct GatewayService {
    shared: Arc<Shared>,
}

type Reply<T> = Result<Response<T>, Status>;

impl GatewayService {
    /// The start of every call that touches the broker: notes the bot, and refuses when the gateway is not ready.
    fn enter<T>(&self, req: &Request<T>) -> Result<Arc<str>, Status> {
        let bot = self.bot(req);
        match *self.shared.state.borrow() {
            GatewayState::Ready => Ok(bot),
            GatewayState::Starting => Err(gateway_status(
                Kind::GatewayStarting,
                "the gateway is starting: it is comparing its journal with the broker, try again",
            )),
            GatewayState::Stopping => Err(status_of(&Error::ShuttingDown)),
        }
    }

    /// The name of the bot, and a note that it called.
    fn bot<T>(&self, req: &Request<T>) -> Arc<str> {
        let name = req
            .extensions()
            .get::<BotName>()
            .map_or_else(|| Arc::from(crate::access::ANONYMOUS), |b| Arc::clone(&b.0));
        self.shared.bot_entry(&name, |b| {
            b.calls += 1;
            b.last_seen_ns = now_ns();
        });
        name
    }

    /// A read: the answer or the status of the error.
    async fn read<T, F>(&self, work: F) -> Reply<T>
    where
        F: Future<Output = Result<T, Error>>,
    {
        work.await.map(Response::new).map_err(|e| status_of(&e))
    }

    /// A command: runs to its end even if the bot stops waiting. A place that is dropped half way could be at the
    /// broker already.
    async fn command<T, F>(&self, work: F) -> Reply<T>
    where
        F: Future<Output = Result<T, Error>> + Send + 'static,
        T: Send + 'static,
    {
        match tokio::spawn(work).await {
            Ok(result) => result.map(Response::new).map_err(|e| status_of(&e)),
            Err(_) => Err(status_of(&Error::Unknown)),
        }
    }

    fn shared(&self) -> Arc<Shared> {
        Arc::clone(&self.shared)
    }

    /// A subscription for `bot`: the one at the broker if another bot already holds the same, else a new one.
    async fn subscription<F>(
        &self,
        bot: &Arc<str>,
        kind: &str,
        request: String,
        open: F,
    ) -> Reply<pb::SubscribeResponse>
    where
        F: Future<Output = Result<brokapi::core::SubscriptionId, Error>>,
    {
        let sh = self.shared();
        let what = kind.trim_start_matches("subscribe_").to_owned();
        let sub = sh
            .subs
            .subscribe(bot, subs::key(kind, &request), what, || open)
            .await
            .map_err(|e| status_of(&e))?;
        // A bot that subscribes without reading events is gone as soon as it has been quiet long enough.
        if sh.bot_entry(bot, |b| b.streams == 0) {
            sh.schedule_release(bot);
        }
        Ok(Response::new(pb::SubscribeResponse { sub: sub.value() }))
    }
}

fn bad<T>(r: Result<T, Invalid>) -> Result<T, Status> {
    r.map_err(invalid_status)
}

fn journal_error(e: std::io::Error) -> Error {
    Error::Transport(brokapi::core::TransportError::Message(format!(
        "the journal cannot be written, the order was not sent: {e}"
    )))
}

#[tonic::async_trait]
impl pb::gateway_server::Gateway for GatewayService {
    // ---- orders

    async fn place(&self, req: Request<pb::PlaceOrderRequest>) -> Reply<pb::OrderAck> {
        let bot = self.enter(&req)?;
        let request = bad(cv::place_from(req.get_ref()))?;
        let sh = self.shared();
        self.command(async move {
            sh.journal_open(request.cid, &bot).await?;
            let cid = request.cid;
            let result = sh.client.place(request).await;
            // An order the broker refused for sure is not open. After "unknown" it may be, and stays in the journal.
            if let Err(e) = &result
                && !matches!(e, Error::Unknown)
            {
                sh.journal.close_order(cid);
            }
            result.map(|a| cv::ack_to(&a))
        })
        .await
    }

    async fn cancel(&self, req: Request<pb::CancelRequest>) -> Reply<pb::CancelResponse> {
        self.enter(&req)?;
        let cid = bad(cv::cid(&req.get_ref().cid))?;
        let sh = self.shared();
        self.command(async move { sh.client.cancel(cid).await.map(|()| pb::CancelResponse {}) })
            .await
    }

    async fn replace(&self, req: Request<pb::ReplaceOrderRequest>) -> Reply<pb::OrderAck> {
        let bot = self.enter(&req)?;
        let request = bad(cv::replace_from(req.get_ref()))?;
        let sh = self.shared();
        self.command(async move {
            sh.journal_open(request.cid, &bot).await?;
            let cid = request.cid;
            let result = sh.client.replace(request).await;
            if let Err(e) = &result
                && !matches!(e, Error::Unknown)
            {
                sh.journal.close_order(cid);
            }
            result.map(|a| cv::ack_to(&a))
        })
        .await
    }

    async fn order_state(&self, req: Request<pb::OrderStateRequest>) -> Reply<pb::OrderSnapshot> {
        self.enter(&req)?;
        let cid = bad(cv::cid(&req.get_ref().cid))?;
        let sh = self.shared();
        self.read(async move { sh.client.order_state(cid).await.map(|o| cv::order_to(&o)) })
            .await
    }

    async fn active_orders(
        &self,
        req: Request<pb::ActiveOrdersRequest>,
    ) -> Reply<pb::ActiveOrdersResponse> {
        self.enter(&req)?;
        let sh = self.shared();
        self.read(async move {
            let orders = sh.client.active_orders().await?;
            Ok(pb::ActiveOrdersResponse {
                orders: orders.iter().map(cv::order_to).collect(),
            })
        })
        .await
    }

    async fn reconcile(&self, req: Request<pb::ReconcileRequest>) -> Reply<pb::ReconcileReport> {
        self.enter(&req)?;
        let mut ledger = bad(cv::cids_from(&req.get_ref().ledger))?;
        let include_journal = req.get_ref().include_journal;
        let sh = self.shared();
        self.command(async move {
            if include_journal {
                for cid in sh.journal.open_orders() {
                    if !ledger.contains(&cid) {
                        ledger.push(cid);
                    }
                }
            }
            let report = sh.client.reconcile(&ledger).await?;
            for cid in &report.closed_offline {
                sh.journal.close_order(*cid);
            }
            Ok(cv::report_to(&report))
        })
        .await
    }

    async fn subscribe_order_state(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Reply<pb::SubscribeResponse> {
        let bot = self.enter(&req)?;
        let sh = self.shared();
        self.subscription(
            &bot,
            "subscribe_order_state",
            format!("{:?}", req.get_ref()),
            async move { sh.client.subscribe_order_state().await },
        )
        .await
    }

    // ---- stop orders

    async fn place_stop(&self, req: Request<pb::PlaceStopOrderRequest>) -> Reply<pb::OrderAck> {
        self.enter(&req)?;
        let request = bad(cv::place_stop_from(req.get_ref()))?;
        let sh = self.shared();
        self.command(async move { sh.client.place_stop(request).await.map(|a| cv::ack_to(&a)) })
            .await
    }

    async fn cancel_stop(&self, req: Request<pb::CancelStopRequest>) -> Reply<pb::CancelResponse> {
        self.enter(&req)?;
        let stop = bad(cv::stop_ref_from(&req.get_ref().stop))?;
        let sh = self.shared();
        self.command(async move {
            sh.client
                .cancel_stop(stop)
                .await
                .map(|()| pb::CancelResponse {})
        })
        .await
    }

    async fn active_stop_orders(
        &self,
        req: Request<pb::ActiveOrdersRequest>,
    ) -> Reply<pb::ActiveStopOrdersResponse> {
        self.enter(&req)?;
        let sh = self.shared();
        self.read(async move {
            let stops = sh.client.active_stop_orders().await?;
            Ok(pb::ActiveStopOrdersResponse {
                stop_orders: stops.iter().map(cv::stop_to).collect(),
            })
        })
        .await
    }

    async fn subscribe_stop_orders(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Reply<pb::SubscribeResponse> {
        let bot = self.enter(&req)?;
        let sh = self.shared();
        self.subscription(
            &bot,
            "subscribe_stop_orders",
            format!("{:?}", req.get_ref()),
            async move { sh.client.subscribe_stop_orders().await },
        )
        .await
    }

    // ---- account

    async fn portfolio(&self, req: Request<pb::AccountRequest>) -> Reply<pb::Portfolio> {
        self.enter(&req)?;
        let sh = self.shared();
        self.read(async move { sh.client.portfolio().await.map(|p| cv::portfolio_to(&p)) })
            .await
    }

    async fn positions(&self, req: Request<pb::AccountRequest>) -> Reply<pb::PositionsResponse> {
        self.enter(&req)?;
        let sh = self.shared();
        self.read(async move {
            let positions = sh.client.positions().await?;
            Ok(pb::PositionsResponse {
                positions: cv::positions_to(&positions),
            })
        })
        .await
    }

    async fn balances(&self, req: Request<pb::AccountRequest>) -> Reply<pb::BalancesResponse> {
        self.enter(&req)?;
        let sh = self.shared();
        self.read(async move {
            let balances = sh.client.balances().await?;
            Ok(pb::BalancesResponse {
                balances: cv::balances_to(&balances),
            })
        })
        .await
    }

    async fn executions(
        &self,
        req: Request<pb::ExecutionsRequest>,
    ) -> Reply<pb::ExecutionsResponse> {
        self.enter(&req)?;
        let range = bad(cv::range_from(&req.get_ref().range))?;
        let sh = self.shared();
        self.read(async move {
            let executions = sh.client.executions(range).await?;
            Ok(pb::ExecutionsResponse {
                executions: executions.iter().map(cv::execution_to).collect(),
            })
        })
        .await
    }

    async fn subscribe_executions(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Reply<pb::SubscribeResponse> {
        let bot = self.enter(&req)?;
        let sh = self.shared();
        self.subscription(
            &bot,
            "subscribe_executions",
            format!("{:?}", req.get_ref()),
            async move { sh.client.subscribe_executions().await },
        )
        .await
    }

    async fn subscribe_portfolio(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Reply<pb::SubscribeResponse> {
        let bot = self.enter(&req)?;
        let opts = bad(cv::opts_from(&req.get_ref().opts))?;
        let sh = self.shared();
        self.subscription(
            &bot,
            "subscribe_portfolio",
            format!("{:?}", req.get_ref()),
            async move { sh.client.subscribe_portfolio(opts).await },
        )
        .await
    }

    // ---- market data

    async fn order_book(&self, req: Request<pb::OrderBookRequest>) -> Reply<pb::OrderBook> {
        self.enter(&req)?;
        let instrument = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let depth = req.get_ref().depth;
        let sh = self.shared();
        self.read(async move {
            sh.client
                .order_book(&instrument, depth)
                .await
                .map(|b| cv::book_to(&b))
        })
        .await
    }

    async fn last_trades(
        &self,
        req: Request<pb::LastTradesRequest>,
    ) -> Reply<pb::LastTradesResponse> {
        self.enter(&req)?;
        let instrument = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let limit = req.get_ref().limit;
        let sh = self.shared();
        self.read(async move {
            let trades = sh.client.last_trades(&instrument, limit).await?;
            Ok(pb::LastTradesResponse {
                trades: trades.iter().map(cv::trade_to).collect(),
            })
        })
        .await
    }

    async fn candles(&self, req: Request<pb::CandlesRequest>) -> Reply<pb::CandlesResponse> {
        self.enter(&req)?;
        let instrument = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let timeframe = bad(cv::timeframe_from(req.get_ref().timeframe))?;
        let range = bad(cv::range_from(&req.get_ref().range))?;
        let sh = self.shared();
        self.read(async move {
            let candles = sh.client.candles(&instrument, timeframe, range).await?;
            Ok(pb::CandlesResponse {
                candles: candles.iter().map(cv::candle_to).collect(),
            })
        })
        .await
    }

    async fn subscribe_order_book(
        &self,
        req: Request<pb::SubscribeInstrumentRequest>,
    ) -> Reply<pb::SubscribeResponse> {
        let bot = self.enter(&req)?;
        let instrument = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let opts = bad(cv::opts_from(&req.get_ref().opts))?;
        let sh = self.shared();
        self.subscription(
            &bot,
            "subscribe_order_book",
            format!("{:?}", req.get_ref()),
            async move { sh.client.subscribe_order_book(&instrument, opts).await },
        )
        .await
    }

    async fn subscribe_market_trades(
        &self,
        req: Request<pb::SubscribeInstrumentRequest>,
    ) -> Reply<pb::SubscribeResponse> {
        let bot = self.enter(&req)?;
        let instrument = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let opts = bad(cv::opts_from(&req.get_ref().opts))?;
        let sh = self.shared();
        self.subscription(
            &bot,
            "subscribe_market_trades",
            format!("{:?}", req.get_ref()),
            async move { sh.client.subscribe_market_trades(&instrument, opts).await },
        )
        .await
    }

    async fn subscribe_quotes(
        &self,
        req: Request<pb::SubscribeInstrumentRequest>,
    ) -> Reply<pb::SubscribeResponse> {
        let bot = self.enter(&req)?;
        let instrument = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let opts = bad(cv::opts_from(&req.get_ref().opts))?;
        let sh = self.shared();
        self.subscription(
            &bot,
            "subscribe_quotes",
            format!("{:?}", req.get_ref()),
            async move { sh.client.subscribe_quotes(&instrument, opts).await },
        )
        .await
    }

    async fn subscribe_candles(
        &self,
        req: Request<pb::SubscribeCandlesRequest>,
    ) -> Reply<pb::SubscribeResponse> {
        let bot = self.enter(&req)?;
        let instrument = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let timeframe = bad(cv::timeframe_from(req.get_ref().timeframe))?;
        let opts = bad(cv::opts_from(&req.get_ref().opts))?;
        let sh = self.shared();
        self.subscription(
            &bot,
            "subscribe_candles",
            format!("{:?}", req.get_ref()),
            async move {
                sh.client
                    .subscribe_candles(&instrument, timeframe, opts)
                    .await
            },
        )
        .await
    }

    async fn unsubscribe(
        &self,
        req: Request<pb::UnsubscribeRequest>,
    ) -> Reply<pb::UnsubscribeResponse> {
        let bot = self.enter(&req)?;
        let sub = brokapi::core::SubscriptionId::new(req.get_ref().sub);
        let sh = self.shared();
        let result = sh
            .subs
            .unsubscribe(&bot, sub, || {
                let client = sh.client.clone();
                async move { client.unsubscribe(sub).await }
            })
            .await;
        result
            .map(|()| Response::new(pb::UnsubscribeResponse {}))
            .map_err(|e| status_of(&e))
    }

    // ---- instruments

    async fn resolve_instrument(
        &self,
        req: Request<pb::ResolveInstrumentRequest>,
    ) -> Reply<pb::InstrumentId> {
        self.enter(&req)?;
        let ticker = req.get_ref().ticker.clone();
        let board = req.get_ref().board.clone();
        let sh = self.shared();
        match sh.client.instrument(&ticker, board.as_deref()).await {
            Ok(id) => Ok(Response::new(cv::instrument_to(&id))),
            Err(ClientError::Broker(e)) => Err(status_of(&e)),
            Err(ClientError::Arg(m)) => Err(gateway_status(Kind::BadRequest, m)),
            Err(ClientError::Closed) => Err(status_of(&Error::ShuttingDown)),
        }
    }

    async fn find(&self, req: Request<pb::FindRequest>) -> Reply<pb::FindResponse> {
        self.enter(&req)?;
        let query = bad(cv::query_from(&req.get_ref().query))?;
        let sh = self.shared();
        self.read(async move {
            let found = sh.client.find(query).await?;
            Ok(pb::FindResponse {
                instruments: found.iter().map(cv::instrument_info_to).collect(),
            })
        })
        .await
    }

    async fn get_instrument(&self, req: Request<pb::InstrumentRequest>) -> Reply<pb::Instrument> {
        self.enter(&req)?;
        let id = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let sh = self.shared();
        self.read(async move { sh.client.get(&id).await.map(|i| cv::instrument_info_to(&i)) })
            .await
    }

    async fn trading_params(
        &self,
        req: Request<pb::InstrumentRequest>,
    ) -> Reply<pb::TradingParams> {
        self.enter(&req)?;
        let id = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let sh = self.shared();
        self.read(async move {
            sh.client
                .trading_params(&id)
                .await
                .map(|p| cv::params_to(&p))
        })
        .await
    }

    async fn trading_status(
        &self,
        req: Request<pb::InstrumentRequest>,
    ) -> Reply<pb::TradingStatus> {
        self.enter(&req)?;
        let id = bad(cv::instrument_from(&req.get_ref().instrument))?;
        let sh = self.shared();
        self.read(async move {
            sh.client
                .trading_status(&id)
                .await
                .map(|s| cv::status_to(&s))
        })
        .await
    }

    async fn schedule(&self, req: Request<pb::ScheduleRequest>) -> Reply<pb::Schedule> {
        self.enter(&req)?;
        let exchange = req.get_ref().exchange.clone();
        let range = bad(cv::range_from(&req.get_ref().range))?;
        let sh = self.shared();
        self.read(async move {
            InstrumentSource::schedule(&sh.client, &exchange, range)
                .await
                .map(|s| cv::schedule_to(&s))
        })
        .await
    }

    // ---- the rest

    async fn raw(&self, req: Request<pb::RawRequest>) -> Reply<pb::RawResponse> {
        self.enter(&req)?;
        let r = req.get_ref();
        let args: serde_json::Value = if r.args_json.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&r.args_json).map_err(|e| {
                gateway_status(Kind::BadRequest, format!("args_json is not JSON: {e}"))
            })?
        };
        let (method, command) = (r.method.clone(), r.command);
        let sh = self.shared();
        let work = async move {
            match sh.client.raw(&method, &args, command).await {
                Ok(reply) => Ok(pb::RawResponse {
                    reply_json: reply.to_string(),
                }),
                Err(ClientError::Broker(e)) => Err(Ok(e)),
                Err(ClientError::Arg(m)) => Err(Err(m)),
                Err(ClientError::Closed) => Err(Ok(Error::ShuttingDown)),
            }
        };
        let outcome = if command {
            tokio::spawn(work)
                .await
                .map_err(|_| status_of(&Error::Unknown))?
        } else {
            work.await
        };
        match outcome {
            Ok(reply) => Ok(Response::new(reply)),
            Err(Ok(e)) => Err(status_of(&e)),
            Err(Err(m)) => Err(gateway_status(Kind::BadRequest, m)),
        }
    }

    async fn caps(&self, req: Request<pb::CapsRequest>) -> Reply<pb::CapsResponse> {
        self.bot(&req);
        match self.shared.client.caps().await {
            Some(caps) => Ok(Response::new(pb::CapsResponse {
                md: Some(cv::md_caps_to(&caps.md)),
                ud: Some(cv::ud_caps_to(&caps.ud)),
                candle_spans: cv::candle_spans_to(&self.shared.client),
            })),
            None => Err(status_of(&Error::ShuttingDown)),
        }
    }

    async fn health(&self, req: Request<pb::HealthRequest>) -> Reply<pb::HealthResponse> {
        self.bot(&req);
        Ok(Response::new(self.shared.health()))
    }

    type EventsStream = Pin<Box<dyn Stream<Item = Result<pb::EventFrame, Status>> + Send>>;

    async fn events(&self, req: Request<pb::EventsRequest>) -> Reply<Self::EventsStream> {
        let bot = self.enter(&req)?;
        let r = req.get_ref();
        let resume = (r.epoch != 0).then_some((r.epoch, r.last_seq));
        let own_only = r.own_orders_only;
        let sh = self.shared();
        let attached = sh.hub.attach(&bot, sh.bot_buffer, resume);
        sh.bot_entry(&bot, |b| {
            b.streams += 1;
            b.generation += 1;
        });
        let (out, rx) = mpsc::channel(8);
        tokio::spawn(forward(sh, bot, attached, own_only, out));
        let stream: Self::EventsStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }
}

/// Moves the frames of one bot's stream from the hub to the connection.
async fn forward(
    sh: Arc<Shared>,
    bot: Arc<str>,
    attached: Attached,
    own_only: bool,
    out: mpsc::Sender<Result<pb::EventFrame, Status>>,
) {
    use pb::event_frame::Frame;
    let Attached {
        id,
        ready,
        gap,
        replay,
        mut live,
        lagged,
    } = attached;
    let mut stopping = sh.stopping.subscribe();
    let mut last_seq = 0;

    let wanted = |e: &pb::Event| -> bool {
        if !own_only || !cv::event_is_order_bound(e) {
            return true;
        }
        cv::event_cid(e)
            .and_then(|cid| sh.journal.bot_of(&cid))
            .is_some_and(|owner| owner == *bot)
    };
    let frame = |f: Frame| Ok(pb::EventFrame { frame: Some(f) });

    'run: {
        if out.send(frame(Frame::Ready(ready))).await.is_err() {
            break 'run;
        }
        if let Some(gap) = gap
            && out.send(frame(Frame::Gap(gap))).await.is_err()
        {
            break 'run;
        }
        for event in replay {
            last_seq = event.seq;
            if wanted(&event)
                && out
                    .send(frame(Frame::Event((*event).clone())))
                    .await
                    .is_err()
            {
                break 'run;
            }
        }
        loop {
            tokio::select! {
                next = live.recv() => match next {
                    Some(event) => {
                        last_seq = event.seq;
                        if wanted(&event)
                            && out.send(frame(Frame::Event((*event).clone()))).await.is_err()
                        {
                            break 'run;
                        }
                    }
                    None => break,
                },
                () = async { let _ = stopping.wait_for(|s| *s).await; } => break 'run,
                () = out.closed() => break 'run,
            }
        }
        // The hub dropped this stream: the bot was too slow. What was in its buffer is delivered, and the last frame says why.
        if lagged.load(std::sync::atomic::Ordering::SeqCst) {
            let _ = out
                .send(frame(Frame::Lagged(pb::Lagged { last_seq })))
                .await;
        }
    }
    sh.hub.detach(id);
    let none_left = sh.bot_entry(&bot, |b| {
        b.streams = b.streams.saturating_sub(1);
        b.streams == 0
    });
    if none_left && !*sh.stopping.borrow() {
        sh.schedule_release(&bot);
    }
}
