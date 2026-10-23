//! The service: one task for each gateway that keeps its process alive, and the API of the commands.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::access::{Access, Address, load_or_create_key_in};
use crate::server::{BoundSocket, bind, check_listen, io};
use crate::{GatewayError, RemoteClient, pb as gw};

use super::api::Api;
use super::config::{Config, GatewaySpec, Layout};
use super::pb;
use super::restart::{Next, Restarts};
use super::{logs, proc, registry};

/// The file with the access key of the service.
pub const KEY_FILE: &str = "service.key";
/// The file that says where the service listens.
pub const ADDRESS_FILE: &str = "service.addr";

/// Why a service did not start.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Another service uses the state directory.
    #[error(
        "the state directory {state_dir} is used by another service (pid {pid}, config {config}): `brokapi down` stops it, or give this config another state_dir"
    )]
    Occupied {
        /// The directory.
        state_dir: String,
        /// The process of the other service.
        pid: u32,
        /// Its config.
        config: String,
    },
    /// The socket, a file or the key.
    #[error(transparent)]
    Setup(#[from] GatewayError),
}

/// How a service runs.
#[derive(Debug, Clone)]
pub struct ServiceOptions {
    /// The program that is started for a gateway: `<exe> -c <config> gateway run <name>`. Normally the running
    /// program itself.
    pub exe: PathBuf,
}

/// What a gateway process is doing, as the service puts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayState {
    /// The process runs and the gateway is comparing its journal with the broker (or has not answered yet).
    Starting,
    /// The gateway takes commands.
    Running,
    /// The process stopped on its own and will be started again.
    Restarting,
    /// The service has asked the process to stop.
    Stopping,
    /// Stopped, and it stays so until `start`.
    Stopped,
    /// Stopped on its own too many times: it waits for `start`.
    Failed,
}

impl GatewayState {
    /// The word `brokapi ps` shows.
    pub fn as_str(self) -> &'static str {
        match self {
            GatewayState::Starting => "starting",
            GatewayState::Running => "running",
            GatewayState::Restarting => "restarting",
            GatewayState::Stopping => "stopping",
            GatewayState::Stopped => "stopped",
            GatewayState::Failed => "failed",
        }
    }
}

enum Cmd {
    Start(oneshot::Sender<()>),
    Restart(oneshot::Sender<()>),
    Stop(oneshot::Sender<()>),
    /// The service ends. `leave`: the gateway process is left running.
    Quit {
        leave: bool,
    },
}

struct Status {
    state: GatewayState,
    pid: Option<u32>,
    started: Option<std::time::Instant>,
    restarts: u32,
    last_error: String,
    restart_at: Option<tokio::time::Instant>,
    adopted: bool,
    health: Option<gw::HealthResponse>,
    cpu: f32,
    memory: u64,
}

pub(crate) struct Slot {
    pub(crate) name: String,
    pub(crate) spec: Mutex<GatewaySpec>,
    status: Mutex<Status>,
    tx: mpsc::UnboundedSender<Cmd>,
    task: Mutex<Option<JoinHandle<()>>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) struct Inner {
    pub(crate) config: Mutex<Config>,
    pub(crate) layout: Layout,
    pub(crate) exe: PathBuf,
    pub(crate) slots: tokio::sync::Mutex<BTreeMap<String, Arc<Slot>>>,
    pub(crate) started: std::time::Instant,
    pub(crate) started_unix: u64,
    pub(crate) address: Address,
    pub(crate) shutdown: watch::Sender<Option<bool>>,
}

/// A running service.
pub struct Service {
    inner: Arc<Inner>,
    key: String,
    stop_server: Option<oneshot::Sender<()>>,
    server: JoinHandle<()>,
    monitor: JoinHandle<()>,
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl Service {
    /// Starts a service for this config: listens, and starts or takes over the gateways.
    ///
    /// Gateways whose processes are still running from an earlier service (one that was killed) are taken over, not
    /// started again.
    pub async fn start(config: Config, options: ServiceOptions) -> Result<Self, ServiceError> {
        let layout = config.layout();
        let state_dir = layout.root.clone();
        std::fs::create_dir_all(layout.logs())
            .map_err(io(format!("making {}", state_dir.display())))?;
        let me = std::process::id();
        let occupant = registry::find(&state_dir)
            .map(|e| (e.pid, e.config.display().to_string()))
            .or_else(|| {
                proc::read_pid(&layout.service_pid())
                    .filter(|p| proc::is_brokapi(*p))
                    .map(|p| (p, "unknown".to_owned()))
            });
        if let Some((pid, config)) = occupant
            && pid != me
        {
            return Err(ServiceError::Occupied {
                state_dir: state_dir.display().to_string(),
                pid,
                config,
            });
        }

        check_listen(&config.listen())?;
        let key = load_or_create_key_in(&state_dir, KEY_FILE).map_err(io("the service key"))?;
        let (socket, address) = bind(&config.listen(), &state_dir, "service.sock").await?;
        std::fs::write(state_dir.join(ADDRESS_FILE), address.to_string())
            .map_err(io("writing the address file"))?;
        std::fs::write(layout.service_pid(), me.to_string()).map_err(io("writing the pid file"))?;
        let started_unix = unix_now();
        registry::register(&registry::Entry {
            config: config.path.clone(),
            state_dir: state_dir.clone(),
            pid: me,
            address: address.to_string(),
            started_unix,
        })
        .map_err(io("writing the registry"))?;

        let (shutdown, _) = watch::channel(None);
        let inner = Arc::new(Inner {
            config: Mutex::new(config.clone()),
            layout,
            exe: options.exe,
            slots: tokio::sync::Mutex::new(BTreeMap::new()),
            started: brokapi::runtime::now(),
            started_unix,
            address: address.clone(),
            shutdown,
        });
        {
            let mut slots = inner.slots.lock().await;
            for spec in &config.gateways {
                let slot = Inner::make_slot(&inner, spec.clone(), spec.autostart);
                slots.insert(spec.name.clone(), slot);
            }
        }

        let access = Access::new(&key);
        let service = pb::service_server::ServiceServer::new(Api {
            inner: Arc::clone(&inner),
        });
        let service = tonic::service::interceptor::InterceptedService::new(service, move |req| {
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
                    tracing::error!("the service API ended with an error: {e}");
                }
            }),
            BoundSocket::Tcp(listener) => tokio::spawn(async move {
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                if let Err(e) = router.serve_with_incoming_shutdown(incoming, signal).await {
                    tracing::error!("the service API ended with an error: {e}");
                }
            }),
        };
        let monitor = tokio::spawn(monitor_usage(Arc::clone(&inner)));
        tracing::info!(
            pid = me,
            config = %config.path.display(),
            address = %address,
            gateways = config.gateways.len(),
            "the service runs"
        );
        Ok(Self {
            inner,
            key,
            stop_server: Some(stop_server),
            server,
            monitor,
        })
    }

    /// Where the service listens.
    pub fn address(&self) -> &Address {
        &self.inner.address
    }

    /// The access key of the service: the contents of `service.key`.
    pub fn access_key(&self) -> &str {
        &self.key
    }

    /// Waits until a `Shutdown` call comes. Returns whether the gateways are to be left running.
    pub async fn shutdown_requested(&self) -> bool {
        let mut rx = self.inner.shutdown.subscribe();
        match rx.wait_for(|v| v.is_some()).await {
            Ok(v) => v.unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Stops the service. The gateways are stopped too, unless `leave_gateways`: then their processes go on and a service
    /// started later takes them over.
    pub async fn stop(mut self, leave_gateways: bool) {
        let slots: Vec<Arc<Slot>> = {
            let mut all = self.inner.slots.lock().await;
            std::mem::take(&mut *all).into_values().collect()
        };
        for slot in &slots {
            let _ = slot.tx.send(Cmd::Quit {
                leave: leave_gateways,
            });
        }
        for slot in &slots {
            let task = lock(&slot.task).take();
            if let Some(task) = task {
                let _ = task.await;
            }
        }
        self.monitor.abort();
        if let Some(stop) = self.stop_server.take() {
            let _ = stop.send(());
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut self.server).await;
        self.cleanup();
        tracing::info!("the service stopped");
    }

    /// Ends the service the way a crash would: nothing is cleaned up and no gateway is touched. For tests of the takeover.
    pub async fn abandon(self) {
        self.monitor.abort();
        self.server.abort();
        let slots: Vec<Arc<Slot>> = self.inner.slots.lock().await.values().cloned().collect();
        for slot in slots {
            if let Some(task) = lock(&slot.task).take() {
                task.abort();
            }
        }
    }

    fn cleanup(&self) {
        let dir = &self.inner.layout.root;
        if let Address::Unix(path) = &self.inner.address {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_file(dir.join(ADDRESS_FILE));
        let _ = std::fs::remove_file(self.inner.layout.service_pid());
        registry::unregister(dir, std::process::id());
    }
}

/// Reads the CPU and the memory of the gateway processes at the rhythm of the checks.
async fn monitor_usage(inner: Arc<Inner>) {
    let mut usage = proc::Usage::new();
    let every = lock(&inner.config)
        .service
        .check_interval
        .max(Duration::from_millis(250));
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let slots: Vec<Arc<Slot>> = inner.slots.lock().await.values().cloned().collect();
        let pids: Vec<u32> = slots.iter().filter_map(|s| lock(&s.status).pid).collect();
        if pids.is_empty() {
            continue;
        }
        let read = usage.read(&pids);
        for slot in slots {
            let mut st = lock(&slot.status);
            if let Some((cpu, mem)) = st.pid.and_then(|p| read.get(&p)) {
                st.cpu = *cpu;
                st.memory = *mem;
            }
        }
    }
}

impl Inner {
    pub(crate) fn make_slot(inner: &Arc<Inner>, spec: GatewaySpec, start: bool) -> Arc<Slot> {
        let (tx, rx) = mpsc::unbounded_channel();
        let slot = Arc::new(Slot {
            name: spec.name.clone(),
            spec: Mutex::new(spec),
            status: Mutex::new(Status {
                state: GatewayState::Stopped,
                pid: None,
                started: None,
                restarts: 0,
                last_error: String::new(),
                restart_at: None,
                adopted: false,
                health: None,
                cpu: 0.0,
                memory: 0,
            }),
            tx,
            task: Mutex::new(None),
        });
        let task = tokio::spawn(run_slot(Arc::clone(&slot), Arc::clone(inner), rx, start));
        *lock(&slot.task) = Some(task);
        slot
    }

    /// The gateway's word on itself, from a connection made for the purpose.
    pub(crate) async fn fresh_health(&self, name: &str) -> Option<gw::HealthResponse> {
        let mut cache = None;
        fetch_health(&self.layout, name, &mut cache).await
    }
}

impl Slot {
    async fn ask(&self, make: impl FnOnce(oneshot::Sender<()>) -> Cmd) -> bool {
        let (tx, rx) = oneshot::channel();
        self.tx.send(make(tx)).is_ok() && rx.await.is_ok()
    }

    /// Starts the gateway if it is stopped or failed. Returns when the process is started (not when it is ready).
    pub(crate) async fn start(&self) -> bool {
        self.ask(Cmd::Start).await
    }

    /// Stops the gateway and starts it again.
    pub(crate) async fn restart(&self) -> bool {
        self.ask(Cmd::Restart).await
    }

    /// Stops the gateway and keeps it stopped.
    pub(crate) async fn stop(&self) -> bool {
        self.ask(Cmd::Stop).await
    }

    /// What the service knows of the gateway, with the health it was given (the caller fetches it fresh).
    pub(crate) fn info(
        &self,
        health: Option<gw::HealthResponse>,
        layout: &Layout,
    ) -> pb::GatewayInfo {
        let spec = lock(&self.spec).clone();
        let st = lock(&self.status);
        let running = matches!(st.state, GatewayState::Starting | GatewayState::Running);
        let health = health.or_else(|| st.health.clone().filter(|_| running));
        pb::GatewayInfo {
            name: self.name.clone(),
            broker: spec.broker.clone(),
            state: st.state.as_str().to_owned(),
            pid: st.pid.unwrap_or(0),
            uptime_ns: match (st.started, running) {
                (Some(s), true) => {
                    crate::metrics::nanos(brokapi::runtime::now().saturating_duration_since(s))
                }
                _ => 0,
            },
            restarts: st.restarts,
            last_error: st.last_error.clone(),
            restart_in_ns: st.restart_at.map_or(0, |at| {
                crate::metrics::nanos(at.saturating_duration_since(tokio::time::Instant::now()))
            }),
            cpu_percent: if running { st.cpu } else { 0.0 },
            memory_bytes: if running { st.memory } else { 0 },
            account: spec.account.clone().unwrap_or_default(),
            sandbox: spec.sandbox,
            adopted: st.adopted,
            address: if running {
                std::fs::read_to_string(layout.gateway_dir(&self.name).join(crate::ADDRESS_FILE))
                    .unwrap_or_default()
            } else {
                String::new()
            },
            state_dir: layout.gateway_dir(&self.name).display().to_string(),
            health,
        }
    }

    pub(crate) fn state(&self) -> GatewayState {
        lock(&self.status).state
    }
}

impl Slot {
    pub(crate) fn quit_now(&self, leave: bool) {
        let _ = self.tx.send(Cmd::Quit { leave });
    }

    pub(crate) async fn join(&self) {
        let task = lock(&self.task).take();
        if let Some(t) = task {
            let _ = t.await;
        }
    }

    /// Replaces the description of the gateway (after a reload).
    pub(crate) fn set_spec(&self, spec: GatewaySpec) {
        *lock(&self.spec) = spec;
    }
}

// ---------------------------------------------------------------------------------------------------- one gateway

enum Handle {
    Child(tokio::process::Child, u32),
    /// A process of an earlier service, found running.
    Adopted(u32),
}

fn describe(status: std::process::ExitStatus) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return format!("killed by signal {sig}");
        }
    }
    match status.code() {
        Some(code) => format!("exited with code {code}"),
        None => "exited".to_owned(),
    }
}

impl Handle {
    fn pid(&self) -> u32 {
        match self {
            Handle::Child(_, pid) | Handle::Adopted(pid) => *pid,
        }
    }

    /// Waits until the process is gone and says how it went.
    async fn wait(&mut self, poll: Duration) -> String {
        match self {
            Handle::Child(child, _) => match child.wait().await {
                Ok(status) => describe(status),
                Err(e) => format!("cannot wait for the process: {e}"),
            },
            Handle::Adopted(pid) => {
                let pid = *pid;
                loop {
                    if !proc::is_alive(pid) {
                        return "the process is gone".to_owned();
                    }
                    tokio::time::sleep(poll).await;
                }
            }
        }
    }
}

enum Phase {
    Idle,
    Failed,
    Backoff(tokio::time::Instant),
    Running(Handle),
}

struct Runner {
    slot: Arc<Slot>,
    inner: Arc<Inner>,
    restarts: Restarts,
    cache: Option<RemoteClient>,
}

async fn fetch_health(
    layout: &Layout,
    name: &str,
    cache: &mut Option<RemoteClient>,
) -> Option<gw::HealthResponse> {
    if cache.is_none() {
        *cache = RemoteClient::connect_dir(&layout.gateway_dir(name), "brokapi-service")
            .await
            .ok();
    }
    let client = cache.as_ref()?;
    match tokio::time::timeout(Duration::from_secs(2), client.health()).await {
        Ok(Ok(h)) => Some(h),
        _ => {
            *cache = None;
            None
        }
    }
}

impl Runner {
    fn status<R>(&self, f: impl FnOnce(&mut Status) -> R) -> R {
        f(&mut lock(&self.slot.status))
    }

    /// Takes over a process of an earlier service, if one runs.
    async fn adopt(&mut self) -> Option<Handle> {
        let pid_file = self.inner.layout.gateway_pid(&self.slot.name);
        let pid = proc::read_pid(&pid_file)?;
        if !proc::is_brokapi(pid) {
            let _ = std::fs::remove_file(&pid_file);
            return None;
        }
        match fetch_health(&self.inner.layout, &self.slot.name, &mut self.cache).await {
            Some(h) => {
                tracing::info!(gateway = %self.slot.name, pid, "taken over a running gateway");
                self.status(|s| {
                    s.pid = Some(pid);
                    s.adopted = true;
                    s.started =
                        brokapi::runtime::now().checked_sub(Duration::from_nanos(h.uptime_ns));
                    s.state = state_of(&h);
                    s.health = Some(h);
                });
                Some(Handle::Adopted(pid))
            }
            None => {
                // A process that is ours by name but does not answer would keep the socket: end it.
                tracing::warn!(gateway = %self.slot.name, pid, "a gateway process does not answer, ending it");
                proc::kill(pid);
                let _ = std::fs::remove_file(&pid_file);
                None
            }
        }
    }

    async fn launch(&mut self) -> Phase {
        let config_path = lock(&self.inner.config).path.clone();
        let mut cmd = tokio::process::Command::new(&self.inner.exe);
        cmd.arg("-c")
            .arg(&config_path)
            .arg("gateway")
            .arg("run")
            .arg(&self.slot.name)
            .arg("--log-file")
            .arg(self.inner.layout.gateway_log(&self.slot.name));
        proc::detach(&mut cmd);
        let spawned = cmd.spawn().and_then(|child| {
            child
                .id()
                .map(|pid| (child, pid))
                .ok_or_else(|| std::io::Error::other("the process ended at once"))
        });
        match spawned {
            Ok((child, pid)) => {
                tracing::info!(gateway = %self.slot.name, pid, "gateway started");
                self.cache = None;
                self.status(|s| {
                    s.state = GatewayState::Starting;
                    s.pid = Some(pid);
                    s.started = Some(brokapi::runtime::now());
                    s.restart_at = None;
                    s.adopted = false;
                    s.health = None;
                });
                Phase::Running(Handle::Child(child, pid))
            }
            Err(e) => self.ended(format!("cannot start {}: {e}", self.inner.exe.display())),
        }
    }

    /// The process ended on its own.
    fn ended(&mut self, why: String) -> Phase {
        let detail = self.last_log_problem();
        let why = if detail.is_empty() {
            why
        } else {
            format!("{why}: {detail}")
        };
        tracing::warn!(gateway = %self.slot.name, "the gateway stopped: {why}");
        self.cache = None;
        let next = self.restarts.on_exit(brokapi::runtime::now());
        let total = self.restarts.total();
        self.status(|s| {
            s.pid = None;
            s.started = None;
            s.health = None;
            s.adopted = false;
            s.last_error = why;
            s.restarts = total;
        });
        match next {
            Next::After(pause) => {
                let at = tokio::time::Instant::now() + pause;
                self.status(|s| {
                    s.state = GatewayState::Restarting;
                    s.restart_at = Some(at);
                });
                Phase::Backoff(at)
            }
            Next::GiveUp => {
                tracing::error!(gateway = %self.slot.name, "too many restarts: the gateway is failed, `brokapi start` starts it");
                self.status(|s| {
                    s.state = GatewayState::Failed;
                    s.restart_at = None;
                });
                Phase::Failed
            }
        }
    }

    /// The last line of the gateway's log that says what was wrong, for `last_error`.
    fn last_log_problem(&self) -> String {
        let path = self.inner.layout.gateway_log(&self.slot.name);
        let lines = logs::tail_lines(&path, 30).unwrap_or_default();
        lines
            .iter()
            .rev()
            .find(|l| l.contains("ERROR") || l.contains("WARN"))
            .or(lines.last())
            .map(|l| {
                // "2026-10-23T10:00:00.123456Z ERROR text" -> "text"
                let text = l.splitn(3, ' ').nth(2).unwrap_or(l);
                text.trim().to_owned()
            })
            .unwrap_or_default()
    }

    async fn halt(&mut self, mut h: Handle) {
        self.status(|s| s.state = GatewayState::Stopping);
        let (stop_timeout, poll) = {
            let c = lock(&self.inner.config);
            (c.service.stop_timeout, c.service.check_interval)
        };
        let pid = h.pid();
        proc::terminate(pid);
        let gone =
            tokio::time::timeout(stop_timeout, h.wait(poll.min(Duration::from_millis(100)))).await;
        if gone.is_err() {
            tracing::warn!(gateway = %self.slot.name, pid, "the gateway does not stop, ending it");
            proc::kill(pid);
            if let Handle::Child(child, _) = &mut h {
                let _ = child.wait().await;
            }
        }
        self.cache = None;
        let _ = std::fs::remove_file(self.inner.layout.gateway_pid(&self.slot.name));
        self.status(|s| {
            s.state = GatewayState::Stopped;
            s.pid = None;
            s.started = None;
            s.health = None;
            s.restart_at = None;
            s.adopted = false;
            s.cpu = 0.0;
            s.memory = 0;
        });
        tracing::info!(gateway = %self.slot.name, pid, "gateway stopped");
    }

    async fn look(&mut self) {
        if let Some(h) = fetch_health(&self.inner.layout, &self.slot.name, &mut self.cache).await {
            self.status(|s| {
                if matches!(s.state, GatewayState::Starting | GatewayState::Running) {
                    s.state = state_of(&h);
                }
                s.health = Some(h);
            });
        } else {
            self.status(|s| {
                if s.state == GatewayState::Running {
                    s.state = GatewayState::Starting;
                }
            });
        }
    }
}

fn state_of(h: &gw::HealthResponse) -> GatewayState {
    match gw::GatewayState::try_from(h.state) {
        Ok(gw::GatewayState::Ready) => GatewayState::Running,
        Ok(gw::GatewayState::Stopping) => GatewayState::Stopping,
        _ => GatewayState::Starting,
    }
}

async fn run_slot(
    slot: Arc<Slot>,
    inner: Arc<Inner>,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
    start: bool,
) {
    let (policy, every) = {
        let c = lock(&inner.config);
        (c.service.restart.clone(), c.service.check_interval)
    };
    let mut r = Runner {
        slot,
        inner,
        restarts: Restarts::new(policy),
        cache: None,
    };
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut phase = match r.adopt().await {
        Some(h) => Phase::Running(h),
        None if start => r.launch().await,
        None => Phase::Idle,
    };
    loop {
        phase = match phase {
            Phase::Idle | Phase::Failed => match rx.recv().await {
                None | Some(Cmd::Quit { .. }) => break,
                Some(Cmd::Start(reply) | Cmd::Restart(reply)) => {
                    r.restarts.reset();
                    let next = r.launch().await;
                    let _ = reply.send(());
                    next
                }
                Some(Cmd::Stop(reply)) => {
                    r.status(|s| s.state = GatewayState::Stopped);
                    let _ = reply.send(());
                    Phase::Idle
                }
            },
            Phase::Backoff(until) => tokio::select! {
                () = tokio::time::sleep_until(until) => r.launch().await,
                cmd = rx.recv() => match cmd {
                    None | Some(Cmd::Quit { .. }) => break,
                    Some(Cmd::Start(reply) | Cmd::Restart(reply)) => {
                        r.restarts.reset();
                        let next = r.launch().await;
                        let _ = reply.send(());
                        next
                    }
                    Some(Cmd::Stop(reply)) => {
                        r.status(|s| {
                            s.state = GatewayState::Stopped;
                            s.restart_at = None;
                        });
                        let _ = reply.send(());
                        Phase::Idle
                    }
                },
            },
            Phase::Running(mut h) => tokio::select! {
                why = h.wait(every) => r.ended(why),
                cmd = rx.recv() => match cmd {
                    None => { r.halt(h).await; break }
                    Some(Cmd::Quit { leave }) => {
                        if leave {
                            // The process goes on. Its handle is let go without waiting for it.
                            r.status(|s| s.state = GatewayState::Stopped);
                        } else {
                            r.halt(h).await;
                        }
                        break;
                    }
                    Some(Cmd::Stop(reply)) => {
                        r.halt(h).await;
                        let _ = reply.send(());
                        Phase::Idle
                    }
                    Some(Cmd::Restart(reply)) => {
                        r.halt(h).await;
                        r.restarts.reset();
                        let next = r.launch().await;
                        let _ = reply.send(());
                        next
                    }
                    Some(Cmd::Start(reply)) => {
                        let _ = reply.send(());
                        Phase::Running(h)
                    }
                },
                _ = tick.tick() => {
                    r.look().await;
                    Phase::Running(h)
                },
            },
        };
    }
}
