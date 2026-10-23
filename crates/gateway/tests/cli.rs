//! The program `brokapi` as a process: the service, its gateway processes, and every command. The brokers are the
//! imitations of `brokapi-mocks` in this test process, reached over localhost. No network, no money.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use brokapi::core::{Error, Event, OrderExecutor};
use brokapi_client::Broker;
use brokapi_gateway::service::{config::Config, proc};
use brokapi_gateway::{RemoteClient, RemoteEvent, pb};
use common::Mock;
use serde_json::Value;

const EXE: &str = env!("CARGO_BIN_EXE_brokapi");

/// A config file, its state directory and the machine registry of one test.
struct Env {
    dir: tempfile::TempDir,
    registry: PathBuf,
    config: PathBuf,
    state: PathBuf,
}

struct Out {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Out {
    fn json(&self) -> Value {
        assert_eq!(self.code, 0, "stderr: {}", self.stderr);
        serde_json::from_str(&self.stdout)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}", self.stdout))
    }
}

/// The `[[gateway]]` section for a gateway on an imitation. The token goes in a file next to the config.
fn gateway_section(
    name: &str,
    broker: Broker,
    mock: &Mock,
    token_file: &Path,
    extra: &str,
) -> String {
    let mut settings = mock.settings().0;
    let (token_key, account_key) = match broker {
        Broker::Bcs => ("refresh_token", None),
        Broker::Alor => ("refresh_token", Some("portfolio")),
        Broker::Tbank => ("token", Some("account_id")),
        Broker::Finam => ("secret", Some("account_id")),
    };
    let token = settings
        .remove(token_key)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    std::fs::write(token_file, token).unwrap();
    let mut out = format!(
        "[[gateway]]\nname = {name:?}\nbroker = {:?}\ntoken_file = {:?}\n",
        broker.as_str(),
        token_file.display().to_string()
    );
    out.push_str(extra);
    if let Some(key) = account_key
        && let Some(account) = settings.remove(key)
    {
        out.push_str(&format!("account = {account}\n"));
    }
    if settings.remove("sandbox").is_some() {
        out.push_str("sandbox = true\n");
    }
    out.push_str("[gateway.settings]\n");
    for (key, value) in settings {
        out.push_str(&format!("{key} = {value}\n"));
    }
    if broker == Broker::Bcs {
        // Room for the orders a test places, and the limits section of the config in use.
        out.push_str("[gateway.limits.place]\nrate = 1000\nper = \"1s\"\nburst = 1000\n");
    }
    out
}

/// The service section: quick checks and short pauses, so that a test does not wait for the defaults.
const SERVICE: &str = r#"
state_dir = "state"
[service]
check_interval = "100ms"
stop_timeout = "5s"
[service.restart]
initial = "50ms"
max_pause = "200ms"
max_restarts = 3
window = "60s"
"#;

impl Env {
    fn new(gateways: &[(&str, Broker, &Mock)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = dir.path().join("registry");
        Self::in_dir(dir, registry, gateways)
    }

    fn in_dir(
        dir: tempfile::TempDir,
        registry: PathBuf,
        gateways: &[(&str, Broker, &Mock)],
    ) -> Self {
        let config = dir.path().join("svc.toml");
        let state = dir.path().join("state");
        let env = Self {
            dir,
            registry,
            config,
            state,
        };
        env.write_config(gateways, "");
        env
    }

    fn write_config(&self, gateways: &[(&str, Broker, &Mock)], extra: &str) {
        let mut text = String::from(SERVICE);
        for (name, broker, mock) in gateways {
            let token = self.dir.path().join(format!("{name}.token"));
            text.push_str(&gateway_section(name, *broker, mock, &token, extra));
        }
        std::fs::write(&self.config, text).unwrap();
    }

    fn gateway_dir(&self, name: &str) -> PathBuf {
        self.state.join("gateways").join(name)
    }

    async fn run(&self, args: &[&str]) -> Out {
        self.run_with(&["-c", self.config.to_str().unwrap()], args)
            .await
    }

    async fn run_with(&self, prefix: &[&str], args: &[&str]) -> Out {
        let all: Vec<String> = prefix.iter().chain(args).map(|s| (*s).to_owned()).collect();
        let (registry, cwd) = (self.registry.clone(), self.dir.path().to_owned());
        tokio::task::spawn_blocking(move || {
            let output = Command::new(EXE)
                .args(&all)
                .env("BROKAPI_REGISTRY_DIR", registry)
                .env_remove("BROKAPI_CONFIG")
                .env_remove("BROKAPI_LOG")
                .current_dir(cwd)
                .output()
                .expect("the program runs");
            Out {
                code: output.status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            }
        })
        .await
        .unwrap()
    }

    /// `brokapi <args> --json`, which must succeed.
    async fn json(&self, args: &[&str]) -> Value {
        let mut all = args.to_vec();
        all.push("--json");
        self.run(&all).await.json()
    }

    async fn up(&self) {
        let out = self.run(&["up", "-d"]).await;
        assert_eq!(out.code, 0, "up -d: {}{}", out.stdout, out.stderr);
    }

    async fn ps(&self) -> Vec<Value> {
        self.json(&["ps"]).await.as_array().unwrap().clone()
    }

    async fn gateway(&self, name: &str) -> Option<Value> {
        let out = self.run(&["ps", "--json"]).await;
        if out.code != 0 {
            return None;
        }
        out.json()
            .as_array()?
            .iter()
            .find(|g| g["name"] == name)
            .cloned()
    }

    async fn wait_state(&self, name: &str, state: &str) -> Value {
        let found = std::sync::Mutex::new(Value::Null);
        brokapi_mocks::time::wait_until_async(&format!("{name} is {state}"), || async {
            match self.gateway(name).await {
                Some(g) if g["state"] == state => {
                    *found.lock().unwrap() = g;
                    true
                }
                _ => false,
            }
        })
        .await;
        found.into_inner().unwrap()
    }

    async fn wait_all_running(&self, names: &[&str]) {
        for name in names {
            self.wait_state(name, "running").await;
        }
    }

    async fn bot(&self, gateway: &str, name: &str) -> RemoteClient {
        RemoteClient::connect_dir(&self.gateway_dir(gateway), name)
            .await
            .expect("a bot connects to the gateway")
    }

    /// A service that is stopped when the test ends, however it ends.
    fn guard(&self) -> Guard<'_> {
        Guard(self)
    }
}

struct Guard<'a>(&'a Env);

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let env = self.0;
        let _ = Command::new(EXE)
            .args(["-c", env.config.to_str().unwrap(), "down"])
            .env("BROKAPI_REGISTRY_DIR", &env.registry)
            .output();
        // Whatever is left of a test that failed half way.
        if let Ok(dirs) = std::fs::read_dir(env.state.join("gateways")) {
            for d in dirs.flatten() {
                if let Some(pid) = proc::read_pid(&d.path().join("gateway.pid")) {
                    proc::kill(pid);
                }
            }
        }
        if let Some(pid) = proc::read_pid(&env.state.join("service.pid")) {
            proc::kill(pid);
        }
    }
}

/// Places a limit order of one lot, again when the limiter of the gateway says wait: the gateway keeps the limits of the
/// broker, and a test that places many orders in a moment meets them.
async fn place(bot: &RemoteClient, price: &str) -> brokapi::core::ClientOrderId {
    let request = common::bcs_order(price);
    let cid = request.cid;
    loop {
        match bot.place(request.clone()).await {
            Ok(_) => return cid,
            Err(Error::RateLimited { retry_after }) => tokio::time::sleep(retry_after).await,
            Err(e) => panic!("place: {e}"),
        }
    }
}

fn pid_of(g: &Value) -> u32 {
    g["pid"].as_u64().expect("a process id") as u32
}

// ---------------------------------------------------------------------------------------------------- the lifecycle

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_service_runs_the_gateways_and_the_commands_see_them() {
    let (bcs, tbank) = (
        Mock::start(Broker::Bcs).await,
        Mock::start(Broker::Tbank).await,
    );
    let env = Env::new(&[
        ("bcs-main", Broker::Bcs, &bcs),
        ("tb", Broker::Tbank, &tbank),
    ]);
    let _guard = env.guard();

    // Before anything runs: the config is fine, and there is no service to talk to.
    let checked = env.json(&["check"]).await;
    assert_eq!(checked["ok"], true);
    assert!(
        checked["gateways"]
            .as_array()
            .unwrap()
            .iter()
            .all(|g| g["access"] == "ok")
    );
    let none = env.run(&["ps"]).await;
    assert_eq!(none.code, 3, "{}", none.stderr);
    assert!(
        none.stderr.contains("no service is running") && none.stderr.contains("brokapi"),
        "{}",
        none.stderr
    );

    env.up().await;
    env.wait_all_running(&["bcs-main", "tb"]).await;
    let ps = env.ps().await;
    assert_eq!(ps.len(), 2);
    assert!(ps.iter().all(|g| g["pid"].is_u64() && g["restarts"] == 0));
    let text = env.run(&["ps"]).await.stdout;
    assert!(
        text.starts_with("NAME") && text.contains("bcs-main") && text.contains("running"),
        "{text}"
    );

    // The machine knows the service.
    let services = env.json(&["services"]).await;
    assert_eq!(services.as_array().unwrap().len(), 1);
    assert_eq!(services[0]["gateways"], 2);
    assert_eq!(services[0]["running"], 2);

    // Everything about one gateway.
    let status = env.json(&["status", "bcs-main"]).await;
    assert_eq!(status["health"]["state"], "ready");
    assert!(
        status["health"]["limits"]
            .as_array()
            .is_some_and(|l| !l.is_empty())
    );
    assert!(
        status["health"]["subscriptions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["owners"][0] == "(gateway)"),
        "the gateway holds its own subscription of the states of orders"
    );
    let text = env.run(&["status", "bcs-main"]).await.stdout;
    assert!(
        text.contains("state") && text.contains("rate limit of the broker"),
        "{text}"
    );

    // One gateway stops, the other works on.
    let before = pid_of(&env.gateway("tb").await.unwrap());
    let out = env.run(&["stop", "tb"]).await;
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(env.gateway("tb").await.unwrap()["state"], "stopped");
    assert!(!proc::is_alive(before), "the process is gone");
    assert!(
        RemoteClient::connect_dir(&env.gateway_dir("tb"), "x")
            .await
            .is_err()
    );
    assert_eq!(env.gateway("bcs-main").await.unwrap()["state"], "running");
    let out = env.run(&["start", "tb", "--wait"]).await;
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(env.gateway("tb").await.unwrap()["state"], "running");

    // Restart gives a new process.
    let old = pid_of(&env.gateway("bcs-main").await.unwrap());
    let out = env.run(&["restart", "bcs-main", "--wait"]).await;
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_ne!(pid_of(&env.gateway("bcs-main").await.unwrap()), old);

    // Down stops the service and its gateways, and leaves nothing behind.
    let pids: Vec<u32> = env.ps().await.iter().map(pid_of).collect();
    let out = env.run(&["down"]).await;
    assert_eq!(out.code, 0, "{}", out.stderr);
    for pid in pids {
        assert!(!proc::is_alive(pid), "gateway {pid} is gone");
    }
    assert!(env.json(&["services"]).await.as_array().unwrap().is_empty());
    assert_eq!(env.run(&["ps"]).await.code, 3);
    assert!(!env.state.join("service.addr").exists());
    assert_eq!(env.run(&["down"]).await.code, 3, "nothing to stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_killed_gateway_is_started_again_checks_its_journal_and_the_bot_catches_up() {
    let bcs = Mock::start(Broker::Bcs).await;
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();
    env.up().await;
    env.wait_all_running(&["bcs-main"]).await;

    // A bot has an order open and has seen events.
    let bot = env.bot("bcs-main", "bot-1").await;
    let mut stream = bot.events(None, false).await.unwrap();
    let request = common::bcs_order("1.0");
    let cid = request.cid;
    bot.place(request).await.expect("place");
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await
            .expect("an event comes")
        {
            Some(Ok(RemoteEvent::Event { envelope, .. }))
                if matches!(envelope.payload, Event::Order(_)) =>
            {
                break;
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    let resume = stream.resume_point();
    assert!(resume.last_seq > 0);

    // The process of the gateway dies.
    let first = env.gateway("bcs-main").await.unwrap();
    assert!(proc::kill(pid_of(&first)));
    let again = env.wait_state("bcs-main", "running").await;
    assert_ne!(pid_of(&again), pid_of(&first), "a new process");
    assert_eq!(again["restarts"], 1);
    assert!(
        again["last_error"].as_str().unwrap().contains("signal 9")
            || again["last_error"].as_str().unwrap().contains("exited"),
        "{again}"
    );

    // It compared its journal with the broker before it took commands: the order is known and still open.
    assert!(again["health"]["last_seq"].as_u64().is_some());
    let bot = env.bot("bcs-main", "bot-1").await;
    let health = bot.health().await.unwrap();
    assert_eq!(health.open_orders, 1);
    assert_ne!(health.epoch, resume.epoch);

    // The bot comes back with its number. The numbers began again, so it is told to reconcile, and it does.
    let mut stream = bot.events(Some(resume), false).await.unwrap();
    match stream.next().await {
        Some(Ok(RemoteEvent::Gap(gap))) => assert_eq!(gap.reason, pb::gap::Reason::NewEpoch as i32),
        other => panic!("expected a gap, got {other:?}"),
    }
    let report = bot.reconcile(&[cid]).await.unwrap();
    assert!(report.confirmed.iter().any(|o| o.cid == cid), "{report:?}");

    // And it gets what happens next.
    bcs.fill(cid).await;
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await
            .expect("an event comes")
        {
            Some(Ok(RemoteEvent::Event { envelope, .. })) => {
                if let Event::Order(o) = envelope.payload
                    && o.cid == cid
                    && o.state.is_final()
                {
                    break;
                }
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    brokapi_mocks::time::wait_until_async("the journal is closed", || async {
        bot.health()
            .await
            .map(|h| h.open_orders == 0)
            .unwrap_or(false)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_services_with_two_configs_run_side_by_side() {
    let (bcs, tbank) = (
        Mock::start(Broker::Bcs).await,
        Mock::start(Broker::Tbank).await,
    );
    let shared = tempfile::tempdir().unwrap();
    let registry = shared.path().join("registry");
    let prod = Env::in_dir(
        tempfile::tempdir().unwrap(),
        registry.clone(),
        &[("prod", Broker::Bcs, &bcs)],
    );
    let test = Env::in_dir(
        tempfile::tempdir().unwrap(),
        registry,
        &[("sandbox", Broker::Tbank, &tbank)],
    );
    let (_a, _b) = (prod.guard(), test.guard());
    prod.up().await;
    test.up().await;
    prod.wait_all_running(&["prod"]).await;
    test.wait_all_running(&["sandbox"]).await;

    // Each sees its own gateways only, and the machine sees both services.
    assert_eq!(prod.ps().await.len(), 1);
    assert_eq!(test.ps().await[0]["name"], "sandbox");
    assert_eq!(prod.run(&["status", "sandbox"]).await.code, 4);
    let services = prod.json(&["services"]).await;
    assert_eq!(services.as_array().unwrap().len(), 2);

    // A third config on the state directory of the first is refused.
    let clash = prod.dir.path().join("clash.toml");
    std::fs::write(&clash, std::fs::read_to_string(&prod.config).unwrap()).unwrap();
    let out = prod
        .run_with(&["-c", clash.to_str().unwrap()], &["up", "-d"])
        .await;
    assert_eq!(out.code, 1);
    assert!(
        out.stderr.contains("already runs") || out.stderr.contains("used by another service"),
        "{}",
        out.stderr
    );
    // Nor can the same config be started twice.
    assert_eq!(prod.run(&["up", "-d"]).await.code, 1);

    // Stopping one leaves the other.
    assert_eq!(prod.run(&["down"]).await.code, 0);
    assert_eq!(prod.run(&["ps"]).await.code, 3);
    assert_eq!(test.gateway("sandbox").await.unwrap()["state"], "running");
    assert_eq!(test.json(&["services"]).await.as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_that_keeps_stopping_is_marked_failed_and_start_brings_it_back() {
    let bcs = Mock::start(Broker::Bcs).await;
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();
    // The token is gone: the gateway process cannot start, however often it is tried.
    let token = env.dir.path().join("bcs-main.token");
    let saved = std::fs::read_to_string(&token).unwrap();
    std::fs::remove_file(&token).unwrap();
    env.up().await;
    let failed = env.wait_state("bcs-main", "failed").await;
    assert_eq!(failed["restarts"], 3, "{failed}");
    assert!(
        failed["last_error"]
            .as_str()
            .unwrap()
            .contains("token file"),
        "{failed}"
    );
    assert!(failed["pid"].is_null());
    // It stays failed: nothing starts it by itself.
    let ps = env.run(&["ps"]).await.stdout;
    assert!(ps.contains("failed") && ps.contains("token file"), "{ps}");

    std::fs::write(&token, saved).unwrap();
    let out = env.run(&["start", "bcs-main", "--wait"]).await;
    assert_eq!(out.code, 0, "{}{}", out.stdout, out.stderr);
    assert_eq!(env.gateway("bcs-main").await.unwrap()["state"], "running");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_the_service_dies_the_gateways_live_and_a_new_service_takes_them_over() {
    let (bcs, tbank) = (
        Mock::start(Broker::Bcs).await,
        Mock::start(Broker::Tbank).await,
    );
    let env = Env::new(&[
        ("bcs-main", Broker::Bcs, &bcs),
        ("tb", Broker::Tbank, &tbank),
    ]);
    let _guard = env.guard();
    env.up().await;
    env.wait_all_running(&["bcs-main", "tb"]).await;
    let gateways: Vec<(String, u32)> = env
        .ps()
        .await
        .iter()
        .map(|g| (g["name"].as_str().unwrap().to_owned(), pid_of(g)))
        .collect();
    let service_pid = env.json(&["services"]).await[0]["pid"].as_u64().unwrap() as u32;

    // The service is killed, not stopped.
    assert!(proc::kill(service_pid));
    brokapi_mocks::time::wait_until("the service is gone", || !proc::is_alive(service_pid)).await;
    for (_, pid) in &gateways {
        assert!(proc::is_alive(*pid), "the gateway {pid} lives on");
    }
    // A bot does not notice: the gateway answers.
    let bot = env.bot("bcs-main", "bot-1").await;
    assert_eq!(
        bot.health().await.unwrap().state,
        pb::GatewayState::Ready as i32
    );
    assert_eq!(
        env.run(&["ps"]).await.code,
        3,
        "the commands see that there is no service"
    );
    assert!(
        env.json(&["services"]).await.as_array().unwrap().is_empty(),
        "the dead one is not listed"
    );

    // A new service finds the processes and takes them over instead of starting new ones.
    env.up().await;
    for (name, pid) in &gateways {
        let g = env.wait_state(name, "running").await;
        assert_eq!(pid_of(&g), *pid, "{name}: the same process");
        assert_eq!(g["adopted"], true);
        assert_eq!(g["restarts"], 0);
    }
    assert_eq!(
        bot.health().await.unwrap().state,
        pb::GatewayState::Ready as i32
    );

    // And a service that is told to leave the gateways does.
    let out = env.run(&["down", "--leave-gateways"]).await;
    assert_eq!(out.code, 0, "{}", out.stderr);
    for (_, pid) in &gateways {
        assert!(proc::is_alive(*pid));
    }
    env.up().await;
    env.wait_all_running(&["bcs-main", "tb"]).await;
    assert_eq!(pid_of(&env.gateway("tb").await.unwrap()), gateways[1].1);
}

// ---------------------------------------------------------------------------------------------------- the commands

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_remove_and_reload_change_the_gateways_without_stopping_the_service() {
    let (bcs, tbank) = (
        Mock::start(Broker::Bcs).await,
        Mock::start(Broker::Tbank).await,
    );
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();
    env.up().await;
    env.wait_all_running(&["bcs-main"]).await;
    let first = pid_of(&env.gateway("bcs-main").await.unwrap());

    // A gateway is added to the file; the service learns of it on `add`.
    env.write_config(
        &[
            ("bcs-main", Broker::Bcs, &bcs),
            ("tb", Broker::Tbank, &tbank),
        ],
        "",
    );
    let out = env.json(&["add", "tb"]).await;
    assert_eq!(out["name"], "tb");
    env.wait_all_running(&["tb"]).await;
    assert_eq!(env.run(&["add", "tb"]).await.code, 1, "it is there already");
    assert_eq!(env.run(&["add", "nope"]).await.code, 4);
    assert_eq!(
        pid_of(&env.gateway("bcs-main").await.unwrap()),
        first,
        "the first one was not touched"
    );

    // A section changes: reload restarts that gateway only.
    env.write_config(
        &[
            ("bcs-main", Broker::Bcs, &bcs),
            ("tb", Broker::Tbank, &tbank),
        ],
        "bot_buffer = 77\n",
    );
    let r = env.json(&["reload"]).await;
    assert_eq!(
        r["restarted"].as_array().unwrap().len(),
        2,
        "both sections changed: {r}"
    );
    env.wait_all_running(&["bcs-main", "tb"]).await;
    assert_ne!(pid_of(&env.gateway("bcs-main").await.unwrap()), first);
    let r = env.json(&["reload"]).await;
    assert_eq!(r["unchanged"].as_array().unwrap().len(), 2, "{r}");

    // `remove` stops one and forgets it; the file is not touched.
    let out = env.run(&["remove", "tb"]).await;
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(env.ps().await.len(), 1);
    assert_eq!(env.run(&["status", "tb"]).await.code, 4);
    assert!(
        std::fs::read_to_string(&env.config)
            .unwrap()
            .contains("\"tb\"")
    );

    // The file still lists it, so a reload brings it back.
    let r = env.json(&["reload"]).await;
    assert_eq!(r["started"][0], "tb", "{r}");
    env.wait_all_running(&["tb"]).await;

    // A gateway that is gone from the file goes on reload.
    env.write_config(&[("tb", Broker::Tbank, &tbank)], "");
    let r = env.json(&["reload"]).await;
    assert_eq!(r["stopped"][0], "bcs-main");
    assert_eq!(r["restarted"][0], "tb", "its section lost bot_buffer: {r}");
    env.wait_all_running(&["tb"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reading_commands_go_through_the_gateway() {
    let bcs = Mock::start(Broker::Bcs).await;
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();
    env.up().await;
    env.wait_all_running(&["bcs-main"]).await;

    let portfolio = env.json(&["portfolio", "bcs-main"]).await;
    assert!(
        portfolio["balances"].is_array() && portfolio["positions"].is_array(),
        "{portfolio}"
    );
    let text = env.run(&["portfolio", "bcs-main"]).await.stdout;
    assert!(
        text.contains("CURRENCY") && text.contains("INSTRUMENT"),
        "{text}"
    );

    let book = env
        .json(&[
            "book", "bcs-main", "SBER", "--board", "TQBR", "--depth", "3",
        ])
        .await;
    let snapshot = &book["Snapshot"];
    assert!(!snapshot["bids"].as_array().unwrap().is_empty(), "{book}");
    assert!(snapshot["bids"].as_array().unwrap().len() <= 3);
    let text = env
        .run(&["book", "bcs-main", "SBER", "--board", "TQBR"])
        .await
        .stdout;
    assert!(
        text.contains("SBER TQBR") && text.contains("BID QTY"),
        "{text}"
    );

    let quote = env
        .json(&["quote", "bcs-main", "SBER", "--board", "TQBR"])
        .await;
    assert_eq!(quote["instrument"], "SBER TQBR");
    assert!(
        quote["bid"]["price"].is_string() && quote["ask"]["price"].is_string(),
        "{quote}"
    );
    assert!(
        env.run(&["quote", "bcs-main", "SBER", "--board", "TQBR"])
            .await
            .stdout
            .contains("bid")
    );

    // An order placed by a bot shows in `orders`; stop orders are empty.
    let bot = env.bot("bcs-main", "bot-1").await;
    let request = common::bcs_order("1.0");
    let cid = request.cid;
    bot.place(request).await.unwrap();
    let orders = env.json(&["orders", "bcs-main"]).await;
    assert_eq!(orders.as_array().unwrap().len(), 1);
    assert_eq!(orders[0]["cid"], cid.to_string());
    assert!(
        env.run(&["orders", "bcs-main"])
            .await
            .stdout
            .contains(&cid.to_string())
    );
    assert!(
        env.json(&["stops", "bcs-main"])
            .await
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        env.run(&["stops", "bcs-main"])
            .await
            .stdout
            .contains("BROKER ID")
    );

    // A gateway that is not there.
    assert_eq!(env.run(&["orders", "nope"]).await.code, 4);
    // `direct` asks the broker without the service, with the token of the gateway.
    let direct = env
        .json(&["direct", "bcs", "orders", "--from", "bcs-main"])
        .await;
    assert_eq!(direct.as_array().unwrap().len(), 1);
    let direct = env
        .json(&["direct", "bcs", "portfolio", "--from", "bcs-main"])
        .await;
    assert!(direct["balances"].is_array());
    assert_eq!(
        env.run(&["direct", "tbank", "orders", "--from", "bcs-main"])
            .await
            .code,
        2,
        "wrong broker"
    );
    let out = env.run(&["direct", "bcs", "portfolio"]).await;
    assert_eq!(out.code, 1);
    assert!(out.stderr.contains("BCS_REFRESH_TOKEN"), "{}", out.stderr);
    assert_eq!(env.run(&["direct", "nope", "portfolio"]).await.code, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_all_clears_the_account_and_stop_can_do_it_first() {
    let bcs = Mock::start(Broker::Bcs).await;
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();
    env.up().await;
    env.wait_all_running(&["bcs-main"]).await;
    let bot = env.bot("bcs-main", "bot-1").await;
    for price in ["1.0", "1.1", "1.2"] {
        place(&bot, price).await;
    }
    assert_eq!(
        env.json(&["orders", "bcs-main"])
            .await
            .as_array()
            .unwrap()
            .len(),
        3
    );

    let report = env.json(&["cancel-all", "bcs-main"]).await;
    assert_eq!(report["orders"], 3, "{report}");
    assert!(report["errors"].as_array().unwrap().is_empty());
    assert!(
        env.json(&["orders", "bcs-main"])
            .await
            .as_array()
            .unwrap()
            .is_empty()
    );
    let out = env.run(&["cancel-all", "bcs-main"]).await;
    assert!(
        out.stdout.contains("cancelled 0 order"),
        "{}{}",
        out.stdout,
        out.stderr
    );
    assert_eq!(env.run(&["cancel-all", "nope"]).await.code, 4);

    // `stop --cancel-orders` does it before the gateway goes.
    place(&bot, "1.0").await;
    place(&bot, "1.1").await;
    let stopped = env.json(&["stop", "bcs-main", "--cancel-orders"]).await;
    assert_eq!(stopped[0]["cancelled_orders"], 2, "{stopped}");
    assert_eq!(stopped[0]["state"], "stopped");
    let direct = env
        .json(&["direct", "bcs", "orders", "--from", "bcs-main"])
        .await;
    assert!(
        direct.as_array().unwrap().is_empty(),
        "nothing is left at the broker"
    );
    // Plain `stop` leaves orders alone.
    env.run(&["start", "bcs-main", "--wait"]).await;
    let bot = env.bot("bcs-main", "bot-1").await;
    place(&bot, "1.0").await;
    env.json(&["stop", "bcs-main"]).await;
    let direct = env
        .json(&["direct", "bcs", "orders", "--from", "bcs-main"])
        .await;
    assert_eq!(
        direct.as_array().unwrap().len(),
        1,
        "the order stays at the broker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logs_top_and_init() {
    let bcs = Mock::start(Broker::Bcs).await;
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();

    // No log yet.
    let none = env.run(&["logs", "bcs-main"]).await;
    assert_eq!(none.code, 0);
    assert!(none.stdout.contains("no log yet"));

    env.up().await;
    env.wait_all_running(&["bcs-main"]).await;
    let service = env.run(&["logs"]).await.stdout;
    assert!(service.contains("the service runs"), "{service}");
    let gateway = env.run(&["logs", "bcs-main", "-n", "100"]).await;
    assert!(
        gateway.stdout.contains("the gateway runs"),
        "{}",
        gateway.stdout
    );
    assert_eq!(env.run(&["logs", "nope"]).await.code, 4);
    let lines: Vec<Value> = env
        .run(&["logs", "bcs-main", "--json"])
        .await
        .stdout
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(
        lines
            .iter()
            .any(|l| l["line"].as_str().unwrap().contains("the gateway runs"))
    );

    // `logs -f` shows what comes: a restart writes to the log of the gateway.
    {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut follow = tokio::process::Command::new(EXE)
            .args([
                "-c",
                env.config.to_str().unwrap(),
                "logs",
                "bcs-main",
                "-f",
                "-n",
                "0",
            ])
            .env("BROKAPI_REGISTRY_DIR", &env.registry)
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut reader = BufReader::new(follow.stdout.take().unwrap()).lines();
        let out = env.run(&["restart", "bcs-main", "--wait"]).await;
        assert_eq!(out.code, 0, "{}", out.stderr);
        let mut seen = Vec::new();
        let found = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while let Ok(Some(line)) = reader.next_line().await {
                seen.push(line.clone());
                if line.contains("the gateway runs") {
                    return true;
                }
            }
            false
        })
        .await;
        assert_eq!(found, Ok(true), "{seen:?}");
        let _ = follow.kill().await;
    }

    // `top` draws the table again and again, and is JSON by lines.
    let top = env.run(&["top", "-n", "2", "-i", "0.1"]).await;
    assert_eq!(top.code, 0, "{}", top.stderr);
    assert_eq!(
        top.stdout.matches("brokapi top").count(),
        2,
        "{}",
        top.stdout
    );
    assert!(
        top.stdout.contains("CPU") && top.stdout.contains("MEM") && top.stdout.contains("bcs-main")
    );
    let top = env.run(&["top", "-n", "1", "--json"]).await;
    let frame: Value = serde_json::from_str(top.stdout.lines().next().unwrap()).unwrap();
    assert_eq!(frame["gateways"][0]["name"], "bcs-main");
    assert!(frame["gateways"][0]["memory_bytes"].as_u64().unwrap() > 0);

    // init
    let fresh = tempfile::tempdir().unwrap();
    let path = fresh.path().join("sub").join("new.toml");
    let out = env.run_with(&[], &["init", path.to_str().unwrap()]).await;
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert!(path.exists() && fresh.path().join("sub").join("new.state").is_dir());
    let again = env.run_with(&[], &["init", path.to_str().unwrap()]).await;
    assert_eq!(again.code, 1, "does not replace a file");
    assert_eq!(
        env.run_with(&[], &["init", path.to_str().unwrap(), "--force"])
            .await
            .code,
        0
    );
    // The sample is a config that loads, but its token is not there: check says so, offline and for the exit code.
    let out = env
        .run_with(&["-c", path.to_str().unwrap()], &["check", "--offline"])
        .await;
    assert_eq!(out.code, 1);
    assert!(
        out.stdout.contains("TBANK_TOKEN"),
        "{}{}",
        out.stdout,
        out.stderr
    );
    Config::load(&path).expect("the sample loads");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mistakes_have_clear_messages_and_exit_codes() {
    let bcs = Mock::start(Broker::Bcs).await;
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();

    // Help lists the codes.
    let help = env.run(&["--help"]).await;
    assert_eq!(help.code, 0);
    assert!(
        help.stdout.contains("Exit codes:") && help.stdout.contains("no service is running"),
        "{}",
        help.stdout
    );
    for command in [
        "init",
        "check",
        "up",
        "down",
        "services",
        "ps",
        "top",
        "start",
        "stop",
        "restart",
        "add",
        "remove",
        "reload",
        "status",
        "logs",
        "gateway",
        "portfolio",
        "orders",
        "stops",
        "book",
        "quote",
        "direct",
        "cancel-all",
    ] {
        assert!(
            help.stdout.contains(&format!("\n  {command} ")),
            "`{command}` is in the help"
        );
    }

    // No config at all.
    let out = env.run_with(&[], &["ps"]).await;
    assert_eq!(out.code, 2);
    assert!(
        out.stderr.contains("no config file found") && out.stderr.contains("brokapi init"),
        "{}",
        out.stderr
    );
    let out = env.run_with(&["-c", "/no/such.toml"], &["ps"]).await;
    assert_eq!(out.code, 2);
    assert!(out.stderr.contains("/no/such.toml"));

    // A bad file names the mistake.
    let bad = env.dir.path().join("bad.toml");
    std::fs::write(
        &bad,
        "[[gateway]]\nname = \"a\"\nbroker = \"bcs\"\nbogus = 1\n",
    )
    .unwrap();
    let out = env
        .run_with(&["-c", bad.to_str().unwrap()], &["check"])
        .await;
    assert_eq!(out.code, 2);
    assert!(out.stderr.contains("bogus"), "{}", out.stderr);

    // Usage.
    assert_eq!(env.run(&["frobnicate"]).await.code, 2);
    assert_eq!(env.run(&["start"]).await.code, 2);

    // A broker that refuses the token: check finds it, with the exit code 1.
    let token = env.dir.path().join("bcs-main.token");
    let good = std::fs::read_to_string(&token).unwrap();
    std::fs::write(&token, "").unwrap();
    let out = env.run(&["check"]).await;
    assert_eq!(out.code, 1);
    assert!(out.stdout.contains("token is empty"), "{}", out.stdout);
    std::fs::write(&token, good).unwrap();
    let only = env.run(&["check", "bcs-main"]).await;
    assert_eq!(only.code, 0, "{}{}", only.stdout, only.stderr);
    assert_eq!(env.run(&["check", "nope"]).await.code, 4);

    // Commands that need a gateway, when it is not running.
    let out = env.run(&["portfolio", "bcs-main"]).await;
    assert_eq!(out.code, 4);
    assert!(
        out.stderr.contains("not running") && out.stderr.contains(" up`"),
        "{}",
        out.stderr
    );
    for args in [
        &["start", "bcs-main"][..],
        &["stop", "bcs-main"],
        &["reload"],
        &["status", "bcs-main"],
        &["top", "-n", "1"],
    ] {
        assert_eq!(env.run(args).await.code, 3, "{args:?} without a service");
    }
    // `gateway run` in the foreground refuses a gateway the config does not have, and a missing token.
    let out = env.run(&["gateway", "run", "nope"]).await;
    assert_eq!(out.code, 2);
    assert!(out.stderr.contains("no gateway named"), "{}", out.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_runs_in_the_foreground_without_the_service() {
    let bcs = Mock::start(Broker::Bcs).await;
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();
    let mut child = tokio::process::Command::new(EXE)
        .args([
            "-c",
            env.config.to_str().unwrap(),
            "gateway",
            "run",
            "bcs-main",
        ])
        .env("BROKAPI_REGISTRY_DIR", &env.registry)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    brokapi_mocks::time::wait_until_async("the gateway answers", || async {
        match RemoteClient::connect_dir(&env.gateway_dir("bcs-main"), "bot-1").await {
            Ok(bot) => bot
                .health()
                .await
                .map(|h| h.state == pb::GatewayState::Ready as i32)
                .unwrap_or(false),
            Err(_) => false,
        }
    })
    .await;
    // The same gateway cannot run twice.
    let second = env.run(&["gateway", "run", "bcs-main"]).await;
    assert_eq!(second.code, 1);
    assert!(
        second.stderr.contains("already runs") || second.stderr.contains("already"),
        "{}",
        second.stderr
    );
    // The commands that go to the gateway work with no service.
    assert!(env.json(&["portfolio", "bcs-main"]).await["balances"].is_array());
    #[cfg(unix)]
    {
        proc::terminate(child.id().unwrap());
        let status = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
            .await
            .expect("it stops")
            .unwrap();
        assert!(
            status.success(),
            "a stop by request is a clean end: {status}"
        );
        assert!(!env.gateway_dir("bcs-main").join("gateway.pid").exists());
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill().await;
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn up_in_the_foreground_stops_on_ctrl_c_and_takes_its_gateways_with_it() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let bcs = Mock::start(Broker::Bcs).await;
    let env = Env::new(&[("bcs-main", Broker::Bcs, &bcs)]);
    let _guard = env.guard();
    let mut service = tokio::process::Command::new(EXE)
        .args(["-c", env.config.to_str().unwrap(), "up"])
        .env("BROKAPI_REGISTRY_DIR", &env.registry)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    // It says what it runs, in the terminal it was started in, and it does not return.
    let mut lines = BufReader::new(service.stdout.take().unwrap()).lines();
    let first = lines.next_line().await.unwrap().expect("a line");
    assert!(first.starts_with("service runs, pid"), "{first}");
    env.wait_all_running(&["bcs-main"]).await;
    let gateway = pid_of(&env.gateway("bcs-main").await.unwrap());
    assert!(service.try_wait().unwrap().is_none());

    // Ctrl+C: the terminal sends SIGINT.
    let status = Command::new("kill")
        .args(["-INT", &service.id().unwrap().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let status = tokio::time::timeout(std::time::Duration::from_secs(15), service.wait())
        .await
        .expect("the service stops")
        .unwrap();
    assert!(status.success(), "a clean end: {status}");
    assert!(!proc::is_alive(gateway), "the gateway went with it");
    assert!(env.json(&["services"]).await.as_array().unwrap().is_empty());
    assert!(!env.state.join("service.sock").exists());
}
