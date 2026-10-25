//! The config file `brokapi.toml`: which gateways a service runs, and how.
//!
//! Tokens are never written in the file. A gateway names where its token comes from: an environment variable
//! (`token_env`) or a file (`token_file`). The state directory belongs to one config, so two configs are two
//! independent services.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use brokapi_client::{Broker, BrokerConfig, Settings};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{Listen, RingConfig, valid_bot_name};

/// The longest name of a gateway. It is a directory name and a part of a socket path, which is short on some systems.
pub const MAX_NAME: usize = 32;

/// Why a config cannot be used.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// No config file in any of the places.
    #[error(
        "no config file found. Looked at: {0}. Make one with `brokapi init`, or give the path with -c"
    )]
    NotFound(String),
    /// The file cannot be read.
    #[error("cannot read {path}: {source}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The error of the system.
        source: std::io::Error,
    },
    /// The file is not valid TOML or has a field the program does not know.
    #[error("{path}: {message}")]
    Parse {
        /// The file.
        path: PathBuf,
        /// What is wrong, with the line.
        message: String,
    },
    /// The file reads, but a value cannot work.
    #[error("{path}: {message}")]
    Invalid {
        /// The file.
        path: PathBuf,
        /// What is wrong.
        message: String,
    },
}

mod seconds {
    //! `"500ms"`, `"30s"`, `"5m"`, `"2h"` or a number of seconds.
    use super::*;
    use serde::Deserializer;

    pub fn parse(text: &str) -> Result<Duration, String> {
        let t = text.trim();
        let (num, unit) = t.split_at(t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len()));
        let n: u64 = num.parse().map_err(|_| {
            format!("{text:?} is not a duration: write it like \"500ms\", \"30s\", \"5m\"")
        })?;
        match unit.trim() {
            "ms" => Ok(Duration::from_millis(n)),
            "" | "s" => Ok(Duration::from_secs(n)),
            "m" => Ok(Duration::from_secs(n * 60)),
            "h" => Ok(Duration::from_secs(n * 3600)),
            other => Err(format!(
                "{text:?}: the unit {other:?} is not known, use ms, s, m or h"
            )),
        }
    }

    pub fn de<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Text(String),
            Number(u64),
        }
        match Either::deserialize(d)? {
            Either::Text(t) => parse(&t).map_err(serde::de::Error::custom),
            Either::Number(n) => Ok(Duration::from_secs(n)),
        }
    }
}

#[cfg(test)]
pub(crate) use seconds::parse as parse_duration;

fn default_true() -> bool {
    true
}

/// How a service starts its gateways again after they stop on their own.
///
/// After an exit the service waits `initial`, then twice as long after the next, up to `max_pause`. When a gateway has
/// stopped `max_restarts` times within `window`, it is marked `failed` and waits for `brokapi start`. A gateway that
/// runs longer than `window` without stopping starts counting from the beginning.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RestartPolicy {
    /// The pause before the first restart.
    #[serde(deserialize_with = "seconds::de")]
    pub initial: Duration,
    /// The longest pause.
    #[serde(deserialize_with = "seconds::de")]
    pub max_pause: Duration,
    /// How many restarts within `window` are allowed.
    pub max_restarts: u32,
    /// The period in which the restarts are counted.
    #[serde(deserialize_with = "seconds::de")]
    pub window: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max_pause: Duration::from_secs(30),
            max_restarts: 5,
            window: Duration::from_secs(60),
        }
    }
}

/// Logs of the service and the gateways: files in `logs/` of the state directory.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogPolicy {
    /// The size at which a file is closed and a new one started, in megabytes.
    pub max_size_mb: u64,
    /// How many old files are kept next to the current one.
    pub keep: usize,
}

impl Default for LogPolicy {
    fn default() -> Self {
        Self {
            max_size_mb: 10,
            keep: 5,
        }
    }
}

/// The `[service]` section.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceSection {
    /// Where the service listens for commands: `unix:<path>` or `tcp:127.0.0.1:<port>`. By default a socket in the state
    /// directory (Linux, macOS) or a free port of `127.0.0.1` (Windows).
    pub listen: Option<String>,
    /// How often the service looks at its gateways.
    #[serde(deserialize_with = "seconds::de")]
    pub check_interval: Duration,
    /// How long a gateway has to stop on a request before it is ended by force.
    #[serde(deserialize_with = "seconds::de")]
    pub stop_timeout: Duration,
    /// The restart policy.
    pub restart: RestartPolicy,
    /// The log files.
    pub logs: LogPolicy,
}

impl Default for ServiceSection {
    fn default() -> Self {
        Self {
            listen: None,
            check_interval: Duration::from_secs(1),
            stop_timeout: Duration::from_secs(10),
            restart: RestartPolicy::default(),
            logs: LogPolicy::default(),
        }
    }
}

/// The ring of recent events a gateway keeps for the bots that reconnect.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RingSection {
    /// How many events at most.
    pub size: usize,
    /// How old the oldest may be.
    #[serde(deserialize_with = "seconds::de")]
    pub max_age: Duration,
}

impl Default for RingSection {
    fn default() -> Self {
        let d = RingConfig::default();
        Self {
            size: d.max_events,
            max_age: d.max_age,
        }
    }
}

/// One bucket of the rate limit, written in the config to replace the default one.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketSection {
    /// Tokens added per `per`.
    pub rate: u32,
    /// The period.
    #[serde(deserialize_with = "seconds::de")]
    pub per: Duration,
    /// The capacity of the bucket.
    pub burst: u32,
}

/// A `[[gateway]]` section: one connection to one account of one broker.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewaySpec {
    /// The name. The commands use it, and the state of the gateway is in a directory of this name.
    pub name: String,
    /// `bcs`, `tbank`, `finam`, `alor` or `bybit`.
    pub broker: String,
    /// The account: `account_id` of T-Bank and Finam, `portfolio` of ALOR. BCS and Bybit have no choice of account (a
    /// Bybit key belongs to one account).
    pub account: Option<String>,
    /// The sandbox of the broker (T-Bank `sandbox`, ALOR `test`, the demo account of Bybit). BCS and Finam have none.
    #[serde(default)]
    pub sandbox: bool,
    /// The environment variable that holds the token (for Bybit: the API secret).
    pub token_env: Option<String>,
    /// The file that holds the token (the whole contents, trimmed).
    pub token_file: Option<PathBuf>,
    /// Bybit only: the environment variable that holds the API key. The key is the first half of the pair, the secret
    /// is the token.
    pub key_env: Option<String>,
    /// Bybit only: the file that holds the API key.
    pub key_file: Option<PathBuf>,
    /// Whether the service starts it on `up`. A gateway with `autostart = false` waits for `brokapi start`.
    #[serde(default = "default_true")]
    pub autostart: bool,
    /// The ring of recent events.
    #[serde(default)]
    pub ring: RingSection,
    /// Buckets of the rate limit of the broker to replace, by the name of the bucket (see the page of the broker).
    #[serde(default)]
    pub limits: std::collections::BTreeMap<String, BucketSection>,
    /// How many events wait for one bot before it is cut off as too slow.
    pub bot_buffer: Option<usize>,
    /// How long a bot may be away before the gateway lets go of its subscriptions.
    #[serde(default, deserialize_with = "opt_seconds")]
    pub bot_grace: Option<Duration>,
    /// Other settings of the connector, as in the settings of the library (`endpoint`, `write`, `exchange`, `app_name`).
    /// A token cannot be written here.
    #[serde(default)]
    pub settings: toml::Table,
}

fn opt_seconds<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    seconds::de(d).map(Some)
}

/// The token keys a gateway must not carry in its `settings`.
const SECRET_KEYS: [&str; 4] = ["token", "refresh_token", "secret", "api_secret"];

impl GatewaySpec {
    /// The broker.
    pub fn broker(&self) -> Result<Broker, String> {
        self.broker.parse::<Broker>().map_err(|e| e.to_string())
    }

    /// The token, from the variable or the file the gateway names. Never printed.
    pub fn token(&self) -> Result<String, String> {
        let raw = match (&self.token_env, &self.token_file) {
            (Some(var), None) => std::env::var(var).map_err(|_| {
                format!(
                    "gateway {}: the environment variable {var} (token_env) is not set",
                    self.name
                )
            })?,
            (None, Some(path)) => std::fs::read_to_string(path).map_err(|e| {
                format!(
                    "gateway {}: cannot read the token file {}: {e}",
                    self.name,
                    path.display()
                )
            })?,
            _ => {
                return Err(format!(
                    "gateway {}: say where the token is: token_env = \"NAME\" or token_file = \"path\"",
                    self.name
                ));
            }
        };
        let token = raw.trim().to_owned();
        if token.is_empty() {
            return Err(format!("gateway {}: the token is empty", self.name));
        }
        Ok(token)
    }

    /// The API key of a Bybit gateway, from the variable or the file the gateway names. Never printed.
    pub fn api_key(&self) -> Result<String, String> {
        let raw = match (&self.key_env, &self.key_file) {
            (Some(var), None) => std::env::var(var).map_err(|_| {
                format!(
                    "gateway {}: the environment variable {var} (key_env) is not set",
                    self.name
                )
            })?,
            (None, Some(path)) => std::fs::read_to_string(path).map_err(|e| {
                format!(
                    "gateway {}: cannot read the key file {}: {e}",
                    self.name,
                    path.display()
                )
            })?,
            _ => {
                return Err(format!(
                    "gateway {}: say where the API key is: key_env = \"NAME\" or key_file = \"path\" (the secret is the token)",
                    self.name
                ));
            }
        };
        let key = raw.trim().to_owned();
        if key.is_empty() {
            return Err(format!("gateway {}: the API key is empty", self.name));
        }
        Ok(key)
    }

    /// The settings of the library for this gateway, with the token in them.
    pub fn settings_json(&self) -> Result<Map<String, Value>, String> {
        let broker = self.broker()?;
        let mut map = match serde_json::to_value(&self.settings) {
            Ok(Value::Object(m)) => m,
            _ => Map::new(),
        };
        let token = self.token()?;
        let (token_key, account_key) = match broker {
            Broker::Bcs => ("refresh_token", None),
            Broker::Alor => ("refresh_token", Some("portfolio")),
            Broker::Tbank => ("token", Some("account_id")),
            Broker::Finam => ("secret", Some("account_id")),
            Broker::Bybit => ("api_secret", None),
        };
        map.insert(token_key.into(), Value::String(token));
        if broker == Broker::Bybit {
            map.insert("api_key".into(), Value::String(self.api_key()?));
        } else if self.key_env.is_some() || self.key_file.is_some() {
            return Err(format!(
                "gateway {}: {broker} has one token and no separate key, remove key_env and key_file",
                self.name
            ));
        }
        if let Some(account) = &self.account {
            match account_key {
                Some(key) => {
                    map.insert(key.into(), Value::String(account.clone()));
                }
                None => {
                    return Err(format!(
                        "gateway {}: {broker} has no choice of account, remove `account`",
                        self.name
                    ));
                }
            }
        }
        if self.sandbox {
            match broker {
                Broker::Tbank => {
                    map.insert("sandbox".into(), Value::Bool(true));
                }
                Broker::Alor => {
                    map.insert("test".into(), Value::Bool(true));
                }
                Broker::Bybit => {
                    map.insert("env".into(), Value::String("demo".into()));
                }
                _ => {
                    return Err(format!(
                        "gateway {}: {broker} has no sandbox, remove `sandbox`",
                        self.name
                    ));
                }
            }
        }
        Ok(map)
    }

    /// How to connect to the broker.
    pub fn broker_config(&self) -> Result<BrokerConfig, String> {
        let broker = self.broker()?;
        let settings = Settings(self.settings_json()?);
        BrokerConfig::from_settings(broker, &settings)
            .map_err(|e| format!("gateway {}: {e}", self.name))
    }

    /// The ring of the gateway.
    pub fn ring_config(&self) -> RingConfig {
        RingConfig {
            max_events: self.ring.size,
            max_age: self.ring.max_age,
        }
    }

    /// The rate limiter with the buckets of the config in place of the default ones.
    pub fn limiter(&self) -> Result<Option<brokapi_client::Limiter>, String> {
        if self.limits.is_empty() {
            return Ok(None);
        }
        let broker = self.broker()?;
        let mut limits = match broker {
            Broker::Bcs => brokapi::bcs::limits::default_limits(),
            Broker::Tbank => brokapi::tbank::limits::default_limits(),
            Broker::Finam => brokapi::finam::limits::default_limits(),
            Broker::Alor => brokapi::alor::limits::default_limits(),
            Broker::Bybit => brokapi::bybit::limits::default_limits(),
        };
        for (name, b) in &self.limits {
            let Some(bucket) = limits.buckets.get_mut(name) else {
                let mut known: Vec<_> = limits.buckets.keys().cloned().collect();
                known.sort();
                return Err(format!(
                    "gateway {}: {broker} has no limit bucket {name:?}; it has: {}",
                    self.name,
                    known.join(", ")
                ));
            };
            bucket.rate = b.rate;
            bucket.per = b.per;
            bucket.burst = b.burst;
        }
        Ok(Some(brokapi_client::Limiter::with_limits(broker, &limits)))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    state_dir: Option<PathBuf>,
    #[serde(default)]
    service: ServiceSection,
    #[serde(default, rename = "gateway")]
    gateways: Vec<GatewaySpec>,
}

/// A config file, read and checked.
#[derive(Debug, Clone)]
pub struct Config {
    /// The absolute path of the file.
    pub path: PathBuf,
    /// The state directory: the files of the service and of every gateway. Absolute.
    pub state_dir: PathBuf,
    /// The `[service]` section.
    pub service: ServiceSection,
    /// The gateways, in the order of the file.
    pub gateways: Vec<GatewaySpec>,
}

/// Makes a directory of the state (and the missing ones above it) that only the user can open: it holds the access keys
/// of the service and of the gateways. A directory that is already there keeps its rights.
pub fn make_private_dir(path: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(path)
}

/// The places a config is looked for when `-c` and `BROKAPI_CONFIG` say nothing.
pub fn default_config_paths() -> Vec<PathBuf> {
    let mut paths = vec![PathBuf::from("brokapi.toml")];
    if let Some(dirs) = directories::ProjectDirs::from("", "", "brokapi") {
        paths.push(dirs.config_dir().join("brokapi.toml"));
    }
    paths
}

/// The config file to use: the one asked for, else the first default place that has one.
pub fn find_config(explicit: Option<&Path>) -> Result<PathBuf, ConfigError> {
    if let Some(path) = explicit {
        return Ok(path.to_owned());
    }
    let places = default_config_paths();
    for p in &places {
        if p.exists() {
            return Ok(p.clone());
        }
    }
    Err(ConfigError::NotFound(
        places
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", "),
    ))
}

impl Config {
    /// Reads and checks the config at `path` (see [`find_config`]).
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let abs = std::path::absolute(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(&text, &abs)
    }

    /// Checks the text of a config that is, or would be, at `path` (absolute).
    pub fn parse(text: &str, path: &Path) -> Result<Self, ConfigError> {
        let invalid = |message: String| ConfigError::Invalid {
            path: path.to_owned(),
            message,
        };
        let raw: RawConfig = toml::from_str(text).map_err(|e| ConfigError::Parse {
            path: path.to_owned(),
            message: e.to_string(),
        })?;
        let dir = path.parent().unwrap_or(Path::new("."));
        let state_dir = match raw.state_dir {
            Some(p) if p.is_absolute() => p,
            Some(p) => dir.join(p),
            None => {
                let stem = path
                    .file_stem()
                    .map_or_else(|| "brokapi".into(), |s| s.to_string_lossy().into_owned());
                dir.join(format!("{stem}.state"))
            }
        };
        let mut seen = HashSet::new();
        for g in &raw.gateways {
            if g.name.len() > MAX_NAME || !valid_bot_name(&g.name) {
                return Err(invalid(format!(
                    "gateway name {:?}: 1 to {MAX_NAME} letters, digits, '.', '_' or '-'",
                    g.name
                )));
            }
            if !seen.insert(g.name.clone()) {
                return Err(invalid(format!("two gateways are named {:?}", g.name)));
            }
            g.broker()
                .map_err(|e| invalid(format!("gateway {}: {e}", g.name)))?;
            if g.token_env.is_some() == g.token_file.is_some() {
                return Err(invalid(format!(
                    "gateway {}: give exactly one of token_env = \"NAME\" and token_file = \"path\"",
                    g.name
                )));
            }
            for key in SECRET_KEYS {
                if g.settings.contains_key(key) {
                    return Err(invalid(format!(
                        "gateway {}: `{key}` in settings: tokens do not go in the config, use token_env or token_file",
                        g.name
                    )));
                }
            }
        }
        if let Some(listen) = &raw.service.listen {
            parse_listen(listen).map_err(|e| invalid(format!("[service] listen: {e}")))?;
        }
        if raw.service.restart.max_restarts == 0 {
            return Err(invalid(
                "[service.restart] max_restarts must be at least 1".into(),
            ));
        }
        Ok(Self {
            path: path.to_owned(),
            state_dir,
            service: raw.service,
            gateways: raw.gateways,
        })
    }

    /// The gateway of this name.
    pub fn gateway(&self, name: &str) -> Option<&GatewaySpec> {
        self.gateways.iter().find(|g| g.name == name)
    }

    /// Where the service listens.
    pub fn listen(&self) -> Listen {
        self.service
            .listen
            .as_deref()
            .and_then(|l| parse_listen(l).ok())
            .unwrap_or(Listen::Auto)
    }

    /// The paths of the state directory.
    pub fn layout(&self) -> Layout {
        Layout::new(&self.state_dir)
    }
}

pub(crate) fn parse_listen(text: &str) -> Result<Listen, String> {
    match text.parse::<crate::Address>()? {
        crate::Address::Unix(p) => Ok(Listen::Unix(p)),
        crate::Address::Tcp(a) => Ok(Listen::Tcp(a)),
    }
}

/// The files of a state directory.
///
/// ```text
/// <state>/service.key  service.addr  service.sock  service.pid   the service
/// <state>/logs/service.log  logs/<gateway>.log                    the logs
/// <state>/gateways/<gateway>/                                     one gateway: journal, access.key, gateway.addr, gateway.pid
/// ```
#[derive(Debug, Clone)]
pub struct Layout {
    /// The state directory.
    pub root: PathBuf,
}

impl Layout {
    /// The layout of this directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The pid of the service.
    pub fn service_pid(&self) -> PathBuf {
        self.root.join("service.pid")
    }

    /// The directory of the log files.
    pub fn logs(&self) -> PathBuf {
        self.root.join("logs")
    }

    /// The log of the service.
    pub fn service_log(&self) -> PathBuf {
        self.logs().join("service.log")
    }

    /// The log of a gateway.
    pub fn gateway_log(&self, name: &str) -> PathBuf {
        self.logs().join(format!("{name}.log"))
    }

    /// The directory of a gateway.
    pub fn gateway_dir(&self, name: &str) -> PathBuf {
        self.root.join("gateways").join(name)
    }

    /// The pid of a gateway.
    pub fn gateway_pid(&self, name: &str) -> PathBuf {
        self.gateway_dir(name).join("gateway.pid")
    }
}

/// The text of the sample config that `brokapi init` writes.
pub fn sample(state_dir: &str) -> String {
    format!(
        r#"# brokapi.toml - the config of one brokapi service.
#
# `brokapi up` starts the service, and the service runs one gateway process for every [[gateway]] below. Two config files
# are two independent services: each has its own state directory and its own socket.
# Tokens do not go in this file. A gateway says where its token is: token_env (an environment variable) or token_file.

# The files of the service and of the gateways: journals, access keys, sockets, logs. Relative to this file.
state_dir = "{state_dir}"

[service]
# How often the service looks at its gateways.
check_interval = "1s"
# How long a gateway has to stop on a request before it is ended by force.
stop_timeout = "10s"

# A gateway that stops on its own is started again after 500ms, then 1s, 2s ... up to 30s. After 5 restarts within a
# minute it is marked `failed` and waits for `brokapi start <name>`.
[service.restart]
initial = "500ms"
max_pause = "30s"
max_restarts = 5
window = "60s"

# The log files in <state_dir>/logs: a new file at 10 MB, 5 old ones are kept.
[service.logs]
max_size_mb = 10
keep = 5

# One [[gateway]] is one account at one broker. The bots of the account connect to it, not to the broker.
[[gateway]]
name = "tbank-sandbox"
broker = "tbank"             # bcs, tbank, finam, alor or bybit
account = "2000000000"       # T-Bank and Finam: account_id, ALOR: portfolio. BCS and Bybit have none.
sandbox = true               # T-Bank, ALOR and Bybit (the demo account) have a sandbox
token_env = "TBANK_TOKEN"    # or: token_file = "/path/to/token". Bybit: this is the API secret, and key_env (or
                             # key_file) names the API key
# autostart = false          # do not start on `brokapi up`, wait for `brokapi start`

# The events a bot that reconnects can still get, by number and by age.
# [gateway.ring]
# size = 10000
# max_age = "5m"

# Replace a bucket of the rate limit of the broker. The names of the buckets of this gateway are in `brokapi status <name>`.
# [gateway.limits.order_post]
# rate = 5
# per = "1s"
# burst = 5

# Other settings of the connector, as in the settings of the library. For BCS orders need `write = true`.
# [gateway.settings]
# write = true
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Config, ConfigError> {
        Config::parse(text, Path::new("/etc/brokapi/prod.toml"))
    }

    #[test]
    fn the_sample_is_a_valid_config() {
        let c = parse(&sample("state")).unwrap();
        assert_eq!(c.gateways.len(), 1);
        assert_eq!(c.gateways[0].name, "tbank-sandbox");
        assert_eq!(c.state_dir, Path::new("/etc/brokapi/state"));
        assert_eq!(c.service.restart, RestartPolicy::default());
    }

    #[test]
    fn the_state_dir_follows_the_config_unless_said() {
        let c = parse("").unwrap();
        assert_eq!(c.state_dir, Path::new("/etc/brokapi/prod.state"));
        let c = parse("state_dir = \"/var/lib/b\"").unwrap();
        assert_eq!(c.state_dir, Path::new("/var/lib/b"));
    }

    #[test]
    fn mistakes_are_named() {
        let g = |body: &str| format!("[[gateway]]\nname = \"a\"\nbroker = \"bcs\"\n{body}");
        let msg = |t: String| parse(&t).unwrap_err().to_string();
        assert!(
            msg(g("")).contains("exactly one of token_env"),
            "no token source"
        );
        assert!(msg(g("token_env = \"X\"\ntoken_file = \"y\"")).contains("exactly one"));
        assert!(
            msg(g(
                "token_env = \"X\"\n[gateway.settings]\nrefresh_token = \"abc\""
            ))
            .contains("tokens do not go in the config")
        );
        assert!(msg(g("token_env = \"X\"\nbogus = 1")).contains("bogus"));
        assert!(
            msg("[[gateway]]\nname = \"a b\"\nbroker = \"bcs\"\ntoken_env = \"X\"".into())
                .contains("gateway name")
        );
        assert!(
            msg("[[gateway]]\nname = \"a\"\nbroker = \"nope\"\ntoken_env = \"X\"".into())
                .contains("unknown broker")
        );
        let twice = format!("{}\n{}", g("token_env = \"X\""), g("token_env = \"X\""));
        assert!(msg(twice).contains("two gateways are named"));
    }

    #[test]
    fn a_bybit_gateway_has_a_key_and_a_secret_and_a_demo_account_for_a_sandbox() {
        let dir = std::env::temp_dir().join(format!("brokapi-bybit-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("secret"), "the-secret\n").unwrap();
        std::fs::write(dir.join("key"), "the-key").unwrap();
        let section = |extra: &str| {
            format!(
                "[[gateway]]\nname = \"by\"\nbroker = \"bybit\"\ntoken_file = {:?}\n{extra}",
                dir.join("secret").display().to_string()
            )
        };
        let spec = |extra: &str| parse(&section(extra)).unwrap().gateways.remove(0);
        // Without a key the gateway says where to put it, and the secret alone is not enough.
        let err = spec("").settings_json().unwrap_err();
        assert!(err.contains("key_env") && err.contains("key_file"), "{err}");
        let with_key = format!(
            "key_file = {:?}\nsandbox = true\n",
            dir.join("key").display().to_string()
        );
        let map = spec(&with_key).settings_json().unwrap();
        assert_eq!(map["api_secret"], "the-secret");
        assert_eq!(map["api_key"], "the-key");
        assert_eq!(map["env"], "demo");
        match spec(&with_key).broker_config().unwrap() {
            brokapi_client::BrokerConfig::Bybit(cfg) => {
                assert_eq!(cfg.http_base_url, "https://api-demo.bybit.com");
                assert!(cfg.is_demo());
            }
            other => panic!("{other:?}"),
        }
        // Without `sandbox` the main exchange is the default.
        let main = spec(&format!(
            "key_file = {:?}\n",
            dir.join("key").display().to_string()
        ));
        match main.broker_config().unwrap() {
            brokapi_client::BrokerConfig::Bybit(cfg) => {
                assert_eq!(cfg.http_base_url, "https://api.bybit.com")
            }
            other => panic!("{other:?}"),
        }
        // A key for a broker with one token is a mistake that is named, and the secret cannot be written in the file.
        let bcs = parse(&format!(
            "[[gateway]]\nname = \"a\"\nbroker = \"bcs\"\ntoken_file = {:?}\nkey_file = {:?}\n",
            dir.join("secret").display().to_string(),
            dir.join("key").display().to_string()
        ))
        .unwrap();
        assert!(
            bcs.gateways[0]
                .settings_json()
                .unwrap_err()
                .contains("no separate key")
        );
        let text = section("[gateway.settings]\napi_secret = \"abc\"\n");
        assert!(
            parse(&text)
                .unwrap_err()
                .to_string()
                .contains("tokens do not go in the config")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("7").unwrap(), Duration::from_secs(7));
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn the_settings_of_each_broker() {
        // The token comes from a file here: the variables of the process are not touched by a test.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        std::fs::write(&file, "  secret-value\n").unwrap();
        let mk = |broker: &str, extra: &str| {
            let text = format!(
                "[[gateway]]\nname = \"a\"\nbroker = \"{broker}\"\ntoken_file = {:?}\n{extra}",
                file.display().to_string()
            );
            parse(&text).unwrap().gateways[0].settings_json()
        };
        let tbank = mk("tbank", "account = \"A1\"\nsandbox = true").unwrap();
        assert_eq!(tbank["token"], "secret-value");
        assert_eq!(tbank["account_id"], "A1");
        assert_eq!(tbank["sandbox"], true);
        let alor = mk("alor", "account = \"P1\"\nsandbox = true").unwrap();
        assert_eq!(alor["refresh_token"], "secret-value");
        assert_eq!(
            (alor["portfolio"].as_str(), alor["test"].as_bool()),
            (Some("P1"), Some(true))
        );
        assert_eq!(
            mk("finam", "account = \"F1\"").unwrap()["secret"],
            "secret-value"
        );
        assert!(
            mk("bcs", "sandbox = true")
                .unwrap_err()
                .contains("no sandbox")
        );
        assert!(
            mk("bcs", "account = \"x\"")
                .unwrap_err()
                .contains("no choice of account")
        );
    }

    #[test]
    fn a_missing_token_says_where_it_was_looked_for() {
        let c = parse(
            "[[gateway]]\nname = \"a\"\nbroker = \"bcs\"\ntoken_env = \"BROKAPI_SURELY_NOT_SET_9\"",
        )
        .unwrap();
        let err = c.gateways[0].token().unwrap_err();
        assert!(err.contains("BROKAPI_SURELY_NOT_SET_9"), "{err}");
        let c =
            parse("[[gateway]]\nname = \"a\"\nbroker = \"bcs\"\ntoken_file = \"/no/such/file\"")
                .unwrap();
        assert!(c.gateways[0].token().unwrap_err().contains("/no/such/file"));
    }

    #[test]
    fn limits_replace_known_buckets_only() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t");
        std::fs::write(&file, "x").unwrap();
        let text = |bucket: &str| {
            format!(
                "[[gateway]]\nname = \"a\"\nbroker = \"bcs\"\ntoken_file = {:?}\n[gateway.limits.{bucket}]\nrate = 1\nper = \"1s\"\nburst = 2\n",
                file.display().to_string()
            )
        };
        assert!(
            parse(&text("place")).unwrap().gateways[0]
                .limiter()
                .unwrap()
                .is_some()
        );
        let err = parse(&text("nope")).unwrap().gateways[0]
            .limiter()
            .err()
            .unwrap();
        assert!(err.contains("no limit bucket"), "{err}");
    }
}
