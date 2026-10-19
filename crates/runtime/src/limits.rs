//! Rate limits and connection quotas

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use brokapi_core::{ConfigError, Error};
use serde::Deserialize;

/// Name of a token bucket in a limits file, for example `"global"` or `"orders"`.
pub type BucketId = String;

/// Name of an operation in the `route` table, for example `"PlaceOrder"`.
///
/// The names are chosen by the connector that calls [`BrokerLimiter::acquire`]. A name that has no
/// entry in the route table falls back to [`DEFAULT_ROUTE`].
pub type MethodId = String;

/// Route key for "any method that has no route of its own".
///
/// The limits files set it to the broker's general bucket, so a call made through `raw()` for a
/// method the library never named still spends quota instead of going out for free. If the route
/// table has no `"*"` entry, an unnamed method spends nothing.
pub const DEFAULT_ROUTE: &str = "*";

/// Token-bucket configuration for one limit bucket.
///
/// A bucket starts full with `burst` tokens. It gains `rate` tokens every `per`, continuously (not
/// in steps), and never holds more than `burst`. One call spends one token. So a quiet client may
/// send `burst` calls at once, and a busy one is held to `rate` calls per `per`.
///
/// In a limits file the three keys are written as `rate = 10`, `per = "1s"`, `burst = 3`. The
/// `per` value is an integer followed by `ms`, `s` or `m` (see [`BrokerLimits::from_toml_str`]).
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_runtime::BucketCfg;
///
/// let cfg = BucketCfg { rate: 10, per: Duration::from_secs(1), burst: 3 };
/// assert_eq!(cfg.burst, 3);
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BucketCfg {
    /// Tokens added per `per`. Must be greater than zero for the bucket to ever refill.
    pub rate: u32,
    /// The period in which `rate` tokens are added.
    #[serde(with = "humantime_serde")]
    pub per: Duration,
    /// Bucket capacity, and the number of tokens it holds at start.
    pub burst: u32,
}

/// Connection and subscription quotas of one broker user.
///
/// [`BrokerLimiter`] only stores and returns these numbers (see [`BrokerLimiter::conn_quota`]).
/// It does not open or close connections. A connector decides what to do with them. For example
/// the Finam connector reads `max_subs_per_conn` and refuses a new stream with
/// [`brokapi_core::Error::Rejected`] once the quota is used. The other fields are carried for the
/// connectors that need them and are not enforced by the runtime.
///
/// # Examples
///
/// ```
/// use brokapi_runtime::ConnQuota;
///
/// let q = ConnQuota::default();
/// assert_eq!((q.max_conns, q.max_subs_per_conn), (2, 50));
/// assert!(!q.supports_multiplex);
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ConnQuota {
    /// How many connections the broker lets one user hold at the same time.
    pub max_conns: usize,
    /// How many subscriptions (streams) one connection may carry.
    pub max_subs_per_conn: usize,
    /// Whether one connection can carry several subscriptions at once.
    pub supports_multiplex: bool,
    /// Connections kept for trading commands only, so a flood of market data cannot delay an order.
    /// Optional in a limits file, `0` when absent. Not read by the runtime.
    #[serde(default)]
    pub dedicated_command_conns: usize,
}

impl Default for ConnQuota {
    /// Cautious values: 2 connections, 50 subscriptions per connection, no multiplexing, no
    /// dedicated command connections.
    fn default() -> Self {
        Self {
            max_conns: 2,
            max_subs_per_conn: 50,
            supports_multiplex: false,
            dedicated_command_conns: 0,
        }
    }
}

/// The full limit policy of one broker user: token buckets, routes and connection quotas.
///
/// Normally read from the `[broker]` section of a limits file by [`from_toml_str`](Self::from_toml_str)
/// or [`from_toml_section`](Self::from_toml_section), then handed to [`BrokerLimiter::new`].
///
/// Limits belong to the broker user (the token or account), not to a connector. Build one
/// [`BrokerLimiter`] per user and share it between all connectors of that user. Two limiters for
/// one broker limit would each allow the full rate, so together they would exceed the quota the
/// broker gave.
///
/// # Examples
///
/// ```
/// use brokapi_runtime::BrokerLimits;
///
/// let raw = r#"
/// [demo.buckets.orders]
/// rate = 10
/// per = "1s"
/// burst = 3
///
/// [demo.route]
/// PlaceOrder = ["orders"]
///
/// [demo.conns]
/// max_conns = 2
/// max_subs_per_conn = 50
/// supports_multiplex = false
/// "#;
/// let limits = BrokerLimits::from_toml_str(raw)?;
/// assert_eq!(limits.buckets["orders"].burst, 3);
/// assert_eq!(limits.buckets_for("PlaceOrder"), vec!["orders".to_string()]);
/// # Ok::<(), brokapi_core::ConfigError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BrokerLimits {
    /// The token buckets by name.
    pub buckets: HashMap<BucketId, BucketCfg>,
    /// For each method name, the buckets one call spends from (one token from each). A bucket
    /// named here that is missing from `buckets` is a configuration error found at
    /// [`BrokerLimiter::acquire`].
    pub route: HashMap<MethodId, Vec<BucketId>>,
    /// Connection and subscription quotas.
    pub conns: ConnQuota,
}

/// Top-level TOML envelope: one section per broker (`[bcs]`, `[tbank]`, `[finam]`).
type LimitsFile = HashMap<String, BrokerLimits>;

impl BrokerLimits {
    /// Reads a limits file that holds exactly one broker section.
    ///
    /// The file layout is one top-level table per broker, with `buckets`, `route` and `conns`
    /// below it: `[bcs.buckets.global]`, `[bcs.route]`, `[bcs.conns]`. A `per` value is an integer
    /// followed by `ms`, `s` or `m` (`"250ms"`, `"1s"`, `"1m"`). Fractions (`"1.5s"`), other
    /// units (`"1h"`) and spaces inside (`"1 s"`) are refused. `buckets` and `route` must be
    /// present. `dedicated_command_conns` may be left out. Keys the parser does not know are
    /// ignored, so a misspelled optional key is silently lost.
    ///
    /// Use [`from_toml_section`](Self::from_toml_section) for a file that holds several brokers.
    ///
    /// # Errors
    ///
    /// Returns [`brokapi_core::ConfigError::Message`] when the text is not valid TOML, when a
    /// required key is missing or has the wrong type, when `per` cannot be parsed, or when the file
    /// has zero or several broker sections.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_runtime::BrokerLimits;
    ///
    /// let raw = r#"
    /// [demo.buckets.global]
    /// rate = 100
    /// per = "1s"
    /// burst = 20
    /// [demo.route]
    /// "*" = ["global"]
    /// [demo.conns]
    /// max_conns = 4
    /// max_subs_per_conn = 50
    /// supports_multiplex = false
    /// "#;
    /// let limits = BrokerLimits::from_toml_str(raw)?;
    /// assert_eq!(limits.conns.max_conns, 4);
    /// assert_eq!(limits.conns.dedicated_command_conns, 0);
    ///
    /// // An unsupported duration unit is a configuration error, not a silent default.
    /// let bad = raw.replace("\"1s\"", "\"1h\"");
    /// assert!(BrokerLimits::from_toml_str(&bad).is_err());
    ///
    /// // Two broker sections: pick one with `from_toml_section`.
    /// let two = format!("{raw}\n{}", raw.replace("demo", "other"));
    /// assert!(BrokerLimits::from_toml_str(&two).is_err());
    /// assert!(BrokerLimits::from_toml_section(&two, "other").is_ok());
    /// # Ok::<(), brokapi_core::ConfigError>(())
    /// ```
    pub fn from_toml_str(raw: &str) -> Result<Self, ConfigError> {
        let mut file: LimitsFile =
            toml::from_str(raw).map_err(|e| ConfigError::Message(e.to_string()))?;
        if file.len() != 1 {
            return Err(ConfigError::Message(format!(
                "limits file must have exactly one broker section, found {}; use from_toml_section",
                file.len()
            )));
        }
        Ok(file.drain().next().expect("one section").1)
    }

    /// Reads one named broker section (for example `"tbank"`) of a limits file.
    ///
    /// Other sections are parsed too, so they must be valid, but they are not returned. The
    /// format is the same as in [`from_toml_str`](Self::from_toml_str).
    ///
    /// # Errors
    ///
    /// Returns [`brokapi_core::ConfigError::Message`] for invalid TOML or an invalid section, and
    /// when the file has no `[section]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_runtime::BrokerLimits;
    ///
    /// let raw = r#"
    /// [a.buckets.g]
    /// rate = 1
    /// per = "1s"
    /// burst = 1
    /// [a.route]
    /// "*" = ["g"]
    /// [a.conns]
    /// max_conns = 1
    /// max_subs_per_conn = 1
    /// supports_multiplex = false
    /// "#;
    /// assert!(BrokerLimits::from_toml_section(raw, "a").is_ok());
    /// assert!(BrokerLimits::from_toml_section(raw, "b").is_err());
    /// ```
    pub fn from_toml_section(raw: &str, section: &str) -> Result<Self, ConfigError> {
        let mut file: LimitsFile =
            toml::from_str(raw).map_err(|e| ConfigError::Message(e.to_string()))?;
        file.remove(section)
            .ok_or_else(|| ConfigError::Message(format!("no [{section}] section in limits file")))
    }

    /// Reads a limits file from disk. The file must hold exactly one broker section.
    ///
    /// Same as [`from_toml_str`](Self::from_toml_str) after reading the file.
    ///
    /// # Errors
    ///
    /// Returns [`brokapi_core::ConfigError::Message`] when the file cannot be read (the message
    /// names the path), and for every reason [`from_toml_str`](Self::from_toml_str) lists.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_runtime::BrokerLimits;
    ///
    /// let err = BrokerLimits::from_toml_path("/definitely/not/here.toml").unwrap_err();
    /// assert!(err.to_string().contains("/definitely/not/here.toml"));
    /// ```
    pub fn from_toml_path(path: &str) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Message(format!("read {path}: {e}")))?;
        Self::from_toml_str(&raw)
    }

    /// The buckets a call to `method` spends from: its own route, else the [`DEFAULT_ROUTE`]
    /// route, else none.
    ///
    /// The ids come back sorted and without repeats. This is the order in which
    /// [`BrokerLimiter::acquire`] takes the tokens. The ids are not checked against
    /// [`buckets`](Self::buckets).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use brokapi_runtime::{BrokerLimits, ConnQuota, DEFAULT_ROUTE};
    ///
    /// let limits = BrokerLimits {
    ///     buckets: HashMap::new(),
    ///     route: HashMap::from([
    ///         ("PlaceOrder".to_string(), vec!["orders".to_string(), "global".to_string()]),
    ///         (DEFAULT_ROUTE.to_string(), vec!["global".to_string()]),
    ///     ]),
    ///     conns: ConnQuota::default(),
    /// };
    /// assert_eq!(limits.buckets_for("PlaceOrder"), ["global", "orders"]);
    /// assert_eq!(limits.buckets_for("SomethingNew"), ["global"]);
    /// ```
    pub fn buckets_for(&self, method: &str) -> Vec<BucketId> {
        let mut ids = self
            .route
            .get(method)
            .or_else(|| self.route.get(DEFAULT_ROUTE))
            .cloned()
            .unwrap_or_default();
        ids.sort();
        ids.dedup();
        ids
    }
}

#[derive(Debug)]
struct TokenBucket {
    cfg: BucketCfg,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(cfg: BucketCfg, now: Instant) -> Self {
        Self {
            tokens: cfg.burst as f64,
            last_refill: now,
            cfg,
        }
    }

    fn refill(&mut self, now: Instant) {
        if now <= self.last_refill {
            return;
        }
        let elapsed = now.duration_since(self.last_refill);
        let rate_per_sec = self.cfg.rate as f64 / self.cfg.per.as_secs_f64().max(1e-9);
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() * rate_per_sec).min(self.cfg.burst as f64);
        self.last_refill = now;
    }

    fn try_acquire(&mut self, now: Instant) -> Result<(), Duration> {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            let deficit = 1.0 - self.tokens;
            let rate_per_sec = self.cfg.rate as f64 / self.cfg.per.as_secs_f64().max(1e-9);
            let wait_secs = deficit / rate_per_sec.max(1e-9);
            Err(Duration::from_secs_f64(wait_secs))
        }
    }

    fn release(&mut self) {
        self.tokens = (self.tokens + 1.0).min(self.cfg.burst as f64);
    }
}

/// Shared multi-bucket rate limiter: one instance per broker user.
///
/// Every request a connector sends first calls [`acquire`](Self::acquire) with the name of the
/// method. The limiter spends one token from each bucket on the method's route, or spends nothing
/// and refuses. It never waits and never sends anything. The caller decides whether to wait for
/// `retry_after` and try again.
///
/// Give the same `Arc<BrokerLimiter>` to every connector of one broker user (market data and user
/// data together). The broker counts calls per user, so two limiters for one user would each let
/// through the full rate and together exceed it. A different user (another token) needs its own
/// limiter.
///
/// The limiter is thread-safe. Its internal lock is held only for a short, non-blocking update.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use std::time::{Duration, Instant};
/// use brokapi_core::Error;
/// use brokapi_runtime::{BrokerLimiter, BrokerLimits, BucketCfg, ConnQuota};
///
/// // One bucket of 2 tokens that refills 1 token per second. PlaceOrder spends from it.
/// let limits = BrokerLimits {
///     buckets: HashMap::from([(
///         "orders".to_string(),
///         BucketCfg { rate: 1, per: Duration::from_secs(1), burst: 2 },
///     )]),
///     route: HashMap::from([("PlaceOrder".to_string(), vec!["orders".to_string()])]),
///     conns: ConnQuota::default(),
/// };
/// let limiter = BrokerLimiter::new(&limits);
///
/// let t0 = Instant::now();
/// assert!(limiter.acquire("PlaceOrder", t0).is_ok());
/// assert!(limiter.acquire("PlaceOrder", t0).is_ok());
/// // The burst is spent: the third call is refused, nothing was sent.
/// match limiter.acquire("PlaceOrder", t0) {
///     Err(Error::RateLimited { retry_after }) => assert_eq!(retry_after, Duration::from_secs(1)),
///     other => panic!("expected RateLimited, got {other:?}"),
/// }
/// // A second later one token is back.
/// assert!(limiter.acquire("PlaceOrder", t0 + Duration::from_secs(1)).is_ok());
/// ```
pub struct BrokerLimiter {
    inner: Mutex<LimiterInner>,
}

struct LimiterInner {
    buckets: HashMap<BucketId, TokenBucket>,
    route: HashMap<MethodId, Vec<BucketId>>,
    conns: ConnQuota,
}

impl BrokerLimiter {
    /// Creates a limiter from a policy. Every bucket starts full.
    ///
    /// The limiter copies the policy, so later changes to `cfg` have no effect on it. Returns an
    /// [`Arc`] because the limiter is meant to be shared between connectors.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_runtime::{BrokerLimiter, BrokerLimits};
    ///
    /// # let raw = r#"
    /// # [demo.buckets.global]
    /// # rate = 100
    /// # per = "1s"
    /// # burst = 20
    /// # [demo.route]
    /// # "*" = ["global"]
    /// # [demo.conns]
    /// # max_conns = 4
    /// # max_subs_per_conn = 50
    /// # supports_multiplex = false
    /// # "#;
    /// let limits = BrokerLimits::from_toml_str(raw)?;
    /// let limiter = BrokerLimiter::new(&limits);
    /// // Clone the Arc for each connector of the same broker user.
    /// let for_market_data = std::sync::Arc::clone(&limiter);
    /// assert_eq!(for_market_data.conn_quota().max_conns, 4);
    /// # Ok::<(), brokapi_core::ConfigError>(())
    /// ```
    pub fn new(cfg: &BrokerLimits) -> Arc<Self> {
        let now = Instant::now();
        let buckets = cfg
            .buckets
            .iter()
            .map(|(id, bucket_cfg)| (id.clone(), TokenBucket::new(bucket_cfg.clone(), now)))
            .collect();
        Arc::new(Self {
            inner: Mutex::new(LimiterInner {
                buckets,
                route: cfg.route.clone(),
                conns: cfg.conns.clone(),
            }),
        })
    }

    /// The connection quota from the policy this limiter was built with.
    ///
    /// The limiter does not enforce it, see [`ConnQuota`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use brokapi_runtime::{BrokerLimiter, BrokerLimits, ConnQuota};
    ///
    /// let limits = BrokerLimits {
    ///     buckets: HashMap::new(),
    ///     route: HashMap::new(),
    ///     conns: ConnQuota { max_subs_per_conn: 7, ..ConnQuota::default() },
    /// };
    /// assert_eq!(BrokerLimiter::new(&limits).conn_quota().max_subs_per_conn, 7);
    /// ```
    pub fn conn_quota(&self) -> ConnQuota {
        self.inner.lock().expect("limiter poisoned").conns.clone()
    }

    /// Spends one token from every bucket on the route of `method`, all or nothing.
    ///
    /// The route is the method's own entry, else the [`DEFAULT_ROUTE`] entry, else empty (then the
    /// call is free). Buckets are taken in sorted id order. If any bucket is empty, the tokens
    /// already taken from the earlier buckets are given back (up to each bucket's `burst`) and the
    /// call is refused. A refused call therefore costs nothing.
    ///
    /// `now` is the current time. Pass `Instant::now()` in production. Tests pass a made-up
    /// instant to move time without sleeping. Time must not go backwards: an instant earlier than
    /// the last one seen refills nothing.
    ///
    /// The `retry_after` of a refusal is the time the first empty bucket (in sorted order) needs
    /// to get one token back. Another bucket on the route may need longer, so a retry after
    /// `retry_after` can be refused again.
    ///
    /// A refusal is local: nothing was sent to the broker, so repeating the call later cannot
    /// create a duplicate order.
    ///
    /// # Errors
    ///
    /// - [`brokapi_core::Error::RateLimited`] when a bucket on the route has less than one token.
    /// - [`brokapi_core::Error::Config`] when the route names a bucket that is not defined. No
    ///   token is spent in this case.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use std::time::{Duration, Instant};
    /// use brokapi_core::Error;
    /// use brokapi_runtime::{BrokerLimiter, BrokerLimits, BucketCfg, ConnQuota};
    ///
    /// let one_per_sec = |burst| BucketCfg { rate: 1, per: Duration::from_secs(1), burst };
    /// let limits = BrokerLimits {
    ///     buckets: HashMap::from([
    ///         ("global".to_string(), one_per_sec(10)),
    ///         ("orders".to_string(), one_per_sec(1)),
    ///     ]),
    ///     route: HashMap::from([
    ///         ("PlaceOrder".to_string(), vec!["global".to_string(), "orders".to_string()]),
    ///         ("GetOrder".to_string(), vec!["global".to_string()]),
    ///         ("Broken".to_string(), vec!["nope".to_string()]),
    ///     ]),
    ///     conns: ConnQuota::default(),
    /// };
    /// let limiter = BrokerLimiter::new(&limits);
    /// let t0 = Instant::now();
    ///
    /// assert!(limiter.acquire("PlaceOrder", t0).is_ok());
    /// // "orders" is empty now. The refusal gives back the token taken from "global".
    /// assert!(matches!(limiter.acquire("PlaceOrder", t0), Err(Error::RateLimited { .. })));
    /// // A method with no route is free.
    /// assert!(limiter.acquire("Unlisted", t0).is_ok());
    /// // A route to a bucket that does not exist is a configuration error.
    /// assert!(matches!(limiter.acquire("Broken", t0), Err(Error::Config(_))));
    /// ```
    pub fn acquire(&self, method: &str, now: Instant) -> Result<(), Error> {
        let mut inner = self.inner.lock().expect("limiter poisoned");
        let bucket_ids = {
            let mut ids = inner
                .route
                .get(method)
                .or_else(|| inner.route.get(DEFAULT_ROUTE))
                .cloned()
                .unwrap_or_default();
            ids.sort();
            ids.dedup();
            ids
        };

        // A route to a bucket that does not exist is refused before any token is taken, so the
        // buckets that sort ahead of the missing one do not lose a token for nothing.
        if let Some(missing) = bucket_ids
            .iter()
            .find(|id| !inner.buckets.contains_key(*id))
        {
            return Err(Error::Config(ConfigError::Message(format!(
                "unknown bucket {missing}"
            ))));
        }

        let mut acquired = Vec::new();
        for id in bucket_ids {
            let bucket = inner.buckets.get_mut(&id).ok_or_else(|| {
                Error::Config(ConfigError::Message(format!("unknown bucket {id}")))
            })?;
            match bucket.try_acquire(now) {
                Ok(()) => acquired.push(id),
                Err(retry_after) => {
                    for rollback in acquired.iter().rev() {
                        if let Some(b) = inner.buckets.get_mut(rollback) {
                            b.release();
                        }
                    }
                    return Err(Error::RateLimited { retry_after });
                }
            }
        }
        Ok(())
    }
}

mod humantime_serde {
    use serde::{Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }

    fn parse_duration(s: &str) -> Result<Duration, String> {
        let s = s.trim();
        if let Some(num) = s.strip_suffix("ms") {
            let v: u64 = num
                .parse()
                .map_err(|e: std::num::ParseIntError| e.to_string())?;
            return Ok(Duration::from_millis(v));
        }
        if let Some(num) = s.strip_suffix('s') {
            let v: u64 = num
                .parse()
                .map_err(|e: std::num::ParseIntError| e.to_string())?;
            return Ok(Duration::from_secs(v));
        }
        if let Some(num) = s.strip_suffix('m') {
            let v: u64 = num
                .parse()
                .map_err(|e: std::num::ParseIntError| e.to_string())?;
            return Ok(Duration::from_secs(v * 60));
        }
        Err(format!("unsupported duration: {s}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_limits() -> BrokerLimits {
        BrokerLimits {
            buckets: HashMap::from([
                (
                    "global".into(),
                    BucketCfg {
                        rate: 10,
                        per: Duration::from_secs(1),
                        burst: 10,
                    },
                ),
                (
                    "orders".into(),
                    BucketCfg {
                        rate: 2,
                        per: Duration::from_secs(1),
                        burst: 2,
                    },
                ),
            ]),
            route: HashMap::from([
                ("PlaceOrder".into(), vec!["global".into(), "orders".into()]),
                ("CancelOrder".into(), vec!["global".into()]),
            ]),
            conns: ConnQuota::default(),
        }
    }

    #[test]
    fn sequential_acquire_rolls_back_on_failure() {
        let limiter = BrokerLimiter::new(&sample_limits());
        let t0 = Instant::now();
        assert!(limiter.acquire("PlaceOrder", t0).is_ok());
        assert!(limiter.acquire("PlaceOrder", t0).is_ok());
        assert!(matches!(
            limiter.acquire("PlaceOrder", t0),
            Err(Error::RateLimited { .. })
        ));
        assert!(limiter.acquire("CancelOrder", t0).is_ok());
    }

    #[test]
    fn unknown_method_falls_back_to_the_default_route() {
        let mut limits = sample_limits();
        // Without a default route an unnamed method is free (the old behavior).
        let free = BrokerLimiter::new(&limits);
        let t0 = Instant::now();
        for _ in 0..50 {
            assert!(free.acquire("SomethingNew", t0).is_ok());
        }
        limits
            .route
            .insert(DEFAULT_ROUTE.into(), vec!["global".into(), "orders".into()]);
        assert_eq!(
            limits.buckets_for("SomethingNew"),
            vec!["global".to_string(), "orders".to_string()]
        );
        // A method with its own route keeps it.
        assert_eq!(
            limits.buckets_for("CancelOrder"),
            vec!["global".to_string()]
        );
        let limiter = BrokerLimiter::new(&limits);
        assert!(limiter.acquire("SomethingNew", t0).is_ok());
        assert!(limiter.acquire("SomethingNew", t0).is_ok());
        assert!(matches!(
            limiter.acquire("SomethingNew", t0),
            Err(Error::RateLimited { .. })
        ));
        // The shared bucket is the same one the named methods spend.
        assert!(matches!(
            limiter.acquire("PlaceOrder", t0),
            Err(Error::RateLimited { .. })
        ));
    }

    #[test]
    fn parses_bcs_limits_toml() {
        let raw = include_str!("../../../configs/bcs_limits.toml");
        let limits = BrokerLimits::from_toml_str(raw).expect("parse");
        assert!(limits.buckets.contains_key("global"));
        assert!(limits.route.contains_key("PlaceOrder"));
    }

    #[test]
    fn unknown_bucket_in_route_spends_nothing() {
        let mut limits = sample_limits();
        // "global" sorts ahead of "zzz-missing": it must not lose a token to the failed call.
        limits
            .route
            .insert("Broken".into(), vec!["global".into(), "zzz-missing".into()]);
        let limiter = BrokerLimiter::new(&limits);
        let t0 = Instant::now();
        for _ in 0..50 {
            assert!(matches!(
                limiter.acquire("Broken", t0),
                Err(Error::Config(_))
            ));
        }
        // The global bucket (burst 10) is still full.
        for _ in 0..10 {
            assert!(limiter.acquire("CancelOrder", t0).is_ok());
        }
        assert!(matches!(
            limiter.acquire("CancelOrder", t0),
            Err(Error::RateLimited { .. })
        ));
    }
}
