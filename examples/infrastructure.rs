//! One process, three brokers, four accounts: a shared event channel, one order journal in a file, and a restart
//! that reconciles every account against that journal.
//!
//! The code of the page "Как собрать инфраструктуру на несколько брокеров и счетов" of the book
//! (docs/src/rust/howto-infrastructure.md). The accounts are BCS (one), T-Bank (two, one token) and Finam (one). `run` plays
//! two lives of the process against the in-process imitations: the first places an order on every account and goes away
//! without cancelling (and leaves a journal line for an order that was never sent), while the process is down one order
//! is filled, the second life reconciles every account with the journal and finds out what became of everything.
//! Nothing here touches the network or money.
//!
//! ```text
//! cargo run --example infrastructure
//! ```
#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

// ANCHOR: imports
use brokapi::core::{
    ClientOrderId, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentQuery,
    InstrumentSource, NativeId, OrderExecutor, OrderState, OrderType, PlaceOrderRequest,
    PortfolioSource, Price, Qty, ReconcileReport, ShutdownMode, Side,
};
use brokapi::runtime::BrokerLimiter;
use tokio::sync::mpsc;
// ANCHOR_END: imports

// ANCHOR: journal
/// What the journal knows about an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Written BEFORE `place`. After a crash the order may or may not exist at the broker.
    Sending,
    /// The broker has it and it works.
    Working,
    /// Final, with the text of how it ended.
    Done(String),
}

/// The order journal: a text file, one line per change, `account TAB cid TAB state TAB detail`. The last line of a cid
/// wins. Every line is flushed to disk before the call that needs it: a journal that is written after `place` loses
/// the order that a crash cuts in between.
pub struct Journal {
    path: PathBuf,
    file: File,
    orders: HashMap<ClientOrderId, (String, State)>,
}

impl Journal {
    /// Opens the file (creates it if there is none) and reads what an earlier life wrote.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut orders = HashMap::new();
        if path.exists() {
            for line in BufReader::new(File::open(path)?).lines() {
                let line = line?;
                let mut parts = line.splitn(4, '\t');
                let (Some(account), Some(cid), Some(state)) =
                    (parts.next(), parts.next(), parts.next())
                else {
                    continue; // a line cut by a crash: it carries no complete change
                };
                let Ok(cid) = cid.parse::<ClientOrderId>() else {
                    continue;
                };
                let state = match state {
                    "sending" => State::Sending,
                    "working" => State::Working,
                    _ => State::Done(parts.next().unwrap_or("").to_string()),
                };
                orders.insert(cid, (account.to_string(), state));
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            orders,
        })
    }

    /// Writes a change and waits until the disk has it.
    pub fn set(&mut self, account: &str, cid: ClientOrderId, state: State) -> io::Result<()> {
        let (word, detail) = match &state {
            State::Sending => ("sending", String::new()),
            State::Working => ("working", String::new()),
            State::Done(text) => ("done", text.replace(['\t', '\n', '\r'], " ")),
        };
        writeln!(self.file, "{account}\t{cid}\t{word}\t{detail}")?;
        self.file.sync_data()?;
        self.orders.insert(cid, (account.to_string(), state));
        Ok(())
    }

    pub fn state(&self, cid: ClientOrderId) -> Option<&State> {
        self.orders.get(&cid).map(|(_, s)| s)
    }

    /// The ledger of one account for `reconcile`: orders it believes are alive.
    pub fn live(&self, account: &str) -> Vec<ClientOrderId> {
        self.orders
            .iter()
            .filter(|(_, (a, s))| a == account && !matches!(s, State::Done(_)))
            .map(|(cid, _)| *cid)
            .collect()
    }
}
// ANCHOR_END: journal

// ANCHOR: settings
/// What the process needs to connect. In production it comes from the environment (`from_env`), in the example from
/// the imitations.
pub struct Settings {
    pub bcs: brokapi::bcs::protocol::config::BcsConfig,
    pub tbank: brokapi::tbank::protocol::TbankConfig,
    /// Two accounts behind one T-Bank token.
    pub tbank_accounts: [String; 2],
    pub finam: brokapi::finam::FinamApiConfig,
    pub finam_account: String,
}

impl Settings {
    /// BCS_REFRESH_TOKEN, BCS_IS_WRITE, TBANK_TOKEN, TBANK_SANDBOX, TBANK_ACCOUNT_ID, TBANK_ACCOUNT_ID_2, FINAM_SECRET,
    /// FINAM_ACCOUNT_ID.
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        dotenv::dotenv().ok();
        let (finam, account) = brokapi::finam::api_config_from_env()?;
        Ok(Self {
            bcs: brokapi::bcs::protocol::config::BcsConfig::from_env()?,
            tbank: brokapi::tbank::protocol::TbankConfig::from_env()?,
            tbank_accounts: [
                std::env::var("TBANK_ACCOUNT_ID")?,
                std::env::var("TBANK_ACCOUNT_ID_2")?,
            ],
            finam,
            finam_account: account.ok_or("FINAM_ACCOUNT_ID is not set")?,
        })
    }
}
// ANCHOR_END: settings

// ANCHOR: account
/// What the process needs from an account, whatever the broker is. A blanket impl makes every connector one.
pub trait Account: OrderExecutor + ExecutionSource + PortfolioSource {}
impl<T: OrderExecutor + ExecutionSource + PortfolioSource> Account for T {}

/// How a `place` ended, for the caller.
#[derive(Debug, PartialEq, Eq)]
pub enum Placed {
    /// The broker accepted it.
    Working,
    /// The broker refused it. No order exists.
    Refused(String),
    /// The command did not leave the process (gate closed, quota). No order exists, the cid is not spent.
    NotSent(String),
    /// The outcome is unknown. The journal says `Sending`; the next reconcile or `order_state` settles it.
    Unknown,
}
// ANCHOR_END: account

// ANCHOR: infra
/// All the connectors of the process and the journal.
pub struct Infra {
    pub bcs_md: brokapi::bcs::MarketDataConnector,
    pub tbank_md: brokapi::tbank::MarketDataConnector,
    pub finam_md: brokapi::finam::MarketDataConnector,
    pub bcs: brokapi::bcs::UserDataConnector,
    pub tbank_a: brokapi::tbank::UserDataConnector,
    pub tbank_b: brokapi::tbank::UserDataConnector,
    pub finam: brokapi::finam::UserDataConnector,
    pub journal: Journal,
    /// Accounts that lost events (`Overflow`): reconcile them before trading.
    pub degraded: Vec<&'static str>,
}

/// The names of the accounts: the journal key and the `source` label of the envelopes.
pub const ACCOUNTS: [&str; 4] = ["bcs", "tbank-a", "tbank-b", "finam"];

impl Infra {
    /// Connects everything, subscribes, reconciles every account with the journal, and only then returns.
    pub async fn start(
        settings: &Settings,
        journal_path: &Path,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Envelope>), Box<dyn std::error::Error>> {
        use brokapi::{bcs, finam, tbank};

        // One limiter per area where the broker counts a limit. T-Bank counts per user: both accounts and the market
        // data connector share one. BCS counts per token, Finam per user. The three brokers never share.
        let bcs_limits = BrokerLimiter::new(&bcs::limits::default_limits());
        let tbank_limits = BrokerLimiter::new(&tbank::limits::default_limits());
        let finam_limits = BrokerLimiter::new(&finam::limits::default_limits());

        // One channel for everything: the `source` label of an envelope says whose it is. A small task moves the
        // envelopes into the process's own queue at once, so the library's channel is always empty and the process
        // may wait for the brokers as long as it needs (rule 5 of the contract).
        let (tx, channel) = mpsc::channel::<Envelope>(8192);
        let events = forward(channel);

        // Finam: one secret, one JWT for the market data and the account.
        let finam_api = settings
            .finam
            .clone()
            .with_tokens(settings.finam.shared_tokens().await?);

        let infra = Self {
            bcs_md: bcs::MarketDataConnector::start(
                bcs::MarketDataConfig::new(settings.bcs.clone(), "bcs-md"),
                bcs_limits.clone(),
                tx.clone(),
            )
            .await?,
            tbank_md: tbank::MarketDataConnector::start(
                tbank::MarketDataConfig::new(settings.tbank.clone(), "tbank-md"),
                tbank_limits.clone(),
                tx.clone(),
            )
            .await?,
            finam_md: finam::MarketDataConnector::start(
                finam::MarketDataConfig::new(finam_api.clone(), "finam-md"),
                finam_limits.clone(),
                tx.clone(),
            )
            .await?,
            // The `source` of an account connector is the account name.
            bcs: bcs::UserDataConnector::start(
                bcs::UserDataConfig::new(settings.bcs.clone(), "bcs"),
                bcs_limits,
                tx.clone(),
            )
            .await?,
            tbank_a: tbank::UserDataConnector::start(
                tbank::UserDataConfig::new(
                    settings
                        .tbank
                        .clone()
                        .with_account(&settings.tbank_accounts[0]),
                    "tbank-a",
                ),
                tbank_limits.clone(),
                tx.clone(),
            )
            .await?,
            tbank_b: tbank::UserDataConnector::start(
                tbank::UserDataConfig::new(
                    settings
                        .tbank
                        .clone()
                        .with_account(&settings.tbank_accounts[1]),
                    "tbank-b",
                ),
                tbank_limits,
                tx.clone(),
            )
            .await?,
            finam: finam::UserDataConnector::start(
                finam::UserDataConfig::new(finam_api, &settings.finam_account, "finam"),
                finam_limits,
                tx,
            )
            .await?,
            journal: Journal::open(journal_path)?,
            degraded: Vec::new(),
        };

        // Subscribe to the statuses and fills of every account FIRST, then reconcile it with ITS part of the journal.
        for (_, account) in infra.accounts() {
            account.subscribe_order_state().await?;
            account.subscribe_executions().await?;
        }
        let mut infra = infra;
        for name in ACCOUNTS {
            infra.reconcile_retry(name).await?;
        }
        Ok((infra, events))
    }

    /// The accounts by name, as the trait the process needs.
    pub fn accounts(&self) -> [(&'static str, &dyn Account); 4] {
        [
            ("bcs", &self.bcs),
            ("tbank-a", &self.tbank_a),
            ("tbank-b", &self.tbank_b),
            ("finam", &self.finam),
        ]
    }

    pub fn account(&self, name: &str) -> &dyn Account {
        self.accounts()
            .into_iter()
            .find(|(n, _)| *n == name)
            .map(|(_, a)| a)
            .unwrap_or_else(|| panic!("no account {name}"))
    }
    // ANCHOR_END: infra

    // ANCHOR: recover
    /// A reconcile that does not give up on the first hiccup: a limit or a network error means "no sealed answer
    /// yet", so wait and ask again, a few times. Any other error is returned: a retry would fail the same way.
    pub async fn reconcile_retry(&mut self, name: &'static str) -> Result<ReconcileReport, Error> {
        let mut pause = Duration::from_millis(200);
        for attempt in 1..=5 {
            match self.reconcile(name).await {
                Ok(report) => return Ok(report),
                Err(Error::RateLimited { retry_after }) if attempt < 5 => {
                    tokio::time::sleep(retry_after).await
                }
                Err(Error::Transport(_)) if attempt < 5 => {
                    tokio::time::sleep(pause).await;
                    pause *= 2;
                }
                Err(other) => return Err(other),
            }
        }
        unreachable!("the last attempt returns")
    }

    /// After `Overflow` (the accounts in `degraded`): read the queue to the end, THEN reconcile each such account.
    /// The events that did arrive are the history up to the hole. The states after the hole come from the reconcile
    /// and `order_state`; fills since the hole from `executions(range)` if the process keeps its own totals.
    pub async fn recover(
        &mut self,
        events: &mut mpsc::UnboundedReceiver<Envelope>,
    ) -> Result<(), Error> {
        while let Ok(env) = events.try_recv() {
            self.route(env);
        }
        for name in std::mem::take(&mut self.degraded) {
            self.reconcile_retry(name).await?;
        }
        Ok(())
    }
    // ANCHOR_END: recover

    // ANCHOR: reconcile
    /// Reconciles one account with the journal and writes down what it found. The gate of the account opens only if
    /// this succeeds; if it fails, repeat it later and do not trade on that account meanwhile.
    pub async fn reconcile(&mut self, name: &'static str) -> Result<ReconcileReport, Error> {
        let ledger = self.journal.live(name);
        let report = self.account(name).reconcile(&ledger).await?;
        for order in &report.confirmed {
            self.write(name, order.cid, State::Working);
        }
        for cid in &report.closed_offline {
            // Not alive at the broker. Ask how it ended. "Not found" can also mean "never sent": a crash between the
            // journal line and the call. Both are final for the journal; a new order gets a new cid.
            let how = self.settle(name, *cid).await?;
            self.write(name, *cid, State::Done(how));
        }
        // report.foreign: orders nobody here placed. Shown, never touched.
        self.degraded.retain(|n| *n != name);
        Ok(report)
    }

    /// The final state of an order that is not alive any more, as text for the journal.
    async fn settle(&self, name: &str, cid: ClientOrderId) -> Result<String, Error> {
        // How long "not found" may still change after a restart is not measured on a real broker: here half a second
        // is enough for the imitations, take seconds for a real one.
        let deadline = tokio::time::Instant::now() + NOT_FOUND_PATIENCE;
        loop {
            match self.account(name).order_state(cid).await {
                Ok(order) => return Ok(state_word(&order.state).to_string()),
                // The broker does not know it. Right after a cut that is not final, so wait a little.
                Err(e) if is_not_found(&e) => {}
                Err(Error::RateLimited { .. } | Error::Transport(_)) => {}
                Err(other) => return Err(other),
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok("never reached the broker".to_string());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The journal must not lose a line. If the disk fails, trading must stop: there is nothing to say here.
    fn write(&mut self, name: &str, cid: ClientOrderId, state: State) {
        self.journal
            .set(name, cid, state)
            .expect("the journal must be writable");
    }
    // ANCHOR_END: reconcile

    // ANCHOR: place
    /// Places an order on an account: journal first, then the command, then the journal again.
    pub async fn place(&mut self, name: &'static str, req: PlaceOrderRequest) -> Placed {
        if self.degraded.contains(&name) {
            return Placed::NotSent("the account lost events, reconcile first".into());
        }
        let cid = req.cid;
        self.write(name, cid, State::Sending);
        match self.account(name).place(req).await {
            Ok(_) => {
                self.write(name, cid, State::Working);
                Placed::Working
            }
            Err(Error::Rejected { code, message }) => {
                self.write(name, cid, State::Done(format!("rejected {code}")));
                Placed::Refused(message)
            }
            // Nothing left the process: a closed gate, a local limit, a wrong setup, a missing capability, a stop.
            Err(
                e @ (Error::NotReconciled
                | Error::Degraded
                | Error::ShuttingDown
                | Error::RateLimited { .. }
                | Error::Config(_)
                | Error::Unsupported { .. }),
            ) => {
                self.write(name, cid, State::Done("not sent".into()));
                Placed::NotSent(e.to_string())
            }
            // Unknown or Transport: the order may exist. Leave `Sending`: it is in the ledger of the next reconcile.
            Err(Error::Unknown | Error::Transport(_)) => Placed::Unknown,
        }
    }
    // ANCHOR_END: place

    // ANCHOR: route
    /// One envelope of the shared channel: the label says whose it is, the payload what to do with it.
    pub fn route(&mut self, env: Envelope) {
        let Some(name) = ACCOUNTS.iter().copied().find(|n| **n == *env.source) else {
            return; // a market data connector: the strategy reads those, not the infrastructure
        };
        match env.payload {
            Event::Order(order) => {
                let state = match &order.state {
                    OrderState::Acked { .. } | OrderState::PartiallyFilled { .. } => State::Working,
                    OrderState::InFlight { .. } | OrderState::Unknown { .. } => return,
                    final_state => State::Done(state_word(final_state).to_string()),
                };
                self.write(name, order.cid, state);
            }
            Event::Overflow { .. } => self.degraded.push(name),
            _ => {}
        }
    }
    // ANCHOR_END: route

    // ANCHOR: stop
    /// Stops everything. Accounts first, so no command is taken while the market data is still closing. Orders at the
    /// brokers are not touched.
    pub async fn stop(self, mode: ShutdownMode) {
        self.bcs.shutdown(mode).await;
        self.tbank_a.shutdown(mode).await;
        self.tbank_b.shutdown(mode).await;
        self.finam.shutdown(mode).await;
        self.bcs_md.shutdown(mode).await;
        self.tbank_md.shutdown(mode).await;
        self.finam_md.shutdown(mode).await;
    }
    // ANCHOR_END: stop
}

// ANCHOR: helpers
/// How long a restarted process asks about an order the broker does not know, before it closes the journal line.
const NOT_FOUND_PATIENCE: Duration = Duration::from_millis(500);

/// The connectors write into a bounded channel and never wait for it: this task moves every envelope into the
/// process's own queue. The queue is unbounded, so watch its length.
fn forward(mut events: mpsc::Receiver<Envelope>) -> mpsc::UnboundedReceiver<Envelope> {
    let (queue_tx, queue) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(env) = events.recv().await {
            if queue_tx.send(env).is_err() {
                break;
            }
        }
    });
    queue
}

fn state_word(state: &OrderState) -> &'static str {
    match state {
        OrderState::InFlight { .. } => "in flight",
        OrderState::Unknown { .. } => "unknown",
        OrderState::Acked { .. } => "working",
        OrderState::PartiallyFilled { .. } => "partly filled",
        OrderState::Filled { .. } => "filled",
        OrderState::Cancelled { .. } => "cancelled",
        OrderState::Rejected { .. } => "rejected",
    }
}

/// "The broker does not know this order". All brokers say it as `Rejected` with the code `not_found`.
fn is_not_found(e: &Error) -> bool {
    e.is_not_found()
}
// ANCHOR_END: helpers

/// The instrument of each account in the form its broker wants. In real code every one comes from `find`.
pub async fn sber_for(
    infra: &Infra,
    name: &str,
) -> Result<InstrumentId, Box<dyn std::error::Error>> {
    use brokapi::core::InstrumentKey;
    Ok(match name {
        "bcs" => InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        ),
        "tbank-a" | "tbank-b" => {
            infra
                .tbank_md
                .find(InstrumentQuery::ByTicker {
                    ticker: "SBER".into(),
                    exchange: Some("TQBR".into()),
                })
                .await?
                .into_iter()
                .next()
                .ok_or("SBER is not in the T-Bank directory")?
                .id
        }
        _ => brokapi::finam::util::instrument_from_symbol("SBER@MISX"),
    })
}

fn buy(instrument: InstrumentId, cid: ClientOrderId) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid,
        instrument,
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(Price::from(200)),
    }
}

/// What the test looks at after the two lives.
#[derive(Debug)]
pub struct Result2 {
    pub cids: HashMap<&'static str, ClientOrderId>,
    pub orphan: ClientOrderId,
    pub journal: HashMap<ClientOrderId, State>,
    pub reports: HashMap<&'static str, (usize, usize, usize)>,
}

/// Reads the channel into the journal until `done` holds or `how_long` passes.
async fn pump(
    infra: &mut Infra,
    events: &mut mpsc::UnboundedReceiver<Envelope>,
    how_long: Duration,
    done: impl Fn(&Infra) -> bool,
) {
    let _ = tokio::time::timeout(how_long, async {
        while !done(infra) {
            match events.recv().await {
                Some(env) => infra.route(env),
                None => break,
            }
        }
    })
    .await;
}

/// The two lives of the process. `dir` holds the journal.
pub async fn run(dir: &Path) -> Result<Result2, Box<dyn std::error::Error>> {
    use brokapi_mocks::{bcs as mbcs, finam as mfinam, tbank as mtbank};

    // ANCHOR: mock_settings
    // The imitations live through both lives of the process, like the brokers do.
    let bcs_server = mbcs::MockServer::start(Default::default()).await;
    let tbank_server = mtbank::MockServer::start(Default::default()).await;
    let finam_server = mfinam::MockServer::start().await;
    let settings = Settings {
        bcs: brokapi::bcs::protocol::config::BcsConfig::for_mock(
            "mock-refresh-token",
            &bcs_server.http_base,
        )
        .with_write(true),
        tbank: brokapi::tbank::protocol::TbankConfig::for_mock(
            mtbank::TOKEN,
            &tbank_server.endpoint,
        ),
        tbank_accounts: [mtbank::ACCOUNT_A.into(), mtbank::ACCOUNT_B.into()],
        finam: brokapi::finam::FinamApiConfig::new(mfinam::SECRET)
            .with_endpoint(finam_server.url.clone()),
        finam_account: "ACC1".into(),
    };
    // ANCHOR_END: mock_settings
    let journal_path = dir.join("orders.journal");
    let _ = std::fs::remove_file(&journal_path);

    // ---- first life: an order on every account, then the process goes away without cancelling anything.
    let (mut infra, mut events) = Infra::start(&settings, &journal_path).await?;
    let mut cids = HashMap::new();
    for name in ACCOUNTS {
        let cid = ClientOrderId::new_v4();
        let instrument = sber_for(&infra, name).await?;
        let placed = infra.place(name, buy(instrument, cid)).await;
        assert_eq!(placed, Placed::Working, "{name}");
        cids.insert(name, cid);
    }
    // The journal line of an order that a crash cut off before `place` was called.
    let orphan = ClientOrderId::new_v4();
    infra.journal.set("tbank-b", orphan, State::Sending)?;
    pump(&mut infra, &mut events, Duration::from_secs(5), |i| {
        ACCOUNTS
            .iter()
            .all(|n| i.journal.state(cids[n]) == Some(&State::Working))
    })
    .await;
    infra.stop(ShutdownMode::Immediate).await;
    println!("first life over: the orders stay on the exchanges");

    // ---- while the process is down, the order of T-Bank account A is executed.
    tbank_server.fill(mtbank::ACCOUNT_A, &cids["tbank-a"].hyphenated().to_string());

    // ---- second life: the journal is read, every account is reconciled with its part of it.
    let (mut infra, mut events) = Infra::start(&settings, &journal_path).await?;
    let mut reports = HashMap::new();
    for name in ACCOUNTS {
        // `start` reconciled already; a second reconcile is cheap, safe, and shows the report.
        let r = infra.reconcile(name).await?;
        reports.insert(
            name,
            (r.confirmed.len(), r.closed_offline.len(), r.foreign.len()),
        );
    }
    // Take the remaining orders off before leaving (a choice of this process; the library never does it).
    for name in ["bcs", "tbank-b", "finam"] {
        infra.account(name).cancel(cids[name]).await?;
    }
    pump(&mut infra, &mut events, Duration::from_secs(5), |i| {
        ["bcs", "tbank-b", "finam"]
            .iter()
            .all(|n| matches!(i.journal.state(cids[n]), Some(State::Done(_))))
    })
    .await;
    let journal = ACCOUNTS
        .iter()
        .map(|n| cids[n])
        .chain([orphan])
        .filter_map(|c| infra.journal.state(c).cloned().map(|s| (c, s)))
        .collect();
    // An account that lost events is reconciled after the queue is read to the end.
    infra.degraded.push("bcs");
    infra.recover(&mut events).await?;
    assert!(infra.degraded.is_empty());
    infra
        .stop(ShutdownMode::Graceful {
            deadline: Duration::from_secs(5),
        })
        .await;
    Ok(Result2 {
        cids,
        orphan,
        journal,
        reports,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir();
    let r = run(&dir).await?;
    for (account, cid) in &r.cids {
        println!("{account}: {cid} -> {:?}", r.journal[cid]);
    }
    Ok(())
}
