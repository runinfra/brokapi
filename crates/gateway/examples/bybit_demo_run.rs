//! A live run of the whole stack on the demo account of Bybit: the service, a gateway, the commands, a bot, a killed
//! gateway and the reconcile after its restart.
//!
//! The demo account has virtual money, round the clock, and its own key (Bybit site: switch to "Demo Trading", then
//! the API page). Put the pair in `.env`:
//!
//! ```text
//! BYBIT_DEMO_API_KEY=...
//! BYBIT_DEMO_API_SECRET=...
//! ```
//!
//! and run it with one command (`tools/bybit-demo-run` builds what it needs):
//!
//! ```text
//! cargo run -p brokapi-gateway --example bybit_demo_run
//! ```
//!
//! What it does, step by step, and stops at the first step that does not hold:
//!
//! 1. `brokapi check` and `brokapi up -d` with a config of one gateway on the demo account, then `ps`.
//! 2. `quote` and `book` through the gateway.
//! 3. A bot (a client of the gateway): the account must be a demo one, funds are asked for if the wallet is empty, a limit
//!    order far from the market is placed, seen in the stream of orders and cancelled, a stop order is placed and listed.
//!    One limit order and the stop order are left standing.
//! 4. The process of the gateway is killed. The service starts it again, the gateway reconciles its journal with the
//!    exchange, and still knows the limit order.
//! 5. The bot connects again, reconciles with its ledger, finds both orders alive, cancels them, and `orders`, `stops` and
//!    `cancel-all` agree there is nothing left.
//! 6. `brokapi down`.
//!
//! Orders are placed only on an account the gateway reports as a demo one. Without the key pair the program says which
//! variables are missing and exits with code 2. A failed step exits with code 1 and tries to leave the account clean.
//!
//! `--self-test` runs the same steps against the in-process imitation of the exchange (no key, no network): it proves the
//! scenario itself, not the exchange. `--keep` leaves the temporary directory (config, state, logs) in place.

use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::time::Duration;

use brokapi::core::{
    ClientOrderId, Decimal, Envelope, Event, InstrumentId, InstrumentSource, OrderBook,
    OrderBookSource, OrderExecutor, OrderState, OrderType, PlaceOrderRequest,
    PlaceStopOrderRequest, PortfolioSource, Price, Qty, RoundMode, Side, StopExpiry,
    StopOrderExecutor, StopOrderKind, StopOrderRef, StopOrderState, SubscribeOpts,
};
use brokapi_gateway::RemoteClient;
use serde_json::{Value, json};
use tokio::sync::mpsc;

const GATEWAY: &str = "bybit-demo";
const STEPS: usize = 12;
const SYMBOL: &str = "BTCUSDT";
/// The wallet is topped up when the free USDT is below this.
const MIN_FUNDS: i64 = 1_000;
const WAIT: Duration = Duration::from_secs(60);

type Res<T = ()> = Result<T, String>;

struct Run {
    brokapi: PathBuf,
    config: PathBuf,
    dir: PathBuf,
    env: Vec<(String, String)>,
    step: usize,
    keep: bool,
    up: bool,
}

impl Run {
    fn say(&mut self, title: &str) {
        self.step += 1;
        println!("[{}/{STEPS}] {title}", self.step);
    }

    fn ok(&self, what: &str) {
        println!("      ok: {what}");
    }

    /// Runs `brokapi -c <config> --json <args>` and returns the output, or the reason it failed.
    async fn cmd(&self, args: &[&str]) -> Res<(i32, String, String)> {
        let mut c = tokio::process::Command::new(&self.brokapi);
        c.arg("-c").arg(&self.config).args(args);
        c.envs(self.env.iter().cloned());
        c.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let out = tokio::time::timeout(WAIT, c.output())
            .await
            .map_err(|_| format!("`brokapi {}` did not end in {WAIT:?}", args.join(" ")))?
            .map_err(|e| format!("cannot run {}: {e}", self.brokapi.display()))?;
        Ok((
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ))
    }

    /// A command that must end with code 0; its JSON output.
    async fn json(&self, args: &[&str]) -> Res<Value> {
        let mut all = vec!["--json"];
        all.extend_from_slice(args);
        let (code, out, err) = self.cmd(&all).await?;
        if code != 0 {
            return Err(format!(
                "`brokapi {}` ended with code {code}: {}{}",
                args.join(" "),
                err.trim(),
                out.trim()
            ));
        }
        serde_json::from_str(&out)
            .map_err(|e| format!("`brokapi {}` printed no JSON ({e}): {out}", args.join(" ")))
    }

    async fn gateway(&self) -> Res<Value> {
        let ps = self.json(&["ps"]).await?;
        ps.as_array()
            .and_then(|a| a.iter().find(|g| g["name"] == GATEWAY).cloned())
            .ok_or_else(|| format!("`ps` does not list the gateway {GATEWAY}: {ps}"))
    }

    /// Waits until the gateway is running and answers, as a different process than `not_pid` when that is given.
    async fn wait_running(&self, not_pid: Option<u64>, what: &str) -> Res<Value> {
        let end = tokio::time::Instant::now() + WAIT;
        let mut last;
        loop {
            match self.gateway().await {
                Ok(g) => {
                    last = g.to_string();
                    let running = g["state"] == "running" && g["health"]["state"] == "ready";
                    let pid = g["pid"].as_u64();
                    if running && pid.is_some() && pid != not_pid {
                        return Ok(g);
                    }
                }
                Err(e) => last = e,
            }
            if tokio::time::Instant::now() > end {
                return Err(format!(
                    "{what}: the gateway was not running and ready in {WAIT:?}; last seen: {last}\n{}",
                    self.log_tail()
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn log_tail(&self) -> String {
        let log = self
            .dir
            .join("state")
            .join("logs")
            .join(format!("{GATEWAY}.log"));
        match std::fs::read_to_string(&log) {
            Ok(t) => {
                let lines: Vec<&str> = t.lines().collect();
                let from = lines.len().saturating_sub(15);
                format!(
                    "--- the end of {}\n{}",
                    log.display(),
                    lines[from..].join("\n")
                )
            }
            Err(_) => format!("(no log at {})", log.display()),
        }
    }

    fn gateway_dir(&self) -> PathBuf {
        self.dir.join("state").join("gateways").join(GATEWAY)
    }

    /// Takes the account clean and stops the service. Used when a step fails and at the end.
    async fn clean_up(&mut self) {
        if self.up {
            let _ = self.cmd(&["cancel-all", GATEWAY]).await;
            let _ = self.cmd(&["down"]).await;
            self.up = false;
        }
        if self.keep {
            println!("kept: {}", self.dir.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn find_env_file(explicit: Option<&str>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        return Some(PathBuf::from(p));
    }
    let mut dir = std::env::current_dir().ok()?;
    for _ in 0..4 {
        let candidate = dir.join(".env");
        if candidate.is_file() {
            return Some(candidate);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

/// The program `brokapi` next to this example in the target directory, or the one `BROKAPI_BIN` names.
fn find_brokapi() -> Res<PathBuf> {
    if let Ok(p) = std::env::var("BROKAPI_BIN") {
        return Ok(PathBuf::from(p));
    }
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let name = if cfg!(windows) {
        "brokapi.exe"
    } else {
        "brokapi"
    };
    let candidate = exe.parent().and_then(Path::parent).map(|d| d.join(name));
    match candidate {
        Some(p) if p.is_file() => Ok(p),
        _ => Err("the program `brokapi` is not built: run `cargo build -p brokapi-gateway --bin brokapi` (tools/bybit-demo-run does), or set BROKAPI_BIN".into()),
    }
}

fn dec(s: &str) -> Decimal {
    s.parse().expect("a decimal")
}

/// Reads the stream until an event the test accepts, or fails after the wait.
async fn next_event(
    rx: &mut mpsc::Receiver<Envelope>,
    what: &str,
    pred: impl Fn(&Event) -> bool,
) -> Res<Event> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(env)) if pred(&env.payload) => return Ok(env.payload),
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(format!(
                    "the stream of events ended while waiting for {what}"
                ));
            }
            Err(_) => return Err(format!("no {what} in 30 seconds")),
        }
    }
}

fn e<T: std::fmt::Display>(what: &str) -> impl FnOnce(T) -> String + '_ {
    move |err| format!("{what}: {err}")
}

/// A quantity worth about 12 USDT at `price`, on the step of the instrument and not below the smallest order.
fn quantity(price: Price, lot: Qty, min_qty: Qty) -> Res<Qty> {
    let want = Qty::new(dec("12") / price.decimal());
    let q = want
        .round_to_step(lot, RoundMode::Up)
        .map_err(e("quantity"))?;
    Ok(if q < min_qty { min_qty } else { q })
}

struct Plan {
    bid_price: Price,
    stop_price: Price,
    qty: Qty,
}

async fn plan(bot: &RemoteClient, id: &InstrumentId) -> Res<Plan> {
    let params = bot.trading_params(id).await.map_err(e("trading_params"))?;
    let book = bot.order_book(id, 5).await.map_err(e("order_book"))?;
    let (bids, asks) = match book {
        OrderBook::Snapshot { bids, asks, .. } | OrderBook::Increment { bids, asks, .. } => {
            (bids, asks)
        }
    };
    let best_bid = bids.first().ok_or("the book has no bid")?.price;
    let best_ask = asks.first().ok_or("the book has no ask")?.price;
    // A buy 30% under the market is far from a fill. A stop-loss sell 40% under it waits for a crash.
    let bid_price = Price::new(best_bid.decimal() * dec("0.7"))
        .round_to_step(params.price_step, RoundMode::Down)
        .map_err(e("price"))?;
    let stop_price = Price::new(best_ask.decimal() * dec("0.6"))
        .round_to_step(params.price_step, RoundMode::Down)
        .map_err(e("price"))?;
    let qty = quantity(bid_price, params.lot_size, params.min_qty)?;
    Ok(Plan {
        bid_price,
        stop_price,
        qty,
    })
}

fn limit_buy(id: &InstrumentId, p: &Plan) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: id.clone(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: p.qty,
        price: Some(p.bid_price),
    }
}

fn stop_loss(id: &InstrumentId, p: &Plan) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: id.clone(),
        side: Side::Sell,
        kind: StopOrderKind::StopLoss,
        qty: p.qty,
        stop_price: p.stop_price,
        limit_price: None,
        expiry: StopExpiry::GoodTillCancel,
    }
}

/// The wallet is topped up from the demo faucet when it holds less than [`MIN_FUNDS`] of free USDT.
async fn ensure_funds(bot: &RemoteClient) -> Res<String> {
    let free = |p: &brokapi::core::Portfolio| {
        p.balances
            .iter()
            .find(|b| b.currency == "USDT")
            .map(|b| b.available.decimal())
            .unwrap_or_default()
    };
    let before = free(&bot.portfolio().await.map_err(e("portfolio"))?);
    if before >= Decimal::from(MIN_FUNDS) {
        return Ok(format!("{before} USDT free, no top-up needed"));
    }
    let body =
        json!({"adjustType": 0, "utaDemoApplyMoney": [{"coin": "USDT", "amountStr": "10000"}]});
    bot.raw("POST /v5/account/demo-apply-money", &json!({ "body": body }), true)
        .await
        .map_err(|err| format!("the demo account has {before} USDT free and the top-up (one request a minute) was refused: {err}"))?;
    let end = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let now = free(&bot.portfolio().await.map_err(e("portfolio"))?);
        if now >= Decimal::from(MIN_FUNDS) {
            return Ok(format!("{before} -> {now} USDT free after the top-up"));
        }
        if tokio::time::Instant::now() > end {
            return Err(format!(
                "the top-up was accepted but the wallet still shows {now} USDT free"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// What the bot leaves standing: the cids of one limit order and one stop order.
struct Ledger {
    order: ClientOrderId,
    stop: ClientOrderId,
}

async fn bot_places(run: &mut Run, symbol: &str) -> Res<Ledger> {
    run.say("the bot connects to the gateway and checks that the account is a demo one");
    let bot = RemoteClient::connect_dir(&run.gateway_dir(), "demo-run")
        .await
        .map_err(e("the bot cannot connect"))?;
    let (_, ud) = bot.caps().await.map_err(e("caps"))?;
    if !ud.sandbox {
        return Err("the gateway reports an account that is not a demo one: the run places orders and will not go on".into());
    }
    run.ok("the gateway reports a sandbox (virtual) account");

    run.say("the wallet and the order book");
    let funds = ensure_funds(&bot).await?;
    run.ok(&funds);
    let id = bot
        .resolve_instrument(symbol, None)
        .await
        .map_err(e("resolve_instrument"))?;
    let p = plan(&bot, &id).await?;
    run.ok(&format!(
        "{symbol}: buy {} at {} (30% under the market), stop-loss sell at {}",
        p.qty, p.bid_price, p.stop_price
    ));

    let mut events = bot.event_channel(1024).await.map_err(e("event stream"))?;
    bot.subscribe_order_state()
        .await
        .map_err(e("subscribe_order_state"))?;
    bot.subscribe_portfolio(SubscribeOpts::lossless(64))
        .await
        .map_err(e("subscribe_portfolio"))?;
    bot.reconcile(&[]).await.map_err(e("reconcile"))?;

    run.say("a limit order far from the market: placed, seen in the stream, cancelled");
    let a = limit_buy(&id, &p);
    let ack = bot.place(a.clone()).await.map_err(e("place"))?;
    if ack.cid != a.cid || ack.broker_id.as_str().is_empty() {
        return Err(format!("the ack is wrong: {ack:?}"));
    }
    next_event(
        &mut events,
        "the order in the stream",
        |ev| matches!(ev, Event::Order(o) if o.cid == a.cid),
    )
    .await?;
    let state = bot.order_state(a.cid).await.map_err(e("order_state"))?;
    if !matches!(
        state.state,
        OrderState::Acked { .. } | OrderState::PartiallyFilled { .. }
    ) {
        return Err(format!("the working order is {:?}", state.state));
    }
    if !bot
        .active_orders()
        .await
        .map_err(e("active_orders"))?
        .iter()
        .any(|o| o.cid == a.cid)
    {
        return Err("active_orders does not list the order".into());
    }
    bot.cancel(a.cid).await.map_err(e("cancel"))?;
    next_event(&mut events, "the cancel in the stream", |ev| {
        matches!(ev, Event::Order(o) if o.cid == a.cid && matches!(o.state, OrderState::Cancelled { .. }))
    })
    .await?;
    let state = bot
        .order_state(a.cid)
        .await
        .map_err(e("order_state after the cancel"))?;
    if !matches!(state.state, OrderState::Cancelled { .. }) {
        return Err(format!("after the cancel the order is {:?}", state.state));
    }
    run.ok("place, stream, order_state and cancel agree");

    run.say("one limit order and one stop order are placed and left standing");
    let b = limit_buy(&id, &p);
    bot.place(b.clone()).await.map_err(e("place"))?;
    let s = stop_loss(&id, &p);
    bot.place_stop(s.clone()).await.map_err(e("place_stop"))?;
    let stops = bot
        .active_stop_orders()
        .await
        .map_err(e("active_stop_orders"))?;
    match stops.iter().find(|x| x.cid == Some(s.cid)) {
        Some(x) if x.state == StopOrderState::Active && x.stop_price == p.stop_price => {}
        other => {
            return Err(format!(
                "the stop order is not listed as waiting at {}: {other:?}",
                p.stop_price
            ));
        }
    }
    if bot
        .active_orders()
        .await
        .map_err(e("active_orders"))?
        .iter()
        .any(|o| o.cid == s.cid)
    {
        return Err("a waiting stop order is listed among the active orders".into());
    }
    run.ok("the order is working, the stop order waits and is listed apart");

    // PLACE FOR THE BOTS OF OTHER LANGUAGES: a bot in Python or in Go does its part of the run here, against the same
    // gateway (the directory is printed below), and leaves its orders for the kill that follows.
    println!(
        "      (bots in other languages: none yet; the gateway directory is {})",
        run.gateway_dir().display()
    );
    Ok(Ledger {
        order: b.cid,
        stop: s.cid,
    })
}

async fn kill_and_reconcile(run: &mut Run) -> Res {
    run.say("the process of the gateway is killed, the service starts it again");
    let g = run.gateway().await?;
    let pid = g["pid"].as_u64().ok_or("the gateway has no pid")?;
    let status = if cfg!(windows) {
        std::process::Command::new("taskkill")
            .args(["/F", "/PID", &pid.to_string()])
            .status()
    } else {
        std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status()
    }
    .map_err(e("cannot kill the process"))?;
    if !status.success() {
        return Err(format!("cannot kill the process {pid}"));
    }
    let g = run.wait_running(Some(pid), "after the kill").await?;
    let new_pid = g["pid"].as_u64().unwrap_or(0);
    if g["restarts"].as_u64().unwrap_or(0) < 1 {
        return Err(format!(
            "the gateway runs again as {new_pid} but `ps` counts no restart: {g}"
        ));
    }
    run.ok(&format!(
        "pid {pid} -> {new_pid}, restarts {}",
        g["restarts"]
    ));

    run.say("the restarted gateway has reconciled its journal and still knows the limit order");
    let status = run.json(&["status", GATEWAY]).await?;
    if status["health"]["state"] != "ready" {
        return Err(format!("the gateway is not ready: {status}"));
    }
    // The journal of the gateway keeps the orders of the bots; a stop order is the business of the bot's own ledger.
    let open = status["active_orders"].as_u64().unwrap_or(u64::MAX);
    if open != 1 {
        return Err(format!(
            "the journal of the gateway holds {open} open order(s), expected 1 (the limit order): {status}"
        ));
    }
    run.ok("state ready, the limit order is in the journal");
    Ok(())
}

async fn bot_reconciles(run: &mut Run, ledger: &Ledger) -> Res {
    run.say("the bot connects again and reconciles with its ledger");
    let bot = RemoteClient::connect_dir(&run.gateway_dir(), "demo-run")
        .await
        .map_err(e("the bot cannot connect again"))?;
    let report = bot
        .reconcile(&[ledger.order, ledger.stop])
        .await
        .map_err(e("reconcile"))?;
    let mut confirmed: Vec<_> = report.confirmed.iter().map(|o| o.cid).collect();
    confirmed.sort();
    let mut want = vec![ledger.order, ledger.stop];
    want.sort();
    if confirmed != want || !report.closed_offline.is_empty() {
        return Err(format!(
            "the ledger has two orders alive, the reconcile says: confirmed {confirmed:?}, closed {:?}",
            report.closed_offline
        ));
    }
    if !report.foreign.is_empty() {
        println!(
            "      note: {} order(s) of someone else on the demo account",
            report.foreign.len()
        );
    }
    run.ok("both orders confirmed, none closed");

    run.say("the bot cancels both; orders, stops and cancel-all agree there is nothing left");
    bot.cancel(ledger.order).await.map_err(e("cancel"))?;
    bot.cancel_stop(StopOrderRef::Cid(ledger.stop))
        .await
        .map_err(e("cancel_stop"))?;
    for what in ["orders", "stops"] {
        let list = run.json(&[what, GATEWAY]).await?;
        let n = list.as_array().map(Vec::len).unwrap_or(usize::MAX);
        if n != 0 {
            return Err(format!("`brokapi {what}` still lists {n} item(s): {list}"));
        }
    }
    let (code, out, err) = run.cmd(&["cancel-all", GATEWAY]).await?;
    if code != 0 {
        return Err(format!("`cancel-all` ended with code {code}: {out}{err}"));
    }
    run.ok("nothing left");
    Ok(())
}

async fn scenario(run: &mut Run, symbol: &str) -> Res {
    run.say("`brokapi check` reads the account without placing anything");
    let checked = run.json(&["check"]).await?;
    if checked["ok"] != true {
        return Err(format!("`check` is not ok: {checked}"));
    }
    run.ok("the config and the key pair are good");

    run.say("`brokapi up -d` starts the service and the gateway; `ps` shows it");
    let (code, out, err) = run.cmd(&["up", "-d"]).await?;
    if code != 0 {
        return Err(format!("`up -d` ended with code {code}: {out}{err}"));
    }
    run.up = true;
    let g = run.wait_running(None, "after up").await?;
    if g["sandbox"] != true {
        return Err(format!(
            "`ps` does not say the gateway is on a sandbox account: {g}"
        ));
    }
    run.ok(&format!("{GATEWAY} running, pid {}", g["pid"]));

    run.say("`quote` and `book` through the gateway");
    let q = run.json(&["quote", GATEWAY, symbol]).await?;
    let price = |v: &Value| v["price"].as_str().and_then(|s| s.parse::<Decimal>().ok());
    let (bid, ask) = (price(&q["bid"]), price(&q["ask"]));
    match (bid, ask) {
        (Some(b), Some(a)) if b > Decimal::ZERO && b < a => {}
        _ => return Err(format!("the quote is not a bid under an ask: {q}")),
    }
    let book = run.json(&["book", GATEWAY, symbol, "--depth", "5"]).await?;
    let levels = |side: &str| book[side].as_array().map(Vec::len).unwrap_or(0);
    let snapshot = book.get("Snapshot").unwrap_or(&book);
    let n = |side: &str| snapshot[side].as_array().map(Vec::len).unwrap_or(0);
    if n("bids") == 0 || n("asks") == 0 {
        return Err(format!(
            "the book has an empty side ({} bids, {} asks): {book}",
            levels("bids"),
            levels("asks")
        ));
    }
    run.ok(&format!(
        "bid {} ask {}, a book of {} and {} levels",
        bid.unwrap(),
        ask.unwrap(),
        n("bids"),
        n("asks")
    ));

    let ledger = bot_places(run, symbol).await?;
    kill_and_reconcile(run).await?;
    bot_reconciles(run, &ledger).await?;

    run.say("`brokapi down`");
    let (code, out, err) = run.cmd(&["down"]).await?;
    if code != 0 {
        return Err(format!("`down` ended with code {code}: {out}{err}"));
    }
    run.up = false;
    run.ok("the service is stopped");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| args.iter().any(|a| a == name);
    let value = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    if flag("--help") || flag("-h") {
        println!(
            "usage: bybit_demo_run [--self-test] [--keep] [--env-file PATH] [--symbol BTCUSDT]\n\n\
             Reads BYBIT_DEMO_API_KEY and BYBIT_DEMO_API_SECRET from the environment or from a .env file (this directory\n\
             or up to three above it). --self-test uses the in-process imitation instead of the exchange."
        );
        return ExitCode::SUCCESS;
    }
    let self_test = flag("--self-test");
    let symbol = value("--symbol").unwrap_or_else(|| SYMBOL.to_string());

    if let Some(path) = find_env_file(value("--env-file").as_deref()) {
        dotenvy_load(&path);
    }
    // The imitation, started here, has a pair of its own.
    let mock = if self_test {
        Some(brokapi_mocks::bybit::MockServer::start(Default::default()).await)
    } else {
        None
    };
    let (key, secret) = if self_test {
        (
            brokapi_mocks::bybit::DEFAULT_KEY.to_string(),
            brokapi_mocks::bybit::SECRET.to_string(),
        )
    } else {
        let key = std::env::var("BYBIT_DEMO_API_KEY")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let secret = std::env::var("BYBIT_DEMO_API_SECRET")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let missing: Vec<&str> = [
            ("BYBIT_DEMO_API_KEY", &key),
            ("BYBIT_DEMO_API_SECRET", &secret),
        ]
        .iter()
        .filter(|(_, v)| v.is_none())
        .map(|(n, _)| *n)
        .collect();
        if !missing.is_empty() {
            eprintln!("bybit_demo_run: not set: {}", missing.join(", "));
            eprintln!(
                "Make a key in the demo mode of the Bybit site (Demo Trading, then the API page) and put it in .env:"
            );
            eprintln!("  BYBIT_DEMO_API_KEY=...\n  BYBIT_DEMO_API_SECRET=...");
            eprintln!(
                "Nothing was started. (`--self-test` runs the same steps against the imitation, with no key.)"
            );
            return ExitCode::from(2);
        }
        (key.unwrap(), secret.unwrap())
    };

    let brokapi = match find_brokapi() {
        Ok(p) => p,
        Err(m) => {
            eprintln!("bybit_demo_run: {m}");
            return ExitCode::from(2);
        }
    };
    let dir = std::env::temp_dir().join(format!("brokapi-bybit-demo-{}", std::process::id()));
    if let Err(err) = std::fs::create_dir_all(&dir) {
        eprintln!("bybit_demo_run: cannot make {}: {err}", dir.display());
        return ExitCode::from(2);
    }
    let settings = mock
        .as_ref()
        .map(|m| format!("[gateway.settings]\nendpoint = {:?}\n", m.http_base))
        .unwrap_or_default();
    let config = dir.join("brokapi.toml");
    let text = format!(
        "state_dir = {:?}\n\n[service]\ncheck_interval = \"500ms\"\n\n[service.restart]\ninitial = \"300ms\"\nmax_pause = \"2s\"\nmax_restarts = 5\nwindow = \"60s\"\n\n\
         [[gateway]]\nname = \"{GATEWAY}\"\nbroker = \"bybit\"\nsandbox = true\ntoken_env = \"BYBIT_DEMO_API_SECRET\"\nkey_env = \"BYBIT_DEMO_API_KEY\"\n{settings}",
        dir.join("state").display().to_string()
    );
    if let Err(err) = std::fs::write(&config, text) {
        eprintln!("bybit_demo_run: cannot write the config: {err}");
        return ExitCode::from(2);
    }
    let mut run = Run {
        brokapi,
        config,
        dir,
        env: vec![
            ("BYBIT_DEMO_API_KEY".into(), key),
            ("BYBIT_DEMO_API_SECRET".into(), secret),
        ],
        step: 0,
        keep: flag("--keep"),
        up: false,
    };
    println!(
        "Bybit demo run{}: gateway {GATEWAY}, symbol {symbol}, directory {}",
        if self_test {
            " (against the imitation)"
        } else {
            ""
        },
        run.dir.display()
    );
    let result = scenario(&mut run, &symbol).await;
    run.clean_up().await;
    drop(mock);
    match result {
        Ok(()) => {
            println!("\nAll {} steps passed.", run.step);
            ExitCode::SUCCESS
        }
        Err(why) => {
            eprintln!("\nFAILED at step {} of {STEPS}: {why}", run.step);
            ExitCode::from(1)
        }
    }
}

/// Puts the lines `NAME=value` of a .env file in the environment, without replacing what is already there. Only the
/// variables of the demo key pair are taken: the file may hold other secrets, and they stay out of the processes started here.
fn dotenvy_load(path: &Path) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return;
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((name, value)) = line.split_once('=') {
            let name = name.trim().trim_start_matches("export ").trim();
            let value = value.trim().trim_matches('"').trim_matches('\'');
            if matches!(name, "BYBIT_DEMO_API_KEY" | "BYBIT_DEMO_API_SECRET")
                && std::env::var_os(name).is_none()
            {
                // SAFETY: the program is single threaded here, before anything else runs.
                unsafe { std::env::set_var(name, value) };
            }
        }
    }
}
