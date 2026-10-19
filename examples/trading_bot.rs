//! An event-driven trading bot: order book in, signal, order, statuses, a way out of `Unknown`, a clean stop.
//!
//! The code of the page "Как сделать торгового бота на событиях" of the book (docs/src/howto-trading-bot.md).
//! The bot is written against the traits of `brokapi::core`, so the same `TradingBot` runs on every broker.
//! `run_bcs` plays it on the BCS imitation (the order is filled, or the bot stops and cancels it), `run_tbank_unknown`
//! plays it on the T-Bank imitation where the answer to `place` is lost, so the bot has to leave `Unknown`.
//! Nothing here touches the network or money.
//!
//! The strategy is a stand-in: when the book arrives and there is no order, buy the minimum quantity 1% below the
//! best bid. Everything around it (journal first, reconcile first, outcomes of every command, `Resync`,
//! `Overflow`, stop) is what a real bot needs.
//!
//! ```text
//! cargo run --example trading_bot
//! ```
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// ANCHOR: imports
use brokapi::core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentQuery,
    InstrumentSource, OrderBook, OrderBookSource, OrderExecutor, OrderSnapshot, OrderState,
    OrderType, PlaceOrderRequest, Price, Qty, RoundMode, ShutdownMode, Side, SubscribeOpts,
    SubscriptionId, TradingParams,
};
use tokio::sync::mpsc;
// ANCHOR_END: imports

// ANCHOR: journal
/// What the bot knows about one of its orders. The journal is the bot's memory: the library keeps nothing
/// on disk. A real bot writes every change to a file or a database before it goes on (see the infrastructure
/// example). Here the journal is in memory, to keep the example short.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// Written BEFORE `place` is called. If the process dies now, the order may or may not exist.
    Sending,
    /// The broker accepted the order, or the stream says it works.
    Working,
    /// The outcome of the command is not known. The bot asks the broker with `order_state`.
    Check,
    /// Final. The text says how it ended.
    Done(String),
}

#[derive(Debug, Default)]
pub struct Journal {
    pub entries: Vec<(ClientOrderId, Entry)>,
}

impl Journal {
    pub fn set(&mut self, cid: ClientOrderId, entry: Entry) {
        match self.entries.iter_mut().find(|(c, _)| *c == cid) {
            Some(slot) => slot.1 = entry,
            None => self.entries.push((cid, entry)),
        }
    }

    pub fn get(&self, cid: ClientOrderId) -> Option<&Entry> {
        self.entries.iter().find(|(c, _)| *c == cid).map(|(_, e)| e)
    }

    /// The cids the bot believes are alive: the ledger it gives to `reconcile`.
    pub fn live(&self) -> Vec<ClientOrderId> {
        self.entries
            .iter()
            .filter(|(_, e)| !matches!(e, Entry::Done(_)))
            .map(|(c, _)| *c)
            .collect()
    }
}
// ANCHOR_END: journal

// ANCHOR: types
/// How far below the best bid the bot buys: 99%. (`Decimal::new` is not `const`, `from_parts` is.)
const BUY_AT: Decimal = Decimal::from_parts(99, 0, 0, false, 2);

/// The top of the book the bot trades from. Dropped on `Resync`.
#[derive(Debug, Clone, Copy)]
pub struct Top {
    pub bid: Price,
    pub ask: Option<Price>,
}

/// BCS names an instrument by ticker and board.
fn sber_bcs() -> InstrumentId {
    use brokapi::core::{InstrumentKey, NativeId};
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

/// The connectors write into a bounded channel and never wait for it. A small task moves every envelope into
/// the bot's own queue at once, so the channel is always empty and the bot may take its time (a command can wait
/// seconds for the broker). The queue is unbounded: watch its length in a real bot.
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
// ANCHOR_END: types

// ANCHOR: bot
/// The bot. `M` is the market data connector, `U` the connector of one account.
pub struct TradingBot<'a, M, U> {
    md: &'a M,
    ud: &'a U,
    instrument: InstrumentId,
    params: TradingParams,
    pub journal: Journal,
    /// The latest book. `None` after `Resync`: do not trade from a book that may be stale.
    top: Option<Top>,
    /// The order that works now. One at a time in this bot.
    pub working: Option<ClientOrderId>,
    /// The bot is stopping: no new orders.
    stopping: bool,
    /// A command found the gate closed, or `Overflow` came: reconcile before the next command.
    need_reconcile: bool,
    /// Do not place before this moment (a rate limit asked to wait).
    pause_until: Option<tokio::time::Instant>,
    /// What each of OUR orders has filled so far, from the order snapshots.
    fills: HashMap<ClientOrderId, Qty>,
    /// Which connector the book comes from, and the id of the book subscription: an `Overflow` of THAT
    /// subscription needs a new subscription, an `Overflow` of the order stream needs a reconcile.
    md_label: Arc<str>,
    book_sub: Option<SubscriptionId>,
    /// How many more orders the bot may place. The stand-in strategy buys once.
    pub budget: u32,
    /// The price of the last order that was accepted.
    pub placed_price: Option<Price>,
}

impl<'a, M, U> TradingBot<'a, M, U>
where
    M: OrderBookSource + InstrumentSource,
    U: OrderExecutor + ExecutionSource,
{
    /// Start-up in the order the library asks for: subscribe, reconcile, only then trade.
    pub async fn start(
        md: &'a M,
        ud: &'a U,
        instrument: InstrumentId,
        mut journal: Journal,
        price_grid: &brokapi::runtime::PriceGrid,
        md_label: &str,
    ) -> Result<TradingBot<'a, M, U>, Error> {
        // 1. Statuses and fills first: an event between the snapshot of the reconcile and the
        //    subscription would be lost otherwise.
        ud.subscribe_order_state().await?;
        ud.subscribe_executions().await?;

        // 2. The ledger comes from the journal. A new bot has an empty one.
        let report = ud.reconcile(&journal.live()).await?;
        for order in &report.confirmed {
            journal.set(order.cid, Entry::Working);
        }
        for cid in &report.closed_offline {
            // Not alive at the broker any more. Find out how it ended and write it down.
            let how = match ud.order_state(*cid).await {
                Ok(order) => format!("{:?}", order.state),
                Err(_) => "the broker does not know it".to_string(),
            };
            journal.set(*cid, Entry::Done(how));
        }
        // report.foreign: somebody else's orders. Look, do not touch.

        // 3. The rules of the instrument. The price step goes to the connector too, so a price off the
        //    step is refused before it is sent.
        let params = md.trading_params(&instrument).await?;
        price_grid.learn(&params);

        // 4. The order book. Only the latest book matters: Conflate.
        let book_sub = md
            .subscribe_order_book(
                &instrument,
                SubscribeOpts {
                    depth: Some(5),
                    ..SubscribeOpts::conflate()
                },
            )
            .await?;

        Ok(Self {
            md,
            ud,
            instrument,
            params,
            journal,
            top: None,
            working: report.confirmed.first().map(|o| o.cid),
            stopping: false,
            need_reconcile: false,
            pause_until: None,
            fills: HashMap::new(),
            md_label: Arc::from(md_label),
            book_sub: Some(book_sub),
            budget: 1,
            placed_price: None,
        })
    }

    /// One event from the shared channel.
    pub async fn on_event(&mut self, env: Envelope) -> Result<(), Error> {
        let source = env.source.clone();
        match env.payload {
            Event::OrderBook(OrderBook::Snapshot { bids, asks, .. }) => {
                self.top = bids.first().map(|b| Top {
                    bid: b.price,
                    ask: asks.first().map(|a| a.price),
                });
                if self.working.is_none() && !self.stopping && self.budget > 0 {
                    self.try_buy().await?;
                }
            }
            // The book may have lost data: forget it until the next snapshot.
            Event::Resync { .. } => self.top = None,
            Event::Order(order) => self.on_order(&order),
            // The fills are counted from the order snapshots (`filled`), which a reconcile restores after a hole.
            Event::Execution(_) => {}
            // The book subscription lost events: only the book is void, subscribe again. Own events lost: trading
            // is closed until a reconcile.
            Event::Overflow { sub, .. }
                if Some(sub) == self.book_sub && *source == *self.md_label =>
            {
                self.top = None;
                self.md.unsubscribe(sub).await?;
                self.book_sub = Some(
                    self.md
                        .subscribe_order_book(&self.instrument, SubscribeOpts::conflate())
                        .await?,
                );
            }
            Event::Overflow { .. } => self.need_reconcile = true,
            _ => {}
        }
        Ok(())
    }

    /// The statuses of the stream drive the journal. Final states free the bot to trade again.
    fn on_order(&mut self, order: &OrderSnapshot) {
        // The stream carries every order of the account, also those placed by hand. Only the journal's are ours.
        if self.journal.get(order.cid).is_none() {
            return;
        }
        self.fills.insert(order.cid, order.filled);
        let entry = match &order.state {
            OrderState::Acked { .. } | OrderState::PartiallyFilled { .. } => Entry::Working,
            OrderState::InFlight { .. } | OrderState::Unknown { .. } => Entry::Check,
            OrderState::Filled { .. } => Entry::Done("filled".into()),
            OrderState::Cancelled { .. } => Entry::Done("cancelled".into()),
            OrderState::Rejected { reason, .. } => Entry::Done(format!("rejected: {reason}")),
        };
        let is_final = matches!(entry, Entry::Done(_));
        self.journal.set(order.cid, entry);
        if is_final && self.working == Some(order.cid) {
            self.working = None;
        }
    }

    /// How much our orders have filled.
    pub fn filled(&self) -> Qty {
        self.fills.values().copied().sum()
    }

    /// A timer, not only the book, drives a retry: after `RateLimited` or a closed gate a quiet book would
    /// never fire the signal again.
    pub async fn on_tick(&mut self) -> Result<(), Error> {
        if self.working.is_none() && !self.stopping && self.budget > 0 && self.top.is_some() {
            self.try_buy().await?;
        }
        Ok(())
    }
    // ANCHOR_END: bot

    // ANCHOR: place
    /// The signal, and the order with every outcome handled.
    async fn try_buy(&mut self) -> Result<(), Error> {
        if self.need_reconcile {
            self.reconcile_now().await?;
        }
        if let Some(until) = self.pause_until {
            if tokio::time::Instant::now() < until {
                return Ok(());
            }
            self.pause_until = None;
        }
        let Some(top) = self.top else { return Ok(()) };
        // Exact decimal arithmetic. Round DOWN for a buy so the price never exceeds the plan, and to the step.
        let price = (top.bid * BUY_AT)
            .round_to_step(self.params.price_step, RoundMode::Down)
            .map_err(|e| Error::Config(brokapi::core::ConfigError::Message(e.to_string())))?;

        // The cid is made by the bot, and written to the journal BEFORE the call.
        let cid = ClientOrderId::new_v4();
        self.journal.set(cid, Entry::Sending);
        let request = PlaceOrderRequest {
            cid,
            instrument: self.instrument.clone(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: self.params.min_qty,
            price: Some(price),
        };
        match self.ud.place(request).await {
            Ok(_ack) => {
                self.journal.set(cid, Entry::Working);
                self.working = Some(cid);
                self.budget -= 1;
                self.placed_price = Some(price);
            }
            // The broker said no. The order does not exist. A new try needs a new cid.
            Err(Error::Rejected { code, message }) => {
                self.journal
                    .set(cid, Entry::Done(format!("rejected {code}: {message}")));
            }
            // The gate is closed. Nothing was sent. Reconcile, and the next signal will try again.
            Err(Error::NotReconciled | Error::Degraded) => {
                self.journal
                    .set(cid, Entry::Done("not sent: gate closed".into()));
                self.need_reconcile = true;
            }
            // The local limiter refused. Nothing was sent. Wait, do not hammer.
            Err(Error::RateLimited { retry_after }) => {
                self.journal
                    .set(cid, Entry::Done("not sent: rate limited".into()));
                self.pause_until = Some(tokio::time::Instant::now() + retry_after);
            }
            // The command may have reached the broker. NEVER place again. Ask.
            Err(Error::Unknown | Error::Transport(_)) => {
                self.journal.set(cid, Entry::Check);
                self.settle_unknown(cid).await?;
            }
            // Setup errors, Unsupported, ShuttingDown: the bot cannot go on.
            Err(other) => return Err(other),
        }
        Ok(())
    }
    // ANCHOR_END: place

    // ANCHOR: unknown
    /// The way out of `Unknown`: ask the broker about the same cid until it answers or patience runs out.
    async fn settle_unknown(&mut self, cid: ClientOrderId) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match self.ud.order_state(cid).await {
                Ok(order) => {
                    // The order exists. Continue as with any other.
                    self.on_order(&order);
                    if !matches!(self.journal.get(cid), Some(Entry::Done(_))) {
                        self.working = Some(cid);
                    }
                    self.budget = self.budget.saturating_sub(1);
                    self.placed_price = order.price;
                    return Ok(());
                }
                // "Not found" right after a cut is not final: the request may still be on its way.
                Err(Error::Rejected { code, .. }) if code.eq_ignore_ascii_case("not_found") => {}
                Err(Error::RateLimited { .. } | Error::Transport(_)) => {}
                Err(other) => return Err(other),
            }
            if tokio::time::Instant::now() >= deadline {
                // Still unknown to the broker: it was not placed. A new order will have a new cid.
                self.journal.set(
                    cid,
                    Entry::Done("not placed (the broker never knew it)".into()),
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    // ANCHOR_END: unknown

    // ANCHOR: recover
    /// After `Overflow` or `Degraded`: read what is waiting in the channel to the end, then reconcile.
    async fn reconcile_now(&mut self) -> Result<(), Error> {
        let report = self.ud.reconcile(&self.journal.live()).await?;
        for order in &report.confirmed {
            self.on_order(order);
        }
        for cid in &report.closed_offline {
            let how = match self.ud.order_state(*cid).await {
                Ok(order) => {
                    self.on_order(&order);
                    continue;
                }
                Err(_) => "the broker does not know it".to_string(),
            };
            self.journal.set(*cid, Entry::Done(how));
            if self.working == Some(*cid) {
                self.working = None;
            }
        }
        self.need_reconcile = false;
        Ok(())
    }

    /// The loop calls this after every event.
    pub async fn recover_if_needed(
        &mut self,
        events: &mut mpsc::UnboundedReceiver<Envelope>,
    ) -> Result<(), Error> {
        if self.need_reconcile {
            // First the events that did arrive: they are the history up to the hole.
            while let Ok(env) = events.try_recv() {
                self.on_event(env).await?;
            }
            self.reconcile_now().await?;
        }
        Ok(())
    }
    // ANCHOR_END: recover

    // ANCHOR: stop
    /// A clean stop. No new orders. The bot takes its own working order off (a choice of THIS bot: the
    /// library never cancels anything on shutdown), waits for the final status, and writes it down.
    pub async fn stop(
        &mut self,
        events: &mut mpsc::UnboundedReceiver<Envelope>,
    ) -> Result<(), Error> {
        self.stopping = true;
        if let Some(cid) = self.working {
            for _ in 0..5 {
                match self.ud.cancel(cid).await {
                    Ok(()) => break,
                    // The order was filled before the cancel arrived: read the result instead.
                    Err(Error::Rejected { .. }) => break,
                    // Nothing was sent: wait, or reconcile, and try again. Leaving the order is not an option.
                    Err(Error::RateLimited { retry_after }) => {
                        tokio::time::sleep(retry_after).await
                    }
                    Err(Error::NotReconciled | Error::Degraded) => self.reconcile_now().await?,
                    // Maybe cancelled, maybe not: ask, and cancel again only if it still works.
                    Err(Error::Unknown | Error::Transport(_)) => {
                        if let Ok(order) = self.ud.order_state(cid).await {
                            self.on_order(&order);
                            if self.working != Some(cid) {
                                break;
                            }
                        }
                    }
                    Err(other) => return Err(other),
                }
            }
            // The final status comes on the stream; the read below is the proof.
            let _ = tokio::time::timeout(Duration::from_secs(5), async {
                while self.working.is_some() {
                    match events.recv().await {
                        Some(env) => self.on_event(env).await?,
                        None => break,
                    }
                }
                Ok::<(), Error>(())
            })
            .await;
            if self.working.is_some() {
                let order = self.ud.order_state(cid).await?;
                self.on_order(&order);
            }
        }
        Ok(())
    }
    // ANCHOR_END: stop
}

/// What the example's "exchange" does with the bot's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ending {
    /// The exchange executes the order.
    Filled,
    /// The bot is told to stop while the order works, and cancels it.
    StoppedByBot,
}

/// The result the tests look at.
#[derive(Debug)]
pub struct Outcome {
    pub journal: Vec<(ClientOrderId, Entry)>,
    pub filled: Qty,
    pub price: Option<Price>,
}

/// Plays the bot on the BCS imitation. `ending` says how the order ends.
pub async fn run_bcs(ending: Ending) -> Result<Outcome, Box<dyn std::error::Error>> {
    // ANCHOR: wiring
    use brokapi::bcs::limits::default_limits;
    use brokapi::bcs::protocol::config::BcsConfig;
    use brokapi::bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
    use brokapi::runtime::BrokerLimiter;

    let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);

    // One limiter for the token (BCS counts limits per token), one channel for all events.
    let limiter = BrokerLimiter::new(&default_limits());
    let (tx, events) = mpsc::channel::<Envelope>(1024);
    let mut queue = forward(events);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "bcs-md"),
        limiter.clone(),
        tx.clone(),
    )
    .await?;
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, tx).await?;
    let grid = ud.price_grid().clone();

    let mut bot =
        TradingBot::start(&md, &ud, sber_bcs(), Journal::default(), &grid, "bcs-md").await?;
    // ANCHOR_END: wiring

    // ANCHOR: loop
    // The loop: events in, the bot decides. A deadline keeps the example from hanging if something is wrong.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut placed = None;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            env = tokio::time::timeout_at(deadline, queue.recv()) => {
                bot.on_event(env?.ok_or("the channel closed")?).await?;
            }
            _ = tick.tick() => bot.on_tick().await?,
        }
        bot.recover_if_needed(&mut queue).await?;

        // The example plays the exchange: once the order works, fill it, or ask the bot to stop.
        if ending == Ending::Filled
            && placed.is_some()
            && bot.working.is_none()
            && bot.filled().is_positive()
        {
            break; // the order is final and its fill is booked (the two events can come in either order)
        }
        if let Some(cid) = bot.working
            && placed != Some(cid)
        {
            placed = Some(cid);
            match ending {
                Ending::Filled => {
                    let price = Price::new(Decimal::new(24200, 2));
                    server.fill(&cid.hyphenated().to_string(), Qty::from(1), price);
                }
                Ending::StoppedByBot => break,
            }
        }
    }
    bot.stop(&mut queue).await?;
    // ANCHOR_END: loop

    let outcome = Outcome {
        journal: bot.journal.entries.clone(),
        filled: bot.filled(),
        price: bot.placed_price,
    };
    // ANCHOR: shutdown
    // The bot borrows the connectors and `shutdown` takes them by value: let the bot go first.
    drop(bot);
    // Stop the account connector first (no more commands), then the market data.
    let stop = ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    };
    let report = ud.shutdown(stop).await;
    md.shutdown(stop).await;
    println!(
        "stopped: {} command(s) were left without an answer",
        report.inflight_abandoned.len()
    );
    // ANCHOR_END: shutdown
    Ok(outcome)
}

/// Plays the bot on the T-Bank imitation, where the answer to the first `place` is lost.
pub async fn run_tbank_unknown() -> Result<Outcome, Box<dyn std::error::Error>> {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use brokapi::runtime::BrokerLimiter;
    use brokapi::tbank::limits::default_limits;
    use brokapi::tbank::protocol::TbankConfig;
    use brokapi::tbank::{
        MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
    };
    use brokapi_mocks::tbank::{ACCOUNT_A, MockOptions, MockServer, TOKEN};

    // The mock answers a place 1.5 s late, the connector gives up after 200 ms: the answer is lost, the
    // order is not. That is a real `Unknown`.
    let opts = Arc::new(MockOptions::default());
    opts.post_delay_ms.store(1500, Ordering::SeqCst);
    let server = MockServer::start(opts).await;
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint)
        .with_account(ACCOUNT_A)
        .with_request_timeout(Duration::from_millis(200));

    let limiter = BrokerLimiter::new(&default_limits());
    let (tx, events) = mpsc::channel::<Envelope>(1024);
    let mut queue = forward(events);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "tbank-md"),
        limiter.clone(),
        tx.clone(),
    )
    .await?;
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), limiter, tx).await?;
    let grid = ud.price_grid().clone();

    // T-Bank names an instrument by uid: ask the directory.
    let sber = md
        .find(InstrumentQuery::ByTicker {
            ticker: "SBER".into(),
            exchange: Some("TQBR".into()),
        })
        .await?
        .into_iter()
        .next()
        .ok_or("SBER is not in the directory")?
        .id;

    let mut bot = TradingBot::start(&md, &ud, sber, Journal::default(), &grid, "tbank-md").await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while bot.working.is_none() {
        let env = tokio::time::timeout_at(deadline, queue.recv())
            .await?
            .ok_or("the channel closed")?;
        bot.on_event(env).await?;
    }
    bot.stop(&mut queue).await?;

    let outcome = Outcome {
        journal: bot.journal.entries.clone(),
        filled: bot.filled(),
        price: bot.placed_price,
    };
    drop(bot);
    let stop = ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    };
    ud.shutdown(stop).await;
    md.shutdown(stop).await;
    Ok(outcome)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let outcome = run_bcs(Ending::Filled).await?;
    println!("journal: {:?}", outcome.journal);
    Ok(())
}
