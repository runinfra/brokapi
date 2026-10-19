//! The short snippets of the page "Контракт для агентов" of the book (docs/src/agent-contract.md): one snippet per rule.
//!
//! They are not a program: each function shows the few lines that obey one rule, written against the traits of
//! `brokapi::core`, so they compile for every broker. The build compiles them (`cargo build --examples`), so a snippet
//! cannot drift from the API. The runnable programs are `trading_bot`, `infrastructure` and `research`.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use brokapi::core::{
    ClientOrderId, ConfigError, Decimal, Envelope, Error, Event, InstrumentKey, OrderBook,
    OrderExecutor, PlaceOrderRequest, Price, ReconcileReport, RoundMode, ShutdownMode,
    SubscribeOpts, TradingParams,
};
use brokapi::runtime::BrokerLimiter;
use tokio::sync::mpsc;

/// Your own order journal. The library keeps nothing on disk, so this is yours: a file or a database.
pub trait Journal {
    /// Writes "about to send this cid" to disk and returns only when it is there.
    fn sending(&mut self, cid: ClientOrderId) -> std::io::Result<()>;
    /// The cids you believe are alive: not final in the journal.
    fn live(&self) -> Vec<ClientOrderId>;
    fn done(&mut self, cid: ClientOrderId, how: &str) -> std::io::Result<()>;
}

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// Rule 1: the bot makes the cid and writes it down before the call.
pub async fn rule_cid_first<U: OrderExecutor>(
    ud: &U,
    journal: &mut impl Journal,
    request: PlaceOrderRequest,
) -> Res {
    // ANCHOR: cid_first
    let cid = request.cid; // made by the bot: ClientOrderId::new_v4()
    journal.sending(cid)?; // on disk BEFORE the call
    let result = ud.place(request).await;
    // ANCHOR_END: cid_first
    let _ = result;
    Ok(())
}

/// Rule 2: subscribe, reconcile, only then trade.
pub async fn rule_reconcile_first<U: OrderExecutor>(
    ud: &U,
    journal: &impl Journal,
    request: PlaceOrderRequest,
) -> Res {
    // ANCHOR: reconcile_first
    ud.subscribe_order_state().await?; // before the reconcile, or an event in between is lost
    let report = ud.reconcile(&journal.live()).await?; // an empty slice for a bot without a journal
    // report.confirmed, report.closed_offline, report.foreign: handle them (see the lifecycle page)
    ud.place(request).await?; // before the reconcile this is Error::NotReconciled
    // ANCHOR_END: reconcile_first
    let _: ReconcileReport = report;
    Ok(())
}

/// Rule 3: no answer is not "no". Ask, do not repeat.
pub async fn rule_unknown<U: OrderExecutor>(ud: &U, request: PlaceOrderRequest) -> Res {
    // ANCHOR: unknown
    let cid = request.cid;
    match ud.place(request).await {
        Ok(_) | Err(Error::Rejected { .. }) => {}
        Err(Error::Unknown | Error::Transport(_)) => {
            // The order may exist. NEVER place again. Ask about THE SAME cid (see the recovery page for the loop).
            let _state = ud.order_state(cid).await;
        }
        Err(other) => return Err(other.into()),
    }
    // ANCHOR_END: unknown
    Ok(())
}

/// Rule 4: one limiter per area where the broker counts a limit, shared by every connector in that area.
pub async fn rule_one_limiter(
    md_cfg: brokapi::tbank::MarketDataConfig,
    ud_a_cfg: brokapi::tbank::UserDataConfig,
    ud_b_cfg: brokapi::tbank::UserDataConfig,
) -> Res {
    // ANCHOR: one_limiter
    // T-Bank counts per user: the market data and BOTH accounts share one limiter.
    let limiter = BrokerLimiter::new(&brokapi::tbank::limits::default_limits());
    let (tx, _events) = mpsc::channel::<Envelope>(8192);
    let md =
        brokapi::tbank::MarketDataConnector::start(md_cfg, limiter.clone(), tx.clone()).await?;
    let a = brokapi::tbank::UserDataConnector::start(ud_a_cfg, limiter.clone(), tx.clone()).await?;
    let b = brokapi::tbank::UserDataConnector::start(ud_b_cfg, limiter, tx).await?;
    // ANCHOR_END: one_limiter
    let _ = (md, a, b);
    Ok(())
}

/// Rule 4, the other half: the library never waits for a quota. You do.
pub async fn rule_rate_limited<U: OrderExecutor>(ud: &U, request: PlaceOrderRequest) -> Res {
    // ANCHOR: rate_limited
    if let Err(Error::RateLimited { retry_after }) = ud.place(request).await {
        // Nothing was sent. Wait; the next try gets a NEW cid (always safe).
        tokio::time::sleep(retry_after).await;
    }
    // ANCHOR_END: rate_limited
    Ok(())
}

/// Rule 5: the channel is read all the time, by a task that does nothing slow.
pub fn rule_read_the_channel(
    mut events: mpsc::Receiver<Envelope>,
) -> mpsc::UnboundedReceiver<Envelope> {
    // ANCHOR: read_the_channel
    // The reader does nothing but move envelopes to the bot's own queue, so the library's channel is always empty.
    // The bot takes envelopes from `queue` and may await commands as long as it needs.
    let (queue_tx, queue) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(env) = events.recv().await {
            if queue_tx.send(env).is_err() {
                break;
            }
        }
    });
    // ANCHOR_END: read_the_channel
    queue
}

/// Rule 6: Overflow or Degraded. Read to the end, reconcile, restore from snapshots.
pub async fn rule_overflow<U: OrderExecutor>(
    ud: &U,
    events: &mut mpsc::Receiver<Envelope>,
    journal: &impl Journal,
) -> Res {
    // ANCHOR: overflow
    while let Ok(env) = events.try_recv() {
        let _ = env; // the events that DID arrive are the history up to the hole: handle them as always
    }
    let report = ud.reconcile(&journal.live()).await?; // opens the gate again
    // then restore: report.confirmed (states), executions(range) since lost_from, portfolio()
    // ANCHOR_END: overflow
    let _ = report;
    Ok(())
}

/// Rule 7: Resync means the stored book is void.
pub fn rule_resync(books: &mut HashMap<InstrumentKey, OrderBook>, event: Event) {
    // ANCHOR: resync
    match event {
        Event::Resync { instrument, .. } => {
            books.remove(&instrument.key); // do not trade from it; the next snapshot replaces it
        }
        Event::OrderBook(book @ OrderBook::Snapshot { .. }) => {
            if let OrderBook::Snapshot { instrument, .. } = &book {
                books.insert(instrument.key.clone(), book.clone());
            }
        }
        _ => {}
    }
    // ANCHOR_END: resync
}

/// Rule 8: exact numbers, rounded to the step of the instrument.
pub fn rule_decimal(best_bid: Price, params: &TradingParams) -> Result<Price, Error> {
    // ANCHOR: decimal
    // 1% below the bid, rounded DOWN to the price step (for a sell round UP). No f64 anywhere.
    let wanted = best_bid * Decimal::new(99, 2);
    let price = wanted
        .round_to_step(params.price_step, RoundMode::Down)
        .map_err(|e| Error::Config(ConfigError::Message(e.to_string())))?;
    // ANCHOR_END: decimal
    Ok(price)
}

/// Rule 9: the connector refuses a price off the step, if it knows the step.
pub fn rule_price_grid(price_grid: &brokapi::runtime::PriceGrid, params: &TradingParams) {
    // ANCHOR: price_grid
    price_grid.learn(params); // once per instrument; then place() refuses a price off the step before sending
    // ANCHOR_END: price_grid
}

/// Rule 10: shutdown cancels nothing. If the bot wants its orders gone, it cancels them first.
pub async fn rule_stop<U: OrderExecutor>(ud: &U, working: &[ClientOrderId]) -> Res {
    // ANCHOR: stop
    for cid in working {
        // Ok(()) means cancelled. Rejected: already final. Unknown: ask order_state, cancel again only if it works.
        let _ = ud.cancel(*cid).await;
    }
    // Then the connector: `shutdown` takes it by value. Account connectors first, market data last.
    // ud.shutdown(ShutdownMode::Graceful { deadline: Duration::from_secs(5) }).await;
    // ANCHOR_END: stop
    let _ = (ShutdownMode::Immediate, Duration::from_secs(1));
    Ok(())
}

/// Rule 11: orders of other people are shown, never touched.
pub fn rule_foreign(report: &ReconcileReport) {
    // ANCHOR: foreign
    for order in &report.foreign {
        // Placed by hand or by another program. No cancel, no replace. A person decides.
        println!("foreign order {}: {:?}", order.cid, order.state);
    }
    // ANCHOR_END: foreign
}

/// Rule 12: a long candle history is read in windows, within the limit.
pub async fn rule_history<M: brokapi::core::MarketDataSource>(
    md: &M,
    id: &brokapi::core::InstrumentId,
    range: brokapi::core::TimeRange,
) -> Res {
    // ANCHOR: history
    use brokapi::runtime::history::{CandlePages, HistoryOpts};
    let mut pages = CandlePages::new(
        md,
        id,
        brokapi::core::Timeframe::H1,
        range,
        HistoryOpts::default(),
    );
    while let Some(bars) = pages.next_page().await? {
        // write `bars` to disk; after a stop continue from pages.resume_from()
        let _ = bars;
    }
    // ANCHOR_END: history
    Ok(())
}

/// A subscription for the book: Conflate, because only the latest book matters.
pub async fn rule_book_subscription<M: brokapi::core::OrderBookSource>(
    md: &M,
    id: &brokapi::core::InstrumentId,
) -> Res {
    // ANCHOR: book_subscription
    let sub = md
        .subscribe_order_book(
            id,
            SubscribeOpts {
                depth: Some(10),
                ..SubscribeOpts::conflate()
            },
        )
        .await?;
    // events of this subscription: env.source (which connector) + env.sub == Some(sub)
    // ANCHOR_END: book_subscription
    let _ = sub;
    Ok(())
}

fn main() {
    println!("the snippets of the contract page: they only have to compile");
}
