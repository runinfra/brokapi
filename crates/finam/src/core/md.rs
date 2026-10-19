//! Market-data core: books rebuilt from action rows, quotes, trades, bars.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

use brokapi_core::{Event, InstrumentId, ResyncReason, SubscriptionId, Timeframe};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pb;
use brokapi_sansio::{
    BrokerCore, CoreEvent, DesiredSubscription, EventSink, Outbound, SubscriptionKind,
    SubscriptionSet,
};

use super::{StreamEvent, SubStatus};
use crate::map::m10::M10Builder;
use crate::map::market::{
    ApplyOutcome, BookState, book_snapshot, map_bar, map_quote, map_trade, stream_rows,
};

/// Default depth of emitted books when the subscription did not ask for one.
pub const DEFAULT_DEPTH: u32 = 50;

/// What arrived on a market-data stream.
#[derive(Debug, Clone)]
pub enum MdEvent {
    /// A message of an order book stream: snapshot or action rows.
    OrderBook(pb::SubscribeOrderBookResponse),
    /// A message of a quote stream. It can carry an in-band error instead of quotes.
    Quotes(pb::SubscribeQuoteResponse),
    /// A message of a market trades stream.
    Trades(pb::SubscribeLatestTradesResponse),
    /// A message of a bars stream.
    Bars(pb::SubscribeBarsResponse),
    /// The stream was opened (also after a reopen).
    Connected,
    /// The stream broke or ended and is being reopened. The text says why.
    Disconnected(String),
    /// The broker refuses the stream (bad symbol, no permission). The text says why.
    Failed(String),
}

/// One stream event of one subscription.
#[derive(Debug, Clone)]
pub struct MdFrame {
    /// The subscription whose stream produced the event.
    pub sub: SubscriptionId,
    /// What happened on that stream.
    pub event: MdEvent,
}

impl MdFrame {
    /// Frame for a stream lifecycle event (connected, disconnected, failed).
    ///
    /// `None` for [`StreamEvent::Msg`], because a message has its own typed variant of [`MdEvent`].
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::SubscriptionId;
    /// use brokapi_finam::core::{MdEvent, MdFrame, StreamEvent};
    ///
    /// let sub = SubscriptionId::new(1);
    /// let frame = MdFrame::lifecycle(sub, StreamEvent::Connected).unwrap();
    /// assert!(matches!(frame.event, MdEvent::Connected));
    /// assert!(MdFrame::lifecycle(sub, StreamEvent::Msg(())).is_none());
    /// ```
    pub fn lifecycle(sub: SubscriptionId, ev: StreamEvent<()>) -> Option<Self> {
        let event = match ev {
            StreamEvent::Connected => MdEvent::Connected,
            StreamEvent::Disconnected(r) => MdEvent::Disconnected(r),
            StreamEvent::Failed(r) => MdEvent::Failed(r),
            StreamEvent::Msg(()) => return None,
        };
        Some(Self { sub, event })
    }
}

#[derive(Debug, Default)]
struct BookEntry {
    state: BookState,
    /// How many times the stream has been (re)opened.
    connects: u32,
}

#[derive(Debug, Default)]
struct SeenTrades {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl SeenTrades {
    const CAP: usize = 2048;

    /// `true` the first time an id is seen. Streams replay recent trades after a reconnect.
    fn first_time(&mut self, id: &str) -> bool {
        if id.is_empty() {
            return true;
        }
        if !self.ids.insert(id.to_string()) {
            return false;
        }
        self.order.push_back(id.to_string());
        if self.order.len() > Self::CAP
            && let Some(old) = self.order.pop_front()
        {
            self.ids.remove(&old);
        }
        true
    }
}

/// Market data core: turns stream frames into events and keeps the state behind them.
///
/// Per subscription it holds the local order book (rebuilt from action rows), the ids of trades
/// already seen (a reconnect replays recent trades, a repeat is dropped), the M10 candle being built
/// from M5 bars, and the stream status.
///
/// Implements `BrokerCore` from `brokapi-sansio`. See the [module docs](crate::core) for an example.
#[derive(Debug, Default)]
pub struct MdCore {
    desired: SubscriptionSet,
    status: HashMap<SubscriptionId, SubStatus>,
    books: HashMap<SubscriptionId, BookEntry>,
    seen_trades: HashMap<SubscriptionId, SeenTrades>,
    /// M10 subscriptions: the stream is M5, the 10-minute candle is built here.
    m10: HashMap<SubscriptionId, M10Builder>,
    /// Subscriptions whose stream must be reopened (book out of sync).
    restarts: Vec<SubscriptionId>,
}

impl MdCore {
    /// An empty core with no subscriptions.
    pub fn new() -> Self {
        Self::default()
    }

    /// The subscriptions the core was told to serve (set by `apply`).
    pub fn desired(&self) -> &SubscriptionSet {
        &self.desired
    }

    /// Stream status of subscription `sub`. `None` for an id the core does not know.
    pub fn status(&self, sub: SubscriptionId) -> Option<SubStatus> {
        self.status.get(&sub).cloned()
    }

    /// Streams to reopen because their local book became unusable. Drains the list.
    pub fn take_restarts(&mut self) -> Vec<SubscriptionId> {
        std::mem::take(&mut self.restarts)
    }

    fn kind(&self, sub: SubscriptionId) -> Option<&SubscriptionKind> {
        self.desired.find(sub).map(|d| &d.kind)
    }

    fn on_order_book(
        &mut self,
        sub: SubscriptionId,
        instrument: InstrumentId,
        depth: u32,
        msg: pb::StreamOrderBook,
        now: Instant,
        out: &mut EventSink,
    ) {
        let rows = stream_rows(&msg.rows);
        let entry = self.books.entry(sub).or_default();
        match entry.state.apply(&rows, msg.is_data_snapshot) {
            ApplyOutcome::NeedSnapshot => {
                // Increment without a base: drop it and ask for a fresh stream (which starts
                // with a snapshot). The consumer is told the book is not trustworthy.
                self.restarts.push(sub);
                out.push(CoreEvent {
                    sub: Some(sub),
                    payload: Event::Resync {
                        sub,
                        instrument,
                        reason: ResyncReason::SequenceGap,
                    },
                });
            }
            ApplyOutcome::Applied => {
                let broker_ts = rows.iter().filter_map(|r| r.ts).max();
                let book = book_snapshot(&instrument, &entry.state, depth as usize, broker_ts, now);
                out.push(CoreEvent {
                    sub: Some(sub),
                    payload: Event::OrderBook(book),
                });
            }
        }
    }
}

impl BrokerCore for MdCore {
    type Frame = MdFrame;

    fn on_frame(&mut self, frame: MdFrame, now: Instant, out: &mut EventSink) {
        let MdFrame { sub, event } = frame;
        let Some(kind) = self.kind(sub).cloned() else {
            // Late frame of an unsubscribed stream.
            return;
        };
        match event {
            MdEvent::Connected => {
                self.status.insert(sub, SubStatus::Active);
                if let SubscriptionKind::OrderBook { instrument, .. } = &kind {
                    // A (re)opened stream starts with a snapshot: the old book is void.
                    let entry = self.books.entry(sub).or_default();
                    let reopened = entry.connects > 0;
                    entry.state.reset();
                    entry.connects += 1;
                    if reopened {
                        out.push(CoreEvent {
                            sub: Some(sub),
                            payload: Event::Resync {
                                sub,
                                instrument: instrument.clone(),
                                reason: ResyncReason::SubscriptionReset,
                            },
                        });
                    }
                }
            }
            MdEvent::Disconnected(r) => {
                self.status.insert(sub, SubStatus::Reconnecting(r));
            }
            MdEvent::Failed(r) => {
                self.status.insert(sub, SubStatus::Failed(r));
            }
            MdEvent::OrderBook(msg) => {
                if let SubscriptionKind::OrderBook { instrument, depth } = kind {
                    self.status.insert(sub, SubStatus::Active);
                    let symbol = format!("{}@{}", instrument.key.symbol, instrument.key.mic);
                    for book in msg.order_book {
                        if !book.symbol.is_empty() && book.symbol != symbol {
                            continue;
                        }
                        self.on_order_book(sub, instrument.clone(), depth, book, now, out);
                    }
                }
            }
            MdEvent::Quotes(msg) => {
                if let SubscriptionKind::Quotes { instrument } = kind {
                    if let Some(e) = &msg.error {
                        self.status.insert(
                            sub,
                            SubStatus::Erroring(format!("{}: {}", e.code, e.description)),
                        );
                        return;
                    }
                    self.status.insert(sub, SubStatus::Active);
                    let symbol = format!("{}@{}", instrument.key.symbol, instrument.key.mic);
                    for q in &msg.quote {
                        // One stream per subscription, but be strict about the symbol.
                        if !q.symbol.is_empty() && q.symbol != symbol {
                            continue;
                        }
                        out.push(CoreEvent {
                            sub: Some(sub),
                            payload: Event::Quote(map_quote(&instrument, q, now)),
                        });
                    }
                }
            }
            MdEvent::Trades(msg) => {
                if let SubscriptionKind::LastTrades { instrument } = kind {
                    self.status.insert(sub, SubStatus::Active);
                    let seen = self.seen_trades.entry(sub).or_default();
                    for t in &msg.trades {
                        if !seen.first_time(&t.trade_id) {
                            continue;
                        }
                        if let Some(trade) = map_trade(&instrument, t, now) {
                            out.push(CoreEvent {
                                sub: Some(sub),
                                payload: Event::MarketTrade(trade),
                            });
                        }
                    }
                }
            }
            MdEvent::Bars(msg) => {
                if let SubscriptionKind::LastCandle {
                    instrument,
                    timeframe,
                } = kind
                {
                    self.status.insert(sub, SubStatus::Active);
                    for b in &msg.bars {
                        let candle = if timeframe == Timeframe::M10 {
                            // The wire stream is M5.
                            let m5 = map_bar(&instrument, Timeframe::M5, b, now);
                            match self.m10.entry(sub).or_default().on_m5(&m5) {
                                Some(c) => c,
                                None => continue,
                            }
                        } else {
                            map_bar(&instrument, timeframe, b, now)
                        };
                        out.push(CoreEvent {
                            sub: Some(sub),
                            payload: Event::Candle(candle),
                        });
                    }
                }
            }
        }
    }

    /// gRPC streams are opened by the stream tasks, nothing is queued for a socket.
    fn poll_transmit(&mut self) -> Option<Outbound> {
        None
    }

    fn poll_timeout(&self) -> Option<Instant> {
        None
    }

    fn on_timeout(&mut self, _now: Instant, _out: &mut EventSink) {}

    fn apply(&mut self, desired: &SubscriptionSet) {
        let keep: HashSet<SubscriptionId> = desired.iter().map(|d| d.id).collect();
        self.status.retain(|id, _| keep.contains(id));
        self.books.retain(|id, _| keep.contains(id));
        self.seen_trades.retain(|id, _| keep.contains(id));
        self.m10.retain(|id, _| keep.contains(id));
        for DesiredSubscription { id, kind } in desired.iter() {
            self.status.entry(*id).or_insert(SubStatus::Connecting);
            if matches!(kind, SubscriptionKind::OrderBook { .. }) {
                self.books.entry(*id).or_default();
            }
        }
        self.desired = desired.clone();
    }
}
