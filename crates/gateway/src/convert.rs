//! The values of `brokapi-core` and their form on the wire.
//!
//! Going to the wire cannot fail. Coming back can: a bad number, a bad id, a field that must be there and is not. That
//! is [`Invalid`], and it means the other side did not follow the `.proto` (a bot sending a malformed request, or a
//! gateway of another version).

use std::str::FromStr;
use std::sync::Arc;

use brokapi::core::{
    Backpressure, Balance, BookLevel, BrokerOrderId, Candle, ClientOrderId, Envelope, Event,
    Execution, Instrument, InstrumentId, InstrumentKey, InstrumentKind, InstrumentQuery,
    MarketDataCaps, MarketTrade, Money, NativeId, OrderAck, OrderBook, OrderSnapshot, OrderState,
    OrderType, PlaceIdempotency, PlaceOrderRequest, PlaceStopOrderRequest, Portfolio, Position,
    Price, Qty, Quote, ReconcileReport, ReplaceOrderRequest, ResyncReason, Schedule,
    SessionInterval, SessionKind, Side, StopExpiry, StopOrderKind, StopOrderRef, StopOrderSnapshot,
    StopOrderState, StreamingCaps, SubscribeOpts, SubscriptionId, TimeRange, Timeframe, Timestamps,
    TradingAvailability, TradingParams, TradingStatus, UnixNanos, UserDataCaps,
};

use crate::pb;

/// A message that does not follow the `.proto`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid(pub String);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Invalid {}

type R<T> = Result<T, Invalid>;

fn bad<T>(what: impl Into<String>) -> R<T> {
    Err(Invalid(what.into()))
}

fn need<T>(value: Option<T>, what: &str) -> R<T> {
    value.ok_or_else(|| Invalid(format!("{what} is missing")))
}

/// A decimal number (`Price`, `Qty`, `Money`) from its text.
pub fn number<T: FromStr>(text: &str, what: &str) -> R<T> {
    text.parse::<T>()
        .map_err(|_| Invalid(format!("{what} {text:?} is not a decimal number")))
}

fn opt_number<T: FromStr>(text: &Option<String>, what: &str) -> R<Option<T>> {
    text.as_deref().map(|t| number(t, what)).transpose()
}

/// An order id from its text.
pub fn cid(text: &str) -> R<ClientOrderId> {
    ClientOrderId::parse_str(text)
        .map_err(|_| Invalid(format!("the order id {text:?} is not a UUID")))
}

fn cid_text(cid: &ClientOrderId) -> String {
    cid.hyphenated().to_string()
}

fn list<A, B>(items: &[A], f: impl Fn(&A) -> R<B>) -> R<Vec<B>> {
    items.iter().map(f).collect()
}

// ---------------------------------------------------------------------------------------------------- small enums

fn side_to(s: Side) -> i32 {
    match s {
        Side::Buy => pb::Side::Buy as i32,
        Side::Sell => pb::Side::Sell as i32,
    }
}

fn side_from(v: i32) -> R<Side> {
    match pb::Side::try_from(v) {
        Ok(pb::Side::Buy) => Ok(Side::Buy),
        Ok(pb::Side::Sell) => Ok(Side::Sell),
        _ => bad("side is not set"),
    }
}

fn order_type_to(t: OrderType) -> i32 {
    match t {
        OrderType::Market => pb::OrderType::Market as i32,
        OrderType::Limit => pb::OrderType::Limit as i32,
    }
}

fn order_type_from(v: i32) -> R<OrderType> {
    match pb::OrderType::try_from(v) {
        Ok(pb::OrderType::Market) => Ok(OrderType::Market),
        Ok(pb::OrderType::Limit) => Ok(OrderType::Limit),
        _ => bad("order_type is not set"),
    }
}

/// A timeframe to its number on the wire.
pub fn timeframe_to(t: Timeframe) -> i32 {
    let value = match t {
        Timeframe::M1 => pb::Timeframe::M1,
        Timeframe::M5 => pb::Timeframe::M5,
        Timeframe::M10 => pb::Timeframe::M10,
        Timeframe::M15 => pb::Timeframe::M15,
        Timeframe::M30 => pb::Timeframe::M30,
        Timeframe::H1 => pb::Timeframe::H1,
        Timeframe::H4 => pb::Timeframe::H4,
        Timeframe::D1 => pb::Timeframe::D1,
        Timeframe::W1 => pb::Timeframe::W1,
        Timeframe::MN => pb::Timeframe::Mn,
    };
    value as i32
}

/// Every timeframe, for a table that has a row for each.
pub const TIMEFRAMES: [Timeframe; 10] = [
    Timeframe::M1,
    Timeframe::M5,
    Timeframe::M10,
    Timeframe::M15,
    Timeframe::M30,
    Timeframe::H1,
    Timeframe::H4,
    Timeframe::D1,
    Timeframe::W1,
    Timeframe::MN,
];

/// The longest span of one `candles` request for each timeframe that the source knows a limit for.
pub fn candle_spans_to(source: &impl brokapi::core::MarketDataSource) -> Vec<pb::CandleSpan> {
    TIMEFRAMES
        .iter()
        .filter_map(|&tf| {
            source.candles_max_span(tf).map(|span| pb::CandleSpan {
                timeframe: timeframe_to(tf),
                span_ns: u64::try_from(span.as_nanos()).unwrap_or(u64::MAX),
            })
        })
        .collect()
}

/// The table of [`candle_spans_to`] read back.
pub fn candle_spans_from(spans: &[pb::CandleSpan]) -> Vec<(Timeframe, std::time::Duration)> {
    spans
        .iter()
        .filter_map(|s| {
            Some((
                timeframe_from(s.timeframe).ok()?,
                std::time::Duration::from_nanos(s.span_ns),
            ))
        })
        .collect()
}

/// A timeframe from its number on the wire.
pub fn timeframe_from(v: i32) -> R<Timeframe> {
    Ok(match pb::Timeframe::try_from(v) {
        Ok(pb::Timeframe::M1) => Timeframe::M1,
        Ok(pb::Timeframe::M5) => Timeframe::M5,
        Ok(pb::Timeframe::M10) => Timeframe::M10,
        Ok(pb::Timeframe::M15) => Timeframe::M15,
        Ok(pb::Timeframe::M30) => Timeframe::M30,
        Ok(pb::Timeframe::H1) => Timeframe::H1,
        Ok(pb::Timeframe::H4) => Timeframe::H4,
        Ok(pb::Timeframe::D1) => Timeframe::D1,
        Ok(pb::Timeframe::W1) => Timeframe::W1,
        Ok(pb::Timeframe::Mn) => Timeframe::MN,
        _ => return bad("timeframe is not set"),
    })
}

fn kind_to(k: InstrumentKind) -> i32 {
    let value = match k {
        InstrumentKind::Stock => pb::InstrumentKind::Stock,
        InstrumentKind::Bond => pb::InstrumentKind::Bond,
        InstrumentKind::Option => pb::InstrumentKind::Option,
        InstrumentKind::Future => pb::InstrumentKind::Future,
        InstrumentKind::Perpetual => pb::InstrumentKind::Perpetual,
        InstrumentKind::Etf => pb::InstrumentKind::Etf,
        InstrumentKind::MutualFund => pb::InstrumentKind::MutualFund,
        InstrumentKind::Currency => pb::InstrumentKind::Currency,
        InstrumentKind::Other => pb::InstrumentKind::Other,
    };
    value as i32
}

fn kind_from(v: i32) -> R<InstrumentKind> {
    Ok(match pb::InstrumentKind::try_from(v) {
        Ok(pb::InstrumentKind::Stock) => InstrumentKind::Stock,
        Ok(pb::InstrumentKind::Bond) => InstrumentKind::Bond,
        Ok(pb::InstrumentKind::Option) => InstrumentKind::Option,
        Ok(pb::InstrumentKind::Future) => InstrumentKind::Future,
        Ok(pb::InstrumentKind::Perpetual) => InstrumentKind::Perpetual,
        Ok(pb::InstrumentKind::Etf) => InstrumentKind::Etf,
        Ok(pb::InstrumentKind::MutualFund) => InstrumentKind::MutualFund,
        Ok(pb::InstrumentKind::Currency) => InstrumentKind::Currency,
        Ok(pb::InstrumentKind::Other) => InstrumentKind::Other,
        _ => return bad("instrument kind is not set"),
    })
}

fn stop_kind_to(k: StopOrderKind) -> i32 {
    let value = match k {
        StopOrderKind::StopLoss => pb::StopOrderKind::StopLoss,
        StopOrderKind::TakeProfit => pb::StopOrderKind::TakeProfit,
        StopOrderKind::StopLimit => pb::StopOrderKind::StopLimit,
    };
    value as i32
}

fn stop_kind_from(v: i32) -> R<StopOrderKind> {
    Ok(match pb::StopOrderKind::try_from(v) {
        Ok(pb::StopOrderKind::StopLoss) => StopOrderKind::StopLoss,
        Ok(pb::StopOrderKind::TakeProfit) => StopOrderKind::TakeProfit,
        Ok(pb::StopOrderKind::StopLimit) => StopOrderKind::StopLimit,
        _ => return bad("stop order kind is not set"),
    })
}

// ---------------------------------------------------------------------------------------------------- time, ids

/// A timestamp to the wire.
pub fn ts_to(ts: &Timestamps) -> pb::Timestamps {
    pb::Timestamps {
        rx_ts_ns: ts.rx_ts.as_nanos(),
        broker_ts_ns: ts.broker_ts.map(UnixNanos::as_nanos),
    }
}

/// A timestamp from the wire. The monotonic part is the moment of this call: it never travels.
pub fn ts_from(ts: &Option<pb::Timestamps>) -> R<Timestamps> {
    let ts = need(ts.as_ref(), "ts")?;
    let out = Timestamps::new(UnixNanos::from_nanos(ts.rx_ts_ns), brokapi::runtime::now());
    Ok(match ts.broker_ts_ns {
        Some(ns) => out.with_broker_ts(UnixNanos::from_nanos(ns)),
        None => out,
    })
}

/// A time range to the wire.
pub fn range_to(r: TimeRange) -> pb::TimeRange {
    pb::TimeRange {
        from_ns: r.from.as_nanos(),
        to_ns: r.to.as_nanos(),
    }
}

/// A time range from the wire.
pub fn range_from(r: &Option<pb::TimeRange>) -> R<TimeRange> {
    let r = need(r.as_ref(), "range")?;
    Ok(TimeRange::new(
        UnixNanos::from_nanos(r.from_ns),
        UnixNanos::from_nanos(r.to_ns),
    ))
}

/// An instrument id to the wire.
pub fn instrument_to(id: &InstrumentId) -> pb::InstrumentId {
    use pb::native_id::Id;
    let id_ = match &id.native {
        NativeId::SymbolMic { symbol, mic } => Id::SymbolMic(pb::SymbolMic {
            symbol: symbol.clone(),
            mic: mic.clone(),
        }),
        NativeId::TickerClass { ticker, class_code } => Id::TickerClass(pb::TickerClass {
            ticker: ticker.clone(),
            class_code: class_code.clone(),
        }),
        NativeId::Figi(v) => Id::Figi(v.clone()),
        NativeId::Uid(v) => Id::Uid(v.clone()),
        // A way of naming that a later version adds: the key still identifies the instrument.
        _ => Id::Uid(id.key.display()),
    };
    pb::InstrumentId {
        symbol: id.key.symbol.clone(),
        mic: id.key.mic.clone(),
        native: Some(pb::NativeId { id: Some(id_) }),
    }
}

/// An instrument id from the wire.
pub fn instrument_from(id: &Option<pb::InstrumentId>) -> R<InstrumentId> {
    use pb::native_id::Id;
    let id = need(id.as_ref(), "instrument")?;
    let native = match need(
        id.native.as_ref().and_then(|n| n.id.as_ref()),
        "instrument.native",
    )? {
        Id::SymbolMic(v) => NativeId::SymbolMic {
            symbol: v.symbol.clone(),
            mic: v.mic.clone(),
        },
        Id::TickerClass(v) => NativeId::TickerClass {
            ticker: v.ticker.clone(),
            class_code: v.class_code.clone(),
        },
        Id::Figi(v) => NativeId::Figi(v.clone()),
        Id::Uid(v) => NativeId::Uid(v.clone()),
    };
    Ok(InstrumentId::new(
        InstrumentKey::new(id.symbol.clone(), id.mic.clone()),
        native,
    ))
}

// ---------------------------------------------------------------------------------------------------- subscriptions

/// Subscription options to the wire.
pub fn opts_to(o: &SubscribeOpts) -> pb::SubscribeOpts {
    let (backpressure, cap) = match o.backpressure {
        Backpressure::Conflate => (pb::Backpressure::Conflate, 0),
        Backpressure::Lossless { cap } => (pb::Backpressure::Lossless, cap as u64),
        Backpressure::DropNewest { cap } => (pb::Backpressure::DropNewest, cap as u64),
        _ => (pb::Backpressure::Unspecified, 0),
    };
    pb::SubscribeOpts {
        backpressure: backpressure as i32,
        cap,
        depth: o.depth,
    }
}

/// Subscription options from the wire.
pub fn opts_from(o: &Option<pb::SubscribeOpts>) -> R<SubscribeOpts> {
    let o = need(o.as_ref(), "opts")?;
    let cap = usize::try_from(o.cap).map_err(|_| Invalid("opts.cap is too large".into()))?;
    let backpressure = match pb::Backpressure::try_from(o.backpressure) {
        Ok(pb::Backpressure::Conflate) => Backpressure::Conflate,
        Ok(pb::Backpressure::Lossless) => Backpressure::Lossless { cap },
        Ok(pb::Backpressure::DropNewest) => Backpressure::DropNewest { cap },
        _ => return bad("opts.backpressure is not set"),
    };
    Ok(SubscribeOpts {
        backpressure,
        depth: o.depth,
    })
}

// ---------------------------------------------------------------------------------------------------- orders

/// A place request to the wire.
pub fn place_to(r: &PlaceOrderRequest) -> pb::PlaceOrderRequest {
    pb::PlaceOrderRequest {
        cid: cid_text(&r.cid),
        instrument: Some(instrument_to(&r.instrument)),
        side: side_to(r.side),
        order_type: order_type_to(r.order_type),
        qty: r.qty.to_string(),
        price: r.price.map(|p| p.to_string()),
    }
}

/// A place request from the wire.
pub fn place_from(r: &pb::PlaceOrderRequest) -> R<PlaceOrderRequest> {
    Ok(PlaceOrderRequest {
        cid: cid(&r.cid)?,
        instrument: instrument_from(&r.instrument)?,
        side: side_from(r.side)?,
        order_type: order_type_from(r.order_type)?,
        qty: number(&r.qty, "qty")?,
        price: opt_number(&r.price, "price")?,
    })
}

/// A replace request to the wire.
pub fn replace_to(r: &ReplaceOrderRequest) -> pb::ReplaceOrderRequest {
    pb::ReplaceOrderRequest {
        cid: cid_text(&r.cid),
        original_cid: cid_text(&r.original_cid),
        qty: r.qty.map(|q| q.to_string()),
        price: r.price.map(|p| p.to_string()),
    }
}

/// A replace request from the wire.
pub fn replace_from(r: &pb::ReplaceOrderRequest) -> R<ReplaceOrderRequest> {
    Ok(ReplaceOrderRequest {
        cid: cid(&r.cid)?,
        original_cid: cid(&r.original_cid)?,
        qty: opt_number(&r.qty, "qty")?,
        price: opt_number(&r.price, "price")?,
    })
}

/// An acknowledgement to the wire.
pub fn ack_to(a: &OrderAck) -> pb::OrderAck {
    pb::OrderAck {
        cid: cid_text(&a.cid),
        broker_id: a.broker_id.as_str().to_owned(),
        ts: Some(ts_to(&a.ts)),
    }
}

/// An acknowledgement from the wire.
pub fn ack_from(a: &pb::OrderAck) -> R<OrderAck> {
    Ok(OrderAck {
        cid: cid(&a.cid)?,
        broker_id: BrokerOrderId::new(a.broker_id.clone()),
        ts: ts_from(&a.ts)?,
    })
}

fn state_to(s: &OrderState) -> pb::OrderState {
    use pb::OrderStateKind as K;
    let mut out = pb::OrderState {
        kind: K::Unspecified as i32,
        cid: cid_text(&s.cid()),
        ..Default::default()
    };
    match s {
        OrderState::InFlight { .. } => out.kind = K::InFlight as i32,
        OrderState::Unknown { .. } => out.kind = K::Unknown as i32,
        OrderState::Acked { broker_id, .. } => {
            out.kind = K::Acked as i32;
            out.broker_id = broker_id.as_str().to_owned();
        }
        OrderState::PartiallyFilled { filled, left, .. } => {
            out.kind = K::PartiallyFilled as i32;
            out.filled = filled.to_string();
            out.left = left.to_string();
        }
        OrderState::Filled { .. } => out.kind = K::Filled as i32,
        OrderState::Cancelled { .. } => out.kind = K::Cancelled as i32,
        OrderState::Rejected { reason, .. } => {
            out.kind = K::Rejected as i32;
            out.reason = reason.clone();
        }
        _ => {}
    }
    out
}

fn state_from(s: &Option<pb::OrderState>) -> R<OrderState> {
    use pb::OrderStateKind as K;
    let s = need(s.as_ref(), "state")?;
    let cid = cid(&s.cid)?;
    let now = brokapi::runtime::now();
    Ok(match K::try_from(s.kind) {
        Ok(K::InFlight) => OrderState::InFlight { cid, sent_at: now },
        Ok(K::Unknown) => OrderState::Unknown { cid, since: now },
        Ok(K::Acked) => OrderState::Acked {
            cid,
            broker_id: BrokerOrderId::new(s.broker_id.clone()),
        },
        Ok(K::PartiallyFilled) => OrderState::PartiallyFilled {
            cid,
            filled: number(&s.filled, "state.filled")?,
            left: number(&s.left, "state.left")?,
        },
        Ok(K::Filled) => OrderState::Filled { cid },
        Ok(K::Cancelled) => OrderState::Cancelled { cid },
        Ok(K::Rejected) => OrderState::Rejected {
            cid,
            reason: s.reason.clone(),
        },
        _ => return bad("state.kind is not set"),
    })
}

/// An order snapshot to the wire.
pub fn order_to(o: &OrderSnapshot) -> pb::OrderSnapshot {
    pb::OrderSnapshot {
        cid: cid_text(&o.cid),
        broker_id: o.broker_id.as_ref().map(|b| b.as_str().to_owned()),
        instrument: Some(instrument_to(&o.instrument)),
        state: Some(state_to(&o.state)),
        side: side_to(o.side),
        order_type: order_type_to(o.order_type),
        qty: o.qty.to_string(),
        filled: o.filled.to_string(),
        price: o.price.map(|p| p.to_string()),
        ts: Some(ts_to(&o.ts)),
    }
}

/// An order snapshot from the wire.
pub fn order_from(o: &pb::OrderSnapshot) -> R<OrderSnapshot> {
    Ok(OrderSnapshot::new(
        cid(&o.cid)?,
        instrument_from(&o.instrument)?,
        state_from(&o.state)?,
        side_from(o.side)?,
        order_type_from(o.order_type)?,
        number(&o.qty, "qty")?,
        ts_from(&o.ts)?,
    )
    .with_broker_id(o.broker_id.clone().map(BrokerOrderId::new))
    .with_filled(number(&o.filled, "filled")?)
    .with_price(opt_number(&o.price, "price")?))
}

/// A reconcile report to the wire.
pub fn report_to(r: &ReconcileReport) -> pb::ReconcileReport {
    pb::ReconcileReport {
        confirmed: r.confirmed.iter().map(order_to).collect(),
        closed_offline: r.closed_offline.iter().map(cid_text).collect(),
        foreign: r.foreign.iter().map(order_to).collect(),
    }
}

/// A reconcile report from the wire.
pub fn report_from(r: &pb::ReconcileReport) -> R<ReconcileReport> {
    Ok(ReconcileReport::new(
        list(&r.confirmed, order_from)?,
        list(&r.closed_offline, |c| cid(c))?,
        list(&r.foreign, order_from)?,
    ))
}

/// A list of order ids to the wire.
pub fn cids_to(cids: &[ClientOrderId]) -> Vec<String> {
    cids.iter().map(cid_text).collect()
}

/// A list of order ids from the wire.
pub fn cids_from(cids: &[String]) -> R<Vec<ClientOrderId>> {
    list(cids, |c| cid(c))
}

/// An order id to the wire.
pub fn cid_to(c: &ClientOrderId) -> String {
    cid_text(c)
}

// ---------------------------------------------------------------------------------------------------- stop orders

fn expiry_to(e: &StopExpiry) -> pb::StopExpiry {
    use pb::stop_expiry::Kind;
    let (kind, date_ns) = match e {
        StopExpiry::GoodTillCancel => (Kind::GoodTillCancel, 0),
        StopExpiry::EndOfDay => (Kind::EndOfDay, 0),
        StopExpiry::GoodTillDate(t) => (Kind::GoodTillDate, t.as_nanos()),
    };
    pb::StopExpiry {
        kind: kind as i32,
        date_ns,
    }
}

fn expiry_from(e: &Option<pb::StopExpiry>) -> R<StopExpiry> {
    use pb::stop_expiry::Kind;
    let e = need(e.as_ref(), "expiry")?;
    Ok(match Kind::try_from(e.kind) {
        Ok(Kind::GoodTillCancel) => StopExpiry::GoodTillCancel,
        Ok(Kind::EndOfDay) => StopExpiry::EndOfDay,
        Ok(Kind::GoodTillDate) => StopExpiry::GoodTillDate(UnixNanos::from_nanos(e.date_ns)),
        _ => return bad("expiry.kind is not set"),
    })
}

/// A place stop order request to the wire.
pub fn place_stop_to(r: &PlaceStopOrderRequest) -> pb::PlaceStopOrderRequest {
    pb::PlaceStopOrderRequest {
        cid: cid_text(&r.cid),
        instrument: Some(instrument_to(&r.instrument)),
        side: side_to(r.side),
        kind: stop_kind_to(r.kind),
        qty: r.qty.to_string(),
        stop_price: r.stop_price.to_string(),
        limit_price: r.limit_price.map(|p| p.to_string()),
        expiry: Some(expiry_to(&r.expiry)),
    }
}

/// A place stop order request from the wire.
pub fn place_stop_from(r: &pb::PlaceStopOrderRequest) -> R<PlaceStopOrderRequest> {
    Ok(PlaceStopOrderRequest {
        cid: cid(&r.cid)?,
        instrument: instrument_from(&r.instrument)?,
        side: side_from(r.side)?,
        kind: stop_kind_from(r.kind)?,
        qty: number(&r.qty, "qty")?,
        stop_price: number(&r.stop_price, "stop_price")?,
        limit_price: opt_number(&r.limit_price, "limit_price")?,
        expiry: expiry_from(&r.expiry)?,
    })
}

/// A stop order reference to the wire.
pub fn stop_ref_to(r: &StopOrderRef) -> pb::StopOrderRef {
    use pb::stop_order_ref::Ref;
    pb::StopOrderRef {
        r#ref: Some(match r {
            StopOrderRef::Cid(c) => Ref::Cid(cid_text(c)),
            StopOrderRef::Broker(b) => Ref::BrokerId(b.as_str().to_owned()),
        }),
    }
}

/// A stop order reference from the wire.
pub fn stop_ref_from(r: &Option<pb::StopOrderRef>) -> R<StopOrderRef> {
    use pb::stop_order_ref::Ref;
    match need(r.as_ref().and_then(|r| r.r#ref.as_ref()), "stop")? {
        Ref::Cid(c) => Ok(StopOrderRef::Cid(cid(c)?)),
        Ref::BrokerId(b) => Ok(StopOrderRef::Broker(BrokerOrderId::new(b.clone()))),
    }
}

fn stop_state_to(s: &StopOrderState) -> pb::StopOrderState {
    use pb::stop_order_state::Kind;
    let (kind, other) = match s {
        StopOrderState::Active => (Kind::Active, String::new()),
        StopOrderState::Triggered => (Kind::Triggered, String::new()),
        StopOrderState::Cancelled => (Kind::Cancelled, String::new()),
        StopOrderState::Expired => (Kind::Expired, String::new()),
        StopOrderState::Other(text) => (Kind::Other, text.clone()),
        _ => (Kind::Other, String::new()),
    };
    pb::StopOrderState {
        kind: kind as i32,
        other,
    }
}

fn stop_state_from(s: &Option<pb::StopOrderState>) -> R<StopOrderState> {
    use pb::stop_order_state::Kind;
    let s = need(s.as_ref(), "state")?;
    Ok(match Kind::try_from(s.kind) {
        Ok(Kind::Active) => StopOrderState::Active,
        Ok(Kind::Triggered) => StopOrderState::Triggered,
        Ok(Kind::Cancelled) => StopOrderState::Cancelled,
        Ok(Kind::Expired) => StopOrderState::Expired,
        Ok(Kind::Other) => StopOrderState::Other(s.other.clone()),
        _ => return bad("state.kind is not set"),
    })
}

/// A stop order snapshot to the wire.
pub fn stop_to(s: &StopOrderSnapshot) -> pb::StopOrderSnapshot {
    pb::StopOrderSnapshot {
        cid: s.cid.as_ref().map(cid_text),
        broker_id: s.broker_id.as_str().to_owned(),
        instrument: Some(instrument_to(&s.instrument)),
        side: side_to(s.side),
        kind: stop_kind_to(s.kind),
        qty: s.qty.to_string(),
        stop_price: s.stop_price.to_string(),
        limit_price: s.limit_price.map(|p| p.to_string()),
        expiry: Some(expiry_to(&s.expiry)),
        state: Some(stop_state_to(&s.state)),
        ts: Some(ts_to(&s.ts)),
    }
}

/// A stop order snapshot from the wire.
pub fn stop_from(s: &pb::StopOrderSnapshot) -> R<StopOrderSnapshot> {
    Ok(StopOrderSnapshot {
        cid: s.cid.as_deref().map(cid).transpose()?,
        broker_id: BrokerOrderId::new(s.broker_id.clone()),
        instrument: instrument_from(&s.instrument)?,
        side: side_from(s.side)?,
        kind: stop_kind_from(s.kind)?,
        qty: number(&s.qty, "qty")?,
        stop_price: number(&s.stop_price, "stop_price")?,
        limit_price: opt_number(&s.limit_price, "limit_price")?,
        expiry: expiry_from(&s.expiry)?,
        state: stop_state_from(&s.state)?,
        ts: ts_from(&s.ts)?,
    })
}

// ---------------------------------------------------------------------------------------------------- market data

fn level_to(l: &BookLevel) -> pb::BookLevel {
    pb::BookLevel {
        price: l.price.to_string(),
        qty: l.qty.to_string(),
    }
}

fn level_from(l: &pb::BookLevel) -> R<BookLevel> {
    Ok(BookLevel {
        price: number(&l.price, "price")?,
        qty: number(&l.qty, "qty")?,
    })
}

/// An order book to the wire.
pub fn book_to(b: &OrderBook) -> pb::OrderBook {
    match b {
        OrderBook::Snapshot {
            instrument,
            bids,
            asks,
            ts,
        } => pb::OrderBook {
            instrument: Some(instrument_to(instrument)),
            incremental: false,
            seq: 0,
            bids: bids.iter().map(level_to).collect(),
            asks: asks.iter().map(level_to).collect(),
            ts: Some(ts_to(ts)),
        },
        OrderBook::Increment {
            instrument,
            seq,
            bids,
            asks,
            ts,
        } => pb::OrderBook {
            instrument: Some(instrument_to(instrument)),
            incremental: true,
            seq: *seq,
            bids: bids.iter().map(level_to).collect(),
            asks: asks.iter().map(level_to).collect(),
            ts: Some(ts_to(ts)),
        },
    }
}

/// An order book from the wire.
pub fn book_from(b: &pb::OrderBook) -> R<OrderBook> {
    let instrument = instrument_from(&b.instrument)?;
    let bids = list(&b.bids, level_from)?;
    let asks = list(&b.asks, level_from)?;
    let ts = ts_from(&b.ts)?;
    Ok(if b.incremental {
        OrderBook::Increment {
            instrument,
            seq: b.seq,
            bids,
            asks,
            ts,
        }
    } else {
        OrderBook::Snapshot {
            instrument,
            bids,
            asks,
            ts,
        }
    })
}

/// A market trade to the wire.
pub fn trade_to(t: &MarketTrade) -> pb::MarketTrade {
    pb::MarketTrade {
        instrument: Some(instrument_to(&t.instrument)),
        price: t.price.to_string(),
        qty: t.qty.to_string(),
        side: side_to(t.side),
        trade_id: t.trade_id.clone(),
        ts: Some(ts_to(&t.ts)),
    }
}

/// A market trade from the wire.
pub fn trade_from(t: &pb::MarketTrade) -> R<MarketTrade> {
    Ok(MarketTrade {
        instrument: instrument_from(&t.instrument)?,
        price: number(&t.price, "price")?,
        qty: number(&t.qty, "qty")?,
        side: side_from(t.side)?,
        trade_id: t.trade_id.clone(),
        ts: ts_from(&t.ts)?,
    })
}

fn quote_to(q: &Quote) -> pb::Quote {
    pb::Quote {
        instrument: Some(instrument_to(&q.instrument)),
        bid: q.bid.as_ref().map(level_to),
        ask: q.ask.as_ref().map(level_to),
        ts: Some(ts_to(&q.ts)),
    }
}

fn quote_from(q: &pb::Quote) -> R<Quote> {
    Ok(Quote {
        instrument: instrument_from(&q.instrument)?,
        bid: q.bid.as_ref().map(level_from).transpose()?,
        ask: q.ask.as_ref().map(level_from).transpose()?,
        ts: ts_from(&q.ts)?,
    })
}

/// A candle to the wire.
pub fn candle_to(c: &Candle) -> pb::Candle {
    pb::Candle {
        instrument: Some(instrument_to(&c.instrument)),
        timeframe: timeframe_to(c.timeframe),
        open: c.open.to_string(),
        high: c.high.to_string(),
        low: c.low.to_string(),
        close: c.close.to_string(),
        volume: c.volume.to_string(),
        open_ts_ns: c.open_ts.as_nanos(),
        close_ts_ns: c.close_ts.as_nanos(),
        ts: Some(ts_to(&c.ts)),
    }
}

/// A candle from the wire.
pub fn candle_from(c: &pb::Candle) -> R<Candle> {
    Ok(Candle {
        instrument: instrument_from(&c.instrument)?,
        timeframe: timeframe_from(c.timeframe)?,
        open: number(&c.open, "open")?,
        high: number(&c.high, "high")?,
        low: number(&c.low, "low")?,
        close: number(&c.close, "close")?,
        volume: number(&c.volume, "volume")?,
        open_ts: UnixNanos::from_nanos(c.open_ts_ns),
        close_ts: UnixNanos::from_nanos(c.close_ts_ns),
        ts: ts_from(&c.ts)?,
    })
}

// ---------------------------------------------------------------------------------------------------- account

fn balance_to(b: &Balance) -> pb::Balance {
    pb::Balance {
        currency: b.currency.clone(),
        available: b.available.to_string(),
        blocked: b.blocked.to_string(),
        ts: Some(ts_to(&b.ts)),
    }
}

fn balance_from(b: &pb::Balance) -> R<Balance> {
    Ok(Balance {
        currency: b.currency.clone(),
        available: number::<Money>(&b.available, "available")?,
        blocked: number::<Money>(&b.blocked, "blocked")?,
        ts: ts_from(&b.ts)?,
    })
}

fn position_to(p: &Position) -> pb::Position {
    pb::Position {
        instrument: Some(instrument_to(&p.instrument)),
        qty: p.qty.to_string(),
        avg_price: p.avg_price.map(|v| v.to_string()),
        current_price: p.current_price.map(|v| v.to_string()),
        ts: Some(ts_to(&p.ts)),
    }
}

fn position_from(p: &pb::Position) -> R<Position> {
    Ok(Position {
        instrument: instrument_from(&p.instrument)?,
        qty: number::<Qty>(&p.qty, "qty")?,
        avg_price: opt_number::<Price>(&p.avg_price, "avg_price")?,
        current_price: opt_number::<Price>(&p.current_price, "current_price")?,
        ts: ts_from(&p.ts)?,
    })
}

/// Balances to the wire.
pub fn balances_to(b: &[Balance]) -> Vec<pb::Balance> {
    b.iter().map(balance_to).collect()
}

/// Balances from the wire.
pub fn balances_from(b: &[pb::Balance]) -> R<Vec<Balance>> {
    list(b, balance_from)
}

/// Positions to the wire.
pub fn positions_to(p: &[Position]) -> Vec<pb::Position> {
    p.iter().map(position_to).collect()
}

/// Positions from the wire.
pub fn positions_from(p: &[pb::Position]) -> R<Vec<Position>> {
    list(p, position_from)
}

/// A portfolio to the wire.
pub fn portfolio_to(p: &Portfolio) -> pb::Portfolio {
    pb::Portfolio {
        balances: balances_to(&p.balances),
        positions: positions_to(&p.positions),
        ts: Some(ts_to(&p.ts)),
    }
}

/// A portfolio from the wire.
pub fn portfolio_from(p: &pb::Portfolio) -> R<Portfolio> {
    Ok(Portfolio {
        balances: balances_from(&p.balances)?,
        positions: positions_from(&p.positions)?,
        ts: ts_from(&p.ts)?,
    })
}

/// An execution to the wire.
pub fn execution_to(e: &Execution) -> pb::Execution {
    pb::Execution {
        cid: e.cid.as_ref().map(cid_text),
        broker_order_id: e.broker_order_id.as_str().to_owned(),
        instrument: Some(instrument_to(&e.instrument)),
        side: side_to(e.side),
        price: e.price.to_string(),
        qty: e.qty.to_string(),
        commission: e.commission.to_string(),
        trade_id: e.trade_id.clone(),
        ts: Some(ts_to(&e.ts)),
    }
}

/// An execution from the wire.
pub fn execution_from(e: &pb::Execution) -> R<Execution> {
    Ok(Execution {
        cid: e.cid.as_deref().map(cid).transpose()?,
        broker_order_id: BrokerOrderId::new(e.broker_order_id.clone()),
        instrument: instrument_from(&e.instrument)?,
        side: side_from(e.side)?,
        price: number(&e.price, "price")?,
        qty: number(&e.qty, "qty")?,
        commission: number(&e.commission, "commission")?,
        trade_id: e.trade_id.clone(),
        ts: ts_from(&e.ts)?,
    })
}

// ---------------------------------------------------------------------------------------------------- instruments

/// An instrument to the wire.
pub fn instrument_info_to(i: &Instrument) -> pb::Instrument {
    pb::Instrument {
        id: Some(instrument_to(&i.id)),
        name: i.name.clone(),
        isin: i.isin.clone(),
        kind: kind_to(i.kind),
        currency: i.currency.clone(),
        exchange: i.exchange.clone(),
    }
}

/// An instrument from the wire.
pub fn instrument_info_from(i: &pb::Instrument) -> R<Instrument> {
    Ok(Instrument {
        id: instrument_from(&i.id)?,
        name: i.name.clone(),
        isin: i.isin.clone(),
        kind: kind_from(i.kind)?,
        currency: i.currency.clone(),
        exchange: i.exchange.clone(),
    })
}

/// An instrument query to the wire.
pub fn query_to(q: &InstrumentQuery) -> pb::InstrumentQuery {
    use pb::instrument_query::{ByTicker, Query};
    pb::InstrumentQuery {
        query: Some(match q {
            InstrumentQuery::ByTicker { ticker, exchange } => Query::ByTicker(ByTicker {
                ticker: ticker.clone(),
                exchange: exchange.clone(),
            }),
            InstrumentQuery::ByIsin(isin) => Query::ByIsin(isin.clone()),
            InstrumentQuery::ByKind(kind) => Query::ByKind(kind_to(*kind)),
        }),
    }
}

/// An instrument query from the wire.
pub fn query_from(q: &Option<pb::InstrumentQuery>) -> R<InstrumentQuery> {
    use pb::instrument_query::Query;
    Ok(
        match need(q.as_ref().and_then(|q| q.query.as_ref()), "query")? {
            Query::ByTicker(t) => InstrumentQuery::ByTicker {
                ticker: t.ticker.clone(),
                exchange: t.exchange.clone(),
            },
            Query::ByIsin(isin) => InstrumentQuery::ByIsin(isin.clone()),
            Query::ByKind(kind) => InstrumentQuery::ByKind(kind_from(*kind)?),
        },
    )
}

/// Trading parameters to the wire.
pub fn params_to(p: &TradingParams) -> pb::TradingParams {
    pb::TradingParams {
        instrument: Some(instrument_to(&p.instrument)),
        lot_size: p.lot_size.to_string(),
        min_qty: p.min_qty.to_string(),
        price_step: p.price_step.to_string(),
        price_step_value: p.price_step_value.to_string(),
        currency: p.currency.clone(),
    }
}

/// Trading parameters from the wire.
pub fn params_from(p: &pb::TradingParams) -> R<TradingParams> {
    Ok(TradingParams {
        instrument: instrument_from(&p.instrument)?,
        lot_size: number(&p.lot_size, "lot_size")?,
        min_qty: number(&p.min_qty, "min_qty")?,
        price_step: number(&p.price_step, "price_step")?,
        price_step_value: number(&p.price_step_value, "price_step_value")?,
        currency: p.currency.clone(),
    })
}

/// A trading status to the wire.
pub fn status_to(s: &TradingStatus) -> pb::TradingStatus {
    let availability = match s.availability {
        TradingAvailability::Open => pb::TradingAvailability::Open,
        TradingAvailability::Closed => pb::TradingAvailability::Closed,
        TradingAvailability::Break => pb::TradingAvailability::Break,
        TradingAvailability::Unknown => pb::TradingAvailability::Unknown,
    };
    pb::TradingStatus {
        instrument: Some(instrument_to(&s.instrument)),
        availability: availability as i32,
        reason: s.reason.clone(),
        ts_ns: s.ts.map(UnixNanos::as_nanos),
    }
}

/// A trading status from the wire.
pub fn status_from(s: &pb::TradingStatus) -> R<TradingStatus> {
    let availability = match pb::TradingAvailability::try_from(s.availability) {
        Ok(pb::TradingAvailability::Open) => TradingAvailability::Open,
        Ok(pb::TradingAvailability::Closed) => TradingAvailability::Closed,
        Ok(pb::TradingAvailability::Break) => TradingAvailability::Break,
        Ok(pb::TradingAvailability::Unknown) => TradingAvailability::Unknown,
        _ => return bad("availability is not set"),
    };
    Ok(TradingStatus {
        instrument: instrument_from(&s.instrument)?,
        availability,
        reason: s.reason.clone(),
        ts: s.ts_ns.map(UnixNanos::from_nanos),
    })
}

/// A schedule to the wire.
pub fn schedule_to(s: &Schedule) -> pb::Schedule {
    pb::Schedule {
        exchange: s.exchange.clone(),
        sessions: s
            .sessions
            .iter()
            .map(|i| pb::SessionInterval {
                start_ns: i.start.as_nanos(),
                end_ns: i.end.as_nanos(),
                kind: match i.kind {
                    SessionKind::PreMarket => pb::SessionKind::PreMarket,
                    SessionKind::Main => pb::SessionKind::Main,
                    SessionKind::PostMarket => pb::SessionKind::PostMarket,
                    SessionKind::Auction => pb::SessionKind::Auction,
                    SessionKind::Other => pb::SessionKind::Other,
                } as i32,
            })
            .collect(),
        ts: Some(ts_to(&s.ts)),
    }
}

/// A schedule from the wire.
pub fn schedule_from(s: &pb::Schedule) -> R<Schedule> {
    let sessions = list(&s.sessions, |i| {
        let kind = match pb::SessionKind::try_from(i.kind) {
            Ok(pb::SessionKind::PreMarket) => SessionKind::PreMarket,
            Ok(pb::SessionKind::Main) => SessionKind::Main,
            Ok(pb::SessionKind::PostMarket) => SessionKind::PostMarket,
            Ok(pb::SessionKind::Auction) => SessionKind::Auction,
            Ok(pb::SessionKind::Other) => SessionKind::Other,
            _ => return bad("session kind is not set"),
        };
        Ok(SessionInterval {
            start: UnixNanos::from_nanos(i.start_ns),
            end: UnixNanos::from_nanos(i.end_ns),
            kind,
        })
    })?;
    Ok(Schedule {
        exchange: s.exchange.clone(),
        sessions,
        ts: ts_from(&s.ts)?,
    })
}

// ---------------------------------------------------------------------------------------------------- capabilities

/// Market data capabilities to the wire.
pub fn md_caps_to(c: &MarketDataCaps) -> pb::MarketDataCaps {
    pb::MarketDataCaps {
        max_order_book_depth: c.max_order_book_depth,
        incremental_order_book: c.incremental_order_book,
        streaming: Some(pb::StreamingCaps {
            order_book: c.streaming.order_book,
            market_trades: c.streaming.market_trades,
            quotes: c.streaming.quotes,
            candles: c.streaming.candles,
        }),
    }
}

/// Market data capabilities from the wire.
pub fn md_caps_from(c: &Option<pb::MarketDataCaps>) -> R<MarketDataCaps> {
    let c = need(c.as_ref(), "md caps")?;
    let s = need(c.streaming.as_ref(), "md caps streaming")?;
    Ok(MarketDataCaps::new(
        c.max_order_book_depth,
        c.incremental_order_book,
        StreamingCaps::new(s.order_book, s.market_trades, s.quotes, s.candles),
    ))
}

/// Account capabilities to the wire.
pub fn ud_caps_to(c: &UserDataCaps) -> pb::UserDataCaps {
    use pb::place_idempotency::Kind;
    let (kind, window_secs) = match c.place_idempotency {
        PlaceIdempotency::LabelOnly => (Kind::LabelOnly, 0),
        PlaceIdempotency::RejectDuplicate { window } => (Kind::RejectDuplicate, window.as_secs()),
        PlaceIdempotency::Idempotent { window } => (Kind::Idempotent, window.as_secs()),
    };
    pb::UserDataCaps {
        place_idempotency: Some(pb::PlaceIdempotency {
            kind: kind as i32,
            window_secs,
        }),
        ws_rpc: c.ws_rpc,
        ordered_command_channel: c.ordered_command_channel,
        supports_replace: c.supports_replace,
        supports_stop_orders: c.supports_stop_orders,
        streams_executions: c.streams_executions,
        sandbox: c.sandbox,
    }
}

/// Account capabilities from the wire.
pub fn ud_caps_from(c: &Option<pb::UserDataCaps>) -> R<UserDataCaps> {
    use pb::place_idempotency::Kind;
    let c = need(c.as_ref(), "ud caps")?;
    let p = need(c.place_idempotency.as_ref(), "ud caps place_idempotency")?;
    let window = std::time::Duration::from_secs(p.window_secs);
    let place_idempotency = match Kind::try_from(p.kind) {
        Ok(Kind::LabelOnly) => PlaceIdempotency::LabelOnly,
        Ok(Kind::RejectDuplicate) => PlaceIdempotency::RejectDuplicate { window },
        Ok(Kind::Idempotent) => PlaceIdempotency::Idempotent { window },
        _ => return bad("place_idempotency.kind is not set"),
    };
    Ok(UserDataCaps::new(
        place_idempotency,
        c.ws_rpc,
        c.ordered_command_channel,
        c.supports_replace,
        c.supports_stop_orders,
        c.streams_executions,
        c.sandbox,
    ))
}

// ---------------------------------------------------------------------------------------------------- events

/// An envelope as the event number `seq` of the gateway. `None` for an event of a kind this version does not know.
pub fn event_to(seq: u64, env: &Envelope) -> Option<pb::Event> {
    use pb::event::Payload as P;
    let payload = match &env.payload {
        Event::OrderBook(b) => P::OrderBook(book_to(b)),
        Event::MarketTrade(t) => P::MarketTrade(trade_to(t)),
        Event::Quote(q) => P::Quote(quote_to(q)),
        Event::Candle(c) => P::Candle(candle_to(c)),
        Event::Order(o) => P::Order(order_to(o)),
        Event::Execution(e) => P::Execution(execution_to(e)),
        Event::Portfolio(p) => P::Portfolio(portfolio_to(p)),
        Event::Positions(p) => P::Positions(pb::PositionList {
            positions: positions_to(p),
        }),
        Event::Balances(b) => P::Balances(pb::BalanceList {
            balances: balances_to(b),
        }),
        Event::Resync {
            sub,
            instrument,
            reason,
        } => P::Resync(pb::Resync {
            sub: sub.value(),
            instrument: Some(instrument_to(instrument)),
            reason: match reason {
                ResyncReason::SequenceGap => pb::ResyncReason::SequenceGap,
                ResyncReason::SubscriptionReset => pb::ResyncReason::SubscriptionReset,
                ResyncReason::Manual => pb::ResyncReason::Manual,
                _ => pb::ResyncReason::Unspecified,
            } as i32,
        }),
        Event::Overflow { sub, lost_from } => P::Overflow(pb::Overflow {
            sub: sub.value(),
            lost_from_ns: lost_from.as_nanos(),
        }),
        Event::Reconnected { sub, gap_from } => P::Reconnected(pb::Reconnected {
            sub: sub.value(),
            gap_from_ns: gap_from.as_nanos(),
        }),
        _ => return None,
    };
    Some(pb::Event {
        seq,
        source: env.source.to_string(),
        sub: env.sub.map(SubscriptionId::value),
        ts: Some(ts_to(&env.ts)),
        payload: Some(payload),
    })
}

/// The order id an event is about, if it is about an order of a bot: for the filter "my orders only".
pub fn event_cid(e: &pb::Event) -> Option<ClientOrderId> {
    use pb::event::Payload as P;
    match e.payload.as_ref()? {
        P::Order(o) => ClientOrderId::parse_str(&o.cid).ok(),
        P::Execution(x) => x
            .cid
            .as_deref()
            .and_then(|c| ClientOrderId::parse_str(c).ok()),
        _ => None,
    }
}

/// Whether an event is one of the kinds that belong to an order (the filter applies to these only).
pub fn event_is_order_bound(e: &pb::Event) -> bool {
    use pb::event::Payload as P;
    matches!(e.payload, Some(P::Order(_)) | Some(P::Execution(_)))
}

/// An event from the wire, as the envelope the connector made.
pub fn event_from(e: &pb::Event) -> R<Envelope> {
    use pb::event::Payload as P;
    let payload = match need(e.payload.as_ref(), "event payload")? {
        P::OrderBook(b) => Event::OrderBook(book_from(b)?),
        P::MarketTrade(t) => Event::MarketTrade(trade_from(t)?),
        P::Quote(q) => Event::Quote(quote_from(q)?),
        P::Candle(c) => Event::Candle(candle_from(c)?),
        P::Order(o) => Event::Order(order_from(o)?),
        P::Execution(x) => Event::Execution(execution_from(x)?),
        P::Portfolio(p) => Event::Portfolio(portfolio_from(p)?),
        P::Positions(p) => Event::Positions(positions_from(&p.positions)?),
        P::Balances(b) => Event::Balances(balances_from(&b.balances)?),
        P::Resync(r) => Event::Resync {
            sub: SubscriptionId::new(r.sub),
            instrument: instrument_from(&r.instrument)?,
            reason: match pb::ResyncReason::try_from(r.reason) {
                Ok(pb::ResyncReason::SequenceGap) => ResyncReason::SequenceGap,
                Ok(pb::ResyncReason::SubscriptionReset) => ResyncReason::SubscriptionReset,
                Ok(pb::ResyncReason::Manual) => ResyncReason::Manual,
                _ => return bad("resync reason is not set"),
            },
        },
        P::Overflow(o) => Event::Overflow {
            sub: SubscriptionId::new(o.sub),
            lost_from: UnixNanos::from_nanos(o.lost_from_ns),
        },
        P::Reconnected(r) => Event::Reconnected {
            sub: SubscriptionId::new(r.sub),
            gap_from: UnixNanos::from_nanos(r.gap_from_ns),
        },
    };
    let env = Envelope::new(Arc::from(e.source.as_str()), ts_from(&e.ts)?, payload);
    Ok(env.with_sub_opt(e.sub.map(SubscriptionId::new)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn id() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        )
    }

    fn ts() -> Timestamps {
        Timestamps::new(
            UnixNanos::from_nanos(1_792_368_000_123_456_789),
            brokapi::runtime::now(),
        )
        .with_broker_ts(UnixNanos::from_nanos(1_792_368_000_000_000_001))
    }

    fn price(text: &str) -> Price {
        text.parse().unwrap()
    }

    fn snapshot(state: OrderState) -> OrderSnapshot {
        OrderSnapshot::new(
            state.cid(),
            id(),
            state,
            Side::Sell,
            OrderType::Limit,
            "25".parse().unwrap(),
            ts(),
        )
        .with_broker_id(Some(BrokerOrderId::new("B-1")))
        .with_filled("5".parse().unwrap())
        .with_price(Some(price("101.250")))
    }

    #[test]
    fn numbers_keep_every_digit_they_were_given() {
        // "101.250" and "101.25" are the same number but not the same text: the text a bot sent is the text it gets back.
        for text in ["0", "0.1", "101.250", "-0.5", "123456789012345.678901234"] {
            let p = price(text);
            let back: Price = number(&p.to_string(), "price").unwrap();
            assert_eq!(back.to_string(), text);
            assert_eq!(back, p);
        }
        assert!(number::<Price>("", "price").is_err());
        assert!(number::<Price>("0,5", "price").is_err());
    }

    #[test]
    fn every_state_of_an_order_comes_back() {
        let cid = ClientOrderId::new_v4();
        let now = brokapi::runtime::now();
        let states = [
            OrderState::InFlight { cid, sent_at: now },
            OrderState::Unknown { cid, since: now },
            OrderState::Acked {
                cid,
                broker_id: BrokerOrderId::new("B-1"),
            },
            OrderState::PartiallyFilled {
                cid,
                filled: "3".parse().unwrap(),
                left: "7.5".parse().unwrap(),
            },
            OrderState::Filled { cid },
            OrderState::Cancelled { cid },
            OrderState::Rejected {
                cid,
                reason: "no money".into(),
            },
        ];
        for state in states {
            let order = snapshot(state);
            let mut back = order_from(&order_to(&order)).unwrap();
            // The moment a state began is the clock of the process that made it and does not travel: the receiver stamps its own.
            assert_eq!(back.state.name(), order.state.name());
            assert_eq!(back.state.cid(), order.state.cid());
            if matches!(
                order.state,
                OrderState::InFlight { .. } | OrderState::Unknown { .. }
            ) {
                back.state = order.state.clone();
            }
            assert_eq!(back, order, "{:?}", order.state);
        }
    }

    #[test]
    fn requests_come_back() {
        let place = PlaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            instrument: id(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: "10".parse().unwrap(),
            price: Some(price("1.0")),
        };
        assert_eq!(place_from(&place_to(&place)).unwrap(), place);
        let market = PlaceOrderRequest {
            order_type: OrderType::Market,
            price: None,
            ..place.clone()
        };
        assert_eq!(place_from(&place_to(&market)).unwrap(), market);
        let replace = ReplaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            original_cid: place.cid,
            qty: None,
            price: Some(price("2.5")),
        };
        assert_eq!(replace_from(&replace_to(&replace)).unwrap(), replace);
        for expiry in [
            StopExpiry::GoodTillCancel,
            StopExpiry::EndOfDay,
            StopExpiry::GoodTillDate(UnixNanos::from_nanos(1_800_000_000_000_000_000)),
        ] {
            let stop = PlaceStopOrderRequest {
                cid: ClientOrderId::new_v4(),
                instrument: id(),
                side: Side::Sell,
                kind: StopOrderKind::StopLimit,
                qty: "10".parse().unwrap(),
                stop_price: price("90"),
                limit_price: Some(price("89.5")),
                expiry,
            };
            assert_eq!(place_stop_from(&place_stop_to(&stop)).unwrap(), stop);
        }
    }

    #[test]
    fn a_request_that_does_not_follow_the_proto_is_named() {
        let mut bad = place_to(&PlaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            instrument: id(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: "10".parse().unwrap(),
            price: None,
        });
        bad.cid = "not-a-uuid".into();
        assert!(place_from(&bad).unwrap_err().0.contains("UUID"));
        bad.cid = ClientOrderId::new_v4().to_string();
        bad.qty = "ten".into();
        assert!(place_from(&bad).unwrap_err().0.contains("qty"));
        bad.qty = "10".into();
        bad.side = 0;
        assert!(place_from(&bad).unwrap_err().0.contains("side"));
        bad.side = 1;
        bad.instrument = None;
        assert!(place_from(&bad).unwrap_err().0.contains("instrument"));
    }

    #[test]
    fn instruments_ids_of_every_kind_come_back() {
        let natives = [
            NativeId::SymbolMic {
                symbol: "SBER".into(),
                mic: "MISX".into(),
            },
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
            NativeId::Figi("BBG004730N88".into()),
            NativeId::Uid("e6123145-9665-43e0-8413-cd61b8aa9b13".into()),
        ];
        for native in natives {
            let id = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), native);
            assert_eq!(instrument_from(&Some(instrument_to(&id))).unwrap(), id);
        }
    }

    #[test]
    fn market_values_come_back() {
        let level = |p: &str, q: &str| BookLevel {
            price: price(p),
            qty: q.parse().unwrap(),
        };
        let snapshot = OrderBook::Snapshot {
            instrument: id(),
            bids: vec![level("100.5", "10"), level("100.4", "20")],
            asks: vec![level("100.6", "5")],
            ts: ts(),
        };
        assert_eq!(book_from(&book_to(&snapshot)).unwrap(), snapshot);
        let increment = OrderBook::Increment {
            instrument: id(),
            seq: 42,
            bids: vec![],
            asks: vec![level("100.7", "0")],
            ts: ts(),
        };
        assert_eq!(book_from(&book_to(&increment)).unwrap(), increment);
        let trade = MarketTrade {
            instrument: id(),
            price: price("100.55"),
            qty: "3".parse().unwrap(),
            side: Side::Buy,
            trade_id: Some("T-1".into()),
            ts: ts(),
        };
        assert_eq!(trade_from(&trade_to(&trade)).unwrap(), trade);
        let candle = Candle {
            instrument: id(),
            timeframe: Timeframe::MN,
            open: price("1"),
            high: price("3"),
            low: price("0.5"),
            close: price("2"),
            volume: "1000".parse().unwrap(),
            open_ts: UnixNanos::from_nanos(10),
            close_ts: UnixNanos::from_nanos(20),
            ts: ts(),
        };
        assert_eq!(candle_from(&candle_to(&candle)).unwrap(), candle);
        for tf in [
            Timeframe::M1,
            Timeframe::M5,
            Timeframe::M10,
            Timeframe::M15,
            Timeframe::M30,
            Timeframe::H1,
            Timeframe::H4,
            Timeframe::D1,
            Timeframe::W1,
            Timeframe::MN,
        ] {
            assert_eq!(timeframe_from(timeframe_to(tf)).unwrap(), tf);
        }
    }

    #[test]
    fn the_account_comes_back() {
        let balance = Balance {
            currency: "RUB".into(),
            available: "1000.50".parse().unwrap(),
            blocked: "10".parse().unwrap(),
            ts: ts(),
        };
        let position = Position {
            instrument: id(),
            qty: "30".parse().unwrap(),
            avg_price: Some(price("99.9")),
            current_price: None,
            ts: ts(),
        };
        let portfolio = Portfolio {
            balances: vec![balance],
            positions: vec![position],
            ts: ts(),
        };
        assert_eq!(
            portfolio_from(&portfolio_to(&portfolio)).unwrap(),
            portfolio
        );
        let execution = Execution {
            cid: None,
            broker_order_id: BrokerOrderId::new("B-9"),
            instrument: id(),
            side: Side::Sell,
            price: price("101"),
            qty: "2".parse().unwrap(),
            commission: "0.05".parse().unwrap(),
            trade_id: "TR-1".into(),
            ts: ts(),
        };
        assert_eq!(
            execution_from(&execution_to(&execution)).unwrap(),
            execution
        );
        let with_cid = Execution {
            cid: Some(ClientOrderId::new_v4()),
            ..execution
        };
        assert_eq!(execution_from(&execution_to(&with_cid)).unwrap(), with_cid);
    }

    #[test]
    fn instruments_and_their_rules_come_back() {
        let instrument = Instrument {
            id: id(),
            name: "Sberbank".into(),
            isin: Some("RU0009029540".into()),
            kind: InstrumentKind::Stock,
            currency: "RUB".into(),
            exchange: "MOEX".into(),
        };
        assert_eq!(
            instrument_info_from(&instrument_info_to(&instrument)).unwrap(),
            instrument
        );
        let params = TradingParams {
            instrument: id(),
            lot_size: "10".parse().unwrap(),
            min_qty: "1".parse().unwrap(),
            price_step: price("0.01"),
            price_step_value: "0.01".parse().unwrap(),
            currency: "RUB".into(),
        };
        assert_eq!(params_from(&params_to(&params)).unwrap(), params);
        for availability in [
            TradingAvailability::Open,
            TradingAvailability::Closed,
            TradingAvailability::Break,
            TradingAvailability::Unknown,
        ] {
            let status = TradingStatus {
                instrument: id(),
                availability,
                reason: Some("auction".into()),
                ts: Some(UnixNanos::from_nanos(5)),
            };
            assert_eq!(status_from(&status_to(&status)).unwrap(), status);
        }
        let schedule = Schedule {
            exchange: "MOEX".into(),
            sessions: vec![SessionInterval {
                start: UnixNanos::from_nanos(1),
                end: UnixNanos::from_nanos(2),
                kind: SessionKind::Auction,
            }],
            ts: ts(),
        };
        assert_eq!(schedule_from(&schedule_to(&schedule)).unwrap(), schedule);
        for query in [
            InstrumentQuery::ByTicker {
                ticker: "SBER".into(),
                exchange: Some("TQBR".into()),
            },
            InstrumentQuery::ByTicker {
                ticker: "SBER".into(),
                exchange: None,
            },
            InstrumentQuery::ByIsin("RU0009029540".into()),
            InstrumentQuery::ByKind(InstrumentKind::Bond),
        ] {
            assert_eq!(query_from(&Some(query_to(&query))).unwrap(), query);
        }
    }

    #[test]
    fn capabilities_come_back() {
        let md = MarketDataCaps::new(50, true, StreamingCaps::new(true, false, true, false));
        assert_eq!(md_caps_from(&Some(md_caps_to(&md))).unwrap(), md);
        for place_idempotency in [
            PlaceIdempotency::LabelOnly,
            PlaceIdempotency::RejectDuplicate {
                window: Duration::from_secs(300),
            },
            PlaceIdempotency::Idempotent {
                window: Duration::from_secs(60),
            },
        ] {
            let ud = UserDataCaps::new(place_idempotency, true, false, true, false, true, false);
            assert_eq!(ud_caps_from(&Some(ud_caps_to(&ud))).unwrap(), ud);
        }
    }

    #[test]
    fn every_event_comes_back_with_its_number() {
        let cid = ClientOrderId::new_v4();
        let sub = SubscriptionId::new(7);
        let payloads = vec![
            Event::OrderBook(OrderBook::Snapshot {
                instrument: id(),
                bids: vec![],
                asks: vec![],
                ts: ts(),
            }),
            Event::Order(snapshot(OrderState::Filled { cid })),
            Event::Positions(vec![]),
            Event::Balances(vec![]),
            Event::Resync {
                sub,
                instrument: id(),
                reason: ResyncReason::SequenceGap,
            },
            Event::Overflow {
                sub,
                lost_from: UnixNanos::from_nanos(9),
            },
            Event::Reconnected {
                sub,
                gap_from: UnixNanos::from_nanos(10),
            },
        ];
        for (n, payload) in payloads.into_iter().enumerate() {
            let env = Envelope::new(Arc::from("bcs-ud"), ts(), payload).with_sub(sub);
            let wire = event_to(n as u64 + 1, &env).expect("a known kind");
            assert_eq!(wire.seq, n as u64 + 1);
            assert_eq!(event_from(&wire).unwrap(), env);
        }
        // An event without a subscription stays without.
        let plain = Envelope::new(Arc::from("bcs-md"), ts(), Event::Balances(vec![]));
        assert_eq!(event_from(&event_to(1, &plain).unwrap()).unwrap().sub, None);
    }

    #[test]
    fn the_orders_of_an_event_are_found_for_the_filter() {
        let cid = ClientOrderId::new_v4();
        let env = Envelope::new(
            Arc::from("bcs-ud"),
            ts(),
            Event::Order(snapshot(OrderState::Filled { cid })),
        );
        let wire = event_to(1, &env).unwrap();
        assert!(event_is_order_bound(&wire));
        assert_eq!(event_cid(&wire), Some(cid));
        let other = event_to(
            2,
            &Envelope::new(Arc::from("x"), ts(), Event::Balances(vec![])),
        )
        .unwrap();
        assert!(!event_is_order_bound(&other));
        assert_eq!(event_cid(&other), None);
    }
}
