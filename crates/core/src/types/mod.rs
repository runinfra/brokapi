//! Domain types: everything the traits take and return, flat re-exported at the crate root.
//!
//! Groups: identifiers and reference data ([`InstrumentId`], [`Instrument`]), market data
//! ([`OrderBook`], [`Candle`]), orders ([`PlaceOrderRequest`], [`OrderSnapshot`]), stop orders
//! ([`PlaceStopOrderRequest`]), own fills ([`Execution`]), the portfolio ([`Portfolio`]), what a
//! connector can do ([`UserDataCaps`]), the event channel ([`Envelope`], [`Event`]), subscription
//! options ([`SubscribeOpts`]), shutdown ([`ShutdownMode`]), and the exact numbers [`Price`], [`Qty`], [`Money`] and
//! [`UnixNanos`]. Use them as `brokapi_core::Type`.

mod caps;
mod envelope;
mod execution;
mod instrument;
mod market;
mod number;
mod order;
mod portfolio;
mod primitives;
mod shutdown;
mod stop;
mod subscription;
mod time;

pub use caps::{MarketDataCaps, PlaceIdempotency, StreamingCaps, UserDataCaps};
pub use envelope::{ENVELOPE_SCHEMA, Envelope, Event};
pub use execution::Execution;
pub use instrument::{
    Instrument, InstrumentId, InstrumentKey, InstrumentKind, InstrumentQuery, NativeId, Schedule,
    SessionInterval, SessionKind, TradingAvailability, TradingParams, TradingStatus,
    check_price_step,
};
pub use market::{
    BookLevel, Candle, MarketTrade, OrderBook, Quote, ResyncReason, TimeRange, Timeframe,
};
pub use number::{Money, NumberError, Price, Qty, RoundMode};
pub use order::{
    BrokerOrderId, ClientOrderId, OrderAck, OrderSnapshot, OrderState, PlaceOrderRequest,
    ReconcileReport, ReplaceOrderRequest,
};
pub use portfolio::{Balance, Portfolio, Position};
pub use primitives::{OrderType, Side};
pub use shutdown::{ShutdownMode, ShutdownReport};
pub use stop::{
    PlaceStopOrderRequest, StopExpiry, StopOrderKind, StopOrderRef, StopOrderSnapshot,
    StopOrderState,
};
pub use subscription::{Backpressure, SubscribeOpts, SubscriptionId};
pub use time::{Timestamps, UnixNanos};
