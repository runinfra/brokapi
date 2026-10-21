//! Capability declarations: what a connector can do, stated as data.
//!
//! Differences between brokers are declared in [`MarketDataCaps`] and [`UserDataCaps`], so the
//! caller reads them instead of trying a call and failing. A method the broker cannot do answers
//! [`Error::Unsupported`](crate::Error::Unsupported) at once, without a network call. The library
//! never replaces a missing stream by polling behind your back.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Which streams a market data connector offers.
///
/// A `false` means the matching `subscribe_*` method answers
/// [`Error::Unsupported`](crate::Error::Unsupported). Poll the REST read yourself if you need the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StreamingCaps {
    /// [`OrderBookSource::subscribe_order_book`](crate::OrderBookSource::subscribe_order_book) works.
    pub order_book: bool,
    /// [`MarketDataSource::subscribe_market_trades`](crate::MarketDataSource::subscribe_market_trades) works.
    pub market_trades: bool,
    /// [`MarketDataSource::subscribe_quotes`](crate::MarketDataSource::subscribe_quotes) works.
    pub quotes: bool,
    /// [`MarketDataSource::subscribe_candles`](crate::MarketDataSource::subscribe_candles) works.
    pub candles: bool,
}

impl StreamingCaps {
    /// Caps with the four streams given: order book, market trades, quotes, candles.
    ///
    /// The struct is `#[non_exhaustive]`: it gets a new field when a stream is added, so build it with this
    /// function (or [`NONE`](Self::NONE) and set the fields) and not with a struct literal.
    pub const fn new(order_book: bool, market_trades: bool, quotes: bool, candles: bool) -> Self {
        Self {
            order_book,
            market_trades,
            quotes,
            candles,
        }
    }

    /// No streams at all.
    pub const NONE: Self = Self {
        order_book: false,
        market_trades: false,
        quotes: false,
        candles: false,
    };
}

/// What a market data connector can do. Read it from the connector's `caps()` method.
///
/// # Examples
///
/// ```
/// use brokapi_core::{MarketDataCaps, StreamingCaps};
///
/// let caps = MarketDataCaps::new(20, false, StreamingCaps::new(true, false, false, false));
/// assert!(caps.streaming.order_book && !caps.streaming.quotes);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MarketDataCaps {
    /// Deepest order book the connector serves. A larger `depth` is cut to this value (the BCS
    /// connector does so silently).
    pub max_order_book_depth: u32,
    /// `true` when a stream may send [`OrderBook::Increment`](crate::OrderBook::Increment). `false`
    /// means snapshots only.
    pub incremental_order_book: bool,
    /// Which streams exist.
    pub streaming: StreamingCaps,
}

/// What the broker does when the same place request arrives twice with the same cid.
///
/// It matters only for your own decision. The library never repeats a place, whatever the value.
/// Until a connector's behaviour is confirmed on a live account it declares `LabelOnly`, the
/// safest assumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlaceIdempotency {
    /// Cid is a correlation label only. A retry creates a second order.
    LabelOnly,
    /// Broker rejects duplicate cid within the window.
    RejectDuplicate {
        /// How long the broker remembers a cid.
        #[serde(with = "duration_secs")]
        window: Duration,
    },
    /// Broker returns the original order state for duplicate cid.
    Idempotent {
        /// How long the broker remembers a cid.
        #[serde(with = "duration_secs")]
        window: Duration,
    },
}

mod duration_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(value.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

/// What a user data connector (orders, fills, portfolio) can do. Read it from the connector's
/// `caps()` method.
///
/// # Examples
///
/// Check before you rely on an optional feature:
///
/// ```
/// use brokapi_core::UserDataCaps;
///
/// fn can_move_orders(caps: &UserDataCaps) -> bool {
///     caps.supports_replace
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UserDataCaps {
    /// What the broker does on a repeated cid, see [`PlaceIdempotency`].
    pub place_idempotency: PlaceIdempotency,
    /// Commands can go over a WebSocket with a correlation id (request and response on one socket).
    /// `false` means commands use HTTP or gRPC.
    pub ws_rpc: bool,
    /// Commands are executed by the broker in the order sent, within one channel.
    pub ordered_command_channel: bool,
    /// [`OrderExecutor::replace`](crate::OrderExecutor::replace) works. `false` means it answers
    /// [`Error::Unsupported`](crate::Error::Unsupported).
    pub supports_replace: bool,
    /// [`StopOrderExecutor`](crate::StopOrderExecutor) works. `false` means every method of it
    /// answers [`Error::Unsupported`](crate::Error::Unsupported).
    pub supports_stop_orders: bool,
    /// [`ExecutionSource::subscribe_executions`](crate::ExecutionSource::subscribe_executions) works
    /// as a stream. `false` means only the history call exists.
    pub streams_executions: bool,
    /// The connector talks to the broker's test environment (sandbox), not to real money.
    pub sandbox: bool,
}

impl MarketDataCaps {
    /// Caps of a market data connector. The struct is `#[non_exhaustive]`: build it with this function.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{MarketDataCaps, StreamingCaps};
    ///
    /// let caps = MarketDataCaps::new(20, false, StreamingCaps::new(true, true, true, true));
    /// assert_eq!(caps.max_order_book_depth, 20);
    /// ```
    pub const fn new(
        max_order_book_depth: u32,
        incremental_order_book: bool,
        streaming: StreamingCaps,
    ) -> Self {
        Self {
            max_order_book_depth,
            incremental_order_book,
            streaming,
        }
    }
}

impl UserDataCaps {
    /// Caps of a user data connector, the fields in the order they are declared. The struct is
    /// `#[non_exhaustive]`: build it with this function.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{PlaceIdempotency, UserDataCaps};
    ///
    /// let caps = UserDataCaps::new(PlaceIdempotency::LabelOnly, false, false, true, true, true, false);
    /// assert!(caps.supports_replace && !caps.sandbox);
    /// ```
    pub const fn new(
        place_idempotency: PlaceIdempotency,
        ws_rpc: bool,
        ordered_command_channel: bool,
        supports_replace: bool,
        supports_stop_orders: bool,
        streams_executions: bool,
        sandbox: bool,
    ) -> Self {
        Self {
            place_idempotency,
            ws_rpc,
            ordered_command_channel,
            supports_replace,
            supports_stop_orders,
            streams_executions,
            sandbox,
        }
    }
}
