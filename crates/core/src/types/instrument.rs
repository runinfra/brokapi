//! Instrument identifiers ([`InstrumentId`], [`InstrumentKey`], [`NativeId`]), the directory
//! ([`Instrument`], [`InstrumentQuery`]), trading parameters, status and schedule.

use serde::{Deserialize, Serialize};

use super::number::{Money, NumberError, Price, Qty, RoundMode};
use super::time::{Timestamps, UnixNanos};
use crate::error::Error;

/// The library's own name of an instrument, the same for every broker: a symbol and a market.
///
/// Printed as `SYMBOL@MIC` by [`display`](Self::display). Use it to compare, log and index
/// instruments. It is NOT what you send to a broker: that is [`NativeId`]. The `mic` is the market
/// code as the connector sets it. The BCS examples in this documentation put the board code
/// (`TQBR`) there. Not verified that every connector uses an ISO 10383 MIC.
///
/// # Examples
///
/// ```
/// use brokapi_core::InstrumentKey;
///
/// let key = InstrumentKey::new("SBER", "TQBR");
/// assert_eq!(key.display(), "SBER@TQBR");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstrumentKey {
    /// Ticker of the instrument, for example `SBER`.
    pub symbol: String,
    /// Market (or board) code, for example `TQBR`.
    pub mic: String,
}

impl InstrumentKey {
    /// A key from a symbol and a market code.
    pub fn new(symbol: impl Into<String>, mic: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            mic: mic.into(),
        }
    }

    /// The key as text, `SYMBOL@MIC`.
    pub fn display(&self) -> String {
        format!("{}@{}", self.symbol, self.mic)
    }
}

/// How a broker names an instrument in its own API. The kind depends on the broker.
///
/// A connector reads only its own kind. An [`InstrumentId`] with another kind is refused with
/// [`Error::Config`](crate::Error::Config) (the BCS connector wants
/// [`TickerClass`](Self::TickerClass)). Turning a key into a native id and back needs no network
/// call.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NativeId {
    /// Finam style: symbol and market code.
    SymbolMic {
        /// Ticker, for example `SBER`.
        symbol: String,
        /// Market code, for example `MISX`.
        mic: String,
    },
    /// BCS style: ticker and class code (the board).
    TickerClass {
        /// Ticker, for example `SBER`.
        ticker: String,
        /// Class code of the board, for example `TQBR`.
        class_code: String,
    },
    /// T-Bank style: the FIGI of the instrument.
    Figi(String),
    /// T-Bank style: the broker's `uid` of the instrument.
    Uid(String),
}

/// An instrument as both the library and the broker name it: the same key for every broker, and the
/// form the broker wants.
///
/// Every connector method that takes an instrument takes an `InstrumentId`. The `key` is for you,
/// the `native` is for the connector. Get one from [`InstrumentSource::find`](crate::InstrumentSource::find),
/// or build it by hand when you know the broker's naming.
///
/// # Examples
///
/// ```
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId};
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// assert_eq!(sber.key.display(), "SBER@TQBR");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstrumentId {
    /// The library's name, the same for every broker.
    pub key: InstrumentKey,
    /// The broker's name, what the connector sends.
    pub native: NativeId,
}

impl InstrumentId {
    /// An id from a key and the broker's native form.
    pub fn new(key: InstrumentKey, native: NativeId) -> Self {
        Self { key, native }
    }
}

/// The class of an instrument, for directory search and as a field of [`Instrument`].
///
/// A connector may map only some kinds in a search. The BCS connector answers
/// [`Error::Unsupported`](crate::Error::Unsupported) to a search by `Option` or `Other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstrumentKind {
    /// A share.
    Stock,
    /// A bond.
    Bond,
    /// An option.
    Option,
    /// A futures contract.
    Future,
    /// An exchange traded fund.
    Etf,
    /// A mutual fund.
    MutualFund,
    /// A currency.
    Currency,
    /// Anything the connector could not classify.
    Other,
}

/// A directory lookup for [`InstrumentSource::find`](crate::InstrumentSource::find).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstrumentQuery {
    /// By ticker, optionally narrowed to one exchange. The BCS connector ignores `exchange`.
    ByTicker {
        /// Ticker to look for.
        ticker: String,
        /// Exchange to narrow to, if the broker supports it.
        exchange: Option<String>,
    },
    /// By ISIN.
    ByIsin(String),
    /// All instruments of one kind. Can be a long list, a connector may page through it.
    ByKind(InstrumentKind),
}

/// A directory entry: what an instrument is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instrument {
    /// The instrument, ready to pass to other methods.
    pub id: InstrumentId,
    /// Display name.
    pub name: String,
    /// ISIN, when the broker gives it.
    pub isin: Option<String>,
    /// Class of the instrument.
    pub kind: InstrumentKind,
    /// Currency of trading, as the broker writes it (for example `RUB`).
    pub currency: String,
    /// Exchange or board, as the broker writes it.
    pub exchange: String,
}

/// The rules for sizing and pricing an order of one instrument.
///
/// The library does NOT check the quantity against these. Round your quantity and price with them
/// before [`OrderExecutor::place`](crate::OrderExecutor::place): [`TradingParams::round_price`] and
/// [`TradingParams::round_qty`] do it exactly. A connector that knows the price step refuses an order
/// price off the grid before sending anything, see [`TradingParams::check_price`].
///
/// # Examples
///
/// ```
/// use brokapi_core::{Decimal, InstrumentId, InstrumentKey, NativeId, Price, RoundMode, TradingParams, dec};
///
/// let params = TradingParams {
///     instrument: InstrumentId::new(
///         InstrumentKey::new("SBER", "TQBR"),
///         NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
///     ),
///     lot_size: dec!(10).into(),
///     min_qty: dec!(1).into(),
///     price_step: dec!(0.01).into(),
///     price_step_value: dec!(0.01).into(),
///     currency: "RUB".into(),
/// };
/// // 244.307 is not on the grid, 244.30 is.
/// let price = Price::new(dec!(244.307));
/// assert!(!params.is_price_on_grid(price));
/// assert_eq!(params.round_price(price, RoundMode::Down)?, Price::new(dec!(244.30)));
/// assert!(params.check_price(Price::new(dec!(244.30))).is_ok());
/// assert!(params.check_price(price).is_err());
/// # Ok::<(), brokapi_core::NumberError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradingParams {
    /// The instrument the numbers are for.
    pub instrument: InstrumentId,
    /// Units of the instrument in one lot.
    pub lot_size: Qty,
    /// Smallest order quantity. The BCS connector sets it equal to `lot_size`.
    pub min_qty: Qty,
    /// Smallest price move. A price must be a multiple of it.
    pub price_step: Price,
    /// What one price step is worth in money, as the broker reports it.
    pub price_step_value: Money,
    /// Currency of the prices, as the broker writes it (for example `RUB`).
    pub currency: String,
}

impl TradingParams {
    /// Rounds a price to the grid of [`price_step`](Self::price_step), exactly.
    ///
    /// # Errors
    ///
    /// [`NumberError::StepNotPositive`] if the broker gave no usable step.
    pub fn round_price(&self, price: Price, mode: RoundMode) -> Result<Price, NumberError> {
        price.round_to_step(self.price_step, mode)
    }

    /// Rounds a quantity to a multiple of [`min_qty`](Self::min_qty), exactly.
    ///
    /// # Errors
    ///
    /// [`NumberError::StepNotPositive`] if the broker gave no usable minimal quantity.
    pub fn round_qty(&self, qty: Qty, mode: RoundMode) -> Result<Qty, NumberError> {
        qty.round_to_step(self.min_qty, mode)
    }

    /// Whether the price is a whole number of price steps. `true` when the step is not usable
    /// (zero or below), because then nothing can be said against the price.
    pub fn is_price_on_grid(&self, price: Price) -> bool {
        !self.price_step.is_positive() || price.is_on_step(self.price_step)
    }

    /// Checks a price against the grid, with the error a connector gives for an order.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`] with the code `price_off_step`.
    pub fn check_price(&self, price: Price) -> Result<(), Error> {
        check_price_step(price, self.price_step)
    }
}

/// Refuses a price that is not a whole number of `step`: the check a connector runs before sending an
/// order when it knows the step. A step that is zero or below means "not known" and passes.
///
/// # Errors
///
/// [`Error::Rejected`] with the code `price_off_step`.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Decimal, Error, Price, check_price_step, dec};
///
/// let step = Price::new(dec!(0.05));
/// assert!(check_price_step(Price::new(dec!(101.05)), step).is_ok());
/// let e = check_price_step(Price::new(dec!(101.03)), step).unwrap_err();
/// assert!(matches!(e, Error::Rejected { ref code, .. } if code == "price_off_step"));
/// ```
pub fn check_price_step(price: Price, step: Price) -> Result<(), Error> {
    if !step.is_positive() || price.is_on_step(step) {
        return Ok(());
    }
    Err(Error::Rejected {
        code: "price_off_step".into(),
        message: format!("price {price} is not a multiple of the price step {step}"),
    })
}

/// Whether an instrument can be traded now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradingAvailability {
    /// Trading is going on.
    Open,
    /// The market is closed.
    Closed,
    /// A pause inside the trading day.
    Break,
    /// The broker's status is not one of the above. The BCS connector gives this for any status other
    /// than open or close.
    Unknown,
}

/// The trading status of one instrument now: see [`InstrumentSource::trading_status`](crate::InstrumentSource::trading_status).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradingStatus {
    /// The instrument.
    pub instrument: InstrumentId,
    /// Open, closed, break or unknown.
    pub availability: TradingAvailability,
    /// The broker's explanation or session name, for a human. Not a stable code.
    pub reason: Option<String>,
    /// Broker time of the status, when the broker gives it (the BCS connector gives none).
    pub ts: Option<UnixNanos>,
}

/// One trading session of a day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInterval {
    /// Start of the session.
    pub start: UnixNanos,
    /// End of the session.
    pub end: UnixNanos,
    /// Which part of the day it is.
    pub kind: SessionKind,
}

/// The part of the trading day a [`SessionInterval`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionKind {
    /// Before the main session.
    PreMarket,
    /// The main session.
    Main,
    /// After the main session (evening, for example).
    PostMarket,
    /// An opening or closing auction.
    Auction,
    /// Anything else, or a name the connector could not classify.
    Other,
}

/// The trading sessions of an exchange (or, for BCS, of an instrument) over a time range: see
/// [`InstrumentSource::schedule`](crate::InstrumentSource::schedule).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schedule {
    /// The key the schedule was asked for, as the connector understands it.
    pub exchange: String,
    /// Trading sessions of the range. The BCS connector gives none on a non-working day.
    pub sessions: Vec<SessionInterval>,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}
