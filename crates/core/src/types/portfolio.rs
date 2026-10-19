//! Portfolio types: money balances, positions and the whole portfolio.

use serde::{Deserialize, Serialize};

use super::instrument::InstrumentId;
use super::number::{Money, Price, Qty};
use super::time::Timestamps;

/// Money of one currency on the account.
///
/// Amounts are in the currency itself (not in lots, not in kopecks), as the broker reports them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Balance {
    /// Currency, as the broker writes it (for example `RUB`).
    pub currency: String,
    /// Money free to use.
    pub available: Money,
    /// Money held back by the broker (for example for working orders).
    pub blocked: Money,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}

/// How much of one instrument the account holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Position {
    /// The instrument held.
    pub instrument: InstrumentId,
    /// Quantity held, see [`Qty`] for the unit. Negative for a short position where the broker has it.
    pub qty: Qty,
    /// Average price at which the position was built, when the broker gives it.
    pub avg_price: Option<Price>,
    /// Last known price, when the broker gives it.
    pub current_price: Option<Price>,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}

/// The account in one piece: money and positions at the same moment.
///
/// From [`PortfolioSource::portfolio`](crate::PortfolioSource::portfolio), and as
/// [`Event::Portfolio`](crate::Event::Portfolio) from a portfolio subscription. A snapshot replaces
/// the previous one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Portfolio {
    /// Money by currency.
    pub balances: Vec<Balance>,
    /// Instruments held.
    pub positions: Vec<Position>,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}
