//! The price a test sets for a ticker. The order book, the last deals and the bars of a mock follow it, and the open
//! market data streams of the ticker get a new message when it changes.
//!
//! Without a price set, each mock answers its recorded numbers, as before.
//!
//! ```
//! use brokapi_mocks::market::{Prices, levels};
//! use rust_decimal::Decimal;
//!
//! let prices = Prices::default();
//! assert_eq!(prices.get("SBER"), None);
//! prices.set("SBER", Decimal::new(10050, 2));
//! assert_eq!(prices.get("sber"), Some(Decimal::new(10050, 2)));
//! let (bids, asks) = levels(Decimal::new(10050, 2), 2);
//! assert_eq!(bids, [Decimal::new(10049, 2), Decimal::new(10048, 2)]);
//! assert_eq!(asks, [Decimal::new(10051, 2), Decimal::new(10052, 2)]);
//! ```

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use rust_decimal::Decimal;
use tokio::sync::watch;

/// The id of a new deal of a mock, never the same twice in a process. Connectors drop a deal whose id they have seen, and
/// an id made of the clock repeats within one tick of it (on Windows a tick is about 15 ms).
pub(crate) fn deal_id() -> i64 {
    static NEXT: AtomicI64 = AtomicI64::new(1_000_000_000);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// The step between two levels of a book the mocks build around a price.
pub const STEP: Decimal = Decimal::from_parts(1, 0, 0, false, 2);

/// The prices of the tickers a test set, by ticker (case does not matter).
#[derive(Debug)]
pub struct Prices {
    by_ticker: Mutex<HashMap<String, Decimal>>,
    /// Counts the changes. A stream that sees it move sends the price of each of its tickers again.
    changed: watch::Sender<u64>,
}

impl Default for Prices {
    fn default() -> Self {
        Self {
            by_ticker: Mutex::default(),
            changed: watch::channel(0).0,
        }
    }
}

impl Prices {
    /// The market of `ticker` moves to `price`.
    pub fn set(&self, ticker: &str, price: Decimal) {
        let ticker = ticker.to_ascii_uppercase();
        self.by_ticker.lock().unwrap().insert(ticker, price);
        self.changed.send_modify(|n| *n += 1);
    }

    /// The price set for `ticker`, if any.
    pub fn get(&self, ticker: &str) -> Option<Decimal> {
        self.by_ticker
            .lock()
            .unwrap()
            .get(&ticker.to_ascii_uppercase())
            .copied()
    }

    /// Changes from now on: `changed().await` returns after the next [`set`](Self::set) of any ticker.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        let mut rx = self.changed.subscribe();
        rx.mark_unchanged();
        rx
    }
}

/// Bars of the price: open, high, low and close all at `price`, at the open times `times` (seconds since the epoch).
pub fn flat_bars(times: impl IntoIterator<Item = i64>, price: Decimal) -> Vec<crate::SyntheticBar> {
    times
        .into_iter()
        .map(|t| (t, price, price, price, price, 1000))
        .collect()
}

/// `depth` bid prices from one [`STEP`] below `price` down, and `depth` ask prices from one step above up.
pub fn levels(price: Decimal, depth: usize) -> (Vec<Decimal>, Vec<Decimal>) {
    let at = |i: usize| STEP * Decimal::from(i + 1);
    (
        (0..depth).map(|i| price - at(i)).collect(),
        (0..depth).map(|i| price + at(i)).collect(),
    )
}
