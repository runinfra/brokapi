//! The key that names an instrument in WebSocket subscriptions.

use serde::{Deserialize, Serialize};

/// Names one instrument for the broker: a ticker and a class code (board).
///
/// The same ticker can trade on several boards (for example `SBER` on `TQBR`), so the broker always
/// wants both. The class code is the code of the board on the exchange, `TQBR` for most Russian
/// shares.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
///
/// let key = InstrumentKey::new("SBER", "TQBR");
/// assert_eq!(serde_json::to_string(&key)?, r#"{"ticker":"SBER","classCode":"TQBR"}"#);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentKey {
    /// Exchange ticker, for example `SBER`.
    pub ticker: String,
    /// Class code (board) of the ticker, for example `TQBR`.
    pub class_code: String,
}

impl InstrumentKey {
    /// Builds a key from a ticker and a class code.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
    ///
    /// let key = InstrumentKey::new(String::from("GAZP"), "TQBR");
    /// assert_eq!((key.ticker.as_str(), key.class_code.as_str()), ("GAZP", "TQBR"));
    /// ```
    pub fn new(ticker: impl Into<String>, class_code: impl Into<String>) -> Self {
        Self {
            ticker: ticker.into(),
            class_code: class_code.into(),
        }
    }
}
