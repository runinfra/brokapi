//! Small value types used by several models.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// An amount together with its settlement mode.
///
/// The settlement mode (`type`) is the term in which the trade is settled, for example `T0`, `T1` or
/// `T365`. The broker docs describe it and give no closed list.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::types::QuantityWithType;
///
/// let q: QuantityWithType = serde_json::from_str(r#"{"type":"T0","value":12.50}"#)?;
/// assert_eq!(q.settlement_type, "T0");
/// assert_eq!(q.value.to_string(), "12.50");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantityWithType {
    /// Settlement mode, such as `T0` (JSON field `type`).
    #[serde(rename = "type")]
    pub settlement_type: String,
    /// The amount, exactly as the broker wrote it.
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision::serialize")]
    pub value: Decimal,
}

/// Page number and page size for the paged REST lists.
///
/// This is a helper for your own code: the client methods take `size` and `page` as separate
/// arguments. The broker pages from 0, with `size` from 1 to 100 (default 50), so
/// [`Pagination::default`] is the first page of 50.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::types::Pagination;
///
/// let first = Pagination::default();
/// assert_eq!((first.size, first.page), (50, 0));
/// let third = Pagination::new(100, 2);
/// assert_eq!(third.page, 2);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Pagination {
    /// Records per page. The broker accepts 1 to 100.
    pub size: u32,
    /// Page number, counted from 0.
    pub page: u32,
}

impl Default for Pagination {
    fn default() -> Self {
        Self { size: 50, page: 0 }
    }
}

impl Pagination {
    /// Builds a page request. Nothing is checked: a `size` outside 1 to 100 is refused by the broker.
    pub fn new(size: u32, page: u32) -> Self {
        Self { size, page }
    }
}

/// A trading board: a class code on an exchange.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::types::Board;
///
/// let b: Board = serde_json::from_str(r#"{"classCode":"TQBR","exchange":"MOEX"}"#)?;
/// assert_eq!(b.class_code, "TQBR");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Board {
    /// Class code of the board, for example `TQBR`.
    pub class_code: String,
    /// Exchange code, for example `MOEX`.
    pub exchange: String,
}
