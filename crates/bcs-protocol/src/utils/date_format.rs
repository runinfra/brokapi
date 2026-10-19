//! The date-time text format of the BCS API, for use with `#[serde(with = "...")]`.
//!
//! The broker sends and wants UTC date-times as ISO 8601 text with milliseconds and a `Z`, for example
//! `2026-10-19T07:05:00.000Z`. All date-times of the API are UTC (broker docs). Use
//! `#[serde(with = "brokapi_bcs_protocol::utils::date_format")]` on a `DateTime<Utc>` field,
//! `date_format::option` on an `Option<DateTime<Utc>>`, and [`moex`] where the broker names the
//! "Moscow Exchange format" (it is the same text format, see there).
//!
//! # Examples
//!
//! ```
//! use brokapi_bcs_protocol::utils::date_format;
//! use chrono::{DateTime, TimeZone, Utc};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, PartialEq, Debug)]
//! struct Bar {
//!     #[serde(with = "date_format")]
//!     time: DateTime<Utc>,
//! }
//!
//! let bar = Bar { time: Utc.with_ymd_and_hms(2026, 10, 19, 7, 5, 0).unwrap() };
//! let text = serde_json::to_string(&bar)?;
//! assert_eq!(text, r#"{"time":"2026-10-19T07:05:00.000Z"}"#);
//! assert_eq!(serde_json::from_str::<Bar>(&text)?, bar);
//! # Ok::<(), serde_json::Error>(())
//! ```

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde::{self, Deserialize, Deserializer, Serializer};

/// ISO 8601 with milliseconds and a literal `Z`, the format of the API.
const FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";

/// Writes a UTC date-time as text with milliseconds and a `Z`, for example `2026-10-19T07:05:00.000Z`.
///
/// Use it through `#[serde(with = "...")]`. Time below a millisecond is cut off.
///
/// # Errors
///
/// Only the errors of the serializer. Formatting itself cannot fail.
pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = date.format(FORMAT).to_string();
    serializer.serialize_str(&s)
}

/// Reads a date-time text into a UTC `DateTime`.
///
/// It accepts any RFC 3339 text (with `Z` or with an offset, which is converted to UTC) and the plain
/// `YYYY-MM-DDTHH:MM:SS.mmmZ` text.
///
/// # Errors
///
/// A serde error if the text is not a date-time in one of those forms. An empty text is an error here
/// (the [`option`] module treats it as no value).
pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    DateTime::parse_from_rfc3339(&s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(&s, FORMAT).map(|ndt| ndt.and_utc()))
        .map_err(serde::de::Error::custom)
}

/// The same format for an optional date-time (`Option<DateTime<Utc>>`).
///
/// Reading is more forgiving than [`deserialize`]: `null`, an empty text and the epoch text
/// `1970-01-01T00:00:00.000Z` all read as `None`. The broker uses the epoch as "no time" in some
/// order fields. Writing `None` gives `null`. Use it with `#[serde(default, with = "...option")]`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::date_format;
/// use chrono::{DateTime, Utc};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Event {
///     #[serde(default, with = "date_format::option")]
///     at: Option<DateTime<Utc>>,
/// }
///
/// let set: Event = serde_json::from_str(r#"{"at":"2026-10-19T07:05:00.000Z"}"#)?;
/// assert!(set.at.is_some());
/// let epoch: Event = serde_json::from_str(r#"{"at":"1970-01-01T00:00:00.000Z"}"#)?;
/// assert!(epoch.at.is_none());
/// let missing: Event = serde_json::from_str("{}")?;
/// assert!(missing.at.is_none());
/// # Ok::<(), serde_json::Error>(())
/// ```
pub mod option {
    use super::*;

    /// Writes `Some` as in [`super::serialize`] and `None` as `null`.
    ///
    /// # Errors
    ///
    /// Only the errors of the serializer.
    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(d) => super::serialize(d, serializer),
            None => serializer.serialize_none(),
        }
    }

    /// Reads `null`, an empty text and the epoch text as `None`, and any other date-time as `Some`.
    ///
    /// # Errors
    ///
    /// A serde error if the text is not a date-time in the forms [`super::deserialize`] accepts.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        match s {
            Some(ref s) if s.is_empty() || s == "1970-01-01T00:00:00.000Z" => Ok(None),
            Some(s) => DateTime::parse_from_rfc3339(&s)
                .map(|dt| Some(dt.with_timezone(&Utc)))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(&s, FORMAT).map(|ndt| Some(ndt.and_utc()))
                })
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

/// The format the broker docs call the Moscow Exchange format: `yyyy-MM-dd'T'HH:mm:ss.SSSX`.
///
/// In the code it is the same text as the main module (UTC, milliseconds, `Z`). It is a separate module
/// because the order search request names its period fields in this format. Use it with
/// `#[serde(with = "...::moex")]`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::date_format;
/// use chrono::{DateTime, TimeZone, Utc};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Period {
///     #[serde(with = "date_format::moex")]
///     from: DateTime<Utc>,
/// }
///
/// let p = Period { from: Utc.with_ymd_and_hms(2026, 1, 26, 0, 0, 0).unwrap() };
/// assert_eq!(serde_json::to_string(&p)?, r#"{"from":"2026-01-26T00:00:00.000Z"}"#);
/// # Ok::<(), serde_json::Error>(())
/// ```
pub mod moex {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    const MOEX_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";

    /// Writes a UTC date-time as `2026-10-19T07:05:00.000Z`.
    ///
    /// # Errors
    ///
    /// Only the errors of the serializer.
    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = date.format(MOEX_FORMAT).to_string();
        serializer.serialize_str(&s)
    }

    /// Reads an RFC 3339 text or the plain `YYYY-MM-DDTHH:MM:SS.mmmZ` text into a UTC date-time.
    ///
    /// # Errors
    ///
    /// A serde error if the text is not a date-time in one of those forms.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(&s, MOEX_FORMAT).map(|ndt| ndt.and_utc())
            })
            .map_err(serde::de::Error::custom)
    }
}

/// The current time as milliseconds since the Unix epoch (UTC).
///
/// Returns 0 if the system clock is before the epoch. The WebSocket session uses it for its pong
/// watchdog.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::date_format::now_millis;
///
/// assert!(now_millis() > 1_700_000_000_000); // later than November 2023
/// ```
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
