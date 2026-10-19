//! The client order id codec: the mapping between the library's [`ClientOrderId`] and the form a broker accepts.
//!
//! You generate a [`ClientOrderId`] (a UUID v4) for every order. A broker may take a different form
//! in its own field: a shorter string, other characters. A [`CidCodec`] is the pure function pair
//! that converts one to the other, so an order seen in a broker's answer or stream can be tied back
//! to your cid. A connector owns its codec. Most callers never use this module directly. It is public
//! for connector authors and for tools that read a broker's raw data.

use thiserror::Error;
use uuid::Uuid;

use crate::types::ClientOrderId;

/// The form of a client order id as the broker sees it: a string in the broker's own field.
///
/// Produced by [`CidCodec::encode`] and read back by [`CidCodec::decode`]. The content depends on
/// the broker and may carry a prefix or a changed alphabet, so treat it as opaque.
///
/// # Examples
///
/// ```
/// use brokapi_core::NativeCid;
///
/// let n = NativeCid::new("abc");
/// assert_eq!(n.as_str(), "abc");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NativeCid(pub String);

impl NativeCid {
    /// Wraps a string. Nothing is checked here: [`CidCodec::encode`] makes the checks.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The string as the broker sees it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The characters a broker accepts in its client order id field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Charset {
    /// Digits `0-9` only.
    Numeric,
    /// Digits and capital letters `A-Z`.
    AlnumUpper,
    /// Any printable ASCII.
    Ascii,
    /// Digits and lowercase `a-f`.
    HexLower,
}

/// What the broker's client order id field can hold, declared by a [`CidCodec`].
///
/// The connector checks it at start. If the field cannot carry a UUID without loss, that is a
/// configuration error at start ([`Error::Config`](crate::Error::Config)), never a degradation at run
/// time: an order whose cid cannot be read back cannot be tied to your ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CidCaps {
    /// Longest native string the broker's field accepts, in characters.
    pub max_len: usize,
    /// Characters the field accepts.
    pub charset: Charset,
    /// How many bits of the UUID the broker field can keep. 128 means the whole UUID.
    pub native_bits: u32,
    /// `true` when [`CidCodec::decode`] always gives back what [`CidCodec::encode`] was given.
    pub lossless: bool,
}

/// Why a [`CidCodec`] cannot encode or accept a cid.
///
/// These are setup problems of a codec, found by the connector at start, so they are reported as
/// [`Error::Config`](crate::Error::Config).
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CidError {
    /// The encoded string is longer than the broker's field.
    #[error("native cid too long: {len} > {max}")]
    TooLong {
        /// Length of the encoded string.
        len: usize,
        /// Longest string the field takes, see [`CidCaps::max_len`].
        max: usize,
    },
    /// The encoded string has a character the broker's field does not accept.
    #[error("invalid character {ch:?} for charset {charset:?}")]
    InvalidChar {
        /// The offending character.
        ch: char,
        /// The set the field accepts.
        charset: Charset,
    },
    /// The codec cannot give the UUID back from the native form, but the connector needs that.
    #[error("lossless decode impossible for this codec")]
    NotLossless,
}

/// A pure, stateless mapping between [`ClientOrderId`] and the broker's [`NativeCid`].
///
/// Rules for an implementation:
/// - `encode` is a pure function: the same cid always gives the same string, with no state and no
///   network.
/// - `decode(encode(cid))` gives `Some(cid)` for every cid (lossless). A broker whose field cannot
///   keep a UUID needs another carrier field, not a lossy codec.
/// - `decode` gives `None` for a string that this codec did not make (for example the cid of an
///   order placed from the broker's app).
///
/// # Examples
///
/// ```
/// use brokapi_core::{CidCodec, ClientOrderId, HexUuidCodec};
///
/// let codec = HexUuidCodec;
/// let cid = ClientOrderId::new_v4();
/// let native = codec.encode(cid).unwrap();
/// assert_eq!(native.as_str().len(), 32);
/// assert_eq!(codec.decode(&native), Some(cid));
/// ```
pub trait CidCodec: Send + Sync {
    /// What the broker's field can hold. Checked by the connector at start.
    fn caps(&self) -> CidCaps;

    /// The broker form of `cid`.
    ///
    /// # Errors
    ///
    /// [`CidError::TooLong`] or [`CidError::InvalidChar`] when the result does not fit the field
    /// described by [`caps`](Self::caps). A correct codec never returns them for a UUID.
    fn encode(&self, cid: ClientOrderId) -> Result<NativeCid, CidError>;

    /// The cid behind a native string, or `None` if this codec did not make that string.
    fn decode(&self, native: &NativeCid) -> Option<ClientOrderId>;
}

/// A lossless codec: the UUID as 32 lowercase hex digits, no hyphens.
///
/// Meant for tests and for brokers whose field holds at least 128 bits. [`decode`](CidCodec::decode)
/// is more tolerant than `encode`: it also reads the hyphenated form and capital letters.
///
/// # Examples
///
/// ```
/// use brokapi_core::{CidCodec, Charset, HexUuidCodec};
///
/// let caps = HexUuidCodec.caps();
/// assert!(caps.lossless);
/// assert_eq!(caps.max_len, 32);
/// assert_eq!(caps.charset, Charset::HexLower);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct HexUuidCodec;

impl CidCodec for HexUuidCodec {
    fn caps(&self) -> CidCaps {
        CidCaps {
            max_len: 32,
            charset: Charset::HexLower,
            native_bits: 128,
            lossless: true,
        }
    }

    fn encode(&self, cid: ClientOrderId) -> Result<NativeCid, CidError> {
        Ok(NativeCid(cid.as_simple().to_string()))
    }

    fn decode(&self, native: &NativeCid) -> Option<ClientOrderId> {
        Uuid::parse_str(native.as_str()).ok()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    prop_compose! {
        fn arb_client_order_id()(bytes in any::<[u8; 16]>()) -> ClientOrderId {
            ClientOrderId::from_bytes(bytes)
        }
    }

    proptest! {
        #[test]
        fn hex_uuid_roundtrip(cid in arb_client_order_id()) {
            let codec = HexUuidCodec;
            let native = codec.encode(cid).expect("encode");
            let decoded = codec.decode(&native).expect("decode");
            prop_assert_eq!(decoded, cid);
        }
    }
}
