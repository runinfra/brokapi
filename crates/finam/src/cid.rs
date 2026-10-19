//! Client order id codec
//!
//! Finam's `client_order_id` holds at most 20 characters (alphabet not documented) and a UUID is
//! 128 bits, so a UUID cannot be carried there losslessly. The order `comment` ("order label",
//! up to 128 characters) can, and Finam echoes the whole `Order` in order state and the same label
//! in `AccountTrade.comment`. So:
//!
//! - the lossless, reversible form (this codec) goes into `comment`: `bc1:<32 hex>`;
//! - `client_order_id` carries a 20-hex-char tag derived from the UUID (80 bits). It is not
//!   reversible, it is a secondary key used when the comment is missing in an echo.
//!
//! That the comment is echoed back unchanged is not verified against the live API.
//!
//! # Examples
//!
//! ```
//! use brokapi_core::{CidCodec, ClientOrderId};
//! use brokapi_finam::FinamCidCodec;
//!
//! let codec = FinamCidCodec;
//! let cid = ClientOrderId::new_v4();
//!
//! // The comment carries the whole id and decodes back to it.
//! let comment = codec.encode(cid).unwrap();
//! assert!(comment.as_str().starts_with("bc1:"));
//! assert_eq!(codec.decode(&comment), Some(cid));
//!
//! // The `client_order_id` tag is short and cannot be decoded.
//! assert_eq!(codec.client_order_id(cid).len(), 20);
//! ```

use brokapi_core::{
    Charset, CidCaps, CidCodec, CidError, ClientOrderId, ConfigError, Error, NativeCid,
};
use uuid::Uuid;

/// Marks a comment written by this library: `bc1:` followed by 32 hex characters.
///
/// A comment without the prefix (an order placed by hand, or by another program) does not decode.
pub const COMMENT_PREFIX: &str = "bc1:";
/// Documented limit of `Order.client_order_id`.
pub const CLIENT_ORDER_ID_MAX: usize = 20;
/// Documented limit of `Order.comment`.
pub const COMMENT_MAX: usize = 128;

/// Codec between the library's client order id (a UUID) and the fields Finam stores on an order.
///
/// Implements [`CidCodec`]: [`encode`](CidCodec::encode) makes the lossless text for the order
/// `comment`, [`decode`](CidCodec::decode) reads it back. [`FinamCidCodec::client_order_id`] makes the
/// short, one-way tag for the `client_order_id` field.
///
/// The connector uses it on its own. You need it to read the cid out of an order you fetched through
/// `raw()`, or to find your order at the broker by its comment.
///
/// See the [module docs](self) for the reason for two fields.
#[derive(Debug, Clone, Copy, Default)]
pub struct FinamCidCodec;

impl FinamCidCodec {
    /// The 20 character tag for the `client_order_id` field of an order.
    ///
    /// 20 lowercase hex chars from 10 fully random bytes of the UUID (v4 version/variant skipped).
    /// It is one-way: 80 bits of 128 cannot rebuild the cid. It is only a second key for the case
    /// when the comment is missing in an echo.
    pub fn client_order_id(&self, cid: ClientOrderId) -> String {
        const PICK: [usize; 10] = [0, 1, 2, 3, 4, 5, 7, 9, 10, 11];
        let b = cid.as_bytes();
        PICK.iter().map(|&i| format!("{:02x}", b[i])).collect()
    }

    /// Startup check: the formats must fit the broker limits, otherwise it is a
    /// configuration error, not a runtime degradation.
    ///
    /// Encodes a sample cid, checks the length of the comment and of the tag, and decodes the
    /// comment back. [`UserDataConnector::start`](crate::UserDataConnector::start) calls it.
    ///
    /// # Errors
    ///
    /// [`brokapi_core::Error::Config`] when a form does not fit the documented limits
    /// ([`COMMENT_MAX`], [`CLIENT_ORDER_ID_MAX`]) or does not decode back to the same cid.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_finam::FinamCidCodec;
    ///
    /// FinamCidCodec.validate().expect("the codec fits the documented Finam limits");
    /// ```
    pub fn validate(&self) -> Result<(), Error> {
        let sample = Uuid::from_bytes([0xff; 16]);
        let comment = self.encode(sample).map_err(cfg)?;
        if comment.as_str().len() > COMMENT_MAX {
            return Err(cfg(CidError::TooLong {
                len: comment.as_str().len(),
                max: COMMENT_MAX,
            }));
        }
        let tag = self.client_order_id(sample);
        if tag.len() > CLIENT_ORDER_ID_MAX {
            return Err(cfg(CidError::TooLong {
                len: tag.len(),
                max: CLIENT_ORDER_ID_MAX,
            }));
        }
        match self.decode(&comment) {
            Some(back) if back == sample => Ok(()),
            _ => Err(cfg(CidError::NotLossless)),
        }
    }
}

fn cfg(e: CidError) -> Error {
    Error::Config(ConfigError::Message(format!("finam cid codec: {e}")))
}

impl CidCodec for FinamCidCodec {
    fn caps(&self) -> CidCaps {
        CidCaps {
            // Capacity of the carrying field (`comment`), not of `client_order_id`.
            max_len: COMMENT_MAX,
            charset: Charset::Ascii,
            native_bits: 128,
            lossless: true,
        }
    }

    fn encode(&self, cid: ClientOrderId) -> Result<NativeCid, CidError> {
        Ok(NativeCid::new(format!(
            "{COMMENT_PREFIX}{}",
            cid.as_simple()
        )))
    }

    fn decode(&self, native: &NativeCid) -> Option<ClientOrderId> {
        let rest = native.as_str().strip_prefix(COMMENT_PREFIX)?;
        Uuid::parse_str(rest).ok()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn comment_roundtrip(bytes in any::<[u8; 16]>()) {
            let codec = FinamCidCodec;
            let cid = ClientOrderId::from_bytes(bytes);
            let native = codec.encode(cid).unwrap();
            prop_assert!(native.as_str().len() <= COMMENT_MAX);
            prop_assert_eq!(codec.decode(&native), Some(cid));
        }

        #[test]
        fn tag_fits_the_broker_field(bytes in any::<[u8; 16]>()) {
            let tag = FinamCidCodec.client_order_id(ClientOrderId::from_bytes(bytes));
            prop_assert_eq!(tag.len(), CLIENT_ORDER_ID_MAX);
            prop_assert!(tag.bytes().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn foreign_comments_do_not_decode() {
        let c = FinamCidCodec;
        assert_eq!(c.decode(&NativeCid::new("my own label")), None);
        assert_eq!(c.decode(&NativeCid::new("bc1:not-a-uuid")), None);
    }

    #[test]
    fn config_check_passes() {
        FinamCidCodec.validate().unwrap();
    }
}
