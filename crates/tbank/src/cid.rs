//! T-Bank client order id codec
//!
//! `PostOrderRequest.order_id` is the idempotency key: "в формате UID, максимальная длина 36 символов"
//! (orders.proto). A hyphenated UUID is exactly 36 characters, so the cid fits without loss.
//! The same value comes back as `order_request_id` in `PostOrderResponse`, `OrderState` and
//! `OrderStateStream`, and `CancelOrder` / `GetOrderState` accept it with `ORDER_ID_TYPE_REQUEST`.

use brokapi_core::{Charset, CidCaps, CidCodec, CidError, ClientOrderId, NativeCid};
use uuid::Uuid;

/// The client order id codec of T-Bank: the cid is sent as a hyphenated UUID.
///
/// A hyphenated UUID is exactly 36 characters, which is the limit of `PostOrderRequest.order_id`, so
/// the cid fits without loss and comes back unchanged in `order_request_id`. The connectors use this
/// codec for you. Use it yourself when you read broker messages outside the connector (for example
/// through [`TbankRaw`](crate::TbankRaw)) and want to know whether an order is yours.
///
/// # Examples
///
/// ```
/// use brokapi_core::{CidCodec, ClientOrderId, NativeCid};
/// use brokapi_tbank::TbankCidCodec;
///
/// let codec = TbankCidCodec;
/// assert_eq!(codec.caps().max_len, 36);
/// assert!(codec.caps().lossless);
///
/// let cid = ClientOrderId::new_v4();
/// let native = codec.encode(cid).expect("a UUID always fits");
/// assert_eq!(native.as_str().len(), 36);
/// assert_eq!(codec.decode(&native), Some(cid));
///
/// // An id that is not a UUID was not placed by this library.
/// assert_eq!(codec.decode(&NativeCid::new("placed-by-hand")), None);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TbankCidCodec;

impl CidCodec for TbankCidCodec {
    fn caps(&self) -> CidCaps {
        CidCaps {
            max_len: 36,
            charset: Charset::Ascii,
            native_bits: 128,
            lossless: true,
        }
    }

    fn encode(&self, cid: ClientOrderId) -> Result<NativeCid, CidError> {
        let s = cid.hyphenated().to_string();
        let max = self.caps().max_len;
        if s.len() > max {
            return Err(CidError::TooLong { len: s.len(), max });
        }
        Ok(NativeCid::new(s))
    }

    fn decode(&self, native: &NativeCid) -> Option<ClientOrderId> {
        Uuid::parse_str(native.as_str()).ok()
    }
}

/// `order_request_id` of a broker message -> our cid, if it is one.
///
/// Returns `None` for an empty string or anything that is not a UUID: the order was placed
/// elsewhere (phone, another program) and carries no cid of ours.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::cid::parse_request_id;
///
/// let cid = parse_request_id("67e55044-10b1-426f-9247-bb680e5fe0c8");
/// assert!(cid.is_some());
/// assert!(parse_request_id("").is_none());
/// ```
pub fn parse_request_id(raw: &str) -> Option<ClientOrderId> {
    TbankCidCodec.decode(&NativeCid::new(raw))
}

/// Stable stand-in cid for an order that was not placed by us (phone, another robot): the broker gives
/// no `order_request_id`, but `ReconcileReport::foreign` needs a non-nil cid. Derived from the exchange
/// order id (FNV-1a), never produced by `Uuid::new_v4`.
///
/// The same exchange id always gives the same value, so repeated reports of one foreign order line
/// up. The value is not a key you can send back to the broker: the broker does not know it.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::cid::synthetic_cid;
///
/// assert_eq!(synthetic_cid("EX1"), synthetic_cid("EX1"));
/// assert_ne!(synthetic_cid("EX1"), synthetic_cid("EX2"));
/// ```
pub fn synthetic_cid(exchange_order_id: &str) -> ClientOrderId {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in exchange_order_id.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    Uuid::from_u64_pair(0x7462_616e_6b2d_666f, h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn tbank_cid_roundtrip(bytes in any::<[u8; 16]>()) {
            let cid = ClientOrderId::from_bytes(bytes);
            let codec = TbankCidCodec;
            let native = codec.encode(cid).expect("encode");
            prop_assert!(native.as_str().len() <= 36);
            prop_assert_eq!(codec.decode(&native), Some(cid));
        }
    }

    #[test]
    fn synthetic_is_stable_and_distinct() {
        assert_eq!(synthetic_cid("123"), synthetic_cid("123"));
        assert_ne!(synthetic_cid("123"), synthetic_cid("124"));
        assert!(!synthetic_cid("123").is_nil());
    }

    #[test]
    fn foreign_request_id_is_not_a_cid() {
        assert_eq!(parse_request_id(""), None);
        assert_eq!(parse_request_id("not-a-uuid"), None);
    }
}
