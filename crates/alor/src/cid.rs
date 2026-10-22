//! The client order id codec: ALOR keeps a comment of up to 100 characters on every order, and a full UUID
//! fits into it.

use brokapi_core::{Charset, CidCaps, CidCodec, CidError, ClientOrderId, NativeCid};
use uuid::Uuid;

/// The cid is written into the `comment` of the order as a hyphenated UUID (lossless, 128-bit). The broker
/// returns the comment in the order list, in the order stream and in the trades, which is how a cid comes back.
/// The same text is the `X-REQID` of the command, so a repeated command carries the same id.
#[derive(Debug, Clone, Copy, Default)]
pub struct AlorCidCodec;

impl CidCodec for AlorCidCodec {
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
        if s.len() > self.caps().max_len {
            return Err(CidError::TooLong {
                len: s.len(),
                max: self.caps().max_len,
            });
        }
        Ok(NativeCid::new(s))
    }

    fn decode(&self, native: &NativeCid) -> Option<ClientOrderId> {
        Uuid::parse_str(native.as_str()).ok()
    }
}

/// The cid of an order that carries none of ours (placed in the app of the broker, or by another program).
pub fn cid_from_comment(comment: Option<&str>) -> Option<ClientOrderId> {
    Uuid::parse_str(comment?.trim()).ok()
}

/// Namespace of the stable cids given to orders that are not ours.
const FOREIGN_NS: Uuid = Uuid::from_u128(0x7bc5_2f01_53a4_4d0e_9b6a_0c8e_a102_0001);

/// Stable cid for an order of this account that this connector did not place: the same order number always
/// gives the same cid, so it can be told apart from ours and found again.
pub fn foreign_cid(order_id: &str) -> ClientOrderId {
    Uuid::new_v5(&FOREIGN_NS, order_id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn foreign_cids_are_stable_and_distinct() {
        assert_eq!(foreign_cid("18995978560"), foreign_cid("18995978560"));
        assert_ne!(foreign_cid("18995978560"), foreign_cid("18995978561"));
        assert_eq!(foreign_cid("x").get_version_num(), 5);
    }

    #[test]
    fn only_a_whole_uuid_in_the_comment_is_a_cid() {
        let cid = Uuid::new_v4();
        assert_eq!(cid_from_comment(Some(&cid.to_string())), Some(cid));
        assert_eq!(cid_from_comment(Some("my first order")), None);
        assert_eq!(cid_from_comment(None), None);
    }

    proptest! {
        #[test]
        fn alor_cid_roundtrip(bytes in any::<[u8; 16]>()) {
            let cid = ClientOrderId::from_bytes(bytes);
            let native = AlorCidCodec.encode(cid).expect("encode");
            prop_assert_eq!(AlorCidCodec.decode(&native), Some(cid));
        }
    }
}
