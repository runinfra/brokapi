//! The client order id codec: BCS accepts a full UUID as `clientOrderId`.

use brokapi_core::{Charset, CidCaps, CidCodec, CidError, ClientOrderId, NativeCid};
use uuid::Uuid;

/// BCS accepts full UUID strings as `clientOrderId` (lossless, 128-bit).
#[derive(Debug, Clone, Copy, Default)]
pub struct BcsCidCodec;

impl CidCodec for BcsCidCodec {
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

/// Namespace of the stable cids given to orders that are not ours.
const FOREIGN_NS: Uuid = Uuid::from_u128(0x7bc5_2f01_53a4_4d0e_9b6a_0c8e_bc50_0001);

/// Stable cid for an order of this account that this connector did not place (the broker's order
/// list carries only the broker's id, not our `clientOrderId`): the same order id always gives the
/// same cid, so it can be told apart from ours and found again.
pub fn foreign_cid(order_id: &str) -> ClientOrderId {
    Uuid::new_v5(&FOREIGN_NS, order_id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn arb_cid()(bytes in any::<[u8; 16]>()) -> ClientOrderId {
            ClientOrderId::from_bytes(bytes)
        }
    }

    #[test]
    fn foreign_cids_are_stable_and_distinct() {
        assert_eq!(
            foreign_cid("20261019-TQBR-1"),
            foreign_cid("20261019-TQBR-1")
        );
        assert_ne!(
            foreign_cid("20261019-TQBR-1"),
            foreign_cid("20261019-TQBR-2")
        );
        assert_eq!(foreign_cid("x").get_version_num(), 5);
    }

    proptest! {
        #[test]
        fn bcs_cid_roundtrip(cid in arb_cid()) {
            let codec = BcsCidCodec;
            let native = codec.encode(cid).expect("encode");
            let decoded = codec.decode(&native).expect("decode");
            prop_assert_eq!(decoded, cid);
        }
    }
}
