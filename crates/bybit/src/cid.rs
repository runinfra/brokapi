//! The client order id codec: Bybit keeps a client order id (`orderLinkId`) of up to 36 characters on every order,
//! and a full hyphenated UUID is exactly 36.

use brokapi_core::{Charset, CidCaps, CidCodec, CidError, ClientOrderId, NativeCid};
use uuid::Uuid;

/// The cid is written into the `orderLinkId` of the order as a hyphenated UUID (lossless, 128-bit, 36 characters of
/// digits, letters and dashes, which the exchange allows). The exchange returns it in the order list, in the order
/// stream and in the fills, which is how a cid comes back. The exchange refuses a second order with the same id while
/// the first one is working, which is what makes a repeated place visible.
#[derive(Debug, Clone, Copy, Default)]
pub struct BybitCidCodec;

impl CidCodec for BybitCidCodec {
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

/// The cid in the `orderLinkId` of an order: a whole UUID, else none (an order placed in the app of the exchange, or by
/// another program).
pub fn cid_from_link_id(link_id: Option<&str>) -> Option<ClientOrderId> {
    Uuid::parse_str(link_id?.trim()).ok()
}

/// Namespace of the stable cids given to orders that are not ours.
const FOREIGN_NS: Uuid = Uuid::from_u128(0x3d19_c0b2_6e7f_4a58_8c41_b7a2_5e90_0002);

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
        assert_eq!(
            foreign_cid("1616024329462743808"),
            foreign_cid("1616024329462743808")
        );
        assert_ne!(
            foreign_cid("1616024329462743808"),
            foreign_cid("1616024329462743809")
        );
        assert_eq!(foreign_cid("x").get_version_num(), 5);
    }

    #[test]
    fn only_a_whole_uuid_in_the_link_id_is_a_cid() {
        let cid = Uuid::new_v4();
        assert_eq!(cid_from_link_id(Some(&cid.to_string())), Some(cid));
        assert_eq!(cid_from_link_id(Some("my-first-order")), None);
        assert_eq!(cid_from_link_id(None), None);
    }

    proptest! {
        #[test]
        fn bybit_cid_roundtrip(bytes in any::<[u8; 16]>()) {
            let cid = ClientOrderId::from_bytes(bytes);
            let native = BybitCidCodec.encode(cid).expect("encode");
            prop_assert_eq!(BybitCidCodec.decode(&native), Some(cid));
        }
    }
}
