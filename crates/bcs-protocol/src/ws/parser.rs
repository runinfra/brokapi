use tracing::warn;

use crate::models::messages::IncomingMessage;
use crate::ws::client::BcsWsChannel;
use crate::ws::models::{MarketDataMessage, WsRawMessage};

/// Parses one text frame of a channel into an [`IncomingMessage`].
///
/// The function never fails and never panics. The order of tries is:
///
/// 1. A frame with `"type": "error"` becomes [`IncomingMessage::Error`].
/// 2. A frame with a `responseType` becomes [`IncomingMessage::MarketData`].
/// 3. Otherwise the frame is read as the data type of `service`: limits, portfolio, order message
///    (execution and events channels), transaction message, margin indicators, or market data.
/// 4. A frame that fits none of these comes back as [`IncomingMessage::Unknown`] with the raw text.
///    A broken frame on the transaction channel is also logged as a warning, without its text (it holds account data).
///
/// A session calls this for every text frame. Call it yourself to test your handler on a sample frame.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::enums::OrderStatus;
/// use brokapi_bcs_protocol::models::messages::IncomingMessage;
/// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
/// use brokapi_bcs_protocol::ws::parse_incoming;
///
/// // An order event: the order is filled.
/// let frame = r#"{
///   "originalClientOrderId": "550e8400-e29b-41d4-a716-446655440000",
///   "clientOrderId": "",
///   "data": {"orderStatus": "2", "executionType": "2", "ticker": "SBER", "classCode": "TQBR",
///            "side": "1", "orderType": "2", "orderQuantity": 1, "executedQuantity": 1,
///            "lastQuantity": 1, "remainedQuantity": 0, "price": 300.0}
/// }"#;
/// match parse_incoming(frame, BcsWsChannel::OrderEvents) {
///     IncomingMessage::OrderExecution(m) => {
///         assert_eq!(m.data.unwrap().order_status, OrderStatus::Filled);
///     }
///     other => panic!("unexpected: {other:?}"),
/// }
///
/// // Text that is not a frame of the broker is kept, not dropped.
/// assert!(matches!(parse_incoming("hello", BcsWsChannel::Limits), IncomingMessage::Unknown(_)));
/// ```
pub fn parse_incoming(text: &str, service: BcsWsChannel) -> IncomingMessage {
    // Сначала определяем тип сообщения
    if let Ok(raw) = serde_json::from_str::<WsRawMessage>(text) {
        // Error
        if raw.msg_type.as_deref() == Some("error")
            && let Ok(err) = serde_json::from_str(text)
        {
            return IncomingMessage::Error(err);
        }
        // Market data (имеет responseType)
        if raw.response_type.is_some()
            && let Ok(md) = serde_json::from_str::<MarketDataMessage>(text)
        {
            return IncomingMessage::MarketData(md);
        }
    }

    // По типу сервиса
    match service {
        BcsWsChannel::Limits => {
            if let Ok(data) = serde_json::from_str(text) {
                return IncomingMessage::Limits(data);
            }
        }
        BcsWsChannel::Portfolio => {
            if let Ok(data) = serde_json::from_str(text) {
                return IncomingMessage::Portfolio(data);
            }
        }
        BcsWsChannel::OrderExecution | BcsWsChannel::OrderEvents => {
            if let Ok(data) = serde_json::from_str(text) {
                return IncomingMessage::OrderExecution(data);
            }
        }
        BcsWsChannel::OrderTransaction => match serde_json::from_str(text) {
            Ok(data) => {
                return IncomingMessage::OrderTransaction(data);
            }
            Err(e) => {
                // Only the kind and the position of the failure: the frame holds account data and the
                // message of serde can quote a value.
                warn!(
                    "[WS_PARSE] OrderTransaction frame not understood: {:?} at line {} column {}, {} bytes",
                    e.classify(),
                    e.line(),
                    e.column(),
                    text.len()
                );
            }
        },
        BcsWsChannel::MarginalIndicators => {
            if let Ok(data) = serde_json::from_str(text) {
                return IncomingMessage::MarginalIndicators(data);
            }
        }
        BcsWsChannel::MarketData => {
            if let Ok(md) = serde_json::from_str::<MarketDataMessage>(text) {
                return IncomingMessage::MarketData(md);
            }
        }
    }

    IncomingMessage::Unknown(text.to_string())
}
