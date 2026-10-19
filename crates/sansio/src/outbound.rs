//! Outbound frames queued by sans-io cores for the transport layer.

/// A wire message a core wants sent, returned by [`BrokerCore::poll_transmit`](crate::BrokerCore::poll_transmit).
///
/// # Examples
///
/// ```
/// use brokapi_sansio::Outbound;
///
/// let Outbound::WsText(text) = Outbound::WsText(r#"{"op":"ping"}"#.to_string());
/// assert!(text.contains("ping"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outbound {
    /// A WebSocket text frame. The string is sent as it is: UTF-8, no added framing.
    WsText(String),
}
