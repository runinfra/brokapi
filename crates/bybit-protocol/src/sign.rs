//! The signature of a request: HMAC-SHA256, written as lower case hex.
//!
//! A REST request signs `timestamp + api_key + recv_window + payload`, where the payload is the query string of a GET
//! or the JSON body of a POST, exactly as sent. The WebSocket login signs `GET/realtime` plus the `expires` time.

use ring::hmac;

/// HMAC-SHA256 of `payload` with `secret`, as lower case hex.
///
/// # Examples
///
/// The test vector 2 of RFC 4231:
///
/// ```
/// use brokapi_bybit_protocol::sign::sign;
///
/// assert_eq!(
///     sign("Jefe", "what do ya want for nothing?"),
///     "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
/// );
/// ```
pub fn sign(secret: &str, payload: &str) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let tag = hmac::sign(&key, payload.as_bytes());
    let mut out = String::with_capacity(64);
    for byte in tag.as_ref() {
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    out
}

/// The string a REST request signs.
///
/// # Examples
///
/// ```
/// use brokapi_bybit_protocol::sign::rest_payload;
///
/// // The example of the documentation: a GET with a query.
/// assert_eq!(
///     rest_payload(1658384314791, "XXXXXXXXXX", 5000, "category=option&symbol=BTC-29JUL22-25000-C"),
///     "1658384314791XXXXXXXXXX5000category=option&symbol=BTC-29JUL22-25000-C"
/// );
/// ```
pub fn rest_payload(timestamp_ms: i64, api_key: &str, recv_window_ms: u64, body: &str) -> String {
    format!("{timestamp_ms}{api_key}{recv_window_ms}{body}")
}

/// The `args` of the WebSocket login: the key, the `expires` time (milliseconds, in the future) and the signature of
/// `GET/realtime{expires}`.
///
/// # Examples
///
/// ```
/// use brokapi_bybit_protocol::sign::ws_auth_args;
///
/// let args = ws_auth_args("key", "secret", 1662350400000);
/// assert_eq!(args[0], "key");
/// assert_eq!(args[1], "1662350400000");
/// assert_eq!(args[2].len(), 64);
/// ```
pub fn ws_auth_args(api_key: &str, api_secret: &str, expires_ms: i64) -> [String; 3] {
    [
        api_key.to_string(),
        expires_ms.to_string(),
        sign(api_secret, &format!("GET/realtime{expires_ms}")),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_is_lower_case_and_64_long() {
        let s = sign("secret", "payload");
        assert_eq!(s.len(), 64);
        assert!(
            s.chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        );
    }

    #[test]
    fn a_post_signs_its_body_text() {
        let body = r#"{"category":"linear"}"#;
        let a = rest_payload(1, "k", 5000, body);
        assert_eq!(a, r#"1k5000{"category":"linear"}"#);
        assert_ne!(sign("s", &a), sign("s", &rest_payload(2, "k", 5000, body)));
    }
}
