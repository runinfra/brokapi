//! The root certificate of the Ministry of Digital Development (Russian Trusted Root CA), trusted for T-Bank only.
//!
//! Both endpoints of T-Bank (production and sandbox) present a chain that ends at this root, and the bundled web PKI
//! roots do not know it: without it every call fails with `UnknownIssuer`. The client adds it to the TLS roots of a
//! connection to a host of `tbank.ru`, and of no other host: a connection elsewhere (an endpoint set by hand, another
//! broker) trusts only the web PKI roots and the root the user passes. The file is the one Gosuslugi publishes, see the
//! fingerprint test in `tests/root_ca.rs`. It is valid until 27.02.2032.

/// The Russian Trusted Root CA, PEM.
pub const RUSSIAN_TRUSTED_ROOT_CA_PEM: &[u8] =
    include_bytes!("../certs/russian_trusted_root_ca.pem");

/// Whether a connection to `endpoint` trusts [`RUSSIAN_TRUSTED_ROOT_CA_PEM`]: an `https://` address whose host is
/// `tbank.ru` or ends with `.tbank.ru`.
///
/// # Examples
///
/// ```
/// use brokapi_tbank_protocol::{PROD_ENDPOINT, SANDBOX_ENDPOINT, trusts_russian_root};
///
/// assert!(trusts_russian_root(PROD_ENDPOINT));
/// assert!(trusts_russian_root(SANDBOX_ENDPOINT));
/// assert!(!trusts_russian_root("https://example.com:443"));
/// assert!(!trusts_russian_root("https://tbank.ru.example.com"));
/// ```
pub fn trusts_russian_root(endpoint: &str) -> bool {
    let Some(rest) = endpoint.strip_prefix("https://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.contains('@') {
        return false;
    }
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _port)| host)
        .to_ascii_lowercase();
    host == "tbank.ru" || host.ends_with(".tbank.ru")
}
