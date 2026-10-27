//! The root of the Ministry of Digital Development that the client trusts for the endpoints of T-Bank: it is the
//! right certificate, and it is trusted for those hosts only.

use base64::Engine;
use brokapi_tbank_protocol::{RUSSIAN_TRUSTED_ROOT_CA_PEM, trusts_russian_root};

/// The SHA-256 of the Russian Trusted Root CA, as `openssl x509 -fingerprint -sha256` prints it (checked 27.10.2026
/// against the file of Gosuslugi and the chain both endpoints of T-Bank present).
const FINGERPRINT: &str = "D2:6D:2D:02:31:B7:C3:9F:92:CC:73:85:12:BA:54:10:35:19:E4:40:5D:68:B5:BD:70:3E:97:88:CA:8E:CF:31";

#[test]
fn the_bundled_root_is_the_russian_trusted_root_ca() {
    let pem = std::str::from_utf8(RUSSIAN_TRUSTED_ROOT_CA_PEM).unwrap();
    let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &der);
    let shown: Vec<String> = digest.as_ref().iter().map(|b| format!("{b:02X}")).collect();
    assert_eq!(shown.join(":"), FINGERPRINT);
}

#[test]
fn the_russian_root_is_trusted_only_for_the_hosts_of_tbank() {
    for endpoint in [
        "https://invest-public-api.tbank.ru:443",
        "https://sandbox-invest-public-api.tbank.ru:443",
        "https://INVEST-PUBLIC-API.TBANK.RU",
        "https://tbank.ru/",
    ] {
        assert!(trusts_russian_root(endpoint), "{endpoint}");
    }
    for endpoint in [
        "https://example.com:443",
        "https://tbank.ru.example.com",
        "https://eviltbank.ru",
        "https://example.com/tbank.ru",
        "https://user@example.com:443",
        "http://invest-public-api.tbank.ru:443",
        "http://127.0.0.1:50051",
    ] {
        assert!(!trusts_russian_root(endpoint), "{endpoint}");
    }
}
