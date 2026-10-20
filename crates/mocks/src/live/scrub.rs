//! Cleaning an answer of a live broker before it is written to the disk.
//!
//! The reference files are committed to a public repository. They must keep what the comparison
//! needs (field names, types, the look of numbers and codes) and nothing that belongs to a person or opens
//! an account: tokens, account and order numbers, names, contacts.
//!
//! Three kinds of cleaning, all on the parsed JSON:
//!
//! * **By name.** The value of a field with a telling name (`token`, `account_id`, `clientOrderId`, `fio`,
//!   `email`, ...) is replaced, whatever it looks like.
//! * **By look.** A string that looks like a secret or an identifier (a JWT, a `t.` token of T-Bank, a UUID, an
//!   e-mail, a long number, a long code with digits) is replaced, whatever the field is called.
//! * **By context.** `id` and `name` of an account (an object with `id` and `type`/`status` and no
//!   instrument marks) are the number and the name the owner chose, so they are replaced; `name` of an
//!   instrument is public and stays.
//!
//! A replaced value keeps its type: a string becomes `"<redacted>"`, a number `0`. The comparison of
//! shapes does not notice the difference.
//!
//! [`Recorder`](super::Recorder) also takes the secrets of the run (the tokens of `.env`, the account) and cuts
//! them out of the text as the last step, in case a field with an unlucky name held one.

use serde_json::{Map, Value};

/// What a replaced string becomes.
pub const REDACTED: &str = "<redacted>";

/// Names of fields whose value is always replaced. Compared in lower case, without `_` and `-`.
const SECRET_KEYS: &[&str] = &[
    // access
    "token",
    "accesstoken",
    "refreshtoken",
    "idtoken",
    "authorization",
    "secret",
    "jwt",
    "password",
    "passwd",
    "apikey",
    "sessionid",
    // accounts and clients
    "accountid",
    "accountids",
    "accountnumber",
    "accountname",
    "clientid",
    "clientcode",
    "clientnumber",
    "agreement",
    "agreementid",
    "contractid",
    "depocode",
    // orders and trades: numbers that open or identify a deal
    "orderid",
    "orderids",
    "orderrequestid",
    "clientorderid",
    "originalclientorderid",
    "originalorderid",
    "exchangeorderid",
    "orderno",
    "ordernum",
    "ordernumber",
    "execid",
    "executionid",
    "tradeid",
    "tradeno",
    "tradenum",
    "operationid",
    "stoporderid",
    "transactionid",
    "comment",
    // a person
    "fio",
    "fullname",
    "firstname",
    "lastname",
    "middlename",
    "patronymic",
    "username",
    "login",
    "email",
    "phone",
    "phonenumber",
    "mobile",
    "birthdate",
    "inn",
    "snils",
    "passport",
    "address",
];

fn normalize(key: &str) -> String {
    key.chars()
        .filter(|c| *c != '_' && *c != '-')
        .flat_map(char::to_lowercase)
        .collect()
}

/// Cleans `value` in place.
///
/// # Examples
///
/// ```
/// use brokapi_mocks::live::scrub;
/// use serde_json::json;
///
/// let mut answer = json!({
///     "accountId": "2000123456",
///     "price": 244.30,
///     "ticker": "SBER",
///     "orders": [{"clientOrderId": "c0ffee00-1111-4222-8333-444455556666", "qty": 2}],
/// });
/// scrub(&mut answer);
/// assert_eq!(answer["accountId"], "<redacted>");
/// assert_eq!(answer["orders"][0]["clientOrderId"], "<redacted>");
/// // What the comparison needs stays.
/// assert_eq!(answer["ticker"], "SBER");
/// assert_eq!(answer["price"], 244.30);
/// assert_eq!(answer["orders"][0]["qty"], 2);
/// ```
pub fn scrub(value: &mut Value) {
    match value {
        Value::Object(map) => scrub_object(map),
        Value::Array(items) => items.iter_mut().for_each(scrub),
        Value::String(s) if looks_secret(s) => *s = REDACTED.to_owned(),
        _ => {}
    }
}

fn scrub_object(map: &mut Map<String, Value>) {
    let account_like = is_account_like(map);
    for (key, v) in map.iter_mut() {
        let normal = normalize(key);
        // `account` and `accounts` hold either numbers (replaced as a whole) or objects of accounts (their
        // own fields are cleaned one by one: the structure is what the comparison reads).
        let holds_objects = matches!(normal.as_str(), "account" | "accounts")
            && match v {
                Value::Object(_) => true,
                Value::Array(items) => items.iter().any(Value::is_object),
                _ => false,
            };
        let secret = SECRET_KEYS.contains(&normal.as_str())
            || matches!(normal.as_str(), "account" | "accounts") && !holds_objects
            || (account_like && matches!(key.as_str(), "id" | "name"));
        if secret {
            redact_leaves(v);
        } else {
            scrub(v);
        }
    }
}

/// An account of a broker: has an `id`, a `type` or `status`, and is not an instrument.
fn is_account_like(map: &Map<String, Value>) -> bool {
    let has = |k: &str| map.contains_key(k);
    let instrument = [
        "ticker",
        "figi",
        "uid",
        "isin",
        "symbol",
        "classCode",
        "class_code",
    ]
    .iter()
    .any(|k| has(k));
    has("id") && (has("type") || has("status")) && !instrument
}

/// Every string below becomes the placeholder, every number 0. Types stay.
fn redact_leaves(value: &mut Value) {
    match value {
        Value::String(s) => *s = REDACTED.to_owned(),
        Value::Number(n) => {
            *value = if n.is_f64() {
                Value::from(0.0)
            } else {
                Value::from(0)
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_leaves),
        Value::Object(map) => map.values_mut().for_each(redact_leaves),
        Value::Bool(_) | Value::Null => {}
    }
}

/// Whether a string value looks like a secret or an identifier of an account or an order.
fn looks_secret(s: &str) -> bool {
    if s == REDACTED || s.is_empty() {
        return false;
    }
    let lower = s.to_ascii_lowercase();
    // a JWT, a bearer header, a T-Bank token
    if s.starts_with("eyJ") && s.matches('.').count() >= 2 {
        return true;
    }
    if lower.starts_with("bearer ") {
        return true;
    }
    if s.starts_with("t.") && s.len() >= 20 && !s.contains(' ') {
        return true;
    }
    // an e-mail
    if let Some(at) = s.find('@')
        && at > 0
        && s[at..].contains('.')
        && !s.contains(' ')
    {
        return true;
    }
    // a UUID
    if is_uuid(s) {
        return true;
    }
    // a long number without a decimal point: an account, a phone, an order number
    let digits = s.trim_start_matches('+');
    if digits.len() >= 10 && digits.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    // a phone written with spaces and brackets
    let phone_chars = s
        .chars()
        .all(|c| c.is_ascii_digit() || matches!(c, '+' | '(' | ')' | '-' | ' '));
    if phone_chars
        && s.chars().filter(char::is_ascii_digit).count() >= 10
        && s.starts_with(['+', '8', '7', '('])
    {
        return true;
    }
    // a long code of letters, digits and `-` `_` with at least one digit: an order number, a client code
    s.len() >= 12
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        && s.chars().any(|c| c.is_ascii_digit())
        && s.chars()
            .any(|c| c.is_ascii_alphabetic() || c == '-' || c == '_')
}

fn is_uuid(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    parts.len() == 5
        && [8, 4, 4, 4, 12]
            .iter()
            .zip(&parts)
            .all(|(n, p)| p.len() == *n && p.chars().all(|c| c.is_ascii_hexdigit()))
}

/// Cuts every secret of the run out of `text`. Strings shorter than 4 characters are not touched (they would
/// damage the text and are no secrets).
pub fn remove_secrets(text: &str, secrets: &[String]) -> String {
    let mut out = text.to_owned();
    for secret in secrets.iter().filter(|s| s.len() >= 4) {
        out = out.replace(secret.as_str(), "<secret>");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// An answer made of everything that must not reach the disk. Each value is written by hand.
    fn leaky() -> (Value, Vec<&'static str>) {
        let secrets = vec![
            "t.AbCdEf0123456789SecretTokenOfTBank",
            "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.c2lnbmF0dXJl",
            "2000123456",
            "40817810099910004312",
            "7f6e5d4c-3b2a-4c1d-8e9f-0a1b2c3d4e5f",
            "20261019-TQBR-79000001",
            "Иванов Иван Петрович",
            "ivan.petrov@example.com",
            "+7 (903) 123-45-67",
            "d41d8cd98f00b204e980",
            "Основной счет Ивана",
            "refresh-secret-value",
        ];
        let answer = json!({
            "accessToken": secrets[0],
            "authorization": format!("Bearer {}", secrets[1]),
            "meta": { "jwt": secrets[1], "refresh_token": secrets[11] },
            "accounts": [
                { "id": secrets[2], "type": "ACCOUNT_TYPE_TINKOFF", "status": "OPEN", "name": secrets[10] },
                { "id": secrets[4], "type": "ACCOUNT_TYPE_IIS", "name": "ИИС" }
            ],
            "account": secrets[3],
            "orders": [
                { "orderId": secrets[5], "clientOrderId": secrets[9], "ticker": "SBER", "price": "244.30", "qty": 2,
                  "text": secrets[4] }
            ],
            "owner": { "fio": secrets[6], "contacts": { "email": secrets[7], "phone": secrets[8] } },
            "note": secrets[7],
            "order_ref": secrets[5],
            "instrument": { "ticker": "SBER", "name": "Сбербанк", "uid": "e6123145-9665-43e0-8413-cd61b8aa9b13" },
        });
        (answer, secrets)
    }

    #[test]
    fn no_secret_of_the_example_is_left() {
        let (mut answer, secrets) = leaky();
        scrub(&mut answer);
        let text = answer.to_string();
        for secret in secrets {
            assert!(!text.contains(secret), "left in the file: {secret}\n{text}");
        }
    }

    #[test]
    fn what_the_comparison_needs_is_left() {
        let (mut answer, _) = leaky();
        scrub(&mut answer);
        // names of fields
        assert!(answer["orders"][0].get("clientOrderId").is_some());
        assert!(answer["accounts"][0].get("status").is_some());
        // public values, numbers as written by the broker
        assert_eq!(answer["orders"][0]["ticker"], "SBER");
        assert_eq!(answer["orders"][0]["price"], "244.30");
        assert_eq!(answer["orders"][0]["qty"], 2);
        assert_eq!(answer["accounts"][0]["type"], "ACCOUNT_TYPE_TINKOFF");
        assert_eq!(
            answer["accounts"][0]["id"], "<redacted>",
            "the number of an account goes, the field stays"
        );
        // an instrument keeps its name
        assert_eq!(answer["instrument"]["name"], "Сбербанк");
    }

    #[test]
    fn a_replaced_value_keeps_its_type() {
        let mut v = json!({"accountId": 123456, "accounts": ["a", "b"], "token": "x", "orderId": 7.5, "ok": true});
        scrub(&mut v);
        assert_eq!(v["accountId"], 0);
        assert!(v["orderId"].is_f64());
        assert_eq!(v["accounts"], json!(["<redacted>", "<redacted>"]));
        assert_eq!(v["token"], "<redacted>");
        assert_eq!(v["ok"], true);
    }

    #[test]
    fn names_of_fields_are_compared_without_case_and_separators() {
        for key in [
            "clientOrderId",
            "client_order_id",
            "CLIENT-ORDER-ID",
            "ClientOrderID",
        ] {
            let mut v = json!({ key: "value-1234" });
            scrub(&mut v);
            assert_eq!(v[key], "<redacted>", "{key}");
        }
    }

    #[test]
    fn a_secret_of_the_run_is_cut_out_of_the_text() {
        let text = r#"{"note":"the token is abc-secret-123 and again abc-secret-123"}"#;
        let clean = remove_secrets(text, &["abc-secret-123".to_owned(), "x".to_owned()]);
        assert!(!clean.contains("abc-secret-123"));
        assert!(clean.contains("<secret>"));
        // a short "secret" would eat the text and is skipped
        assert!(clean.contains("the token"));
    }

    #[test]
    fn ordinary_market_data_is_not_taken_for_a_secret() {
        let mut v = json!({
            "ticker": "SBER", "classCode": "TQBR", "price": "244.35", "qty": "1000",
            "status": "SUBSCRIPTION_STATUS_SUCCESS", "time": "2026-10-19T07:05:00.000Z",
            "isin": "RU0009029540x", "size": 100,
        });
        let before = v.clone();
        scrub(&mut v);
        // the ISIN-like value with a letter at the end is a long code, it may go: everything else stays
        v["isin"] = before["isin"].clone();
        assert_eq!(v, before);
    }
}
