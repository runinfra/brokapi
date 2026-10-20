//! The BCS imitation against the answers of the live broker (reference files in `tests/fixtures/live`).
//!
//! The reference files come from a live run (the page "Live runs" of the book). While there are none, the
//! comparison prints that it was skipped and passes. Once they are in the repository, the same test checks that
//! the imitation answers with the same fields, the same types and the same codes.

use std::collections::BTreeMap;
use std::sync::Arc;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::http::client::BcsHttpClient;
use brokapi_mocks::bcs::{MockOptions, MockServer};
use brokapi_mocks::live::{self, NoReference, Outcome, load_dir};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/live");

/// Every probe of the live run, run against the imitation. The names are those of the reference files.
async fn probes_on_the_mock() -> BTreeMap<String, Outcome> {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let http = BcsHttpClient::new(
        BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true),
    )
    .expect("client");
    let mut all = Vec::new();
    all.extend(live::bcs::read_only(&http, "SBER", "TQBR").await);
    all.extend(live::bcs::order_roundtrip(&http, "SBER", "TQBR", "1.0", 1).await);
    live::by_name(all)
}

#[tokio::test]
async fn the_mock_answers_like_the_live_broker() {
    let reference = load_dir(FIXTURES);
    if reference.is_empty() {
        println!("{}", NoReference("bcs"));
        return;
    }
    live::check(&reference, &probes_on_the_mock().await).assert_ok();
}

/// The probes themselves keep working against the imitation, so the day a live run is made they are known good.
#[tokio::test]
async fn every_probe_runs_on_the_mock() {
    let all = probes_on_the_mock().await;
    assert!(
        all.len() >= 13,
        "the probes: {:?}",
        all.keys().collect::<Vec<_>>()
    );
    assert!(matches!(all["order_book"], Outcome::Answered(_)));
    assert!(
        matches!(all["order_unknown_cid"], Outcome::Refused { ref code, .. } if code == "http_404"),
        "{:?}",
        all["order_unknown_cid"]
    );
    assert!(
        matches!(all["place_order"], Outcome::Answered(_)),
        "{:?}",
        all["place_order"]
    );
}

/// What a live run does, on the imitation: record the answers, read them back, compare. The files are clean
/// and say the same as the answers they came from.
#[tokio::test]
async fn recorded_answers_are_clean_and_compare_equal_to_their_source() {
    let dir = std::env::temp_dir().join(format!("brokapi-bcs-reference-{}", std::process::id()));
    let answers = probes_on_the_mock().await;
    let recorder =
        live::Recorder::new("bcs", &dir).with_secrets(["mock-refresh-token", "mock-access-token"]);
    for (name, outcome) in &answers {
        recorder.record(name, outcome);
    }
    let reference = load_dir(&dir);
    assert_eq!(reference.len(), answers.len());
    for file in std::fs::read_dir(&dir).unwrap() {
        let text = std::fs::read_to_string(file.unwrap().path()).unwrap();
        assert!(
            !text.contains("mock-refresh-token") && !text.contains("mock-access-token"),
            "a secret is in a file:\n{text}"
        );
    }
    live::check(&reference, &answers).assert_ok();
    std::fs::remove_dir_all(&dir).unwrap();
}
