//! The Finam imitation against the answers of the live broker (reference files in `tests/fixtures/live`).
//!
//! The reference files come from a live run (the page "Live runs" of the book). While there are none, the
//! comparison prints that it was skipped and passes. Once they are in the repository, the same test checks that
//! the imitation answers with the same fields, the same types and the same codes.

use std::collections::BTreeMap;

use brokapi_finam_protocol::{ApiEndpoint, FinamApi, start_token_manager};
use brokapi_mocks::finam::{MockServer, SECRET};
use brokapi_mocks::live::{self, NoReference, Outcome, load_dir};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/live");

/// Every probe of the live run, run against the imitation. The names are those of the reference files.
async fn probes_on_the_mock() -> BTreeMap<String, Outcome> {
    let server = MockServer::start().await;
    let endpoint = ApiEndpoint::new(server.url.clone());
    let tokens = start_token_manager(&endpoint, SECRET, "")
        .await
        .expect("tokens");
    let api = FinamApi::connect(&endpoint, tokens, false).expect("client");
    let mut all = Vec::new();
    all.extend(live::finam::read_only(&api, "ACC1", "SBER@MISX").await);
    all.extend(live::finam::order_roundtrip(&api, "ACC1", "SBER@MISX", "1.0", "1").await);
    live::by_name(all)
}

#[tokio::test]
async fn the_mock_answers_like_the_live_broker() {
    let reference = load_dir(FIXTURES);
    if reference.is_empty() {
        println!("{}", NoReference("finam"));
        return;
    }
    live::check(&reference, &probes_on_the_mock().await).assert_ok();
}

/// The probes themselves keep working against the imitation, so the day a live run is made they are known good.
#[tokio::test]
async fn every_probe_runs_on_the_mock() {
    let all = probes_on_the_mock().await;
    assert!(
        all.len() >= 10,
        "the probes: {:?}",
        all.keys().collect::<Vec<_>>()
    );
    assert!(matches!(all["get_account"], Outcome::Answered(_)));
    assert!(
        matches!(all["place_order"], Outcome::Answered(_)),
        "{:?}",
        all["place_order"]
    );
    assert!(
        matches!(all["order_unknown"], Outcome::Refused { .. }),
        "{:?}",
        all["order_unknown"]
    );
}

/// What a live run does, on the imitation: record the answers, read them back, compare. The files are clean
/// and say the same as the answers they came from.
#[tokio::test]
async fn recorded_answers_are_clean_and_compare_equal_to_their_source() {
    let dir = std::env::temp_dir().join(format!("brokapi-finam-reference-{}", std::process::id()));
    let answers = probes_on_the_mock().await;
    let recorder = live::Recorder::new("finam", &dir).with_secrets([SECRET, "ACC1"]);
    for (name, outcome) in &answers {
        recorder.record(name, outcome);
    }
    let reference = load_dir(&dir);
    assert_eq!(reference.len(), answers.len());
    for file in std::fs::read_dir(&dir).unwrap() {
        let text = std::fs::read_to_string(file.unwrap().path()).unwrap();
        assert!(
            !text.contains(SECRET) && !text.contains("ACC1"),
            "a secret is in a file:\n{text}"
        );
    }
    live::check(&reference, &answers).assert_ok();
    std::fs::remove_dir_all(&dir).unwrap();
}
