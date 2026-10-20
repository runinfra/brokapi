//! The T-Bank imitation against the answers of the live broker (reference files in `tests/fixtures/live`).
//!
//! The reference files come from a live run in the sandbox (the page "Live runs" of the book). While there
//! are none, the comparison prints that it was skipped and passes. Once they are in the repository, the same
//! test checks that the imitation answers with the same fields, the same types and the same codes.

use std::collections::BTreeMap;
use std::sync::Arc;

use brokapi_mocks::live::{self, NoReference, Outcome, load_dir};
use brokapi_mocks::tbank::{ACCOUNT_A, MockServer, TOKEN};
use brokapi_tbank_protocol::{TbankClient, TbankConfig};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/live");

/// Every probe of the live run, run against the imitation. The names are those of the reference files.
async fn probes_on_the_mock() -> BTreeMap<String, Outcome> {
    let server = MockServer::start(Arc::default()).await;
    let client = TbankClient::new(&TbankConfig::for_mock(TOKEN, &server.endpoint)).expect("client");
    let mut all = Vec::new();
    all.extend(live::tbank::read_only(&client, ACCOUNT_A, "uid-sber", "SBER", "TQBR").await);
    all.extend(live::tbank::order_roundtrip(&client, ACCOUNT_A, "uid-sber", "1.0", 1).await);
    all.extend(live::tbank::repeated_order_id(&client, ACCOUNT_A, "uid-sber", "1.0", 1).await);
    live::by_name(all)
}

#[tokio::test]
async fn the_mock_answers_like_the_live_broker() {
    let reference = load_dir(FIXTURES);
    if reference.is_empty() {
        println!("{}", NoReference("tbank"));
        return;
    }
    live::check(&reference, &probes_on_the_mock().await).assert_ok();
}

/// The probes themselves keep working against the imitation, so the day a live run is made they are known good.
#[tokio::test]
async fn every_probe_runs_on_the_mock() {
    let all = probes_on_the_mock().await;
    assert!(
        all.len() >= 18,
        "the probes: {:?}",
        all.keys().collect::<Vec<_>>()
    );
    // The mock must answer where the broker answers, and refuse where the broker refuses.
    assert!(matches!(all["get_accounts"], Outcome::Answered(_)));
    assert!(matches!(
        all["order_state_unknown"],
        Outcome::Refused { .. }
    ));
    assert!(
        matches!(all["instrument_unknown"], Outcome::Refused { .. }),
        "{:?}",
        all["instrument_unknown"]
    );
}

/// What a live run does, on the imitation: record the answers, read them back, compare. The files are clean
/// and say the same as the answers they came from.
#[tokio::test]
async fn recorded_answers_are_clean_and_compare_equal_to_their_source() {
    let dir = std::env::temp_dir().join(format!("brokapi-tbank-reference-{}", std::process::id()));
    let answers = probes_on_the_mock().await;
    let recorder = live::Recorder::new("tbank", &dir).with_secrets([TOKEN, ACCOUNT_A]);
    for (name, outcome) in &answers {
        recorder.record(name, outcome);
    }
    let reference = load_dir(&dir);
    assert_eq!(reference.len(), answers.len());
    for file in std::fs::read_dir(&dir).unwrap() {
        let text = std::fs::read_to_string(file.unwrap().path()).unwrap();
        assert!(
            !text.contains(ACCOUNT_A) && !text.contains(TOKEN),
            "a secret is in a file:\n{text}"
        );
    }
    live::check(&reference, &answers).assert_ok();
    std::fs::remove_dir_all(&dir).unwrap();
}
