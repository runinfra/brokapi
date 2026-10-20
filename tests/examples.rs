//! Runs the programs of `examples/` against the in-process broker imitations.
//!
//! The book (docs/src) includes code from these programs, so this is the test of the code the book
//! shows. Each program has `pub async fn run(...)`, and its `main` does the same for a person at a keyboard.
//! They never touch the network: `BROKAPI_LIVE` is not read here.

#[path = "../examples/quickstart_bcs.rs"]
mod quickstart_bcs;

#[tokio::test(flavor = "multi_thread")]
async fn quickstart_bcs_on_the_mock() {
    quickstart_bcs::run(false)
        .await
        .expect("the quick start runs on the BCS mock");
}

#[path = "../examples/lifecycle.rs"]
mod lifecycle;
#[path = "../examples/recovery.rs"]
mod recovery;

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_on_the_mock() {
    lifecycle::run()
        .await
        .expect("a restart finds the order of the first run");
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_on_the_mock() {
    recovery::run()
        .await
        .expect("an Unknown is resolved with order_state");
}

#[path = "../examples/trading_bot.rs"]
mod trading_bot;

use trading_bot::{Ending, Entry};

#[tokio::test(flavor = "multi_thread")]
async fn trading_bot_buys_below_the_bid_and_sees_the_fill() {
    let out = trading_bot::run_bcs(Ending::Filled)
        .await
        .expect("the bot runs on the BCS mock");
    // the best bid of the mock is 244.30: 99% of it on the step of 0.01 is 241.85
    assert_eq!(
        out.price.expect("an order was placed"),
        "241.85".parse::<brokapi::core::Price>().unwrap()
    );
    assert_eq!(out.journal.len(), 1, "one order, one journal entry");
    assert_eq!(out.journal[0].1, Entry::Done("filled".into()));
    assert_eq!(out.filled, brokapi::core::Qty::from(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn trading_bot_cancels_its_order_on_stop() {
    let out = trading_bot::run_bcs(Ending::StoppedByBot)
        .await
        .expect("the bot runs on the BCS mock");
    assert_eq!(out.journal.len(), 1);
    assert_eq!(out.journal[0].1, Entry::Done("cancelled".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn trading_bot_leaves_unknown_through_order_state() {
    let out = trading_bot::run_tbank_unknown()
        .await
        .expect("the bot runs on the T-Bank mock");
    // exactly one order exists: the lost answer was not answered by a second order
    assert_eq!(out.journal.len(), 1, "{:?}", out.journal);
    assert_eq!(out.journal[0].1, Entry::Done("cancelled".into()));
}

#[path = "../examples/infrastructure.rs"]
mod infrastructure;

#[tokio::test(flavor = "multi_thread")]
async fn infrastructure_survives_a_restart_with_the_journal() {
    use infrastructure::State;

    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("infrastructure");
    std::fs::create_dir_all(&dir).unwrap();
    let r = infrastructure::run(&dir)
        .await
        .expect("two lives of the process");

    // The order that was filled while the process was down: closed offline, and the journal knows how it ended.
    assert_eq!(r.journal[&r.cids["tbank-a"]], State::Done("filled".into()));
    // The three that stayed on the exchanges were confirmed by the reconcile, then cancelled by the process.
    for name in ["bcs", "tbank-b", "finam"] {
        assert_eq!(
            r.journal[&r.cids[name]],
            State::Done("cancelled".into()),
            "{name}"
        );
        assert_eq!(r.reports[name].0, 1, "{name}: one confirmed");
    }
    // The line written before a place that never happened is closed, not left hanging.
    assert_eq!(
        r.journal[&r.orphan],
        State::Done("never reached the broker".into())
    );
    // Nobody else's orders on these accounts.
    assert!(r.reports.values().all(|(_, _, foreign)| *foreign == 0));
}

#[path = "../examples/research.rs"]
mod research;

#[tokio::test(flavor = "multi_thread")]
async fn research_engine_downloads_a_year_records_and_replays() {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("research");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let r = research::run(&dir).await.expect("the engine runs");

    // 1. A year of hourly bars, 2 instruments per venue, no gaps and no duplicates.
    for venue in ["bcs", "tbank", "finam"] {
        for ticker in ["SBER", "GAZP"] {
            let text =
                std::fs::read_to_string(dir.join(venue).join(format!("{ticker}_H1.csv"))).unwrap();
            let opens: Vec<i64> = text
                .lines()
                .map(|l| l.split(',').next().unwrap().parse().unwrap())
                .collect();
            assert_eq!(opens.len(), 365 * 24, "{venue} {ticker}");
            assert!(
                opens.windows(2).all(|w| w[1] - w[0] == 3_600_000_000_000),
                "{venue} {ticker}: every hour once, in order"
            );
        }
    }
    // The windows follow each broker's span: BCS 1440 bars (60 days), T-Bank 90 days, Finam 500 bars.
    // (counted at the imitations: the number of calls the library counts also includes the ones the limiter refused)
    assert_eq!(r.bcs_requests, 2 * 7);
    assert_eq!(r.tbank_requests.len(), 2 * 5);
    assert_eq!(r.finam_requests, 2 * 18);
    assert!(
        r.tbank_requests
            .iter()
            .all(|(_, from, to)| to - from <= 90 * 86_400)
    );
    // The small budget made the engine wait, not fail.
    assert!(r.reports.values().any(|v| v.rate_waits > 0));
    // A read refused for good ends in the report and the engine goes on; a network failure is waited out.
    assert_eq!(
        r.fatal.failed.len(),
        2,
        "both instruments are reported: {:?}",
        r.fatal
    );
    assert!(r.transient.failed.is_empty(), "{:?}", r.transient);
    assert_eq!(r.transient.bars, 2 * 365 * 24);
    // A second run finds the files complete.
    assert_eq!(r.second_run_requests, 0);

    // 2. The recorded events come back as they were.
    assert!(r.recorded >= 3, "events were recorded");
    assert_eq!(r.recorded, r.replayed);
    assert_eq!(r.live_stats, r.replay_stats);
    assert!(!r.replay_stats.books.is_empty());
}
