//! A research engine: a year of candles from three brokers within their limits, written to disk and resumable, a live
//! event stream recorded to a file, and the same stream played back from that file.
//!
//! The code of the page "Как собрать исследовательский движок" of the book (docs/src/howto-research.md). `run` works on
//! the in-process imitations of the three brokers, which, like the real ones, refuse a candle request over their span
//! and answer a series for the range asked. Nothing here touches the network or money.
//!
//! ```text
//! cargo run --example research
//! ```
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

// ANCHOR: imports
use brokapi::core::{
    Candle, Envelope, Event, InstrumentId, InstrumentQuery, InstrumentSource, MarketDataSource,
    NativeId, OrderBook, OrderBookSource, SubscribeOpts, TimeRange, Timeframe, UnixNanos,
};
use brokapi::runtime::history::{CandlePages, HistoryError, HistoryOpts};
use brokapi::runtime::record::{EnvelopeReader, EnvelopeWriter, ReplaySpeed, replay};
use brokapi::runtime::{BrokerLimiter, BrokerLimits};
use tokio::sync::mpsc;
// ANCHOR_END: imports

/// What the engine needs from a market data connector, whatever the broker is.
pub trait Market: OrderBookSource + MarketDataSource + InstrumentSource {}
impl<T: OrderBookSource + MarketDataSource + InstrumentSource> Market for T {}

/// One broker as the engine sees it: a name, a connector, and the instruments to download.
pub struct Venue<'a> {
    pub name: &'static str,
    pub md: &'a dyn Market,
    pub instruments: Vec<(String, InstrumentId)>,
}

// ANCHOR: files
/// `<dir>/<venue>/<ticker>_H1.csv`: one line per bar, `open_ts_ns,open,high,low,close,volume`. Text on purpose: any tool
/// reads it, and a prefix of the file is still a valid file.
fn bars_path(dir: &Path, venue: &str, ticker: &str, tf: Timeframe) -> PathBuf {
    dir.join(venue).join(format!("{ticker}_{tf:?}.csv"))
}

/// After a crash the last line may be cut. Drop it, so the file ends with a whole line.
fn repair(path: &Path) -> std::io::Result<()> {
    if let Ok(bytes) = fs::read(path)
        && !bytes.is_empty()
        && bytes.last() != Some(&b'\n')
    {
        let keep = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        OpenOptions::new()
            .write(true)
            .open(path)?
            .set_len(keep as u64)?;
    }
    Ok(())
}

/// Where to continue: after the last bar on disk, or at `from` if there is none. Nothing else is stored: the file is
/// its own checkpoint, so it cannot disagree with itself.
fn resume_point(path: &Path, tf: Timeframe, from: UnixNanos) -> UnixNanos {
    let last = fs::read_to_string(path).ok().and_then(|text| {
        text.lines()
            .last()
            .and_then(|l| l.split(',').next())
            .and_then(|t| t.parse::<i64>().ok())
    });
    match last {
        Some(open) => UnixNanos::from_nanos(open + tf.seconds() as i64 * 1_000_000_000),
        None => from,
    }
}

fn rows(bars: &[Candle]) -> String {
    bars.iter()
        .map(|c| {
            format!(
                "{},{},{},{},{},{}\n",
                c.open_ts.as_nanos(),
                c.open,
                c.high,
                c.low,
                c.close,
                c.volume
            )
        })
        .collect()
}
// ANCHOR_END: files

/// What one venue's download did.
#[derive(Debug, Default, Clone, Copy)]
pub struct VenueReport {
    pub requests: u32,
    pub rate_waits: u32,
    pub bars: usize,
}

// ANCHOR: download
/// Downloads `tf` bars of every instrument of the venue over `range` into files. Restart-safe: run it again after a
/// stop and it continues where each file ends. The library cuts the range into windows the broker accepts, waits out
/// the rate limit and joins the windows without duplicates; this function decides what happens to each page.
pub async fn download(
    venue: &Venue<'_>,
    tf: Timeframe,
    range: TimeRange,
    dir: &Path,
) -> Result<VenueReport, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = VenueReport::default();
    for (ticker, id) in &venue.instruments {
        let path = bars_path(dir, venue.name, ticker, tf);
        fs::create_dir_all(path.parent().expect("a file has a folder"))?;
        repair(&path)?;
        let from = resume_point(&path, tf, range.from);
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;

        let mut pages = CandlePages::new(
            venue.md,
            id,
            tf,
            TimeRange::new(from, range.to),
            HistoryOpts::default(),
        );
        loop {
            match pages.next_page().await {
                Ok(Some(bars)) => {
                    // One write per page, then to disk. The page is on disk before the next one is asked for.
                    file.write_all(rows(&bars).as_bytes())?;
                    file.sync_data()?;
                    report.bars += bars.len();
                }
                Ok(None) => break,
                // A stop in the middle: what was written stays, a rerun continues from `resume_from`. The error
                // class tells whether to retry later (RateLimited, Transport) or to fix the request (the rest).
                Err(HistoryError { error, resume_from }) => {
                    return Err(format!(
                        "{} {ticker}: stopped at {resume_from:?}: {error}",
                        venue.name
                    )
                    .into());
                }
            }
        }
        report.requests += pages.requests();
        report.rate_waits += pages.rate_waits();
    }
    Ok(report)
}
// ANCHOR_END: download

// ANCHOR: stats
/// A tiny analysis over an event stream: how many books came from which connector, and the last mid price of each.
/// The same function reads a live channel and a replay.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub books: BTreeMap<String, usize>,
    pub last_mid: BTreeMap<String, String>,
}

pub async fn analyze(mut events: mpsc::Receiver<Envelope>) -> Stats {
    let mut stats = Stats::default();
    while let Some(env) = events.recv().await {
        if let Event::OrderBook(OrderBook::Snapshot { bids, asks, .. }) = env.payload
            && let (Some(bid), Some(ask)) = (bids.first(), asks.first())
        {
            *stats.books.entry(env.source.to_string()).or_default() += 1;
            // Exact arithmetic, no floats.
            let mid = (bid.price.decimal() + ask.price.decimal()) / brokapi::core::Decimal::from(2);
            stats
                .last_mid
                .insert(env.source.to_string(), mid.to_string());
        }
    }
    stats
}
// ANCHOR_END: stats

// ANCHOR: record
/// Reads the live channel, writes every envelope to the log, and passes a copy on to the analysis. Stops after `max`
/// envelopes or when nothing comes for `quiet`.
pub async fn record(
    mut live: mpsc::Receiver<Envelope>,
    log: &Path,
    max: u64,
    quiet: Duration,
    to_analysis: mpsc::Sender<Envelope>,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let mut writer = EnvelopeWriter::new(std::io::BufWriter::new(fs::File::create(log)?))?;
    while writer.written() < max {
        let Ok(Some(env)) = tokio::time::timeout(quiet, live.recv()).await else {
            break;
        };
        writer.write(&env)?;
        let _ = to_analysis.send(env).await;
    }
    let written = writer.written();
    writer.finish()?; // flushes: the tail of the file must not stay in a buffer
    Ok(written)
}

/// Plays the log into a channel as fast as the consumer reads. Nothing is lost: a replay waits for the consumer.
pub async fn play(
    log: &Path,
    to_analysis: mpsc::Sender<Envelope>,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let reader = EnvelopeReader::new(BufReader::new(fs::File::open(log)?))?;
    let report = replay(reader, &to_analysis, ReplaySpeed::Instant).await?;
    Ok(report.sent)
}
// ANCHOR_END: record

/// A deliberately small budget, so the history download has to wait for the limiter: 200 requests per second, a burst
/// of 2. The real defaults of each broker are in `brokapi::<broker>::limits::default_limits()`.
fn small_budget() -> std::sync::Arc<BrokerLimiter> {
    let raw = r#"
[demo.buckets.market]
rate = 200
per = "1s"
burst = 2
[demo.route]
"*" = ["market"]
[demo.conns]
max_conns = 4
max_subs_per_conn = 50
supports_multiplex = false
"#;
    BrokerLimiter::new(&BrokerLimits::from_toml_str(raw).expect("valid limits"))
}

/// What the test looks at.
#[derive(Debug)]
pub struct Result3 {
    pub reports: BTreeMap<&'static str, VenueReport>,
    pub second_run_requests: u32,
    pub recorded: u64,
    pub replayed: u64,
    pub live_stats: Stats,
    pub replay_stats: Stats,
    pub tbank_requests: Vec<(i32, i64, i64)>,
    pub bcs_requests: usize,
    pub finam_requests: usize,
}

/// The whole engine on the imitations. `dir` holds the data files and the log.
pub async fn run(dir: &Path) -> Result<Result3, Box<dyn std::error::Error + Send + Sync>> {
    use brokapi::{bcs, finam, tbank};
    use brokapi_mocks::{bcs as mbcs, finam as mfinam, tbank as mtbank};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    // Imitations that answer a series for the asked range and refuse a range over the broker's span.
    let bcs_opts = Arc::new(mbcs::MockOptions::default());
    bcs_opts.synthetic_candles.store(true, Ordering::SeqCst);
    let bcs_server = mbcs::MockServer::start(bcs_opts.clone()).await;
    let tbank_opts = Arc::new(mtbank::MockOptions::default());
    tbank_opts.synthetic_candles.store(true, Ordering::SeqCst);
    let tbank_server = mtbank::MockServer::start(tbank_opts.clone()).await;
    let finam_server = mfinam::MockServer::start().await;
    finam_server
        .state
        .options
        .synthetic_candles
        .store(true, Ordering::SeqCst);

    // One channel for the live events of all three market data connectors.
    let (tx, live) = mpsc::channel::<Envelope>(4096);
    let finam_api =
        finam::FinamApiConfig::new(mfinam::SECRET).with_endpoint(finam_server.url.clone());
    let bcs_md = bcs::MarketDataConnector::start(
        bcs::MarketDataConfig::new(
            bcs::protocol::config::BcsConfig::for_mock("mock-refresh-token", &bcs_server.http_base),
            "bcs-md",
        ),
        small_budget(),
        tx.clone(),
    )
    .await?;
    let tbank_md = tbank::MarketDataConnector::start(
        tbank::MarketDataConfig::new(
            tbank::protocol::TbankConfig::for_mock(mtbank::TOKEN, &tbank_server.endpoint),
            "tbank-md",
        ),
        small_budget(),
        tx.clone(),
    )
    .await?;
    let finam_md = finam::MarketDataConnector::start(
        finam::MarketDataConfig::new(finam_api, "finam-md"),
        small_budget(),
        tx,
    )
    .await?;

    // The instruments of each venue in its own form. In real code all of them come from `find`.
    let bcs_id = |t: &str| {
        InstrumentId::new(
            brokapi::core::InstrumentKey::new(t, "TQBR"),
            NativeId::TickerClass {
                ticker: t.into(),
                class_code: "TQBR".into(),
            },
        )
    };
    let mut tbank_ids = Vec::new();
    for t in ["SBER", "GAZP"] {
        let found = tbank_md
            .find(InstrumentQuery::ByTicker {
                ticker: t.into(),
                exchange: Some("TQBR".into()),
            })
            .await?;
        tbank_ids.push((
            t.to_string(),
            found.into_iter().next().ok_or("not in the directory")?.id,
        ));
    }
    let venues = vec![
        Venue {
            name: "bcs",
            md: &bcs_md,
            instruments: ["SBER", "GAZP"]
                .map(|t| (t.to_string(), bcs_id(t)))
                .to_vec(),
        },
        Venue {
            name: "tbank",
            md: &tbank_md,
            instruments: tbank_ids,
        },
        Venue {
            name: "finam",
            md: &finam_md,
            instruments: ["SBER", "GAZP"]
                .map(|t| {
                    (
                        t.to_string(),
                        finam::util::instrument_from_symbol(&format!("{t}@MISX")),
                    )
                })
                .to_vec(),
        },
    ];

    // ---- 1. A year of hourly bars. The three brokers are independent (own limits), so they run side by side.
    let year = TimeRange::new(
        UnixNanos::from_nanos(1_767_225_600_000_000_000), // 2026-01-01
        UnixNanos::from_nanos(1_798_761_600_000_000_000), // 2027-01-01
    );
    let (r_bcs, r_tbank, r_finam) = tokio::try_join!(
        download(&venues[0], Timeframe::H1, year, dir),
        download(&venues[1], Timeframe::H1, year, dir),
        download(&venues[2], Timeframe::H1, year, dir),
    )?;
    let mut reports = BTreeMap::new();
    reports.insert("bcs", r_bcs);
    reports.insert("tbank", r_tbank);
    reports.insert("finam", r_finam);

    // A second run finds every file complete and asks the brokers for nothing.
    let again = download(&venues[0], Timeframe::H1, year, dir).await?;

    // ---- 2. The deals of the tape: the latest ones, to a file of their own.
    for v in &venues {
        let (ticker, id) = &v.instruments[0];
        let trades = v.md.last_trades(id, 50).await?;
        let mut out = String::new();
        for t in &trades {
            out.push_str(&format!(
                "{},{},{},{:?}\n",
                t.ts.rx_ts.as_nanos(),
                t.price,
                t.qty,
                t.side
            ));
        }
        fs::write(dir.join(v.name).join(format!("{ticker}_trades.csv")), out)?;
    }

    // ---- 3. Record the live stream while an analysis reads the same events.
    for v in &venues {
        let (_, id) = &v.instruments[0];
        v.md.subscribe_order_book(id, SubscribeOpts::conflate())
            .await?;
    }
    let (tee_tx, tee_rx) = mpsc::channel::<Envelope>(4096);
    let analysis = tokio::spawn(analyze(tee_rx));
    let log = dir.join("events.log");
    let recorded = record(live, &log, 30, Duration::from_secs(3), tee_tx).await?;
    let live_stats = analysis.await?;

    // ---- 4. The same events again, from the file.
    let (play_tx, play_rx) = mpsc::channel::<Envelope>(16);
    let analysis = tokio::spawn(analyze(play_rx));
    let replayed = play(&log, play_tx).await?;
    let replay_stats = analysis.await?;

    let tbank_requests = tbank_opts.candle_requests.lock().unwrap().clone();
    let bcs_requests = bcs_opts.candle_requests.lock().unwrap().len();
    let finam_requests = finam_server
        .state
        .options
        .bars_requests
        .lock()
        .unwrap()
        .len();
    // Stop the connectors (the borrows of `venues` end here).
    drop(venues);
    let stop = brokapi::core::ShutdownMode::Immediate;
    bcs_md.shutdown(stop).await;
    tbank_md.shutdown(stop).await;
    finam_md.shutdown(stop).await;
    Ok(Result3 {
        reports,
        second_run_requests: again.requests,
        recorded,
        replayed,
        live_stats,
        replay_stats,
        tbank_requests,
        bcs_requests,
        finam_requests,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = std::env::temp_dir().join("brokapi-research");
    let r = run(&dir).await?;
    println!("{:#?}", r.reports);
    Ok(())
}
