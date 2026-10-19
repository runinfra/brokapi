//! Writing the event stream to a file and playing it back.
//!
//! A research program needs the same events twice: once live, to record, and later from a file, as
//! many times as it takes to debug a strategy. This module does the file part with no dependencies
//! beyond JSON. The format is one JSON line per [`Envelope`], after a header line that names the
//! format and the schema of the envelopes:
//!
//! ```text
//! {"brokapi_log":1,"envelope_schema":1}
//! {"schema":1,"source":"bcs-md","sub":1,"ts":{...},"payload":{"OrderBook":{...}}}
//! ```
//!
//! | Need | Item |
//! |---|---|
//! | Append envelopes to a file | [`EnvelopeWriter`] |
//! | Read them back in order | [`EnvelopeReader`] |
//! | Feed them to a channel, as fast as possible or at the recorded pace | [`replay`], [`ReplaySpeed`] |
//!
//! What the file keeps and what it does not:
//! - Prices, quantities and money are written as decimal strings, so they come back exactly.
//! - The monotonic clock [`Timestamps::rx_mono`](brokapi_core::Timestamps::rx_mono) is not stored. A
//!   replayed envelope carries the time of reading in it. Use `rx_ts` and `broker_ts` to order and
//!   measure replayed data, never `rx_mono`.
//! - A line that cannot be read stops the read with an error that names the line. A reader never
//!   skips a bad line silently.
//! - An envelope with a `schema` newer than this build understands is refused
//!   ([`LogError::Schema`]), as the rules of [`ENVELOPE_SCHEMA`] ask.
//!
//! This is a log of a stream, not a catalog: there is no index by instrument or time, so filter
//! while you read. Compare NautilusTrader, whose data catalog keeps typed data in Parquet files and
//! queries it by instrument and time: a catalog is the right tool for years of data, this log is
//! the smallest thing that records a live run and plays it back.
//!
//! # Examples
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Instant;
//! use brokapi_core::{Envelope, Event, Timestamps, UnixNanos};
//! use brokapi_runtime::record::{EnvelopeReader, EnvelopeWriter};
//!
//! let ts = Timestamps::new(UnixNanos::from_nanos(7), Instant::now());
//! let env = Envelope::new(Arc::from("bcs-md"), ts, Event::Balances(vec![]));
//!
//! let mut writer = EnvelopeWriter::new(Vec::new())?;
//! writer.write(&env)?;
//! let bytes = writer.finish()?;
//!
//! let back: Vec<Envelope> = EnvelopeReader::new(bytes.as_slice())?.collect::<Result<_, _>>()?;
//! assert_eq!(back, vec![env]);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fmt;
use std::io::{self, BufRead, Write};
use std::time::Duration;

use brokapi_core::{ENVELOPE_SCHEMA, Envelope};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// The version of the file format. It is in the header line of every file.
pub const LOG_FORMAT: u32 = 1;

#[derive(Serialize, Deserialize)]
struct Header {
    brokapi_log: u32,
    envelope_schema: u16,
}

/// Why a log could not be written or read.
///
/// # Examples
///
/// ```
/// use brokapi_runtime::record::{EnvelopeReader, LogError};
///
/// let err = EnvelopeReader::new(&b"not a log\n"[..]).err().expect("refused");
/// assert!(matches!(err, LogError::Format { line: 1, .. }));
/// ```
#[derive(Debug)]
pub enum LogError {
    /// The file or the stream failed. Check the disk and the path.
    Io(io::Error),
    /// A line is not what the format says. The file is damaged or is not a brokapi log. `line` counts
    /// from 1, the header is line 1.
    Format {
        /// The number of the bad line.
        line: u64,
        /// What is wrong.
        message: String,
    },
    /// The file was written by a newer version. Update the library to read it.
    Schema {
        /// What the file declares.
        found: u32,
        /// The newest this build reads.
        supported: u32,
    },
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "log I/O error: {e}"),
            Self::Format { line, message } => write!(f, "log line {line}: {message}"),
            Self::Schema { found, supported } => write!(
                f,
                "log has format or schema {found}, this build reads up to {supported}"
            ),
        }
    }
}

impl std::error::Error for LogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for LogError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// Appends envelopes to any [`Write`]: a file, a buffer, a compressor.
///
/// Wrap a file in [`std::io::BufWriter`] yourself when you write many envelopes: the writer does not
/// buffer. [`flush`](Self::flush) after the last write, or [`finish`](Self::finish), so the tail
/// is not left in a buffer when the program ends.
///
/// # Examples
///
/// See the module [`record`](crate::record).
pub struct EnvelopeWriter<W: Write> {
    out: W,
    written: u64,
}

impl<W: Write> EnvelopeWriter<W> {
    /// Starts a new log in `out`: writes the header line.
    ///
    /// # Errors
    ///
    /// [`LogError::Io`] when the header cannot be written.
    pub fn new(mut out: W) -> Result<Self, LogError> {
        let header = Header {
            brokapi_log: LOG_FORMAT,
            envelope_schema: ENVELOPE_SCHEMA,
        };
        serde_json::to_writer(&mut out, &header).map_err(json_io)?;
        out.write_all(b"\n")?;
        Ok(Self { out, written: 0 })
    }

    /// Appends one envelope as one line.
    ///
    /// # Errors
    ///
    /// [`LogError::Io`] when the write fails. The line may be half written then: do not append to
    /// the same file after an error, start a new one.
    pub fn write(&mut self, envelope: &Envelope) -> Result<(), LogError> {
        serde_json::to_writer(&mut self.out, envelope).map_err(json_io)?;
        self.out.write_all(b"\n")?;
        self.written += 1;
        Ok(())
    }

    /// How many envelopes were written (the header is not counted).
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Flushes the underlying writer.
    ///
    /// # Errors
    ///
    /// [`LogError::Io`] when the flush fails.
    pub fn flush(&mut self) -> Result<(), LogError> {
        Ok(self.out.flush()?)
    }

    /// Flushes and gives the underlying writer back.
    ///
    /// # Errors
    ///
    /// [`LogError::Io`] when the flush fails.
    pub fn finish(mut self) -> Result<W, LogError> {
        self.out.flush()?;
        Ok(self.out)
    }
}

fn json_io(e: serde_json::Error) -> LogError {
    LogError::Io(io::Error::other(e))
}

/// Reads envelopes back from any [`BufRead`], in the order they were written.
///
/// An iterator of `Result<Envelope, LogError>`. After the first error the iterator ends: what
/// follows a bad line cannot be trusted to be in order.
///
/// # Examples
///
/// See the module [`record`](crate::record).
pub struct EnvelopeReader<R: BufRead> {
    input: R,
    line: u64,
    failed: bool,
    buf: String,
}

impl<R: BufRead> EnvelopeReader<R> {
    /// Opens a log: reads and checks the header line.
    ///
    /// # Errors
    ///
    /// - [`LogError::Format`] when the first line is not a log header (an empty file included).
    /// - [`LogError::Schema`] when the file is of a newer format or holds newer envelopes.
    /// - [`LogError::Io`] when the read fails.
    pub fn new(mut input: R) -> Result<Self, LogError> {
        let mut first = String::new();
        if input.read_line(&mut first)? == 0 {
            return Err(LogError::Format {
                line: 1,
                message: "the file is empty, a log starts with a header line".into(),
            });
        }
        let header: Header =
            serde_json::from_str(first.trim_end()).map_err(|e| LogError::Format {
                line: 1,
                message: format!("not a log header: {e}"),
            })?;
        if header.brokapi_log > LOG_FORMAT {
            return Err(LogError::Schema {
                found: header.brokapi_log,
                supported: LOG_FORMAT,
            });
        }
        if header.envelope_schema > ENVELOPE_SCHEMA {
            return Err(LogError::Schema {
                found: u32::from(header.envelope_schema),
                supported: u32::from(ENVELOPE_SCHEMA),
            });
        }
        Ok(Self {
            input,
            line: 1,
            failed: false,
            buf: String::new(),
        })
    }
}

impl<R: BufRead> Iterator for EnvelopeReader<R> {
    type Item = Result<Envelope, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            self.buf.clear();
            match self.input.read_line(&mut self.buf) {
                Ok(0) => return None,
                Ok(_) => {}
                Err(e) => {
                    self.failed = true;
                    return Some(Err(e.into()));
                }
            }
            self.line += 1;
            let text = self.buf.trim_end();
            if text.is_empty() {
                continue;
            }
            return Some(match serde_json::from_str::<Envelope>(text) {
                Ok(env) if env.schema > ENVELOPE_SCHEMA => {
                    self.failed = true;
                    Err(LogError::Schema {
                        found: u32::from(env.schema),
                        supported: u32::from(ENVELOPE_SCHEMA),
                    })
                }
                Ok(env) => Ok(env),
                Err(e) => {
                    self.failed = true;
                    Err(LogError::Format {
                        line: self.line,
                        message: e.to_string(),
                    })
                }
            });
        }
    }
}

/// How fast [`replay`] plays.
///
/// # Examples
///
/// ```
/// use brokapi_runtime::record::ReplaySpeed;
///
/// assert_eq!(ReplaySpeed::default(), ReplaySpeed::Instant);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ReplaySpeed {
    /// As fast as the consumer reads. For backtests and tests.
    #[default]
    Instant,
    /// At the recorded pace divided by `factor`: `1.0` is real time, `10.0` is ten times faster.
    /// The pace is the difference of `rx_ts` of neighbouring envelopes. A pause is never longer
    /// than `max_gap`, so a night between two sessions does not stop the replay.
    Paced {
        /// The speed-up. Must be above zero.
        factor: f64,
        /// The longest single pause.
        max_gap: Duration,
    },
}

/// What a finished [`replay`] did.
///
/// # Examples
///
/// ```
/// use brokapi_runtime::record::ReplayReport;
///
/// let r = ReplayReport::default();
/// assert_eq!((r.sent, r.stopped_by_consumer), (0, false));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayReport {
    /// Envelopes sent into the channel.
    pub sent: u64,
    /// `true` when the replay ended because the receiver was dropped, not because the log ended.
    pub stopped_by_consumer: bool,
}

/// Sends envelopes into a channel, in order, like a connector would.
///
/// The difference from a live connector is the one that matters for research: `replay` waits for
/// room in the channel ([`mpsc::Sender::send`]), so a slow consumer slows the replay down and no
/// event is lost. A live connector cannot wait and applies the loss policy of the subscription
/// instead. Code that handles `Overflow` therefore needs its own test: a replay never produces one.
///
/// The envelopes keep their `source` and `sub`, so the consumer routes them as it did live.
///
/// # Errors
///
/// The first [`LogError`] from `envelopes`. Envelopes sent before it stay sent.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Instant;
/// use brokapi_core::{Envelope, Event, Timestamps, UnixNanos};
/// use brokapi_runtime::record::{ReplaySpeed, replay};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let ts = |n| Timestamps::new(UnixNanos::from_nanos(n), Instant::now());
/// let log = vec![
///     Envelope::new(Arc::from("bcs-md"), ts(1), Event::Balances(vec![])),
///     Envelope::new(Arc::from("bcs-md"), ts(2), Event::Balances(vec![])),
/// ];
/// let (tx, mut rx) = tokio::sync::mpsc::channel(8);
/// let report = replay(log.into_iter().map(Ok), &tx, ReplaySpeed::Instant).await?;
/// assert_eq!(report.sent, 2);
/// assert!(rx.recv().await.is_some());
/// # Ok(()) }
/// ```
pub async fn replay<I>(
    envelopes: I,
    tx: &mpsc::Sender<Envelope>,
    speed: ReplaySpeed,
) -> Result<ReplayReport, LogError>
where
    I: IntoIterator<Item = Result<Envelope, LogError>>,
{
    let mut report = ReplayReport::default();
    let mut previous: Option<i64> = None;
    for item in envelopes {
        let env = item?;
        if let ReplaySpeed::Paced { factor, max_gap } = speed {
            let now = env.ts.rx_ts.as_nanos();
            if let Some(prev) = previous {
                let gap = now.saturating_sub(prev).max(0) as u64;
                if factor > 0.0 && gap > 0 {
                    let pause = Duration::from_nanos((gap as f64 / factor) as u64).min(max_gap);
                    tokio::time::sleep(pause).await;
                }
            }
            previous = Some(now);
        }
        if tx.send(env).await.is_err() {
            report.stopped_by_consumer = true;
            return Ok(report);
        }
        report.sent += 1;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use brokapi_core::{
        BookLevel, Event, InstrumentId, InstrumentKey, NativeId, OrderBook, Price, Qty,
        SubscriptionId, Timestamps, UnixNanos,
    };

    use super::*;

    fn instrument() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        )
    }

    fn book_env(n: i64, price: &str) -> Envelope {
        let ts = Timestamps::new(UnixNanos::from_nanos(n), Instant::now())
            .with_broker_ts(UnixNanos::from_nanos(n - 1));
        let level = BookLevel {
            price: price.parse::<Price>().unwrap(),
            qty: Qty::from(10),
        };
        Envelope::new(
            Arc::from("bcs-md"),
            ts,
            Event::OrderBook(OrderBook::Snapshot {
                instrument: instrument(),
                bids: vec![level.clone()],
                asks: vec![],
                ts,
            }),
        )
        .with_sub(SubscriptionId::new(3))
    }

    fn write_all(envs: &[Envelope]) -> Vec<u8> {
        let mut w = EnvelopeWriter::new(Vec::new()).unwrap();
        for e in envs {
            w.write(e).unwrap();
        }
        assert_eq!(w.written(), envs.len() as u64);
        w.finish().unwrap()
    }

    #[test]
    fn a_round_trip_keeps_every_field_and_the_exact_decimals() {
        let envs = vec![book_env(10, "244.30"), book_env(20, "0.0001")];
        let bytes = write_all(&envs);
        let back: Vec<Envelope> = EnvelopeReader::new(bytes.as_slice())
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(back, envs);
        // 244.30 is not 244.3 as text: the trailing zero survives.
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("\"244.30\""), "{text}");
    }

    #[test]
    fn a_log_starts_with_a_header() {
        let bytes = write_all(&[]);
        let first = String::from_utf8(bytes).unwrap();
        assert_eq!(first.trim_end(), r#"{"brokapi_log":1,"envelope_schema":1}"#);
    }

    #[test]
    fn an_empty_file_and_a_foreign_file_are_refused() {
        let empty = EnvelopeReader::new(&b""[..]).err().unwrap();
        assert!(matches!(empty, LogError::Format { line: 1, .. }));
        let foreign = EnvelopeReader::new(&b"{\"a\":1}\n"[..]).err().unwrap();
        assert!(matches!(foreign, LogError::Format { line: 1, .. }));
    }

    #[test]
    fn a_newer_format_or_schema_is_refused() {
        let newer_log = br#"{"brokapi_log":2,"envelope_schema":1}"#;
        let err = EnvelopeReader::new(&newer_log[..]).err().unwrap();
        assert!(matches!(
            err,
            LogError::Schema {
                found: 2,
                supported: 1
            }
        ));
        let newer_env = br#"{"brokapi_log":1,"envelope_schema":9}"#;
        assert!(matches!(
            EnvelopeReader::new(&newer_env[..]).err().unwrap(),
            LogError::Schema { found: 9, .. }
        ));
    }

    #[test]
    fn a_bad_line_stops_the_read_and_names_the_line() {
        let mut bytes = write_all(&[book_env(1, "1")]);
        bytes.extend_from_slice(b"{broken\n");
        bytes.extend_from_slice(&write_all(&[book_env(2, "2")])[39..]);
        let mut reader = EnvelopeReader::new(bytes.as_slice()).unwrap();
        assert!(reader.next().unwrap().is_ok());
        let err = reader.next().unwrap().unwrap_err();
        assert!(matches!(err, LogError::Format { line: 3, .. }), "{err}");
        assert!(reader.next().is_none(), "nothing after a bad line");
    }

    #[test]
    fn blank_lines_are_skipped() {
        let mut bytes = write_all(&[book_env(1, "1")]);
        bytes.extend_from_slice(b"\n\n");
        let n = EnvelopeReader::new(bytes.as_slice()).unwrap().count();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn replay_sends_in_order_and_waits_for_a_slow_consumer() {
        let envs: Vec<Envelope> = (1..=50).map(|n| book_env(n, "1")).collect();
        let bytes = write_all(&envs);
        let (tx, mut rx) = mpsc::channel(2); // far smaller than the log
        let reader = EnvelopeReader::new(bytes.as_slice()).unwrap();
        let sender = async { replay(reader, &tx, ReplaySpeed::Instant).await };
        let receiver = async {
            let mut got = Vec::new();
            while got.len() < 50 {
                got.push(rx.recv().await.unwrap());
                tokio::task::yield_now().await;
            }
            got
        };
        let (report, got) = tokio::join!(sender, receiver);
        assert_eq!(report.unwrap().sent, 50);
        assert_eq!(got, envs, "nothing lost, order kept");
    }

    #[tokio::test]
    async fn replay_stops_quietly_when_the_consumer_goes_away() {
        let envs: Vec<Envelope> = (1..=5).map(|n| book_env(n, "1")).collect();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let report = replay(envs.into_iter().map(Ok), &tx, ReplaySpeed::Instant)
            .await
            .unwrap();
        assert!(report.stopped_by_consumer);
        assert_eq!(report.sent, 0);
    }

    #[tokio::test]
    async fn replay_passes_a_read_error_on() {
        let items: Vec<Result<Envelope, LogError>> = vec![
            Ok(book_env(1, "1")),
            Err(LogError::Format {
                line: 3,
                message: "x".into(),
            }),
        ];
        let (tx, mut rx) = mpsc::channel(8);
        let err = replay(items, &tx, ReplaySpeed::Instant).await.unwrap_err();
        assert!(matches!(err, LogError::Format { line: 3, .. }));
        assert!(rx.recv().await.is_some(), "what came before is sent");
    }

    #[tokio::test]
    async fn paced_replay_waits_and_cuts_long_gaps() {
        // 200 ms recorded, 100x faster: about 2 ms. A gap of one hour is cut to max_gap.
        let envs = vec![
            book_env(0, "1"),
            book_env(200_000_000, "1"),
            book_env(3_600_000_000_000, "1"),
        ];
        let (tx, mut rx) = mpsc::channel(8);
        let started = Instant::now();
        let speed = ReplaySpeed::Paced {
            factor: 100.0,
            max_gap: Duration::from_millis(30),
        };
        replay(envs.into_iter().map(Ok), &tx, speed).await.unwrap();
        let took = started.elapsed();
        assert!(
            took >= Duration::from_millis(30),
            "the long gap waits: {took:?}"
        );
        assert!(took < Duration::from_secs(2), "but is cut: {took:?}");
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        assert_eq!(n, 3);
    }
}
