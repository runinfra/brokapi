//! Reading a long candle history in windows that the broker accepts, within the rate limit.
//!
//! One `candles` request covers a limited span (BCS: 1440 bars, T-Bank: from a day for minute bars to
//! ten years for monthly ones). A year of hourly bars is therefore several requests, and each one
//! spends quota. [`CandlePages`] cuts the range into windows, reads them one after another, waits out
//! [`Error::RateLimited`], repeats a read that failed on the network, joins the edges of neighbouring
//! windows without duplicates and always knows where to start again ([`resume_from`](CandlePages::resume_from)).
//!
//! Only reads are repeated here. A candle read changes nothing at the broker, so a repeat is safe.
//! Commands are never repeated anywhere in the library.
//!
//! The helper is written against [`MarketDataSource`], so it works with every connector. The span of
//! a window comes from [`MarketDataSource::candles_max_span`] unless [`HistoryOpts::window`] says
//! otherwise.
//!
//! # Examples
//!
//! ```
//! # use brokapi_core::*;
//! # use brokapi_runtime::history::{CandlePages, HistoryOpts};
//! # async fn demo(md: &impl MarketDataSource, sber: &InstrumentId) -> Result<(), Box<dyn std::error::Error>> {
//! let year = TimeRange::new(
//!     UnixNanos::from_nanos(1_767_225_600_000_000_000), // 2026-01-01
//!     UnixNanos::from_nanos(1_798_761_600_000_000_000), // 2027-01-01
//! );
//! let mut pages = CandlePages::new(md, sber, Timeframe::H1, year, HistoryOpts::default());
//! while let Some(page) = pages.next_page().await? {
//!     // Write the page to disk here. After a crash start again from `pages.resume_from()`.
//!     println!("{} bars, next window starts at {:?}", page.len(), pages.resume_from());
//! }
//! # Ok(()) }
//! ```

use std::fmt;
use std::time::Duration;

use brokapi_core::{
    Candle, Error, InstrumentId, MarketDataSource, TimeRange, Timeframe, UnixNanos,
};

/// How [`CandlePages`] cuts the range and how patient it is.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_runtime::history::HistoryOpts;
///
/// let opts = HistoryOpts { max_rate_waits: 20, ..HistoryOpts::default() };
/// assert_eq!(opts.window, None);
/// assert_eq!(opts.max_rate_wait, Duration::from_secs(60));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryOpts {
    /// The span of one request. `None`: take [`MarketDataSource::candles_max_span`], and when the
    /// connector does not know it, read the whole range in one request.
    pub window: Option<Duration>,
    /// How many times one window waits out [`Error::RateLimited`] before the error is returned.
    /// Default 10.
    pub max_rate_waits: u32,
    /// The longest single pause on [`Error::RateLimited`]. The pause is the `retry_after` of the
    /// error, cut to this value. Default 60 seconds.
    pub max_rate_wait: Duration,
    /// How many times one window is read again after [`Error::Transport`]. Default 2.
    pub transport_retries: u32,
    /// The pause before the first repeat after [`Error::Transport`]. It doubles for each next
    /// repeat. Default 1 second.
    pub transport_backoff: Duration,
}

impl Default for HistoryOpts {
    fn default() -> Self {
        Self {
            window: None,
            max_rate_waits: 10,
            max_rate_wait: Duration::from_secs(60),
            transport_retries: 2,
            transport_backoff: Duration::from_secs(1),
        }
    }
}

/// A history read stopped. Carries the error and the place to start from again.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Error, UnixNanos};
/// use brokapi_runtime::history::HistoryError;
///
/// let e = HistoryError { error: Error::Unknown, resume_from: UnixNanos::ZERO };
/// assert!(e.to_string().contains("history read stopped"));
/// ```
#[derive(Debug, Clone)]
pub struct HistoryError {
    /// What stopped the read. [`Error::class`] says whether it makes sense to try again later.
    pub error: Error,
    /// The start of the window that failed. Everything before it was delivered. Start a new
    /// [`CandlePages`] with a range from here and nothing is read twice.
    pub resume_from: UnixNanos,
}

impl fmt::Display for HistoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "history read stopped at {} ns: {}",
            self.resume_from.as_nanos(),
            self.error
        )
    }
}

impl std::error::Error for HistoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// A reader of candle history, one window per [`next_page`](Self::next_page) call.
///
/// Pages come in time order. Inside the whole read every bar is delivered once: a bar that two
/// neighbouring windows both return (brokers differ on whether the end of a range is included) is
/// kept in the first. A page can be empty (a weekend, a halt): that is not the end, the end is `None`.
///
/// The type borrows the connector and the instrument and holds no network state of its own, so a
/// crash loses only the progress you did not save. Save [`resume_from`](Self::resume_from) after
/// each page you wrote.
///
/// # Examples
///
/// See the module [`history`](crate::history).
pub struct CandlePages<'a, S: MarketDataSource + ?Sized> {
    src: &'a S,
    id: &'a InstrumentId,
    tf: Timeframe,
    next_from: UnixNanos,
    to: UnixNanos,
    window: Option<Duration>,
    opts: HistoryOpts,
    last_open: Option<UnixNanos>,
    requests: u32,
    rate_waits: u32,
}

impl<'a, S: MarketDataSource + ?Sized> CandlePages<'a, S> {
    /// Prepares a read of `tf` bars of `id` over `range`. Nothing is sent yet.
    pub fn new(
        src: &'a S,
        id: &'a InstrumentId,
        tf: Timeframe,
        range: TimeRange,
        opts: HistoryOpts,
    ) -> Self {
        let window = opts.window.or_else(|| src.candles_max_span(tf));
        Self {
            src,
            id,
            tf,
            next_from: range.from,
            to: range.to,
            window,
            opts,
            last_open: None,
            requests: 0,
            rate_waits: 0,
        }
    }

    /// The start of the next window: where to begin again after a stop. Equals the end of the range
    /// when everything is read.
    pub fn resume_from(&self) -> UnixNanos {
        self.next_from
    }

    /// How many `candles` requests were made, repeats included.
    pub fn requests(&self) -> u32 {
        self.requests
    }

    /// How many times a rate limit was waited out.
    pub fn rate_waits(&self) -> u32 {
        self.rate_waits
    }

    /// Reads the next window. `Ok(None)` when the range is finished.
    ///
    /// # Errors
    ///
    /// [`HistoryError`] with the error that stopped the read and `resume_from`:
    /// - [`Error::RateLimited`] after [`HistoryOpts::max_rate_waits`] pauses: the quota is spent by
    ///   others too. Wait longer and start again from `resume_from`.
    /// - [`Error::Transport`] after [`HistoryOpts::transport_retries`] repeats: the broker is
    ///   unreachable. Start again later.
    /// - [`Error::Rejected`], [`Error::Config`], [`Error::Unsupported`]: the same read fails again.
    ///   Check the instrument, the timeframe and the window. Do not loop.
    pub async fn next_page(&mut self) -> Result<Option<Vec<Candle>>, HistoryError> {
        if self.next_from >= self.to {
            return Ok(None);
        }
        let from = self.next_from;
        let end = match self.window {
            Some(w) => {
                let step = i64::try_from(w.as_nanos()).unwrap_or(i64::MAX).max(1);
                UnixNanos::from_nanos(from.as_nanos().saturating_add(step).min(self.to.as_nanos()))
            }
            None => self.to,
        };
        let mut bars = self.read_window(TimeRange::new(from, end)).await?;
        bars.sort_by_key(|c| c.open_ts);
        if let Some(last) = self.last_open {
            bars.retain(|c| c.open_ts > last);
        }
        bars.dedup_by_key(|c| c.open_ts);
        if let Some(c) = bars.last() {
            self.last_open = Some(c.open_ts);
        }
        self.next_from = end;
        Ok(Some(bars))
    }

    /// One window with its patience: pauses on a rate limit, repeats on a network error.
    async fn read_window(&mut self, range: TimeRange) -> Result<Vec<Candle>, HistoryError> {
        let mut waits = 0u32;
        let mut retries = 0u32;
        loop {
            self.requests += 1;
            match self.src.candles(self.id, self.tf, range).await {
                Ok(bars) => return Ok(bars),
                Err(Error::RateLimited { retry_after }) if waits < self.opts.max_rate_waits => {
                    waits += 1;
                    self.rate_waits += 1;
                    tokio::time::sleep(retry_after.min(self.opts.max_rate_wait)).await;
                }
                Err(Error::Transport(_)) if retries < self.opts.transport_retries => {
                    let pause = self.opts.transport_backoff * 2u32.saturating_pow(retries);
                    retries += 1;
                    tokio::time::sleep(pause).await;
                }
                Err(error) => {
                    return Err(HistoryError {
                        error,
                        resume_from: range.from,
                    });
                }
            }
        }
    }
}

/// Reads the whole range into memory: [`CandlePages`] until the end.
///
/// Suits a few thousand bars. For a year of minute bars of many instruments write page by page
/// instead, so memory does not grow.
///
/// # Errors
///
/// As [`CandlePages::next_page`]. Bars read before the error are lost with it: use
/// [`CandlePages`] when you need them.
///
/// # Examples
///
/// ```
/// # use brokapi_core::*;
/// # use brokapi_runtime::history::{load_candles, HistoryOpts};
/// # async fn demo(md: &impl MarketDataSource, sber: &InstrumentId, range: TimeRange) -> Result<(), Box<dyn std::error::Error>> {
/// let bars = load_candles(md, sber, Timeframe::D1, range, HistoryOpts::default()).await?;
/// assert!(bars.windows(2).all(|w| w[0].open_ts < w[1].open_ts));
/// # Ok(()) }
/// ```
pub async fn load_candles<S: MarketDataSource + ?Sized>(
    src: &S,
    id: &InstrumentId,
    tf: Timeframe,
    range: TimeRange,
    opts: HistoryOpts,
) -> Result<Vec<Candle>, HistoryError> {
    let mut pages = CandlePages::new(src, id, tf, range, opts);
    let mut all = Vec::new();
    while let Some(mut page) = pages.next_page().await? {
        all.append(&mut page);
    }
    Ok(all)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;
    use brokapi_core::{
        InstrumentKey, MarketTrade, NativeId, Price, Qty, SubscribeOpts, SubscriptionId, Timestamps,
    };

    use super::*;

    /// A source with one bar per hour, a rate limit on the first calls and an optional window cap.
    struct Fake {
        calls: Mutex<Vec<TimeRange>>,
        rate_limited_first: AtomicU32,
        transport_first: AtomicU32,
        max_span: Option<Duration>,
        /// A bar at the very end of a range is returned too, like a broker that includes the end.
        inclusive_end: bool,
    }

    impl Fake {
        fn new(max_span: Option<Duration>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                rate_limited_first: AtomicU32::new(0),
                transport_first: AtomicU32::new(0),
                max_span,
                inclusive_end: false,
            }
        }
    }

    fn id() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        )
    }

    const HOUR: i64 = 3_600_000_000_000;

    fn bar(open: i64) -> Candle {
        Candle {
            instrument: id(),
            timeframe: Timeframe::H1,
            open: Price::from(1),
            high: Price::from(2),
            low: Price::from(1),
            close: Price::from(2),
            volume: Qty::from(1),
            open_ts: UnixNanos::from_nanos(open),
            close_ts: UnixNanos::from_nanos(open + HOUR),
            ts: Timestamps::new(UnixNanos::ZERO, std::time::Instant::now()),
        }
    }

    #[async_trait]
    impl MarketDataSource for Fake {
        async fn last_trades(&self, _: &InstrumentId, _: u32) -> Result<Vec<MarketTrade>, Error> {
            unimplemented!()
        }
        async fn subscribe_market_trades(
            &self,
            _: &InstrumentId,
            _: SubscribeOpts,
        ) -> Result<SubscriptionId, Error> {
            unimplemented!()
        }
        async fn subscribe_quotes(
            &self,
            _: &InstrumentId,
            _: SubscribeOpts,
        ) -> Result<SubscriptionId, Error> {
            unimplemented!()
        }
        async fn candles(
            &self,
            _: &InstrumentId,
            _: Timeframe,
            range: TimeRange,
        ) -> Result<Vec<Candle>, Error> {
            if let Some(max) = self.max_span {
                let span = (range.to.as_nanos() - range.from.as_nanos()) as u128;
                if span > max.as_nanos() {
                    return Err(Error::Rejected {
                        code: "span".into(),
                        message: "window too long".into(),
                    });
                }
            }
            if self
                .rate_limited_first
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(Error::RateLimited {
                    retry_after: Duration::from_millis(5),
                });
            }
            if self
                .transport_first
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(Error::Transport(brokapi_core::TransportError::Message(
                    "reset".into(),
                )));
            }
            self.calls.lock().unwrap().push(range);
            let mut open = (range.from.as_nanos() + HOUR - 1) / HOUR * HOUR;
            let mut out = Vec::new();
            while open < range.to.as_nanos() || (self.inclusive_end && open == range.to.as_nanos())
            {
                out.push(bar(open));
                open += HOUR;
            }
            Ok(out)
        }
        async fn subscribe_candles(
            &self,
            _: &InstrumentId,
            _: Timeframe,
            _: SubscribeOpts,
        ) -> Result<SubscriptionId, Error> {
            unimplemented!()
        }
        fn candles_max_span(&self, _: Timeframe) -> Option<Duration> {
            self.max_span
        }
    }

    fn hours(n: i64) -> TimeRange {
        TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(n * HOUR))
    }

    fn fast() -> HistoryOpts {
        HistoryOpts {
            transport_backoff: Duration::from_millis(1),
            ..HistoryOpts::default()
        }
    }

    #[tokio::test]
    async fn cuts_the_range_into_windows_the_source_accepts() {
        let fake = Fake::new(Some(Duration::from_secs(10 * 3600)));
        let bars = load_candles(&fake, &id(), Timeframe::H1, hours(35), fast())
            .await
            .unwrap();
        assert_eq!(bars.len(), 35);
        assert!(bars.windows(2).all(|w| w[0].open_ts < w[1].open_ts));
        // 35 hours in windows of 10: 10 + 10 + 10 + 5
        assert_eq!(fake.calls.lock().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn without_a_known_span_the_range_goes_in_one_request() {
        let fake = Fake::new(None);
        let bars = load_candles(&fake, &id(), Timeframe::H1, hours(35), fast())
            .await
            .unwrap();
        assert_eq!(bars.len(), 35);
        assert_eq!(fake.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_window_option_overrides_the_source() {
        let fake = Fake::new(Some(Duration::from_secs(100 * 3600)));
        let opts = HistoryOpts {
            window: Some(Duration::from_secs(12 * 3600)),
            ..fast()
        };
        load_candles(&fake, &id(), Timeframe::H1, hours(36), opts)
            .await
            .unwrap();
        assert_eq!(fake.calls.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn a_bar_on_the_edge_of_two_windows_is_delivered_once() {
        let mut fake = Fake::new(Some(Duration::from_secs(10 * 3600)));
        fake.inclusive_end = true;
        let bars = load_candles(&fake, &id(), Timeframe::H1, hours(30), fast())
            .await
            .unwrap();
        let mut opens: Vec<i64> = bars.iter().map(|b| b.open_ts.as_nanos()).collect();
        let before = opens.len();
        opens.dedup();
        assert_eq!(before, opens.len(), "no bar twice");
        // 0..=30 hours: the last window includes its end
        assert_eq!(opens.len(), 31);
    }

    #[tokio::test]
    async fn a_rate_limit_is_waited_out_and_counted() {
        let fake = Fake::new(Some(Duration::from_secs(10 * 3600)));
        fake.rate_limited_first.store(3, Ordering::SeqCst);
        let inst = id();
        let mut pages = CandlePages::new(&fake, &inst, Timeframe::H1, hours(20), fast());
        while pages.next_page().await.unwrap().is_some() {}
        assert_eq!(pages.rate_waits(), 3);
        assert_eq!(pages.requests(), 5, "2 windows and 3 refused attempts");
        assert_eq!(pages.resume_from(), hours(20).to);
    }

    #[tokio::test]
    async fn too_many_rate_limits_stop_the_read_at_the_failed_window() {
        let fake = Fake::new(Some(Duration::from_secs(10 * 3600)));
        fake.rate_limited_first.store(100, Ordering::SeqCst);
        let opts = HistoryOpts {
            max_rate_waits: 2,
            ..fast()
        };
        let err = load_candles(&fake, &id(), Timeframe::H1, hours(20), opts)
            .await
            .unwrap_err();
        assert!(matches!(err.error, Error::RateLimited { .. }));
        assert_eq!(err.resume_from, UnixNanos::ZERO);
    }

    #[tokio::test]
    async fn a_network_error_is_repeated_a_few_times_then_reported() {
        let fake = Fake::new(None);
        fake.transport_first.store(2, Ordering::SeqCst);
        let bars = load_candles(&fake, &id(), Timeframe::H1, hours(5), fast())
            .await
            .unwrap();
        assert_eq!(bars.len(), 5);

        let fake = Fake::new(None);
        fake.transport_first.store(3, Ordering::SeqCst);
        let err = load_candles(&fake, &id(), Timeframe::H1, hours(5), fast())
            .await
            .unwrap_err();
        assert!(matches!(err.error, Error::Transport(_)));
    }

    #[tokio::test]
    async fn a_refusal_is_not_repeated_and_tells_where_to_resume() {
        // The window option is larger than the source accepts, so every request is refused.
        let fake = Fake::new(Some(Duration::from_secs(5 * 3600)));
        let opts = HistoryOpts {
            window: Some(Duration::from_secs(10 * 3600)),
            ..fast()
        };
        let inst = id();
        let mut pages = CandlePages::new(&fake, &inst, Timeframe::H1, hours(20), opts);
        let err = pages.next_page().await.unwrap_err();
        assert!(matches!(err.error, Error::Rejected { .. }));
        assert_eq!(pages.requests(), 1, "a refusal is final");
        assert_eq!(err.resume_from, UnixNanos::ZERO);
    }

    #[tokio::test]
    async fn an_empty_range_is_finished_at_once() {
        let fake = Fake::new(None);
        let inst = id();
        let mut pages = CandlePages::new(&fake, &inst, Timeframe::H1, hours(0), fast());
        assert!(pages.next_page().await.unwrap().is_none());
        assert_eq!(pages.requests(), 0);
    }
}
