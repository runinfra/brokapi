//! One typed method per REST endpoint of the BCS Trade API, as `impl` blocks of [`BcsHttpClient`].
//!
//! The methods are grouped by the service of the broker: authorization, information (reference data),
//! limits, margin, market data, orders, portfolio, trades and non-trade operations. All of them are
//! thin: they build the URL, call [`BcsHttpClient::get`], [`BcsHttpClient::get_with_query`] or
//! [`BcsHttpClient::post`], and return the broker's model. They do not retry (apart from the one
//! repeat after a 401, see [`BcsHttpClient`]), do not throttle and do not interpret the reply.
//!
//! All dates and times the broker sends are UTC, as its docs say.

use crate::error::BcsApiError;
use crate::http::client::BcsHttpClient;
use crate::http::models::*;
use chrono::Utc;
use uuid::Uuid;

// ─── auth ─────────────────────────────────────────────────────────────────────

impl BcsHttpClient {
    /// Gets a fresh access token and reports it in a [`TokenResponse`].
    ///
    /// It calls [`BcsHttpClient::refresh_access_token`], so the token service is contacted every time.
    /// The returned value is **built locally**, not copied from the broker's reply: `expires_in` and
    /// `refresh_expires_in` are always `0`, `token_type` is the fixed text `Bearer`, `session_state`
    /// and `scope` are `None`, and `refresh_token` is the refresh token the client holds now. Read the
    /// lifetimes from the broker docs (24 hours and 90 days), not from this value.
    ///
    /// You do not have to call it: every other method authorizes itself.
    ///
    /// # Errors
    ///
    /// As [`BcsHttpClient::refresh_access_token`]: [`BcsApiError::AuthError`] if the refresh token is
    /// refused, [`BcsApiError::HttpError`] on transport failures.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let token = client.authenticate().await?;
    /// assert!(!token.access_token.is_empty());
    /// assert_eq!(token.token_type, "Bearer");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn authenticate(&self) -> Result<TokenResponse, BcsApiError> {
        let token = self.refresh_access_token().await?;
        Ok(TokenResponse {
            access_token: token,
            expires_in: 0,
            refresh_expires_in: 0,
            refresh_token: Some(self.current_refresh_token().await),
            token_type: "Bearer".to_string(),
            session_state: None,
            scope: None,
        })
    }
}

// ─── information ──────────────────────────────────────────────────────────────

const DAILY_SCHEDULE_PATH: &str =
    "/trade-api-information-service/api/v1/trading-schedule/daily-schedule";
const TRADING_STATUS_PATH: &str = "/trade-api-information-service/api/v1/trading-schedule/status";
const INSTRUMENTS_BY_ISINS_PATH: &str =
    "/trade-api-information-service/api/v1/instruments/by-isins";
const INSTRUMENTS_BY_TICKERS_PATH: &str =
    "/trade-api-information-service/api/v1/instruments/by-tickers";
const INSTRUMENTS_BY_TYPE_PATH: &str = "/trade-api-information-service/api/v1/instruments/by-type";

impl BcsHttpClient {
    /// Gets the trading schedule of one instrument for the day: `GET .../trading-schedule/daily-schedule`.
    ///
    /// Both `class_code` and `ticker` are required by the broker. The reply lists the sessions of the
    /// day with their start and end as times of day (`HH:MM:SS`). The broker docs do not say in which
    /// time zone those times are, and the names of the sessions are in Russian (for example
    /// "Торговый период"). Not verified against the live API.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::ValidationError`] if a parameter is missing or empty. See [`BcsApiError`] for the
    /// other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::DailyScheduleRequest;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let day = client
    ///     .get_daily_schedule(&DailyScheduleRequest { class_code: "TQBR".into(), ticker: "SBER".into() })
    ///     .await?;
    /// assert!(day.is_work_day);
    /// assert!(!day.daily_schedule.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_daily_schedule(
        &self,
        req: &DailyScheduleRequest,
    ) -> Result<DailyScheduleResponse, BcsApiError> {
        let query = [
            ("classCode", req.class_code.as_str()),
            ("ticker", req.ticker.as_str()),
        ];
        self.get_with_query(DAILY_SCHEDULE_PATH, &query).await
    }

    /// Gets the current trading session status of a board: `GET .../trading-schedule/status`.
    ///
    /// The status is per class code (board), not per instrument. The reply says whether the session is
    /// open and when the status changes next.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::ValidationError`] if `class_code` is missing. [`BcsApiError::ParseError`] if the
    /// reply carries a `nextSessionDate` that is not an ISO 8601 date-time. See [`BcsApiError`] for the
    /// other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::TradingStatusRequest;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let status = client
    ///     .get_trading_status(&TradingStatusRequest { class_code: "TQBR".into() })
    ///     .await?;
    /// assert_eq!(status.trading_session_status, "OPEN");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_trading_status(
        &self,
        req: &TradingStatusRequest,
    ) -> Result<TradingStatusResponse, BcsApiError> {
        let query = [("classCode", req.class_code.as_str())];
        self.get_with_query(TRADING_STATUS_PATH, &query).await
    }

    /// Looks up instruments by ISIN: `POST .../instruments/by-isins`.
    ///
    /// `size` (1 to 100, default 50) and `page` (from 0) page the answer. The broker does not say how
    /// many pages there are: a page shorter than `size` is the last one, a full page means "ask for
    /// `page + 1`". The parameters are sent in the query string (the broker docs mark them as path
    /// parameters, but the path has none, not verified against the live API).
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`]. An unknown ISIN is probably just missing from the list (not verified
    /// against the live API).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::InstrumentsByIsinsRequest;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let found = client
    ///     .get_instruments_by_isins(
    ///         &InstrumentsByIsinsRequest { isins: vec!["RU0009029540".into()] },
    ///         Some(50),
    ///         Some(0),
    ///     )
    ///     .await?;
    /// assert_eq!(found[0].ticker, "SBER");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_instruments_by_isins(
        &self,
        req: &InstrumentsByIsinsRequest,
        size: Option<u32>,
        page: Option<u32>,
    ) -> Result<Vec<InstrumentInfo>, BcsApiError> {
        let mut path = INSTRUMENTS_BY_ISINS_PATH.to_string();
        let mut params = Vec::new();
        if let Some(s) = size {
            params.push(format!("size={}", s));
        }
        if let Some(p) = page {
            params.push(format!("page={}", p));
        }
        if !params.is_empty() {
            path.push('?');
            path.push_str(&params.join("&"));
        }
        self.post(&path, req).await
    }

    /// Looks up instruments by ticker: `POST .../instruments/by-tickers`.
    ///
    /// One ticker can trade on several boards, so the answer has an [`InstrumentInfo`] per instrument
    /// and each lists its [`InstrumentInfo::boards`]. Paging works as in
    /// [`BcsHttpClient::get_instruments_by_isins`].
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::InstrumentsByTickersRequest;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let found = client
    ///     .get_instruments_by_tickers(&InstrumentsByTickersRequest { tickers: vec!["SBER".into()] }, None, None)
    ///     .await?;
    /// let params = found[0].trading_params_primary().expect("a primary board");
    /// assert_eq!(params.class_code, "TQBR");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_instruments_by_tickers(
        &self,
        req: &InstrumentsByTickersRequest,
        size: Option<u32>,
        page: Option<u32>,
    ) -> Result<Vec<InstrumentInfo>, BcsApiError> {
        let mut path = INSTRUMENTS_BY_TICKERS_PATH.to_string();
        let mut params = Vec::new();
        if let Some(s) = size {
            params.push(format!("size={}", s));
        }
        if let Some(p) = page {
            params.push(format!("page={}", p));
        }
        if !params.is_empty() {
            path.push('?');
            path.push_str(&params.join("&"));
        }
        self.post(&path, req).await
    }

    /// Lists instruments of one kind: `GET .../instruments/by-type`.
    ///
    /// [`InstrumentsByTypeRequest::instrument_type`] must be one of the broker's names: `CURRENCY`,
    /// `STOCK`, `FOREIGN_STOCK`, `BONDS`, `NOTES`, `DEPOSITARY_RECEIPTS`, `EURO_BONDS`, `MUTUAL_FUNDS`,
    /// `ETF`, `FUTURES`, `OPTIONS`, `GOODS`, `INDICES`. For `OPTIONS` the broker also requires the base
    /// asset ticker. A full list is long, so read it page by page: `size` 1 to 100, `page` from 0, a
    /// page shorter than `size` is the last.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::ValidationError`] for an unknown type, a missing base asset for options, or a
    /// `size` outside 1 to 100. See [`BcsApiError`] for the other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::error::BcsApiError;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::InstrumentsByTypeRequest;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let mut req = InstrumentsByTypeRequest {
    ///     instrument_type: "STOCK".into(),
    ///     base_asset_ticker: None,
    ///     size: Some(100),
    ///     page: Some(0),
    /// };
    /// let stocks = client.get_instruments_by_type(&req).await?;
    /// assert!(!stocks.is_empty());
    ///
    /// // A name the broker does not know is refused.
    /// req.instrument_type = "Stock".into();
    /// let err = client.get_instruments_by_type(&req).await.unwrap_err();
    /// assert!(matches!(err, BcsApiError::ValidationError { .. }));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_instruments_by_type(
        &self,
        req: &InstrumentsByTypeRequest,
    ) -> Result<Vec<InstrumentInfo>, BcsApiError> {
        let mut query: Vec<(&str, String)> = vec![("type", req.instrument_type.clone())];
        if let Some(ref ba) = req.base_asset_ticker {
            query.push(("baseAssetTicker", ba.clone()));
        }
        if let Some(s) = req.size {
            query.push(("size", s.to_string()));
        }
        if let Some(p) = req.page {
            query.push(("page", p.to_string()));
        }
        self.get_with_query(INSTRUMENTS_BY_TYPE_PATH, &query).await
    }
}

// ─── limits ───────────────────────────────────────────────────────────────────

const LIMITS_PATH: &str = "/trade-api-bff-limit/api/v1/limits";

impl BcsHttpClient {
    /// Gets the account limits: positions, money and futures limits (`GET .../limits`).
    ///
    /// It is a snapshot of what the account holds and what is locked by active orders. The same data
    /// comes as a stream on the limits WebSocket channel, see [`crate::ws::client::BcsWsClient::connect_limits`].
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let limits = client.get_limits().await?;
    /// let rub = limits.money_limits.iter().find(|m| m.currency.as_deref() == Some("RUB"));
    /// assert!(rub.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_limits(&self) -> Result<LimitsResponse, BcsApiError> {
        self.get(LIMITS_PATH).await
    }
}

// ─── marginal ─────────────────────────────────────────────────────────────────

const MARGINAL_PATH: &str = "/trade-api-bff-marginal-indicators/api/v1/instruments-discounts";

impl BcsHttpClient {
    /// Gets the margin discounts of instruments (`GET .../instruments-discounts`).
    ///
    /// A discount is the broker's rate for an instrument bought on margin (long) or sold short. The
    /// broker docs give the field names but not the unit (fraction or percent), not verified against
    /// the live API.
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let discounts = client.get_instruments_discounts().await?;
    /// assert_eq!(discounts[0].ticker, "SBER");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_instruments_discounts(
        &self,
    ) -> Result<InstrumentDiscountsResponse, BcsApiError> {
        self.get(MARGINAL_PATH).await
    }
}

// ─── market_data ──────────────────────────────────────────────────────────────

const CANDLES_PATH: &str = "/trade-api-market-data-connector/api/v1/candles-chart";
const ORDER_BOOK_PATH: &str = "/trade-api-market-data-connector/api/v1/order-book";
const LAST_TRADES_PATH: &str = "/trade-api-market-data-connector/api/v1/last-trades";
const QUOTES_PATH: &str = "/trade-api-market-data-connector/api/v1/quotes";

impl BcsHttpClient {
    /// Gets historical candles: `GET .../candles-chart`.
    ///
    /// The broker allows at most **1440 bars** in one request. A longer interval is refused, so split
    /// it or use a larger [`CandlesRequest::time_frame`]. The time frame names are
    /// `M1 M5 M15 M30 H1 H4 D W MN`. Times are UTC.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::ValidationError`] for a time frame the broker does not know or an interval of
    /// more than 1440 bars. See [`BcsApiError`] for the other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::error::BcsApiError;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::CandlesRequest;
    /// use chrono::{TimeZone, Utc};
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let mut req = CandlesRequest {
    ///     ticker: "SBER".into(),
    ///     class_code: "TQBR".into(),
    ///     start_date: Utc.with_ymd_and_hms(2026, 10, 19, 7, 0, 0).unwrap(),
    ///     end_date: Utc.with_ymd_and_hms(2026, 10, 19, 8, 0, 0).unwrap(),
    ///     time_frame: "M1".into(),
    /// };
    /// let candles = client.get_candles(&req).await?;
    /// assert!(!candles.bars.is_empty());
    ///
    /// // "M10" is not a time frame of the broker.
    /// req.time_frame = "M10".into();
    /// let err = client.get_candles(&req).await.unwrap_err();
    /// assert!(matches!(err, BcsApiError::ValidationError { .. }));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_candles(
        &self,
        request: &CandlesRequest,
    ) -> Result<CandlesResponse, BcsApiError> {
        self.get_with_query(CANDLES_PATH, request).await
    }

    /// Gets a snapshot of the order book: `GET .../order-book`.
    ///
    /// `depth` is 1 to 20 levels per side (the broker default is 20). For a stream of changes use the
    /// market data WebSocket, see [`crate::ws::client::BcsWsClient::subscribe_order_book`].
    ///
    /// # Errors
    ///
    /// [`BcsApiError::RateLimitExceeded`] on a bare HTTP 429. [`BcsApiError::ValidationError`] for a
    /// `depth` outside the range or an unknown instrument. See [`BcsApiError`] for the other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let options = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
    /// # let server = brokapi_mocks::bcs::MockServer::start(options.clone()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::error::BcsApiError;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::OrderBookRequest;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let req = OrderBookRequest { ticker: "SBER".into(), class_code: "TQBR".into(), depth: 10 };
    /// let book = client.get_order_book(&req).await?;
    /// assert!(!book.bids.is_empty() && !book.asks.is_empty());
    ///
    /// // When the broker says "too many requests" you get RateLimitExceeded: wait, then call again.
    /// # options.enable_rate_limit_order_book();
    /// let err = client.get_order_book(&req).await.unwrap_err();
    /// assert!(matches!(err, BcsApiError::RateLimitExceeded));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_order_book(
        &self,
        request: &OrderBookRequest,
    ) -> Result<OrderBookResponse, BcsApiError> {
        self.get_with_query(ORDER_BOOK_PATH, request).await
    }

    /// Gets the anonymous trades of the current session: `POST .../last-trades`.
    ///
    /// The request names the instrument and may narrow the period and the side. The broker has no
    /// `limit` parameter. The reply is read in both shapes seen so far, `{"records": [...]}` (the
    /// docs) and a bare array (older samples). Records carry no instrument, it is the one you asked
    /// for.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::ValidationError`] if `ticker` or `class_code` is missing. See [`BcsApiError`] for
    /// the other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::LastTradesRequest;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let trades = client
    ///     .get_last_trades(&LastTradesRequest {
    ///         ticker: "SBER".into(),
    ///         class_code: "TQBR".into(),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// assert!(trades.iter().all(|t| t.price > dec!(0.0)));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_last_trades(
        &self,
        request: &LastTradesRequest,
    ) -> Result<LastTradesResponse, BcsApiError> {
        let wire: LastTradesWire = self.post(LAST_TRADES_PATH, request).await?;
        Ok(wire.into_vec())
    }

    /// Gets current quotes for a list of instruments (1 to 100): `POST .../quotes`.
    ///
    /// Every field of a [`QuoteRecord`] except the key may be missing, so check before you use it.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::ValidationError`] for an empty list or more than 100 instruments (the docs'
    /// range, not verified against the live API). See [`BcsApiError`] for the other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::{QuoteInstrument, QuotesRequest};
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let quotes = client
    ///     .get_quotes(&QuotesRequest { instruments: vec![QuoteInstrument::new("SBER", "TQBR")] })
    ///     .await?;
    /// let sber = &quotes.records[0];
    /// assert!(sber.bid.is_some() && sber.offer.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_quotes(&self, request: &QuotesRequest) -> Result<QuotesResponse, BcsApiError> {
        self.post(QUOTES_PATH, request).await
    }
}

// ─── orders ───────────────────────────────────────────────────────────────────

const ORDERS_SEARCH_PATH: &str = "/trade-api-bff-order-details/api/v1/orders";
const ORDERS_OPERATIONS_PATH: &str = "/trade-api-bff-operations/api/v1/orders";

impl BcsHttpClient {
    /// Lists your orders by filter: `POST .../orders/search`.
    ///
    /// The list holds only orders placed since 2026-01-26 and at most the 1000 latest (broker docs).
    /// `size` is 1 to 100 (default 50), `page` starts at 0. The records carry the broker's own order id
    /// and **not** your `clientOrderId`. To find an order by your id use [`BcsHttpClient::get_order`].
    ///
    /// The status codes of this list differ from the codes of order events: see [`OrderRecord`].
    ///
    /// # Errors
    ///
    /// [`BcsApiError::ValidationError`] for a bad period or filter. See [`BcsApiError`] for the other
    /// variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::{CreateOrderRequest, OrderSearchRequest};
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use chrono::{Duration, Utc};
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// # let cid = uuid::Uuid::new_v4();
    /// # client.create_order_sync(&CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0))).await?;
    /// let now = Utc::now();
    /// let request = OrderSearchRequest {
    ///     start_date_time: now - Duration::hours(24),
    ///     end_date_time: now,
    ///     side: None,
    ///     order_status: Some(vec![3]), // 3 = active in the order list
    ///     order_types: None,
    ///     tickers: Some(vec!["SBER".into()]),
    ///     class_codes: None,
    /// };
    /// let page = client.search_orders(request, Some(0), Some(50)).await?;
    /// assert_eq!(page.records.len(), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_orders(
        &self,
        request: OrderSearchRequest,
        page: Option<i32>,
        size: Option<i32>,
    ) -> Result<OrderSearchResponse, BcsApiError> {
        let mut url = format!("{}/search", ORDERS_SEARCH_PATH);
        let mut query_params = vec![];
        if let Some(p) = page {
            query_params.push(format!("page={}", p));
        }
        if let Some(s) = size {
            query_params.push(format!("size={}", s));
        }
        if !query_params.is_empty() {
            url.push('?');
            url.push_str(&query_params.join("&"));
        }
        self.post(&url, &Some(&request)).await
    }

    /// Lists your orders of the last `hours` hours, optionally for some tickers.
    ///
    /// A shortcut over [`BcsHttpClient::search_orders`]: the period is `now - hours` to `now` by the
    /// local clock, there is no other filter, and the first page of the default size (50) is returned.
    /// It does not read further pages: when `total_pages` of the answer is above 1, call
    /// [`BcsHttpClient::search_orders`] yourself.
    ///
    /// # Errors
    ///
    /// The same as [`BcsHttpClient::search_orders`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let recent = client.search_orders_simple(Some(vec!["SBER".to_string()]), 24).await?;
    /// assert_eq!(recent.total_records, 0); // a fresh mock account has no orders
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_orders_simple(
        &self,
        tickers: Option<Vec<String>>,
        hours: i64,
    ) -> Result<OrderSearchResponse, BcsApiError> {
        let end = Utc::now();
        let start = end - chrono::Duration::hours(hours);
        let request = OrderSearchRequest {
            start_date_time: start,
            end_date_time: end,
            side: None,
            order_status: None,
            order_types: None,
            tickers,
            class_codes: None,
        };
        self.search_orders(request, None, None).await
    }

    /// Places an order and returns when the broker has **accepted the request**: `POST .../orders`.
    ///
    /// Needs a write token ([`crate::config::BcsConfig::is_write`]). The reply (`status`) only
    /// confirms that the request was taken. It does not say that the order is on the exchange or
    /// filled. The state comes later on the order events channel
    /// ([`crate::ws::client::BcsWsClient::connect_order_events`]) or from [`BcsHttpClient::get_order`].
    /// To get the state in the reply use [`BcsHttpClient::create_order_sync`].
    ///
    /// Your `client_order_id` is the id you must keep. The broker docs say it is used to track the
    /// order and to prevent duplicates. The quantity is in pieces, not in lots.
    ///
    /// # No automatic retry
    ///
    /// If the call ends with a timeout or a transport error, **do not send it again**. The order may
    /// exist. A repeat could place a second order. Look the order up first with
    /// [`BcsHttpClient::get_order`] by the same `client_order_id`: an answer means it exists,
    /// [`BcsApiError::NotFound`] means it does not (not verified against the live API how long the
    /// broker needs before the order becomes visible).
    ///
    /// # Errors
    ///
    /// * [`BcsApiError::ValidationError`]: the broker refused the order, nothing was placed.
    /// * [`BcsApiError::HttpError`] with `FORBIDDEN` or `USER_BLOCKED` in `message`: no trading rights,
    ///   for example a read token (not verified against the live API which type the broker uses).
    /// * [`BcsApiError::RateLimitExceeded`] or [`BcsApiError::HttpError`] with status 429: too many
    ///   requests. The broker also caps orders per day (200000 by its docs).
    /// * [`BcsApiError::HttpError`] without a status or [`BcsApiError::ParseError`]: unknown outcome,
    ///   see above.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::error::BcsApiError;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::CreateOrderRequest;
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    ///
    /// // Buy 1 piece of SBER at 300.00. Keep `cid`: it is how you find this order later.
    /// let cid = Uuid::new_v4();
    /// let ack = client
    ///     .create_order(CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0)))
    ///     .await?;
    /// assert_eq!(ack.client_order_id, cid);
    ///
    /// // A refusal is a ValidationError and means "no order". (The imitation refuses this price.)
    /// let refused = client
    ///     .create_order(CreateOrderRequest::limit(
    ///         Uuid::new_v4(), "SBER", "TQBR", OrderSide::Buy, 1, brokapi_conformance::REJECT_PRICE.decimal(),
    ///     ))
    ///     .await;
    /// assert!(matches!(refused, Err(BcsApiError::ValidationError { .. })));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_order(
        &self,
        request: CreateOrderRequest,
    ) -> Result<OrderOperationResponse, BcsApiError> {
        self.post(ORDERS_OPERATIONS_PATH, &Some(&request)).await
    }

    /// Cancels an order, the **deprecated** form with the id in the path: `POST .../orders/{id}/cancel`.
    ///
    /// The broker docs list this method in their deprecated section. Use
    /// [`BcsHttpClient::cancel_order_by_id`]. The in-process imitation does not serve the deprecated
    /// forms, so this method has no runnable example. `original_client_order_id` is the
    /// `client_order_id` of the order to cancel, `request.client_order_id` is the id of this request.
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`]. A cancel that timed out has an unknown outcome: ask for the order with
    /// [`BcsHttpClient::get_order`] before you decide.
    pub async fn cancel_order(
        &self,
        original_client_order_id: Uuid,
        request: CancelOrderRequest,
    ) -> Result<OrderOperationResponse, BcsApiError> {
        let url = format!(
            "{}/{}/cancel",
            ORDERS_OPERATIONS_PATH, original_client_order_id
        );
        self.post(&url, &Some(&request)).await
    }

    /// Changes an order, the **deprecated** form with the id in the path: `POST .../orders/{id}`.
    ///
    /// The broker docs list this method in their deprecated section. Use [`BcsHttpClient::edit_order`].
    /// The in-process imitation does not serve the deprecated forms, so this method has no runnable
    /// example.
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`].
    pub async fn modify_order(
        &self,
        original_client_order_id: Uuid,
        request: ModifyOrderRequest,
    ) -> Result<OrderOperationResponse, BcsApiError> {
        let url = format!("{}/{}", ORDERS_OPERATIONS_PATH, original_client_order_id);
        self.post(&url, &Some(&request)).await
    }

    /// Places an order and waits for the broker to process it: `POST .../orders/sync`.
    ///
    /// The body is the same as for [`BcsHttpClient::create_order`]. The reply is the current state of
    /// the order, in the format of [`BcsHttpClient::get_order`]. A refusal of the exchange can show up
    /// as an order in status "rejected" (`orderStatus = 8`) with a `rejectReason` and a normal HTTP 200,
    /// and not as an error (not verified against the live API). Check
    /// [`OrderExecutionData::order_status`] and [`OrderExecutionData::reject_reason`], not only `Ok`.
    ///
    /// The rules for timeouts are the same as for [`BcsHttpClient::create_order`]: **never repeat** a
    /// placement that ended without an answer. Look the order up first.
    ///
    /// # Errors
    ///
    /// The same as [`BcsHttpClient::create_order`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::CreateOrderRequest;
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let cid = Uuid::new_v4();
    /// let state = client
    ///     .create_order_sync(&CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0)))
    ///     .await?;
    /// let report = &state.data[0];
    /// assert_eq!(report.order_status.as_deref(), Some("0")); // 0 = new
    /// assert_eq!(report.ticker.as_deref(), Some("SBER"));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_order_sync(
        &self,
        request: &CreateOrderRequest,
    ) -> Result<OrderStateListResponse, BcsApiError> {
        self.post(&format!("{}/sync", ORDERS_OPERATIONS_PATH), request)
            .await
    }

    /// Cancels an order named in the body: `POST .../orders/cancel`.
    ///
    /// This is the current form (the id is in the body, not in the path). The reply only confirms that
    /// the cancel request was taken. The order is cancelled when its state says so: watch the order
    /// events channel or call [`BcsHttpClient::get_order`], or use [`BcsHttpClient::cancel_order_by_id_sync`].
    /// The broker allows to cancel orders in status new (0), partly filled (1), replaced (5) and
    /// waiting for confirmation (10).
    ///
    /// A cancel that ended without an answer has an unknown outcome. Asking again is harmless in
    /// principle (the order is cancelled once), but check the order first.
    ///
    /// # Errors
    ///
    /// * [`BcsApiError::NotFound`]: no such order (not verified against the live API).
    /// * [`BcsApiError::ValidationError`]: the order is not in a state that can be cancelled.
    /// * Other variants as in [`BcsApiError`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::{CancelByIdRequest, CreateOrderRequest};
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let cid = Uuid::new_v4();
    /// client
    ///     .create_order_sync(&CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0)))
    ///     .await?;
    ///
    /// // `cid` names the order, the second id names this cancel request.
    /// let ack = client
    ///     .cancel_order_by_id(&CancelByIdRequest::by_client_order_id(cid, Uuid::new_v4()))
    ///     .await?;
    /// assert_eq!(ack.status, "OK");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_order_by_id(
        &self,
        request: &CancelByIdRequest,
    ) -> Result<OrderOperationResponse, BcsApiError> {
        self.post(&format!("{}/cancel", ORDERS_OPERATIONS_PATH), request)
            .await
    }

    /// Cancels an order and waits for the result: `POST .../orders/cancel/sync`.
    ///
    /// The reply is the order state after the cancel, in the format of [`BcsHttpClient::get_order`]:
    /// look at [`OrderExecutionData::order_status`] (`4` = cancelled). Rules and errors are as for
    /// [`BcsHttpClient::cancel_order_by_id`].
    ///
    /// # Errors
    ///
    /// The same as [`BcsHttpClient::cancel_order_by_id`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::{CancelByIdRequest, CreateOrderRequest};
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let cid = Uuid::new_v4();
    /// client
    ///     .create_order_sync(&CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0)))
    ///     .await?;
    /// let state = client
    ///     .cancel_order_by_id_sync(&CancelByIdRequest::by_client_order_id(cid, Uuid::new_v4()))
    ///     .await?;
    /// assert_eq!(state.data[0].order_status.as_deref(), Some("4")); // 4 = cancelled
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_order_by_id_sync(
        &self,
        request: &CancelByIdRequest,
    ) -> Result<OrderStateListResponse, BcsApiError> {
        self.post(&format!("{}/cancel/sync", ORDERS_OPERATIONS_PATH), request)
            .await
    }

    /// Changes an order: `POST .../orders/edit`.
    ///
    /// The broker does the change as a **cancel of the original and a new order** (broker docs). So the
    /// `client_order_id` of the request is the id of the **new** order, and the original ends in status
    /// "replaced" (5, by the status table of the docs, not verified against the live API). Orders in
    /// status new (0), partly filled (1), replaced (5) and waiting for confirmation (10) can be changed. The reply only confirms that the request was taken.
    ///
    /// Because it creates an order, an edit that ended without an answer has an unknown outcome and
    /// must **not** be repeated before you look at the orders.
    ///
    /// # Errors
    ///
    /// As for [`BcsHttpClient::create_order`], plus [`BcsApiError::NotFound`] for an unknown original
    /// order (not verified against the live API).
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::{CreateOrderRequest, EditOrderRequest, OrderExtras};
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let cid = Uuid::new_v4();
    /// client
    ///     .create_order_sync(&CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0)))
    ///     .await?;
    ///
    /// let new_cid = Uuid::new_v4();
    /// let ack = client
    ///     .edit_order(&EditOrderRequest {
    ///         order_id_type: Some("1".into()),
    ///         order_id: cid.to_string(),
    ///         client_order_id: new_cid,
    ///         order_type: None,
    ///         price: Some(dec!(301.0)),
    ///         order_quantity: 1,
    ///         extras: OrderExtras::default(),
    ///     })
    ///     .await?;
    /// assert_eq!(ack.client_order_id, new_cid);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn edit_order(
        &self,
        request: &EditOrderRequest,
    ) -> Result<OrderOperationResponse, BcsApiError> {
        self.post(&format!("{}/edit", ORDERS_OPERATIONS_PATH), request)
            .await
    }

    /// Changes an order and waits for the result: `POST .../orders/edit/sync`.
    ///
    /// The reply is the state of the **new** order, in the format of [`BcsHttpClient::get_order`].
    /// Everything else is as for [`BcsHttpClient::edit_order`].
    ///
    /// # Errors
    ///
    /// The same as [`BcsHttpClient::edit_order`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::{CreateOrderRequest, EditOrderRequest, OrderExtras};
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let cid = Uuid::new_v4();
    /// client
    ///     .create_order_sync(&CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0)))
    ///     .await?;
    ///
    /// let state = client
    ///     .edit_order_sync(&EditOrderRequest {
    ///         order_id_type: None, // default: the id is the clientOrderId of the original
    ///         order_id: cid.to_string(),
    ///         client_order_id: Uuid::new_v4(),
    ///         order_type: None,
    ///         price: Some(dec!(301.0)),
    ///         order_quantity: 2,
    ///         extras: OrderExtras::default(),
    ///     })
    ///     .await?;
    /// assert_eq!(state.original_client_order_id.as_deref(), Some(cid.to_string().as_str()));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn edit_order_sync(
        &self,
        request: &EditOrderRequest,
    ) -> Result<OrderStateListResponse, BcsApiError> {
        self.post(&format!("{}/edit/sync", ORDERS_OPERATIONS_PATH), request)
            .await
    }

    /// Gets the current state of one order: `GET .../orders?orderIdType=&orderId=`.
    ///
    /// `order_id_type` is `"1"` for your `clientOrderId` (the broker default) or `"2"` for the
    /// exchange order id (format `YYMMDD-ClassCode-OrderNumber`). It works for orders of any
    /// supported type. The reply holds a list of reports. The docs do not say how many reports the list
    /// has, so read the last one if you need the latest state.
    ///
    /// This is the method to ask after a placement that timed out. [`BcsApiError::NotFound`] (HTTP 404)
    /// means the broker has no such order.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::NotFound`] if there is no such order. See [`BcsApiError`] for the other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::error::BcsApiError;
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::CreateOrderRequest;
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let cid = Uuid::new_v4();
    /// client
    ///     .create_order_sync(&CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0)))
    ///     .await?;
    ///
    /// let state = client.get_order("1", &cid.to_string()).await?;
    /// assert_eq!(state.data[0].executed_quantity, Some(dec!(0.0)));
    ///
    /// // An id the broker does not know.
    /// let missing = client.get_order("1", &Uuid::new_v4().to_string()).await;
    /// assert!(matches!(missing, Err(BcsApiError::NotFound { .. })));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_order(
        &self,
        order_id_type: &str,
        order_id: &str,
    ) -> Result<OrderStateListResponse, BcsApiError> {
        let query = [("orderIdType", order_id_type), ("orderId", order_id)];
        self.get_with_query(ORDERS_OPERATIONS_PATH, &query).await
    }

    /// Gets the state of an order, the **deprecated** form with the id in the path: `GET .../orders/{id}`.
    ///
    /// The broker docs list this method in their deprecated section. Use [`BcsHttpClient::get_order`].
    /// The in-process imitation does not serve the deprecated forms, so this method has no runnable
    /// example. Unlike the current form, `data` of the reply is a single object.
    ///
    /// # Errors
    ///
    /// [`BcsApiError::NotFound`] if there is no such order. See [`BcsApiError`] for the other variants.
    pub async fn get_order_status(
        &self,
        original_client_order_id: Uuid,
    ) -> Result<OrderStatusResponse, BcsApiError> {
        let url = format!("{}/{}", ORDERS_OPERATIONS_PATH, original_client_order_id);
        self.get(&url).await
    }
}

// ─── portfolio ────────────────────────────────────────────────────────────────

const PORTFOLIO_PATH: &str = "/trade-api-bff-portfolio/api/v1/portfolio";

impl BcsHttpClient {
    /// Gets the portfolio: one record per position and per currency balance (`GET .../portfolio`).
    ///
    /// The list holds positions of several kinds, told apart by [`PortfolioPosition::position_type`].
    /// Many fields are absent for some kinds, so they are all optional. The same data streams on the
    /// portfolio WebSocket channel, see [`crate::ws::client::BcsWsClient::connect_portfolio`].
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let positions = client.get_portfolio().await?;
    /// assert!(!positions.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_portfolio(&self) -> Result<PortfolioResponse, BcsApiError> {
        self.get(PORTFOLIO_PATH).await
    }
}

// ─── trades ───────────────────────────────────────────────────────────────────

const TRADES_BASE: &str = "/trade-api-bff-trade-details/api/v1/trades";

impl BcsHttpClient {
    /// Lists your own trades (fills) by filter: `POST .../trades/search`.
    ///
    /// The list holds trades made since 2026-01-26 and at most the 1000 latest (broker docs). `size`
    /// is 1 to 100 (default 50), `page` starts at 0. A record has the exchange order number and the
    /// trade number but **not** your `clientOrderId`, and no commission.
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let options = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
    /// # let server = brokapi_mocks::bcs::MockServer::start(options).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::{CreateOrderRequest, TradeSearchRequest};
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// # let cid = Uuid::new_v4();
    /// # client.create_order_sync(&CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, dec!(300.0))).await?;
    /// # server.fill(&cid.to_string(), brokapi_core::Qty::from(1), brokapi_core::Price::from(300)); // the imitation fills the order
    /// let trades = client
    ///     .search_trades(
    ///         &TradeSearchRequest {
    ///             start_date_time: "2026-01-26T00:00:00.000Z".into(),
    ///             end_date_time: "2099-01-01T00:00:00.000Z".into(),
    ///             side: None,
    ///             tickers: Some(vec!["SBER".into()]),
    ///         },
    ///         Some(50),
    ///         Some(0),
    ///     )
    ///     .await?;
    /// assert_eq!(trades.records.len(), 1);
    /// assert_eq!(trades.records[0].price, Some(dec!(300.0)));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_trades(
        &self,
        req: &TradeSearchRequest,
        size: Option<u32>,
        page: Option<u32>,
    ) -> Result<PaginatedResponse<TradeRecord>, BcsApiError> {
        let mut path = format!("{}/search", TRADES_BASE);
        let mut params = Vec::new();
        if let Some(s) = size {
            params.push(format!("size={}", s));
        }
        if let Some(p) = page {
            params.push(format!("page={}", p));
        }
        if !params.is_empty() {
            path = format!("{}?{}", path, params.join("&"));
        }
        self.post(&path, req).await
    }
}

// ─── non-trade operations ─────────────────────────────────────────────────────

const OPERATIONS_SEARCH_PATH: &str = "/trade-api-bff-nontrade-operations/api/v1/operations/search";

impl BcsHttpClient {
    /// Lists non-trade operations: deposits, withdrawals, dividends, coupons, commissions, taxes
    /// (`POST .../operations/search`).
    ///
    /// All filters in the request are optional. `size` (1 to 100, default 50) and `page` (from 0) are
    /// sent in the query string. The broker docs mark them as path parameters, but the path of the
    /// method has none (the same as for the trades search), so the query is an assumption that is not
    /// verified against the live API. This service has the tightest rate limit of the API, 3 requests
    /// per second.
    ///
    /// # Errors
    ///
    /// See [`BcsApiError`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_bcs_protocol::http::client::BcsHttpClient;
    /// use brokapi_bcs_protocol::http::models::OperationHistoryRequest;
    ///
    /// let client = BcsHttpClient::new(cfg)?;
    /// let history = client
    ///     .search_operations(
    ///         &OperationHistoryRequest {
    ///             operation_types: Some(vec!["PayIn".into()]),
    ///             ..Default::default()
    ///         },
    ///         Some(20),
    ///         Some(0),
    ///     )
    ///     .await?;
    /// assert_eq!(history.records[0].operation_type.as_deref(), Some("PayIn"));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_operations(
        &self,
        req: &OperationHistoryRequest,
        size: Option<u32>,
        page: Option<u32>,
    ) -> Result<OperationHistoryResponse, BcsApiError> {
        let mut path = OPERATIONS_SEARCH_PATH.to_string();
        let mut params = Vec::new();
        if let Some(s) = size {
            params.push(format!("size={}", s));
        }
        if let Some(p) = page {
            params.push(format!("page={}", p));
        }
        if !params.is_empty() {
            path.push('?');
            path.push_str(&params.join("&"));
        }
        self.post(&path, req).await
    }
}
