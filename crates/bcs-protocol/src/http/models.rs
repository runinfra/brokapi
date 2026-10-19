//! Request and response models of the REST API, one struct per JSON shape of the broker.
//!
//! Field names are in Rust style and map to the broker's camelCase names through `serde`. Wherever the
//! broker docs say a value is one of a closed set (side, status, instrument type), these models keep
//! it as text when a new value must not break parsing, and use an enum when the set is stable. Almost
//! every field of a reply is an `Option`, because the broker fills only what fits the object. Check
//! before you use a field.
//!
//! All dates and times from the broker are UTC (broker docs). Quantities of orders are in pieces.

use crate::models::enums::OrderSide;
use crate::utils::date_format;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── auth ─────────────────────────────────────────────────────────────────────

/// The form the client sends to the token service to trade a refresh token for an access token.
///
/// The client builds and sends it for you. It is public so you can see the exact wire form. It is sent as
/// `application/x-www-form-urlencoded`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::TokenRequest;
///
/// let form = TokenRequest {
///     grant_type: "refresh_token".into(),
///     refresh_token: "my-refresh-token".into(),
///     client_id: "trade-api-read".into(),
/// };
/// assert_eq!(
///     serde_urlencoded::to_string(&form)?,
///     "grant_type=refresh_token&refresh_token=my-refresh-token&client_id=trade-api-read"
/// );
/// # Ok::<(), serde_urlencoded::ser::Error>(())
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct TokenRequest {
    /// Always `refresh_token` here: the OAuth grant that renews a token.
    pub grant_type: String,
    /// The refresh token. A secret.
    pub refresh_token: String,
    /// `trade-api-read` for a read token, `trade-api-write` for a trading token.
    pub client_id: String,
}

/// The answer of the token service (OAuth style).
///
/// The client reads it to get the new tokens. [`BcsHttpClient::authenticate`](crate::http::client::BcsHttpClient::authenticate)
/// returns a value of this type, but builds it locally, see there.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::TokenResponse;
///
/// let reply = r#"{"access_token":"a","expires_in":86400,"refresh_expires_in":7776000,
///                 "refresh_token":"r","token_type":"bearer"}"#;
/// let token: TokenResponse = serde_json::from_str(reply)?;
/// assert_eq!(token.expires_in, 86_400); // 24 hours
/// assert_eq!(token.refresh_token.as_deref(), Some("r"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    /// The access token to put in `Authorization: Bearer ...`. A secret.
    pub access_token: String,
    /// Lifetime of the access token in seconds. The broker docs say 24 hours in total.
    pub expires_in: u64,
    /// Lifetime of the refresh token in seconds. The broker docs say 90 days in total.
    pub refresh_expires_in: u64,
    /// A new refresh token, if the token service rotates it. Keep it in place of the old one. `None` if the reply carried none.
    pub refresh_token: Option<String>,
    /// Type of the access token, `bearer` by the token service.
    pub token_type: String,
    /// Session id of the token service, if it sends one.
    #[serde(default)]
    pub session_state: Option<String>,
    /// Scope granted to the token, if the token service sends one.
    #[serde(default)]
    pub scope: Option<String>,
}

// ─── common ───────────────────────────────────────────────────────────────────

/// A page of a list, with the totals the broker reports.
///
/// Pages count from 0. `total_records` and `total_pages` are for the whole result of the filter, not for
/// this page. The lists of orders and trades hold at most the 1000 latest records (broker docs).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::PaginatedResponse;
///
/// let page: PaginatedResponse<String> =
///     serde_json::from_str(r#"{"records":["a","b"],"totalRecords":2,"totalPages":1}"#)?;
/// assert_eq!((page.records.len(), page.total_pages), (2, 1));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedResponse<T> {
    /// The records of this page.
    pub records: Vec<T>,
    /// Number of records for the whole filter.
    pub total_records: u64,
    /// Number of pages for the whole filter, at the page size of the request.
    pub total_pages: u64,
}

/// A generic acknowledgement of a command: the id of the request and a status text.
///
/// No client method returns it. [`OrderOperationResponse`] is the one the order methods use.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationResponse {
    /// Id of the request, as text.
    pub client_order_id: String,
    /// Status text of the broker.
    pub status: String,
}

// ─── information ──────────────────────────────────────────────────────────────

/// Parameters of [`BcsHttpClient::get_daily_schedule`](crate::http::client::BcsHttpClient::get_daily_schedule).
///
/// Both fields are required by the broker. Sent as a query string.
#[derive(Debug, Clone)]
pub struct DailyScheduleRequest {
    /// Class code (board), for example `TQBR`.
    pub class_code: String,
    /// Ticker, for example `SBER`.
    pub ticker: String,
}

/// One session of the day in a daily schedule.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::ScheduleEntry;
///
/// let e: ScheduleEntry = serde_json::from_str(
///     r#"{"startDate":"10:00:00","endDate":"18:50:00","tradingSessionType":"Торговый период","tradingSessionStatus":"OPEN"}"#,
/// )?;
/// assert_eq!(e.trading_session_status, "OPEN");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleEntry {
    /// Start of the session as a time of day, `HH:MM:SS`. Despite the name it is not a date. The time zone is not stated in the broker docs (not verified against the live API).
    pub start_date: String,
    /// End of the session as a time of day, `HH:MM:SS`. The time zone is not stated in the broker docs.
    pub end_date: String,
    /// Name of the session, in Russian. The docs list: technical break of QUIK, opening auction, non-working time or day off or holiday, evening session, trading period, closing auction.
    pub trading_session_type: String,
    /// `OPEN` or `CLOSE`.
    pub trading_session_status: String,
}

/// The trading schedule of one instrument for the day.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DailyScheduleResponse {
    /// `true` if the day is a trading day.
    pub is_work_day: bool,
    /// The sessions of the day, with their status.
    pub daily_schedule: Vec<ScheduleEntry>,
}

/// Body of [`BcsHttpClient::get_instruments_by_isins`](crate::http::client::BcsHttpClient::get_instruments_by_isins).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::InstrumentsByIsinsRequest;
///
/// let body = serde_json::to_string(&InstrumentsByIsinsRequest { isins: vec!["RU0009029540".into()] })?;
/// assert_eq!(body, r#"{"isins":["RU0009029540"]}"#);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct InstrumentsByIsinsRequest {
    /// ISIN codes to look up. The broker wants at least one, all different.
    pub isins: Vec<String>,
}

/// Body of [`BcsHttpClient::get_instruments_by_tickers`](crate::http::client::BcsHttpClient::get_instruments_by_tickers).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::InstrumentsByTickersRequest;
///
/// let body = serde_json::to_string(&InstrumentsByTickersRequest { tickers: vec!["SBER".into()] })?;
/// assert_eq!(body, r#"{"tickers":["SBER"]}"#);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct InstrumentsByTickersRequest {
    /// Tickers to look up. The broker wants at least one, all different.
    pub tickers: Vec<String>,
}

/// Parameters of [`BcsHttpClient::get_instruments_by_type`](crate::http::client::BcsHttpClient::get_instruments_by_type).
///
/// Sent as a query string. Fields left as `None` are not sent.
#[derive(Debug, Clone)]
pub struct InstrumentsByTypeRequest {
    /// Instrument type in the broker spelling: `CURRENCY`, `STOCK`, `FOREIGN_STOCK`, `BONDS`, `NOTES`, `DEPOSITARY_RECEIPTS`, `EURO_BONDS`, `MUTUAL_FUNDS`, `ETF`, `FUTURES`, `OPTIONS`, `GOODS` or `INDICES`. Another spelling (for example `Stock`) is refused.
    pub instrument_type: String,
    /// Ticker of the base asset. The broker requires it when the type is `OPTIONS`.
    pub base_asset_ticker: Option<String>,
    /// Page size, 1 to 100. The broker default is 50.
    pub size: Option<u32>,
    /// Page number, from 0. The broker default is 0.
    pub page: Option<u32>,
}

/// Parameters of [`BcsHttpClient::get_trading_status`](crate::http::client::BcsHttpClient::get_trading_status).
#[derive(Debug, Clone)]
pub struct TradingStatusRequest {
    /// Class code (board), for example `TQBR`. Required.
    pub class_code: String,
}

/// The trading session status of a board.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::TradingStatusResponse;
///
/// let s: TradingStatusResponse = serde_json::from_str(
///     r#"{"tradingSessionTypeId":1,"tradingSessionType":"MAIN","tradingSessionStatus":"OPEN",
///         "nextSessionDate":"2026-10-20T07:00:00.000Z"}"#,
/// )?;
/// assert_eq!(s.trading_session_status, "OPEN");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradingStatusResponse {
    /// Id of the current session.
    pub trading_session_type_id: i32,
    /// Name of the current session.
    pub trading_session_type: String,
    /// `OPEN` or `CLOSE`.
    pub trading_session_status: String,
    /// Date and time (UTC) of the next change of the status, as the broker docs describe it.
    #[serde(with = "date_format")]
    pub next_session_date: DateTime<Utc>,
}

/// Reference data of one instrument, as the information service returns it.
///
/// Only `ticker` is always there. Every other field is optional, because the broker fills the ones that fit
/// the instrument type (a coupon rate only for a bond, a base asset only for a derivative). Do not trade
/// on a default: see [`InstrumentInfo::trading_params_for_board`] for what is assumed when a field is missing.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::InstrumentInfo;
///
/// let info: InstrumentInfo = serde_json::from_str(
///     r#"{"ticker":"SBER","boards":[{"classCode":"TQBR","exchange":"MOEX"}],"lotSize":1,
///         "minimumStep":0.01,"scale":2,"tradingCurrency":"RUB","primaryBoard":"TQBR"}"#,
/// )?;
/// let params = info.trading_params_primary().expect("a board");
/// assert_eq!(params.class_code, "TQBR");
/// assert_eq!(params.price_step, dec!(0.01));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentInfo {
    /// Ticker of the instrument.
    pub ticker: String,
    /// The boards (class codes and exchanges) where the instrument trades.
    #[serde(default)]
    pub boards: Vec<InstrumentBoard>,
    /// Short name.
    #[serde(default)]
    pub short_name: Option<String>,
    /// Name for display.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Instrument type in the broker spelling (JSON field `type`): `CURRENCY`, `STOCK`, `FOREIGN_STOCK`, `BONDS`, `NOTES`, `DEPOSITARY_RECEIPTS`, `EURO_BONDS`, `MUTUAL_FUNDS`, `ETF`, `FUTURES`, `OPTIONS`, `GOODS`, `INDICES`.
    #[serde(rename = "type", default)]
    pub instrument_type_name: Option<String>,
    /// ISIN code.
    #[serde(default)]
    pub isin: Option<String>,
    /// State registration code of the issue.
    #[serde(default)]
    pub registration_code: Option<String>,
    /// Name of the issuer.
    #[serde(default)]
    pub issuer_name: Option<String>,
    /// Currency of the instrument (the trading currency).
    #[serde(default)]
    pub trading_currency: Option<String>,
    /// Face value (nominal), for bonds.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub face_value: Option<Decimal>,
    /// Precision: the number of decimal places of the price.
    #[serde(default)]
    pub scale: Option<u32>,
    /// Minimum price step. A price you send must be a whole number of steps.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub minimum_step: Option<Decimal>,
    /// Accrued coupon income at the settlement date.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub accrued_int: Option<Decimal>,
    /// Currency of the price step value.
    #[serde(default)]
    pub currency_step_price: Option<String>,
    /// Default settlement code.
    #[serde(default)]
    pub settle_code: Option<String>,
    /// Instrument type in the broker spelling (JSON field `instrumentType`). Same values as [`InstrumentInfo::instrument_type_name`].
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Settlement currency.
    #[serde(default)]
    pub settlement_currency: Option<String>,
    /// Settlement date, kept as the text the broker sends (a date-time).
    #[serde(default)]
    pub settlement_date: Option<String>,
    /// Maturity (redemption) date, kept as the text the broker sends.
    #[serde(default)]
    pub maturity_date: Option<String>,
    /// Size of a lot: pieces in one lot. Order quantities of the API are in pieces, not lots.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub lot_size: Option<Decimal>,
    /// `true` for an instrument only for qualified investors.
    #[serde(default)]
    pub is_qualified_only: Option<bool>,
    /// `true` if a short sale is available.
    #[serde(default)]
    pub is_can_short: Option<bool>,
    /// Name of the base asset of a derivative contract.
    #[serde(default)]
    pub base_asset: Option<String>,
    /// `true` if an unqualified investor may trade the instrument.
    #[serde(default)]
    pub available_for_unqualified: Option<bool>,
    /// Cost of one price step, in the currency of the step price.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub step_price: Option<Decimal>,
    /// `true` for a product of the broker itself.
    #[serde(default)]
    pub is_bcs_product: Option<bool>,
    /// Link to a logo. Deprecated by the broker.
    #[serde(default)]
    pub logo_link: Option<String>,
    /// Number of coupons per year, for bonds.
    #[serde(default)]
    pub coupons_per_year: Option<u32>,
    /// Coupon rate in percent per year, for bonds.
    #[serde(default)]
    pub coupon_rate: Option<f64>,
    /// Date of the next coupon payment, kept as the text the broker sends.
    #[serde(default)]
    pub next_coupon: Option<String>,
    /// Marking of a complex product by the knowledge tests (the broker docs call it "marking by tests").
    #[serde(default)]
    pub complex_product: Option<f64>,
    /// Type of the base asset, for futures.
    #[serde(default)]
    pub base_asset_future: Option<String>,
    /// Name of the instrument subtype.
    #[serde(default)]
    pub sub_type: Option<String>,
    /// Business sector of the issuer.
    #[serde(default)]
    pub business_sector: Option<String>,
    /// Dividend yield: the stated annual dividend divided by the closing price.
    #[serde(default)]
    pub dividend_yield: Option<f64>,
    /// Analyst target price.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub target_price: Option<Decimal>,
    /// Market capitalisation.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub mktcap: Option<Decimal>,
    /// `true` if the class is blocked for trading.
    #[serde(default)]
    pub is_blocked: Option<bool>,
    /// Class code of the primary board. Empty if the broker did not send it.
    #[serde(default)]
    pub primary_board: String,
}

/// A board an instrument trades on.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentBoard {
    /// Class code of the board, for example `TQBR`.
    pub class_code: String,
    /// Exchange code, for example `MOEX`.
    pub exchange: String,
}

/// Normalized trading parameters extracted from `InstrumentInfo` for domain mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradingParamsSnapshot {
    /// Ticker.
    pub ticker: String,
    /// Class code (board) the parameters are for.
    pub class_code: String,
    /// Pieces in one lot. `1` if the broker sent none (a default, not data).
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision::serialize")]
    pub lot_size: Decimal,
    /// Minimum price step. `0.01` if the broker sent none (a default, not data).
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision::serialize")]
    pub price_step: Decimal,
    /// Cost of one price step. If the broker sent none it is the price step (a default).
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision::serialize")]
    pub price_step_value: Decimal,
    /// Decimal places of the price. `2` if the broker sent none (a default).
    pub scale: u32,
    /// Trading currency. `RUB` if the broker sent none (a default).
    pub currency: String,
}

impl InstrumentInfo {
    /// Builds [`TradingParamsSnapshot`] for one board (class code).
    ///
    /// Returns `None` when the instrument lists boards and `class_code` is not one of them. If the
    /// instrument lists no boards at all, any `class_code` is accepted.
    ///
    /// **A missing field is replaced by a default, not reported:** lot size 1, price step 0.01, price
    /// step value equal to the step, scale 2, currency `RUB`. The defaults are a guess. Check the
    /// fields of [`InstrumentInfo`] itself before you trade on them.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rust_decimal::Decimal;
    /// # use rust_decimal_macros::dec;
    /// use brokapi_bcs_protocol::http::models::InstrumentInfo;
    ///
    /// let info: InstrumentInfo = serde_json::from_str(
    ///     r#"{"ticker":"SBER","boards":[{"classCode":"TQBR","exchange":"MOEX"}],"lotSize":10,"minimumStep":0.05}"#,
    /// )?;
    /// let p = info.trading_params_for_board("TQBR").expect("listed board");
    /// assert_eq!((p.lot_size, p.price_step, p.scale), (dec!(10.0), dec!(0.05), 2)); // scale 2 is a default
    /// assert!(info.trading_params_for_board("SMAL").is_none()); // not a board of this instrument
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn trading_params_for_board(&self, class_code: &str) -> Option<TradingParamsSnapshot> {
        if !self.boards.is_empty() && !self.boards.iter().any(|b| b.class_code == class_code) {
            return None;
        }

        Some(TradingParamsSnapshot {
            ticker: self.ticker.clone(),
            class_code: class_code.to_string(),
            lot_size: self.lot_size.unwrap_or(Decimal::ONE),
            price_step: self.minimum_step.unwrap_or(Decimal::new(1, 2)),
            price_step_value: self
                .step_price
                .unwrap_or_else(|| self.minimum_step.unwrap_or(Decimal::new(1, 2))),
            scale: self.scale.unwrap_or(2),
            currency: self
                .trading_currency
                .clone()
                .unwrap_or_else(|| "RUB".to_string()),
        })
    }

    /// Builds [`TradingParamsSnapshot`] for the primary board of the instrument.
    ///
    /// The primary board is [`InstrumentInfo::primary_board`], or the first listed board if that is
    /// empty. Returns `None` if there is neither. The defaults of
    /// [`InstrumentInfo::trading_params_for_board`] apply.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::http::models::InstrumentInfo;
    ///
    /// let info: InstrumentInfo = serde_json::from_str(r#"{"ticker":"SBER","primaryBoard":"TQBR"}"#)?;
    /// assert_eq!(info.trading_params_primary().unwrap().class_code, "TQBR");
    ///
    /// let nothing: InstrumentInfo = serde_json::from_str(r#"{"ticker":"X"}"#)?;
    /// assert!(nothing.trading_params_primary().is_none());
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn trading_params_primary(&self) -> Option<TradingParamsSnapshot> {
        let class_code = if self.primary_board.is_empty() {
            self.boards.first()?.class_code.clone()
        } else {
            self.primary_board.clone()
        };
        self.trading_params_for_board(&class_code)
    }
}

// ─── limits ───────────────────────────────────────────────────────────────────

/// A position record with the fields of a portfolio position.
///
/// No client method returns it. Use [`LimitsResponse`] for the limits and [`PortfolioPosition`] for positions.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitPosition {
    /// Id of the sub-account. Deprecated by the broker.
    #[serde(default)]
    pub sub_account_id: Option<String>,
    /// Id of the general agreement. Deprecated by the broker.
    #[serde(default)]
    pub agreement_id: Option<String>,
    /// Trading account.
    #[serde(default)]
    pub account: Option<String>,
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Name of the instrument for display.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Price currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Instrument type, as text.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Settlement term code.
    #[serde(default)]
    pub term: Option<String>,
    /// Quantity in pieces.
    #[serde(default)]
    pub quantity: Option<Decimal>,
    /// Quantity held by active orders or by collateral (initial margin).
    #[serde(default)]
    pub locked: Option<Decimal>,
    /// Balance price (the price the position was opened at), in the price currency.
    #[serde(default)]
    pub balance_price: Option<Decimal>,
    /// Current price.
    #[serde(default)]
    pub current_price: Option<Decimal>,
    /// Balance value (at the opening price), in the price currency.
    #[serde(default)]
    pub balance_value: Option<Decimal>,
    /// Balance value in roubles.
    #[serde(default)]
    pub balance_value_rub: Option<Decimal>,
    /// Current value in the price currency.
    #[serde(default)]
    pub current_value: Option<Decimal>,
    /// Current value in roubles.
    #[serde(default)]
    pub current_value_rub: Option<Decimal>,
    /// Price change since the position was opened (JSON field `unrealizedPL`, with capital letters).
    #[serde(default, rename = "unrealizedPL", alias = "unrealizedPl")]
    pub unrealized_pl: Option<Decimal>,
    /// The same change in percent (JSON field `unrealizedPercentPL`).
    #[serde(default, rename = "unrealizedPercentPL", alias = "unrealizedPercentPl")]
    pub unrealized_percent_pl: Option<f64>,
    /// Price change over the day (JSON field `dailyPL`). The broker docs word it as "daily change of the price since the position was opened".
    #[serde(default, rename = "dailyPL", alias = "dailyPl")]
    pub daily_pl: Option<Decimal>,
    /// The same change in percent (JSON field `dailyPercentPL`).
    #[serde(default, rename = "dailyPercentPL", alias = "dailyPercentPl")]
    pub daily_percent_pl: Option<f64>,
    /// Share of the portfolio in percent, of its value in roubles.
    #[serde(default)]
    pub portfolio_share: Option<f64>,
    /// `true` if the class is blocked.
    #[serde(default)]
    pub is_blocked: Option<bool>,
    /// `true` if the trading account is blocked.
    #[serde(default)]
    pub is_blocked_trade_account: Option<bool>,
}

/// The account limits: positions, money and futures limits.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::LimitsResponse;
///
/// let limits: LimitsResponse = serde_json::from_str(
///     r#"{"moneyLimits":[{"currencyCode":"RUB","locked":10.0,"quantity":{"type":"T0","value":500.0}}]}"#,
/// )?;
/// assert!(limits.depo_limit.is_empty()); // a list the broker left out reads as empty
/// assert_eq!(limits.money_limits[0].currency.as_deref(), Some("RUB"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitsResponse {
    /// Securities positions, one record per instrument and settlement mode.
    #[serde(default)]
    pub depo_limit: Vec<DepoLimit>,
    /// Money balances, one record per currency and settlement mode.
    #[serde(default)]
    pub money_limits: Vec<MoneyLimit>,
    /// Limits of the derivatives market.
    #[serde(default)]
    pub futures_limits: Vec<FuturesLimit>,
}

/// A securities position in the limits.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepoLimit {
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(default)]
    pub class_code: Option<String>,
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Weighted average price at which the position was opened.
    #[serde(default)]
    pub average_price: Option<Decimal>,
    /// Quantity in pieces, with the settlement mode.
    pub quantity: Option<Quantity>,
    /// Quantity in lots, with the settlement mode.
    pub quantity_batch: Option<Quantity>,
    /// Instrument type as the broker names it.
    #[serde(default)]
    pub instrument_type: Option<String>,
}

/// A money balance in the limits.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoneyLimit {
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Currency code (JSON field `currencyCode`).
    #[serde(default, rename = "currencyCode")]
    pub currency: Option<String>,
    /// Money held by active orders.
    #[serde(default)]
    pub locked: Option<Decimal>,
    /// Weighted average opening price.
    #[serde(default)]
    pub average_price: Option<Decimal>,
    /// Instrument type as the broker names it.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Amount of money, with the settlement mode.
    pub quantity: Option<Quantity>,
    /// When the broker last updated the record, kept as text.
    #[serde(default)]
    pub load_date: Option<String>,
}

/// A limit of the derivatives market (collateral and variation margin).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuturesLimit {
    /// Currency code (JSON field `currencyCode`).
    #[serde(default, rename = "currencyCode")]
    pub currency: Option<String>,
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Current limit of open positions.
    #[serde(default)]
    pub cbp_limit: Option<Decimal>,
    /// Positions after the last clearing.
    #[serde(default)]
    pub cbpl_used: Option<Decimal>,
    /// Estimated value of the planned positions after the nearest clearing.
    #[serde(default)]
    pub cbpl_planned: Option<Decimal>,
    /// Collateral (initial margin) reserved for the client orders.
    #[serde(default)]
    pub cbpl_used_for_orders: Option<Decimal>,
    /// Collateral (initial margin) reserved for the open client positions.
    #[serde(default)]
    pub cbpl_used_for_positions: Option<Decimal>,
    /// Estimate of the variation margin on the positions.
    #[serde(default)]
    pub var_margin: Option<Decimal>,
    /// Variation margin credited during clearing.
    #[serde(default)]
    pub real_var_margin: Option<Decimal>,
    /// Variation margin for the current day (the field is spelled `accruedint` by the broker).
    #[serde(default)]
    pub accruedint: Option<Decimal>,
    /// Premiums on options.
    #[serde(default)]
    pub options_premium: Option<Decimal>,
    /// Instrument type, as text.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// When the broker last updated the record, kept as text.
    #[serde(default)]
    pub load_date: Option<String>,
}

/// An amount with its settlement mode (REST limits).
///
/// The settlement mode is the term in which the trade settles (for example `T0`, `T1`, `T365`).
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::Quantity;
///
/// let q: Quantity = serde_json::from_str(r#"{"type":"T365","value":10}"#)?;
/// assert_eq!((q.quantity_type.as_str(), q.value), ("T365", dec!(10.0)));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Quantity {
    /// Settlement mode, such as `T0` (JSON field `type`).
    #[serde(rename = "type")]
    pub quantity_type: String,
    /// The amount: pieces for `quantity`, lots for `quantityBatch`, money for a money limit.
    pub value: Decimal,
}

// ─── marginal ─────────────────────────────────────────────────────────────────

/// The margin discount rates of one instrument.
///
/// The broker docs name the fields "rate for long" and "rate for short" and do not give the unit
/// (fraction or percent). Not verified against the live API.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentDiscount {
    /// Ticker.
    pub ticker: String,
    /// Rate for a long position. `0` if the broker sent none.
    #[serde(default)]
    pub discount_long: f64,
    /// Rate for a short position. `0` if the broker sent none.
    #[serde(default)]
    pub discount_short: f64,
}

/// The answer of [`BcsHttpClient::get_instruments_discounts`](crate::http::client::BcsHttpClient::get_instruments_discounts): a list of discounts.
pub type InstrumentDiscountsResponse = Vec<InstrumentDiscount>;

// ─── market_data ──────────────────────────────────────────────────────────────

/// Parameters of [`BcsHttpClient::get_candles`](crate::http::client::BcsHttpClient::get_candles).
///
/// Sent as a query string. The broker allows at most 1440 bars per request.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::CandlesRequest;
/// use chrono::{TimeZone, Utc};
///
/// let req = CandlesRequest {
///     ticker: "SBER".into(),
///     class_code: "TQBR".into(),
///     start_date: Utc.with_ymd_and_hms(2026, 10, 19, 7, 0, 0).unwrap(),
///     end_date: Utc.with_ymd_and_hms(2026, 10, 19, 8, 0, 0).unwrap(),
///     time_frame: "M5".into(),
/// };
/// assert_eq!(
///     serde_urlencoded::to_string(&req)?,
///     "ticker=SBER&classCode=TQBR&startDate=2026-10-19T07%3A00%3A00.000Z&endDate=2026-10-19T08%3A00%3A00.000Z&timeFrame=M5"
/// );
/// # Ok::<(), serde_urlencoded::ser::Error>(())
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandlesRequest {
    /// Ticker.
    pub ticker: String,
    /// Class code (board).
    pub class_code: String,
    /// Start of the period (UTC).
    #[serde(with = "date_format")]
    pub start_date: DateTime<Utc>,
    /// End of the period (UTC).
    #[serde(with = "date_format")]
    pub end_date: DateTime<Utc>,
    /// Time frame of a bar: `M1`, `M5`, `M15`, `M30`, `H1`, `H4`, `D`, `W` or `MN`. A string, so what you send is what the broker gets.
    pub time_frame: String,
}

/// One candle (bar).
#[derive(Debug, Clone, Deserialize)]
pub struct CandleBar {
    /// Opening time of the bar (UTC).
    #[serde(with = "date_format")]
    pub time: DateTime<Utc>,
    /// Opening price.
    pub open: Decimal,
    /// Closing price.
    pub close: Decimal,
    /// Highest price.
    pub high: Decimal,
    /// Lowest price.
    pub low: Decimal,
    /// Traded volume of the bar.
    pub volume: Decimal,
}

/// Historical candles of one instrument.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::CandlesResponse;
///
/// let c: CandlesResponse = serde_json::from_str(
///     r#"{"ticker":"SBER","classCode":"TQBR","timeFrame":"M1",
///         "startDate":"2026-10-19T07:00:00.000Z","endDate":"2026-10-19T07:01:00.000Z",
///         "bars":[{"time":"2026-10-19T07:00:00.000Z","open":300,"close":301,"high":302,"low":299,"volume":1200}]}"#,
/// )?;
/// assert_eq!(c.bars[0].close, dec!(301.0));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CandlesResponse {
    /// Ticker.
    pub ticker: String,
    /// Class code (board).
    pub class_code: String,
    /// Start of the period (UTC).
    #[serde(with = "date_format")]
    pub start_date: DateTime<Utc>,
    /// End of the period (UTC).
    #[serde(with = "date_format")]
    pub end_date: DateTime<Utc>,
    /// Time frame of the bars, as the broker names it.
    pub time_frame: String,
    /// The bars of the period.
    pub bars: Vec<CandleBar>,
}

/// Parameters of [`BcsHttpClient::get_order_book`](crate::http::client::BcsHttpClient::get_order_book).
///
/// Sent as a query string.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookRequest {
    /// Ticker.
    pub ticker: String,
    /// Class code (board).
    pub class_code: String,
    /// Levels per side, 1 to 20. The broker default is 20, this field is always sent.
    pub depth: u32,
}

/// One price level of an order book: a price and the quantity at it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderBookLevel {
    /// Price of the level.
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision::serialize")]
    pub price: Decimal,
    /// Quantity at the level, in pieces.
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision::serialize")]
    pub quantity: Decimal,
}

/// A snapshot of the order book.
///
/// `bids` run from the best (highest) price down and `asks` from the best (lowest) price up, as the broker
/// sends them. The code does not sort them.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::OrderBookResponse;
///
/// let book: OrderBookResponse = serde_json::from_str(
///     r#"{"ticker":"SBER","classCode":"TQBR","depth":2,"bidVolume":190,"askVolume":170,
///         "bids":[{"price":244.30,"quantity":100},{"price":244.25,"quantity":90}],
///         "asks":[{"price":244.35,"quantity":120},{"price":244.40,"quantity":50}]}"#,
/// )?;
/// assert_eq!(book.bids[0].price, dec!(244.30));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookResponse {
    /// Ticker.
    pub ticker: String,
    /// Class code (board).
    pub class_code: String,
    /// Depth of the book that was returned.
    #[serde(default)]
    pub depth: Option<u32>,
    /// Time the book was updated (UTC), kept as the text the broker sends.
    #[serde(default)]
    pub date_time: Option<String>,
    /// Total quantity of the bids within `depth`, in pieces. It is the sum over the levels returned, not over the whole book.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub bid_volume: Option<Decimal>,
    /// Total quantity of the asks within `depth`, in pieces. It is the sum over the levels returned, not over the whole book.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub ask_volume: Option<Decimal>,
    /// Buy side, best price first.
    #[serde(default)]
    pub bids: Vec<OrderBookLevel>,
    /// Sell side, best price first.
    #[serde(default)]
    pub asks: Vec<OrderBookLevel>,
}

/// `POST /api/v1/last-trades` body (schema checked 2026-10-19): the instrument, an optional period of
/// the current session and an optional side. The API has no `limit`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::LastTradesRequest;
///
/// let req = LastTradesRequest { ticker: "SBER".into(), class_code: "TQBR".into(), side: Some("1".into()), ..Default::default() };
/// let json = serde_json::to_value(&req)?;
/// assert_eq!(json["side"], "1");
/// assert!(json.get("startDateTime").is_none()); // unset period is not sent
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LastTradesRequest {
    /// Ticker. Required.
    pub ticker: String,
    /// Class code (board). Required.
    pub class_code: String,
    /// Start of the period inside the current session (UTC). Left out when `None`.
    #[serde(skip_serializing_if = "Option::is_none", with = "date_format::option")]
    pub start_date_time: Option<DateTime<Utc>>,
    /// End of the period (UTC). Left out when `None`.
    #[serde(skip_serializing_if = "Option::is_none", with = "date_format::option")]
    pub end_date_time: Option<DateTime<Utc>>,
    /// `1` buy, `2` sell.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
}

/// One trade of the tape. The docs give `dateTime`, `side` (`1` buy, `2` sell), `price`, `volume` (in
/// the currency) and `quantity` (pieces), and NO instrument: it is the one asked for. `ticker` and
/// `classCode` are read if the broker sends them (older samples have them).
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::LastTradeRecord;
///
/// let t: LastTradeRecord = serde_json::from_str(
///     r#"{"dateTime":"2026-10-19T07:05:00.000Z","side":"1","price":300.1,"volume":60020,"quantity":200}"#,
/// )?;
/// assert_eq!(t.quantity, dec!(200.0));
/// assert!(t.ticker.is_none()); // the documented record has no instrument
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LastTradeRecord {
    /// Ticker, if the broker sends it. The documented record has none: it is the instrument you asked for.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code, if the broker sends it. The documented record has none.
    #[serde(default)]
    pub class_code: Option<String>,
    /// Side of the trade as text: `1` buy, `2` sell on REST. Older samples carry `BUY` and `SELL`.
    #[serde(default)]
    pub side: Option<String>,
    /// Trade volume in the instrument currency.
    #[serde(default)]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub volume: Option<Decimal>,
    /// Trade price.
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision::serialize")]
    pub price: Decimal,
    /// Quantity in pieces.
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision::serialize")]
    pub quantity: Decimal,
    /// Time of the trade (UTC), kept as the text the broker sends.
    #[serde(default)]
    pub date_time: Option<String>,
}

/// The anonymous trades of the session: a list of [`LastTradeRecord`]. See [`BcsHttpClient::get_last_trades`](crate::http::client::BcsHttpClient::get_last_trades).
pub type LastTradesResponse = Vec<LastTradeRecord>;

/// The two shapes the tape comes in: the docs say `{"records": [...]}`, the first samples and the
/// fixtures are a bare array. Both are read.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub(crate) enum LastTradesWire {
    Records { records: Vec<LastTradeRecord> },
    Plain(Vec<LastTradeRecord>),
}

impl LastTradesWire {
    pub(crate) fn into_vec(self) -> LastTradesResponse {
        match self {
            Self::Records { records } | Self::Plain(records) => records,
        }
    }
}

// ─── quotes ───────────────────────────────────────────────────────────────────

/// One instrument of a quotes request (`POST /api/v1/quotes`, schema `QuotesRequest.instruments[]`).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::QuoteInstrument;
///
/// let q = QuoteInstrument::new("SBER", "TQBR");
/// assert_eq!(serde_json::to_string(&q)?, r#"{"ticker":"SBER","classCode":"TQBR"}"#);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteInstrument {
    /// Ticker.
    pub ticker: String,
    /// Class code (board).
    pub class_code: String,
}

impl QuoteInstrument {
    /// Builds the key of one instrument for a quotes request.
    pub fn new(ticker: impl Into<String>, class_code: impl Into<String>) -> Self {
        Self {
            ticker: ticker.into(),
            class_code: class_code.into(),
        }
    }
}

/// `POST /api/v1/quotes` body. The docs allow 1 to 100 instruments.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::{QuoteInstrument, QuotesRequest};
///
/// let req = QuotesRequest { instruments: vec![QuoteInstrument::new("SBER", "TQBR")] };
/// assert_eq!(
///     serde_json::to_string(&req)?,
///     r#"{"instruments":[{"ticker":"SBER","classCode":"TQBR"}]}"#
/// );
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QuotesRequest {
    /// The instruments to quote. The docs allow 1 to 100.
    pub instruments: Vec<QuoteInstrument>,
}

/// Current quote of one instrument (schema `MarketDataResponse`). Every field except the key may be
/// absent: the docs mark none of them required.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::QuoteRecord;
///
/// let q: QuoteRecord = serde_json::from_str(
///     r#"{"ticker":"SBER","classCode":"TQBR","securityTradingStatus":17,"bid":305.86,"offer":305.96,"last":305.97}"#,
/// )?;
/// assert_eq!(q.security_trading_status, Some(17)); // 17 = trading is open
/// assert!(q.theoretical_price.is_none()); // a field the broker left out
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteRecord {
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(default)]
    pub class_code: Option<String>,
    /// Data update time (UTC), ISO 8601.
    #[serde(default)]
    pub date_time: Option<String>,
    /// 2 halted, 17 open, 18 closed, 100 closing, 101 opening, 102 auction, 103 closing auction,
    /// 104 discrete auction (docs, `securityTradingStatus`).
    #[serde(default)]
    pub security_trading_status: Option<i32>,
    /// Price currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Best buy price.
    #[serde(default)]
    pub bid: Option<Decimal>,
    /// Best sell price.
    #[serde(default)]
    pub offer: Option<Decimal>,
    /// Opening price of the session.
    #[serde(default)]
    pub open: Option<Decimal>,
    /// Close of the previous session.
    #[serde(default)]
    pub close: Option<Decimal>,
    /// Highest price of the day.
    #[serde(default)]
    pub high: Option<Decimal>,
    /// Lowest price of the day.
    #[serde(default)]
    pub low: Option<Decimal>,
    /// Calculated price (options).
    #[serde(default)]
    pub theoretical_price: Option<Decimal>,
    /// Price of the last trade.
    #[serde(default)]
    pub last: Option<Decimal>,
    /// Yield of the buy quote (for bonds).
    #[serde(default)]
    pub bid_yield: Option<f64>,
    /// Yield of the sell quote (for bonds).
    #[serde(default)]
    pub offer_yield: Option<f64>,
    /// Price change over the session, in the price currency.
    #[serde(default)]
    pub change: Option<Decimal>,
    /// Price change over the session, percent.
    #[serde(default)]
    pub change_rate: Option<f64>,
}

/// Response of `POST /api/v1/quotes` (schema `QuotesListResponse`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct QuotesResponse {
    /// One quote per instrument that the broker knows.
    #[serde(default)]
    pub records: Vec<QuoteRecord>,
}

// ─── non-trade operations ─────────────────────────────────────────────────────

/// `POST /api/v1/operations/search` body (schema `OperationRequest`), all filters optional.
///
/// `operation_types`: PayOut, PayIn, PayTransfer, Dividend, DividendDebt, BondPayingOff, Commission,
/// IncomeTax, SecurityOut, SecurityIn, BondPartRepayment, BondFullRepayment,
/// StructuralProductRepayment, InvestmentShareRepayment, PayOutFx, PayOutDfa, PayOutDu, PayInFx,
/// PayInDfa, VarMargin. `statuses`: Approved, InProgress, Rejected. Kept as strings so a value the
/// broker adds later does not break parsing.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OperationHistoryRequest;
///
/// let req = OperationHistoryRequest {
///     operation_types: Some(vec!["Dividend".into(), "BondPayingOff".into()]),
///     ..Default::default()
/// };
/// assert_eq!(
///     serde_json::to_value(&req)?,
///     serde_json::json!({ "operationTypes": ["Dividend", "BondPayingOff"] })
/// );
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationHistoryRequest {
    /// Operation types to include, in the broker spelling (see the list above). All types if `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_types: Option<Vec<String>>,
    /// Statuses to include: `Approved`, `InProgress`, `Rejected`. All if `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statuses: Option<Vec<String>>,
    /// Start of the period, ISO 8601 date-time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date_time: Option<String>,
    /// End of the period, ISO 8601 date-time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date_time: Option<String>,
    /// Only operations on these ISINs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isins: Option<Vec<String>>,
    /// Only operations on these tickers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tickers: Option<Vec<String>>,
    /// Only operations in these currencies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currencies: Option<Vec<String>>,
}

/// One non-trade operation (schema `Operation`): deposits, withdrawals, dividends, coupons,
/// commissions, taxes, securities in and out.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OperationRecord;
///
/// let op: OperationRecord = serde_json::from_str(
///     r#"{"id":"op-1","type":"PayIn","status":"Approved","sum":1000.0,"currency":"RUB","balanceChange":"Positive"}"#,
/// )?;
/// assert_eq!(op.operation_type.as_deref(), Some("PayIn"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationRecord {
    /// Id of the operation.
    #[serde(default)]
    pub id: Option<String>,
    /// Date of the operation, kept as text.
    #[serde(default)]
    pub date: Option<String>,
    /// IIA (tax-advantaged account) flag.
    #[serde(default)]
    pub is_iia: Option<bool>,
    /// Ticker of the issuer, for securities operations.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code of the issuer, for securities operations.
    #[serde(default)]
    pub class_code: Option<String>,
    /// Operation type, see [`OperationHistoryRequest`] for the values.
    #[serde(default, rename = "type")]
    pub operation_type: Option<String>,
    /// Approved, InProgress or Rejected.
    #[serde(default)]
    pub status: Option<String>,
    /// Amount of the operation.
    #[serde(default)]
    pub sum: Option<Decimal>,
    /// Currency of the operation.
    #[serde(default)]
    pub currency: Option<String>,
    /// The whole remaining balance is withdrawn.
    #[serde(default)]
    pub is_rests_pay_out: Option<bool>,
    /// ISIN of the instrument.
    #[serde(default)]
    pub isin: Option<String>,
    /// Name of the instrument (the broker docs say "name of the financial instrument").
    #[serde(default)]
    pub issuer_name: Option<String>,
    /// How the account was topped up.
    #[serde(default)]
    pub replenishment_type: Option<String>,
    /// Expected time to execute the operation.
    #[serde(default)]
    pub execution_period: Option<String>,
    /// Neutral, Negative or Positive.
    #[serde(default)]
    pub balance_change: Option<String>,
    /// Why the operation was rejected, if it was.
    #[serde(default)]
    pub rejection_reason: Option<String>,
}

/// Response of `POST /api/v1/operations/search` (schema `OperationListResponse`). Unlike the other
/// paginated lists, the docs give only `records` and `pageSize` (no total).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OperationHistoryResponse;
///
/// let page: OperationHistoryResponse = serde_json::from_str(r#"{"records":[],"pageSize":50}"#)?;
/// assert_eq!(page.page_size, Some(50));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationHistoryResponse {
    /// The operations of the page.
    #[serde(default)]
    pub records: Vec<OperationRecord>,
    /// Records per page, as the broker reports it. There is no total count: read pages until one is shorter than the page size.
    #[serde(default)]
    pub page_size: Option<u32>,
}

// ─── orders ───────────────────────────────────────────────────────────────────

/// Type of an order for placement (`orderType` of the create and edit requests). The wire form is a digit as text.
///
/// The connector sends market and limit orders, and conditional orders for stop orders. The other types
/// are in the broker docs and need the optional parameters of [`OrderExtras`]. How a combination
/// behaves is described in the docs of the broker and is not verified against the live API.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OrderType;
///
/// assert_eq!(serde_json::to_string(&OrderType::Limit)?, "\"2\"");
/// assert_eq!(serde_json::to_string(&OrderType::Conditional)?, "\"5\"");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    /// Market order (`"1"`).
    #[serde(rename = "1")]
    Market,
    /// Limit order (`"2"`).
    #[serde(rename = "2")]
    Limit,
    /// Algorithmic "good till date" order (`timeInForce = 3`).
    #[serde(rename = "3")]
    Algorithmic,
    /// Iceberg: `visible` is the shown quantity.
    #[serde(rename = "4")]
    Iceberg,
    /// Conditional: stop-loss and/or take-profit (`stopPrice`, `takePrice`, `execution*Type`, `spread`,
    /// `offset`). Added to the docs after the first live runs. The connector uses it for stop orders.
    #[serde(rename = "5")]
    Conditional,
}

/// Body of [`BcsHttpClient::search_orders`](crate::http::client::BcsHttpClient::search_orders).
///
/// The filters use the codes of the **order list**, which differ from the codes of order events. Status:
/// `1` cancelled, `2` executed, `3` active. Side: `1` buy, `2` sell. Types: `1` market, `2` limit,
/// `3` iceberg, `4` stop-limit, `5` take-profit (makes a limit order), `6` stop-loss, `7` take-profit and
/// stop-loss, `10` limit order for 30 days, `11` take-profit. Filters left as `None` are not sent.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OrderSearchRequest;
/// use chrono::{TimeZone, Utc};
///
/// let req = OrderSearchRequest {
///     start_date_time: Utc.with_ymd_and_hms(2026, 10, 19, 0, 0, 0).unwrap(),
///     end_date_time: Utc.with_ymd_and_hms(2026, 10, 20, 0, 0, 0).unwrap(),
///     side: Some(1),
///     order_status: Some(vec![3]),
///     order_types: None,
///     tickers: Some(vec!["SBER".into()]),
///     class_codes: None,
/// };
/// let json = serde_json::to_value(&req)?;
/// assert_eq!(json["orderStatus"], serde_json::json!([3]));
/// assert!(json.get("orderTypes").is_none());
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderSearchRequest {
    /// Start of the period (UTC).
    #[serde(with = "date_format::moex")]
    pub start_date_time: DateTime<Utc>,
    /// End of the period (UTC).
    #[serde(with = "date_format::moex")]
    pub end_date_time: DateTime<Utc>,
    /// `1` buy or `2` sell. Both if `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<i32>,
    /// Statuses of the order list: `1` cancelled, `2` executed, `3` active. All if `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_status: Option<Vec<i32>>,
    /// Types of the order list (see above). All if `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_types: Option<Vec<i32>>,
    /// Only these tickers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tickers: Option<Vec<String>>,
    /// Only these class codes (boards).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_codes: Option<Vec<String>>,
}

/// One page of the order list, with the totals.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderSearchResponse {
    /// The orders of the page.
    pub records: Vec<OrderRecord>,
    /// Number of orders for the whole filter.
    pub total_records: i64,
    /// Number of pages for the whole filter.
    pub total_pages: i32,
}

/// One order in the order list (`POST .../orders/search`).
///
/// This record is **not** the same as the order state of [`OrderExecutionData`]. It uses numbers for side,
/// type and status, with the meaning of the **list**: status `1` cancelled, `2` executed, `3` active.
/// It carries the broker's order id and no `clientOrderId`. Quantities come in pieces and in lots.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OrderRecord;
///
/// let r: OrderRecord = serde_json::from_str(
///     r#"{"orderId":"20261019-TQBR-79000001","orderNum":79000001,"ticker":"SBER","classCode":"TQBR",
///         "side":1,"orderType":2,"orderStatus":3,"price":300.0,"orderQuantity":1,"remainedQuantity":1,
///         "orderDateTime":"2026-10-19T07:05:00.000Z"}"#,
/// )?;
/// assert_eq!(r.order_status, Some(3)); // 3 = active in the list
/// assert!(r.order_date_time.is_some());
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRecord {
    /// Id of the order at the exchange, in the form `YYYYMMDD-ClassCode-OrderNumber`. The only field the code requires.
    pub order_id: String,
    /// Order number.
    #[serde(default)]
    pub order_num: Option<i64>,
    /// Client code.
    #[serde(default)]
    pub client_code: Option<String>,
    /// Executed volume in the currency.
    #[serde(default)]
    pub executed_value: Option<Decimal>,
    /// Trading date, kept as text.
    #[serde(default)]
    pub trade_date: Option<String>,
    /// Time the order was placed (UTC). `None` if absent or the epoch.
    #[serde(default, with = "date_format::option")]
    pub order_date_time: Option<DateTime<Utc>>,
    /// Time of the last update (UTC). `None` if absent or the epoch.
    #[serde(default, with = "date_format::option")]
    pub update_date_time: Option<DateTime<Utc>>,
    /// Ticker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_code: Option<String>,
    /// `1` buy, `2` sell.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<i32>,
    /// Type of the order, the codes of the order list (see [`OrderSearchRequest`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_type: Option<i32>,
    /// Status of the order, the codes of the order list: `1` cancelled, `2` executed, `3` active. These are not the codes of order events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_status: Option<i32>,
    /// Quantity of the order, in pieces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_quantity: Option<Decimal>,
    /// Executed quantity, in pieces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executed_quantity: Option<Decimal>,
    /// Price of the order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
    /// Average execution price.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_price: Option<Decimal>,
    // Fields of the current docs (`POST /orders/search`). The list does not carry our `clientOrderId`.
    /// Remaining quantity, in pieces.
    #[serde(default)]
    pub remained_quantity: Option<Decimal>,
    /// Quantity of the order, in lots.
    #[serde(default)]
    pub order_quantity_lots: Option<Decimal>,
    /// Remaining quantity, in lots.
    #[serde(default)]
    pub remained_quantity_lots: Option<Decimal>,
    /// Executed quantity, in lots.
    #[serde(default)]
    pub executed_quantity_lots: Option<Decimal>,
    /// Settlement currency.
    #[serde(default)]
    pub settlement_currency: Option<String>,
    /// Why the order was rejected, if it was.
    #[serde(default)]
    pub reject_reason: Option<String>,
    /// Conditional orders: take-profit and stop-loss trigger prices.
    #[serde(default)]
    pub take_price: Option<Decimal>,
    /// Stop-loss trigger price of a conditional order.
    #[serde(default)]
    pub stop_price: Option<Decimal>,
    /// Number of the linked order (the other leg of a TP+SL pair, or the order a stop produced).
    #[serde(default)]
    pub linked_order: Option<String>,
    /// Number of the stop order.
    #[serde(default)]
    pub stop_order: Option<String>,
    /// `1` market, `2` limit.
    #[serde(default)]
    pub market_take_profit: Option<i32>,
    /// Stop-loss execution: `1` market order, `2` limit order.
    #[serde(default)]
    pub market_stop_loss: Option<i32>,
    /// Stop price by position.
    #[serde(default)]
    pub position_price_stop: Option<Decimal>,
    /// Limit price by position.
    #[serde(default)]
    pub position_price_limit: Option<Decimal>,
}

/// Optional order parameters of the current docs (`POST /api/v1/orders`, `/orders/edit`, and their
/// `/sync` twins): time in force, stop-loss and take-profit, iceberg, trailing spread and offset.
/// Everything is optional and omitted when `None`. What a combination means for the broker is in the
/// docs. The connector fills them for its stop orders (conditional orders).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OrderExtras;
///
/// // Good Till Date: valid until the given day.
/// let extras = OrderExtras {
///     time_in_force: Some("3".into()),
///     expiry_date: Some("20261120".into()),
///     ..Default::default()
/// };
/// let json = serde_json::to_value(&extras)?;
/// assert_eq!(json["timeInForce"], "3");
/// assert!(json.get("stopPrice").is_none()); // unset fields are not sent
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderExtras {
    /// `1` Day (default), `2` Fill Or Kill, `3` Good Till Date (algorithmic orders only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    /// `YYYYMMDD`, required with `time_in_force = "3"`, at most 30 days ahead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry_date: Option<String>,
    /// Stop price of a conditional order. Required for scenarios with a stop-loss.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub stop_price: Option<Decimal>,
    /// Take-profit price of a conditional order. Required for scenarios with a take-profit.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub take_price: Option<Decimal>,
    /// Shown quantity of an iceberg order, below `order_quantity`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visible: Option<i64>,
    /// `1` market, `2` limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_take_profit_type: Option<i32>,
    /// `1` market, `2` limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_stop_loss_type: Option<i32>,
    /// Protective spread of a conditional order.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub spread: Option<Decimal>,
    /// `0` price currency, `1` percent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spread_type: Option<i32>,
    /// Offset of a conditional order.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub offset: Option<Decimal>,
    /// `0` price currency, `1` percent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_type: Option<i32>,
}

/// Body of an order placement: [`BcsHttpClient::create_order`](crate::http::client::BcsHttpClient::create_order)
/// and [`BcsHttpClient::create_order_sync`](crate::http::client::BcsHttpClient::create_order_sync).
///
/// Build it with [`CreateOrderRequest::market`] or [`CreateOrderRequest::limit`]. The quantity is in
/// **pieces**, not lots, and the price is per piece. A price you send must be a whole number of price steps
/// ([`crate::utils::precision`]) and inside the exchange band, or the exchange refuses the order.
///
/// The fields of [`OrderExtras`] are flattened into the body, all of them optional.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::CreateOrderRequest;
/// use brokapi_bcs_protocol::models::enums::OrderSide;
/// use uuid::Uuid;
///
/// let req = CreateOrderRequest::limit(Uuid::nil(), "SBER", "TQBR", OrderSide::Buy, 10, dec!(300.5));
/// assert_eq!(
///     serde_json::to_value(&req)?,
///     serde_json::json!({
///         "clientOrderId": "00000000-0000-0000-0000-000000000000",
///         "side": "1",
///         "orderType": "2",
///         "orderQuantity": 10,
///         "ticker": "SBER",
///         "classCode": "TQBR",
///         "price": 300.5
///     })
/// );
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderRequest {
    /// Your id of the order. Generate a new UUID per order and keep it: it is how you find the order later. The broker docs say it is used to track the order and to prevent duplicates.
    pub client_order_id: Uuid,
    /// Buy or sell.
    pub side: OrderSide,
    /// Type of the order.
    pub order_type: OrderType,
    /// Quantity in pieces (not lots), at least 1.
    pub order_quantity: i64,
    /// Ticker.
    pub ticker: String,
    /// Class code (board), for example `TQBR`.
    pub class_code: String,
    /// Price per piece, above 0, up to 8 decimals. Left out for a market order.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub price: Option<Decimal>,
    /// Optional parameters (time in force, stops, iceberg, spread, offset), flattened into the body.
    #[serde(flatten)]
    pub extras: OrderExtras,
}

impl CreateOrderRequest {
    /// Builds a market order. The `client_order_id` is yours: generate a new UUID per order and keep it.
    ///
    /// The quantity is in pieces. A market order has no price. The order is not sent by this call, pass
    /// the result to [`BcsHttpClient::create_order`](crate::http::client::BcsHttpClient::create_order).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::http::models::CreateOrderRequest;
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use uuid::Uuid;
    ///
    /// let req = CreateOrderRequest::market(Uuid::new_v4(), "SBER", "TQBR", OrderSide::Sell, 3);
    /// assert!(req.price.is_none());
    /// assert_eq!(serde_json::to_value(&req)?["orderType"], "1");
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn market(
        client_order_id: Uuid,
        ticker: impl Into<String>,
        class_code: impl Into<String>,
        side: OrderSide,
        quantity: i64,
    ) -> Self {
        Self {
            client_order_id,
            side,
            order_type: OrderType::Market,
            order_quantity: quantity,
            ticker: ticker.into(),
            class_code: class_code.into(),
            price: None,
            extras: OrderExtras::default(),
        }
    }

    /// Builds a limit order. The `client_order_id` is yours: generate a new UUID per order and keep it.
    ///
    /// The quantity is in pieces and the price is per piece. The price must be a whole number of price
    /// steps and inside the exchange band, or the order is refused. See [`crate::utils::precision`] to
    /// put a price on the step grid. The order is not sent by this call, pass the result to
    /// [`BcsHttpClient::create_order`](crate::http::client::BcsHttpClient::create_order).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::http::models::CreateOrderRequest;
    /// use brokapi_bcs_protocol::models::enums::OrderSide;
    /// use brokapi_bcs_protocol::utils::precision::precise_down;
    /// use rust_decimal::Decimal;
    /// use uuid::Uuid;
    ///
    /// // The price comes from a calculation: put it on the 0.05 step grid first.
    /// let price = precise_down(Decimal::new(10103, 2), Decimal::new(5, 2))?;
    /// let req = CreateOrderRequest::limit(Uuid::new_v4(), "SBER", "TQBR", OrderSide::Buy, 2, price);
    /// assert_eq!(req.price, Some(Decimal::new(10100, 2)));
    /// # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
    /// ```
    pub fn limit(
        client_order_id: Uuid,
        ticker: impl Into<String>,
        class_code: impl Into<String>,
        side: OrderSide,
        quantity: i64,
        price: Decimal,
    ) -> Self {
        Self {
            client_order_id,
            side,
            order_type: OrderType::Limit,
            order_quantity: quantity,
            ticker: ticker.into(),
            class_code: class_code.into(),
            price: Some(price),
            extras: OrderExtras::default(),
        }
    }
}

/// Body of the **deprecated** cancel, [`BcsHttpClient::cancel_order`](crate::http::client::BcsHttpClient::cancel_order).
///
/// Use [`CancelByIdRequest`] with [`BcsHttpClient::cancel_order_by_id`](crate::http::client::BcsHttpClient::cancel_order_by_id)
/// instead.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderRequest {
    /// Id of this cancel request (a new UUID).
    pub client_order_id: Uuid,
}

/// Body of the **deprecated** change, [`BcsHttpClient::modify_order`](crate::http::client::BcsHttpClient::modify_order).
///
/// Use [`EditOrderRequest`] with [`BcsHttpClient::edit_order`](crate::http::client::BcsHttpClient::edit_order) instead.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModifyOrderRequest {
    /// Id of this change request (a new UUID).
    pub client_order_id: Uuid,
    /// New quantity in pieces.
    pub order_quantity: i64,
    /// New price per piece. Left out when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub price: Option<Decimal>,
}

/// `POST /api/v1/orders/cancel` and `/orders/cancel/sync` body: the order is named in the body, not
/// in the path (the `/orders/{id}/cancel` of `CancelOrderRequest` is the deprecated form).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::CancelByIdRequest;
/// use uuid::Uuid;
///
/// let original = Uuid::nil();
/// let req = CancelByIdRequest::by_client_order_id(original, Uuid::nil());
/// let json = serde_json::to_value(&req)?;
/// assert_eq!(json["orderIdType"], "1");
/// assert_eq!(json["orderId"], "00000000-0000-0000-0000-000000000000");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelByIdRequest {
    /// `1` = `order_id` is the `clientOrderId` of the original order (default), `2` = exchange order id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id_type: Option<String>,
    /// The id of the order to cancel, of the kind named by `order_id_type`. For kind `1` a UUID text, for kind `2` the exchange id in the form `YYMMDD-ClassCode-OrderNumber`.
    pub order_id: String,
    /// Id of this request, a new UUID.
    pub client_order_id: Uuid,
}

impl CancelByIdRequest {
    /// Builds a cancel for the order whose `clientOrderId` is `original`.
    ///
    /// `request_id` is the id of the cancel request itself, a new UUID. The call sets the id kind to
    /// `1` (the `clientOrderId` of the original). To cancel by the exchange id build the struct
    /// yourself with `order_id_type = Some("2")`.
    pub fn by_client_order_id(original: Uuid, request_id: Uuid) -> Self {
        Self {
            order_id_type: Some("1".into()),
            order_id: original.hyphenated().to_string(),
            client_order_id: request_id,
        }
    }
}

/// `POST /api/v1/orders/edit` and `/orders/edit/sync` body. The edit is a cancel of the original and
/// a new order (docs), so `client_order_id` is the id of the new one.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::{EditOrderRequest, OrderExtras};
/// use uuid::Uuid;
///
/// let req = EditOrderRequest {
///     order_id_type: None,
///     order_id: Uuid::nil().to_string(),
///     client_order_id: Uuid::nil(),
///     order_type: None,
///     price: Some(dec!(301.0)),
///     order_quantity: 5,
///     extras: OrderExtras::default(),
/// };
/// let json = serde_json::to_value(&req)?;
/// assert_eq!(json["price"], 301.0);
/// assert!(json.get("orderIdType").is_none()); // the broker default (1) applies
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EditOrderRequest {
    /// `1` (default) if `order_id` is the `clientOrderId` of the original order, `2` if it is the exchange order id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id_type: Option<String>,
    /// The id of the order to change, of the kind named by `order_id_type`.
    pub order_id: String,
    /// The id of the **new** order that replaces the original. A new UUID.
    pub client_order_id: Uuid,
    /// Type of the new order. Left out when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_type: Option<OrderType>,
    /// Price of the new order per piece. Left out when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "rust_decimal::serde::arbitrary_precision_option::serialize")]
    pub price: Option<Decimal>,
    /// Quantity of the new order, in pieces.
    pub order_quantity: i64,
    /// Optional parameters of the new order, flattened into the body.
    #[serde(flatten)]
    pub extras: OrderExtras,
}

/// Answer of the synchronous methods and of `GET /api/v1/orders`: the order's current state. Unlike
/// the deprecated status call, `data` is a list.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OrderStateListResponse;
///
/// let state: OrderStateListResponse = serde_json::from_str(
///     r#"{"clientOrderId":"r-1","originalClientOrderId":"o-1","data":[{"orderStatus":"0"}]}"#,
/// )?;
/// assert_eq!(state.original_client_order_id.as_deref(), Some("o-1"));
/// assert_eq!(state.data.len(), 1);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderStateListResponse {
    /// Id of this request (not of the order).
    pub client_order_id: String,
    /// The `clientOrderId` you gave when you placed the order. Same as [`OrderStatusResponse::original_client_order_id`].
    #[serde(default)]
    pub original_client_order_id: Option<String>,
    /// The reports of the order. Empty if the broker sent none.
    #[serde(default)]
    pub data: Vec<OrderExecutionData>,
}

/// The acknowledgement of an asynchronous order command (place, cancel, edit).
///
/// It means the broker **took the request**. It does not say the order is working or filled. The state
/// comes on the order events channel or from
/// [`BcsHttpClient::get_order`](crate::http::client::BcsHttpClient::get_order).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::OrderOperationResponse;
///
/// let ack: OrderOperationResponse = serde_json::from_str(
///     r#"{"clientOrderId":"550e8400-e29b-41d4-a716-446655440000","status":"OK"}"#,
/// )?;
/// assert_eq!(ack.status, "OK");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderOperationResponse {
    /// The id you sent: the id of the order for a placement, of the request for a cancel.
    pub client_order_id: Uuid,
    /// Completion status text of the broker. The docs do not list its values.
    pub status: String,
}

/// The state of an order from the **deprecated** status method,
/// [`BcsHttpClient::get_order_status`](crate::http::client::BcsHttpClient::get_order_status).
///
/// Unlike [`OrderStateListResponse`], `data` is a single object here.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderStatusResponse {
    /// Id of the request.
    pub client_order_id: String,
    /// The `clientOrderId` you gave when you placed the order.
    pub original_client_order_id: String,
    /// The report of the order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<OrderExecutionData>,
}

/// One report about an order: its state, quantities and the last fill.
///
/// This is the REST form. The WebSocket form is [`crate::ws::models::OrderExecutionData`], which has typed
/// enums instead of text. Numbers of quantity are in **pieces**. Status and type codes are text digits:
/// status `0` new, `1` partly filled, `2` filled, `4` cancelled, `5` replaced, `6` cancel pending,
/// `8` rejected, `9` replace pending, `10` waiting for confirmation. Side `1` buy, `2` sell. Order type
/// `1` market, `2` limit, `3` algorithmic, `4` iceberg, `5` conditional.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::OrderStateListResponse;
///
/// let state: OrderStateListResponse = serde_json::from_str(
///     r#"{"clientOrderId":"r-1","originalClientOrderId":"o-1","data":[
///         {"orderStatus":"1","executionType":"1","orderQuantity":10,"executedQuantity":4,
///          "remainedQuantity":6,"ticker":"SBER","classCode":"TQBR","side":"1","orderType":"2","price":300.0}]}"#,
/// )?;
/// let report = &state.data[0];
/// assert_eq!(report.order_status.as_deref(), Some("1")); // partly filled
/// assert_eq!(report.remained_quantity, Some(dec!(6.0)));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderExecutionData {
    /// Type of the message, as text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_type: Option<String>,
    /// State of the order, a digit as text (see the struct docs).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_status: Option<String>,
    /// Type of the report, a digit as text: `0` new, `1` partial fill, `2` fill, `4` cancelled, `5` replaced, `6` cancel pending, `8` rejected, `9` suspended, `10` waiting for confirmation, `11` trade, `12` order status, `13` corrected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_type: Option<String>,
    /// Quantity of the order, in pieces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_quantity: Option<Decimal>,
    /// Executed quantity, in pieces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executed_quantity: Option<Decimal>,
    /// Quantity of the latest fill, in pieces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quantity: Option<Decimal>,
    /// Remaining quantity, in pieces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remained_quantity: Option<Decimal>,
    /// Ticker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_code: Option<String>,
    /// `1` buy, `2` sell.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    /// Type of the order, a digit as text (see the struct docs).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_type: Option<String>,
    /// Average execution price. For repo and spread deals, the price of the first part.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_price: Option<Decimal>,
    /// Id of the order at the exchange.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    /// Id of the trade (fill).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    /// Price of the order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
    /// Currency.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    /// Client code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_code: Option<String>,
    /// Time of the transaction (UTC). `None` if absent or the epoch.
    #[serde(default, with = "date_format::option")]
    pub transaction_time: Option<DateTime<Utc>>,
    /// Order number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_number: Option<String>,
    /// Accrued coupon income.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accrued_coupon: Option<Decimal>,
    /// Volume of the trade.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_value: Option<Decimal>,
    /// Commission.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commission: Option<Decimal>,
    /// Exchange id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_exchange: Option<String>,
    // Fields of the current docs (GET /orders, sync methods).
    /// Why the order was rejected, when its status is `8`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
    /// Time in force: `1` Day, `2` Fill Or Kill, `3` Good Till Date. Shown for market, limit and algorithmic orders.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<i32>,
    /// End date `YYYYMMDD`, shown when `time_in_force` is 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_date: Option<String>,
    /// Stop price of a conditional order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<Decimal>,
    /// Take-profit price of a conditional order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub take_price: Option<Decimal>,
    /// Shown quantity of an iceberg order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible: Option<i64>,
    /// Protective spread of a conditional order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread: Option<Decimal>,
    /// Unit of `spread`: `0` price currency, `1` percent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_type: Option<i32>,
    /// Offset of a conditional order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<Decimal>,
    /// Unit of `offset`: `0` price currency, `1` percent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_type: Option<i32>,
}

// ─── portfolio ────────────────────────────────────────────────────────────────

/// One record of the portfolio: a securities position or a currency balance.
///
/// Records come in several kinds (see [`PortfolioPosition::position_type`]) and each kind fills only the
/// fields that fit, so all but `type` are optional. Money values come in the price currency and also in
/// roubles, dollars and euros. "P/L" fields are the change since the position was opened.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::PortfolioPosition;
///
/// let p: PortfolioPosition = serde_json::from_str(
///     r#"{"type":"depoLimit","ticker":"SBER","board":"TQBR","quantity":10,"currentPrice":244.0,
///         "currentValueRub":2440.0,"unrealizedPL":40.0,"unrealizedPercentPL":1.67}"#,
/// )?;
/// assert_eq!(p.quantity, Some(dec!(10.0)));
/// assert_eq!(p.unrealized_pl, Some(dec!(40.0)));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioPosition {
    /// Kind of the record (JSON field `type`), for example `depoLimit` for a securities position and `moneyLimit` for a currency balance. The broker docs list no values.
    #[serde(rename = "type")]
    pub position_type: String,
    /// Id of the sub-account. Deprecated by the broker.
    #[serde(default)]
    pub sub_account_id: Option<String>,
    /// Id of the general agreement. Deprecated by the broker.
    #[serde(default)]
    pub agreement_id: Option<String>,
    /// Trading account.
    #[serde(default)]
    pub account: Option<String>,
    /// Exchange code.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Name of the instrument for display.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Ticker of the base asset, for derivatives.
    #[serde(default)]
    pub base_asset_ticker: Option<String>,
    /// Price currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Top-level instrument type, as text. The broker docs list no values.
    #[serde(default)]
    pub upper_type: Option<String>,
    /// Instrument type, as text.
    #[serde(default)]
    pub instrument_type: Option<String>,
    /// Settlement term code.
    #[serde(default)]
    pub term: Option<String>,
    /// Quantity in pieces.
    #[serde(default)]
    pub quantity: Option<Decimal>,
    /// Quantity held by active orders or by collateral (initial margin).
    #[serde(default)]
    pub locked: Option<Decimal>,
    /// Balance price (the price the position was opened at), in the price currency.
    #[serde(default)]
    pub balance_price: Option<Decimal>,
    /// Current price.
    #[serde(default)]
    pub current_price: Option<Decimal>,
    /// Balance value (at the opening price), in the price currency.
    #[serde(default)]
    pub balance_value: Option<Decimal>,
    /// Balance value in roubles.
    #[serde(default)]
    pub balance_value_rub: Option<Decimal>,
    /// Balance value in dollars.
    #[serde(default)]
    pub balance_value_usd: Option<Decimal>,
    /// Balance value in euros.
    #[serde(default)]
    pub balance_value_eur: Option<Decimal>,
    /// Current value in the price currency.
    #[serde(default)]
    pub current_value: Option<Decimal>,
    /// Current value in roubles.
    #[serde(default)]
    pub current_value_rub: Option<Decimal>,
    /// Current value in dollars.
    #[serde(default)]
    pub current_value_usd: Option<Decimal>,
    /// Current value in euros.
    #[serde(default)]
    pub current_value_eur: Option<Decimal>,
    /// Price change since the position was opened (JSON field `unrealizedPL`, with capital letters).
    #[serde(default, rename = "unrealizedPL", alias = "unrealizedPl")]
    pub unrealized_pl: Option<Decimal>,
    /// The same change in percent (JSON field `unrealizedPercentPL`).
    #[serde(default, rename = "unrealizedPercentPL", alias = "unrealizedPercentPl")]
    pub unrealized_percent_pl: Option<f64>,
    /// Price change over the day (JSON field `dailyPL`). The broker docs word it as "daily change of the price since the position was opened".
    #[serde(default, rename = "dailyPL", alias = "dailyPl")]
    pub daily_pl: Option<Decimal>,
    /// The same change in percent (JSON field `dailyPercentPL`).
    #[serde(default, rename = "dailyPercentPL", alias = "dailyPercentPl")]
    pub daily_percent_pl: Option<f64>,
    /// Share of the portfolio in percent, of its value in roubles.
    #[serde(default)]
    pub portfolio_share: Option<f64>,
    /// Number of decimal places of the price.
    #[serde(default)]
    pub scale: Option<u32>,
    /// Minimum price step.
    #[serde(default)]
    pub minimum_step: Option<Decimal>,
    /// Class code (board) of the instrument.
    #[serde(default)]
    pub board: Option<String>,
    /// Unit of the price.
    #[serde(default)]
    pub price_unit: Option<String>,
    /// Face value, for bonds.
    #[serde(default)]
    pub face_value: Option<Decimal>,
    /// Accrued coupon income, for bonds.
    #[serde(default)]
    pub accrued_income: Option<Decimal>,
    /// Link to a logo. Deprecated by the broker.
    #[serde(default)]
    pub logo_link: Option<String>,
    /// `true` if the class is blocked.
    #[serde(default)]
    pub is_blocked: Option<bool>,
    /// `true` if the trading account is blocked.
    #[serde(default)]
    pub is_blocked_trade_account: Option<bool>,
    /// Amount reserved as collateral (initial margin) on futures.
    #[serde(default)]
    pub locked_for_futures: Option<Decimal>,
    /// Quantity in a lot.
    #[serde(default)]
    pub ratio_quantity: Option<Decimal>,
    /// Expiration date, kept as text.
    #[serde(default)]
    pub expire_date: Option<String>,
}

/// The answer of [`BcsHttpClient::get_portfolio`](crate::http::client::BcsHttpClient::get_portfolio): a list of positions.
pub type PortfolioResponse = Vec<PortfolioPosition>;

// ─── trades ───────────────────────────────────────────────────────────────────

/// Body of [`BcsHttpClient::search_trades`](crate::http::client::BcsHttpClient::search_trades).
///
/// The period is text in ISO 8601 form (for example `2026-10-19T00:00:00.000Z`, UTC). The broker also has
/// filters by trade number and class code that this struct does not send.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::http::models::TradeSearchRequest;
///
/// let req = TradeSearchRequest {
///     start_date_time: "2026-10-19T00:00:00.000Z".into(),
///     end_date_time: "2026-10-20T00:00:00.000Z".into(),
///     side: Some("1".into()),
///     tickers: None,
/// };
/// assert_eq!(serde_json::to_value(&req)?["side"], "1");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeSearchRequest {
    /// Start of the period, ISO 8601 text (UTC).
    pub start_date_time: String,
    /// End of the period, ISO 8601 text (UTC).
    pub end_date_time: String,
    /// `1` buy, `2` sell. Both if `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    /// Only these tickers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tickers: Option<Vec<String>>,
}

/// One own trade (fill) in the trades list.
///
/// The current docs give a **flat** record (the trade fields sit in the record). The `client_order_id` and
/// `data` fields belong to an older shape and are `None` against a broker that follows the docs. A record
/// holds the exchange order number and the trade number, but not your `clientOrderId` and no commission.
/// Quantity comes in pieces and in lots.
///
/// # Examples
///
/// ```
/// # use rust_decimal::Decimal;
/// # use rust_decimal_macros::dec;
/// use brokapi_bcs_protocol::http::models::TradeRecord;
///
/// let t: TradeRecord = serde_json::from_str(
///     r#"{"orderNum":79000001,"tradeNum":5,"ticker":"SBER","classCode":"TQBR","side":"1",
///         "price":300.0,"volume":3000.0,"tradeQuantity":10,"tradeDateTime":"2026-10-19T07:05:00.000Z"}"#,
/// )?;
/// assert_eq!(t.trade_quantity, Some(dec!(10.0)));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeRecord {
    /// Your order id. Older shape only: the current docs record has none.
    #[serde(default)]
    pub client_order_id: Option<String>,
    /// A nested report. Older shape only: the current docs record is flat.
    #[serde(default)]
    pub data: Option<TradeData>,
    // The flat record of the docs (`POST /trades/search`, schema `TradesSearchResponse`, checked
    // 2026-10-19): the trade fields sit in the record itself, there is no `data` wrapper and no
    // `clientOrderId`. The `data` form above is what the first version of this model assumed.
    /// Number of the order at the exchange.
    #[serde(default)]
    pub order_num: Option<i64>,
    /// Number of the trade.
    #[serde(default)]
    pub trade_num: Option<i64>,
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(default)]
    pub class_code: Option<String>,
    /// `1` buy, `2` sell.
    #[serde(default)]
    pub side: Option<String>,
    /// Trade price.
    #[serde(default)]
    pub price: Option<Decimal>,
    /// Trade volume in the currency.
    #[serde(default)]
    pub volume: Option<Decimal>,
    /// Quantity in pieces.
    #[serde(default)]
    pub trade_quantity: Option<Decimal>,
    /// Quantity in lots.
    #[serde(default)]
    pub trade_quantity_lots: Option<Decimal>,
    /// Time of the trade (UTC), kept as the text the broker sends.
    #[serde(default)]
    pub trade_date_time: Option<String>,
}

/// The nested report of an older trade record shape. See [`TradeRecord`].
///
/// It has the fields of an order report. Prefer the flat fields of [`TradeRecord`].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeData {
    /// Type of the message, as text.
    #[serde(default)]
    pub message_type: Option<String>,
    /// State of the order, a digit as text.
    #[serde(default)]
    pub order_status: Option<String>,
    /// Type of the report, a digit as text.
    #[serde(default)]
    pub execution_type: Option<String>,
    /// Quantity of the order, in pieces.
    #[serde(default)]
    pub order_quantity: Option<Decimal>,
    /// Executed quantity, in pieces.
    #[serde(default)]
    pub executed_quantity: Option<Decimal>,
    /// Quantity of the latest fill, in pieces.
    #[serde(default)]
    pub last_quantity: Option<Decimal>,
    /// Remaining quantity, in pieces.
    #[serde(default)]
    pub remained_quantity: Option<Decimal>,
    /// Ticker.
    #[serde(default)]
    pub ticker: Option<String>,
    /// Class code (board).
    #[serde(default)]
    pub class_code: Option<String>,
    /// `1` buy, `2` sell.
    #[serde(default)]
    pub side: Option<String>,
    /// Type of the order, a digit as text.
    #[serde(default)]
    pub order_type: Option<String>,
    /// Average execution price.
    #[serde(default)]
    pub average_price: Option<Decimal>,
    /// Id of the order at the exchange.
    #[serde(default)]
    pub order_id: Option<String>,
    /// Id of the trade.
    #[serde(default)]
    pub execution_id: Option<String>,
    /// Price.
    #[serde(default)]
    pub price: Option<Decimal>,
    /// Currency.
    #[serde(default)]
    pub currency: Option<String>,
    /// Client code.
    #[serde(default)]
    pub client_code: Option<String>,
    /// Time of the transaction, kept as text.
    #[serde(default)]
    pub transaction_time: Option<String>,
    /// Order number.
    #[serde(default)]
    pub order_number: Option<String>,
    /// Accrued coupon income.
    #[serde(default)]
    pub accrued_coupon: Option<Decimal>,
    /// Volume of the trade.
    #[serde(default)]
    pub execution_value: Option<Decimal>,
    /// Commission.
    #[serde(default)]
    pub commission: Option<Decimal>,
    /// Exchange id.
    #[serde(default)]
    pub security_exchange: Option<String>,
}

#[cfg(test)]
mod last_trades_tests {
    use super::*;

    #[test]
    fn the_tape_is_read_in_both_shapes() {
        let wrapped: LastTradesWire = serde_json::from_str(
            r#"{"records":[{"dateTime":"2026-10-19T07:05:00.000Z","side":"1","price":300.1,"volume":60020,"quantity":200}]}"#,
        )
        .unwrap();
        let plain: LastTradesWire = serde_json::from_str(
            r#"[{"ticker":"SBER","classCode":"TQBR","side":"BUY","price":300.1,"quantity":200}]"#,
        )
        .unwrap();
        assert_eq!(wrapped.into_vec().len(), 1);
        assert_eq!(plain.into_vec()[0].ticker.as_deref(), Some("SBER"));
    }

    /// The untagged wrapper buffers the numbers before it reads them. They must keep their digits
    /// there too: `244.30` is not an `f64`.
    #[test]
    fn the_untagged_wrapper_keeps_the_digits() {
        for body in [
            r#"[{"price":244.30,"quantity":5}]"#,
            r#"{"records":[{"price":244.30,"quantity":0.0001}]}"#,
        ] {
            let wire: LastTradesWire = serde_json::from_str(body).unwrap();
            let trades = wire.into_vec();
            assert_eq!(trades[0].price.to_string(), "244.30", "{body}");
        }
    }

    #[test]
    fn request_sends_only_what_is_set() {
        let r = LastTradesRequest {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&r).unwrap(),
            serde_json::json!({"ticker":"SBER","classCode":"TQBR"})
        );
    }
}
