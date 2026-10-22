//! One typed method per REST endpoint of the ALOR OpenAPI, as `impl` blocks of [`AlorHttpClient`].
//!
//! All of the 58 operations of the broker's OpenAPI file are here: the reference data and market data, the
//! reads of the portfolio (orders, trades, positions, summary, risks), the order commands (limit, market, stop
//! and stop-limit orders, cancel, the legacy stop-loss and take-profit family), the order groups and the
//! estimates. The methods are thin: they build the URL, call [`AlorHttpClient::read`] or
//! [`AlorHttpClient::command`] and return the broker's model. They do not retry (apart from the one repeat of
//! a read after a 401), do not throttle and do not interpret the reply.
//!
//! The replies that the connector does not use are returned as [`serde_json::Value`] (numbers keep their
//! digits). The query parameters that a method does not name are given in its `query` argument as pairs of
//! the broker's names, for example `[("limit", "10".to_string())]`.
//!
//! Every read asks for the `Simple` format of the broker (the original field names).

use reqwest::Method;
use serde_json::Value;

use crate::error::AlorApiError;
use crate::http::client::AlorHttpClient;
use crate::http::models::*;

/// Percent-encodes one segment of a URL path.
fn enc(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn simple(mut query: Vec<(&'static str, String)>) -> Vec<(&'static str, String)> {
    query.push(("format", "Simple".to_string()));
    query
}

fn opt(query: &mut Vec<(&'static str, String)>, key: &'static str, value: Option<impl ToString>) {
    if let Some(v) = value {
        query.push((key, v.to_string()));
    }
}

fn with_extra<'a>(
    mut base: Vec<(&'a str, String)>,
    extra: &'a [(&'a str, String)],
) -> Vec<(&'a str, String)> {
    base.extend(extra.iter().map(|(k, v)| (*k, v.clone())));
    base
}

// ─── reference data ───────────────────────────────────────────────────────────

impl AlorHttpClient {
    /// `GET /md/v2/Securities`: a search over the instruments of all markets. `query` takes `query` (a
    /// ticker part), `limit`, `offset`, `sector` (`FORTS`, `FOND`, `CURR`), `cficode`, `exchange`,
    /// `instrumentGroup` and `includeNonBaseBoards`.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_securities(
        &self,
        query: &[(&str, String)],
    ) -> Result<Vec<SecurityInfo>, AlorApiError> {
        self.read("/md/v2/Securities", &with_extra(simple(vec![]), query))
            .await
    }

    /// `GET /md/v2/Securities/{exchange}`: the instruments of one exchange. `query` is as in
    /// [`AlorHttpClient::get_securities`] (`limit`, `offset`, `sector`, ...).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_securities_by_exchange(
        &self,
        exchange: &str,
        query: &[(&str, String)],
    ) -> Result<Vec<SecurityInfo>, AlorApiError> {
        self.read(
            &format!("/md/v2/Securities/{}", enc(exchange)),
            &with_extra(simple(vec![]), query),
        )
        .await
    }

    /// `GET /md/v2/Securities/{exchange}/{symbol}`: one instrument, in the trading mode `group` (the main one if
    /// left out).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`]; [`AlorApiError::NotFound`] for an unknown instrument.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_alor_protocol::config::AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_alor_protocol::http::client::AlorHttpClient;
    ///
    /// let client = AlorHttpClient::new(cfg)?;
    /// let sber = client.get_security("MOEX", "SBER", Some("TQBR")).await?;
    /// assert_eq!(sber.symbol, "SBER");
    /// assert!(sber.lot_size.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_security(
        &self,
        exchange: &str,
        symbol: &str,
        group: Option<&str>,
    ) -> Result<SecurityInfo, AlorApiError> {
        let mut q = simple(vec![]);
        opt(&mut q, "instrumentGroup", group);
        self.read(
            &format!("/md/v2/Securities/{}/{}", enc(exchange), enc(symbol)),
            &q,
        )
        .await
    }

    /// `GET /md/v2/Securities/{exchange}/{symbol}/availableBoards`: the trading modes the instrument trades in.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_available_boards(
        &self,
        exchange: &str,
        symbol: &str,
    ) -> Result<Value, AlorApiError> {
        self.read(
            &format!(
                "/md/v2/Securities/{}/{}/availableBoards",
                enc(exchange),
                enc(symbol)
            ),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Securities/{symbols}/quotes`: the quotations of several instruments. `symbols` are pairs of
    /// exchange and ticker.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_quotes(
        &self,
        symbols: &[(&str, &str)],
    ) -> Result<Vec<QuoteRecord>, AlorApiError> {
        let joined = symbols
            .iter()
            .map(|(e, s)| format!("{}:{}", enc(e), enc(s)))
            .collect::<Vec<_>>()
            .join(",");
        self.read(
            &format!("/md/v2/Securities/{joined}/quotes"),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Securities/currencyPairs`: the currency pairs.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_currency_pairs(&self) -> Result<Value, AlorApiError> {
        self.read("/md/v2/Securities/currencyPairs", &simple(vec![]))
            .await
    }

    /// `GET /md/v2/Securities/{exchange}/{symbol}/actualFuturesQuote`: the quotation of the nearest futures of
    /// an underlying.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_actual_futures_quote(
        &self,
        exchange: &str,
        symbol: &str,
    ) -> Result<Value, AlorApiError> {
        self.read(
            &format!(
                "/md/v2/Securities/{}/{}/actualFuturesQuote",
                enc(exchange),
                enc(symbol)
            ),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/risk/rates`: the risk rates of instruments. `query` takes `exchange`, `ticker`, `riskCategoryId`,
    /// `search`, `limit`, `offset`, ...
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_risk_rates(&self, query: &[(&str, String)]) -> Result<Value, AlorApiError> {
        self.read("/md/v2/risk/rates", &with_extra(simple(vec![]), query))
            .await
    }

    /// `GET /md/v2/time`: the server time, Unix seconds. The broker asks for it rarely (the time is not exact).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_time(&self) -> Result<i64, AlorApiError> {
        self.read("/md/v2/time", &[]).await
    }

    /// `GET /md/status/{exchange}/{market}`: the state of the trading session of a market (`FORTS`, `FOND`, `CURR`,
    /// `SPBX`).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_market_status(
        &self,
        exchange: &str,
        market: &str,
    ) -> Result<MarketStatus, AlorApiError> {
        self.read(
            &format!("/md/status/{}/{}", enc(exchange), enc(market)),
            &simple(vec![]),
        )
        .await
    }
}

// ─── market data ──────────────────────────────────────────────────────────────

impl AlorHttpClient {
    /// `GET /md/v2/orderbooks/{exchange}/{symbol}`: the order book. `depth` is 20 by default and 50 at most.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_order_book(
        &self,
        exchange: &str,
        symbol: &str,
        group: Option<&str>,
        depth: Option<u32>,
    ) -> Result<OrderBook, AlorApiError> {
        let mut q = simple(vec![]);
        opt(&mut q, "instrumentGroup", group);
        opt(&mut q, "depth", depth);
        self.read(
            &format!("/md/v2/orderbooks/{}/{}", enc(exchange), enc(symbol)),
            &q,
        )
        .await
    }

    /// `GET /md/v2/history`: candles. `tf` is a number of seconds (`60`, `3600`) or `D`, `W`, `M`, `Y`; `from` and
    /// `to` are Unix seconds; `count_back` asks for at least that many candles.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    #[allow(clippy::too_many_arguments)]
    pub async fn get_history(
        &self,
        exchange: &str,
        symbol: &str,
        group: Option<&str>,
        tf: &str,
        from: i64,
        to: i64,
        count_back: Option<u32>,
    ) -> Result<HistoryResponse, AlorApiError> {
        let mut q = simple(vec![
            ("symbol", symbol.to_string()),
            ("exchange", exchange.to_string()),
            ("tf", tf.to_string()),
            ("from", from.to_string()),
            ("to", to.to_string()),
        ]);
        opt(&mut q, "instrumentGroup", group);
        opt(&mut q, "countBack", count_back);
        self.read("/md/v2/history", &q).await
    }

    /// `GET /md/v2/Securities/{exchange}/{symbol}/alltrades`: the trades of the market of today. `query` takes
    /// `from`, `to` (Unix seconds), `fromId`, `toId`, `qtyFrom`, `qtyTo`, `priceFrom`, `priceTo`, `side`,
    /// `offset`, `take`, `descending`, `includeVirtualTrades`.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_all_trades(
        &self,
        exchange: &str,
        symbol: &str,
        group: Option<&str>,
        query: &[(&str, String)],
    ) -> Result<Vec<MarketTradeRecord>, AlorApiError> {
        let mut q = simple(vec![]);
        opt(&mut q, "instrumentGroup", group);
        self.read(
            &format!(
                "/md/v2/Securities/{}/{}/alltrades",
                enc(exchange),
                enc(symbol)
            ),
            &with_extra(q, query),
        )
        .await
    }

    /// `GET /md/v2/Securities/{exchange}/{symbol}/alltrades/history`: the trades of the market of the past days,
    /// a page at a time. `query` takes `from`, `to` (Unix seconds), `limit` (required by the broker, 1 to 50000)
    /// and `offset`.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_all_trades_history(
        &self,
        exchange: &str,
        symbol: &str,
        group: Option<&str>,
        query: &[(&str, String)],
    ) -> Result<AllTradesHistory, AlorApiError> {
        let mut q = simple(vec![]);
        opt(&mut q, "instrumentGroup", group);
        self.read(
            &format!(
                "/md/v2/Securities/{}/{}/alltrades/history",
                enc(exchange),
                enc(symbol)
            ),
            &with_extra(q, query),
        )
        .await
    }
}

// ─── the portfolio ────────────────────────────────────────────────────────────

impl AlorHttpClient {
    fn client_path(exchange: &str, portfolio: &str, tail: &str) -> String {
        format!("/md/v2/Clients/{}/{}/{tail}", enc(exchange), enc(portfolio))
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/orders`: all orders of the portfolio of today.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_orders(
        &self,
        exchange: &str,
        portfolio: &str,
    ) -> Result<Vec<OrderRecord>, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, "orders"),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/orders/{orderId}`: one order. [`AlorApiError::NotFound`] when the
    /// broker does not know the number.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_order(
        &self,
        exchange: &str,
        portfolio: &str,
        order_id: &str,
    ) -> Result<OrderRecord, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, &format!("orders/{}", enc(order_id))),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/stoporders`: all stop orders of the portfolio of today.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_stop_orders(
        &self,
        exchange: &str,
        portfolio: &str,
    ) -> Result<Vec<StopOrderRecord>, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, "stoporders"),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/stoporders/{orderId}`: one stop order.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_stop_order(
        &self,
        exchange: &str,
        portfolio: &str,
        order_id: &str,
    ) -> Result<StopOrderRecord, AlorApiError> {
        self.read(
            &Self::client_path(
                exchange,
                portfolio,
                &format!("stoporders/{}", enc(order_id)),
            ),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/legacy/{exchange}/{portfolio}/money`: the money of the portfolio (the old form).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_money_legacy(
        &self,
        exchange: &str,
        portfolio: &str,
    ) -> Result<Value, AlorApiError> {
        self.read(
            &format!(
                "/md/v2/Clients/legacy/{}/{}/money",
                enc(exchange),
                enc(portfolio)
            ),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/summary`: the summary of the portfolio.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_summary(
        &self,
        exchange: &str,
        portfolio: &str,
    ) -> Result<SummaryRecord, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, "summary"),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/positions`: the positions, with the money as positions of
    /// currencies unless `without_currency` is set.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_positions(
        &self,
        exchange: &str,
        portfolio: &str,
        without_currency: bool,
    ) -> Result<Vec<PositionRecord>, AlorApiError> {
        let mut q = simple(vec![]);
        if without_currency {
            q.push(("withoutCurrency", "true".to_string()));
        }
        self.read(&Self::client_path(exchange, portfolio, "positions"), &q)
            .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/positions/{symbol}`: the position in one instrument.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_position(
        &self,
        exchange: &str,
        portfolio: &str,
        symbol: &str,
    ) -> Result<PositionRecord, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, &format!("positions/{}", enc(symbol))),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{login}/positions`: the positions of all portfolios of a trade login.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_login_positions(
        &self,
        login: &str,
    ) -> Result<Vec<PositionRecord>, AlorApiError> {
        self.read(
            &format!("/md/v2/Clients/{}/positions", enc(login)),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/trades`: the trades of the portfolio of today.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_trades(
        &self,
        exchange: &str,
        portfolio: &str,
    ) -> Result<Vec<TradeRecord>, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, "trades"),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/{symbol}/trades`: the trades of the portfolio in one instrument.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_symbol_trades(
        &self,
        exchange: &str,
        portfolio: &str,
        symbol: &str,
    ) -> Result<Vec<TradeRecord>, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, &format!("{}/trades", enc(symbol))),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/fortsrisk`: the risks of the futures market.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_forts_risk(
        &self,
        exchange: &str,
        portfolio: &str,
    ) -> Result<Value, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, "fortsrisk"),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/v2/Clients/{exchange}/{portfolio}/risk`: the risks of the portfolio.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_risk(&self, exchange: &str, portfolio: &str) -> Result<Value, AlorApiError> {
        self.read(
            &Self::client_path(exchange, portfolio, "risk"),
            &simple(vec![]),
        )
        .await
    }

    /// `GET /md/stats/{exchange}/{portfolio}/history/trades`: the statistics of the trades (the old form).
    /// `query` takes `dateFrom`, `ticker`, `id`, `limit`, `side`, `descending`, ...
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_trade_stats(
        &self,
        exchange: &str,
        portfolio: &str,
        query: &[(&str, String)],
    ) -> Result<Value, AlorApiError> {
        self.read(
            &format!(
                "/md/stats/{}/{}/history/trades",
                enc(exchange),
                enc(portfolio)
            ),
            &with_extra(simple(vec![]), query),
        )
        .await
    }

    /// `GET /md/stats/{exchange}/{portfolio}/history/trades/{symbol}`: the same for one instrument.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_trade_stats_by_symbol(
        &self,
        exchange: &str,
        portfolio: &str,
        symbol: &str,
        query: &[(&str, String)],
    ) -> Result<Value, AlorApiError> {
        self.read(
            &format!(
                "/md/stats/{}/{}/history/trades/{}",
                enc(exchange),
                enc(portfolio),
                enc(symbol)
            ),
            &with_extra(simple(vec![]), query),
        )
        .await
    }

    /// `GET /md/v2/Stats/{exchange}/{portfolio}/history/trades`: the trades of the portfolio of the past days,
    /// a page at a time. `query` takes `dateFrom` (a date or a date and time), `from` (the number of the
    /// first trade), `limit` (1 to 1000), `ticker`, `side`, `orderByTradeDate`, `descending`, `withRepo`.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_history_trades(
        &self,
        exchange: &str,
        portfolio: &str,
        query: &[(&str, String)],
    ) -> Result<Vec<TradeRecord>, AlorApiError> {
        self.read(
            &format!(
                "/md/v2/Stats/{}/{}/history/trades",
                enc(exchange),
                enc(portfolio)
            ),
            &with_extra(simple(vec![]), query),
        )
        .await
    }

    /// `GET /md/v2/Stats/{exchange}/{portfolio}/history/trades/{symbol}`: the same for one instrument.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_history_trades_by_symbol(
        &self,
        exchange: &str,
        portfolio: &str,
        symbol: &str,
        query: &[(&str, String)],
    ) -> Result<Vec<TradeRecord>, AlorApiError> {
        self.read(
            &format!(
                "/md/v2/Stats/{}/{}/history/trades/{}",
                enc(exchange),
                enc(portfolio),
                enc(symbol)
            ),
            &with_extra(simple(vec![]), query),
        )
        .await
    }
}

// ─── order commands ───────────────────────────────────────────────────────────

const ACTIONS: &str = "/commandapi/warptrans/TRADE/v2/client/orders/actions";

impl AlorHttpClient {
    /// `POST .../orders/actions/limit`: a limit order. `req_id` is the header `X-REQID`: unique for the whole
    /// system; the broker answers a repeated id with a copy of its first answer and does not place a second order.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn create_limit_order(
        &self,
        req_id: &str,
        order: &LimitOrderRequest,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::POST,
            &format!("{ACTIONS}/limit"),
            &[],
            Some(order),
            Some(req_id),
        )
        .await
    }

    /// `POST .../orders/actions/market`: a market order. See [`AlorHttpClient::create_limit_order`] for `req_id`.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn create_market_order(
        &self,
        req_id: &str,
        order: &MarketOrderRequest,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::POST,
            &format!("{ACTIONS}/market"),
            &[],
            Some(order),
            Some(req_id),
        )
        .await
    }

    /// `PUT .../orders/actions/limit/{orderId}`: changes a limit order (the broker takes the new quantity and
    /// price and answers with the number of the order that replaced it).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn update_limit_order(
        &self,
        req_id: &str,
        order_id: &str,
        order: &LimitOrderRequest,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::PUT,
            &format!("{ACTIONS}/limit/{}", enc(order_id)),
            &[],
            Some(order),
            Some(req_id),
        )
        .await
    }

    /// `PUT .../orders/actions/market/{orderId}`: changes a market order.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn update_market_order(
        &self,
        req_id: &str,
        order_id: &str,
        order: &MarketOrderRequest,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::PUT,
            &format!("{ACTIONS}/market/{}", enc(order_id)),
            &[],
            Some(order),
            Some(req_id),
        )
        .await
    }

    /// `POST .../orders/actions/stop`: a stop order that makes a market order on trigger.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn create_stop_order(
        &self,
        req_id: &str,
        order: &StopOrderRequest,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::POST,
            &format!("{ACTIONS}/stop"),
            &[],
            Some(order),
            Some(req_id),
        )
        .await
    }

    /// `POST .../orders/actions/stopLimit`: a stop order that makes a limit order on trigger (`order.price`).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn create_stop_limit_order(
        &self,
        req_id: &str,
        order: &StopOrderRequest,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::POST,
            &format!("{ACTIONS}/stopLimit"),
            &[],
            Some(order),
            Some(req_id),
        )
        .await
    }

    /// `PUT .../orders/actions/stop/{stopOrderId}`: changes a stop order.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn update_stop_order(
        &self,
        req_id: &str,
        order_id: &str,
        order: &StopOrderRequest,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::PUT,
            &format!("{ACTIONS}/stop/{}", enc(order_id)),
            &[],
            Some(order),
            Some(req_id),
        )
        .await
    }

    /// `PUT .../orders/actions/stopLimit/{stopOrderId}`: changes a stop-limit order.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn update_stop_limit_order(
        &self,
        req_id: &str,
        order_id: &str,
        order: &StopOrderRequest,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::PUT,
            &format!("{ACTIONS}/stopLimit/{}", enc(order_id)),
            &[],
            Some(order),
            Some(req_id),
        )
        .await
    }

    /// `DELETE .../orders/{orderId}`: cancels an order, or a stop order when `stop` is set. The broker names the
    /// exchange and the portfolio in the query, not in the path.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn cancel_order(
        &self,
        exchange: &str,
        portfolio: &str,
        order_id: &str,
        stop: bool,
    ) -> Result<Value, AlorApiError> {
        let q = [
            ("portfolio", portfolio.to_string()),
            ("exchange", exchange.to_string()),
            ("stop", stop.to_string()),
            ("format", "Simple".to_string()),
        ];
        self.command(
            Method::DELETE,
            &format!(
                "/commandapi/warptrans/TRADE/v2/client/orders/{}",
                enc(order_id)
            ),
            &q,
            None::<&()>,
            None,
        )
        .await
    }

    /// `DELETE .../orders/all`: cancels all orders of the portfolio on the exchange (the stop orders when
    /// `stop` is `Some(true)`).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. The call is never repeated.
    pub async fn cancel_all_orders(
        &self,
        exchange: &str,
        portfolio: &str,
        stop: Option<bool>,
    ) -> Result<Value, AlorApiError> {
        let mut q = vec![
            ("exchange", exchange.to_string()),
            ("portfolio", portfolio.to_string()),
        ];
        opt(&mut q, "stop", stop);
        self.command(
            Method::DELETE,
            "/commandapi/warptrans/TRADE/v2/client/orders/all",
            &q,
            None::<&()>,
            None,
        )
        .await
    }

    /// `POST .../orders/estimate`: estimates one order (the quantity the portfolio can buy or sell, the cost, the
    /// commission). The body is `portfolio`, `ticker`, `exchange`, `price`, `lotQuantity` or `budget`, `board`,
    /// `includeLimitOrders`.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`]. It changes nothing at the broker, but is not repeated either.
    pub async fn estimate_order(&self, body: &Value) -> Result<Value, AlorApiError> {
        self.command(
            Method::POST,
            "/commandapi/warptrans/TRADE/v2/client/orders/estimate",
            &[],
            Some(body),
            None,
        )
        .await
    }

    /// `POST .../orders/estimate/all`: estimates a list of orders. The body is a list of the bodies of
    /// [`AlorHttpClient::estimate_order`].
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::estimate_order`].
    pub async fn estimate_orders(&self, body: &Value) -> Result<Value, AlorApiError> {
        self.command(
            Method::POST,
            "/commandapi/warptrans/TRADE/v2/client/orders/estimate/all",
            &[],
            Some(body),
            None,
        )
        .await
    }
}

// ─── order groups ─────────────────────────────────────────────────────────────

impl AlorHttpClient {
    /// `GET /commandapi/api/orderGroups`: the order groups of the user.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_order_groups(&self) -> Result<Value, AlorApiError> {
        self.read("/commandapi/api/orderGroups", &[]).await
    }

    /// `POST /commandapi/api/orderGroups`: makes a group of orders (`orders` and `executionPolicy`), where
    /// the execution or cancel of one order triggers the others.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn create_order_group(&self, body: &Value) -> Result<Value, AlorApiError> {
        self.command(
            Method::POST,
            "/commandapi/api/orderGroups",
            &[],
            Some(body),
            None,
        )
        .await
    }

    /// `GET /commandapi/api/orderGroups/{orderGroupId}`: one order group.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::read`].
    pub async fn get_order_group(&self, group_id: &str) -> Result<Value, AlorApiError> {
        self.read(
            &format!("/commandapi/api/orderGroups/{}", enc(group_id)),
            &[],
        )
        .await
    }

    /// `PUT /commandapi/api/orderGroups/{orderGroupId}`: changes an order group.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn update_order_group(
        &self,
        group_id: &str,
        body: &Value,
    ) -> Result<Value, AlorApiError> {
        self.command(
            Method::PUT,
            &format!("/commandapi/api/orderGroups/{}", enc(group_id)),
            &[],
            Some(body),
            None,
        )
        .await
    }

    /// `DELETE /commandapi/api/orderGroups/{orderGroupId}`: removes an order group (the orders stay).
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn delete_order_group(&self, group_id: &str) -> Result<Value, AlorApiError> {
        self.command(
            Method::DELETE,
            &format!("/commandapi/api/orderGroups/{}", enc(group_id)),
            &[],
            None::<&()>,
            None,
        )
        .await
    }
}

// ─── the legacy stop-loss and take-profit family ──────────────────────────────

impl AlorHttpClient {
    async fn legacy_create(
        &self,
        kind: &str,
        server: &str,
        req_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::POST,
            &format!("/warptrans/{}/v2/client/orders/actions/{kind}", enc(server)),
            &[],
            Some(body),
            Some(req_id),
        )
        .await
    }

    async fn legacy_update(
        &self,
        kind: &str,
        server: &str,
        req_id: &str,
        order_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.command(
            Method::PUT,
            &format!(
                "/warptrans/{}/v2/client/orders/actions/{kind}/{}",
                enc(server),
                enc(order_id)
            ),
            &[],
            Some(body),
            Some(req_id),
        )
        .await
    }

    /// `POST /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLoss`: the old stop-loss order. The broker
    /// marks the whole family as outdated: use [`AlorHttpClient::create_stop_order`].
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn create_stop_loss(
        &self,
        server: &str,
        req_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.legacy_create("stopLoss", server, req_id, body).await
    }

    /// `POST .../actions/stopLossLimit`: the old stop-loss order with a limit price.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn create_stop_loss_limit(
        &self,
        server: &str,
        req_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.legacy_create("stopLossLimit", server, req_id, body)
            .await
    }

    /// `POST .../actions/takeProfit`: the old take-profit order.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn create_take_profit(
        &self,
        server: &str,
        req_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.legacy_create("takeProfit", server, req_id, body).await
    }

    /// `POST .../actions/takeProfitLimit`: the old take-profit order with a limit price.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn create_take_profit_limit(
        &self,
        server: &str,
        req_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.legacy_create("takeProfitLimit", server, req_id, body)
            .await
    }

    /// `PUT .../actions/stopLoss/{orderId}`: changes an old stop-loss order.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn update_stop_loss(
        &self,
        server: &str,
        req_id: &str,
        order_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.legacy_update("stopLoss", server, req_id, order_id, body)
            .await
    }

    /// `PUT .../actions/stopLossLimit/{orderId}`: changes an old stop-loss order with a limit price.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn update_stop_loss_limit(
        &self,
        server: &str,
        req_id: &str,
        order_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.legacy_update("stopLossLimit", server, req_id, order_id, body)
            .await
    }

    /// `PUT .../actions/takeProfit/{orderId}`: changes an old take-profit order.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn update_take_profit(
        &self,
        server: &str,
        req_id: &str,
        order_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.legacy_update("takeProfit", server, req_id, order_id, body)
            .await
    }

    /// `PUT .../actions/takeProfitLimit/{orderId}`: changes an old take-profit order with a limit price.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn update_take_profit_limit(
        &self,
        server: &str,
        req_id: &str,
        order_id: &str,
        body: &Value,
    ) -> Result<OrderCommandResponse, AlorApiError> {
        self.legacy_update("takeProfitLimit", server, req_id, order_id, body)
            .await
    }

    /// `DELETE /warptrans/{tradeServerCode}/v2/client/orders/{orderId}`: cancels an order by the old path.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::command`].
    pub async fn cancel_order_legacy(
        &self,
        server: &str,
        order_id: &str,
        query: &[(&str, String)],
    ) -> Result<Value, AlorApiError> {
        self.command(
            Method::DELETE,
            &format!(
                "/warptrans/{}/v2/client/orders/{}",
                enc(server),
                enc(order_id)
            ),
            query,
            None::<&()>,
            None,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::enc;

    #[test]
    fn path_segments_are_percent_encoded() {
        assert_eq!(enc("SBER"), "SBER");
        assert_eq!(enc("Si-12.26"), "Si-12.26");
        assert_eq!(enc("a b/c"), "a%20b%2Fc");
        assert_eq!(enc("ЛУКОЙЛ"), "%D0%9B%D0%A3%D0%9A%D0%9E%D0%99%D0%9B");
    }
}
