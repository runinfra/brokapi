//! One method per REST endpoint the connectors use.
//!
//! Numbers and enumerations are the words of the API. Nothing here throttles, retries or interprets: it builds the
//! request, signs it when the endpoint is private, and parses the reply.

use rust_decimal::Decimal;

use super::client::BybitHttpClient;
use super::models::{
    AmendOrderRequest, CancelAllResult, CancelOrderRequest, CreateOrderRequest, ExecutionRecord,
    InstrumentInfo, Kline, KlineResult, OrderBookResult, OrderRecord, OrderRef, Page,
    PositionRecord, PublicTradeRecord, ServerTime, Ticker, WalletRecord,
};
use crate::error::BybitApiError;

impl BybitHttpClient {
    /// `GET /v5/market/time`.
    pub async fn get_server_time(&self) -> Result<ServerTime, BybitApiError> {
        self.get_public("/v5/market/time", &[]).await
    }

    /// `GET /v5/market/instruments-info` for one symbol.
    ///
    /// # Errors
    ///
    /// [`BybitApiError::Api`] with code 10001 for a symbol the exchange does not know.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bybit_protocol::config::BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
    /// use brokapi_bybit_protocol::http::client::BybitHttpClient;
    ///
    /// let client = BybitHttpClient::new(cfg)?;
    /// let info = client.get_instrument("linear", "MOCKUSDT").await?;
    /// assert!(info.price_filter.tick_size.is_sign_positive());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_instrument(
        &self,
        category: &str,
        symbol: &str,
    ) -> Result<InstrumentInfo, BybitApiError> {
        let page: Page<InstrumentInfo> = self
            .get_public(
                "/v5/market/instruments-info",
                &[("category", category.into()), ("symbol", symbol.into())],
            )
            .await?;
        page.list
            .into_iter()
            .next()
            .ok_or_else(|| BybitApiError::Api {
                code: 10001,
                message: format!("params error: symbol invalid ({symbol} is not in the answer)"),
            })
    }

    /// `GET /v5/market/instruments-info`, one page of a category.
    pub async fn get_instruments_page(
        &self,
        category: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<InstrumentInfo>, BybitApiError> {
        let mut q = vec![
            ("category", category.to_string()),
            ("limit", limit.to_string()),
        ];
        if let Some(c) = cursor.filter(|c| !c.is_empty()) {
            q.push(("cursor", c.to_string()));
        }
        self.get_public("/v5/market/instruments-info", &q).await
    }

    /// `GET /v5/market/orderbook`, `limit` levels per side (1 to 1000 for linear; the exchange default is 25).
    pub async fn get_order_book(
        &self,
        category: &str,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<OrderBookView, BybitApiError> {
        let mut q = vec![
            ("category", category.to_string()),
            ("symbol", symbol.to_string()),
        ];
        if let Some(l) = limit {
            q.push(("limit", l.to_string()));
        }
        let r: OrderBookResult = self.get_public("/v5/market/orderbook", &q).await?;
        Ok(OrderBookView {
            symbol: r.symbol,
            bids: r.bids.iter().map(|l| (l.0, l.1)).collect(),
            asks: r.asks.iter().map(|l| (l.0, l.1)).collect(),
            ts_ms: r.ts,
            update_id: r.update_id,
        })
    }

    /// `GET /v5/market/tickers` for one symbol.
    pub async fn get_ticker(&self, category: &str, symbol: &str) -> Result<Ticker, BybitApiError> {
        let page: Page<Ticker> = self
            .get_public(
                "/v5/market/tickers",
                &[("category", category.into()), ("symbol", symbol.into())],
            )
            .await?;
        page.list
            .into_iter()
            .next()
            .ok_or_else(|| BybitApiError::Api {
                code: 10001,
                message: format!("params error: symbol invalid ({symbol} is not in the answer)"),
            })
    }

    /// `GET /v5/market/recent-trade`, the newest first. `limit` is 1 to 1000.
    pub async fn get_recent_trades(
        &self,
        category: &str,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<PublicTradeRecord>, BybitApiError> {
        let page: Page<PublicTradeRecord> = self
            .get_public(
                "/v5/market/recent-trade",
                &[
                    ("category", category.into()),
                    ("symbol", symbol.into()),
                    ("limit", limit.to_string()),
                ],
            )
            .await?;
        Ok(page.list)
    }

    /// `GET /v5/market/kline`: candles from `start_ms` to `end_ms`, at most `limit` (1 to 1000), oldest first.
    /// `interval` is one of `1 3 5 15 30 60 120 240 360 720 D W M`.
    pub async fn get_klines(
        &self,
        category: &str,
        symbol: &str,
        interval: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        limit: u32,
    ) -> Result<Vec<Kline>, BybitApiError> {
        let mut q = vec![
            ("category", category.to_string()),
            ("symbol", symbol.to_string()),
            ("interval", interval.to_string()),
            ("limit", limit.to_string()),
        ];
        if let Some(s) = start_ms {
            q.push(("start", s.to_string()));
        }
        if let Some(e) = end_ms {
            q.push(("end", e.to_string()));
        }
        let r: KlineResult = self.get_public("/v5/market/kline", &q).await?;
        let mut out: Vec<Kline> = r
            .list
            .iter()
            .filter_map(|row| Kline::from_row(row))
            .collect();
        out.sort_by_key(|k| k.start_ms);
        Ok(out)
    }

    /// `POST /v5/order/create`. A command: sent once.
    pub async fn create_order(&self, body: &CreateOrderRequest) -> Result<OrderRef, BybitApiError> {
        self.post_command("/v5/order/create", body).await
    }

    /// `POST /v5/order/amend`. A command: sent once.
    pub async fn amend_order(&self, body: &AmendOrderRequest) -> Result<OrderRef, BybitApiError> {
        self.post_command("/v5/order/amend", body).await
    }

    /// `POST /v5/order/cancel`. A command: sent once.
    pub async fn cancel_order(&self, body: &CancelOrderRequest) -> Result<OrderRef, BybitApiError> {
        self.post_command("/v5/order/cancel", body).await
    }

    /// `POST /v5/order/cancel-all` for the symbol, or for the whole settle coin when `symbol` is `None`.
    pub async fn cancel_all(
        &self,
        category: &str,
        symbol: Option<&str>,
        settle_coin: &str,
    ) -> Result<CancelAllResult, BybitApiError> {
        let mut body = serde_json::json!({"category": category});
        match symbol {
            Some(s) => body["symbol"] = s.into(),
            None => body["settleCoin"] = settle_coin.into(),
        }
        self.post_command("/v5/order/cancel-all", &body).await
    }

    /// `GET /v5/order/realtime`: one page of the working orders (conditional ones included), or the order with the
    /// given `orderLinkId` or `orderId` whatever its status (the exchange keeps the 500 latest closed ones).
    #[allow(clippy::too_many_arguments)]
    pub async fn get_open_orders(
        &self,
        category: &str,
        symbol: Option<&str>,
        settle_coin: Option<&str>,
        order_id: Option<&str>,
        order_link_id: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<OrderRecord>, BybitApiError> {
        let q = order_query(
            category,
            symbol,
            settle_coin,
            order_id,
            order_link_id,
            limit,
            cursor,
        );
        self.get_private("/v5/order/realtime", &q).await
    }

    /// `GET /v5/order/history`: the orders of the last 7 days (or of the given range), closed ones included.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_order_history(
        &self,
        category: &str,
        symbol: Option<&str>,
        order_id: Option<&str>,
        order_link_id: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<OrderRecord>, BybitApiError> {
        let q = order_query(
            category,
            symbol,
            None,
            order_id,
            order_link_id,
            limit,
            cursor,
        );
        self.get_private("/v5/order/history", &q).await
    }

    /// `GET /v5/execution/list`: fills in `[start_ms, end_ms]`, at most 7 days apart.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_executions(
        &self,
        category: &str,
        symbol: Option<&str>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<ExecutionRecord>, BybitApiError> {
        let mut q = vec![
            ("category", category.to_string()),
            ("limit", limit.to_string()),
        ];
        if let Some(s) = symbol {
            q.push(("symbol", s.to_string()));
        }
        if let Some(s) = start_ms {
            q.push(("startTime", s.to_string()));
        }
        if let Some(e) = end_ms {
            q.push(("endTime", e.to_string()));
        }
        if let Some(c) = cursor.filter(|c| !c.is_empty()) {
            q.push(("cursor", c.to_string()));
        }
        self.get_private("/v5/execution/list", &q).await
    }

    /// `GET /v5/position/list` for the settle coin (every position of the category with a size above zero).
    pub async fn get_positions(
        &self,
        category: &str,
        settle_coin: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<PositionRecord>, BybitApiError> {
        let mut q = vec![
            ("category", category.to_string()),
            ("settleCoin", settle_coin.to_string()),
            ("limit", limit.to_string()),
        ];
        if let Some(c) = cursor.filter(|c| !c.is_empty()) {
            q.push(("cursor", c.to_string()));
        }
        self.get_private("/v5/position/list", &q).await
    }

    /// `GET /v5/account/wallet-balance` of the unified account.
    pub async fn get_wallet_balance(&self) -> Result<Vec<WalletRecord>, BybitApiError> {
        #[derive(serde::Deserialize)]
        struct R {
            #[serde(default)]
            list: Vec<WalletRecord>,
        }
        let r: R = self
            .get_private(
                "/v5/account/wallet-balance",
                &[("accountType", "UNIFIED".to_string())],
            )
            .await?;
        Ok(r.list)
    }

    /// `POST /v5/account/demo-apply-money`: adds virtual money to a demo account (`coin`, `amount`). The exchange
    /// allows one such request a minute, and only on the demo contour.
    pub async fn demo_apply_money(
        &self,
        coin: &str,
        amount: Decimal,
    ) -> Result<serde_json::Value, BybitApiError> {
        let body = serde_json::json!({
            "adjustType": 0,
            "utaDemoApplyMoney": [{"coin": coin, "amountStr": amount.to_string()}],
        });
        self.post_command("/v5/account/demo-apply-money", &body)
            .await
    }
}

/// An order book as plain tuples (price, size), best first.
#[derive(Debug, Clone)]
pub struct OrderBookView {
    /// The symbol.
    pub symbol: String,
    /// Bids, best first.
    pub bids: Vec<(Decimal, Decimal)>,
    /// Asks, best first.
    pub asks: Vec<(Decimal, Decimal)>,
    /// The time the exchange made the data, milliseconds.
    pub ts_ms: i64,
    /// The update id.
    pub update_id: i64,
}

fn order_query(
    category: &str,
    symbol: Option<&str>,
    settle_coin: Option<&str>,
    order_id: Option<&str>,
    order_link_id: Option<&str>,
    limit: u32,
    cursor: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut q = vec![
        ("category", category.to_string()),
        ("limit", limit.to_string()),
    ];
    if let Some(s) = symbol {
        q.push(("symbol", s.to_string()));
    }
    if let Some(s) = settle_coin {
        q.push(("settleCoin", s.to_string()));
    }
    if let Some(s) = order_id {
        q.push(("orderId", s.to_string()));
    }
    if let Some(s) = order_link_id {
        q.push(("orderLinkId", s.to_string()));
    }
    if let Some(c) = cursor.filter(|c| !c.is_empty()) {
        q.push(("cursor", c.to_string()));
    }
    q
}
