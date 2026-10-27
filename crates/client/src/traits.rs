//! [`Client`] speaks the traits of `brokapi-core`: each call goes to the connector of the broker.

use async_trait::async_trait;
use brokapi::core::{
    Balance, Candle, ClientOrderId, Error, Execution, ExecutionSource, Instrument, InstrumentId,
    InstrumentQuery, InstrumentSource, MarketDataSource, MarketTrade, OrderAck, OrderBook,
    OrderBookSource, OrderExecutor, OrderSnapshot, PlaceOrderRequest, PlaceStopOrderRequest,
    Portfolio, PortfolioSource, Position, ReconcileReport, ReplaceOrderRequest, Schedule,
    StopOrderExecutor, StopOrderRef, StopOrderSnapshot, SubscribeOpts, SubscriptionId, TimeRange,
    Timeframe, TradingParams, TradingStatus,
};

use crate::Client;

#[async_trait]
impl OrderBookSource for Client {
    async fn order_book(&self, id: &InstrumentId, depth: u32) -> Result<OrderBook, Error> {
        self.conn().await?.md().order_book(id, depth).await
    }

    async fn subscribe_order_book(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        self.conn().await?.md().subscribe_order_book(id, opts).await
    }

    async fn unsubscribe(&self, sub: SubscriptionId) -> Result<(), Error> {
        self.conn().await?.md().unsubscribe(sub).await
    }
}

#[async_trait]
impl MarketDataSource for Client {
    async fn last_trades(&self, id: &InstrumentId, limit: u32) -> Result<Vec<MarketTrade>, Error> {
        self.conn().await?.md().last_trades(id, limit).await
    }

    async fn subscribe_market_trades(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        self.conn()
            .await?
            .md()
            .subscribe_market_trades(id, opts)
            .await
    }

    async fn subscribe_quotes(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        self.conn().await?.md().subscribe_quotes(id, opts).await
    }

    async fn candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        range: TimeRange,
    ) -> Result<Vec<Candle>, Error> {
        self.conn().await?.md().candles(id, tf, range).await
    }

    fn candles_max_span(&self, tf: Timeframe) -> Option<std::time::Duration> {
        // A synchronous answer: the connectors are there unless the client is closed, and a closed one has no limit to name.
        let guard = self.inner.conn.try_read().ok()?;
        guard.as_ref()?.md().candles_max_span(tf)
    }

    async fn subscribe_candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        self.conn()
            .await?
            .md()
            .subscribe_candles(id, tf, opts)
            .await
    }
}

#[async_trait]
impl InstrumentSource for Client {
    async fn find(&self, q: InstrumentQuery) -> Result<Vec<Instrument>, Error> {
        self.conn().await?.md().find(q).await
    }

    async fn get(&self, id: &InstrumentId) -> Result<Instrument, Error> {
        self.conn().await?.md().get(id).await
    }

    /// Also tells the account connector the price step, so that a price off the step is refused before anything is
    /// sent.
    async fn trading_params(&self, id: &InstrumentId) -> Result<TradingParams, Error> {
        let conn = self.conn().await?;
        let params = conn.md().trading_params(id).await?;
        conn.learn_grid(&params);
        Ok(params)
    }

    async fn trading_status(&self, id: &InstrumentId) -> Result<TradingStatus, Error> {
        self.conn().await?.md().trading_status(id).await
    }

    async fn schedule(&self, exchange: &str, range: TimeRange) -> Result<Schedule, Error> {
        self.conn().await?.md().schedule(exchange, range).await
    }
}

#[async_trait]
impl OrderExecutor for Client {
    async fn place(&self, req: PlaceOrderRequest) -> Result<OrderAck, Error> {
        self.inner.guard.place(&req)?;
        self.conn().await?.ud().place(req).await
    }

    async fn cancel(&self, cid: ClientOrderId) -> Result<(), Error> {
        self.inner.guard.command("cancel")?;
        self.conn().await?.ud().cancel(cid).await
    }

    async fn replace(&self, req: ReplaceOrderRequest) -> Result<OrderAck, Error> {
        let guard = &self.inner.guard;
        guard.command("replace")?;
        let conn = self.conn().await?;
        if guard.max_order_qty.is_some() || guard.max_order_value.is_some() {
            // What the new order will be: the given quantity and price, the rest from the order it replaces.
            let (qty, price) = match (req.qty, req.price) {
                (Some(qty), Some(price)) => (qty, Some(price)),
                (qty, price) => {
                    let old = conn.ud().order_state(req.original_cid).await?;
                    (qty.unwrap_or(old.qty), price.or(old.price))
                }
            };
            guard.caps(qty, price)?;
        }
        conn.ud().replace(req).await
    }

    async fn order_state(&self, cid: ClientOrderId) -> Result<OrderSnapshot, Error> {
        self.conn().await?.ud().order_state(cid).await
    }

    async fn active_orders(&self) -> Result<Vec<OrderSnapshot>, Error> {
        self.conn().await?.ud().active_orders().await
    }

    async fn subscribe_order_state(&self) -> Result<SubscriptionId, Error> {
        self.conn().await?.ud().subscribe_order_state().await
    }

    async fn reconcile(&self, ledger: &[ClientOrderId]) -> Result<ReconcileReport, Error> {
        self.conn().await?.ud().reconcile(ledger).await
    }
}

#[async_trait]
impl ExecutionSource for Client {
    async fn executions(&self, range: TimeRange) -> Result<Vec<Execution>, Error> {
        self.conn().await?.ud().executions(range).await
    }

    async fn subscribe_executions(&self) -> Result<SubscriptionId, Error> {
        self.conn().await?.ud().subscribe_executions().await
    }
}

#[async_trait]
impl PortfolioSource for Client {
    async fn portfolio(&self) -> Result<Portfolio, Error> {
        self.conn().await?.ud().portfolio().await
    }

    async fn positions(&self) -> Result<Vec<Position>, Error> {
        self.conn().await?.ud().positions().await
    }

    async fn balances(&self) -> Result<Vec<Balance>, Error> {
        self.conn().await?.ud().balances().await
    }

    async fn subscribe_portfolio(&self, opts: SubscribeOpts) -> Result<SubscriptionId, Error> {
        self.conn().await?.ud().subscribe_portfolio(opts).await
    }
}

#[async_trait]
impl StopOrderExecutor for Client {
    async fn place_stop(&self, req: PlaceStopOrderRequest) -> Result<OrderAck, Error> {
        self.inner.guard.place_stop(&req)?;
        self.conn().await?.ud().place_stop(req).await
    }

    async fn cancel_stop(&self, stop: StopOrderRef) -> Result<(), Error> {
        self.inner.guard.command("cancel_stop")?;
        self.conn().await?.ud().cancel_stop(stop).await
    }

    async fn active_stop_orders(&self) -> Result<Vec<StopOrderSnapshot>, Error> {
        self.conn().await?.ud().active_stop_orders().await
    }

    async fn subscribe_stop_orders(&self) -> Result<SubscriptionId, Error> {
        self.conn().await?.ud().subscribe_stop_orders().await
    }
}
