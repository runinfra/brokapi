//! Calls by method name: JSON in, JSON out. This is the form in which the Python module and the gateway's `Raw`
//! reach a [`Client`]; the typed traits of [`Client`] are the form for Rust.

use std::time::Duration;

use brokapi::core::{
    ClientOrderId, ExecutionSource, InstrumentQuery, InstrumentSource, MarketDataSource,
    OrderBookSource, OrderExecutor, PlaceOrderRequest, PlaceStopOrderRequest, PortfolioSource,
    ReplaceOrderRequest, StopOrderExecutor, StopOrderRef, SubscribeOpts, SubscriptionId, TimeRange,
    Timeframe, UnixNanos,
};
use brokapi::runtime::history::HistoryOpts;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::ClientError;
use crate::{CandlePager, Client};

/// Commands that change something at the broker. A `raw` call is one when the caller says so (`"command": true`).
pub fn is_command(method: &str, args: &Value) -> bool {
    match method {
        "place" | "cancel" | "replace" | "reconcile" | "place_stop" | "cancel_stop" => true,
        "raw" => args
            .get("command")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        _ => false,
    }
}

fn get<T: DeserializeOwned>(args: &Value, key: &str) -> Result<T, ClientError> {
    let v = args
        .get(key)
        .ok_or_else(|| ClientError::Arg(format!("missing argument {key}")))?;
    serde_json::from_value(v.clone()).map_err(|e| ClientError::Arg(format!("argument {key}: {e}")))
}

fn get_opt<T: DeserializeOwned>(args: &Value, key: &str) -> Result<Option<T>, ClientError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => serde_json::from_value(v.clone())
            .map(Some)
            .map_err(|e| ClientError::Arg(format!("argument {key}: {e}"))),
    }
}

fn range(args: &Value) -> Result<TimeRange, ClientError> {
    Ok(TimeRange::new(
        UnixNanos::from_nanos(get(args, "start")?),
        UnixNanos::from_nanos(get(args, "end")?),
    ))
}

fn out<T: Serialize>(value: T) -> Result<String, ClientError> {
    Ok(serde_json::to_string(&value)?)
}

impl Client {
    /// One call by method name, the arguments a JSON object, the result JSON text.
    ///
    /// The names are those of the traits of `brokapi-core` (`place`, `cancel`, `order_book`, `portfolio`,
    /// `subscribe_quotes`, ...), plus `caps`, `instrument` (a ticker to an instrument id) and `raw` (any method of the
    /// broker's API). The keys of `args` are the names of the parameters (`instrument`, `request`, `cid`, `opts`,
    /// `start` and `end` in nanoseconds). An unknown method or a missing key is [`ClientError::Arg`].
    ///
    /// A command ([`is_command`]) runs to its end even if the caller stops waiting: a place that is dropped half way
    /// could be at the broker already, and nobody would know.
    pub async fn call(&self, method: &str, args: &Value) -> Result<String, ClientError> {
        if self.is_closed() {
            return Err(ClientError::Closed);
        }
        if is_command(method, args) {
            let (client, method, args) = (self.clone(), method.to_owned(), args.clone());
            match tokio::spawn(async move { client.dispatch(&method, &args).await }).await {
                Ok(out) => out,
                Err(e) => Err(ClientError::Arg(format!("the command task failed: {e}"))),
            }
        } else {
            self.dispatch(method, args).await
        }
    }

    /// A reader of candle history from the arguments of a call by name: `instrument`, `timeframe`, `start`, `end`
    /// and optionally `window_secs`, `max_rate_waits`, `transport_retries`.
    pub fn candle_pages_from_json(&self, args: &Value) -> Result<CandlePager, ClientError> {
        let mut opts = HistoryOpts::default();
        if let Some(w) = args.get("window_secs").and_then(Value::as_f64) {
            opts.window = Some(Duration::from_secs_f64(w));
        }
        if let Some(n) = args.get("max_rate_waits").and_then(Value::as_u64) {
            opts.max_rate_waits = n as u32;
        }
        if let Some(n) = args.get("transport_retries").and_then(Value::as_u64) {
            opts.transport_retries = n as u32;
        }
        Ok(self.candle_pages(
            get(args, "instrument")?,
            get::<Timeframe>(args, "timeframe")?,
            range(args)?,
            opts,
        ))
    }

    async fn dispatch(&self, method: &str, args: &Value) -> Result<String, ClientError> {
        match method {
            "caps" => out(self.caps().await.ok_or(ClientError::Closed)?),
            "instrument" => {
                let ticker: String = get(args, "ticker")?;
                let board: Option<String> = get_opt(args, "board")?;
                out(self.instrument(&ticker, board.as_deref()).await?)
            }

            // Market data: reads.
            "order_book" => out(self
                .order_book(&get(args, "instrument")?, get(args, "depth")?)
                .await?),
            "last_trades" => out(self
                .last_trades(&get(args, "instrument")?, get(args, "limit")?)
                .await?),
            "candles" => {
                let tf: Timeframe = get(args, "timeframe")?;
                out(self
                    .candles(&get(args, "instrument")?, tf, range(args)?)
                    .await?)
            }
            "find" => out(self.find(get::<InstrumentQuery>(args, "query")?).await?),
            "get_instrument" => out(self.get(&get(args, "instrument")?).await?),
            "trading_params" => out(self.trading_params(&get(args, "instrument")?).await?),
            "trading_status" => out(self.trading_status(&get(args, "instrument")?).await?),
            "schedule" => {
                let exchange: String = get(args, "exchange")?;
                out(InstrumentSource::schedule(self, &exchange, range(args)?).await?)
            }

            // Market data: subscriptions.
            "subscribe_order_book" => {
                let opts: SubscribeOpts = get(args, "opts")?;
                out(self
                    .subscribe_order_book(&get(args, "instrument")?, opts)
                    .await?
                    .value())
            }
            "subscribe_trades" => {
                let opts: SubscribeOpts = get(args, "opts")?;
                out(self
                    .subscribe_market_trades(&get(args, "instrument")?, opts)
                    .await?
                    .value())
            }
            "subscribe_quotes" => {
                let opts: SubscribeOpts = get(args, "opts")?;
                out(self
                    .subscribe_quotes(&get(args, "instrument")?, opts)
                    .await?
                    .value())
            }
            "subscribe_candles" => {
                let opts: SubscribeOpts = get(args, "opts")?;
                let tf: Timeframe = get(args, "timeframe")?;
                out(self
                    .subscribe_candles(&get(args, "instrument")?, tf, opts)
                    .await?
                    .value())
            }
            "unsubscribe" => {
                let sub = SubscriptionId::new(get(args, "sub")?);
                OrderBookSource::unsubscribe(self, sub).await?;
                out(Value::Null)
            }

            // The account: reads and subscriptions.
            "portfolio" => out(PortfolioSource::portfolio(self).await?),
            "positions" => out(self.positions().await?),
            "balances" => out(self.balances().await?),
            "executions" => out(ExecutionSource::executions(self, range(args)?).await?),
            "subscribe_executions" => out(self.subscribe_executions().await?.value()),
            "subscribe_portfolio" => {
                let opts: SubscribeOpts = get(args, "opts")?;
                out(self.subscribe_portfolio(opts).await?.value())
            }
            "subscribe_order_state" => out(self.subscribe_order_state().await?.value()),

            // The account: orders.
            "reconcile" => {
                let ledger: Vec<ClientOrderId> = get(args, "ledger")?;
                out(OrderExecutor::reconcile(self, &ledger).await?)
            }
            "place" => out(self
                .place(get::<PlaceOrderRequest>(args, "request")?)
                .await?),
            "cancel" => {
                OrderExecutor::cancel(self, get(args, "cid")?).await?;
                out(Value::Null)
            }
            "replace" => out(self
                .replace(get::<ReplaceOrderRequest>(args, "request")?)
                .await?),
            "order_state" => out(self.order_state(get(args, "cid")?).await?),
            "active_orders" => out(self.active_orders().await?),

            // Stop orders: the same gate as plain orders.
            "place_stop" => out(self
                .place_stop(get::<PlaceStopOrderRequest>(args, "request")?)
                .await?),
            "cancel_stop" => {
                self.cancel_stop(get::<StopOrderRef>(args, "stop")?).await?;
                out(Value::Null)
            }
            "active_stop_orders" => out(self.active_stop_orders().await?),
            "subscribe_stop_orders" => out(self.subscribe_stop_orders().await?.value()),

            // Any method of the broker's API, by name.
            "raw" => {
                let name: String = get(args, "method")?;
                let params = args.get("args").cloned().unwrap_or(Value::Null);
                let command: bool = get_opt(args, "command")?.unwrap_or(false);
                out(self.raw(&name, &params, command).await?)
            }

            other => Err(ClientError::Arg(format!("unknown method {other:?}"))),
        }
    }
}
