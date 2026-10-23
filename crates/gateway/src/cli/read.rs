//! The reading commands: portfolio, orders, stop orders, order book, quote. The same code runs through a gateway
//! (`RemoteClient`) and straight at a broker (`Client`): both implement the traits of `brokapi-core`.

use brokapi::core::{
    Error, InstrumentId, MarketDataSource, OrderBook, OrderBookSource, OrderExecutor,
    PortfolioSource, StopOrderExecutor,
};
use serde_json::{Value, json};

use super::fmt::table;

/// What to read.
#[derive(Debug, Clone)]
pub enum Read {
    /// Money and positions.
    Portfolio,
    /// Active orders.
    Orders,
    /// Active stop orders.
    Stops,
    /// The order book to a depth.
    Book {
        /// The depth.
        depth: u32,
    },
    /// Best bid, best ask and the last trade.
    Quote,
}

fn json_of<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// Reads. The answer is JSON: what `--json` prints, and what [`render`] turns into text.
pub async fn read<T>(
    client: &T,
    what: &Read,
    instrument: Option<&InstrumentId>,
) -> Result<Value, Error>
where
    T: PortfolioSource + OrderExecutor + StopOrderExecutor + OrderBookSource + MarketDataSource,
{
    let need = || {
        instrument.ok_or_else(|| {
            Error::Config(brokapi::core::ConfigError::Message(
                "this reading needs an instrument".into(),
            ))
        })
    };
    Ok(match what {
        Read::Portfolio => json_of(&client.portfolio().await?),
        Read::Orders => json_of(&client.active_orders().await?),
        Read::Stops => json_of(&client.active_stop_orders().await?),
        Read::Book { depth } => json_of(&client.order_book(need()?, *depth).await?),
        Read::Quote => {
            let id = need()?;
            let book = client.order_book(id, 1).await?;
            let (bids, asks) = match &book {
                OrderBook::Snapshot { bids, asks, .. } => (bids.clone(), asks.clone()),
                OrderBook::Increment { bids, asks, .. } => (bids.clone(), asks.clone()),
            };
            let last = match client.last_trades(id, 1).await {
                Ok(trades) => trades.first().map(|t| json_of(&t.price)),
                Err(Error::Unsupported { .. }) => None,
                Err(e) => return Err(e),
            };
            json!({
                "instrument": format!("{} {}", id.key.symbol, id.key.mic),
                "bid": bids.first().map(json_of),
                "ask": asks.first().map(json_of),
                "last": last,
            })
        }
    })
}

fn text(v: &Value) -> String {
    match v {
        Value::Null => "-".into(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn instrument_of(v: &Value) -> String {
    let key = &v["instrument"]["key"];
    match (key["symbol"].as_str(), key["mic"].as_str()) {
        (Some(s), Some(m)) => format!("{s} {m}"),
        _ => "-".into(),
    }
}

/// The name of a state or a kind that serde writes as a word or as `{"Name": {...}}`.
fn name_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Object(m) => m.keys().next().cloned().unwrap_or_default(),
        other => text(other),
    }
}

/// The text a person reads.
pub fn render(what: &Read, value: &Value) -> String {
    match what {
        Read::Portfolio => {
            let mut out = String::new();
            let mut rows = vec![vec![
                "CURRENCY".into(),
                "AVAILABLE".into(),
                "BLOCKED".into(),
            ]];
            for b in value["balances"].as_array().into_iter().flatten() {
                rows.push(vec![
                    text(&b["currency"]),
                    text(&b["available"]),
                    text(&b["blocked"]),
                ]);
            }
            out.push_str(&table(&rows));
            out.push('\n');
            let mut rows = vec![vec![
                "INSTRUMENT".into(),
                "QTY".into(),
                "AVG PRICE".into(),
                "PRICE".into(),
            ]];
            for p in value["positions"].as_array().into_iter().flatten() {
                rows.push(vec![
                    instrument_of(p),
                    text(&p["qty"]),
                    text(&p["avg_price"]),
                    text(&p["current_price"]),
                ]);
            }
            out.push_str(&table(&rows));
            out
        }
        Read::Orders => {
            let mut rows = vec![
                [
                    "ORDER",
                    "INSTRUMENT",
                    "SIDE",
                    "TYPE",
                    "QTY",
                    "FILLED",
                    "PRICE",
                    "STATE",
                ]
                .map(String::from)
                .to_vec(),
            ];
            for o in value.as_array().into_iter().flatten() {
                rows.push(vec![
                    text(&o["cid"]),
                    instrument_of(o),
                    name_of(&o["side"]),
                    name_of(&o["order_type"]),
                    text(&o["qty"]),
                    text(&o["filled"]),
                    text(&o["price"]),
                    name_of(&o["state"]),
                ]);
            }
            table(&rows)
        }
        Read::Stops => {
            let mut rows = vec![
                [
                    "BROKER ID",
                    "INSTRUMENT",
                    "SIDE",
                    "KIND",
                    "QTY",
                    "STOP PRICE",
                    "LIMIT PRICE",
                    "STATE",
                ]
                .map(String::from)
                .to_vec(),
            ];
            for s in value.as_array().into_iter().flatten() {
                rows.push(vec![
                    text(&s["broker_id"]),
                    instrument_of(s),
                    name_of(&s["side"]),
                    name_of(&s["kind"]),
                    text(&s["qty"]),
                    text(&s["stop_price"]),
                    text(&s["limit_price"]),
                    name_of(&s["state"]),
                ]);
            }
            table(&rows)
        }
        Read::Book { .. } => {
            let book = value
                .get("Snapshot")
                .or_else(|| value.get("Increment"))
                .unwrap_or(value);
            let side = |name: &str| -> Vec<(String, String)> {
                book[name]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|l| (text(&l["price"]), text(&l["qty"])))
                    .collect()
            };
            let (bids, asks) = (side("bids"), side("asks"));
            let mut rows = vec![
                ["BID QTY", "BID", "ASK", "ASK QTY"]
                    .map(String::from)
                    .to_vec(),
            ];
            for i in 0..bids.len().max(asks.len()) {
                let empty = ("".to_owned(), "".to_owned());
                let (bp, bq) = bids.get(i).unwrap_or(&empty);
                let (ap, aq) = asks.get(i).unwrap_or(&empty);
                rows.push(vec![bq.clone(), bp.clone(), ap.clone(), aq.clone()]);
            }
            format!("{}\n{}", instrument_of(book), table(&rows))
        }
        Read::Quote => {
            let level = |v: &Value| {
                if v.is_null() {
                    "-".to_owned()
                } else {
                    format!("{} x {}", text(&v["price"]), text(&v["qty"]))
                }
            };
            format!(
                "{}\nbid   {}\nask   {}\nlast  {}\n",
                text(&value["instrument"]),
                level(&value["bid"]),
                level(&value["ask"]),
                text(&value["last"])
            )
        }
    }
}
