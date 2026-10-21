//! `raw`: any method of a broker's API, by name, with JSON in and JSON out.
//!
//! In Rust `connector.raw()` takes a closure that calls a typed client. Python has no such closure, so the method is
//! named and its arguments are JSON:
//!
//! - T-Bank and Finam speak gRPC. The `.proto` descriptors are compiled into the protocol crates
//!   (`FILE_DESCRIPTOR_SET`), the method is found in them by its name, the JSON becomes the request message
//!   (`prost-reflect`) and the reply becomes JSON again. The call goes through the same `raw()` of the connector as
//!   in Rust: the quota is taken by the method name, the token is the connector's, a timeout or a lost reply of a
//!   command is `Unknown`.
//! - BCS speaks REST. The operations that the library knows by name (the names of its rate limit file) are a table of
//!   verb and path. Any other path is called as `"GET /path"` or `"POST /path"`.
//!
//! Streaming methods are refused: the quota covers only the opening of a stream and the stream is then yours to read,
//! which a Python call cannot hand over.

use std::sync::OnceLock;

use brokapi::bcs::BcsRaw;
use brokapi::core::Error;
use brokapi::finam::FinamRaw;
use brokapi::tbank::TbankRaw;
use prost_reflect::{
    DescriptorPool, DeserializeOptions, DynamicMessage, MessageDescriptor, MethodDescriptor,
    SerializeOptions,
};
use serde_json::{Value, json};
use tonic::client::{Grpc, GrpcService};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::{Request, Response, Status};

use crate::errors::{CallError, config};

// ---------------------------------------------------------------------------------------------------- gRPC

fn pool(broker: &str) -> &'static DescriptorPool {
    static TBANK: OnceLock<DescriptorPool> = OnceLock::new();
    static FINAM: OnceLock<DescriptorPool> = OnceLock::new();
    let (cell, bytes) = if broker == "tbank" {
        (&TBANK, brokapi::tbank::protocol::FILE_DESCRIPTOR_SET)
    } else {
        (&FINAM, brokapi::finam::protocol::FILE_DESCRIPTOR_SET)
    };
    cell.get_or_init(|| DescriptorPool::decode(bytes).expect("the compiled descriptors are valid"))
}

/// The method named `name`: `GetAccounts`, `UsersService/GetAccounts` or `pkg.UsersService/GetAccounts`.
fn find_method(pool: &DescriptorPool, name: &str) -> Result<MethodDescriptor, CallError> {
    let mut hits = Vec::new();
    for service in pool.services() {
        for method in service.methods() {
            let short = format!("{}/{}", service.name(), method.name());
            let full = format!("{}/{}", service.full_name(), method.name());
            if method.name() == name || short == name || full == name {
                hits.push(method);
            }
        }
    }
    match hits.len() {
        1 => Ok(hits.remove(0)),
        0 => Err(CallError::Broker(config(format!(
            "this broker has no method {name:?}: use the name of a method from its API description \
             (for example GetAccounts), nothing was sent"
        )))),
        _ => {
            let names: Vec<String> = hits
                .iter()
                .map(|m| format!("{}/{}", m.parent_service().name(), m.name()))
                .collect();
            Err(CallError::Broker(config(format!(
                "the method name {name:?} is in several services, name the service too: {}",
                names.join(", ")
            ))))
        }
    }
}

/// A request that is ready to send: the method found, the arguments turned into the message.
#[derive(Clone)]
pub struct Prepared {
    /// The name the quota is taken by: the method name without the service.
    pub quota: String,
    path: PathAndQuery,
    request: DynamicMessage,
    response: MessageDescriptor,
}

/// Finds the method and builds its request from `args`. Fails before anything is sent or counted.
pub fn prepare(broker: &str, method: &str, args: &Value) -> Result<Prepared, CallError> {
    let pool = pool(broker);
    let m = find_method(pool, method)?;
    if m.is_client_streaming() || m.is_server_streaming() {
        return Err(CallError::Broker(Error::Unsupported {
            capability: "raw_streaming".into(),
        }));
    }
    let args = if args.is_null() {
        json!({})
    } else {
        args.clone()
    };
    let options = DeserializeOptions::new().deny_unknown_fields(true);
    let request =
        DynamicMessage::deserialize_with_options(m.input(), args, &options).map_err(|e| {
            CallError::Broker(config(format!(
                "the arguments do not fit {}: {e}",
                m.input().name()
            )))
        })?;
    let path = format!("/{}/{}", m.parent_service().full_name(), m.name())
        .parse::<PathAndQuery>()
        .map_err(|e| CallError::Arg(e.to_string()))?;
    Ok(Prepared {
        quota: m.name().to_string(),
        path,
        request,
        response: m.output(),
    })
}

/// The reply as JSON: the field names of the `.proto`, 64 bit integers as text, defaults written out.
fn reply_json(message: &DynamicMessage) -> Result<Value, CallError> {
    let options = SerializeOptions::new()
        .use_proto_field_name(true)
        .skip_default_fields(false);
    message
        .serialize_with_options(serde_json::value::Serializer, &options)
        .map_err(|e| CallError::Arg(format!("the reply cannot be written as JSON: {e}")))
}

struct DynCodec {
    response: MessageDescriptor,
}

struct DynEncoder;

struct DynDecoder {
    response: MessageDescriptor,
}

impl Codec for DynCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
    type Encoder = DynEncoder;
    type Decoder = DynDecoder;

    fn encoder(&mut self) -> DynEncoder {
        DynEncoder
    }

    fn decoder(&mut self) -> DynDecoder {
        DynDecoder {
            response: self.response.clone(),
        }
    }
}

impl Encoder for DynEncoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn encode(&mut self, item: DynamicMessage, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        prost::Message::encode(&item, dst)
            .map_err(|e| Status::internal(format!("cannot encode the request: {e}")))
    }
}

impl Decoder for DynDecoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<DynamicMessage>, Status> {
        DynamicMessage::decode(self.response.clone(), src)
            .map(Some)
            .map_err(|e| Status::internal(format!("cannot decode the reply: {e}")))
    }
}

/// One unary call over the authorized channel of a connector.
async fn unary<T>(mut grpc: Grpc<T>, prepared: Prepared) -> Result<Response<DynamicMessage>, Status>
where
    T: GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>,
{
    grpc.ready().await.map_err(|e| {
        let e: tonic::codegen::StdError = e.into();
        Status::unknown(format!("the service was not ready: {e}"))
    })?;
    grpc.unary(
        Request::new(prepared.request),
        prepared.path,
        DynCodec {
            response: prepared.response,
        },
    )
    .await
}

/// T-Bank: a method of the API by name.
pub async fn tbank(
    raw: TbankRaw,
    method: &str,
    args: &Value,
    command: bool,
) -> Result<Value, CallError> {
    let prepared = prepare("tbank", method, args)?;
    let quota = prepared.quota.clone();
    let call = move |client: brokapi::tbank::protocol::TbankClient| async move {
        unary(client.grpc(), prepared).await
    };
    let reply = if command {
        raw.call_command(&quota, call).await?
    } else {
        raw.call(&quota, call).await?
    };
    reply_json(&reply)
}

/// Finam: a method of the API by name.
pub async fn finam(
    raw: FinamRaw,
    method: &str,
    args: &Value,
    command: bool,
) -> Result<Value, CallError> {
    let prepared = prepare("finam", method, args)?;
    let quota = prepared.quota.clone();
    // The closure may run twice (after a renewal of the token), so it keeps its own message.
    let call = move |api: brokapi::finam::protocol::FinamApi| {
        let prepared = prepared.clone();
        async move { unary(api.grpc(), prepared).await }
    };
    let reply = if command {
        raw.call_command(&quota, call).await?
    } else {
        raw.call(&quota, call).await?
    };
    reply_json(&reply)
}

// ---------------------------------------------------------------------------------------------------- BCS

#[derive(Clone, Copy, PartialEq)]
enum Verb {
    Get,
    Post,
}

/// The operations of BCS that the library names (the routes of its rate limit file) and where they live.
/// `{id}` in a path is filled from `args["path"]["id"]`.
const BCS_OPS: &[(&str, Verb, &str)] = &[
    (
        "GetDailySchedule",
        Verb::Get,
        "/trade-api-information-service/api/v1/trading-schedule/daily-schedule",
    ),
    (
        "GetTradingStatus",
        Verb::Get,
        "/trade-api-information-service/api/v1/trading-schedule/status",
    ),
    (
        "GetInstrumentsByIsins",
        Verb::Post,
        "/trade-api-information-service/api/v1/instruments/by-isins",
    ),
    (
        "GetInstrumentsByTickers",
        Verb::Post,
        "/trade-api-information-service/api/v1/instruments/by-tickers",
    ),
    (
        "GetInstrumentsByType",
        Verb::Get,
        "/trade-api-information-service/api/v1/instruments/by-type",
    ),
    ("GetLimits", Verb::Get, "/trade-api-bff-limit/api/v1/limits"),
    (
        "GetInstrumentsDiscounts",
        Verb::Get,
        "/trade-api-bff-marginal-indicators/api/v1/instruments-discounts",
    ),
    (
        "GetCandles",
        Verb::Get,
        "/trade-api-market-data-connector/api/v1/candles-chart",
    ),
    (
        "GetOrderBook",
        Verb::Get,
        "/trade-api-market-data-connector/api/v1/order-book",
    ),
    (
        "GetLastTrades",
        Verb::Post,
        "/trade-api-market-data-connector/api/v1/last-trades",
    ),
    (
        "GetQuotes",
        Verb::Post,
        "/trade-api-market-data-connector/api/v1/quotes",
    ),
    (
        "SearchOrders",
        Verb::Post,
        "/trade-api-bff-order-details/api/v1/orders/search",
    ),
    (
        "GetOrder",
        Verb::Get,
        "/trade-api-bff-operations/api/v1/orders",
    ),
    (
        "GetOrderStatus",
        Verb::Get,
        "/trade-api-bff-operations/api/v1/orders/{id}",
    ),
    (
        "GetPortfolio",
        Verb::Get,
        "/trade-api-bff-portfolio/api/v1/portfolio",
    ),
    (
        "SearchTrades",
        Verb::Post,
        "/trade-api-bff-trade-details/api/v1/trades/search",
    ),
    (
        "GetOperationHistory",
        Verb::Post,
        "/trade-api-bff-nontrade-operations/api/v1/operations/search",
    ),
];

/// Text of a JSON scalar for a URL.
fn url_text(value: &Value) -> Result<String, CallError> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        other => Err(CallError::Broker(config(format!(
            "a query value must be a string, a number or a boolean, got {other}"
        )))),
    }
}

/// `path` + `?query`. A list value repeats the key.
fn with_query(path: &str, query: Option<&Value>) -> Result<String, CallError> {
    let Some(query) = query.filter(|q| !q.is_null()) else {
        return Ok(path.to_string());
    };
    let Value::Object(map) = query else {
        return Err(CallError::Broker(config(
            "args[\"query\"] must be an object",
        )));
    };
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (key, value) in map {
        match value {
            Value::Null => {}
            Value::Array(items) => {
                for item in items {
                    pairs.push((key.clone(), url_text(item)?));
                }
            }
            other => pairs.push((key.clone(), url_text(other)?)),
        }
    }
    if pairs.is_empty() {
        return Ok(path.to_string());
    }
    let encoded = serde_urlencoded::to_string(&pairs)
        .map_err(|e| CallError::Broker(config(format!("the query cannot be written: {e}"))))?;
    Ok(format!("{path}?{encoded}"))
}

/// Where a BCS call goes: the verb, the path with its query and the body.
fn bcs_target(method: &str, args: &Value) -> Result<(Verb, String), CallError> {
    let args = if args.is_null() { &json!({}) } else { args };
    let query = args.get("query");
    let (verb, path) = if let Some((_, verb, template)) =
        BCS_OPS.iter().find(|(name, _, _)| *name == method)
    {
        let mut path = (*template).to_string();
        if path.contains("{id}") {
            let id = args
                .get("path")
                .and_then(|p| p.get("id"))
                .map(url_text)
                .transpose()?
                .ok_or_else(|| {
                    CallError::Broker(config(format!(
                        "{method} needs args[\"path\"][\"id\"] (the id in the path)"
                    )))
                })?;
            if id.is_empty() || id.contains(['/', '?', '#']) {
                return Err(CallError::Broker(config("the id in the path is not valid")));
            }
            path = path.replace("{id}", &id);
        }
        (*verb, path)
    } else if let Some((verb, path)) = method.split_once(' ') {
        let verb = match verb {
            "GET" => Verb::Get,
            "POST" => Verb::Post,
            other => {
                return Err(CallError::Broker(config(format!(
                    "the verb {other:?} is not supported, use GET or POST"
                ))));
            }
        };
        if !path.starts_with('/') || path.contains(['?', '#']) {
            return Err(CallError::Broker(config(
                "after the verb comes a path that starts with / and has no query (the query goes to args[\"query\"])",
            )));
        }
        (verb, path.to_string())
    } else {
        return Err(CallError::Broker(config(format!(
            "BCS has no operation {method:?}: use a name from its rate limit file (GetQuotes, GetPortfolio, ...) or \
             \"GET /path\" / \"POST /path\", nothing was sent"
        ))));
    };
    Ok((verb, with_query(&path, query)?))
}

/// BCS: an operation by name, or any path.
pub async fn bcs(
    raw: BcsRaw,
    method: &str,
    args: &Value,
    command: bool,
) -> Result<Value, CallError> {
    let (verb, path) = bcs_target(method, args)?;
    let body = args
        .get("body")
        .cloned()
        .filter(|b| !b.is_null())
        .unwrap_or_else(|| json!({}));
    let reply: Value = match (verb, command) {
        (Verb::Get, false) => {
            raw.call(method, |http| async move { http.get::<Value>(&path).await })
                .await?
        }
        (Verb::Get, true) => {
            raw.call_command(method, |http| async move { http.get::<Value>(&path).await })
                .await?
        }
        (Verb::Post, false) => {
            raw.call(method, |http| async move {
                http.post::<Value, _>(&path, &body).await
            })
            .await?
        }
        (Verb::Post, true) => {
            raw.call_command(method, |http| async move {
                http.post_command::<Value, _>(&path, &body).await
            })
            .await?
        }
    };
    Ok(reply)
}
