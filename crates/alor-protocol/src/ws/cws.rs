//! The trading channel `/cws`: order commands over one WebSocket connection.
//!
//! The same commands exist over HTTP, and the broker says the WebSocket gives only a small gain in time
//! (up to 5 ms), so the connectors use HTTP. This client is here for those who want the channel anyway. It is
//! plain on purpose: one connection, one command at a time, no reconnect. A command that gets no answer in time
//! is an unknown outcome, as at every broker: find out before you send it again.
//!
//! The connection is authorized once with `authorize` (the broker wants it every 15-20 minutes), and then every
//! command is a message with an `opcode` like `create:limit`, a `guid` of ours and the fields of the order. The
//! answer carries the `guid` back as `requestGuid`.

use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::error::AlorApiError;
use crate::http::client::AlorHttpClient;
use crate::ws::models::CWS_PATH;

type Stream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The opcodes of the order commands of the trading channel.
pub mod opcodes {
    /// Create a market order.
    pub const CREATE_MARKET: &str = "create:market";
    /// Create a limit order.
    pub const CREATE_LIMIT: &str = "create:limit";
    /// Create a stop order (a market order on trigger).
    pub const CREATE_STOP: &str = "create:stop";
    /// Create a stop-limit order.
    pub const CREATE_STOP_LIMIT: &str = "create:stopLimit";
    /// Change a market order.
    pub const UPDATE_MARKET: &str = "update:market";
    /// Change a limit order.
    pub const UPDATE_LIMIT: &str = "update:limit";
    /// Change a stop order.
    pub const UPDATE_STOP: &str = "update:stop";
    /// Change a stop-limit order.
    pub const UPDATE_STOP_LIMIT: &str = "update:stopLimit";
    /// Cancel a market order.
    pub const DELETE_MARKET: &str = "delete:market";
    /// Cancel a limit order.
    pub const DELETE_LIMIT: &str = "delete:limit";
    /// Cancel a stop order.
    pub const DELETE_STOP: &str = "delete:stop";
    /// Cancel a stop-limit order.
    pub const DELETE_STOP_LIMIT: &str = "delete:stopLimit";
}

/// The answer to a command of the trading channel.
#[derive(Debug, Clone, PartialEq)]
pub struct CwsReply {
    /// The `guid` of the command (`requestGuid` of the answer).
    pub guid: String,
    /// The HTTP-like code: 200 done, 400 refused, 401 not authorized.
    pub code: u16,
    /// The text.
    pub message: String,
    /// The number of the order, for an answer to a create or change.
    pub order_number: Option<String>,
    /// The whole answer.
    pub raw: Value,
}

/// An authorized connection to the trading channel.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_alor_protocol::config::AlorConfig::for_mock("mock-refresh-token", &server.http_base);
/// use std::time::Duration;
/// use serde_json::json;
/// use brokapi_alor_protocol::http::client::AlorHttpClient;
/// use brokapi_alor_protocol::ws::cws::{CwsSession, opcodes};
///
/// let http = AlorHttpClient::new(cfg)?;
/// let cws = CwsSession::connect(http, Duration::from_secs(5)).await?;
/// let reply = cws
///     .command(opcodes::CREATE_LIMIT, json!({
///         "side": "buy", "quantity": 1, "price": 100.0,
///         "instrument": {"symbol": "SBER", "exchange": "MOEX", "instrumentGroup": "TQBR"},
///         "user": {"portfolio": "D00001"},
///     }))
///     .await?;
/// assert_eq!(reply.code, 200);
/// assert!(reply.order_number.is_some());
/// # Ok(())
/// # }
/// ```
pub struct CwsSession {
    http: AlorHttpClient,
    stream: Mutex<Stream>,
    answer_timeout: Duration,
    authorized_at: Mutex<Instant>,
}

/// How long an authorization of the connection is trusted before the session authorizes again.
const REAUTHORIZE_AFTER: Duration = Duration::from_secs(15 * 60);

impl CwsSession {
    /// Opens the connection and authorizes it. `answer_timeout` is how long a command waits for its answer.
    ///
    /// # Errors
    ///
    /// [`AlorApiError::Ws`] when the connection or the authorization fails; the errors of
    /// [`AlorHttpClient::get_access_token`] when there is no token.
    pub async fn connect(
        http: AlorHttpClient,
        answer_timeout: Duration,
    ) -> Result<Self, AlorApiError> {
        let url = format!("{}{}", http.ws_base_url(), CWS_PATH);
        let (stream, _) = connect_async(url.as_str())
            .await
            .map_err(|e| AlorApiError::Ws {
                code: None,
                message: format!("connect to {url}: {e}"),
            })?;
        let session = Self {
            http,
            stream: Mutex::new(stream),
            answer_timeout,
            authorized_at: Mutex::new(Instant::now()),
        };
        session.authorize().await?;
        Ok(session)
    }

    /// Authorizes the connection with a fresh access token.
    ///
    /// # Errors
    ///
    /// [`AlorApiError::Ws`] when the broker refuses (code 401) or does not answer in time.
    pub async fn authorize(&self) -> Result<(), AlorApiError> {
        let token = self.http.get_access_token().await?;
        let reply = self
            .exchange(json!({"opcode": "authorize", "token": token}), false)
            .await?;
        if reply.code != 200 {
            return Err(AlorApiError::Ws {
                code: Some(reply.code),
                message: reply.message,
            });
        }
        *self.authorized_at.lock().await = Instant::now();
        Ok(())
    }

    /// Sends a command and waits for its answer. `fields` is the object of the order (`side`, `quantity`,
    /// `price`, `instrument`, `user`, ...); the session adds `opcode` and a new `guid`.
    ///
    /// A refusal of the broker (code 400) is returned as a reply, not as an error: the caller reads
    /// [`CwsReply::code`] and [`CwsReply::message`].
    ///
    /// # Errors
    ///
    /// [`AlorApiError::Ws`] when the connection breaks or no answer comes in time. Both are an unknown
    /// outcome for the command.
    pub async fn command(&self, opcode: &str, fields: Value) -> Result<CwsReply, AlorApiError> {
        if self.authorized_at.lock().await.elapsed() >= REAUTHORIZE_AFTER {
            self.authorize().await?;
        }
        let mut body = fields;
        let obj = body
            .as_object_mut()
            .ok_or_else(|| AlorApiError::Parse("the fields must be a JSON object".into()))?;
        obj.insert("opcode".into(), json!(opcode));
        self.exchange(body, true).await
    }

    async fn exchange(&self, body: Value, _is_command: bool) -> Result<CwsReply, AlorApiError> {
        Self::exchange_on(&self.stream, self.answer_timeout, body).await
    }

    async fn exchange_on<S>(
        stream: &Mutex<WebSocketStream<S>>,
        answer_timeout: Duration,
        mut body: Value,
    ) -> Result<CwsReply, AlorApiError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let guid = uuid_like();
        body["guid"] = json!(guid);
        let mut stream = stream.lock().await;
        stream
            .send(Message::Text(body.to_string().into()))
            .await
            .map_err(|e| AlorApiError::Ws {
                code: None,
                message: format!("write: {e}"),
            })?;
        let wait = async {
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(Message::Text(text)) => {
                        let Ok(v) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        if v.get("requestGuid").and_then(Value::as_str) != Some(guid.as_str()) {
                            continue;
                        }
                        return Ok(CwsReply {
                            guid: guid.clone(),
                            code: v.get("httpCode").and_then(Value::as_u64).unwrap_or(0) as u16,
                            message: v
                                .get("message")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            order_number: v
                                .get("orderNumber")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                            raw: v,
                        });
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            Err(AlorApiError::Ws {
                code: None,
                message: "the connection closed before the answer".into(),
            })
        };
        timeout(answer_timeout, wait)
            .await
            .map_err(|_| AlorApiError::Ws {
                code: None,
                message: "no answer in time".into(),
            })?
    }
}

#[cfg(test)]
mod write_fault_tests;

/// A unique id for a request: the time in nanoseconds and a counter, as text.
fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("cws-{t:x}-{}", N.fetch_add(1, Ordering::Relaxed))
}
