//! [`RemoteClient`]: a bot's side of the gateway, speaking the traits of `brokapi-core`.
//!
//! Code written against [`OrderExecutor`], [`PortfolioSource`], [`OrderBookSource`] and the other traits of the core
//! runs on a `RemoteClient` as it runs on a connector: the same calls, the same events, the same [`Error`]s. The
//! difference is the road: the call goes to the gateway, which holds the connection to the broker.
//!
//! One rule differs on purpose. A command (place, cancel, replace, ...) whose reply is lost on the road, because the
//! connection broke or a deadline passed, is [`Error::Unknown`], like a command whose reply is lost on the road to the
//! broker: the gateway may have run it. It is never sent again by the client. And a command runs to its end when the
//! caller stops waiting.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use brokapi::core::{
    Balance, Candle, ClientOrderId, ConfigError, Envelope, Error, Execution, ExecutionSource,
    Instrument, InstrumentId, InstrumentQuery, InstrumentSource, MarketDataCaps, MarketDataSource,
    MarketTrade, OrderAck, OrderBook, OrderBookSource, OrderExecutor, OrderSnapshot,
    PlaceOrderRequest, PlaceStopOrderRequest, Portfolio, PortfolioSource, Position,
    ReconcileReport, ReplaceOrderRequest, Schedule, StopOrderExecutor, StopOrderRef,
    StopOrderSnapshot, SubscribeOpts, SubscriptionId, TimeRange, Timeframe, TradingParams,
    TradingStatus, TransportError, UserDataCaps,
};
use tokio::sync::mpsc;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use crate::access::{ADDRESS_FILE, Address, BOT_HEADER, KEY_HEADER, read_key, valid_bot_name};
use crate::convert::{self as cv, Invalid};
use crate::error::error_of;
use crate::pb;

type GrpcClient = pb::gateway_client::GatewayClient<Channel>;

/// The largest message the client takes or sends.
const MAX_MESSAGE: usize = 64 * 1024 * 1024;

fn transport(text: impl Into<String>) -> Error {
    Error::Transport(TransportError::Message(text.into()))
}

fn config(text: impl Into<String>) -> Error {
    Error::Config(ConfigError::Message(text.into()))
}

/// A reply that does not follow the `.proto`: a gateway of another version.
fn bad_reply(e: Invalid) -> Error {
    transport(format!("the gateway sent a reply that cannot be read: {e}"))
}

/// How a bot connects.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectOptions {
    /// The HTTP/2 flow control window, in bytes, for a stream and for the connection. The gateway sends no more than this
    /// to a bot that does not read: a small window keeps the memory of a stalled stream small. Without it the default of
    /// the transport.
    pub http2_window: Option<u32>,
}

/// A bot's connection to a gateway. A clone is the same connection.
#[derive(Clone)]
pub struct RemoteClient {
    inner: Arc<Inner>,
}

struct Inner {
    channel: Channel,
    auth: MetadataValue<Ascii>,
    bot: MetadataValue<Ascii>,
    name: String,
}

/// Opens the channel to a gateway (or a service) at an address.
pub(crate) async fn open_channel(
    address: &Address,
    options: ConnectOptions,
) -> Result<Channel, Error> {
    let tune = |endpoint: Endpoint| match options.http2_window {
        Some(window) => endpoint
            .initial_stream_window_size(window)
            .initial_connection_window_size(window),
        None => endpoint,
    };
    match address {
        #[cfg(unix)]
        Address::Unix(path) => {
            let path = path.clone();
            tune(Endpoint::from_static("http://gateway.local"))
                .connect_with_connector(tower::service_fn(move |_: tonic::codegen::http::Uri| {
                    let path = path.clone();
                    async move {
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                            tokio::net::UnixStream::connect(path).await?,
                        ))
                    }
                }))
                .await
        }
        #[cfg(not(unix))]
        Address::Unix(_) => {
            return Err(config("Unix sockets are not available on this platform"));
        }
        Address::Tcp(addr) => {
            tune(
                Endpoint::from_shared(format!("http://{addr}"))
                    .map_err(|e| config(e.to_string()))?,
            )
            .connect()
            .await
        }
    }
    .map_err(|e| transport(format!("cannot connect to {address}: {e}")))
}

impl RemoteClient {
    /// Connects to the gateway at `address` as the bot `bot`, with the access `key`.
    pub async fn connect(address: &Address, key: &str, bot: &str) -> Result<Self, Error> {
        Self::connect_with(address, key, bot, ConnectOptions::default()).await
    }

    /// [`RemoteClient::connect`] with options.
    pub async fn connect_with(
        address: &Address,
        key: &str,
        bot: &str,
        options: ConnectOptions,
    ) -> Result<Self, Error> {
        if !valid_bot_name(bot) {
            return Err(config(format!(
                "the bot name {bot:?} is not valid: 1 to 64 letters, digits, '.', '_' or '-'"
            )));
        }
        let channel = open_channel(address, options).await?;
        let auth = format!("Bearer {key}")
            .parse()
            .map_err(|_| config("the access key is not valid text"))?;
        let bot_value = bot
            .parse()
            .map_err(|_| config("the bot name is not valid text"))?;
        Ok(Self {
            inner: Arc::new(Inner {
                channel,
                auth,
                bot: bot_value,
                name: bot.to_owned(),
            }),
        })
    }

    /// Connects to the gateway whose state directory is `dir`: the address and the key are read from there.
    pub async fn connect_dir(dir: &Path, bot: &str) -> Result<Self, Error> {
        Self::connect_dir_with(dir, bot, ConnectOptions::default()).await
    }

    /// [`RemoteClient::connect_dir`] with options.
    pub async fn connect_dir_with(
        dir: &Path,
        bot: &str,
        options: ConnectOptions,
    ) -> Result<Self, Error> {
        let address: Address = std::fs::read_to_string(dir.join(ADDRESS_FILE))
            .map_err(|e| config(format!("no gateway address in {}: {e}", dir.display())))?
            .trim()
            .parse()
            .map_err(|e: String| config(e))?;
        let key = read_key(dir).map_err(|e| {
            config(format!(
                "cannot read the access key in {}: {e}",
                dir.display()
            ))
        })?;
        Self::connect_with(&address, &key, bot, options).await
    }

    /// The name this bot gave.
    pub fn bot(&self) -> &str {
        &self.inner.name
    }

    fn grpc(&self) -> GrpcClient {
        pb::gateway_client::GatewayClient::new(self.inner.channel.clone())
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE)
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut req = Request::new(message);
        req.metadata_mut()
            .insert(KEY_HEADER, self.inner.auth.clone());
        req.metadata_mut()
            .insert(BOT_HEADER, self.inner.bot.clone());
        req
    }

    /// One call. A command runs to its end even if the caller stops waiting.
    async fn call<Req, Resp, F, Fut>(
        &self,
        command: bool,
        message: Req,
        f: F,
    ) -> Result<Resp, Error>
    where
        Req: Send + 'static,
        Resp: Send + 'static,
        F: FnOnce(GrpcClient, Request<Req>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
    {
        let (client, request) = (self.grpc(), self.request(message));
        let work = async move {
            f(client, request)
                .await
                .map(Response::into_inner)
                .map_err(|status| error_of(&status, command))
        };
        if command {
            brokapi::runtime::run_detached(work)
                .await
                .unwrap_or(Err(Error::Unknown))
        } else {
            work.await
        }
    }

    // ---- the calls that are not in the traits

    /// What the gateway says about itself: its state, its bots, the number of the last event.
    pub async fn health(&self) -> Result<pb::HealthResponse, Error> {
        self.call(false, pb::HealthRequest {}, |mut c, r| async move {
            c.health(r).await
        })
        .await
    }

    /// The capabilities of the two connectors.
    pub async fn caps(&self) -> Result<(MarketDataCaps, UserDataCaps), Error> {
        let r = self
            .call(false, pb::CapsRequest {}, |mut c, r| async move {
                c.caps(r).await
            })
            .await?;
        Ok((
            cv::md_caps_from(&r.md).map_err(bad_reply)?,
            cv::ud_caps_from(&r.ud).map_err(bad_reply)?,
        ))
    }

    /// The instrument of a ticker (`"SBER"`, or `"SBER@TQBR"`), as the broker names it.
    pub async fn resolve_instrument(
        &self,
        ticker: &str,
        board: Option<&str>,
    ) -> Result<InstrumentId, Error> {
        let msg = pb::ResolveInstrumentRequest {
            ticker: ticker.to_owned(),
            board: board.map(str::to_owned),
        };
        let r = self
            .call(false, msg, |mut c, r| async move {
                c.resolve_instrument(r).await
            })
            .await?;
        cv::instrument_from(&Some(r)).map_err(bad_reply)
    }

    /// Any method of the broker's own API by name, arguments and reply as JSON. `command` says that the call changes
    /// something at the broker.
    pub async fn raw(
        &self,
        method: &str,
        args: &serde_json::Value,
        command: bool,
    ) -> Result<serde_json::Value, Error> {
        let msg = pb::RawRequest {
            method: method.to_owned(),
            args_json: args.to_string(),
            command,
        };
        let r = self
            .call(command, msg, |mut c, r| async move { c.raw(r).await })
            .await?;
        serde_json::from_str(&r.reply_json)
            .map_err(|e| transport(format!("the gateway sent a reply that is not JSON: {e}")))
    }

    /// Opens the stream of events. The first frame is read before this returns: from then on the gateway has this stream
    /// registered, and every event that happens is for it. Subscribe after this call, not before.
    ///
    /// `resume` is where to continue after a break (see [`EventStream::resume_point`]). Without it the stream is live
    /// only. `own_orders_only` leaves out the order and execution events of the orders of other bots.
    pub async fn events(
        &self,
        resume: Option<Resume>,
        own_orders_only: bool,
    ) -> Result<EventStream, Error> {
        let msg = pb::EventsRequest {
            epoch: resume.map_or(0, |r| r.epoch),
            last_seq: resume.map_or(0, |r| r.last_seq),
            own_orders_only,
        };
        let mut client = self.grpc();
        let mut stream = client
            .events(self.request(msg))
            .await
            .map_err(|s| error_of(&s, false))?
            .into_inner();
        let first = stream
            .message()
            .await
            .map_err(|s| error_of(&s, false))?
            .and_then(|f| f.frame);
        let Some(pb::event_frame::Frame::Ready(ready)) = first else {
            return Err(transport(
                "the gateway did not start the event stream with `ready`",
            ));
        };
        let last_seq = match resume {
            Some(r) if r.epoch == ready.epoch => r.last_seq,
            _ => ready.next_seq.saturating_sub(1),
        };
        Ok(EventStream {
            stream,
            epoch: ready.epoch,
            next_seq: ready.next_seq,
            last_seq,
        })
    }

    /// The live events as a channel of envelopes, like the one a connector writes to. The channel closes when the stream
    /// ends (the gateway stopped, the connection broke, or this bot was too slow). A gap in the numbers is logged and the
    /// stream goes on: use [`RemoteClient::events`] when a gap or a cut matters to you.
    pub async fn event_channel(&self, buffer: usize) -> Result<mpsc::Receiver<Envelope>, Error> {
        let mut stream = self.events(None, false).await?;
        let (tx, rx) = mpsc::channel(buffer.max(1));
        tokio::spawn(async move {
            while let Some(Ok(next)) = stream.next().await {
                match next {
                    RemoteEvent::Event { envelope, .. } => {
                        if tx.send(envelope).await.is_err() {
                            return;
                        }
                    }
                    RemoteEvent::Gap(gap) => {
                        tracing::warn!(?gap, "events were lost; reconcile");
                    }
                    RemoteEvent::Lagged { .. } => return,
                }
            }
        });
        Ok(rx)
    }
}

/// Where a bot continues its event stream after a break.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resume {
    /// The epoch of the gateway run the numbers belong to.
    pub epoch: u64,
    /// The number of the last event the bot handled.
    pub last_seq: u64,
}

/// One thing that comes on the event stream.
// The event is the case that almost always happens: it is not boxed to make the rare ones smaller.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum RemoteEvent {
    /// An event of the connection, with its number.
    Event {
        /// The number of the event in the gateway.
        seq: u64,
        /// The event, as the connector made it.
        envelope: Envelope,
    },
    /// The events after the number the bot gave are no longer there: call `reconcile` and read the account again.
    Gap(pb::Gap),
    /// The gateway closed this stream because the bot read too slowly. Reconnect with [`EventStream::resume_point`].
    Lagged {
        /// The number of the last event that was delivered.
        last_seq: u64,
    },
}

/// The event stream of a bot, from [`RemoteClient::events`].
pub struct EventStream {
    stream: Streaming<pb::EventFrame>,
    epoch: u64,
    next_seq: u64,
    last_seq: u64,
}

impl EventStream {
    /// The epoch of the gateway run.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The number the first live event has.
    pub fn first_live_seq(&self) -> u64 {
        self.next_seq
    }

    /// Where to continue if this stream breaks: the epoch and the number of the last event read from it.
    pub fn resume_point(&self) -> Resume {
        Resume {
            epoch: self.epoch,
            last_seq: self.last_seq,
        }
    }

    /// The next event, gap or cut. `None` when the stream ended without a word (the gateway stopped, the connection
    /// broke: reconnect with [`EventStream::resume_point`]).
    pub async fn next(&mut self) -> Option<Result<RemoteEvent, Error>> {
        use pb::event_frame::Frame;
        loop {
            let frame = match self.stream.message().await {
                Ok(Some(frame)) => frame.frame?,
                Ok(None) => return None,
                Err(status) => return Some(Err(error_of(&status, false))),
            };
            return Some(match frame {
                Frame::Ready(_) => continue,
                Frame::Event(event) => cv::event_from(&event).map_err(bad_reply).map(|envelope| {
                    self.last_seq = event.seq;
                    RemoteEvent::Event {
                        seq: event.seq,
                        envelope,
                    }
                }),
                Frame::Gap(gap) => {
                    // The stream goes on from now: this is the new point to resume from.
                    self.last_seq = self.next_seq.saturating_sub(1);
                    Ok(RemoteEvent::Gap(gap))
                }
                Frame::Lagged(l) => Ok(RemoteEvent::Lagged {
                    last_seq: l.last_seq,
                }),
            });
        }
    }
}

// ---------------------------------------------------------------------------------------------------- the traits

fn sub(r: pb::SubscribeResponse) -> SubscriptionId {
    SubscriptionId::new(r.sub)
}

#[async_trait]
impl OrderBookSource for RemoteClient {
    async fn order_book(&self, id: &InstrumentId, depth: u32) -> Result<OrderBook, Error> {
        let msg = pb::OrderBookRequest {
            instrument: Some(cv::instrument_to(id)),
            depth,
        };
        let r = self
            .call(false, msg, |mut c, r| async move { c.order_book(r).await })
            .await?;
        cv::book_from(&r).map_err(bad_reply)
    }

    async fn subscribe_order_book(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let msg = pb::SubscribeInstrumentRequest {
            instrument: Some(cv::instrument_to(id)),
            opts: Some(cv::opts_to(&opts)),
        };
        self.call(false, msg, |mut c, r| async move {
            c.subscribe_order_book(r).await
        })
        .await
        .map(sub)
    }

    async fn unsubscribe(&self, subscription: SubscriptionId) -> Result<(), Error> {
        let msg = pb::UnsubscribeRequest {
            sub: subscription.value(),
        };
        self.call(false, msg, |mut c, r| async move { c.unsubscribe(r).await })
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl MarketDataSource for RemoteClient {
    async fn last_trades(&self, id: &InstrumentId, limit: u32) -> Result<Vec<MarketTrade>, Error> {
        let msg = pb::LastTradesRequest {
            instrument: Some(cv::instrument_to(id)),
            limit,
        };
        let r = self
            .call(false, msg, |mut c, r| async move { c.last_trades(r).await })
            .await?;
        r.trades
            .iter()
            .map(cv::trade_from)
            .collect::<Result<_, _>>()
            .map_err(bad_reply)
    }

    async fn subscribe_market_trades(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let msg = pb::SubscribeInstrumentRequest {
            instrument: Some(cv::instrument_to(id)),
            opts: Some(cv::opts_to(&opts)),
        };
        self.call(false, msg, |mut c, r| async move {
            c.subscribe_market_trades(r).await
        })
        .await
        .map(sub)
    }

    async fn subscribe_quotes(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let msg = pb::SubscribeInstrumentRequest {
            instrument: Some(cv::instrument_to(id)),
            opts: Some(cv::opts_to(&opts)),
        };
        self.call(
            false,
            msg,
            |mut c, r| async move { c.subscribe_quotes(r).await },
        )
        .await
        .map(sub)
    }

    async fn candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        range: TimeRange,
    ) -> Result<Vec<Candle>, Error> {
        let msg = pb::CandlesRequest {
            instrument: Some(cv::instrument_to(id)),
            timeframe: cv::timeframe_to(tf),
            range: Some(cv::range_to(range)),
        };
        let r = self
            .call(false, msg, |mut c, r| async move { c.candles(r).await })
            .await?;
        r.candles
            .iter()
            .map(cv::candle_from)
            .collect::<Result<_, _>>()
            .map_err(bad_reply)
    }

    async fn subscribe_candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let msg = pb::SubscribeCandlesRequest {
            instrument: Some(cv::instrument_to(id)),
            timeframe: cv::timeframe_to(tf),
            opts: Some(cv::opts_to(&opts)),
        };
        self.call(false, msg, |mut c, r| async move {
            c.subscribe_candles(r).await
        })
        .await
        .map(sub)
    }
}

#[async_trait]
impl InstrumentSource for RemoteClient {
    async fn find(&self, q: InstrumentQuery) -> Result<Vec<Instrument>, Error> {
        let msg = pb::FindRequest {
            query: Some(cv::query_to(&q)),
        };
        let r = self
            .call(false, msg, |mut c, r| async move { c.find(r).await })
            .await?;
        r.instruments
            .iter()
            .map(cv::instrument_info_from)
            .collect::<Result<_, _>>()
            .map_err(bad_reply)
    }

    async fn get(&self, id: &InstrumentId) -> Result<Instrument, Error> {
        let msg = pb::InstrumentRequest {
            instrument: Some(cv::instrument_to(id)),
        };
        let r = self
            .call(
                false,
                msg,
                |mut c, r| async move { c.get_instrument(r).await },
            )
            .await?;
        cv::instrument_info_from(&r).map_err(bad_reply)
    }

    async fn trading_params(&self, id: &InstrumentId) -> Result<TradingParams, Error> {
        let msg = pb::InstrumentRequest {
            instrument: Some(cv::instrument_to(id)),
        };
        let r = self
            .call(
                false,
                msg,
                |mut c, r| async move { c.trading_params(r).await },
            )
            .await?;
        cv::params_from(&r).map_err(bad_reply)
    }

    async fn trading_status(&self, id: &InstrumentId) -> Result<TradingStatus, Error> {
        let msg = pb::InstrumentRequest {
            instrument: Some(cv::instrument_to(id)),
        };
        let r = self
            .call(
                false,
                msg,
                |mut c, r| async move { c.trading_status(r).await },
            )
            .await?;
        cv::status_from(&r).map_err(bad_reply)
    }

    async fn schedule(&self, exchange: &str, range: TimeRange) -> Result<Schedule, Error> {
        let msg = pb::ScheduleRequest {
            exchange: exchange.to_owned(),
            range: Some(cv::range_to(range)),
        };
        let r = self
            .call(false, msg, |mut c, r| async move { c.schedule(r).await })
            .await?;
        cv::schedule_from(&r).map_err(bad_reply)
    }
}

#[async_trait]
impl OrderExecutor for RemoteClient {
    async fn place(&self, req: PlaceOrderRequest) -> Result<OrderAck, Error> {
        let r = self
            .call(true, cv::place_to(&req), |mut c, r| async move {
                c.place(r).await
            })
            .await?;
        cv::ack_from(&r).map_err(bad_reply)
    }

    async fn cancel(&self, cid: ClientOrderId) -> Result<(), Error> {
        let msg = pb::CancelRequest {
            cid: cv::cid_to(&cid),
        };
        self.call(true, msg, |mut c, r| async move { c.cancel(r).await })
            .await
            .map(|_| ())
    }

    async fn replace(&self, req: ReplaceOrderRequest) -> Result<OrderAck, Error> {
        let r = self
            .call(true, cv::replace_to(&req), |mut c, r| async move {
                c.replace(r).await
            })
            .await?;
        cv::ack_from(&r).map_err(bad_reply)
    }

    async fn order_state(&self, cid: ClientOrderId) -> Result<OrderSnapshot, Error> {
        let msg = pb::OrderStateRequest {
            cid: cv::cid_to(&cid),
        };
        let r = self
            .call(false, msg, |mut c, r| async move { c.order_state(r).await })
            .await?;
        cv::order_from(&r).map_err(bad_reply)
    }

    async fn active_orders(&self) -> Result<Vec<OrderSnapshot>, Error> {
        let r = self
            .call(false, pb::ActiveOrdersRequest {}, |mut c, r| async move {
                c.active_orders(r).await
            })
            .await?;
        r.orders
            .iter()
            .map(cv::order_from)
            .collect::<Result<_, _>>()
            .map_err(bad_reply)
    }

    async fn subscribe_order_state(&self) -> Result<SubscriptionId, Error> {
        let msg = pb::SubscribeRequest { opts: None };
        self.call(false, msg, |mut c, r| async move {
            c.subscribe_order_state(r).await
        })
        .await
        .map(sub)
    }

    /// Asks the gateway to compare `ledger` with the broker. The orders of the other bots of the gateway are not in
    /// your ledger, so they come back as `foreign`.
    async fn reconcile(&self, ledger: &[ClientOrderId]) -> Result<ReconcileReport, Error> {
        let msg = pb::ReconcileRequest {
            ledger: cv::cids_to(ledger),
            include_journal: false,
        };
        let r = self
            .call(true, msg, |mut c, r| async move { c.reconcile(r).await })
            .await?;
        cv::report_from(&r).map_err(bad_reply)
    }
}

#[async_trait]
impl ExecutionSource for RemoteClient {
    async fn executions(&self, range: TimeRange) -> Result<Vec<Execution>, Error> {
        let msg = pb::ExecutionsRequest {
            range: Some(cv::range_to(range)),
        };
        let r = self
            .call(false, msg, |mut c, r| async move { c.executions(r).await })
            .await?;
        r.executions
            .iter()
            .map(cv::execution_from)
            .collect::<Result<_, _>>()
            .map_err(bad_reply)
    }

    async fn subscribe_executions(&self) -> Result<SubscriptionId, Error> {
        let msg = pb::SubscribeRequest { opts: None };
        self.call(false, msg, |mut c, r| async move {
            c.subscribe_executions(r).await
        })
        .await
        .map(sub)
    }
}

#[async_trait]
impl PortfolioSource for RemoteClient {
    async fn portfolio(&self) -> Result<Portfolio, Error> {
        let r = self
            .call(false, pb::AccountRequest {}, |mut c, r| async move {
                c.portfolio(r).await
            })
            .await?;
        cv::portfolio_from(&r).map_err(bad_reply)
    }

    async fn positions(&self) -> Result<Vec<Position>, Error> {
        let r = self
            .call(false, pb::AccountRequest {}, |mut c, r| async move {
                c.positions(r).await
            })
            .await?;
        cv::positions_from(&r.positions).map_err(bad_reply)
    }

    async fn balances(&self) -> Result<Vec<Balance>, Error> {
        let r = self
            .call(false, pb::AccountRequest {}, |mut c, r| async move {
                c.balances(r).await
            })
            .await?;
        cv::balances_from(&r.balances).map_err(bad_reply)
    }

    async fn subscribe_portfolio(&self, opts: SubscribeOpts) -> Result<SubscriptionId, Error> {
        let msg = pb::SubscribeRequest {
            opts: Some(cv::opts_to(&opts)),
        };
        self.call(false, msg, |mut c, r| async move {
            c.subscribe_portfolio(r).await
        })
        .await
        .map(sub)
    }
}

#[async_trait]
impl StopOrderExecutor for RemoteClient {
    async fn place_stop(&self, req: PlaceStopOrderRequest) -> Result<OrderAck, Error> {
        let r = self
            .call(true, cv::place_stop_to(&req), |mut c, r| async move {
                c.place_stop(r).await
            })
            .await?;
        cv::ack_from(&r).map_err(bad_reply)
    }

    async fn cancel_stop(&self, stop: StopOrderRef) -> Result<(), Error> {
        let msg = pb::CancelStopRequest {
            stop: Some(cv::stop_ref_to(&stop)),
        };
        self.call(true, msg, |mut c, r| async move { c.cancel_stop(r).await })
            .await
            .map(|_| ())
    }

    async fn active_stop_orders(&self) -> Result<Vec<StopOrderSnapshot>, Error> {
        let r = self
            .call(false, pb::ActiveOrdersRequest {}, |mut c, r| async move {
                c.active_stop_orders(r).await
            })
            .await?;
        r.stop_orders
            .iter()
            .map(cv::stop_from)
            .collect::<Result<_, _>>()
            .map_err(bad_reply)
    }

    async fn subscribe_stop_orders(&self) -> Result<SubscriptionId, Error> {
        let msg = pb::SubscribeRequest { opts: None };
        self.call(false, msg, |mut c, r| async move {
            c.subscribe_stop_orders(r).await
        })
        .await
        .map(sub)
    }
}
