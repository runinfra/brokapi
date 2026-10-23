//! The brokapi gateway: one process that holds the connection to one broker account and serves many bots over gRPC.
//!
//! A bot that connects to a broker itself owns the connection, the rate limits and the reconcile. With a gateway these
//! belong to one process that outlives the bots: bots start, stop and crash, the gateway keeps the connection, shares
//! one rate limiter between them, journals the orders they place and compares the journal with the broker when it
//! starts. Bots can be in any language: the contract is `proto/brokapi/gateway/v1/*.proto`.
//!
//! This crate has the library, and the program `brokapi` built on it:
//!
//! | You want | Use |
//! |---|---|
//! | Run gateways for your bots, keep them alive, look after them | the program `brokapi` (`cargo install brokapi-gateway`), see below |
//! | Run a gateway in your program | [`Gateway`], [`GatewayConfig`] |
//! | A bot in Rust | [`RemoteClient`], which speaks the traits of `brokapi-core` |
//! | A bot in another language | the generated client of the `.proto` files; [`pb`] is the Rust one |
//! | Run the service from your own program | [`service::Service`] (feature `cli`) |
//!
//! # The program `brokapi`
//!
//! One program is the whole product: `brokapi up` starts a **service** that runs one gateway process for every account in
//! the config file `brokapi.toml`, restarts a gateway that stops, and answers the other commands. The commands list the
//! gateways (`ps`, `top`), start and stop one (`start`, `stop`, `restart`), read the config again (`add`, `remove`,
//! `reload`), show a log (`logs`), read an account (`portfolio`, `orders`, `book`, `quote`), and cancel everything in an
//! emergency (`cancel-all`). `brokapi --help` lists them with the exit codes.
//!
//! ```text
//! $ brokapi init                  # a sample brokapi.toml and its state directory
//! $ brokapi check                 # the file, the tokens, a read-only look at every account
//! $ brokapi up -d                 # the service, in the background
//! $ brokapi ps
//! NAME            BROKER  STATE    PID    UPTIME  RESTARTS  BOTS  EVENTS/S  ORDERS  LIMIT  LAST ERROR
//! tbank-sandbox   tbank   running  4121   2m14s   0         2     0.4       1       8%
//! $ brokapi down
//! ```
//!
//! Two config files are two independent services on one machine: each has its own state directory and its own socket.
//! A service that dies leaves its gateways running, and the next `brokapi up` takes them over. The API between the
//! commands and the service is `proto/brokapi/service/v1/service.proto`, see [`service`].
//!
//! The program and everything it needs (the config file, the service, the commands) are behind the feature `cli`, on by
//! default. A program that only embeds a [`Gateway`] or is a bot turns it off with `default-features = false` and gets
//! the library without `clap`, `toml` and `sysinfo`.
//!
//! # What the gateway guarantees
//!
//! - **One limiter.** All bots of a gateway draw from one rate limit counter of the broker. A bot that asks over the
//!   budget gets `RateLimited`, as it would alone.
//! - **No lost events on a short break.** Every event has a number. A bot that reconnects gives the last number it
//!   handled and gets what it missed from a ring of recent events. A number the ring no longer has is a `Gap`: the bot
//!   must `Reconcile`.
//! - **A slow bot does not slow the others.** Each bot has a buffer of its own. When it is full the bot's stream ends
//!   with `Lagged` and the rest go on.
//! - **Restart with open orders.** The id of an order is written to the journal before the order is sent. After a
//!   restart the gateway compares the journal with the broker before it takes a command: until then `Health` says
//!   `starting` and commands get `UNAVAILABLE`.
//! - **Subscriptions have owners.** The same subscription of two bots is one at the broker. A bot can close only what it
//!   opened, and what a bot opened is closed when the bot has been gone for a while (`GatewayConfig::bot_grace`). The
//!   gateway subscribes to the states of orders itself, so its journal closes an order that no bot is watching.
//! - **Local only.** A Unix socket (or `127.0.0.1` on Windows), a key in a file with mode 600, and a refusal to start on
//!   any other address.
//!
//! # Examples
//!
//! A gateway on an imitation of BCS, and a bot that reads an order book through it. The directory of the gateway holds the
//! socket and the access key, so the bot needs only the directory and its own name.
//!
//! ```
//! use brokapi::core::{OrderBook, OrderBookSource, ShutdownMode};
//! use brokapi_client::{Broker, BrokerConfig, Settings};
//! use brokapi_gateway::{Gateway, GatewayConfig, RemoteClient};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
//! # let settings = Settings::parse(&format!(
//! #     r#"{{"endpoint": "{}", "refresh_token": "mock-refresh-token"}}"#,
//! #     server.http_base
//! # ))?;
//! # let broker = BrokerConfig::from_settings(Broker::Bcs, &settings)?;
//! # let state = tempfile::tempdir()?;
//! // One gateway per broker account. `state` is its directory: the journal, the access key, the socket.
//! let gateway = Gateway::start(GatewayConfig::new("main", state.path()), broker, None).await?;
//! // It takes commands once it has compared its journal with the broker.
//! gateway.ready().await;
//!
//! let bot = RemoteClient::connect_dir(state.path(), "my-bot").await?;
//! let sber = bot.resolve_instrument("SBER", None).await?;
//! assert!(matches!(bot.order_book(&sber, 5).await?, OrderBook::Snapshot { .. }));
//!
//! gateway.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```
//!
//! Documentation of the whole library, with guides: <https://brokapi.ru/docs/>.

#![warn(missing_docs)]

mod access;
#[cfg(feature = "cli")]
pub mod cli;
pub mod convert;
mod error;
mod hub;
mod journal;
mod metrics;
mod remote;
mod server;
#[cfg(feature = "cli")]
pub mod service;
#[cfg(unix)]
mod socket;
mod subs;

/// The messages and the service of the `.proto` files, generated.
#[allow(missing_docs, clippy::all)]
pub mod pb {
    tonic::include_proto!("brokapi.gateway.v1");

    /// The descriptors of the `.proto` files, for tools that read the API by its names.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));
}

pub use access::{
    ADDRESS_FILE, ANONYMOUS, Access, Address, BOT_HEADER, BotName, KEY_FILE, KEY_HEADER, Listen,
    read_key, valid_bot_name,
};
pub use error::{DETAIL_KEY, detail_of, error_of, gateway_status, status_of};
pub use hub::{Hub, RingConfig};
pub use journal::Journal;
pub use remote::{ConnectOptions, EventStream, RemoteClient, RemoteEvent, Resume};
pub use server::{Gateway, GatewayConfig, GatewayError, GatewayState};
