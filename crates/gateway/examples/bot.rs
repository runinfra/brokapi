//! A bot behind a gateway: connect, open the event stream, subscribe, reconcile, trade, and survive a break of the stream.
//!
//! The code of the page "Подключить бота" of the book (docs/src/gateway/bots.md). `run` starts a gateway on the BCS
//! imitation and runs the bot against it, in `tests/docs.rs`: no network, no money. `main` is the same bot for a person at
//! a keyboard, against a gateway that `brokapi up` runs:
//!
//! ```text
//! cargo run -p brokapi-gateway --example bot -- brokapi.state/gateways/bcs-main
//! ```
//!
//! The argument is the directory of the gateway (`<state_dir>/gateways/<name>`). The bot reads the order book and
//! subscribes. It places an order only with `BROKAPI_LIVE=1`, and then the order is real: BCS has no sandbox.
#![allow(dead_code)]

use std::path::Path;

use brokapi::core::{
    ClientOrderId, Envelope, Error, Event, InstrumentId, OrderBookSource, OrderExecutor,
    OrderState, OrderType, PlaceOrderRequest, Price, Qty, Side,
};
use brokapi_gateway::{RemoteClient, RemoteEvent, Resume, pb};

/// What the bot saw, for the test.
#[derive(Debug, Default)]
pub struct Seen {
    /// The order the bot placed.
    pub cid: Option<ClientOrderId>,
    /// The states of that order, in the order they came.
    pub states: Vec<OrderState>,
    /// Gaps in the numbers of events.
    pub gaps: u32,
}

// ANCHOR: follow
/// Reads the events of the gateway until `done` is true, and does what the stream asks for on the way.
///
/// `resume` is where the previous stream stopped (`None` at the first start). `journal` is the bot's own list of the orders
/// it placed and has not seen end.
pub async fn follow(
    bot: &RemoteClient,
    mut resume: Option<Resume>,
    journal: &[ClientOrderId],
    mut on_event: impl FnMut(&Envelope) -> bool,
) -> Result<u32, Error> {
    let mut gaps = 0;
    loop {
        // Open the stream first: from the moment `events` returns, the gateway keeps what happens for this bot.
        let mut stream = bot.events(resume, true).await?;
        // Then subscribe. Asking again is harmless, the gateway counts the request once for the bot.
        bot.subscribe_order_state().await?;
        while let Some(next) = stream.next().await {
            match next? {
                RemoteEvent::Event { envelope, .. } => {
                    if on_event(&envelope) {
                        return Ok(gaps);
                    }
                }
                RemoteEvent::Gap(gap) => {
                    // The gateway no longer has the events after our number. Whatever they said, the account is
                    // not what we last saw: ask the broker. A new epoch means a new process, which holds no subscriptions.
                    gaps += 1;
                    if gap.reason == pb::gap::Reason::NewEpoch as i32 {
                        bot.subscribe_order_state().await?;
                    }
                    bot.reconcile(journal).await?;
                }
                RemoteEvent::Lagged { .. } => break, // we read too slowly: reconnect from the resume point
            }
        }
        // The stream ended (the gateway restarted, the connection broke) or we lagged: continue where we stopped.
        resume = Some(stream.resume_point());
    }
}
// ANCHOR_END: follow

/// Runs the bot against the gateway whose directory is `dir`. Places an order only when `trade` is true.
pub async fn run(dir: &Path, trade: bool) -> Result<Seen, Box<dyn std::error::Error>> {
    // ANCHOR: connect
    // The directory holds the address of the gateway (`gateway.addr`) and the access key (`access.key`). The name is
    // yours: the gateway shows it in `brokapi status` and in its log.
    let bot = RemoteClient::connect_dir(dir, "my-bot").await?;
    // ANCHOR_END: connect

    // ANCHOR: reconcile
    // Before any command: tell the gateway which orders you placed and have not seen end. The report has three lists, as
    // when the bot talks to a broker itself.
    let journal: Vec<ClientOrderId> = Vec::new(); // in a real bot: read from your own file
    let report = bot.reconcile(&journal).await?;
    println!(
        "reconciled: {} confirmed, {} closed while away, {} not ours",
        report.confirmed.len(),
        report.closed_offline.len(),
        report.foreign.len()
    );
    // ANCHOR_END: reconcile

    // ANCHOR: read
    let sber = bot.resolve_instrument("SBER", Some("TQBR")).await?;
    let book = bot.order_book(&sber, 5).await?;
    println!("order book: {book:?}");
    // ANCHOR_END: read

    let mut seen = Seen::default();
    if !trade {
        return Ok(seen);
    }

    // ANCHOR: trade
    // The stream is open before the order goes out, so the first state of the order cannot be missed.
    let mut stream = bot.events(None, true).await?;
    bot.subscribe_order_state().await?;
    let cid = ClientOrderId::new_v4();
    // Write `cid` to your journal here, before the call. After a crash it is the only way back to this order.
    bot.place(far_buy(&sber, cid)).await?;
    // ANCHOR_END: trade
    seen.cid = Some(cid);

    // ANCHOR: states
    bot.cancel(cid).await?;
    // The states of the order come on the stream, the bot does not poll for them.
    while let Some(next) = stream.next().await {
        let RemoteEvent::Event { envelope, .. } = next? else {
            continue; // a gap or a cut: see `follow` below
        };
        if let Event::Order(order) = envelope.payload
            && order.cid == cid
        {
            let ended = matches!(order.state, OrderState::Cancelled { .. });
            seen.states.push(order.state);
            if ended {
                break;
            }
        }
    }
    // ANCHOR_END: states
    Ok(seen)
}

/// A buy of one lot far below the market: it rests, and it can be cancelled.
fn far_buy(instrument: &InstrumentId, cid: ClientOrderId) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid,
        instrument: instrument.clone(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some("1.0".parse::<Price>().expect("a price")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .ok_or("give the directory of the gateway: <state_dir>/gateways/<name>")?;
    let trade = std::env::var("BROKAPI_LIVE").as_deref() == Ok("1");
    run(Path::new(&dir), trade).await?;
    Ok(())
}
