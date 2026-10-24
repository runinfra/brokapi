//! [`Feed`]: the events of a bot as one stream that stays open.
//!
//! [`RemoteClient::events`] gives one stream, and a bot that wants to survive a break has to write the loop around it:
//! take the resume point, open the stream again, tell the gap from a cut. The `follow` of the example bot is that loop.
//! A [`Feed`] is the same loop, run by the library, for the code that only wants to read events (the Python module is one).
//! It reconnects by itself and continues from the last number it handled, so a short break loses nothing. What it cannot
//! make up for it reports as [`FeedItem::Gap`]: the events after the bot's number are no longer at the gateway, and the
//! bot must read the account again and `reconcile`.

use std::time::Duration;

use brokapi::core::{Envelope, Error};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::pb;
use crate::remote::{EventStream, RemoteClient, RemoteEvent};

/// What a [`Feed`] delivers.
#[derive(Debug, Clone)]
// The event is the case that almost always happens: it is not boxed to make the rare one smaller.
#[allow(clippy::large_enum_variant)]
pub enum FeedItem {
    /// An event of the connection, as the connector made it. The numbers of the gateway are handled inside the feed.
    Event(Envelope),
    /// Events were lost: the gateway no longer has what came after the last event the feed delivered. `reason` says why,
    /// and `NEW_EPOCH` means that the gateway is a new process, which holds none of the subscriptions of this bot.
    /// Subscribe again if it is so, read the account, and `reconcile`. The feed goes on from the current place.
    Gap(pb::Gap),
}

/// The pauses between the attempts to open the stream again: none for the first, then this, doubling up to the limit.
const FIRST_PAUSE: Duration = Duration::from_millis(100);
const LAST_PAUSE: Duration = Duration::from_secs(2);

/// The events of a [`RemoteClient`] as one stream that stays open: a break is mended by the feed itself and a loss is
/// reported as [`FeedItem::Gap`]. Dropping the feed ends it.
pub struct Feed {
    rx: mpsc::Receiver<FeedItem>,
    task: JoinHandle<()>,
}

impl RemoteClient {
    /// Opens the event stream and keeps it open: a break is mended with the resume point, and a loss is reported as
    /// [`FeedItem::Gap`]. The stream is registered at the gateway when this returns, so subscribe after it, not before.
    ///
    /// `buffer` is how many events wait for the reader inside the feed. A reader that is slower than the events makes the
    /// gateway cut this bot off after its own buffer is full, which the feed mends the same way, from the resume point.
    /// `own_orders_only` leaves out the order and execution events of the orders of other bots.
    ///
    /// The feed ends when the client is [closed](RemoteClient::close).
    pub async fn feed(&self, buffer: usize, own_orders_only: bool) -> Result<Feed, Error> {
        let first = self.events(None, own_orders_only).await?;
        let (tx, rx) = mpsc::channel(buffer.max(1));
        let client = self.clone();
        let task = tokio::spawn(async move { run(client, first, own_orders_only, tx).await });
        Ok(Feed { rx, task })
    }
}

async fn run(
    client: RemoteClient,
    mut stream: EventStream,
    own_orders_only: bool,
    tx: mpsc::Sender<FeedItem>,
) {
    loop {
        // Read the stream to its end. A reader that is gone or a closed client ends the feed.
        loop {
            let next = tokio::select! {
                next = stream.next() => next,
                () = client.closed() => return,
            };
            let Some(next) = next else { break };
            let item = match next {
                Ok(RemoteEvent::Event { envelope, .. }) => FeedItem::Event(envelope),
                Ok(RemoteEvent::Gap(gap)) => FeedItem::Gap(gap),
                // Too slow: the gateway cut the stream off after it delivered what was in the buffer.
                Ok(RemoteEvent::Lagged { .. }) => break,
                Err(_) => break,
            };
            if tx.send(item).await.is_err() {
                return;
            }
        }
        // The stream ended (the gateway stopped, the connection broke, this bot lagged): open it again from where it stopped.
        let resume = stream.resume_point();
        let mut pause = Duration::ZERO;
        stream = loop {
            if client.is_closed() || tx.is_closed() {
                return;
            }
            let opened = tokio::select! {
                opened = client.events(Some(resume), own_orders_only) => opened,
                () = client.closed() => return,
            };
            match opened {
                Ok(stream) => break stream,
                Err(_) => {
                    tokio::select! {
                        () = tokio::time::sleep(pause) => {}
                        () = client.closed() => return,
                    }
                    pause = if pause.is_zero() {
                        FIRST_PAUSE
                    } else {
                        (pause * 2).min(LAST_PAUSE)
                    };
                }
            }
        };
    }
}

impl Feed {
    /// The next item, or `None` when the feed has ended (the client was closed).
    pub async fn next(&mut self) -> Option<FeedItem> {
        self.rx.recv().await
    }

    /// An item that is already waiting, without waiting for more.
    pub fn try_next(&mut self) -> Option<FeedItem> {
        self.rx.try_recv().ok()
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.task.abort();
    }
}
