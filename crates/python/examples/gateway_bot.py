"""A bot behind a gateway: connect, subscribe, reconcile, trade, and survive the loss of events.

The code of the page "Подключить бота" of the gateway section of the book, the Python part. The bot buys one lot far below
the market, so that the order rests, and follows the order to its end. The strategy is a stub. What a bot behind a gateway
needs is here: one line to connect, `setup` that is run at the start and again when a new gateway process has no memory of
the bot, and the `Gap` that tells it so.

The tests run it against the imitation of BCS, through a gateway that runs in the test, and once at the broker itself: the
bot does not change, only the line that makes the client.
"""

import asyncio
import uuid
from decimal import Decimal
from typing import Optional

import brokapi


# ANCHOR: connect
async def connect() -> brokapi.Client:
    """The one line that differs from a bot at the broker: `brokapi.connect("bcs")` there.

    `"bcs-main"` is the name of the gateway in `brokapi.toml`. The directory of its state is `brokapi.state` here, or what
    `BROKAPI_STATE` says. `bot` is the name of this bot: the gateway shows it in `brokapi status`.
    """
    return await brokapi.connect_gateway("bcs-main", bot="my-bot")
# ANCHOR_END: connect


# ANCHOR: bot
class Bot:
    def __init__(self, client: brokapi.Client, ticker: str = "SBER") -> None:
        self.client = client
        self.ticker = ticker
        # The orders the bot believes are alive. A real bot keeps them in a file and writes the cid BEFORE `place`.
        self.ledger: set[uuid.UUID] = set()
        self.cid: Optional[uuid.UUID] = None
        self.ended: Optional[brokapi.OrderStateKind] = None
        self.placed = asyncio.Event()  # set when the order stands: a hook for the tests
        self.mended = asyncio.Event()  # set after the bot has got over a Gap: a hook for the tests

    async def setup(self) -> None:
        """What a gateway has to be told: the subscriptions first, then the reconcile.

        It runs at the start, and again when a new gateway process has taken the place of the old one (it holds none of the
        subscriptions of this bot). The subscriptions go BEFORE the reconcile: an event between the snapshot of the reconcile
        and the subscription would be lost.
        """
        self.instrument = await self.client.instrument(self.ticker)
        await self.client.subscribe_order_state()
        await self.reconcile()

    async def reconcile(self) -> None:
        report = await self.client.reconcile(sorted(self.ledger))
        for snapshot in report.confirmed:
            self.on_order(snapshot)
        for cid in report.closed_offline:
            # The broker has no such order: it ended while the bot was away, or it was never sent.
            self.ledger.discard(cid)
            self.ended = self.ended or brokapi.OrderStateKind.CANCELLED

    def on_order(self, snapshot: brokapi.OrderSnapshot) -> None:
        if snapshot.cid in self.ledger and snapshot.state.is_final:
            self.ledger.discard(snapshot.cid)
            self.ended = snapshot.state.kind

    async def on_event(self, envelope: brokapi.Envelope) -> None:
        event = envelope.payload
        if isinstance(event, brokapi.OrderSnapshot):
            self.on_order(event)
        elif isinstance(event, brokapi.Gap):
            # Events were lost between the gateway and the bot. A new gateway process has no subscriptions of ours.
            if event.subscriptions_lost:
                await self.client.subscribe_order_state()
            await self.reconcile()
            self.mended.set()
        elif isinstance(event, (brokapi.Overflow, brokapi.Reconnected)):
            # The same loss, found by the connector of the gateway: the same remedy as at a broker.
            await self.reconcile()
# ANCHOR_END: bot

    # ANCHOR: place
    async def place(self) -> None:
        """One lot, far below the market: the order rests. The cid is in the ledger before the call."""
        params = await self.client.trading_params(self.instrument)
        cid = brokapi.new_cid()
        self.ledger.add(cid)
        await self.client.place(
            cid=cid, instrument=self.instrument, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00")
        )
        self.cid = cid
        self.placed.set()
    # ANCHOR_END: place


# ANCHOR: loop
async def run(bot: Bot) -> None:
    """Sets up, places the order, and reads the events until the order has ended."""
    await bot.setup()
    await bot.place()
    async for envelope in bot.client.events:
        await bot.on_event(envelope)
        if bot.ended is not None:
            break
# ANCHOR_END: loop


async def main() -> None:
    client = await connect()
    try:
        bot = Bot(client)
        await run(bot)
        print(f"the order ended: {bot.ended}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
