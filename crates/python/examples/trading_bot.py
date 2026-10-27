"""A trading bot on events: it reads the book, buys once, follows the order and leaves cleanly.

The code of the page "Как написать торгового бота на событиях" of the Python section of the book. The strategy is a
stub: when a book arrives and the bot has no order, buy the minimal quantity 1% below the best bid. Everything else
(the journal, the reconcile, the outcome of every command, `Resync`, `Overflow`, `Reconnected`, the stop) any real bot needs.

The tests run it against the imitation of BCS.
"""

import asyncio
import json
import os
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Optional

import brokapi

BUY_AT = Decimal("0.99")


# ANCHOR: journal
class Journal:
    """The memory of the bot. The library writes nothing to disk, so the bot does.

    One JSON line per change, flushed to disk BEFORE the call that needs it: a journal written after `place` loses
    the order that a crash cuts in between. The last line of a cid wins. A finished order keeps how much was filled:
    after a restart the bot knows what it has bought already.
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = path
        self.orders: dict[uuid.UUID, tuple[str, str]] = {}
        self.filled: dict[uuid.UUID, Decimal] = {}
        if path is not None and path.exists():
            for line in path.read_text().splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue  # a line cut by a crash carries no complete change
                cid = uuid.UUID(row["cid"])
                self.orders[cid] = (row["state"], row.get("detail", ""))
                self.filled[cid] = Decimal(row.get("filled", "0"))

    def set(self, cid: uuid.UUID, state: str, detail: str = "", filled: Decimal = Decimal(0)) -> None:
        """`state`: sending, working, unknown or done. `filled`: how much a finished order bought."""
        self.orders[cid] = (state, detail)
        self.filled[cid] = filled
        if self.path is not None:
            row = {"cid": str(cid), "state": state, "detail": detail, "filled": str(filled)}
            with self.path.open("a") as file:
                file.write(json.dumps(row) + "\n")
                file.flush()
                os.fsync(file.fileno())

    def bought(self) -> Decimal:
        """How much the finished orders bought, in all the lives of the bot."""
        return sum((self.filled[cid] for cid, (state, _) in self.orders.items() if state == "done"), Decimal(0))

    def alive(self) -> list[uuid.UUID]:
        """The ledger for `reconcile`: the orders the bot believes are alive."""
        return [cid for cid, (state, _) in self.orders.items() if state != "done"]
# ANCHOR_END: journal


# ANCHOR: bot
class TradingBot:
    def __init__(self, client: brokapi.Client, ticker: str, journal: Journal, budget: Decimal) -> None:
        self.client = client
        self.ticker = ticker
        self.journal = journal
        self.budget = budget  # how much to buy in all
        self.filled = journal.bought()  # what earlier lives of the bot have bought
        self.working: Optional[uuid.UUID] = None
        self.top_bid: Optional[Decimal] = None
        self.book_sub: Optional[int] = None
        self.need_reconcile = False
        self.placed = asyncio.Event()  # set when the first order stands: a hook for the tests

    @property
    def done(self) -> bool:
        return self.filled >= self.budget and self.working is None

    async def start(self) -> None:
        """In this order: subscriptions, reconcile with the journal, the rules of the instrument, the book.
        Until `start` returns, the bot does not trade."""
        self.instrument = await self.client.instrument(self.ticker)
        await self.client.subscribe_order_state()
        await self.client.subscribe_executions()
        await self.reconcile()
        self.params = await self.client.trading_params(self.instrument)
        self.book_sub = await self.client.subscribe_order_book(self.instrument, depth=1)

    async def reconcile(self) -> None:
        report = await self.client.reconcile(self.journal.alive())
        for snapshot in report.confirmed:
            self.on_order(snapshot)
        for cid in report.closed_offline:
            # Not alive at the broker: it ended while the bot was away (filled, cancelled), or never reached the broker.
            # Which of these, and how much was filled, only `order_state` says. Writing "done" without asking loses a
            # fill, and the bot buys a second time.
            await self.resolve(cid, "never reached the broker")
        self.need_reconcile = False

    async def on_event(self, envelope: brokapi.Envelope) -> None:
        event = envelope.payload
        if isinstance(event, brokapi.OrderBookSnapshot) and envelope.sub == self.book_sub:
            self.top_bid = event.bids[0].price if event.bids else None
            await self.try_buy()
        elif isinstance(event, brokapi.Resync):
            self.top_bid = None  # the next snapshot replaces the book
        elif isinstance(event, brokapi.OrderSnapshot):
            self.on_order(event)
        elif isinstance(event, brokapi.Overflow):
            if event.sub == self.book_sub:
                self.top_bid = None  # a lost book is not a hole in the orders: take a new subscription
                self.book_sub = await self.client.subscribe_order_book(self.instrument, depth=1)
            else:
                self.need_reconcile = True  # the orders may have a hole: read the stream to the end, then reconcile
        elif isinstance(event, brokapi.Reconnected):
            # The stream of the orders or of the fills was cut and opened again: what happened in the gap never
            # reached the bot. The gate is closed until the reconcile, so read the stream to the end, then reconcile.
            self.need_reconcile = True
        elif isinstance(event, brokapi.UnknownEvent):
            # A kind this package does not know. It may be about lost data, like the two above: reconcile.
            self.need_reconcile = True

    def on_order(self, snapshot: brokapi.OrderSnapshot) -> None:
        """The state of an order comes from the events, not from the answer of `place` (it only means "accepted")."""
        if snapshot.cid not in self.journal.orders:
            return  # an order placed by hand: the bot does not enter it into its journal
        if self.journal.orders[snapshot.cid][0] == "done":
            return  # counted already: an event and the reconcile can both bring the end of an order
        if snapshot.state.is_final:
            self.journal.set(snapshot.cid, "done", snapshot.state.kind.value, snapshot.filled)
            # The filled quantity is taken from the snapshot, not summed from `Execution` events: after an
            # overflow the reconcile returns the right number by itself.
            self.filled += snapshot.filled
            if self.working == snapshot.cid:
                self.working = None
        else:
            self.journal.set(snapshot.cid, "working")
            self.working = snapshot.cid
# ANCHOR_END: bot

    # ANCHOR: place
    async def try_buy(self) -> None:
        """The signal, and the order with every outcome handled."""
        if self.need_reconcile:
            await self.reconcile()
        if self.working is not None or self.top_bid is None or self.filled >= self.budget:
            return
        # Exact arithmetic. Round DOWN for a buy so the price never exceeds the plan, and to the step.
        price = brokapi.round_to_step(self.top_bid * BUY_AT, self.params.price_step, brokapi.RoundMode.DOWN)
        cid = brokapi.new_cid()
        self.journal.set(cid, "sending")  # BEFORE the call
        try:
            await self.client.place(
                cid=cid,
                instrument=self.instrument,
                side=brokapi.Side.BUY,
                qty=self.params.min_qty,
                price=price,
            )
        except brokapi.Rejected as error:
            self.journal.set(cid, "done", f"rejected: {error.code}")  # the broker said no: no order exists
        except (brokapi.NotReconciled, brokapi.Degraded):
            self.journal.set(cid, "done", "not sent: gate closed")
            self.need_reconcile = True
        except brokapi.RateLimited as error:
            self.journal.set(cid, "done", "not sent: rate limit")
            await asyncio.sleep(error.retry_after.total_seconds())  # then the next book tries again with a new cid
        except (brokapi.Unknown, brokapi.Transport):
            # Nobody knows whether the order exists. NEVER send it again: ask the broker. `Transport` on a command is
            # read the same way: the connector has no sign that it arrived, but cannot always prove it did not.
            self.journal.set(cid, "unknown")
            await self.resolve(cid, "unknown, not found at the broker")
        else:
            self.working = cid
            self.journal.set(cid, "working")
            self.placed.set()

    async def resolve(self, cid: uuid.UUID, missing: str) -> None:
        """The way out of `Unknown` and of `closed_offline`: `order_state` by the same cid."""
        try:
            snapshot = await self.client.order_state(cid)
        except brokapi.Rejected as error:
            if error.code != "not_found":
                raise
            # The broker has no such order. A broker can be late: a real bot asks again after a pause, and only then
            # writes "not placed". Here it is written at once and the next signal uses a new cid.
            self.journal.set(cid, "done", missing)
            return
        self.on_order(snapshot)
        self.placed.set()
    # ANCHOR_END: place


# ANCHOR: loop
async def run(bot: TradingBot) -> None:
    """Starts the bot and reads events until the budget is bought. The one place that reads the stream."""
    await bot.start()
    try:
        async for envelope in bot.client.events:
            await bot.on_event(envelope)
            if bot.done:
                break
    finally:
        # Orders at the exchange are not cancelled by a stop. This bot leaves none: it stops when its order is done.
        if bot.working is not None:
            await bot.client.cancel(bot.working)
# ANCHOR_END: loop


async def main() -> None:
    client = await brokapi.connect("bcs")
    try:
        bot = TradingBot(client, "SBER", Journal(Path("journal.jsonl")), budget=Decimal(1))
        await run(bot)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
