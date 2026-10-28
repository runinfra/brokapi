"""A bot with a signal and a stop, in one file: the price rose 1% in 5 minutes - buy one lot, then a stop 0.5% below.

The code of the section "Бот с сигналом и стопом" of the page "Стоп-заявки, Python". Everything in one file, to copy as
it is: the journal, the signal, the order with every outcome, the stop after the fill, and what the bot does at every
start (an order that filled while the bot was away, a position left without a stop). The tests run it against the
imitation of Finam: the signal, the stop, two restarts.

    FINAM_SECRET=... python signal_stop_bot.py

Before a real account: the book, page "Перед боевым счетом". The caps of `connect` below stop an order bigger than meant.
"""

import asyncio
import json
import os
import time
import uuid
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Callable, Optional

import brokapi

RISE = Decimal("0.01")  # the signal: the price rose 1% from the lowest of the window
WINDOW = 5 * 60.0  # seconds
STOP_BELOW = Decimal("0.005")  # the stop: 0.5% below the price of the buy


# ANCHOR: journal
class Journal:
    """The memory of the bot, one JSON line per change, flushed BEFORE the call that needs it. The last line of a cid
    wins. It keeps the buy (its state, how much it bought and at what price) and the stop that protects it."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.rows: dict[str, dict[str, str]] = {}
        if path.exists():
            for line in path.read_text().splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue  # a line cut by a crash carries no complete change
                self.rows[row["cid"]] = row

    def set(self, cid: uuid.UUID, kind: str, state: str, filled: Decimal = Decimal(0), price: Decimal = Decimal(0)) -> None:
        """`kind`: buy or stop. `state`: sending, working, unknown or done."""
        row = {"cid": str(cid), "kind": kind, "state": state, "filled": str(filled), "price": str(price)}
        self.rows[row["cid"]] = row
        with self.path.open("a") as file:
            file.write(json.dumps(row) + "\n")
            file.flush()
            os.fsync(file.fileno())

    def buy(self) -> Optional[dict[str, str]]:
        """The buy of the bot, if it made one: this bot buys once and then only protects."""
        return next((row for row in self.rows.values() if row["kind"] == "buy" and row["state"] != "dropped"), None)
# ANCHOR_END: journal


# ANCHOR: bot
class SignalStopBot:
    def __init__(self, client: brokapi.Client, ticker: str, journal: Journal, clock: Callable[[], float] = time.monotonic) -> None:
        self.client = client
        self.ticker = ticker
        self.journal = journal
        self.clock = clock
        self.window: deque[tuple[float, Decimal]] = deque()  # (time, price) of the last WINDOW seconds
        self.need_reconcile = False
        self.protected = asyncio.Event()  # set when the position has its stop: a hook for the tests

    async def start(self) -> None:
        """At every start, the first one and every restart: subscriptions, reconcile, the buy, the stop, the quotes.
        Until `start` returns, the bot does not trade."""
        self.instrument = await self.client.instrument(self.ticker)
        self.params = await self.client.trading_params(self.instrument)
        await self.client.subscribe_order_state()
        await self.reconcile()
        await self.protect()
        await self.client.subscribe_quotes(self.instrument)

    async def reconcile(self) -> None:
        buy = self.journal.buy()
        alive = [uuid.UUID(buy["cid"])] if buy and buy["state"] != "done" else []
        report = await self.client.reconcile(alive)
        for snapshot in report.confirmed:
            self.on_order(snapshot)
        for cid in report.closed_offline:
            await self.resolve(cid)  # it ended while the bot was away: only order_state says how much it bought
        self.need_reconcile = False

    async def resolve(self, cid: uuid.UUID) -> None:
        """The way out of `Unknown` and of `closed_offline`: `order_state` by the same cid, never a second order."""
        try:
            self.on_order(await self.client.order_state(cid))
        except brokapi.Rejected as error:
            if error.code != "not_found":
                raise
            self.journal.set(cid, "buy", "dropped")  # never reached the broker: the next signal may buy

    def on_order(self, snapshot: brokapi.OrderSnapshot) -> None:
        row = self.journal.rows.get(str(snapshot.cid))
        if row is None or row["kind"] != "buy" or row["state"] == "done":
            return  # not ours, or counted already
        if snapshot.state.is_final:
            # How much it bought comes from the snapshot. The price of the buy is the price of the order: it is a
            # limit order at the ask, it fills there or better.
            self.journal.set(snapshot.cid, "buy", "done", snapshot.filled, Decimal(row["price"]))
            if snapshot.filled == 0:
                self.journal.set(snapshot.cid, "buy", "dropped")  # cancelled with nothing bought: the next signal may buy
        else:
            self.journal.set(snapshot.cid, "buy", "working", price=Decimal(row["price"]))
# ANCHOR_END: bot

    # ANCHOR: protect
    async def protect(self) -> None:
        """The position of the bot has a stop, or the bot puts one: after the fill and at every start."""
        buy = self.journal.buy()
        if buy is None or buy["state"] != "done" or Decimal(buy["filled"]) == 0:
            return
        qty = Decimal(buy["filled"])
        stops = [stop for stop in await self.client.active_stop_orders() if stop.state.kind is brokapi.StopOrderStateKind.ACTIVE]
        covered = sum(s.qty for s in stops if s.instrument.key == self.instrument.key and s.side is brokapi.Side.SELL)
        if covered >= qty:
            self.protected.set()  # the stop of an earlier run is there: not a second one
            return
        held = sum(p.qty for p in await self.client.positions() if p.instrument.key == self.instrument.key)
        if held < qty:
            return  # the stop has triggered and sold, or a person sold: nothing to protect, the bot does not buy again
        stop_price = brokapi.round_to_step(Decimal(buy["price"]) * (1 - STOP_BELOW), self.params.price_step, brokapi.RoundMode.DOWN)
        cid = brokapi.new_cid()
        self.journal.set(cid, "stop", "sending", qty, stop_price)
        try:
            await self.client.place_stop(
                cid=cid, instrument=self.instrument, side=brokapi.Side.SELL, kind=brokapi.StopOrderKind.STOP_LOSS,
                qty=qty, stop_price=stop_price,
            )
        except brokapi.Rejected as error:
            self.journal.set(cid, "stop", "done", price=stop_price)
            print(f"NO STOP: the broker refused it ({error.code}), a person decides")
            return
        except (brokapi.Unknown, brokapi.Transport):
            # It may be there. Never put it again under this cid: the next start looks with active_stop_orders.
            self.journal.set(cid, "stop", "unknown", qty, stop_price)
            return
        self.journal.set(cid, "stop", "working", qty, stop_price)
        print(f"stop {qty} @ {stop_price}")
        self.protected.set()
    # ANCHOR_END: protect

    # ANCHOR: signal
    async def on_event(self, envelope: brokapi.Envelope) -> None:
        event = envelope.payload
        if isinstance(event, brokapi.Quote) and event.ask is not None:
            await self.on_price(event.ask.price)
        elif isinstance(event, brokapi.OrderSnapshot):
            self.on_order(event)
            await self.protect()
        elif isinstance(event, (brokapi.Reconnected, brokapi.Overflow, brokapi.UnknownEvent)):
            self.need_reconcile = True  # the orders may have a hole: reconcile before the next command

    async def on_price(self, ask: Decimal) -> None:
        now = self.clock()
        self.window.append((now, ask))
        while self.window and self.window[0][0] < now - WINDOW:
            self.window.popleft()
        if self.need_reconcile:
            await self.reconcile()
            await self.protect()
        if self.journal.buy() is not None:
            return  # one buy in all the lives of the bot
        lowest = min(price for _, price in self.window)
        if ask < lowest * (1 + RISE):
            return
        cid = brokapi.new_cid()
        self.journal.set(cid, "buy", "sending", price=ask)  # BEFORE the call
        try:
            await self.client.place(cid=cid, instrument=self.instrument, side=brokapi.Side.BUY, qty=self.params.lot_size, price=ask)
        except brokapi.Rejected as error:
            self.journal.set(cid, "buy", "dropped")  # the broker or the caps said no: no order exists
            print(f"not bought: {error.code}")
        except (brokapi.NotReconciled, brokapi.Degraded):
            self.journal.set(cid, "buy", "dropped")
            self.need_reconcile = True
        except (brokapi.Unknown, brokapi.Transport):
            self.journal.set(cid, "buy", "unknown", price=ask)  # nobody knows: ask the broker, never send it again
            await self.resolve(cid)
        else:
            self.journal.set(cid, "buy", "working", price=ask)
            print(f"bought {self.params.lot_size} {self.ticker} @ {ask}, waiting for the fill")
    # ANCHOR_END: signal


async def run(bot: SignalStopBot) -> None:
    """Starts the bot and reads the events. The one place that reads the stream. Stopping the bot leaves the stop at the
    broker: the position stays protected."""
    await bot.start()
    async for envelope in bot.client.events:
        await bot.on_event(envelope)


async def main() -> None:
    # A cap on one order: a mistake of the code cannot buy for more (page "Потолки заявок и режим только чтения").
    client = await brokapi.connect("finam", max_order_value="5000")
    try:
        await run(SignalStopBot(client, "GAZP", Journal(Path("signal_stop.jsonl"))))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
