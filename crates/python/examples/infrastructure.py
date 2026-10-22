"""One process, several brokers and accounts: one event stream, one order journal in a file, and a restart that
reconciles every account against that journal.

The code of the page "Как собрать инфраструктуру на несколько брокеров и счетов" of the Python section of the book.
The accounts are BCS (one), T-Bank (two, one token), Finam (one) and ALOR (one). The tests play two lives of the process against
the imitations. Nothing here touches the network or money.
"""

import asyncio
import os
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Optional

import brokapi

ACCOUNTS = ["bcs", "tbank-a", "tbank-b", "finam", "alor"]


# ANCHOR: journal
class Journal:
    """The order journal: a text file, one line per change, `account TAB cid TAB state TAB detail`. The last line of
    a cid wins. Every line is flushed to disk before the call that needs it."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.orders: dict[uuid.UUID, tuple[str, str]] = {}  # cid -> (account, state)
        if path.exists():
            for line in path.read_text().splitlines():
                parts = line.split("\t", 3)
                if len(parts) < 3:
                    continue  # a line cut by a crash carries no complete change
                try:
                    self.orders[uuid.UUID(parts[1])] = (parts[0], parts[2])
                except ValueError:
                    continue

    def set(self, account: str, cid: uuid.UUID, state: str, detail: str = "") -> None:
        """`state`: sending (written BEFORE place), working, or done."""
        self.orders[cid] = (account, state)
        with self.path.open("a") as file:
            file.write(f"{account}\t{cid}\t{state}\t{detail}\n")
            file.flush()
            os.fsync(file.fileno())

    def live(self, account: str) -> list[uuid.UUID]:
        """The ledger of one account for `reconcile`: the orders it believes are alive."""
        return [c for c, (a, s) in self.orders.items() if a == account and s != "done"]
# ANCHOR_END: journal


# ANCHOR: infra
class Infra:
    """All the clients of the process and the journal."""

    def __init__(self, clients: dict[str, brokapi.Client], journal: Journal) -> None:
        self.clients = clients
        self.journal = journal
        self.degraded: set[str] = set()  # accounts that lost events (Overflow, Reconnected): reconcile them before trading

    @classmethod
    async def start(cls, settings: dict[str, dict], journal_path: Path) -> "Infra":
        """Connects everything, subscribes, reconciles every account with the journal, and only then returns."""
        clients: dict[str, brokapi.Client] = {}
        # One limiter per area where the broker counts a limit. T-Bank counts per user: both accounts share one.
        # BCS counts per token, Finam per user, ALOR per login. The brokers never share.
        clients["bcs"] = await brokapi.connect("bcs", name="bcs", **settings.get("bcs", {}))
        clients["tbank-a"] = await brokapi.connect("tbank", name="tbank-a", **settings.get("tbank-a", {}))
        clients["tbank-b"] = await brokapi.connect(
            "tbank", name="tbank-b", limiter=clients["tbank-a"].limiter, **settings.get("tbank-b", {})
        )
        clients["finam"] = await brokapi.connect("finam", name="finam", **settings.get("finam", {}))
        clients["alor"] = await brokapi.connect("alor", name="alor", **settings.get("alor", {}))
        infra = cls(clients, Journal(journal_path))
        # Subscribe to the statuses and fills of every account FIRST, then reconcile it with ITS part of the journal.
        for client in clients.values():
            await client.subscribe_order_state()
            await client.subscribe_executions()
        for name in ACCOUNTS:
            await infra.reconcile(name)
        return infra

    async def reconcile(self, name: str) -> brokapi.ReconcileReport:
        """A reconcile that does not give up on the first hiccup: a limit or a network error means "no sealed
        answer yet", so wait and ask again, a few times. Any other error is raised: a retry would fail the same way."""
        pause = 0.2
        for attempt in range(1, 6):
            try:
                report = await self.clients[name].reconcile(self.journal.live(name))
            except brokapi.RateLimited as error:
                if attempt == 5:
                    raise
                await asyncio.sleep(error.retry_after.total_seconds())
            except brokapi.Transport:
                if attempt == 5:
                    raise
                await asyncio.sleep(pause)
                pause *= 2
            else:
                for snapshot in report.confirmed:
                    self.journal.set(name, snapshot.cid, "working")
                for cid in report.closed_offline:
                    self.journal.set(name, cid, "done", "closed while away or never sent")
                self.degraded.discard(name)
                return report
        raise AssertionError("unreachable")

    async def place(
        self, name: str, instrument: brokapi.InstrumentId, qty: Decimal, price: Decimal
    ) -> tuple[uuid.UUID, Optional[brokapi.BrokapiError]]:
        """One order on one account. The journal line goes first; the outcome is written after."""
        cid = brokapi.new_cid()
        self.journal.set(name, cid, "sending")
        try:
            await self.clients[name].place(
                cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=qty, price=price
            )
        except brokapi.Unknown as error:
            return cid, error  # the journal says "sending": the next reconcile or order_state settles it
        except brokapi.BrokapiError as error:
            self.journal.set(name, cid, "done", f"not placed: {error}")  # nothing was sent, or the broker refused
            return cid, error
        self.journal.set(name, cid, "working")
        return cid, None

    async def close(self) -> None:
        for client in self.clients.values():
            await client.close()
# ANCHOR_END: infra


# ANCHOR: events
async def watch(infra: Infra) -> None:
    """One loop over the events of all the accounts. `source` says whose each event is."""
    async for envelope in brokapi.merge(*infra.clients.values()):
        event = envelope.payload
        name = envelope.source.removesuffix("-ud").removesuffix("-md")
        if isinstance(event, brokapi.OrderSnapshot) and event.state.is_final:
            infra.journal.set(name, event.cid, "done", event.state.kind.value)
        elif isinstance(event, (brokapi.Overflow, brokapi.Reconnected)):
            # Events were lost (a full buffer, or a stream of the orders that was cut and opened again). Read the
            # stream to the end, then reconcile. The loop does both: it reads, and marks the account.
            infra.degraded.add(name)
# ANCHOR_END: events


async def main() -> None:
    # Every setting comes from the environment: BCS_REFRESH_TOKEN, BCS_IS_WRITE, TBANK_TOKEN, FINAM_SECRET,
    # ALOR_REFRESH_TOKEN, ALOR_PORTFOLIO, ...
    # The two T-Bank accounts differ in account_id.
    settings = {
        "tbank-a": {"account_id": os.environ["TBANK_ACCOUNT_ID"]},
        "tbank-b": {"account_id": os.environ["TBANK_ACCOUNT_ID_2"]},
    }
    infra = await Infra.start(settings, Path("orders.journal"))
    try:
        await watch(infra)
    finally:
        await infra.close()


if __name__ == "__main__":
    asyncio.run(main())
