"""The rules of the core as short pieces of code: the Python side of the tabs on the pages "Круг команд" and
"Круг жизни" of the book. The tests run each of them against the imitation of BCS.
"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional, Protocol

import brokapi


class Journal(Protocol):
    def sending(self, cid: uuid.UUID) -> None: ...
    def live(self) -> list[uuid.UUID]: ...


async def rule_cid_first(client: brokapi.Client, journal: Journal, instrument: brokapi.InstrumentId, qty: Decimal, price: Decimal) -> None:
    """Rule 1: the bot makes the cid and writes it down before the call."""
    # ANCHOR: cid_first
    cid = brokapi.new_cid()  # made by the bot
    journal.sending(cid)  # on disk BEFORE the call
    result = await client.place(cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=qty, price=price)
    # ANCHOR_END: cid_first
    assert result.cid == cid


async def rule_reconcile_first(client: brokapi.Client, journal: Journal, instrument: brokapi.InstrumentId, qty: Decimal, price: Decimal) -> brokapi.ReconcileReport:
    """Rule 2: subscribe, reconcile, only then trade."""
    # ANCHOR: reconcile_first
    await client.subscribe_order_state()  # before the reconcile, or an event in between is lost
    report = await client.reconcile(journal.live())  # an empty list for a bot without a journal
    # report.confirmed, report.closed_offline, report.foreign: handle them (see the lifecycle page)
    await client.place(cid=brokapi.new_cid(), instrument=instrument, side=brokapi.Side.BUY, qty=qty, price=price)
    # Before the reconcile the line above raises brokapi.NotReconciled.
    # ANCHOR_END: reconcile_first
    return report


async def rule_unknown(client: brokapi.Client, instrument: brokapi.InstrumentId, qty: Decimal, price: Decimal) -> brokapi.OrderSnapshot | None:
    """Rule 3: no answer is not "no". Ask, do not repeat."""
    # ANCHOR: unknown
    cid = brokapi.new_cid()
    state = None
    try:
        await client.place(cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=qty, price=price)
    except brokapi.Rejected:
        pass  # the broker said no: there is no order
    except (brokapi.Unknown, brokapi.Transport):
        # The order may exist. NEVER place again. Ask about THE SAME cid (see the recovery page for the loop).
        state = await client.order_state(cid)
    # ANCHOR_END: unknown
    return state


async def after_restart(client: brokapi.Client, ledger: list[uuid.UUID]) -> brokapi.ReconcileReport:
    """The second run of a bot: subscribe, then reconcile with the journal, then sort the three lists."""
    # ANCHOR: start
    # After a restart the gate is closed: commands raise NotReconciled.
    # Subscribe BEFORE the reconcile: an event between the snapshot and the subscription would be lost.
    await client.subscribe_order_state()
    # ANCHOR_END: start

    # ANCHOR: reconcile
    # The journal says which orders the bot thinks are alive.
    report = await client.reconcile(ledger)
    for order in report.confirmed:
        print(f"confirmed {order.cid}: still at the broker, filled {order.filled}")
    for cid in report.closed_offline:
        # Find out how it ended: order_state(cid) and executions(...), then write it to the journal.
        print(f"closed offline {cid}: ask order_state and write the result")
    for order in report.foreign:
        # Not ours. Do not touch: a person decides.
        print(f"foreign {order.cid}: shown to a person")
    # ANCHOR_END: reconcile
    return report


async def average_price(client: brokapi.Client, cid: uuid.UUID, start: datetime, end: datetime) -> Optional[Decimal]:
    """How much an order bought, and at what average price: from the fills, the snapshot has no average price.

    A fill without a cid is not counted: the history of BCS gives none, there this returns ``None``."""
    # ANCHOR: average_price
    fills = [e for e in await client.executions(start, end) if e.cid == cid]
    done = sum((e.qty for e in fills), Decimal(0))  # the same number as OrderSnapshot.filled
    if not done:
        return None  # nothing filled yet
    return sum((e.price * e.qty for e in fills), Decimal(0)) / done
    # ANCHOR_END: average_price
