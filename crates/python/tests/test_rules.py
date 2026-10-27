"""The short pieces of code on the pages about orders and reconcile run against the imitation."""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import brokapi
from brokapi import testing

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))
import rules  # noqa: E402


class MemoryJournal:
    def __init__(self) -> None:
        self.cids: list[uuid.UUID] = []

    def sending(self, cid: uuid.UUID) -> None:
        self.cids.append(cid)

    def live(self) -> list[uuid.UUID]:
        return list(self.cids)


async def test_the_rules() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        params = await client.trading_params(sber)
        journal = MemoryJournal()
        price = Decimal("100.00")

        # Rule 1 needs an open gate: reconcile first (rule 2 shows how).
        report = await rules.rule_reconcile_first(client, journal, sber, params.min_qty, price)
        assert report.foreign == []
        await rules.rule_cid_first(client, journal, sber, params.min_qty, price)
        assert len(journal.cids) == 1

        assert await rules.rule_unknown(client, sber, params.min_qty, price) is None  # it was placed: no Unknown

        # A second run of the bot: a new client, the journal says which orders are alive.
        again = await mock.connect(name="again")
        report = await rules.after_restart(again, journal.live())
        assert [o.cid for o in report.confirmed] == journal.cids
        assert len(report.foreign) == 2  # the orders of rule_reconcile_first and rule_unknown are not in the journal


async def test_rule_two_before_the_reconcile_the_gate_is_closed() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        with pytest.raises(brokapi.NotReconciled):
            await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=1, price=Decimal("1"))


async def test_the_average_price_of_an_order_comes_from_its_fills() -> None:
    # ALOR: its imitation keeps every fill in the history, with the cid.
    async with await testing.mock_alor() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        lot = (await client.trading_params(sber)).min_qty
        await client.subscribe_order_state()
        await client.reconcile([])
        cid = brokapi.new_cid()
        await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=lot * 3, price=Decimal("101.00"))
        start = datetime.now(timezone.utc) - timedelta(hours=1)
        end = start + timedelta(hours=2)
        assert await rules.average_price(client, cid, start, end) is None
        # Two fills at two prices: one lot at 100 and two at 101, the average is (100 + 2 * 101) / 3.
        mock.fill(cid, qty=lot, price=Decimal("100.00"))
        mock.fill(cid, qty=lot * 2, price=Decimal("101.00"))
        assert await rules.average_price(client, cid, start, end) == Decimal(302) / Decimal(3)
        snapshot = await client.order_state(cid)
        assert snapshot.filled == lot * 3
        assert snapshot.state.filled is None  # FILLED carries no quantity: read snapshot.filled
