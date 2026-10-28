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


HISTORY_WITH_CID = {"finam": True, "alor": True, "bybit": True, "bcs": False, "tbank": False}
TICKER = {"bcs": "SBER", "tbank": "SBER", "finam": "SBER", "alor": "SBER", "bybit": "MOCKUSDT"}


@pytest.mark.parametrize("broker", sorted(HISTORY_WITH_CID))
async def test_the_history_of_some_brokers_names_no_order_and_the_average_price_is_none(broker: str) -> None:
    # The book says where the recipe works: the history of BCS and T-Bank does not name the order of a fill.
    async with await getattr(testing, f"mock_{broker}")() as mock:
        client = await mock.connect()
        instrument = await client.instrument(TICKER[broker])
        qty = (await client.trading_params(instrument)).min_qty
        await client.reconcile([])
        cid = brokapi.new_cid()
        await client.place(cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=qty, price=Decimal("100.00"))
        mock.fill(cid, qty=qty, price=Decimal("100.00"))
        start = datetime.now(timezone.utc) - timedelta(hours=1)
        end = start + timedelta(hours=2)
        expected = Decimal("100.00") if HISTORY_WITH_CID[broker] else None
        assert await rules.average_price(client, cid, start, end) == expected


def bar(minute: int, close: str) -> brokapi.Candle:
    start = datetime(2026, 10, 27, 10, minute, tzinfo=timezone.utc)
    sber = brokapi.InstrumentId(key=brokapi.InstrumentKey(symbol="SBER", mic="TQBR"), native=brokapi.NativeId("TickerClass", (("ticker", "SBER"), ("class_code", "TQBR"))))
    price = Decimal(close)
    return brokapi.Candle(
        instrument=sber, timeframe=brokapi.Timeframe.M1, open=price, high=price, low=price, close=price,
        volume=Decimal(1), open_ts=start, close_ts=start + timedelta(minutes=1), ts=brokapi.Timestamps(rx_ns=0),
    )


def test_a_bar_of_a_subscription_is_passed_on_once_it_is_closed() -> None:
    bars = rules.ClosedBars()
    assert bars.push(bar(0, "100")) is None
    assert bars.push(bar(0, "101")) is None  # the same bar, still forming
    closed = bars.push(bar(1, "102"))  # the next bar began: the first one is final, with its last close
    assert closed is not None and closed.close == Decimal("101")
    assert bars.push(bar(0, "99")) is None  # a late update of a closed bar changes nothing
    assert bars.push(bar(2, "103")).close == Decimal("102")  # type: ignore[union-attr]


def test_the_forming_bar_of_the_history_is_left_out() -> None:
    history = [bar(0, "100"), bar(1, "101"), bar(2, "102")]
    now = datetime(2026, 10, 27, 10, 2, 30, tzinfo=timezone.utc)  # the bar of 10:02 is still forming
    assert [b.close for b in rules.closed_history(history, now)] == [Decimal("100"), Decimal("101")]
