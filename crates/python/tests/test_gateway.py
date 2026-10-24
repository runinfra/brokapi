"""A bot behind a gateway: what is the same as with the broker, what differs, and what the gateway adds.

The gateway runs in the test process on an imitation of a broker (`MockBroker.start_gateway`). The scenarios of
`test_brokers.py`, `test_stops.py` and `test_raw.py` run on both roads; this file holds what only a gateway has.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import brokapi
from brokapi import _native, testing

from conftest import next_event


async def reconcile_and_place(client: brokapi.Client) -> uuid.UUID:
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    await client.reconcile([])
    cid = brokapi.new_cid()
    await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00"))
    return cid


async def test_the_client_is_the_same_class_with_the_names_of_the_gateway() -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway("bcs-main")
        client = await gateway.connect(bot="my-bot")
        assert isinstance(client, brokapi.Client)
        assert (client.broker, client.name, client.gateway) == ("bcs", "my-bot", "bcs-main")
        assert "my-bot" in repr(client)
        # A client made by `connect` has no gateway.
        direct = await mock.connect()
        assert direct.gateway is None


async def test_the_events_say_which_gateway_connection_they_come_from() -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway("bcs-main")
        client = await gateway.connect(bot="my-bot")
        sber = await client.instrument("SBER")
        await client.subscribe_order_book(sber, depth=1)
        envelope = await next_event(client, brokapi.OrderBookSnapshot)
        assert envelope.source == "bcs-main-md"


async def test_a_gateway_client_has_no_limiter_of_its_own() -> None:
    async with await testing.mock_bcs() as mock:
        client = await (await mock.start_gateway()).connect()
        with pytest.raises(brokapi.Unsupported) as caught:
            client.limiter
        assert caught.value.capability == "limiter"


async def test_the_trading_gate_is_closed_until_the_first_reconcile() -> None:
    async with await testing.mock_bcs() as mock:
        client = await (await mock.start_gateway()).connect()
        sber = await client.instrument("SBER")
        with pytest.raises(brokapi.NotReconciled):
            await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=1, price=Decimal("1"))
        with pytest.raises(brokapi.NotReconciled):
            await client.cancel(brokapi.new_cid())


async def test_a_new_gateway_process_is_a_gap_and_the_bot_mends_it() -> None:
    """The gateway dies and another takes its place. The bot is told, the gate closes, and after a reconcile it goes on."""
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway("bcs-main")
        client = await gateway.connect(bot="my-bot")
        first = await reconcile_and_place(client)
        await client.subscribe_order_state()

        await gateway.restart()

        gap = (await next_event(client, brokapi.Gap)).payload
        assert isinstance(gap, brokapi.Gap)
        assert gap.reason == "new_epoch" and gap.subscriptions_lost
        # Events were lost, so trading waits, as it does after an `Overflow` of a connector. Nothing was sent.
        sber = await client.instrument("SBER")
        with pytest.raises(brokapi.Degraded):
            await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=1, price=Decimal("1"))
        # The new process holds none of the subscriptions: subscribe again, then reconcile with the ledger.
        await client.subscribe_order_state()
        await client.subscribe_executions()
        report = await client.reconcile([first])
        assert [o.cid for o in report.confirmed] == [first], "the order of the first process is in the journal of the gateway"

        # The gate is open and the events flow again.
        mock.fill(first, qty=Decimal(1), price=Decimal("100.00"))
        order = await next_event(client, brokapi.OrderSnapshot, cid=first)
        assert order.source == "bcs-main-ud"
        second = brokapi.new_cid()
        await client.place(cid=second, instrument=sber, side=brokapi.Side.BUY, qty=1, price=Decimal("100.00"))
        await client.cancel(second)


async def test_two_bots_share_a_gateway_and_each_asks_for_its_own_orders() -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway()
        mine = await gateway.connect(bot="mine", own_orders_only=True)
        other = await gateway.connect(bot="other")
        await mine.subscribe_order_state()
        await other.subscribe_order_state()
        theirs = await reconcile_and_place(other)
        ours = await reconcile_and_place(mine)
        await mine.cancel(ours)

        seen: list[uuid.UUID] = []

        async def read() -> None:
            async for envelope in mine.events:
                if isinstance(envelope.payload, brokapi.OrderSnapshot):
                    seen.append(envelope.payload.cid)
                    if envelope.payload.cid == ours and envelope.payload.state.is_final:
                        return

        await asyncio.wait_for(read(), 10)
        assert theirs not in seen and ours in seen
        # The bot that did not ask for the filter sees both.
        both: set[uuid.UUID] = set()

        async def read_all() -> None:
            async for envelope in other.events:
                if isinstance(envelope.payload, brokapi.OrderSnapshot):
                    both.add(envelope.payload.cid)
                    if {theirs, ours} <= both:
                        return

        await asyncio.wait_for(read_all(), 10)
        # Neither bot is asked about the orders of the other as `foreign` by a reconcile that names them.
        report = await mine.reconcile([ours])
        assert [o.cid for o in report.foreign] == [theirs]


async def test_closing_the_client_leaves_the_gateway_and_the_orders() -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway()
        client = await gateway.connect(bot="first")
        cid = await reconcile_and_place(client)

        assert await client.close() == []
        assert await client.close() == []  # twice is safe
        with pytest.raises(RuntimeError, match="closed"):
            await client.active_orders()

        async def to_the_end() -> None:
            # What came before the close is still delivered, then the iteration stops.
            async for _ in client.events:
                pass

        await asyncio.wait_for(to_the_end(), 10)

        # The gateway and the order are still there for the next bot.
        again = await gateway.connect(bot="second")
        report = await again.reconcile([cid])
        assert [o.cid for o in report.confirmed] == [cid]


async def test_a_long_history_is_read_in_the_windows_the_broker_takes() -> None:
    """The gateway tells the client how long one request may be, so the pager cuts the history like it does at the broker."""
    async with await testing.mock_bcs(synthetic_candles=True) as mock:
        direct = await mock.connect()
        client = await (await mock.start_gateway()).connect()
        sber = await client.instrument("SBER")
        start = datetime(2026, 10, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=3)  # 4320 minute bars: three requests of at most 1440
        through = await client.load_candles(sber, brokapi.Timeframe.M1, start, end)
        at_broker = await direct.load_candles(await direct.instrument("SBER"), brokapi.Timeframe.M1, start, end)

        def bars(candles: list[brokapi.Candle]) -> list[tuple[object, ...]]:
            # The receive time differs by road; the bar does not.
            return [(c.open_ts, c.open, c.high, c.low, c.close, c.volume) for c in candles]

        assert bars(through) == bars(at_broker)
        assert len(through) >= 4320
        # One request over the limit is refused by the broker, as it is at the broker.
        with pytest.raises(brokapi.Rejected):
            await client.candles(sber, brokapi.Timeframe.M1, start, end)


async def test_merge_reads_a_direct_client_and_a_gateway_client_together() -> None:
    async with await testing.mock_bcs() as mock:
        direct = await mock.connect(name="direct")
        client = await (await mock.start_gateway("gw")).connect(bot="bot")
        for c in (direct, client):
            await c.subscribe_order_book(await c.instrument("SBER"), depth=1)
        sources: set[str] = set()
        async for envelope in brokapi.merge(direct, client):
            sources.add(envelope.source)
            if sources >= {"direct-md", "gw-md"}:
                break


async def test_a_directory_without_a_gateway_is_a_configuration_error(tmp_path: Path) -> None:
    with pytest.raises(brokapi.Config, match="no gateway address"):
        await brokapi.connect_gateway(dir=tmp_path, bot="my-bot")


async def test_the_key_of_another_gateway_is_refused() -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway()
        key = Path(gateway.directory) / "access.key"
        key.write_text("not-the-key\n")
        with pytest.raises(brokapi.Config, match="refused the access"):
            await gateway.connect(bot="my-bot")


async def test_a_bad_bot_name_is_refused_before_anything_is_sent() -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway()
        with pytest.raises(brokapi.Config, match="not valid"):
            await gateway.connect(bot="my bot")


async def test_the_gateway_is_named_by_its_name_and_the_state_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    """`connect_gateway("bcs-main", state=...)` looks in `<state>/gateways/bcs-main`; the default state is `brokapi.state`."""
    asked: list[str] = []

    async def fake(directory: str, bot: str, *rest: object) -> object:
        asked.append(os.path.normpath(directory))
        raise brokapi.Config("stop here")

    monkeypatch.setattr(_native, "connect_gateway", fake)
    monkeypatch.delenv("BROKAPI_STATE", raising=False)
    for kwargs, expected in [
        ({"state": "/srv/state"}, os.path.normpath(os.path.join("/srv/state", "gateways", "bcs-main"))),
        ({}, os.path.normpath(os.path.join("brokapi.state", "gateways", "bcs-main"))),
    ]:
        with pytest.raises(brokapi.Config):
            await brokapi.connect_gateway("bcs-main", bot="b", **kwargs)
        assert asked.pop() == expected
    monkeypatch.setenv("BROKAPI_STATE", "/from/env")
    with pytest.raises(brokapi.Config):
        await brokapi.connect_gateway("bcs-main", bot="b")
    assert asked.pop() == os.path.normpath(os.path.join("/from/env", "gateways", "bcs-main"))
    with pytest.raises(brokapi.Config):
        await brokapi.connect_gateway(bot="b", dir="/the/dir")
    assert asked.pop() == os.path.normpath("/the/dir")
    with pytest.raises(TypeError, match="not both"):
        await brokapi.connect_gateway("bcs-main", bot="b", dir="/the/dir")
    with pytest.raises(TypeError, match="name the gateway"):
        await brokapi.connect_gateway(bot="b")


def test_a_gap_is_not_an_event_of_a_connector_and_is_not_written_to_a_log(tmp_path: Path) -> None:
    gap = brokapi.Envelope(
        source="gw-gateway",
        sub=None,
        ts=brokapi.Timestamps(rx_ns=0),
        payload=brokapi.Gap(reason="too_old", requested_after=1, first_available=9),
    )
    with brokapi.record.EnvelopeWriter(tmp_path / "log.jsonl") as log:
        with pytest.raises(TypeError, match="wire form"):
            log.write(gap)


async def test_a_recording_of_a_gateway_client_passes_a_gap_on_and_does_not_write_it(tmp_path: Path) -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway()
        client = await gateway.connect(bot="my-bot")
        await client.subscribe_order_state()
        await gateway.restart()
        kinds: list[type] = []

        async def until_the_gap() -> None:
            async with contextlib.aclosing(brokapi.record.record_events(client.events, tmp_path / "run.jsonl")) as recorded:
                async for envelope in recorded:
                    kinds.append(type(envelope.payload))
                    if isinstance(envelope.payload, brokapi.Gap):
                        break

        await asyncio.wait_for(until_the_gap(), 20)
        assert kinds[-1] is brokapi.Gap
        # What was written reads back, without the Gap.
        written = [e.payload async for e in brokapi.record.replay(tmp_path / "run.jsonl")]
        assert not any(isinstance(p, brokapi.Gap) for p in written)
        assert len(written) == len(kinds) - 1
