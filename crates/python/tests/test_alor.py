"""A bot's day on the imitation of ALOR: read, reconcile, place, watch, cancel, fill."""

from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal

import pytest

import brokapi
from brokapi import errors
from conftest import next_event

Pair = tuple["brokapi.testing.MockBroker", brokapi.Client]


async def test_reading_gives_decimals(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    assert str(sber) == "SBER@TQBR"
    book = await client.order_book(sber, depth=5)
    assert isinstance(book, brokapi.OrderBookSnapshot)
    assert isinstance(book.bids[0].price, Decimal)
    assert isinstance(book.bids[0].qty, Decimal)
    assert book.bids[0].price < book.asks[0].price

    portfolio = await client.portfolio()
    assert all(isinstance(b.available, Decimal) for b in portfolio.balances)

    params = await client.trading_params(sber)
    assert params.price_step > 0
    assert isinstance(params.min_qty, Decimal)


async def test_candles_are_decimals_and_time_is_aware(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    from datetime import datetime, timedelta, timezone

    end = datetime(2026, 10, 20, tzinfo=timezone.utc)
    bars = await client.candles(sber, brokapi.Timeframe.M5, end - timedelta(hours=1), end)
    assert bars
    assert isinstance(bars[0].close, Decimal)
    assert bars[0].open_ts.tzinfo is not None
    assert bars[0].timeframe is brokapi.Timeframe.M5


async def test_trading_is_closed_before_reconcile(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    with pytest.raises(brokapi.NotReconciled) as caught:
        await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=1, price=Decimal("1"))
    assert str(caught.value) == "not reconciled: call reconcile() before trading commands"


async def test_place_events_cancel(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    book = await client.order_book(sber, depth=1)
    params = await client.trading_params(sber)
    price = brokapi.round_to_step(book.bids[0].price * Decimal("0.9"), params.price_step, brokapi.RoundMode.DOWN)

    await client.subscribe_order_state()
    report = await client.reconcile([])
    assert report.foreign == []

    cid = brokapi.new_cid()
    ack = await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=price)
    assert ack.cid == cid
    assert ack.broker_id

    seen = await next_event(client, brokapi.OrderSnapshot, cid=cid)
    assert seen.source == "alor-ud"
    assert seen.payload.price == price

    snapshot = await client.order_state(cid)
    assert snapshot.price == price
    assert not snapshot.state.is_final
    assert [o.cid for o in await client.active_orders()] == [cid]

    await client.cancel(cid)
    await next_event(client, brokapi.OrderSnapshot, cid=cid, state=brokapi.OrderState(
        kind=brokapi.OrderStateKind.CANCELLED, cid=cid))
    assert (await client.order_state(cid)).state.kind is brokapi.OrderStateKind.CANCELLED
    assert await client.active_orders() == []


async def test_a_price_off_the_step_is_refused_before_anything_is_sent(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)  # teaches the connector the step
    await client.reconcile([])
    off = params.price_step * Decimal("10.5")
    with pytest.raises(brokapi.Rejected) as caught:
        await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=off)
    assert caught.value.code == "price_off_step"
    assert caught.value.error_class is errors.ErrorClass.FATAL


async def test_a_limit_order_without_a_price_is_rejected(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    await client.reconcile([])
    with pytest.raises(brokapi.Rejected) as caught:
        await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=1)
    assert caught.value.code == "missing_price"


async def test_a_float_price_is_refused_in_python(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    with pytest.raises(TypeError, match="float"):
        await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=1, price=0.1)  # type: ignore[arg-type]


async def test_fill_comes_as_execution_and_order_state(alor: Pair) -> None:
    mock, client = alor
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    await client.subscribe_order_state()
    await client.subscribe_executions()
    await client.reconcile([])
    cid = brokapi.new_cid()
    price = Decimal("200.00")
    await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=price)
    await next_event(client, brokapi.OrderSnapshot, cid=cid)

    mock.fill(cid, qty=params.min_qty, price=price)

    # The order event comes first, then the fill.
    filled = await next_event(client, brokapi.OrderSnapshot, cid=cid, filled=params.min_qty)
    assert filled.payload.state.is_final
    fill = await next_event(client, brokapi.Execution)
    assert fill.payload.cid == cid
    assert fill.payload.price == price
    assert fill.payload.qty == params.min_qty
    assert isinstance(fill.payload.commission, Decimal)
    assert (await client.order_state(cid)).state.kind is brokapi.OrderStateKind.FILLED


async def test_book_subscription_delivers_snapshots(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    sub = await client.subscribe_order_book(sber, depth=5)
    envelope = await next_event(client, brokapi.OrderBookSnapshot)
    assert envelope.sub == sub
    assert envelope.source == "alor-md"
    await client.unsubscribe(sub)


async def test_trades_need_an_explicit_backpressure(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    with pytest.raises(TypeError):
        await client.subscribe_trades(sber)  # type: ignore[call-arg]


async def test_replace_makes_a_new_order_with_the_new_cid(alor: Pair) -> None:
    _, client = alor
    assert (await client.caps()).ud.supports_replace
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    await client.reconcile([])
    first, second = brokapi.new_cid(), brokapi.new_cid()
    await client.place(cid=first, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00"))
    ack = await client.replace(cid=second, original_cid=first, price=Decimal("101.00"))
    assert ack.cid == second
    assert (await client.order_state(second)).price == Decimal("101.00")
    assert [o.cid for o in await client.active_orders()] == [second]


async def test_the_quantity_is_in_pieces_and_a_part_of_a_lot_is_refused(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    assert params.lot_size == Decimal(10)
    await client.reconcile([])
    with pytest.raises(brokapi.Rejected) as caught:
        await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=Decimal(15), price=Decimal("100.00"))
    assert caught.value.code == "invalid_qty"


async def test_close_ends_the_stream_and_is_safe_twice(alor: Pair) -> None:
    _, client = alor
    reports = await client.close()
    assert len(reports) == 2
    assert await client.close() == []
    # What the connectors sent before the close is still delivered, then the stream ends and stays ended.
    assert all(e.source in ("alor-md", "alor-ud") for e in [e async for e in client.events])
    assert [e async for e in client.events] == []
    with pytest.raises(EOFError):
        await client.events.get(timeout=1)
    with pytest.raises(RuntimeError, match="closed"):
        await client.portfolio()


async def test_a_cancelled_wait_does_not_cancel_the_command(alor: Pair) -> None:
    _, client = alor
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    await client.subscribe_order_state()
    await client.reconcile([])
    cid = brokapi.new_cid()
    task = asyncio.create_task(
        client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("200.00"))
    )
    await asyncio.sleep(0)  # let the task send the command, then stop waiting for the answer
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # The order is there anyway: the stream says so, and order_state agrees.
    await next_event(client, brokapi.OrderSnapshot, cid=cid)
    assert (await client.order_state(cid)).cid == cid


async def test_unknown_names_are_rejected_early() -> None:
    with pytest.raises(ValueError, match="unknown broker"):
        await brokapi.connect("nope")  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="does not take"):
        await brokapi.connect("alor", token="x")  # type: ignore[call-overload]


async def test_missing_settings_say_which(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALOR_REFRESH_TOKEN", raising=False)
    monkeypatch.chdir("/")
    with pytest.raises(brokapi.Config) as caught:
        await brokapi.connect("alor")
    assert "ALOR_REFRESH_TOKEN" in str(caught.value)


async def test_settings_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    async with await brokapi.testing.mock_alor() as mock:
        monkeypatch.setenv("ALOR_REFRESH_TOKEN", "mock-refresh-token")
        monkeypatch.setenv("ALOR_PORTFOLIO", "D00001")
        client = await brokapi.connect("alor", endpoint=mock.settings["endpoint"])
        assert (await client.caps()).ud.sandbox is False
        await client.close(None)


def test_cid_is_a_uuid() -> None:
    assert isinstance(brokapi.new_cid(), uuid.UUID)
