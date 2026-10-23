"""A bot's day on the imitation of Bybit: read, reconcile, place, watch, cancel, fill, a stop order."""

from __future__ import annotations

from decimal import Decimal

import pytest

import brokapi
from conftest import next_event

Pair = tuple["brokapi.testing.MockBroker", brokapi.Client]


async def test_reading_gives_decimals_and_a_perpetual(bybit: Pair) -> None:
    _, client = bybit
    perp = await client.instrument("MOCKUSDT")
    assert str(perp) == "MOCKUSDT@linear"
    book = await client.order_book(perp, depth=5)
    assert isinstance(book, brokapi.OrderBookSnapshot)
    assert isinstance(book.bids[0].price, Decimal)
    assert book.bids[0].price < book.asks[0].price

    info = await client.get_instrument(perp)
    assert info.kind is brokapi.InstrumentKind.PERPETUAL
    params = await client.trading_params(perp)
    assert params.lot_size == Decimal("10")
    assert params.price_step == Decimal("0.01")

    portfolio = await client.portfolio()
    assert [b.currency for b in portfolio.balances] == ["USDT"]
    assert portfolio.positions[0].qty == Decimal("0.500")


async def test_place_events_cancel(bybit: Pair) -> None:
    _, client = bybit
    perp = await client.instrument("MOCKUSDT")
    params = await client.trading_params(perp)
    await client.subscribe_order_state()
    await client.reconcile([])

    cid = brokapi.new_cid()
    price = Decimal("50.00")
    ack = await client.place(cid=cid, instrument=perp, side=brokapi.Side.BUY, qty=params.min_qty, price=price)
    assert ack.cid == cid and ack.broker_id
    seen = await next_event(client, brokapi.OrderSnapshot, cid=cid)
    assert seen.source == "bybit-ud"
    assert (await client.order_state(cid)).price == price
    assert [o.cid for o in await client.active_orders()] == [cid]

    await client.cancel(cid)
    assert (await client.order_state(cid)).state.kind is brokapi.OrderStateKind.CANCELLED
    assert await client.active_orders() == []


async def test_the_replace_of_the_shared_interface_is_not_offered(bybit: Pair) -> None:
    _, client = bybit
    perp = await client.instrument("MOCKUSDT")
    params = await client.trading_params(perp)
    await client.reconcile([])
    cid = brokapi.new_cid()
    await client.place(cid=cid, instrument=perp, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("50.00"))
    assert (await client.caps()).ud.supports_replace is False
    with pytest.raises(brokapi.Unsupported):
        await client.replace(cid=brokapi.new_cid(), original_cid=cid, price=Decimal("51.00"))


async def test_a_quantity_off_the_step_is_refused_before_anything_is_sent(bybit: Pair) -> None:
    _, client = bybit
    perp = await client.instrument("MOCKUSDT")
    await client.reconcile([])
    with pytest.raises(brokapi.Rejected) as caught:
        await client.place(cid=brokapi.new_cid(), instrument=perp, side=brokapi.Side.BUY, qty=Decimal("15"), price=Decimal("50.00"))
    assert caught.value.code == "invalid_qty"


async def test_fill_comes_as_execution_and_order_state(bybit: Pair) -> None:
    mock, client = bybit
    perp = await client.instrument("MOCKUSDT")
    params = await client.trading_params(perp)
    await client.subscribe_order_state()
    await client.subscribe_executions()
    await client.reconcile([])
    cid = brokapi.new_cid()
    price = Decimal("50.00")
    await client.place(cid=cid, instrument=perp, side=brokapi.Side.BUY, qty=params.min_qty, price=price)
    await next_event(client, brokapi.OrderSnapshot, cid=cid)

    mock.fill(cid, qty=params.min_qty, price=price)

    filled = await next_event(client, brokapi.OrderSnapshot, cid=cid, filled=params.min_qty)
    assert filled.payload.state.is_final
    fill = await next_event(client, brokapi.Execution)
    assert fill.payload.cid == cid and fill.payload.price == price and fill.payload.qty == params.min_qty
    assert (await client.order_state(cid)).state.kind is brokapi.OrderStateKind.FILLED


async def test_a_stop_order_is_a_conditional_order_listed_apart(bybit: Pair) -> None:
    _, client = bybit
    perp = await client.instrument("MOCKUSDT")
    params = await client.trading_params(perp)
    await client.reconcile([])
    cid = brokapi.new_cid()
    await client.place_stop(
        cid=cid,
        instrument=perp,
        side=brokapi.Side.SELL,
        kind=brokapi.StopOrderKind.STOP_LOSS,
        qty=params.min_qty,
        stop_price=Decimal("90.00"),
    )
    assert [s.cid for s in await client.active_stop_orders()] == [cid]
    assert await client.active_orders() == []
    await client.cancel_stop(cid)
    assert await client.active_stop_orders() == []


async def test_the_raw_call_reaches_any_method_of_the_exchange(bybit: Pair) -> None:
    _, client = bybit
    time = await client.raw("GET /v5/market/time")
    assert int(time["timeSecond"]) > 1_700_000_000


async def test_the_key_pair_is_asked_for_by_name(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("BYBIT_API_KEY", "BYBIT_API_SECRET", "BYBIT_DEMO_API_KEY", "BYBIT_DEMO_API_SECRET", "BYBIT_ENV"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir("/")
    with pytest.raises(brokapi.Config) as caught:
        await brokapi.connect("bybit")  # type: ignore[call-overload]
    assert "BYBIT_API_KEY" in str(caught.value)
    # The demo account has a pair of its own: the main one is not asked for.
    with pytest.raises(brokapi.Config) as caught:
        await brokapi.connect("bybit", env="demo")  # type: ignore[call-overload]
    assert "BYBIT_DEMO_API_KEY" in str(caught.value)
