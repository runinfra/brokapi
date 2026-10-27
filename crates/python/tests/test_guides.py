"""The examples of the guides of the book run against the imitations: the quick start without a token, the recipe
"a bot with a stop: start and restart", the emergency stop of a bot, the first day at Finam and the move from
tinkoff-investments."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

import brokapi
from brokapi import testing

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
sys.path.insert(0, str(EXAMPLES))

import bybit_first_day  # noqa: E402
import emergency_stop  # noqa: E402
import finam_first_day  # noqa: E402
import from_tinkoff  # noqa: E402
import guard  # noqa: E402
import levers  # noqa: E402
import quickstart_tbank  # noqa: E402
import stop_restart  # noqa: E402


async def test_the_quick_start_without_a_token_reads_places_and_cancels(capsys: pytest.CaptureFixture[str]) -> None:
    await quickstart_tbank.on_the_imitation()
    out = capsys.readouterr().out
    assert "SBER: bid " in out
    assert "reconcile: 0 foreign order(s)" in out
    assert "placed: broker order id" in out
    assert "after cancel: Cancelled, orders alive: 0" in out
    assert "the price stands still: bid 273.9" in out
    assert "after set_price: bid 249.99, ask 250.01" in out


async def test_the_quick_start_runs_unchanged_on_every_imitation() -> None:
    """`first_order` is the code of a real run: it takes a client of any broker."""
    for mock_of in (testing.mock_bcs, testing.mock_finam, testing.mock_alor):
        async with await mock_of() as mock:
            await quickstart_tbank.first_order(await mock.connect())


async def test_the_sandbox_run_connects_to_the_sandbox_by_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    async with await testing.mock_tbank() as mock:
        real_connect = brokapi.connect
        seen: dict[str, object] = {}

        async def connect(broker: str, **kw: object) -> brokapi.Client:
            seen.update(kw)
            return await real_connect(broker, **{**mock.settings, "account_id": "acc-a", **kw})  # type: ignore[arg-type]

        monkeypatch.setattr(quickstart_tbank.brokapi, "connect", connect)
        await quickstart_tbank.on_the_sandbox()
        assert seen == {"sandbox": True}


async def test_the_first_day_on_bybit_reads_places_watches_cancels_and_stops(capsys: pytest.CaptureFixture[str]) -> None:
    await bybit_first_day.on_the_imitation()
    out = capsys.readouterr().out
    assert "MOCKUSDT@linear: bid 100.00, ask 100.01, smallest order 10, quantity step 10" in out
    assert "reconcile: 0 foreign order(s)" in out
    assert "placed: order " in out and " at 50.00" in out
    assert "in the stream: Acked" in out
    assert "after cancel: Cancelled, orders alive: 0" in out
    assert "stop orders waiting: 1, ordinary orders: 0" in out
    assert "stop orders after cancel: 0" in out


async def test_the_demo_run_on_bybit_asks_for_the_demo_account_by_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    async with await testing.mock_bybit() as mock:
        real_connect = brokapi.connect
        seen: dict[str, object] = {}

        async def connect(broker: str, **kw: object) -> brokapi.Client:
            seen.update(kw)
            return await real_connect(broker, **{**mock.settings, **kw})  # type: ignore[arg-type]

        monkeypatch.setattr(bybit_first_day.brokapi, "connect", connect)
        await bybit_first_day.on_the_demo_account()
        assert seen == {"env": "demo"}


MOCKS = [testing.mock_bcs, testing.mock_tbank, testing.mock_finam, testing.mock_alor]


@pytest.mark.parametrize("mock_of", MOCKS)
async def test_a_position_without_a_stop_gets_one_and_a_second_start_adds_none(mock_of: object) -> None:
    async with await mock_of() as mock:  # type: ignore[operator]
        client = await mock.connect()
        (position,) = await client.positions()  # the imitation starts with a holding and no stop: a bot after a crash

        assert await stop_restart.protect_on_start(client, [], lambda p: Decimal("100")) == []
        (stop,) = await client.active_stop_orders()
        assert stop.qty == position.qty  # the whole position, not one lot
        assert stop.side is brokapi.Side.SELL and stop.stop_price == Decimal("100")

        # The next start (a restart): the stop of the earlier run is found, no second one is put.
        assert await stop_restart.protect_on_start(client, [], lambda p: Decimal("100")) == []
        assert len(await client.active_stop_orders()) == 1


@pytest.mark.parametrize("mock_of", MOCKS)
async def test_a_lost_answer_of_the_stop_is_looked_up_at_the_start(mock_of: object) -> None:
    async with await mock_of() as mock:  # type: ignore[operator]
        client = await mock.connect()
        mock.lose_stop_answers()  # the broker takes the stop, the answer is lost: `Unknown`
        assert await stop_restart.protect_on_start(client, [], lambda p: Decimal("100")) == []
        assert len(await client.active_stop_orders()) == 1  # found, not doubled


async def test_a_position_that_is_not_the_bots_gets_no_stop() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        assert await stop_restart.protect_on_start(client, [], lambda p: None) == []
        assert await client.active_stop_orders() == []


async def test_a_stop_for_a_part_of_the_position_is_reported_not_topped_up() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        await client.reconcile()
        sber = await client.instrument("SBER")
        (position,) = await client.positions()
        params = await client.trading_params(sber)
        assert params.lot_size < position.qty
        await client.place_stop(
            cid=brokapi.new_cid(),
            instrument=sber,
            side=brokapi.Side.SELL,
            kind=brokapi.StopOrderKind.STOP_LOSS,
            qty=params.lot_size,
            stop_price=Decimal("100"),
        )
        left = await stop_restart.protect_on_start(client, [], lambda p: Decimal("100"))
        assert [p.instrument.key for p in left] == [position.instrument.key]
        assert len(await client.active_stop_orders()) == 1


@pytest.mark.parametrize("mock_of", MOCKS)
async def test_the_emergency_stop_takes_off_own_orders_and_leaves_the_stop_and_foreign_orders(mock_of: object) -> None:
    async with await mock_of() as mock:  # type: ignore[operator]
        bot = await mock.connect(name="bot")
        await bot.reconcile()
        sber = await bot.instrument("SBER")
        params = await bot.trading_params(sber)
        own = []
        for _ in range(2):
            cid = brokapi.new_cid()
            await bot.place(
                cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.lot_size, price=Decimal("100.00")
            )
            own.append(cid)
        foreign = brokapi.new_cid()  # an order the journal of the bot does not know (put by hand, by another program)
        await bot.place(
            cid=foreign, instrument=sber, side=brokapi.Side.BUY, qty=params.lot_size, price=Decimal("100.00")
        )
        await bot.place_stop(
            cid=brokapi.new_cid(),
            instrument=sber,
            side=brokapi.Side.SELL,
            kind=brokapi.StopOrderKind.STOP_LOSS,
            qty=params.lot_size,
            stop_price=Decimal("90"),
        )
        await bot.close()  # the bot is gone; its orders and its stop are at the exchange

        rescue = await mock.connect(name="rescue")  # a new process with the journal of the bot in its hands
        assert await emergency_stop.cancel_own_orders(rescue, own) == []
        left = [order.cid for order in await rescue.active_orders()]
        assert left == [foreign]  # the foreign order is not touched
        assert len(await rescue.active_stop_orders()) == 1  # and the protective stop stands


async def test_the_emergency_stop_reports_an_outcome_that_is_not_clear() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        await client.reconcile()
        sber = await client.instrument("SBER")
        cid = brokapi.new_cid()
        await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=1, price=Decimal("100.00"))
        real = client.cancel

        async def lost(*a: object, **kw: object) -> None:
            raise brokapi.Unknown()

        client.cancel = lost  # type: ignore[method-assign]
        assert await emergency_stop.cancel_own_orders(client, [cid]) == [cid]
        client.cancel = real  # type: ignore[method-assign]
        assert await emergency_stop.cancel_own_orders(client, [cid]) == []


async def test_lots_of_the_old_code_become_units_and_a_quotation_becomes_a_decimal() -> None:
    async with await testing.mock_tbank() as mock:
        client = await mock.connect()
        await client.reconcile()
        cid = await from_tinkoff.place_lots(client, "SBER", 2, Decimal("200.00"))
        (order,) = await client.active_orders()
        assert order.cid == cid and order.qty == Decimal(20)  # the lot of the imitation is 10
        with pytest.raises(brokapi.Rejected) as refused:  # a quantity that is not a whole number of lots
            await client.place(
                cid=brokapi.new_cid(), instrument=order.instrument, side=brokapi.Side.BUY, qty=15, price=Decimal("200.00")
            )
        assert refused.value.code == "invalid_qty"
    assert from_tinkoff.quotation_to_decimal(274, 50_000_000) == Decimal("274.05")
    assert from_tinkoff.quotation_to_decimal(0, -10_000_000) == Decimal("-0.01")
    assert from_tinkoff.quotation_to_decimal(-3, -500_000_000) == Decimal("-3.5")


async def test_the_first_day_at_finam_places_restarts_recognises_and_cancels(capsys: pytest.CaptureFixture[str]) -> None:
    async with await testing.mock_finam() as mock:
        assert await finam_first_day.first_day(mock.connect) is True
        out = capsys.readouterr().out
        assert "came back as ours: 0 foreign" in out
        assert "orders left after the cancel: 0" in out


async def test_the_levers_move_the_price_and_lose_an_answer() -> None:
    assert await levers.the_bot_sees_the_price_come_to_its_level() == [
        Decimal("250.99"),
        Decimal("249.99"),
        Decimal("248.99"),
    ]
    assert await levers.a_lost_answer_is_asked_about_not_sent_again() is brokapi.OrderStateKind.ACKED


async def test_the_limits_on_the_orders_refuse_before_anything_is_sent() -> None:
    assert await guard.look_but_do_not_trade() == ["bid 244.30", "refused: read_only", "refused: max_order_value"]
