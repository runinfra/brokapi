"""A bot's unresolved orders survive failures on every broker, through the real gRPC gateway.

Only local protocol imitations are used. Each scenario checks the account after recovery, not just reconnection.
"""

from decimal import Decimal

import pytest

import brokapi
from brokapi import testing

from conftest import next_event

BROKERS = ["bcs", "tbank", "finam", "alor", "bybit"]


async def order_args(client: brokapi.Client) -> dict:
    ticker = "MOCKUSDT" if client.broker == "bybit" else "SBER"
    instrument = await client.instrument(ticker)
    params = await client.trading_params(instrument)
    return dict(instrument=instrument, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00"))


@pytest.mark.parametrize("broker", BROKERS)
async def test_lost_answer_then_gateway_restart_recovers_one_order(broker: str) -> None:
    async with await getattr(testing, f"mock_{broker}")() as mock:
        gateway = await mock.start_gateway()
        client = await gateway.connect(bot="recovery")
        args = await order_args(client)
        await client.subscribe_order_state()
        await client.reconcile([])
        cid = brokapi.new_cid()

        mock.lose_order_answers()
        with pytest.raises(brokapi.Unknown):
            await client.place(cid=cid, **args)
        mock.lose_order_answers(False)
        assert [o.cid for o in await client.active_orders()] == [cid]
        with pytest.raises(brokapi.Rejected) as caught:
            await client.place(cid=cid, **args)
        assert caught.value.code == "duplicate_place"

        await gateway.restart()
        gap = (await next_event(client, brokapi.Gap)).payload
        assert gap.reason == "new_epoch" and gap.subscriptions_lost
        with pytest.raises(brokapi.Degraded):
            await client.place(cid=brokapi.new_cid(), **args)

        await client.subscribe_order_state()
        report = await client.reconcile([cid])
        assert [o.cid for o in report.confirmed] == [cid]
        assert report.foreign == []
        assert report.closed_offline == []
        # After a restart the bot resolves its recorded cid, never resends it:
        # the connector's once-only memory belongs to one process (orders.md).
        assert [o.cid for o in await client.active_orders()] == [cid]
        await client.cancel(cid)
        assert await client.active_orders() == []


@pytest.mark.parametrize("broker", BROKERS)
async def test_a_protective_stop_survives_restart_and_can_be_cancelled_by_broker_id(broker: str) -> None:
    async with await getattr(testing, f"mock_{broker}")() as mock:
        gateway = await mock.start_gateway()
        client = await gateway.connect(bot="protection")
        args = await order_args(client)
        await client.reconcile([])
        await client.place_stop(
            cid=brokapi.new_cid(), instrument=args["instrument"], side=brokapi.Side.SELL,
            kind=brokapi.StopOrderKind.STOP_LOSS, qty=args["qty"], stop_price=Decimal("90.00"),
        )
        before = await client.active_stop_orders()
        assert len(before) == 1
        original = before[0]
        assert original.broker_id
        await client.close()
        await gateway.restart()

        again = await gateway.connect(bot="protection")
        await again.reconcile([])
        stops = await again.active_stop_orders()
        assert len(stops) == 1
        stop = stops[0]
        assert stop.broker_id == original.broker_id
        assert stop.instrument == original.instrument
        assert stop.side is brokapi.Side.SELL
        assert stop.qty == args["qty"] and stop.stop_price == Decimal("90.00")
        assert stop.state.kind is brokapi.StopOrderStateKind.ACTIVE
        # T-Bank does not echo cid: after a restart the broker's id still identifies the stop.
        await again.cancel_stop(brokapi.StopOrderRef.broker(stop.broker_id))
        assert await again.active_stop_orders() == []


@pytest.mark.parametrize("broker", BROKERS)
async def test_a_fill_while_the_bot_is_away_is_resolved_after_gateway_restart(broker: str) -> None:
    async with await getattr(testing, f"mock_{broker}")() as mock:
        gateway = await mock.start_gateway()
        client = await gateway.connect(bot="before")
        args = await order_args(client)
        await client.reconcile([])
        cid = brokapi.new_cid()
        await client.place(cid=cid, **args)
        await client.close()
        mock.fill(cid, qty=args["qty"], price=args["price"])
        await gateway.restart()

        again = await gateway.connect(bot="after")
        report = await again.reconcile([cid])
        assert report.confirmed == []
        assert report.foreign == []
        assert report.closed_offline == [cid]
        snapshot = await again.order_state(cid)
        assert snapshot.state.kind is brokapi.OrderStateKind.FILLED
        assert snapshot.filled == args["qty"]
        assert await again.active_orders() == []


@pytest.mark.parametrize("broker", BROKERS)
async def test_broker_stream_loss_requires_reconcile_without_losing_the_open_order(broker: str) -> None:
    async with await getattr(testing, f"mock_{broker}")() as mock:
        client = await (await mock.start_gateway()).connect()
        args = await order_args(client)
        await client.subscribe_order_state()
        await client.reconcile([])
        cid = brokapi.new_cid()
        await client.place(cid=cid, **args)

        mock.cut_streams()
        await next_event(client, brokapi.Reconnected)
        with pytest.raises(brokapi.Degraded):
            await client.place(cid=brokapi.new_cid(), **args)
        report = await client.reconcile([cid])
        assert [o.cid for o in report.confirmed] == [cid]
        assert report.foreign == []
        assert [o.cid for o in await client.active_orders()] == [cid]

        await client.cancel(cid)
        assert await client.active_orders() == []
