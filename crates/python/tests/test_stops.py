"""Stop orders on the imitations of all three brokers: put, list, cancel, repeat, trigger, and what is not there."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import brokapi
from brokapi import testing
from brokapi.models import StopExpiry, StopOrderKind, StopOrderRef, StopOrderState, StopOrderStateKind

BROKERS = ["bcs", "tbank", "finam"]
ROADS = BROKERS + [f"gw_{b}" for b in BROKERS]  # the connection in this process, and through a gateway
FAR = Decimal("100")  # far below the market of the imitations (about 250): a sell stop that only waits


@pytest.fixture(params=ROADS)
def pair(request: pytest.FixtureRequest) -> tuple[testing.MockBroker, brokapi.Client]:
    return request.getfixturevalue(request.param)


async def put_stop(
    client: brokapi.Client, **overrides: object
) -> tuple[uuid.UUID, brokapi.OrderAck, brokapi.InstrumentId]:
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    cid = brokapi.new_cid()
    args: dict[str, object] = dict(
        cid=cid,
        instrument=sber,
        side=brokapi.Side.SELL,
        kind=StopOrderKind.STOP_LOSS,
        qty=params.lot_size,
        stop_price=FAR,
    )
    args.update(overrides)
    ack = await client.place_stop(**args)  # type: ignore[arg-type]
    return args["cid"], ack, sber  # type: ignore[return-value]


async def test_a_stop_far_from_the_market_waits_in_the_list_and_is_cancelled(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    _, client = pair
    assert (await client.caps()).ud.supports_stop_orders
    await client.reconcile()

    cid, ack, sber = await put_stop(client)
    assert ack.cid == cid and ack.broker_id

    (stop,) = await client.active_stop_orders()
    assert stop.broker_id == ack.broker_id
    assert stop.cid == cid
    assert stop.instrument.key == sber.key
    assert (stop.side, stop.kind) == (brokapi.Side.SELL, StopOrderKind.STOP_LOSS)
    assert stop.stop_price == FAR and isinstance(stop.stop_price, Decimal)
    assert stop.limit_price is None  # a market order at the trigger
    assert stop.expiry == StopExpiry.good_till_cancel()
    assert stop.state == StopOrderState(StopOrderStateKind.ACTIVE) and not stop.state.is_final
    # A stop order is not a plain order: the list of orders does not show it.
    assert await client.active_orders() == []

    await client.cancel_stop(cid)
    assert await client.active_stop_orders() == []


async def test_a_stop_can_be_cancelled_by_the_id_of_the_broker(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    await client.reconcile()
    _, ack, _ = await put_stop(client)
    await client.cancel_stop(StopOrderRef.broker(ack.broker_id))
    assert await client.active_stop_orders() == []


async def test_the_same_cid_is_not_placed_twice(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    await client.reconcile()
    cid, _, sber = await put_stop(client)
    with pytest.raises(brokapi.Rejected) as error:
        await client.place_stop(
            cid=cid, instrument=sber, side=brokapi.Side.SELL, kind=StopOrderKind.STOP_LOSS, qty=10, stop_price=FAR
        )
    assert error.value.code == "duplicate_place"
    assert len(await client.active_stop_orders()) == 1  # one stop at the broker, not two


async def test_a_stop_limit_without_a_limit_price_is_refused_before_anything_is_sent(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    _, client = pair
    await client.reconcile()
    with pytest.raises(brokapi.Rejected) as error:
        await put_stop(client, kind=StopOrderKind.STOP_LIMIT)
    assert error.value.code == "missing_limit_price"
    assert await client.active_stop_orders() == []

    cid, _, _ = await put_stop(client, kind=StopOrderKind.STOP_LIMIT, limit_price=Decimal("99.5"))
    (stop,) = await client.active_stop_orders()
    assert (stop.kind, stop.limit_price, stop.cid) == (StopOrderKind.STOP_LIMIT, Decimal("99.5"), cid)


async def test_stop_orders_are_behind_the_same_gate_as_plain_orders(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    _, client = pair
    with pytest.raises(brokapi.NotReconciled):
        await put_stop(client)
    with pytest.raises(brokapi.NotReconciled):
        await client.cancel_stop(brokapi.new_cid())
    await client.reconcile()
    await put_stop(client)


async def test_cancelling_a_stop_that_is_not_there_is_a_refusal(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    await client.reconcile()
    with pytest.raises(brokapi.Rejected):
        await client.cancel_stop(StopOrderRef.broker("no-such-stop"))
    with pytest.raises(brokapi.Rejected):
        await client.cancel_stop(brokapi.new_cid())  # a cid this run has not seen


async def test_no_broker_streams_the_state_of_stop_orders(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    with pytest.raises(brokapi.Unsupported) as error:
        await client.subscribe_stop_orders()
    assert error.value.capability == "stop_order_stream"
    assert str(error.value) == "unsupported capability: stop_order_stream"
    assert error.value.error_class is brokapi.ErrorClass.FATAL


async def test_a_stop_is_named_by_a_ref_or_a_uuid_and_nothing_else(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    _, client = pair
    await client.reconcile()
    with pytest.raises(TypeError):
        await client.cancel_stop("ORD1")  # type: ignore[arg-type]  # a string is a cid or a broker id: say which
    with pytest.raises(TypeError, match="float"):
        await put_stop(client, stop_price=100.5)


# ---------------------------------------------------------------------------------------------------- the differences


async def test_the_lifetime_of_a_stop_depends_on_the_broker(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    mock, client = pair
    await client.reconcile()
    until = datetime(2031, 5, 4, 12, 0, tzinfo=timezone.utc)

    # End of day: Finam only.
    if mock.broker == "finam":
        await put_stop(client, expiry=StopExpiry.end_of_day())
        assert (await client.active_stop_orders())[-1].expiry == StopExpiry.end_of_day()
    else:
        with pytest.raises(brokapi.Rejected) as error:
            await put_stop(client, expiry=StopExpiry.end_of_day())
        assert error.value.code == "unsupported_expiry"

    # Until a date: T-Bank, and Finam for a take profit only.
    if mock.broker == "tbank":
        await put_stop(client, expiry=StopExpiry.good_till_date(until))
        assert (await client.active_stop_orders())[-1].expiry == StopExpiry.good_till_date(until)
    else:
        with pytest.raises(brokapi.Rejected) as error:
            await put_stop(client, expiry=StopExpiry.good_till_date(until))
        assert error.value.code == "unsupported_expiry"


async def test_finam_has_no_limit_price_for_a_take_profit_the_others_do(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    mock, client = pair
    await client.reconcile()
    kind = StopOrderKind.TAKE_PROFIT
    if mock.broker == "finam":
        with pytest.raises(brokapi.Rejected) as error:
            await put_stop(client, kind=kind, stop_price=Decimal("400"), limit_price=Decimal("399"))
        assert error.value.code == "unsupported_limit_price"
    else:
        await put_stop(client, kind=kind, stop_price=Decimal("400"), limit_price=Decimal("399"))
        (stop,) = await client.active_stop_orders()
        assert (stop.kind, stop.stop_price, stop.limit_price) == (kind, Decimal("400"), Decimal("399"))


# ---------------------------------------------------------------------------------------------------- the trigger


async def trigger_a_stop(mock: testing.MockBroker, client: brokapi.Client) -> None:
    """A stop that triggers leaves the list of stops and is, from then on, a plain order that is filled."""
    await client.subscribe_order_state()
    await client.subscribe_executions()
    await client.reconcile()
    cid, _, _ = await put_stop(client, stop_price=Decimal("240"))
    assert len(await client.active_stop_orders()) == 1

    mock.fill(cid, qty=Decimal(10), price=Decimal("240"))

    async def filled() -> None:
        async for envelope in client.events:
            payload = envelope.payload
            if isinstance(payload, brokapi.Execution) and payload.cid == cid:
                return

    await asyncio.wait_for(filled(), 10)
    assert await client.active_stop_orders() == []
    assert (await client.order_state(cid)).state.kind is brokapi.OrderStateKind.FILLED


async def test_bcs_stop_that_triggers_becomes_a_filled_order(bcs: tuple[testing.MockBroker, brokapi.Client]) -> None:
    await trigger_a_stop(*bcs)


async def test_finam_stop_that_triggers_becomes_a_filled_order(finam: tuple[testing.MockBroker, brokapi.Client]) -> None:
    await trigger_a_stop(*finam)


async def test_the_imitation_of_tbank_cannot_trigger_a_stop(tbank: tuple[testing.MockBroker, brokapi.Client]) -> None:
    """The stop stays where it is, and the imitation says so: no panic, and it keeps working."""
    mock, client = tbank
    await client.reconcile()
    cid, _, _ = await put_stop(client)
    with pytest.raises(ValueError, match="cannot trigger a stop"):
        mock.fill(cid)
    assert len(await client.active_stop_orders()) == 1
    await client.cancel_stop(cid)


# ---------------------------------------------------------------------------------------------------- the types


def test_the_direction_of_the_trigger_follows_the_kind_and_the_side() -> None:
    sell, buy = brokapi.Side.SELL, brokapi.Side.BUY
    assert not StopOrderKind.STOP_LOSS.triggers_on_rise(sell)
    assert StopOrderKind.STOP_LOSS.triggers_on_rise(buy)
    assert StopOrderKind.TAKE_PROFIT.triggers_on_rise(sell)
    assert not StopOrderKind.TAKE_PROFIT.triggers_on_rise(buy)
    assert not StopOrderKind.STOP_LIMIT.triggers_on_rise(sell)
    assert StopOrderKind.STOP_LIMIT.triggers_on_rise(buy)


def test_the_json_form_is_the_serde_form_of_the_rust_types() -> None:
    cid = uuid.UUID(int=1)
    until = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    assert StopExpiry.good_till_cancel()._to_json() == "GoodTillCancel"
    assert StopExpiry.end_of_day()._to_json() == "EndOfDay"
    assert StopExpiry.good_till_date(until)._to_json() == {"GoodTillDate": 1_000_000_000}
    assert StopExpiry._from_json({"GoodTillDate": 1_000_000_000}) == StopExpiry.good_till_date(until)
    assert StopOrderRef.cid(cid)._to_json() == {"Cid": str(cid)}
    assert StopOrderRef.broker("X1")._to_json() == {"Broker": "X1"}
    assert StopOrderState._from_json("Triggered").is_final
    other = StopOrderState._from_json({"Other": "PartiallyDone"})
    assert (other.kind, other.text, other.is_final) == (StopOrderStateKind.OTHER, "PartiallyDone", False)
    with pytest.raises(ValueError):
        StopExpiry("GoodTillDate")._to_json()


def test_a_naive_moment_is_refused() -> None:
    with pytest.raises(ValueError, match="time zone"):
        StopExpiry.good_till_date(datetime(2031, 1, 1))._to_json()
