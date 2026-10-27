"""The limits of your own on the orders: a read-only mode and caps on one order, checked before anything is sent.

The same on a direct connection and behind a gateway, and the environment of the person sets a floor that the code of a
bot cannot lower.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

import brokapi
from brokapi import testing


async def place(client: brokapi.Client, qty: str, price: str | None, cid: object = None) -> brokapi.OrderAck:
    sber = await client.instrument("SBER")
    return await client.place(
        cid=cid or brokapi.new_cid(),
        instrument=sber,
        side=brokapi.Side.BUY,
        qty=Decimal(qty),
        price=None if price is None else Decimal(price),
        order_type=brokapi.OrderType.MARKET if price is None else brokapi.OrderType.LIMIT,
    )


def refused(caught: pytest.ExceptionInfo[brokapi.Rejected], code: str) -> None:
    assert caught.value.code == code
    assert "nothing was sent" in str(caught.value)


async def test_a_read_only_client_reads_and_sends_no_command() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect(read_only=True)
        sber = await client.instrument("SBER")
        assert (await client.order_book(sber, depth=1)).bids
        assert (await client.portfolio()).positions
        await client.reconcile([])

        with pytest.raises(brokapi.Rejected) as caught:
            await place(client, "1", "240")
        refused(caught, "read_only")
        with pytest.raises(brokapi.Rejected) as caught:
            await client.cancel(brokapi.new_cid())
        refused(caught, "read_only")
        assert await client.active_orders() == []

        # raw passes a read by its name only: the flag `command` is the word of the caller and is not trusted.
        assert await client.raw("GetPortfolio")
        with pytest.raises(brokapi.Rejected, match="read_only"):
            await client.raw("POST /trade-api-bff-operations/api/v1/orders", {"body": {}})
        with pytest.raises(brokapi.Rejected, match="read_only"):
            await client.raw("GetPortfolio", command=True)


async def test_the_caps_refuse_one_order_and_leave_its_cid_unused() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect(max_order_qty=10, max_order_value="2500")
        await client.reconcile([])
        cid = brokapi.new_cid()
        with pytest.raises(brokapi.Rejected) as caught:
            await place(client, "11", "100", cid)
        refused(caught, "max_order_qty")
        with pytest.raises(brokapi.Rejected) as caught:
            await place(client, "10", "250.01", cid)
        refused(caught, "max_order_value")
        # A market order has no price to count by: with the value capped it does not pass.
        with pytest.raises(brokapi.Rejected) as caught:
            await place(client, "1", None, cid)
        refused(caught, "max_order_value")
        assert await client.active_orders() == []

        # Nothing was sent, so the cid is still good.
        ack = await place(client, "10", "250", cid)
        assert ack.cid == cid


async def test_a_replace_is_counted_with_what_it_keeps_of_the_old_order() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect(max_order_value="2500")
        await client.reconcile([])
        first = brokapi.new_cid()
        await place(client, "10", "200", first)
        with pytest.raises(brokapi.Rejected) as caught:
            await client.replace(cid=brokapi.new_cid(), original_cid=first, price=Decimal("300"))
        refused(caught, "max_order_value")
        assert (await client.order_state(first)).price == Decimal("200")


async def test_a_gateway_puts_its_limits_on_every_bot() -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway("bcs-watch", read_only=True)
        bot = await gateway.connect(bot="looker")
        await bot.reconcile([])
        assert (await bot.order_book(await bot.instrument("SBER"), depth=1)).bids
        with pytest.raises(brokapi.Rejected) as caught:
            await place(bot, "1", "240")
        refused(caught, "read_only")
        assert await bot.active_orders() == []


async def test_the_environment_sets_a_floor_the_code_cannot_lower(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKAPI_READ_ONLY", "1")
    monkeypatch.setenv("BROKAPI_MAX_ORDER_QTY", "5")
    async with await testing.mock_bcs() as mock:
        client = await mock.connect(read_only=False, max_order_qty=100)
        await client.reconcile([])
        with pytest.raises(brokapi.Rejected) as caught:
            await place(client, "1", "240")
        refused(caught, "read_only")

    monkeypatch.delenv("BROKAPI_READ_ONLY")
    async with await testing.mock_bcs() as mock:
        client = await mock.connect(max_order_qty=100)
        await client.reconcile([])
        with pytest.raises(brokapi.Rejected) as caught:
            await place(client, "6", "240")
        refused(caught, "max_order_qty")


async def test_a_limit_that_is_not_a_positive_number_is_named() -> None:
    async with await testing.mock_bcs() as mock:
        with pytest.raises(brokapi.Config, match="max_order_value"):
            await mock.connect(max_order_value="-5")
