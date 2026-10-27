"""Levers of the imitations, each in a test of a bot in miniature: a series of prices and a lost answer.

Runs without a broker, a network or money: ``python levers.py``. ``tests/test_guides.py`` runs both.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import brokapi
from brokapi.testing import mock_bcs


# ANCHOR: price
async def the_bot_sees_the_price_come_to_its_level() -> list[Decimal]:
    async with await mock_bcs() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        await client.subscribe_quotes(sber)
        bids = []
        for price in (Decimal("251"), Decimal("250"), Decimal("249")):  # the series a test needs
            mock.set_price("SBER", price)
            # Wait for the quote of this price, not for time: the best bid is a step of 0.01 below it.
            async for envelope in client.events:
                quote = envelope.payload
                if isinstance(quote, brokapi.Quote) and quote.bid and quote.bid.price == price - Decimal("0.01"):
                    bids.append(quote.bid.price)
                    break
        return bids
        # ANCHOR_END: price


# ANCHOR: lost
async def a_lost_answer_is_asked_about_not_sent_again() -> brokapi.OrderStateKind:
    async with await mock_bcs() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        await client.reconcile([])
        mock.lose_order_answers()  # the broker takes the order, the answer is lost on the way
        cid = brokapi.new_cid()
        try:
            await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=Decimal(1), price=Decimal("240"))
        except brokapi.Unknown:
            mock.lose_order_answers(False)
        # The bot asks the broker what became of the order. It never sends the same cid again.
        return (await client.order_state(cid)).state.kind
        # ANCHOR_END: lost


async def main() -> None:
    print("bids:", ", ".join(str(b) for b in await the_bot_sees_the_price_come_to_its_level()))
    print("after a lost answer the order is", (await a_lost_answer_is_asked_about_not_sent_again()).value)


if __name__ == "__main__":
    asyncio.run(main())
