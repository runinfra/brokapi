"""The limits on the orders: a client that may look at the account and not trade, and a cap on one order.

Runs on the imitation of BCS, without a broker, a network or money: ``python guard.py``. ``tests/test_guides.py`` runs it.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import brokapi
from brokapi.testing import mock_bcs


# ANCHOR: guard
async def look_but_do_not_trade() -> list[str]:
    said = []
    async with await mock_bcs() as mock:
        # On a real account: brokapi.connect("bcs", read_only=True), or BROKAPI_READ_ONLY=1 in the environment.
        client = await mock.connect(read_only=True)
        sber = await client.instrument("SBER")
        book = await client.order_book(sber, depth=1)  # reads work
        said.append(f"bid {book.bids[0].price}")
        await client.reconcile([])
        try:
            await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=1, price=Decimal("240"))
        except brokapi.Rejected as refused:  # checked before anything is sent: no quota spent, the cid is still good
            said.append(f"refused: {refused.code}")

        capped = await mock.connect(name="capped", max_order_value="10000")
        await capped.reconcile([])
        try:
            await capped.place(
                cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=100, price=Decimal("240")
            )
        except brokapi.Rejected as refused:  # 100 x 240 = 24000, above the cap
            said.append(f"refused: {refused.code}")
    return said
    # ANCHOR_END: guard


if __name__ == "__main__":
    for line in asyncio.run(look_but_do_not_trade()):
        print(line)
