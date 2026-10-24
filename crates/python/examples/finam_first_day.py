"""The first day at Finam, which has no sandbox: one minimal order far from the market, a restart, a check that the
comment of the order came back, the cancel.

The code of the section "Первый день на Финаме" of the page "Финам". The tests run it against the imitation of Finam.
On the real account it is run by a person, or by an agent only after the word of the owner of the account (rule 0).
"""

import asyncio
from decimal import Decimal
from typing import Awaitable, Callable

import brokapi


# ANCHOR: first_day
async def first_day(connect: Callable[[], Awaitable[brokapi.Client]]) -> bool:
    """Places one lot far below the market, 'restarts' the bot and checks that the broker gave the order back as ours.

    `connect` makes a client of the account. Returns True when the comment of the order came back and the order was
    cancelled. Anything else is a reason to stop: do not trade on this account until it is clear."""
    first = await connect()
    sber = await first.instrument("SBER", "MISX")
    params = await first.trading_params(sber)
    book = await first.order_book(sber, depth=1)
    price = brokapi.round_to_step(book.bids[0].price * Decimal("0.9"), params.price_step, brokapi.RoundMode.DOWN)

    await first.reconcile([])
    cid = brokapi.new_cid()  # in a real bot it is in the journal before the next line
    await first.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.lot_size, price=price)
    await first.close()  # the process "dies": the order stays at the exchange

    # The second run knows only the journal: the cid. If Finam gave the comment back untouched, the order is recognised.
    second = await connect()
    try:
        report = await second.reconcile([cid])
        recognised = [order.cid for order in report.confirmed] == [cid]
        print(f"the order {'came back as ours' if recognised else 'was NOT recognised'}: {len(report.foreign)} foreign")
        for order in report.confirmed:
            await second.cancel(order.cid)
        print(f"orders left after the cancel: {len(await second.active_orders())}")
        return recognised and not report.foreign
    finally:
        await second.close()
        # ANCHOR_END: first_day


async def main() -> None:
    ok = await first_day(lambda: brokapi.connect("finam"))  # FINAM_SECRET and FINAM_ACCOUNT_ID from the environment
    print("the comment of the order came back" if ok else "STOP: the comment did not come back")


if __name__ == "__main__":
    asyncio.run(main())
