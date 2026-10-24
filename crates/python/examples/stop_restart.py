"""A bot with a stop: what to do at every start. Find the positions that have no stop and put the missing one.

The code of the section "Бот со стопом: старт и рестарт" of the page "Стоп-заявки, Python". The tests run it against the
imitations of all four brokers. It builds on `stops.py` (the same folder): `waiting` lists the stops that wait,
`protect_once` puts one and, when the answer is lost, looks for it at the broker instead of putting it again.
"""

import asyncio
import uuid
from decimal import Decimal
from typing import Callable, Sequence

import brokapi
from stops import protect_once, waiting


# ANCHOR: start
async def protect_on_start(
    client: brokapi.Client,
    ledger: Sequence[uuid.UUID],
    stop_price_of: Callable[[brokapi.Position], Decimal | None],
) -> list[brokapi.Position]:
    """Called once, right after the subscriptions, on every start of the bot (the first one and every restart).

    `stop_price_of(position)` is the stop price the bot wants for its own position, or None for a position that is not
    the bot's (the account may hold others, bought by hand): those are left alone.

    Returns the positions that are NOT protected after it. For them the bot does not trade and tells a person."""
    await client.reconcile(ledger)  # the gate of the commands opens; stop orders are behind it too
    unprotected = []
    stops = await waiting(client)  # the stops of every earlier run, whoever put them
    for position in await client.positions():
        stop_price = stop_price_of(position)
        if position.qty <= 0 or stop_price is None:
            continue  # nothing to protect, or not ours (a short position needs a stop on the other side: not in this recipe)
        covered = sum(
            stop.qty
            for stop in stops
            if stop.instrument.key == position.instrument.key and stop.side is brokapi.Side.SELL
        )
        if covered >= position.qty:
            continue  # the stop of the earlier run is there: do not put a second one
        if covered > 0:
            unprotected.append(position)  # a stop for a part: a person decides what to do with the rest
            continue
        # No stop at all. `protect_once` puts it for the whole position. At `Unknown` it looks for the stop at the
        # broker (active_stop_orders) and never sends the same cid again.
        placed = await protect_once(client, position.instrument, stop_price, qty=position.qty)
        if placed.stop is None:
            unprotected.append(position)  # refused, or the stop is not found, or there is one without a cid to look at
    return unprotected
    # ANCHOR_END: start


async def run(client: brokapi.Client) -> None:
    unprotected = await protect_on_start(client, [], lambda position: Decimal("100"))
    if unprotected:
        print("NO STOP on:", ", ".join(str(position.instrument) for position in unprotected))
    else:
        print(f"every position has a stop: {len(await waiting(client))} waiting")


async def main() -> None:
    client = await brokapi.connect("tbank", sandbox=True)
    try:
        await run(client)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
