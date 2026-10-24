"""Emergency stop of a bot: take off its own orders, leave the stops and the foreign orders where they are.

The code of the page "Аварийная остановка бота" of the book. The tests run it against the imitations of all four brokers.
"""

import asyncio
import uuid
from typing import Iterable

import brokapi


# ANCHOR: cancel_own
async def cancel_own_orders(client: brokapi.Client, ledger: Iterable[uuid.UUID]) -> list[uuid.UUID]:
    """Cancels the live orders of the bot (the cids of its journal). Stop orders and foreign orders stay.

    Returns the cids whose outcome is not clear: look at them with `order_state`, do not cancel them again blindly."""
    report = await client.reconcile(list(ledger))  # the gate of the commands is closed in a new process, `cancel` too
    unclear = []
    for order in report.confirmed:  # the orders of the journal that are alive at the broker; `foreign` is not touched
        try:
            await client.cancel(order.cid)
        except brokapi.Rejected:
            pass  # not found: it was filled or cancelled a moment ago. `order_state(cid)` tells which
        except brokapi.RateLimited as limited:
            await asyncio.sleep(limited.retry_after.total_seconds())  # nothing was sent: wait and try this one again
            try:
                await client.cancel(order.cid)
            except brokapi.BrokapiError:
                unclear.append(order.cid)
        except (brokapi.Unknown, brokapi.Transport):
            unclear.append(order.cid)
    return unclear
    # ANCHOR_END: cancel_own


async def run(client: brokapi.Client, ledger: Iterable[uuid.UUID]) -> None:
    unclear = await cancel_own_orders(client, ledger)
    stops = await client.active_stop_orders()
    print(f"unclear: {len(unclear)}, orders left: {len(await client.active_orders())}, stops still standing: {len(stops)}")


async def main() -> None:
    client = await brokapi.connect("tbank", sandbox=True)
    try:
        await run(client, [])  # here: the cids from the journal of the bot
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
