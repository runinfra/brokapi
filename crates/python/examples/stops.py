"""Stop orders: put a stop-loss that protects a position, read it back, find it again after a lost answer, cancel it.

The code of the page "Стоп-заявки, Python" of the book. The tests run it against the imitations of all three brokers.
"""

import asyncio
import uuid
from decimal import Decimal

import brokapi


# ANCHOR: protect
async def protect(
    client: brokapi.Client, instrument: brokapi.InstrumentId, stop_price: Decimal
) -> tuple[uuid.UUID, brokapi.OrderAck]:
    """Puts a stop-loss that sells one lot if the price falls to `stop_price`, at market."""
    params = await client.trading_params(instrument)  # the unit of the quantity is the instrument, not the lot
    cid = brokapi.new_cid()  # made by the bot, and written to its journal BEFORE the call, as for any order
    ack = await client.place_stop(
        cid=cid,
        instrument=instrument,
        side=brokapi.Side.SELL,
        kind=brokapi.StopOrderKind.STOP_LOSS,
        qty=params.lot_size,
        stop_price=stop_price,
        # limit_price=None: a market order at the trigger. expiry=None: until cancelled.
    )
    # Keep ack.broker_id too: after a restart T-Bank cannot find a stop by its cid, only by the id of the broker.
    return cid, ack
    # ANCHOR_END: protect


# ANCHOR: waiting
async def waiting(client: brokapi.Client) -> list[brokapi.StopOrderSnapshot]:
    """The stops that have not triggered yet. This list is the only way to learn that one has: there is no event."""
    return [stop for stop in await client.active_stop_orders() if stop.state.kind is brokapi.StopOrderStateKind.ACTIVE]
    # ANCHOR_END: waiting


# ANCHOR: unknown
async def protect_once(
    client: brokapi.Client, instrument: brokapi.InstrumentId, stop_price: Decimal
) -> brokapi.StopOrderSnapshot | None:
    """Puts the stop; when the answer is lost, looks for it at the broker instead of putting it again."""
    cid = brokapi.new_cid()
    try:
        await client.place_stop(
            cid=cid,
            instrument=instrument,
            side=brokapi.Side.SELL,
            kind=brokapi.StopOrderKind.STOP_LOSS,
            qty=(await client.trading_params(instrument)).lot_size,
            stop_price=stop_price,
        )
    except brokapi.Rejected:
        return None  # the broker said no: there is no stop
    except (brokapi.Unknown, brokapi.Transport):
        pass  # it may be there. NEVER place again under this cid: look.
    return next((stop for stop in await client.active_stop_orders() if stop.cid == cid), None)
    # ANCHOR_END: unknown


# ANCHOR: cancel
async def release(client: brokapi.Client, stop: brokapi.StopOrderSnapshot) -> None:
    """Takes the stop off, by the id of the broker: that works after a restart for every broker."""
    await client.cancel_stop(brokapi.StopOrderRef.broker(stop.broker_id))
    # ANCHOR_END: cancel


async def run(client: brokapi.Client) -> None:
    sber = await client.instrument("SBER")
    await client.reconcile()  # stop orders are behind the same gate as plain orders
    cid, ack = await protect(client, sber, Decimal("100"))
    (stop,) = await waiting(client)
    print(f"stop {stop.kind.value} {stop.state.kind.value} at {stop.stop_price}, cid {cid}, broker id {ack.broker_id}")
    await release(client, stop)
    print(f"cancelled, {len(await waiting(client))} waiting")


async def main() -> None:
    client = await brokapi.connect("bcs")
    try:
        await run(client)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
