"""The first run on Bybit: read, reconcile, one minimal order far from the market, the event, the cancel, a stop order.

The code of the section "Первый запуск" of the page "Bybit". The tests run it against the imitation of Bybit
(`brokapi.testing.mock_bybit`, the instrument `MOCKUSDT`). On the demo account of Bybit it is the same code with
`brokapi.connect("bybit", env="demo")`. On the main account it is run by a person, or by an agent only after the word of
the owner of the account (rule 0).
"""

import asyncio
import uuid

import brokapi
from brokapi.testing import mock_bybit


# ANCHOR: first_day
async def order_event(client: brokapi.Client, cid: uuid.UUID) -> brokapi.OrderSnapshot:
    """The first event of the stream that is a state of the order with this cid."""
    async for event in client.events:
        if isinstance(event.payload, brokapi.OrderSnapshot) and event.payload.cid == cid:
            return event.payload
    raise RuntimeError("the stream ended")


async def first_day(client: brokapi.Client, symbol: str) -> None:
    """Reads the contract, puts the smallest order at half of the best bid, sees it in the stream, takes it off, and
    puts a stop order that is also taken off. Nothing here can fill: both prices are far from the market."""
    # 1. The contract and its rules: the quantity step, the smallest order and the price step. They come from the exchange.
    perp = await client.instrument(symbol)  # "BTCUSDT" becomes "BTCUSDT@linear": the category is part of the id
    params = await client.trading_params(perp)
    book = await client.order_book(perp, depth=1)
    bid = book.bids[0].price
    print(f"{perp}: bid {bid}, ask {book.asks[0].price}, smallest order {params.min_qty}, quantity step {params.lot_size}")

    # 2. Subscribe to the orders, and only then reconcile: no event is lost between the two.
    await client.subscribe_order_state()
    report = await client.reconcile([])  # a new bot has an empty ledger
    print(f"reconcile: {len(report.foreign)} foreign order(s) on the account")

    # 3. The order: half of the best bid, rounded down to the price step. The exchange refuses a price too far from the
    # market with a code of its own, then try a nearer one with a NEW cid.
    price = brokapi.round_to_step(bid / 2, params.price_step, brokapi.RoundMode.DOWN)
    cid = brokapi.new_cid()  # a real bot writes the cid to its journal here, before `place`
    ack = await client.place(cid=cid, instrument=perp, side=brokapi.Side.BUY, qty=params.min_qty, price=price)
    print(f"placed: order {ack.broker_id} at {price}")
    seen = await asyncio.wait_for(order_event(client, cid), timeout=10)  # waits for the event, not for a pause
    print(f"in the stream: {seen.state.kind.value}")

    # 4. Take it off and ask the exchange what became of it. The answer of `order_state` is the proof.
    await client.cancel(cid)
    state = await client.order_state(cid)
    print(f"after cancel: {state.state.kind.value}, orders alive: {len(await client.active_orders())}")

    # 5. A stop order is a conditional order of Bybit. It waits apart from the ordinary orders: `active_stop_orders`.
    stop_cid = brokapi.new_cid()
    await client.place_stop(
        cid=stop_cid,
        instrument=perp,
        side=brokapi.Side.SELL,
        kind=brokapi.StopOrderKind.STOP_LOSS,
        qty=params.min_qty,
        stop_price=brokapi.round_to_step(bid / 2, params.price_step, brokapi.RoundMode.DOWN),
    )
    print(f"stop orders waiting: {len(await client.active_stop_orders())}, ordinary orders: {len(await client.active_orders())}")
    await client.cancel_stop(stop_cid)
    print(f"stop orders after cancel: {len(await client.active_stop_orders())}")
    # ANCHOR_END: first_day


# ANCHOR: mock
async def on_the_imitation() -> None:
    async with await mock_bybit() as mock:
        client = await mock.connect()
        await first_day(client, "MOCKUSDT")
        await client.close()
        # ANCHOR_END: mock


# ANCHOR: demo
async def on_the_demo_account() -> None:
    # BYBIT_DEMO_API_KEY and BYBIT_DEMO_API_SECRET from the environment: a key of the demo account is not valid on the
    # main host, so the contour is chosen by the name, not by the address.
    client = await brokapi.connect("bybit", env="demo")
    try:
        await first_day(client, "BTCUSDT")
    finally:
        await client.close()
        # ANCHOR_END: demo


if __name__ == "__main__":
    asyncio.run(on_the_imitation())
