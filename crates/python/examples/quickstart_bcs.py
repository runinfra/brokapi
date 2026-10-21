"""Quick start on BCS: read first, then one order far from the market, cancelled at once.

This program is the code of the page "Быстрый старт" of the Python section of the book. The book includes the marked
parts below, so the text and the code that is tested cannot drift apart.

Run it with a real account (BCS has no sandbox, the account is real)::

    BCS_REFRESH_TOKEN=... BCS_IS_WRITE=true python quickstart_bcs.py

The tests run `run()` against the imitation of BCS (`brokapi.testing`): no network, no account, no money.
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import brokapi

# The price of the test order is this share of the best bid. Far enough that it does not fill in a normal session,
# near enough that the exchange does not refuse it as being outside the allowed corridor.
FAR_FROM_MARKET = Decimal("0.9")


async def wait_for_order(client: brokapi.Client, cid, kind: brokapi.OrderStateKind) -> None:
    """Reads the events until the order `cid` reaches `kind`, and prints every state it goes through.

    An event is a notice. The answer of `order_state` is the proof, so a missing event is not an error here."""

    async def read() -> None:
        async for envelope in client.events:
            order = envelope.payload
            if isinstance(order, brokapi.OrderSnapshot) and order.cid == cid:
                print(f"event: order {cid} is {order.state.kind.value}")
                if order.state.kind is kind:
                    return

    try:
        await asyncio.wait_for(read(), 5)
    except asyncio.TimeoutError:
        print(f"no event {kind.value} within 5 s, the state is checked with order_state below")


async def run(client: brokapi.Client, *, can_trade: bool) -> None:
    """The whole quick start on a connected client."""
    # ANCHOR: read
    sber = await client.instrument("SBER")  # the ticker on the board TQBR

    # 1. Portfolio: money and positions of the account.
    portfolio = await client.portfolio()
    print(f"portfolio: {len(portfolio.balances)} balance(s), {len(portfolio.positions)} position(s)")
    for balance in portfolio.balances:
        print(f"  money {balance.currency}: {balance.available} free, {balance.blocked} blocked")

    # 2. Order book: best bid and best ask.
    book = await client.order_book(sber, depth=5)
    best_bid = book.bids[0].price if book.bids else None
    best_ask = book.asks[0].price if book.asks else None
    print(f"order book SBER: best bid {best_bid}, best ask {best_ask}")

    # 3. Candles: the last hour of five-minute bars.
    now = datetime.now(timezone.utc)
    bars = await client.candles(sber, brokapi.Timeframe.M5, now - timedelta(hours=1), now)
    print(f"candles M5: {len(bars)} bar(s), last close {bars[-1].close if bars else None}")
    # ANCHOR_END: read

    if not can_trade:
        print("the token is read-only (BCS_IS_WRITE is not true): the order step is skipped")
        return

    # ANCHOR: warning
    print("WARNING: BCS has no sandbox. The account is real, the order below goes to the exchange.")
    # ANCHOR_END: warning

    # ANCHOR: order
    # The price: far below the best bid, a multiple of the price step. The quantity: the minimum.
    params = await client.trading_params(sber)
    reference = best_bid or (bars[-1].close if bars else None)
    if reference is None:
        raise SystemExit("no price to start from: the book and the candles are empty")
    # The rounding is exact: the price is a whole number of steps, no 0.9999999 noise.
    price = brokapi.round_to_step(reference * FAR_FROM_MARKET, params.price_step, brokapi.RoundMode.DOWN)
    print(f"order: buy {params.min_qty} at {price} (best bid {reference}, price step {params.price_step})")

    # The gate is closed until the first reconcile. The ledger of this new bot is empty. Subscribe first, so no
    # event between the snapshot of the reconcile and the subscription is lost.
    await client.subscribe_order_state()
    report = await client.reconcile([])
    print(f"reconcile: {len(report.foreign)} foreign order(s) on the account")

    # The cid is made by the bot and belongs in its journal before the order is sent.
    cid = brokapi.new_cid()
    # Here the bot writes `cid` to its journal as "sending".
    print(f"cid {cid}")
    ack = await client.place(
        cid=cid,
        instrument=sber,
        side=brokapi.Side.BUY,
        qty=params.min_qty,
        price=price,
    )
    print(f"accepted: broker order id {ack.broker_id}")
    await wait_for_order(client, cid, brokapi.OrderStateKind.ACKED)

    # Cancel at once and look at the final state.
    await client.cancel(cid)
    await wait_for_order(client, cid, brokapi.OrderStateKind.CANCELLED)
    last = await client.order_state(cid)
    print(f"order_state: {last.state.kind.value}")
    # ANCHOR_END: order


async def main() -> None:
    # ANCHOR: connect
    # BCS_REFRESH_TOKEN and BCS_IS_WRITE come from the environment (a .env file works)
    client = await brokapi.connect("bcs")
    print("connected: bcs-md and bcs-ud started")
    # ANCHOR_END: connect
    try:
        await run(client, can_trade=os.environ.get("BCS_IS_WRITE") == "true")
    finally:
        # ANCHOR: stop
        await client.close()
        print("stopped")
        # ANCHOR_END: stop


if __name__ == "__main__":
    asyncio.run(main())
