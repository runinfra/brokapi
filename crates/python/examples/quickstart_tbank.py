"""Quick start: the first run without a token and without money, then the sandbox of T-Bank.

This program is the code of the page "Быстрый старт" of the Python section of the book. The book includes the marked
parts below, so the text and the code that is tested cannot drift apart.

Without a token (the imitation of T-Bank in your own process; no account, no network)::

    python quickstart_tbank.py

On the sandbox of T-Bank (a token of the sandbox and an account of the sandbox are needed)::

    TBANK_TOKEN=... python quickstart_tbank.py sandbox

The imitation (`brokapi.testing.mock_tbank`) is a server on 127.0.0.1 that speaks the protocol of T-Bank. The client is
the same `brokapi.Client` that `brokapi.connect("tbank", ...)` returns, so `first_order` below is the code of a real
run: it works with a client of any broker.
"""

import asyncio
import sys
from decimal import Decimal

import brokapi
from brokapi.testing import mock_tbank


# ANCHOR: first_order
async def first_order(client: brokapi.Client) -> None:
    """Reads the market, puts one order far below it and takes it off at once."""
    # 1. What to trade and by what rules: the step of the price and the lot.
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)

    # 2. The quote: the best bid and the best ask are the top of the order book.
    book = await client.order_book(sber, depth=1)
    best_bid = book.bids[0].price
    print(f"SBER: bid {best_bid}, ask {book.asks[0].price}, lot {params.lot_size}, price step {params.price_step}")

    # 3. Subscribe to the states of your orders, and only then reconcile: no event is lost between the two.
    await client.subscribe_order_state()
    report = await client.reconcile([])  # a new bot has an empty ledger. Until now `place` is NotReconciled
    print(f"reconcile: {len(report.foreign)} foreign order(s) on the account")

    # 4. An order far from the market: 90% of the best bid, rounded down to the price step, one lot.
    price = brokapi.round_to_step(best_bid * Decimal("0.9"), params.price_step, brokapi.RoundMode.DOWN)
    cid = brokapi.new_cid()  # a real bot writes the cid to its journal here, before `place`
    ack = await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.lot_size, price=price)
    print(f"placed: broker order id {ack.broker_id}, price {price}")

    # 5. Take it off and ask the broker what became of it. The answer of `order_state` is the proof.
    await client.cancel(cid)
    state = await client.order_state(cid)
    print(f"after cancel: {state.state.kind.value}, orders alive: {len(await client.active_orders())}")
    # ANCHOR_END: first_order


# ANCHOR: mock
async def on_the_imitation() -> None:
    # The block starts the imitation and stops it at the end. `mock.connect()` is `brokapi.connect(...)` to it.
    async with await mock_tbank() as mock:
        client = await mock.connect()
        await first_order(client)

        # The price of the imitation stands still: it has no market behind it.
        sber = await client.instrument("SBER")
        book = await client.order_book(sber, depth=1)
        print(f"the price stands still: bid {book.bids[0].price}")

        # A test moves it: the book is built around the new price, a step of 0.01 on each side.
        mock.set_price("SBER", "250")
        book = await client.order_book(sber, depth=1)
        print(f"after set_price: bid {book.bids[0].price}, ask {book.asks[0].price}")
        # ANCHOR_END: mock


# ANCHOR: sandbox
async def on_the_sandbox() -> None:
    # TBANK_TOKEN is the token of the sandbox. sandbox=True is the address of the sandbox (the real one is the default).
    client = await brokapi.connect("tbank", sandbox=True)
    try:
        await first_order(client)
    finally:
        await client.close()
        # ANCHOR_END: sandbox


if __name__ == "__main__":
    asyncio.run(on_the_sandbox() if sys.argv[1:] == ["sandbox"] else on_the_imitation())
