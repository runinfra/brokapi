"""The imitations themselves: what a test of a bot can make them do, on every broker.

A test of a bot believes what the imitation answers, so the imitation must keep its own books straight: a fill moves the
position and is a trade of the history.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

import brokapi
from brokapi import testing

Pair = tuple[testing.MockBroker, brokapi.Client]

# A ticker each imitation knows and holds none of at the start.
TICKER = {"bcs": "GAZP", "tbank": "GAZP", "finam": "GAZP", "alor": "GAZP", "bybit": "MOCKUSDT"}


@pytest.fixture(params=list(TICKER))
def direct(request: pytest.FixtureRequest) -> Pair:
    return request.getfixturevalue(request.param)


async def held(client: brokapi.Client, instrument: brokapi.InstrumentId) -> Decimal:
    return sum((p.qty for p in await client.positions() if str(p.instrument) == str(instrument)), Decimal(0))


async def filled_order(
    mock: testing.MockBroker, client: brokapi.Client, side: brokapi.Side, qty: Decimal, price: Decimal
) -> brokapi.InstrumentId:
    instrument = await client.instrument(TICKER[mock.broker])
    cid = brokapi.new_cid()
    await client.place(cid=cid, instrument=instrument, side=side, qty=qty, price=price)
    mock.fill(cid, qty=qty, price=price)
    return instrument


async def test_a_fill_moves_the_position(direct: Pair) -> None:
    mock, client = direct
    instrument = await client.instrument(TICKER[mock.broker])
    lot = (await client.trading_params(instrument)).min_qty
    await client.reconcile([])
    before = await held(client, instrument)

    await filled_order(mock, client, brokapi.Side.BUY, lot * 2, Decimal("101.50"))
    assert await held(client, instrument) == before + lot * 2

    await filled_order(mock, client, brokapi.Side.SELL, lot, Decimal("102.00"))
    assert await held(client, instrument) == before + lot


async def test_each_fill_is_a_trade_of_the_history(direct: Pair) -> None:
    mock, client = direct
    instrument = await client.instrument(TICKER[mock.broker])
    lot = (await client.trading_params(instrument)).min_qty
    await client.reconcile([])
    start = datetime.now(timezone.utc) - timedelta(minutes=5)

    cid = brokapi.new_cid()
    await client.place(cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=lot * 3, price=Decimal("101.50"))
    if mock.broker == "tbank":
        # T-Bank fills the whole order at its price.
        mock.fill(cid)
        parts = [lot * 3]
    else:
        mock.fill(cid, qty=lot, price=Decimal("101.50"))
        state = (await client.order_state(cid)).state
        assert state.kind is brokapi.OrderStateKind.PARTIALLY_FILLED
        assert (state.filled, state.left) == (lot, lot * 2)
        mock.fill(cid, qty=lot * 2, price=Decimal("101.60"))
        parts = [lot, lot * 2]
    snapshot = await client.order_state(cid)
    assert snapshot.state.kind is brokapi.OrderStateKind.FILLED
    assert snapshot.filled == lot * 3

    history = await client.executions(start, datetime.now(timezone.utc) + timedelta(minutes=5))
    mine = [e for e in history if str(e.instrument) == str(instrument)]
    assert sorted(e.qty for e in mine) == sorted(parts)
    assert len({e.trade_id for e in mine}) == len(parts)


async def test_a_fill_comes_to_the_portfolio_stream(direct: Pair) -> None:
    mock, client = direct
    instrument = await client.instrument(TICKER[mock.broker])
    lot = (await client.trading_params(instrument)).min_qty
    await client.reconcile([])
    await client.subscribe_portfolio()

    await filled_order(mock, client, brokapi.Side.BUY, lot, Decimal("101.50"))

    async def moved() -> None:
        async for envelope in client.events:
            # A broker sends the whole portfolio (`Portfolio`) or the positions that changed (`PositionsUpdate`).
            update = envelope.payload
            if isinstance(update, (brokapi.Portfolio, brokapi.PositionsUpdate)) and any(
                str(p.instrument) == str(instrument) and p.qty == lot for p in update.positions
            ):
                return

    await asyncio.wait_for(moved(), 10)


async def test_a_lost_answer_of_a_place_is_unknown_and_the_order_is_at_the_broker(direct: Pair) -> None:
    mock, client = direct
    instrument = await client.instrument(TICKER[mock.broker])
    lot = (await client.trading_params(instrument)).min_qty
    await client.reconcile([])

    mock.lose_order_answers()
    cid = brokapi.new_cid()
    with pytest.raises(brokapi.Unknown):
        await client.place(cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=lot, price=Decimal("100.00"))
    mock.lose_order_answers(False)

    # The broker has it, and the cid is used up: a second send of it is refused before it leaves.
    assert [o.cid for o in await client.active_orders()] == [cid]
    with pytest.raises(brokapi.Rejected) as caught:
        await client.place(cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=lot, price=Decimal("100.00"))
    assert caught.value.code == "duplicate_place"


@pytest.mark.parametrize("broker", list(TICKER))
async def test_an_answer_later_than_the_request_timeout_is_unknown(broker: str) -> None:
    async with await getattr(testing, f"mock_{broker}")() as mock:
        client = await mock.connect(request_timeout=0.2)
        instrument = await client.instrument(TICKER[broker])
        lot = (await client.trading_params(instrument)).min_qty
        await client.subscribe_order_state()
        await client.reconcile([])

        mock.delay_orders(1.0)
        cid = brokapi.new_cid()
        with pytest.raises(brokapi.Unknown):
            await client.place(cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=lot, price=Decimal("100.00"))
        mock.delay_orders(0)

        # The broker takes the order all the same: its event comes when the delay is over.
        async def acked() -> None:
            async for envelope in client.events:
                if isinstance(envelope.payload, brokapi.OrderSnapshot) and envelope.payload.cid == cid:
                    return

        await asyncio.wait_for(acked(), 10)


async def test_a_cut_of_the_streams_is_a_reconnect_and_trading_goes_on_after_a_reconcile(direct: Pair) -> None:
    mock, client = direct
    instrument = await client.instrument(TICKER[mock.broker])
    lot = (await client.trading_params(instrument)).min_qty
    await client.subscribe_order_state()
    await client.reconcile([])

    mock.cut_streams()

    async def reconnected() -> None:
        async for envelope in client.events:
            if isinstance(envelope.payload, brokapi.Reconnected):
                return

    await asyncio.wait_for(reconnected(), 10)
    with pytest.raises(brokapi.Degraded):
        await client.place(
            cid=brokapi.new_cid(), instrument=instrument, side=brokapi.Side.BUY, qty=lot, price=Decimal("100.00")
        )
    await client.reconcile([])
    cid = brokapi.new_cid()
    await client.place(cid=cid, instrument=instrument, side=brokapi.Side.BUY, qty=lot, price=Decimal("100.00"))
    assert [o.cid for o in await client.active_orders()] == [cid]


async def test_the_reads_of_the_market_follow_the_price_a_test_sets(direct: Pair) -> None:
    mock, client = direct
    instrument = await client.instrument(TICKER[mock.broker])
    mock.set_price(TICKER[mock.broker], "123.45")

    book = await client.order_book(instrument, depth=3)
    # The best bid a step of 0.01 below the price, the best ask a step above.
    assert [level.price for level in book.bids] == [Decimal("123.44"), Decimal("123.43"), Decimal("123.42")]
    assert [level.price for level in book.asks] == [Decimal("123.46"), Decimal("123.47"), Decimal("123.48")]
    deals = await client.last_trades(instrument, limit=5)
    assert deals and all(deal.price == Decimal("123.45") for deal in deals)
    now = datetime.now(timezone.utc)
    bars = await client.candles(instrument, brokapi.Timeframe.M5, now - timedelta(hours=1), now)
    assert bars and all(bar.close == Decimal("123.45") for bar in bars)


async def test_the_streams_of_the_market_follow_the_price_a_test_sets(direct: Pair) -> None:
    mock, client = direct
    ticker = TICKER[mock.broker]
    instrument = await client.instrument(ticker)
    await client.subscribe_order_book(instrument, depth=5)
    await client.subscribe_quotes(instrument)
    await client.subscribe_trades(instrument, backpressure=brokapi.Backpressure.lossless(1000))
    await client.subscribe_candles(instrument, brokapi.Timeframe.M1)

    async def seen(price: Decimal) -> None:
        """Every kind of market event at `price`: the book and the quote around it, a deal and a bar at it."""
        below = price - Decimal("0.01")
        wanted = {"book", "quote", "deal", "bar"}
        async for envelope in client.events:
            event = envelope.payload
            if isinstance(event, brokapi.OrderBookSnapshot) and event.bids and event.bids[0].price == below:
                wanted.discard("book")
            elif isinstance(event, brokapi.Quote) and event.bid is not None and event.bid.price == below:
                wanted.discard("quote")
            elif isinstance(event, brokapi.MarketTrade) and event.price == price:
                wanted.discard("deal")
            elif isinstance(event, brokapi.Candle) and event.close == price:
                wanted.discard("bar")
            if not wanted:
                return

    for price in (Decimal("110.00"), Decimal("111.50")):
        mock.set_price(ticker, price)
        await asyncio.wait_for(seen(price), 10)


SMALL_BUDGET = """
[bcs.buckets.all]
rate = 1
per = "60s"
burst = 1
[bcs.route]
"*" = ["all"]
[bcs.conns]
max_conns = 4
max_subs_per_conn = 50
supports_multiplex = false
"""


async def test_a_limiter_of_your_own_numbers_refuses_before_the_broker_does() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect(limiter=brokapi.Limiter.from_toml("bcs", SMALL_BUDGET))
        sber = await client.instrument("SBER")
        await client.order_book(sber, depth=1)  # the one request of the budget
        with pytest.raises(brokapi.RateLimited) as caught:
            await client.order_book(sber, depth=1)
        assert caught.value.retry_after > timedelta(seconds=1)


def test_a_limits_file_without_the_section_of_the_broker_is_named() -> None:
    with pytest.raises(brokapi.Config, match=r"no \[tbank\] section"):
        brokapi.Limiter.from_toml("tbank", SMALL_BUDGET)
