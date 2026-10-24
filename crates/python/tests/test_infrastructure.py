"""Several brokers and accounts in one process: shared limits, merged events, history."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

import brokapi
from brokapi import testing


async def test_a_limiter_is_shared_only_inside_one_broker() -> None:
    async with await testing.mock_bcs() as bcs, await testing.mock_tbank() as tbank:
        first = await bcs.connect()
        with pytest.raises(brokapi.Config) as caught:
            await tbank.connect(limiter=first.limiter)
        assert "bcs" in str(caught.value)
        assert first.limiter.broker == "bcs"


async def test_clients_on_one_limiter_spend_one_quota() -> None:
    async def burst(clients: list[brokapi.Client]) -> int:
        sber = await clients[0].instrument("SBER")
        calls = [c.order_book(sber, depth=1) for c in clients for _ in range(30)]
        results = await asyncio.gather(*calls, return_exceptions=True)
        assert all(isinstance(r, (brokapi.OrderBookSnapshot, brokapi.RateLimited)) for r in results)
        return sum(isinstance(r, brokapi.OrderBookSnapshot) for r in results)

    async with await testing.mock_bcs() as one, await testing.mock_bcs() as two:
        shared_a = await one.connect(name="a")
        shared_b = await two.connect(name="b", limiter=shared_a.limiter)
        alone_a = await one.connect(name="c")
        alone_b = await two.connect(name="d")
        together = await burst([shared_a, shared_b])
        apart = await burst([alone_a, alone_b])
        # The same number of calls: one counter lets fewer through than two.
        assert together + 4 < apart


async def test_rate_limited_carries_the_wait() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        results = await asyncio.gather(*[client.order_book(sber) for _ in range(60)], return_exceptions=True)
        limited = [r for r in results if isinstance(r, brokapi.RateLimited)]
        assert limited
        assert limited[0].retry_after > timedelta(0)
        assert limited[0].error_class is brokapi.ErrorClass.RETRYABLE
        assert str(limited[0]).startswith("rate limited, retry after ")


async def test_two_accounts_of_one_tbank_token() -> None:
    async with await testing.mock_tbank() as mock:
        a = await mock.connect(name="tbank-a", account_id="acc-a")
        b = await mock.connect(name="tbank-b", account_id="acc-b", limiter=a.limiter)
        sber = await a.instrument("SBER")
        params = await a.trading_params(sber)
        for client in (a, b):
            await client.reconcile([])
        cid = brokapi.new_cid()
        await a.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00"))
        assert [o.cid for o in await a.active_orders()] == [cid]
        assert await b.active_orders() == []


async def test_merge_reads_two_clients_as_one_stream() -> None:
    async with await testing.mock_bcs() as bcs_mock, await testing.mock_finam() as finam_mock:
        bcs = await bcs_mock.connect(name="bcs")
        finam = await finam_mock.connect(name="finam")
        await bcs.subscribe_order_book(await bcs.instrument("SBER"), depth=1)
        await finam.subscribe_order_book(await finam.instrument("SBER"), depth=1)

        async def sources() -> set[str]:
            seen: set[str] = set()
            async for envelope in brokapi.merge(bcs, finam):
                if isinstance(envelope.payload, brokapi.OrderBookSnapshot):
                    seen.add(envelope.source)
                if len(seen) == 2:
                    return seen
            return seen

        assert await asyncio.wait_for(sources(), 10) == {"bcs-md", "finam-md"}


async def test_merge_ends_when_the_clients_are_closed() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        await client.close()
        closed_at = time.time_ns()
        # The stream ends. An event received before the close (the first portfolio, if it was quick) may still come out of it.
        events = [e async for e in brokapi.merge(client)]
        assert all(e.ts.rx_ns <= closed_at for e in events)


async def test_a_long_history_is_read_in_windows_without_gaps_or_doubles() -> None:
    async with await testing.mock_bcs(synthetic_candles=True) as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 4, 11, tzinfo=timezone.utc)  # 100 days of hours: 2400 bars, BCS takes 1440 per request
        with pytest.raises(brokapi.Rejected):
            await client.candles(sber, brokapi.Timeframe.H1, start, end)

        pages = client.candle_pages(sber, brokapi.Timeframe.H1, start, end)
        sizes = [len(page) async for page in pages]
        assert len(sizes) >= 2
        assert pages.resume_from == end

        bars = await client.load_candles(sber, brokapi.Timeframe.H1, start, end)
        assert len(bars) == 2400
        opens = [b.open_ts for b in bars]
        assert opens == sorted(set(opens))
        assert opens[1] - opens[0] == timedelta(hours=1)
        assert isinstance(bars[0].close, Decimal)
