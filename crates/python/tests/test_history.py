"""A long history read window by window: the window a caller sets, and every bar of the period on every broker."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import brokapi
from brokapi import testing

END = datetime(2026, 10, 1, tzinfo=timezone.utc)
TICKER = {"bcs": "SBER", "tbank": "SBER", "finam": "SBER", "alor": "SBER", "bybit": "BTCUSDT"}


async def test_a_window_of_the_caller_cuts_the_history_into_pages_of_that_span() -> None:
    async with await testing.mock_alor() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        pages = [page async for page in client.candle_pages(
            sber, brokapi.Timeframe.H1, END - timedelta(days=10), END, window=timedelta(hours=50)
        )]
        assert [len(page) for page in pages] == [50, 50, 50, 50, 40]
        assert sum(len(page) for page in pages) == 240


async def test_a_window_that_is_not_a_positive_span_is_refused_before_anything_is_read() -> None:
    async with await testing.mock_alor() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        for bad in (timedelta(0), timedelta(milliseconds=500), timedelta(hours=-1)):
            with pytest.raises(ValueError, match="window"):
                client.candle_pages(sber, brokapi.Timeframe.H1, END - timedelta(days=1), END, window=bad)


@pytest.mark.parametrize("broker", sorted(TICKER))
async def test_the_history_of_a_period_has_every_bar_once_on_every_broker(broker: str) -> None:
    # The book promises the whole period without holes or doubles (python/howto-history.md).
    async with await getattr(testing, f"mock_{broker}")(synthetic_candles=True) as mock:
        client = await mock.connect()
        instrument = await client.instrument(TICKER[broker])
        start = END - timedelta(days=100)
        bars = await client.load_candles(instrument, brokapi.Timeframe.H1, start, END)
        expected = [start + timedelta(hours=h) for h in range(2400)]
        assert [bar.open_ts for bar in bars] == expected
