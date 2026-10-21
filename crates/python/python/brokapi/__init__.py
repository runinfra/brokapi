"""brokapi: one interface to the APIs of Russian brokers (BCS, T-Bank, Finam) for Python.

A thin layer over the Rust core of brokapi. The rules of the core are kept: trading opens after ``reconcile``, the
``cid`` of an order is made by the bot, a command whose outcome is :class:`Unknown` is never repeated, and one rate
limit counter serves one broker area. Money, prices and quantities are :class:`decimal.Decimal`, never ``float``.

Read first, then trade. For example, against the imitations of the brokers (no network, no money):

    import asyncio
    from decimal import Decimal
    import brokapi
    from brokapi.testing import mock_bcs

    async def main():
        async with await mock_bcs() as mock:
            client = await mock.connect()
            sber = await client.instrument("SBER")
            book = await client.order_book(sber, depth=5)
            print(book.bids[0].price)
            await client.close()

    asyncio.run(main())

With a real account, leave out the mock: ``client = await brokapi.connect("bcs")`` reads ``BCS_REFRESH_TOKEN`` from the
environment. The book at https://brokapi.ru/docs/ has the quick start, the how-to pages and the explanations.
"""

from __future__ import annotations

from . import errors, models, numbers, testing
from .client import CandlePager, Client, EventStream, Limiter, connect, merge, new_cid
from .errors import (
    BrokapiError,
    Config,
    Degraded,
    ErrorClass,
    NotReconciled,
    RateLimited,
    Rejected,
    ShuttingDown,
    Transport,
    Unknown,
    Unsupported,
)
from .models import (
    Backpressure,
    Balance,
    BalancesUpdate,
    BookLevel,
    Candle,
    Caps,
    Envelope,
    Event,
    Execution,
    Instrument,
    InstrumentId,
    InstrumentKey,
    InstrumentKind,
    InstrumentQuery,
    MarketDataCaps,
    MarketTrade,
    NativeId,
    OrderAck,
    OrderBook,
    OrderBookIncrement,
    OrderBookSnapshot,
    OrderSnapshot,
    OrderState,
    OrderStateKind,
    OrderType,
    Overflow,
    PlaceIdempotency,
    Portfolio,
    Position,
    PositionsUpdate,
    Quote,
    ReconcileReport,
    Reconnected,
    Resync,
    ResyncReason,
    Schedule,
    SessionInterval,
    SessionKind,
    ShutdownReport,
    Side,
    StreamingCaps,
    Timeframe,
    Timestamps,
    TradingAvailability,
    TradingParams,
    TradingStatus,
    UnknownEvent,
    UserDataCaps,
)
from .numbers import RoundMode, round_to_step

__version__ = "0.1.0a1"

__all__ = [
    "Backpressure",
    "Balance",
    "BalancesUpdate",
    "BookLevel",
    "BrokapiError",
    "CandlePager",
    "Candle",
    "Caps",
    "Client",
    "Config",
    "Degraded",
    "Envelope",
    "ErrorClass",
    "Event",
    "EventStream",
    "Execution",
    "Instrument",
    "InstrumentId",
    "InstrumentKey",
    "InstrumentKind",
    "InstrumentQuery",
    "Limiter",
    "MarketDataCaps",
    "MarketTrade",
    "NativeId",
    "NotReconciled",
    "OrderAck",
    "OrderBook",
    "OrderBookIncrement",
    "OrderBookSnapshot",
    "OrderSnapshot",
    "OrderState",
    "OrderStateKind",
    "OrderType",
    "Overflow",
    "PlaceIdempotency",
    "Portfolio",
    "Position",
    "PositionsUpdate",
    "Quote",
    "RateLimited",
    "ReconcileReport",
    "Reconnected",
    "Rejected",
    "Resync",
    "ResyncReason",
    "RoundMode",
    "Schedule",
    "SessionInterval",
    "SessionKind",
    "ShutdownReport",
    "ShuttingDown",
    "Side",
    "StreamingCaps",
    "Timeframe",
    "Timestamps",
    "TradingAvailability",
    "TradingParams",
    "TradingStatus",
    "Transport",
    "Unknown",
    "UnknownEvent",
    "Unsupported",
    "UserDataCaps",
    "connect",
    "errors",
    "merge",
    "models",
    "new_cid",
    "numbers",
    "round_to_step",
    "testing",
]
