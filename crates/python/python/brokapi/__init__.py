"""brokapi: one interface to the APIs of Russian brokers (BCS, T-Bank, Finam, ALOR) for Python.

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
environment. Through a gateway (the program ``brokapi``, which holds the connection for several bots) it is the same
client: ``client = await brokapi.connect_gateway("bcs-main", bot="my-bot")``. The book at https://brokapi.ru/docs/ has the
quick start, the how-to pages and the explanations.
"""

from __future__ import annotations

from importlib import metadata as _metadata

from . import errors, models, numbers, record, testing
from .client import (
    CandlePager,
    Client,
    EventStream,
    Limiter,
    connect,
    connect_gateway,
    merge,
    new_cid,
    open_tbank_sandbox_account,
)
from .errors import (
    BrokapiError,
    Config,
    Degraded,
    ErrorClass,
    LogError,
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
    Gap,
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
    StopExpiry,
    StopOrderKind,
    StopOrderRef,
    StopOrderSnapshot,
    StopOrderState,
    StopOrderStateKind,
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

try:
    # The one number of the library is `version` in [workspace.package] of Cargo.toml; maturin writes it into the metadata of the package.
    __version__ = _metadata.version("brokapi")
except _metadata.PackageNotFoundError:  # run from a source tree that was never installed (the reference is built like this)
    __version__ = "0+unknown"

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
    "Gap",
    "Instrument",
    "InstrumentId",
    "InstrumentKey",
    "InstrumentKind",
    "InstrumentQuery",
    "Limiter",
    "LogError",
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
    "StopExpiry",
    "StopOrderKind",
    "StopOrderRef",
    "StopOrderSnapshot",
    "StopOrderState",
    "StopOrderStateKind",
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
    "open_tbank_sandbox_account",
    "connect_gateway",
    "errors",
    "merge",
    "models",
    "new_cid",
    "numbers",
    "record",
    "round_to_step",
    "testing",
]
