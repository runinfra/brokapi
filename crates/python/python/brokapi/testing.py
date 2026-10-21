"""Imitations of the brokers, to test a bot without a broker, a network or money.

Each ``mock_*`` function starts a real server on ``127.0.0.1`` that speaks the wire protocol of one broker, and
``mock.connect()`` gives a :class:`~brokapi.Client` connected to it, the same class as for the real broker. Orders
placed on it can be listed, cancelled and filled (``mock.fill``).

What the imitations do **not** prove: that the live broker answers the same. They are written from the documentation
of the brokers, and every example in the book runs against them. The readiness page of the book says what was
checked on the live brokers.

>>> import asyncio
>>> from brokapi.testing import mock_bcs
>>> async def demo():
...     async with await mock_bcs() as mock:
...         client = await mock.connect()
...         sber = await client.instrument("SBER")
...         book = await client.order_book(sber, depth=5)
...         await client.close()
...         return book.bids[0].price > 0
>>> asyncio.run(demo())
True
"""

from __future__ import annotations

import json
import uuid
from decimal import Decimal
from typing import Any, Optional

from . import _codec, _native
from .client import Client, connect

__all__ = ["MockBroker", "mock_bcs", "mock_finam", "mock_tbank"]


class MockBroker:
    """A running imitation of one broker. Use it as ``async with``: it stops when the block ends."""

    def __init__(self, native: Any) -> None:
        self._native = native
        self._clients: list[Client] = []

    @property
    def broker(self) -> str:
        """``bcs``, ``tbank`` or ``finam``."""
        return self._native.broker

    @property
    def settings(self) -> dict[str, Any]:
        """The settings that connect a client to it (``endpoint`` and the credentials it accepts)."""
        return json.loads(self._native.settings())

    async def __aenter__(self) -> "MockBroker":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def connect(self, **overrides: Any) -> Client:
        """A client connected to the imitation. ``overrides`` are the arguments of :func:`brokapi.connect`
        (``name``, ``limiter``, ``account_id`` ...). The client is closed when the imitation is."""
        args = {**self.settings, **overrides}
        client = await connect(self.broker, **args)
        self._clients.append(client)
        return client

    def fill(
        self,
        cid: uuid.UUID,
        *,
        qty: Decimal | str | int | None = None,
        price: Decimal | str | int | None = None,
        account: Optional[str] = None,
    ) -> None:
        """Executes the order ``cid``: it becomes filled, and the fill event goes to the account.

        BCS and Finam need ``qty`` and ``price`` (``qty`` less than the order makes a partial fill on BCS). T-Bank
        fills the whole order at its price and ignores both. ``account`` picks the account of T-Bank
        (``acc-a`` or ``acc-b``) or Finam (``ACC1`` or ``ACC2``).
        """
        self._native.fill(
            _codec.cid_text(cid),
            None if qty is None else _codec.decimal_text(qty, "qty"),
            None if price is None else _codec.decimal_text(price, "price"),
            account,
        )

    async def close(self) -> None:
        """Closes the clients that were connected through it, then stops the server."""
        for client in self._clients:
            await client.close(None)
        self._clients.clear()
        self._native.close()


async def _start(broker: str, synthetic_candles: bool) -> MockBroker:
    if not _native.TESTING:
        raise RuntimeError("this build of brokapi has no imitations (built without the 'testing' feature)")
    return MockBroker(await _native.start_mock(broker, synthetic_candles))


async def mock_bcs(*, synthetic_candles: bool = False) -> MockBroker:
    """An imitation of BCS (HTTP and WebSocket). Its token can trade.

    ``synthetic_candles=True`` makes ``candles`` answer a series for the period asked, one bar per step of the
    timeframe, and refuse a period longer than 1440 bars as BCS does. Off, it answers three fixed bars.
    """
    return await _start("bcs", synthetic_candles)


async def mock_tbank(*, synthetic_candles: bool = False) -> MockBroker:
    """An imitation of T-Bank (gRPC). Two accounts: ``acc-a`` (the default) and ``acc-b``.

    ``synthetic_candles=True``: a series for the period asked, refused when longer than T-Invest allows.
    """
    return await _start("tbank", synthetic_candles)


async def mock_finam(*, synthetic_candles: bool = False) -> MockBroker:
    """An imitation of Finam (gRPC). Two accounts: ``ACC1`` (the default) and ``ACC2``.

    ``synthetic_candles=True``: a series for the period asked, one bar per step of the timeframe.
    """
    return await _start("finam", synthetic_candles)
