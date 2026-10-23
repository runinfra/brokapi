"""Fixtures: a connected client for each broker, against the in-process imitations. No network, no money.

`bcs`, `tbank`, `finam`, `alor` connect to the broker directly. `gw_bcs`, ... give the same pair through a gateway that runs in
the test process on the same imitation: a scenario that takes `pair` runs on both roads.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest_asyncio

import brokapi
from brokapi import testing


@pytest_asyncio.fixture
async def bcs() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async with await testing.mock_bcs() as mock:
        yield mock, await mock.connect()


@pytest_asyncio.fixture
async def alor() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async with await testing.mock_alor() as mock:
        yield mock, await mock.connect()


@pytest_asyncio.fixture
async def tbank() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async with await testing.mock_tbank() as mock:
        yield mock, await mock.connect()


@pytest_asyncio.fixture
async def finam() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async with await testing.mock_finam() as mock:
        yield mock, await mock.connect()


async def _through_a_gateway(
    start: Callable[[], Awaitable[testing.MockBroker]],
) -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    """The imitation with a gateway on it, and a client of that gateway: the same pair as above, reached the other way."""
    async with await start() as mock:
        gateway = await mock.start_gateway()
        yield mock, await gateway.connect(bot="test-bot")


@pytest_asyncio.fixture
async def gw_bcs() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async for pair in _through_a_gateway(testing.mock_bcs):
        yield pair


@pytest_asyncio.fixture
async def gw_alor() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async for pair in _through_a_gateway(testing.mock_alor):
        yield pair


@pytest_asyncio.fixture
async def gw_tbank() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async for pair in _through_a_gateway(testing.mock_tbank):
        yield pair


@pytest_asyncio.fixture
async def gw_finam() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async for pair in _through_a_gateway(testing.mock_finam):
        yield pair


async def next_event(client: brokapi.Client, kind: type, *, timeout: float = 10.0, **match: Any) -> brokapi.Envelope:
    """Reads the stream until an event of type `kind` whose fields equal `match`. Waits for the event, not for time."""

    async def read() -> brokapi.Envelope:
        async for envelope in client.events:
            payload = envelope.payload
            if isinstance(payload, kind) and all(getattr(payload, k) == v for k, v in match.items()):
                return envelope
        raise AssertionError("the stream ended")

    return await asyncio.wait_for(read(), timeout)
