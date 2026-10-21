"""Fixtures: a connected client for each broker, against the in-process imitations. No network, no money."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest_asyncio

import brokapi
from brokapi import testing


@pytest_asyncio.fixture
async def bcs() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async with await testing.mock_bcs() as mock:
        yield mock, await mock.connect()


@pytest_asyncio.fixture
async def tbank() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async with await testing.mock_tbank() as mock:
        yield mock, await mock.connect()


@pytest_asyncio.fixture
async def finam() -> AsyncIterator[tuple[testing.MockBroker, brokapi.Client]]:
    async with await testing.mock_finam() as mock:
        yield mock, await mock.connect()


async def next_event(client: brokapi.Client, kind: type, *, timeout: float = 10.0, **match: Any) -> brokapi.Envelope:
    """Reads the stream until an event of type `kind` whose fields equal `match`. Waits for the event, not for time."""

    async def read() -> brokapi.Envelope:
        async for envelope in client.events:
            payload = envelope.payload
            if isinstance(payload, kind) and all(getattr(payload, k) == v for k, v in match.items()):
                return envelope
        raise AssertionError("the stream ended")

    return await asyncio.wait_for(read(), timeout)
