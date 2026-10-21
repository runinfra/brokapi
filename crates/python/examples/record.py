"""Record the events of a run to a file and play them back: one analysis for the live stream and for the file.

The code of the page "Запись событий и проигрыш, Python" of the book. The tests run it against the imitations.
"""

import asyncio
import contextlib
from collections import Counter
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import brokapi
from brokapi import record


# ANCHOR: stats
@dataclass
class Stats:
    kinds: Counter[str] = field(default_factory=Counter)
    traded: Decimal = Decimal(0)  # the sum of price * qty of your fills


async def analyze(events: AsyncIterator[brokapi.Envelope], stop_after: int | None = None) -> Stats:
    """Reads events and counts them. It does not know whether they are live or from a file."""
    stats = Stats()
    async for envelope in events:
        payload = envelope.payload
        stats.kinds[type(payload).__name__] += 1
        if isinstance(payload, brokapi.Execution):
            stats.traded += payload.price * payload.qty
        if stop_after is not None and sum(stats.kinds.values()) >= stop_after:
            break
    return stats
    # ANCHOR_END: stats


# ANCHOR: record
async def record_run(client: brokapi.Client, path: Path, count: int) -> Stats:
    """The live stream, written to `path` as it is read, and analysed."""
    async with contextlib.aclosing(record.record_events(client.events, path)) as events:
        return await analyze(events, stop_after=count)
    # ANCHOR_END: record


# ANCHOR: replay
async def analyze_file(path: Path) -> Stats:
    """The same analysis over the file. A replay waits for the reader, so nothing is lost."""
    return await analyze(record.replay(path))
    # ANCHOR_END: replay


# ANCHOR: read
def sources(path: Path) -> set[str]:
    """Reading a log without asyncio: an iterator of Envelope. A bad line raises brokapi.LogError and ends it."""
    return {envelope.source for envelope in record.EnvelopeReader(path)}
    # ANCHOR_END: read


async def main() -> None:
    client = await brokapi.connect("bcs")
    try:
        await client.subscribe_order_state()
        await client.subscribe_executions()
        print(await record_run(client, Path("run.jsonl"), count=100))
    finally:
        await client.close()
    print(await analyze_file(Path("run.jsonl")))


if __name__ == "__main__":
    asyncio.run(main())
