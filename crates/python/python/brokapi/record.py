"""The event log: write the events of a run to a file, read them back, play them again.

This is ``brokapi_runtime::record`` of the Rust library, and **the file is the same**: a header line
(``{"brokapi_log":1,"envelope_schema":1}``) and one JSON line per envelope. A log recorded here reads in Rust and the
other way round. Prices, quantities and money are written as decimal strings, so they come back exactly.

| Need | Item |
|---|---|
| Append envelopes to a file | :class:`EnvelopeWriter`, or :func:`record_events` around a stream |
| Read them back in order | :class:`EnvelopeReader` |
| Feed them to the code that reads ``client.events``, fast or at the recorded pace | :func:`replay`, :class:`ReplaySpeed` |

What the file keeps and what it does not:

- The monotonic clock of the receive time (``rx_mono`` in Rust) is not stored, and Python does not show it at all. Use
  ``Timestamps.rx_ns`` and ``broker_ns`` to order and measure replayed data.
- A line that cannot be read stops the read with a :class:`~brokapi.LogError` that names the line. A reader never
  skips a bad line silently.
- An envelope with a ``schema`` newer than this build understands is refused (``LogError`` of kind ``schema``).
- It is a log of a stream, not a catalog: there is no index by instrument or time, so filter while you read.
- Only an envelope that came from a connector or from a log can be written: it carries the line it was made from. An
  ``Envelope`` that you build by hand cannot.

>>> import asyncio, os, tempfile
>>> from brokapi import record
>>> from brokapi.testing import mock_bcs
>>> async def demo():
...     path = os.path.join(tempfile.mkdtemp(), "run.jsonl")
...     async with await mock_bcs() as mock:
...         client = await mock.connect()
...         with record.EnvelopeWriter(path) as log:
...             log.write(await client.events.get(timeout=10))
...         await client.close()
...     return len([envelope async for envelope in record.replay(path)])
>>> asyncio.run(demo())
1
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from . import _native
from .models import Envelope, Gap

__all__ = ["EnvelopeReader", "EnvelopeWriter", "LOG_FORMAT", "ReplaySpeed", "record_events", "replay"]

LOG_FORMAT = 1
"""The version of the file format. It is in the header line of every file."""

_PathLike = str | os.PathLike[str]


class EnvelopeWriter:
    """Appends envelopes to a new log file, one line each. Use it as ``with``: the file is flushed and closed at the end.

    The file is created and the header written when the writer is made. An existing file is an error
    (``LogError`` of kind ``io``) unless ``overwrite=True``: a log is a record of one run, and a second run must not
    silently replace the first. Writes are buffered: :meth:`flush` or :meth:`close` after the last one, so the tail
    is not left in a buffer when the program ends. After a failed write do not append to the same file, start a new
    one.
    """

    def __init__(self, path: _PathLike, *, overwrite: bool = False) -> None:
        self._native = _native.NativeLogWriter(os.fspath(path), overwrite)

    def write(self, envelope: Envelope) -> None:
        """Appends one envelope as one line. Raises :class:`~brokapi.LogError` when the write fails."""
        text = envelope._text
        if text is None:
            raise TypeError(
                "only an envelope that came from a connector or from a log can be written; "
                "an Envelope built by hand has no wire form"
            )
        self._native.write(text)

    @property
    def written(self) -> int:
        """How many envelopes were written (the header is not counted)."""
        return self._native.written()

    def flush(self) -> None:
        """Flushes the buffer to the file."""
        self._native.flush()

    def close(self) -> None:
        """Flushes and closes the file. Safe to call twice."""
        self._native.close()

    def __enter__(self) -> "EnvelopeWriter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class EnvelopeReader:
    """Reads the envelopes of a log back, in the order they were written: ``for envelope in EnvelopeReader(path)``.

    Opening the file reads and checks the header: a file that is not a log, an empty one, or one written by a newer
    version raises :class:`~brokapi.LogError` at once. After the first bad line the iteration raises it and ends: what
    follows a bad line cannot be trusted to be in order.
    """

    def __init__(self, path: _PathLike) -> None:
        self._native = _native.NativeLogReader(os.fspath(path))
        self._done = False

    def __iter__(self) -> Iterator[Envelope]:
        return self

    def __next__(self) -> Envelope:
        if self._done:
            raise StopIteration
        try:
            text = self._native.next_envelope()
        except Exception:
            self._done = True
            raise
        if text is None:
            self._done = True
            raise StopIteration
        return Envelope._from_text(text)


@dataclass(frozen=True, slots=True)
class ReplaySpeed:
    """How fast :func:`replay` plays. Build it with :meth:`instant` or :meth:`paced`."""

    factor: Optional[float] = None
    """``None`` is as fast as the consumer reads. Else the speed-up: ``1.0`` is real time, ``10.0`` ten times faster."""
    max_gap: timedelta = timedelta(seconds=1)
    """The longest single pause, so that a night between two sessions does not stop the replay."""

    @classmethod
    def instant(cls) -> "ReplaySpeed":
        """As fast as the consumer reads. For backtests and tests. The default."""
        return cls()

    @classmethod
    def paced(cls, factor: float = 1.0, max_gap: timedelta = timedelta(seconds=1)) -> "ReplaySpeed":
        """At the recorded pace divided by ``factor``. The pace is the difference of ``rx_ts`` of neighbouring
        envelopes, a pause is never longer than ``max_gap``."""
        if not factor > 0:
            raise ValueError("the speed-up must be above zero")
        return cls(factor, max_gap)


class _Replay:
    """The stream of a played log: ``async for envelope in replay(path)``."""

    def __init__(self, native: _native.NativeReplay) -> None:
        self._native = native

    def __aiter__(self) -> "_Replay":
        return self

    async def __anext__(self) -> Envelope:
        text = await self._native.next_envelope()
        if text is None:
            raise StopAsyncIteration
        return Envelope._from_text(text)


def replay(path: _PathLike, speed: Optional[ReplaySpeed] = None, *, buffer: int = 256) -> AsyncIterator[Envelope]:
    """Plays a log as a stream of envelopes, in order, like ``client.events``: ``async for envelope in replay(path)``.

    The envelopes keep their ``source`` and ``sub``, so the code that reads them routes them as it did live. The
    difference from a live connector is the one that matters for research: a replay **waits** for the reader, so a
    slow reader slows it down and no event is lost. A live connector cannot wait and applies the loss policy of the
    subscription instead. Code that handles ``Overflow`` therefore needs its own test: a replay never produces one.

    ``buffer`` is how many envelopes are read ahead. A bad line raises :class:`~brokapi.LogError` after the envelopes
    before it. The file is opened (and its header checked) at the call, not at the first ``await``.
    """
    speed = speed or ReplaySpeed.instant()
    return _Replay(_native.NativeReplay(os.fspath(path), speed.factor, speed.max_gap.total_seconds(), buffer))


async def record_events(
    events: AsyncIterator[Envelope], path: _PathLike, *, overwrite: bool = False
) -> AsyncIterator[Envelope]:
    """Passes the events of a stream on and writes each one to a log first:
    ``async for envelope in record_events(client.events, "run.jsonl")``.

    It records what your code reads, which is the stream as the bot saw it. Each envelope is flushed to the file as
    it is written, so a program that stops in the middle of a run (a crash, ``break``) leaves a log that is complete up
    to the last envelope it read. The file is closed when the stream ends or the generator is closed
    (``contextlib.aclosing``), not at the ``break`` itself.

    A :class:`~brokapi.Gap` (the stream of a client made by :func:`~brokapi.connect_gateway`) is passed on and not written:
    it is not an event of a connector, and the file has the format of those.
    """
    with EnvelopeWriter(path, overwrite=overwrite) as log:
        async for envelope in events:
            if not isinstance(envelope.payload, Gap):
                log.write(envelope)
                log.flush()
            yield envelope
