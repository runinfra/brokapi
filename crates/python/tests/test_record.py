"""The event log: what is recorded is what is read back and what is replayed, in the format of the Rust library."""

from __future__ import annotations

import asyncio
import contextlib
import pickle
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import brokapi
from brokapi import _native, errors, record, testing

# The header and an envelope as `EnvelopeWriter` of the Rust library writes them (see its documentation).
HEADER = '{"brokapi_log":1,"envelope_schema":1}'


def line(n: int, source: str = "bcs-md") -> str:
    return f'{{"schema":1,"source":"{source}","sub":null,"ts":{{"broker_ts":null,"rx_ts":{n}}},"payload":{{"Balances":[]}}}}'


def write_log(path: Path, *lines: str, header: str = HEADER) -> Path:
    # newline="\n": the format is LF on every system, as the Rust library writes it (text mode would write CRLF on Windows).
    path.write_text("\n".join([header, *lines]) + "\n", newline="\n")
    return path


async def one_run(mock: testing.MockBroker, client: brokapi.Client, path: Path, count: int) -> list[brokapi.Envelope]:
    """A bot that subscribes, places an order that is filled, and records the first `count` events it reads."""
    sber = await client.instrument("SBER")
    qty = (await client.trading_params(sber)).min_qty
    await client.subscribe_order_state()
    await client.subscribe_executions()
    await client.reconcile()
    cid = brokapi.new_cid()
    await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=qty, price=Decimal("250.25"))
    mock.fill(cid, qty=qty, price=Decimal("250.25"))
    seen: list[brokapi.Envelope] = []
    async with contextlib.aclosing(record.record_events(client.events, path)) as stream:
        async for envelope in stream:
            seen.append(envelope)
            if len(seen) == count:
                break
    return seen


async def test_what_is_recorded_is_what_is_read_back_and_what_is_replayed(
    bcs: tuple[testing.MockBroker, brokapi.Client], tmp_path: Path
) -> None:
    mock, client = bcs
    path = tmp_path / "run.jsonl"
    seen = await one_run(mock, client, path, 4)
    assert len(seen) == 4
    kinds = {type(e.payload) for e in seen}
    assert brokapi.OrderSnapshot in kinds  # the log holds the events of the account, not only a first one

    assert list(record.EnvelopeReader(path)) == seen
    assert [e async for e in record.replay(path)] == seen


@pytest.mark.parametrize("name", ["bcs", "tbank", "finam"])
async def test_the_same_sequence_comes_back_for_every_broker(name: str, tmp_path: Path) -> None:
    mock = await {"bcs": testing.mock_bcs, "tbank": testing.mock_tbank, "finam": testing.mock_finam}[name]()
    async with mock:
        client = await mock.connect()
        seen = await one_run(mock, client, tmp_path / "run.jsonl", 3)
    assert [e async for e in record.replay(tmp_path / "run.jsonl")] == seen


async def test_money_comes_back_exactly(bcs: tuple[testing.MockBroker, brokapi.Client], tmp_path: Path) -> None:
    mock, client = bcs
    path = tmp_path / "run.jsonl"
    await one_run(mock, client, path, 4)
    text = path.read_text()
    assert '"250.25"' in text  # a decimal string in the file, never a float
    prices = [
        e.payload.price
        for e in record.EnvelopeReader(path)
        if isinstance(e.payload, brokapi.OrderSnapshot) and e.payload.price is not None
    ]
    assert prices and all(p == Decimal("250.25") for p in prices)


def test_the_file_is_the_format_of_the_rust_library(tmp_path: Path) -> None:
    """A log of Rust reads here, and what is written here is the same text that Rust writes."""
    from_rust = write_log(tmp_path / "rust.jsonl", line(1), line(2, "bcs-ud"))
    envelopes = list(record.EnvelopeReader(from_rust))
    assert [(e.source, e.ts.rx_ns) for e in envelopes] == [("bcs-md", 1), ("bcs-ud", 2)]
    assert record.LOG_FORMAT == 1

    again = tmp_path / "python.jsonl"
    with record.EnvelopeWriter(again) as log:
        for envelope in envelopes:
            log.write(envelope)
        assert log.written == 2
    assert again.read_bytes() == from_rust.read_bytes()


def test_a_log_of_a_new_version_is_refused(tmp_path: Path) -> None:
    newer_format = write_log(tmp_path / "a.jsonl", header='{"brokapi_log":2,"envelope_schema":1}')
    with pytest.raises(brokapi.LogError) as error:
        record.EnvelopeReader(newer_format)
    assert (error.value.kind, error.value.found, error.value.supported) == ("schema", 2, 1)
    assert str(error.value) == "log has format or schema 2, this build reads up to 1"

    newer_envelope = write_log(tmp_path / "b.jsonl", line(1).replace('"schema":1', '"schema":9'))
    with pytest.raises(brokapi.LogError) as error:
        list(record.EnvelopeReader(newer_envelope))
    assert error.value.kind == "schema" and error.value.found == 9


def test_a_bad_line_stops_the_read_and_names_the_line(tmp_path: Path) -> None:
    path = write_log(tmp_path / "bad.jsonl", line(1), line(2), "{not json", line(4))
    reader = record.EnvelopeReader(path)
    assert next(reader).ts.rx_ns == 1 and next(reader).ts.rx_ns == 2
    with pytest.raises(brokapi.LogError) as error:
        next(reader)
    assert (error.value.kind, error.value.line) == ("format", 4)  # the header is line 1
    assert str(error.value).startswith("log line 4: ")
    with pytest.raises(StopIteration):  # what follows a bad line cannot be trusted: the reader ends
        next(reader)


def test_a_file_that_is_not_a_log_is_refused_at_once(tmp_path: Path) -> None:
    (tmp_path / "empty").write_text("")
    with pytest.raises(brokapi.LogError, match="empty") as error:
        record.EnvelopeReader(tmp_path / "empty")
    assert (error.value.kind, error.value.line) == ("format", 1)
    (tmp_path / "text").write_text("not a log\n")
    with pytest.raises(brokapi.LogError) as error:
        record.EnvelopeReader(tmp_path / "text")
    assert error.value.line == 1
    with pytest.raises(brokapi.LogError) as error:
        record.EnvelopeReader(tmp_path / "missing")
    assert error.value.kind == "io" and str(error.value).startswith("log I/O error: ")
    with pytest.raises(brokapi.LogError):
        record.replay(tmp_path / "missing")  # the replay opens the file when it is made, not at the first await


def test_a_second_run_does_not_replace_the_first_log_unless_asked(tmp_path: Path) -> None:
    path = write_log(tmp_path / "run.jsonl", line(1))
    with pytest.raises(brokapi.LogError) as error:
        record.EnvelopeWriter(path)
    assert error.value.kind == "io"
    assert path.read_text().count("\n") == 2  # untouched
    with record.EnvelopeWriter(path, overwrite=True) as log:
        assert log.written == 0
    assert path.read_text() == HEADER + "\n"


def test_only_what_came_from_a_connector_or_a_log_can_be_written(tmp_path: Path) -> None:
    by_hand = brokapi.Envelope(
        source="x", sub=None, ts=brokapi.Timestamps(1), payload=brokapi.BalancesUpdate([])
    )
    with record.EnvelopeWriter(tmp_path / "a.jsonl") as log:
        with pytest.raises(TypeError, match="came from a connector"):
            log.write(by_hand)
        assert log.written == 0


def test_a_closed_writer_says_so(tmp_path: Path) -> None:
    log = record.EnvelopeWriter(tmp_path / "a.jsonl")
    log.close()
    log.close()  # twice is fine
    with pytest.raises(ValueError, match="closed"):
        log.flush()


async def test_a_replay_waits_for_a_slow_reader_and_loses_nothing(tmp_path: Path) -> None:
    """A live connector drops or overflows when the reader is slow. A replay never does."""
    path = write_log(tmp_path / "big.jsonl", *[line(n) for n in range(1, 2001)])
    got: list[int] = []
    async for envelope in record.replay(path, buffer=2):
        got.append(envelope.ts.rx_ns)
        if len(got) % 500 == 0:
            await asyncio.sleep(0)  # a reader that lets the others run
    assert got == list(range(1, 2001))


async def test_a_replay_gives_the_envelopes_before_a_bad_line_and_then_the_error(tmp_path: Path) -> None:
    path = write_log(tmp_path / "bad.jsonl", line(1), line(2), "oops", line(4))
    got: list[int] = []
    with pytest.raises(brokapi.LogError) as error:
        async for envelope in record.replay(path):
            got.append(envelope.ts.rx_ns)
    assert got == [1, 2]
    assert (error.value.kind, error.value.line) == ("format", 4)


async def test_a_paced_replay_keeps_the_order(tmp_path: Path) -> None:
    # Ten seconds apart in the log; ten million times faster, and no pause is longer than a microsecond.
    path = write_log(tmp_path / "paced.jsonl", *[line(n * 10_000_000_000) for n in range(1, 6)])
    speed = record.ReplaySpeed.paced(1e7, max_gap=timedelta(microseconds=1))
    assert [e.ts.rx_ns async for e in record.replay(path, speed)] == [n * 10_000_000_000 for n in range(1, 6)]
    assert record.ReplaySpeed.instant().factor is None
    with pytest.raises(ValueError):
        record.ReplaySpeed.paced(0)


async def test_stopping_the_reader_of_a_replay_early_is_fine(tmp_path: Path) -> None:
    path = write_log(tmp_path / "big.jsonl", *[line(n) for n in range(1, 1001)])
    stream = record.replay(path, buffer=1)
    first = await anext(stream)
    assert first.ts.rx_ns == 1
    del stream  # nobody reads the rest: the replay ends with its reader


async def test_recording_leaves_a_complete_log_even_when_the_loop_is_left(tmp_path: Path) -> None:
    path = write_log(tmp_path / "src.jsonl", *[line(n) for n in range(1, 11)])
    out = tmp_path / "copy.jsonl"
    seen = []
    async for envelope in record.record_events(record.replay(path), out):
        seen.append(envelope)
        if len(seen) == 3:
            break
    # The generator is not closed yet, but each envelope was flushed as it passed.
    assert [e.ts.rx_ns for e in record.EnvelopeReader(out)] == [1, 2, 3]


# ---------------------------------------------------------------------------------------------------- errors


@pytest.mark.parametrize(("kind", "rust_text"), _native.log_error_samples())
def test_the_text_of_a_log_error_is_the_text_in_rust(kind: str, rust_text: str) -> None:
    fields = {"line": 3, "found": 2, "supported": 1}
    error = errors._log_error(kind, rust_text, fields)
    assert isinstance(error, errors.LogError) and isinstance(error, errors.BrokapiError)
    assert str(error) == rust_text
    assert error.kind == kind
    again = pickle.loads(pickle.dumps(error))
    assert (str(again), again.kind, again.line) == (rust_text, kind, 3)
