"""Money never becomes a float, and time always has a zone."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from brokapi import _codec, models


def test_decimal_text_is_exact_and_without_exponent() -> None:
    assert _codec.decimal_text(Decimal("244.30")) == "244.30"
    assert _codec.decimal_text(Decimal("1E+3")) == "1000"
    assert _codec.decimal_text(Decimal("0.0000001")) == "0.0000001"
    assert _codec.decimal_text(5) == "5"
    assert _codec.decimal_text("0.1") == "0.1"


def test_a_float_is_refused() -> None:
    with pytest.raises(TypeError, match="float"):
        _codec.decimal_text(0.1, "price")
    with pytest.raises(TypeError, match="float"):
        _codec.dumps({"price": 0.1})


def test_garbage_is_refused() -> None:
    with pytest.raises(ValueError):
        _codec.decimal_text("abc")
    with pytest.raises(ValueError):
        _codec.decimal_text(Decimal("NaN"))
    with pytest.raises(TypeError):
        _codec.decimal_text(True)


def test_time_round_trip_and_the_zone_is_required() -> None:
    moment = datetime(2026, 10, 21, 7, 0, 0, 123456, tzinfo=timezone.utc)
    assert _codec.from_nanos(_codec.to_nanos(moment)) == moment
    with pytest.raises(ValueError, match="time zone"):
        _codec.to_nanos(datetime(2026, 10, 21))


def test_cid_text() -> None:
    cid = uuid.uuid4()
    assert _codec.cid_text(cid) == str(cid)
    assert _codec.cid_text(str(cid).upper()) == str(cid)
    with pytest.raises(TypeError):
        _codec.cid_text(12)  # type: ignore[arg-type]


def test_decoding_a_book_keeps_decimals() -> None:
    text = (
        '{"Snapshot":{"instrument":{"key":{"symbol":"SBER","mic":"TQBR"},'
        '"native":{"TickerClass":{"ticker":"SBER","class_code":"TQBR"}}},'
        '"bids":[{"price":"244.30","qty":"10"}],"asks":[],"ts":{"broker_ts":null,"rx_ts":1700000000000000123}}}'
    )
    book = models._decode_book(_codec.loads(text))
    assert isinstance(book, models.OrderBookSnapshot)
    assert book.bids[0].price == Decimal("244.30")
    assert str(book.bids[0].price) == "244.30"
    assert isinstance(book.bids[0].price, Decimal)
    assert book.ts.rx_ns == 1_700_000_000_000_000_123
    assert book.instrument.native.kind == "TickerClass"


def test_native_id_round_trips() -> None:
    for raw in ({"Figi": "BBG004730N88"}, {"Uid": "abc"}, {"SymbolMic": {"symbol": "SBER", "mic": "MISX"}}):
        assert _codec.encode(models.NativeId._from_json(raw)) == raw


def test_backpressure_form_is_serde() -> None:
    assert _codec.encode(models.Backpressure.conflate()) == "Conflate"
    assert _codec.encode(models.Backpressure.lossless(5)) == {"Lossless": {"cap": 5}}
    assert _codec.encode(models.Backpressure.drop_newest(7)) == {"DropNewest": {"cap": 7}}


def test_an_event_of_an_unknown_kind_does_not_break_the_stream() -> None:
    event = models._decode_event({"Reconnected": {"sub": 3}})
    assert isinstance(event, models.UnknownEvent)
    assert event.kind == "Reconnected"
    assert event.data == {"sub": 3}


def test_an_error_variant_from_a_newer_core_is_still_a_brokapi_error() -> None:
    from brokapi import errors

    error = errors._from_native("SomethingNew", "something new: text of the core", {})
    assert isinstance(error, errors.BrokapiError)
    assert str(error) == "something new: text of the core"
