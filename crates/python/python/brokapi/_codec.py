"""JSON <-> Python values for the native layer. Private.

The native layer speaks JSON text: the serde form of the types of ``brokapi-core``. Prices, quantities and money are
decimal strings there, and they become :class:`decimal.Decimal` here, so no number is ever a ``float``.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import types
import typing
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def from_nanos(ns: int) -> datetime:
    """Unix nanoseconds -> an aware UTC datetime (microsecond precision)."""
    return _EPOCH + timedelta(microseconds=ns // 1_000)


def to_nanos(moment: datetime | int) -> int:
    """An aware datetime (or ready nanoseconds) -> Unix nanoseconds. A naive datetime is refused: which zone?"""
    if isinstance(moment, int) and not isinstance(moment, bool):
        return moment
    if not isinstance(moment, datetime):
        raise TypeError(f"expected a datetime, got {type(moment).__name__}")
    if moment.tzinfo is None:
        raise ValueError("the datetime has no time zone: use datetime(..., tzinfo=timezone.utc)")
    delta = moment - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def decimal_text(value: Any, what: str = "number") -> str:
    """A ``Decimal``, ``int`` or ``str`` as the text the native layer reads. A ``float`` is refused on purpose."""
    if isinstance(value, bool):
        raise TypeError(f"{what}: expected a Decimal, not bool")
    if isinstance(value, float):
        raise TypeError(f"{what}: a float cannot be exact, pass a Decimal or a str (Decimal('0.1'))")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(f"{what}: {value} is not a finite number")
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        try:
            Decimal(value)
        except ArithmeticError:
            raise ValueError(f"{what}: {value!r} is not a number") from None
        return value.strip()
    raise TypeError(f"{what}: expected a Decimal, got {type(value).__name__}")


def cid_text(cid: uuid.UUID | str) -> str:
    """A client order id as text."""
    if isinstance(cid, uuid.UUID):
        return str(cid)
    if isinstance(cid, str):
        return str(uuid.UUID(cid))
    raise TypeError(f"a cid is a uuid.UUID, got {type(cid).__name__}")


def encode(value: Any) -> Any:
    """Python value -> something ``json.dumps`` writes: the serde form of the Rust type."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        raise TypeError("a float cannot be exact, pass a Decimal or a str")
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return to_nanos(value)
    if isinstance(value, enum.Enum):
        return value.value
    to_json = getattr(value, "_to_json", None)
    if to_json is not None:
        return to_json()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: encode(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    if isinstance(value, dict):
        return {k: encode(v) for k, v in value.items()}
    raise TypeError(f"cannot send {type(value).__name__} to the broker")


def dumps(args: dict[str, Any]) -> str:
    """The arguments of a native call as JSON text."""
    return json.dumps({k: encode(v) for k, v in args.items()}, separators=(",", ":"))


def loads(text: str) -> Any:
    """JSON text from the native layer. Numbers stay ``int``; money is text, so nothing becomes a float."""

    def no_float(literal: str) -> Decimal:
        return Decimal(literal)

    return json.loads(text, parse_float=no_float)


_HINTS: dict[type, dict[str, Any]] = {}


def _hints(cls: type) -> dict[str, Any]:
    cached = _HINTS.get(cls)
    if cached is None:
        import brokapi.models as module  # the namespace in which the annotations are resolved

        cached = typing.get_type_hints(cls, vars(module))
        _HINTS[cls] = cached
    return cached


def decode(tp: Any, value: Any) -> Any:
    """Builds a value of type ``tp`` from the JSON form."""
    if value is None:
        return None
    origin = typing.get_origin(tp)
    if origin is typing.Union or origin is types.UnionType:
        options = [a for a in typing.get_args(tp) if a is not type(None)]
        return decode(options[0], value)
    if origin is list:
        (item,) = typing.get_args(tp)
        return [decode(item, v) for v in value]
    if tp is Decimal:
        return Decimal(value) if isinstance(value, (str, int)) else value
    if tp is datetime:
        return from_nanos(value)
    if tp is uuid.UUID:
        return uuid.UUID(value)
    if tp in (int, str, bool):
        return value
    if isinstance(tp, type) and issubclass(tp, enum.Enum):
        return tp(value)
    from_json = getattr(tp, "_from_json", None)
    if from_json is not None:
        return from_json(value)
    if isinstance(tp, type) and dataclasses.is_dataclass(tp):
        hints = _hints(tp)
        kwargs = {}
        for f in dataclasses.fields(tp):
            if f.name in value:
                kwargs[f.name] = decode(hints[f.name], value[f.name])
        return tp(**kwargs)
    raise TypeError(f"cannot read {tp!r}")
