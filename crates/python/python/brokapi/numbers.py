"""Exact arithmetic on prices and quantities.

A price is a whole number of price steps. These helpers round to the step with :class:`decimal.Decimal`, so
``0.1 + 0.2`` style noise never appears. They are the same rules as ``round_to_step`` of the Rust library.

>>> from decimal import Decimal
>>> from brokapi.numbers import RoundMode, round_to_step
>>> round_to_step(Decimal("244.37"), Decimal("0.05"), RoundMode.DOWN)
Decimal('244.35')
>>> round_to_step(Decimal("244.37"), Decimal("0.05"), RoundMode.UP)
Decimal('244.40')
"""

from __future__ import annotations

import enum
from decimal import Decimal, localcontext

__all__ = ["RoundMode", "is_on_step", "round_to_step"]


class RoundMode(enum.Enum):
    """Where to go when the value is between two steps."""

    DOWN = "Down"
    """To the lower step (a buy price that must not exceed the plan)."""
    UP = "Up"
    """To the higher step."""
    NEAREST = "Nearest"
    """To the nearer step. In the middle, away from zero for positive values."""


def _check_step(step: Decimal) -> None:
    if not step > 0:
        raise ValueError("the step must be above zero")


def is_on_step(value: Decimal, step: Decimal) -> bool:
    """Is ``value`` a whole number of ``step``? A step of zero or below means "no grid" and answers ``True``."""
    if not step > 0:
        return True
    with localcontext() as ctx:
        ctx.prec = 60
        return value % step == 0


def round_to_step(value: Decimal, step: Decimal, mode: RoundMode = RoundMode.DOWN) -> Decimal:
    """Rounds ``value`` to a multiple of ``step``. Exact: a value that already is a multiple is returned as it is."""
    _check_step(step)
    with localcontext() as ctx:
        ctx.prec = 60
        rem = value % step
        if rem == 0:
            return value
        floor = value - rem if rem > 0 else value - rem - step
        ceil = floor + step
        if mode is RoundMode.DOWN:
            return floor
        if mode is RoundMode.UP:
            return ceil
        down, up = value - floor, ceil - value
        return floor if (down < up or (down == up and value < 0)) else ceil
