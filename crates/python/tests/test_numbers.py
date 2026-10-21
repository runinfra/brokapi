from __future__ import annotations

from decimal import Decimal

import pytest

from brokapi import RoundMode, round_to_step
from brokapi.numbers import is_on_step


@pytest.mark.parametrize(
    ("value", "step", "mode", "expected"),
    [
        ("244.37", "0.05", RoundMode.DOWN, "244.35"),
        ("244.37", "0.05", RoundMode.UP, "244.40"),
        ("244.37", "0.05", RoundMode.NEAREST, "244.35"),
        ("244.38", "0.05", RoundMode.NEAREST, "244.40"),
        ("244.35", "0.05", RoundMode.DOWN, "244.35"),
        ("-244.37", "0.05", RoundMode.DOWN, "-244.40"),
        ("-244.37", "0.05", RoundMode.UP, "-244.35"),
        ("0.9", "0.01", RoundMode.DOWN, "0.9"),
        ("272.5", "0.05", RoundMode.NEAREST, "272.5"),
        ("0.025", "0.05", RoundMode.NEAREST, "0.05"),
        ("-0.025", "0.05", RoundMode.NEAREST, "-0.05"),
    ],
)
def test_round_to_step(value: str, step: str, mode: RoundMode, expected: str) -> None:
    assert round_to_step(Decimal(value), Decimal(step), mode) == Decimal(expected)


def test_a_sum_of_tenths_is_a_multiple_of_the_step() -> None:
    total = sum((Decimal("0.1") for _ in range(30)), Decimal(0))
    assert is_on_step(total, Decimal("0.1"))


def test_a_step_of_zero_is_refused_for_rounding_and_means_no_grid_for_the_check() -> None:
    with pytest.raises(ValueError):
        round_to_step(Decimal(1), Decimal(0))
    assert is_on_step(Decimal("1.234"), Decimal(0))
