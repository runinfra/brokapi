"""The errors of Python say what the errors of Rust say."""

from __future__ import annotations

import pickle
from datetime import timedelta

import pytest

from brokapi import _native, errors


def build(label: str) -> errors.BrokapiError:
    name, _, arg = label.partition(":")
    match name:
        case "Config":
            return errors.Config("token is empty")
        case "Transport":
            return errors.Transport("connection reset")
        case "RateLimited":
            seconds = {"1s": 1.0, "250ms": 0.25, "1.5s": 1.5, "2m": 120.0, "15us": 0.000015}[arg]
            return errors.RateLimited(timedelta(seconds=seconds))
        case "Rejected":
            return errors.Rejected("price_off_step", "price 10.005 is not a multiple of the price step 0.01")
        case "Unsupported":
            return errors.Unsupported("stop_order_stream")
        case _:
            return getattr(errors, name)()


@pytest.mark.parametrize(("label", "rust_text"), _native.error_samples())
def test_text_equals_the_text_in_rust(label: str, rust_text: str) -> None:
    assert str(build(label)) == rust_text


@pytest.mark.parametrize(("label", "rust_text"), _native.error_samples())
def test_native_errors_are_built_with_the_rust_text(label: str, rust_text: str) -> None:
    fields: dict[str, object] = {}
    variant = label.partition(":")[0]
    if variant == "RateLimited":
        fields = {"retry_after_ns": 1_000_000_000}
    error = errors._from_native(variant, rust_text, fields)
    assert str(error) == rust_text
    assert isinstance(error, getattr(errors, variant))


def test_the_class_of_each_error_matches_error_class_in_rust() -> None:
    retryable = {errors.Transport, errors.RateLimited}
    for cls in (errors.Config, errors.Rejected, errors.Unsupported, errors.NotReconciled, errors.Degraded, errors.ShuttingDown):
        assert cls.error_class is errors.ErrorClass.FATAL
    for cls in retryable:
        assert cls.error_class is errors.ErrorClass.RETRYABLE
    assert errors.Unknown.error_class is errors.ErrorClass.UNKNOWN


def test_fields_and_pickle() -> None:
    error = errors.Rejected("not_found", "no such order")
    assert (error.code, error.message) == ("not_found", "no such order")
    again = pickle.loads(pickle.dumps(error))
    assert isinstance(again, errors.Rejected)
    assert str(again) == str(error)
    assert again.code == "not_found"
    limited = pickle.loads(pickle.dumps(errors.RateLimited(2)))
    assert limited.retry_after == timedelta(seconds=2)


def test_all_errors_share_a_base() -> None:
    for cls in (errors.Config, errors.Transport, errors.Unknown, errors.Degraded):
        assert issubclass(cls, errors.BrokapiError)
        assert issubclass(cls, Exception)
