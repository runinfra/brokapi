"""Errors of brokapi.

The names and the meaning are those of ``Error`` in the Rust library (``brokapi_core::Error``): one class per
variant, and ``str(error)`` is the text that Rust prints for the same error. The errors page of the book says what
each one means and what to do. ``error_class`` answers the one question that matters most: may the call be tried
again (``RETRYABLE``), will it fail again until something changes (``FATAL``), or is the outcome of a command not
known (``UNKNOWN``).

The one that needs care is :class:`Unknown`. A command (``place``, ``cancel``, ``replace``) was sent and nothing
says whether the broker did it. **Never send it again.** Ask the broker with ``order_state(cid)`` first.

>>> from brokapi import errors
>>> str(errors.NotReconciled())
'not reconciled: call reconcile() before trading commands'
>>> errors.Unknown().error_class is errors.ErrorClass.UNKNOWN
True
"""

from __future__ import annotations

import enum
from datetime import timedelta
from typing import Any, ClassVar, Optional

__all__ = [
    "BrokapiError",
    "Config",
    "Degraded",
    "ErrorClass",
    "LogError",
    "NotReconciled",
    "RateLimited",
    "Rejected",
    "ShuttingDown",
    "Transport",
    "Unknown",
    "Unsupported",
]


class ErrorClass(enum.Enum):
    """The coarse answer to "may I try this call again" (``Error::class`` in Rust)."""

    RETRYABLE = "Retryable"
    """The call failed for a reason that can pass: the network, a rate limit."""
    FATAL = "Fatal"
    """The same call fails again until something else changes: a setup, a refused request, a closed gate."""
    UNKNOWN = "Unknown"
    """The outcome of a command is not known. Repeating it could create a second order."""


def _duration_text(ns: int) -> str:
    """A duration the way Rust prints it with ``{:?}``: ``1s``, ``1.5s``, ``250ms``, ``15µs``."""
    secs, nanos = divmod(ns, 1_000_000_000)
    if secs > 0:
        whole, frac, digits, unit = secs, nanos, 9, "s"
    elif nanos >= 1_000_000:
        whole, frac, digits, unit = nanos // 1_000_000, nanos % 1_000_000, 6, "ms"
    elif nanos >= 1_000:
        whole, frac, digits, unit = nanos // 1_000, nanos % 1_000, 3, "µs"
    else:
        whole, frac, digits, unit = nanos, 0, 0, "ns"
    text = str(whole)
    if frac:
        text += "." + str(frac).rjust(digits, "0").rstrip("0")
    return text + unit


class BrokapiError(Exception):
    """Base class of every error that brokapi raises about a broker or a command."""

    error_class: ClassVar[ErrorClass] = ErrorClass.FATAL
    """May the call be tried again: see :class:`ErrorClass`."""

    def __reduce__(self) -> tuple[Any, ...]:
        return (_restore, (type(self), str(self), dict(self.__dict__)))


def _restore(cls: type[BrokapiError], text: str, attrs: dict[str, Any]) -> BrokapiError:
    error = cls.__new__(cls)
    Exception.__init__(error, text)
    error.__dict__.update(attrs)
    return error


class Config(BrokapiError):
    """The setup is wrong: a missing token, a bad value, an input that cannot work with this broker.

    Nothing was sent. The call fails the same way until the setting is fixed.
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"configuration error: {message}")
        self.message = message


class Transport(BrokapiError):
    """The network or the broker's endpoint failed in a way that is not an answer (connect, TLS, timeout, a cut stream).

    For a reading call, repeat it later. A command that might have reached the broker raises :class:`Unknown`
    instead, so this error on a command means the connector has no sign that it did.
    """

    error_class = ErrorClass.RETRYABLE

    def __init__(self, message: str) -> None:
        super().__init__(f"transport error: {message}")
        self.message = message


class RateLimited(BrokapiError):
    """A rate limit stopped the call: the local counter of the connector or the broker's own answer.

    Wait ``retry_after``, then repeat. A command refused by the local counter was not sent. The library does not
    wait and repeat for you.
    """

    error_class = ErrorClass.RETRYABLE

    retry_after: timedelta
    """How long to wait before the next try. A hint, not a promise that the next call passes."""

    def __init__(self, retry_after: timedelta | float) -> None:
        if not isinstance(retry_after, timedelta):
            retry_after = timedelta(seconds=retry_after)
        ns = (retry_after.days * 86_400 + retry_after.seconds) * 1_000_000_000 + retry_after.microseconds * 1_000
        super().__init__(f"rate limited, retry after {_duration_text(ns)}")
        self.retry_after = retry_after


class Rejected(BrokapiError):
    """The broker (or the connector, before sending) said no to this request, and nothing was done.

    The request is wrong or not allowed: a price outside the band, no money, an unknown order. Change the request.
    Do not repeat it as it is. ``code`` is a short label set by the connector (``validation``, ``missing_price``,
    ``not_found``, ``price_off_step``, ...), safe to compare with ``==``. Do not parse ``message``.
    """

    code: str
    """Short label of the reason, set by the connector."""
    message: str
    """Explanation for a human, usually the broker's own text."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"rejected: {code}: {message}")
        self.code = code
        self.message = message


class Unknown(BrokapiError):
    """A command was sent and nothing says whether the broker did it: a timeout, a cut connection, an HTTP 5xx.

    The order may exist, may be cancelled, or may never have arrived. The library never repeats the command and
    neither should you:

    1. Do not send the command again.
    2. Ask the broker: ``order_state(cid)``, ``active_orders()`` or the executions.
    3. Only when the broker has no such order, place a new one under a NEW ``cid``.
    """

    error_class = ErrorClass.UNKNOWN

    def __init__(self) -> None:
        super().__init__("command outcome unknown: do not send it again, ask the broker with order_state(cid)")


class Unsupported(BrokapiError):
    """This connector or this broker does not have the capability, so the call was not made.

    Raised at once, without a network call. ``capability`` is a short name of what is missing. The library does not
    emulate a missing stream by polling. ``client.caps`` says in advance what a connector can do.
    """

    capability: str
    """Short name of the missing capability, safe to compare with ``==``."""

    def __init__(self, capability: str) -> None:
        super().__init__(f"unsupported capability: {capability}")
        self.capability = capability


class NotReconciled(BrokapiError):
    """The trading gate is closed: ``reconcile()`` has not run successfully yet.

    Call ``reconcile`` with your ledger (an empty list if you have none), then repeat the command. Nothing was sent.
    """

    def __init__(self) -> None:
        super().__init__("not reconciled: call reconcile() before trading commands")


class Degraded(BrokapiError):
    """The trading gate is closed because the connector lost events it could not afford to lose.

    A lossless subscription overflowed (an :class:`~brokapi.Overflow` event was sent). Read the event stream to the
    end, then call ``reconcile()``: a successful one opens the gate again. Nothing was sent.
    """

    def __init__(self) -> None:
        super().__init__(
            "connector degraded: events may be lost, read the event channel to the end and call reconcile()"
        )


class ShuttingDown(BrokapiError):
    """The connector is shutting down and takes no new trading commands.

    Final for this client. Orders already at the exchange are not touched by a shutdown. Connect again to trade.
    """

    def __init__(self) -> None:
        super().__init__("shutting down: the connector takes no new trading commands")


class LogError(BrokapiError):
    """The event log could not be written or read (``LogError`` of the Rust library).

    ``kind`` says why: ``io`` (the file or the disk failed: check the path and the space), ``format`` (a line is not
    what the format says, the file is damaged or is not a log: ``line`` is its number, the header is line 1) or
    ``schema`` (the file was written by a newer version: update the package to read it). A reader never skips a bad
    line silently: it stops with this error.
    """

    kind: str
    """``io``, ``format`` or ``schema``."""
    line: Optional[int]
    """The number of the bad line (from 1), for ``format``."""
    found: Optional[int]
    """The format or schema the file declares, for ``schema``."""
    supported: Optional[int]
    """The newest this build reads, for ``schema``."""

    def __init__(
        self, kind: str, message: str, line: Optional[int] = None, found: Optional[int] = None,
        supported: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.line = line
        self.found = found
        self.supported = supported


def _log_error(kind: str, text: str, fields: dict[str, Any]) -> LogError:
    """Builds the exception for a failure of the log in the native layer. The text is the one that Rust printed."""
    return LogError(kind, text, fields.get("line"), fields.get("found"), fields.get("supported"))


def _from_native(variant: str, text: str, fields: dict[str, Any]) -> BrokapiError:
    """Builds the exception for an error of the native layer. The text is the one that Rust printed."""
    classes = {
        "Config": Config,
        "Transport": Transport,
        "RateLimited": RateLimited,
        "Rejected": Rejected,
        "Unknown": Unknown,
        "Unsupported": Unsupported,
        "NotReconciled": NotReconciled,
        "Degraded": Degraded,
        "ShuttingDown": ShuttingDown,
    }
    # A variant that the core got after this package was built: still an error of brokapi, with the core's text.
    cls = classes.get(variant, BrokapiError)
    error = cls.__new__(cls)
    Exception.__init__(error, text)
    if variant == "RateLimited":
        error.retry_after = timedelta(microseconds=fields["retry_after_ns"] / 1_000)  # type: ignore[attr-defined]
    else:
        for key, value in fields.items():
            setattr(error, key, value)
    return error
