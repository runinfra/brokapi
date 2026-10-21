"""The data that brokapi returns and takes: instruments, order books, orders, events.

Every type is a frozen dataclass, so ``==`` and ``hash`` work and ``match`` can take it apart. Prices, quantities and
money are :class:`decimal.Decimal`, never ``float``. Moments are timezone-aware ``datetime`` in UTC with microsecond
precision. The exact nanoseconds are on :class:`Timestamps`.

The fields have the names of the Rust types in ``brokapi_core``, so the book and the Rust reference describe them
both.
"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Optional, Union

from ._codec import decode, from_nanos

__all__ = [
    "Backpressure",
    "Balance",
    "BalancesUpdate",
    "BookLevel",
    "Caps",
    "Candle",
    "Envelope",
    "Event",
    "Execution",
    "Instrument",
    "InstrumentId",
    "InstrumentKey",
    "InstrumentKind",
    "InstrumentQuery",
    "MarketDataCaps",
    "MarketTrade",
    "NativeId",
    "OrderAck",
    "OrderBook",
    "OrderBookIncrement",
    "OrderBookSnapshot",
    "OrderSnapshot",
    "OrderState",
    "OrderStateKind",
    "OrderType",
    "Overflow",
    "PlaceIdempotency",
    "Portfolio",
    "Position",
    "PositionsUpdate",
    "Quote",
    "ReconcileReport",
    "Resync",
    "ResyncReason",
    "Schedule",
    "SessionInterval",
    "SessionKind",
    "ShutdownReport",
    "Side",
    "StreamingCaps",
    "Timeframe",
    "Timestamps",
    "TradingAvailability",
    "TradingParams",
    "TradingStatus",
    "UnknownEvent",
    "UserDataCaps",
]


# ---------------------------------------------------------------------------------------------------- enums


class Side(enum.Enum):
    """Buy or sell."""

    BUY = "Buy"
    SELL = "Sell"


class OrderType(enum.Enum):
    """Limit order (needs a price) or market order."""

    LIMIT = "Limit"
    MARKET = "Market"


class Timeframe(enum.Enum):
    """Candle length."""

    M1 = "M1"
    M5 = "M5"
    M10 = "M10"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"
    W1 = "W1"
    MN = "MN"

    @property
    def seconds(self) -> int:
        """Length of one candle in seconds (a month is counted as 30 days)."""
        return {
            "M1": 60,
            "M5": 300,
            "M10": 600,
            "M15": 900,
            "M30": 1800,
            "H1": 3600,
            "H4": 14400,
            "D1": 86400,
            "W1": 604800,
            "MN": 2592000,
        }[self.value]


class InstrumentKind(enum.Enum):
    """Kind of an instrument."""

    STOCK = "Stock"
    BOND = "Bond"
    OPTION = "Option"
    FUTURE = "Future"
    ETF = "Etf"
    MUTUAL_FUND = "MutualFund"
    CURRENCY = "Currency"
    OTHER = "Other"


class TradingAvailability(enum.Enum):
    """Whether an instrument trades right now."""

    OPEN = "Open"
    CLOSED = "Closed"
    BREAK = "Break"
    UNKNOWN = "Unknown"


class SessionKind(enum.Enum):
    """Kind of a trading session."""

    PRE_MARKET = "PreMarket"
    MAIN = "Main"
    POST_MARKET = "PostMarket"
    AUCTION = "Auction"
    OTHER = "Other"


class ResyncReason(enum.Enum):
    """Why an order book must be taken again."""

    SEQUENCE_GAP = "SequenceGap"
    SUBSCRIPTION_RESET = "SubscriptionReset"
    MANUAL = "Manual"


class OrderStateKind(enum.Enum):
    """Where an order is in its life. See the orders page of the book for the diagram."""

    IN_FLIGHT = "InFlight"
    """Sent, no answer yet."""
    UNKNOWN = "Unknown"
    """The outcome is not known. Ask the broker with ``order_state(cid)``."""
    ACKED = "Acked"
    """The broker has it and it works."""
    PARTIALLY_FILLED = "PartiallyFilled"
    FILLED = "Filled"
    CANCELLED = "Cancelled"
    REJECTED = "Rejected"


# ---------------------------------------------------------------------------------------------------- time


@dataclass(frozen=True, slots=True)
class Timestamps:
    """When something happened, in nanoseconds since the Unix epoch (exact)."""

    rx_ns: int
    """When the connector received it."""
    broker_ns: Optional[int] = None
    """The time the broker put on it, if it did."""

    @property
    def rx_ts(self) -> datetime:
        """Receive time as an aware UTC datetime."""
        return from_nanos(self.rx_ns)

    @property
    def broker_ts(self) -> Optional[datetime]:
        """Broker time as an aware UTC datetime, if the broker gave one."""
        return None if self.broker_ns is None else from_nanos(self.broker_ns)

    @classmethod
    def _from_json(cls, value: dict[str, Any]) -> "Timestamps":
        return cls(rx_ns=value["rx_ts"], broker_ns=value.get("broker_ts"))


# ---------------------------------------------------------------------------------------------------- instruments


@dataclass(frozen=True, slots=True)
class InstrumentKey:
    """The same for every broker: a symbol on a venue."""

    symbol: str
    mic: str
    """Market identifier. For BCS it is the board (``TQBR``), for Finam the MIC (``MISX``)."""

    def __str__(self) -> str:
        return f"{self.symbol}@{self.mic}"


@dataclass(frozen=True, slots=True)
class NativeId:
    """How the broker itself names an instrument. Opaque: take it from ``client.instrument`` and pass it back."""

    kind: str
    """``SymbolMic``, ``TickerClass``, ``Figi`` or ``Uid``."""
    values: tuple[tuple[str, str], ...]

    def _to_json(self) -> Any:
        if self.kind in ("Figi", "Uid"):
            return {self.kind: self.values[0][1]}
        return {self.kind: dict(self.values)}

    @classmethod
    def _from_json(cls, value: dict[str, Any]) -> "NativeId":
        ((kind, body),) = value.items()
        if isinstance(body, str):
            return cls(kind, (("value", body),))
        return cls(kind, tuple((k, v) for k, v in body.items()))


@dataclass(frozen=True, slots=True)
class InstrumentId:
    """An instrument: the common key and the broker's own id."""

    key: InstrumentKey
    native: NativeId

    def __str__(self) -> str:
        return str(self.key)


@dataclass(frozen=True, slots=True)
class Instrument:
    """A directory entry."""

    id: InstrumentId
    name: str
    kind: InstrumentKind
    currency: str
    exchange: str
    isin: Optional[str] = None


@dataclass(frozen=True, slots=True)
class InstrumentQuery:
    """What to look for in the directory of the broker. Build it with the class methods."""

    _json: Any

    @classmethod
    def by_ticker(cls, ticker: str, exchange: Optional[str] = None) -> "InstrumentQuery":
        """A ticker, optionally on one exchange or board."""
        return cls({"ByTicker": {"ticker": ticker, "exchange": exchange}})

    @classmethod
    def by_isin(cls, isin: str) -> "InstrumentQuery":
        """An ISIN."""
        return cls({"ByIsin": isin})

    @classmethod
    def by_kind(cls, kind: InstrumentKind) -> "InstrumentQuery":
        """All instruments of one kind (can be a long list)."""
        return cls({"ByKind": kind.value})

    def _to_json(self) -> Any:
        return self._json


@dataclass(frozen=True, slots=True)
class TradingParams:
    """What an order must respect: the step of the price, the smallest quantity, the lot."""

    instrument: InstrumentId
    lot_size: Decimal
    min_qty: Decimal
    price_step: Decimal
    price_step_value: Decimal
    currency: str


@dataclass(frozen=True, slots=True)
class TradingStatus:
    """Whether an instrument trades now."""

    instrument: InstrumentId
    availability: TradingAvailability
    reason: Optional[str] = None
    ts: Optional[datetime] = None


@dataclass(frozen=True, slots=True)
class SessionInterval:
    """One trading session."""

    start: datetime
    end: datetime
    kind: SessionKind


@dataclass(frozen=True, slots=True)
class Schedule:
    """Trading sessions of an exchange."""

    exchange: str
    sessions: list[SessionInterval]
    ts: Timestamps


# ---------------------------------------------------------------------------------------------------- market data


@dataclass(frozen=True, slots=True)
class BookLevel:
    """One price level of an order book."""

    price: Decimal
    qty: Decimal


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """The whole book (the levels asked for). Replaces what you had."""

    instrument: InstrumentId
    bids: list[BookLevel]
    """Buy side, best price first."""
    asks: list[BookLevel]
    """Sell side, best price first."""
    ts: Timestamps


@dataclass(frozen=True, slots=True)
class OrderBookIncrement:
    """A change to apply to the book. A level with a zero ``qty`` is removed. ``seq`` grows by one."""

    instrument: InstrumentId
    seq: int
    bids: list[BookLevel]
    asks: list[BookLevel]
    ts: Timestamps


OrderBook = Union[OrderBookSnapshot, OrderBookIncrement]
"""A book message: :class:`OrderBookSnapshot` or :class:`OrderBookIncrement`."""


def _decode_book(value: dict[str, Any]) -> OrderBook:
    ((variant, body),) = value.items()
    cls = OrderBookSnapshot if variant == "Snapshot" else OrderBookIncrement
    return decode(cls, body)


@dataclass(frozen=True, slots=True)
class MarketTrade:
    """A deal on the exchange tape."""

    instrument: InstrumentId
    price: Decimal
    qty: Decimal
    side: Side
    ts: Timestamps
    trade_id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class Quote:
    """Best bid and best ask."""

    instrument: InstrumentId
    ts: Timestamps
    bid: Optional[BookLevel] = None
    ask: Optional[BookLevel] = None


@dataclass(frozen=True, slots=True)
class Candle:
    """One bar."""

    instrument: InstrumentId
    timeframe: Timeframe
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_ts: datetime
    close_ts: datetime
    ts: Timestamps


# ---------------------------------------------------------------------------------------------------- account


@dataclass(frozen=True, slots=True)
class Balance:
    """Money of one currency."""

    currency: str
    available: Decimal
    blocked: Decimal
    ts: Timestamps


@dataclass(frozen=True, slots=True)
class Position:
    """A holding."""

    instrument: InstrumentId
    qty: Decimal
    ts: Timestamps
    avg_price: Optional[Decimal] = None
    current_price: Optional[Decimal] = None


@dataclass(frozen=True, slots=True)
class Portfolio:
    """Money and holdings of the account."""

    balances: list[Balance]
    positions: list[Position]
    ts: Timestamps


@dataclass(frozen=True, slots=True)
class OrderAck:
    """The broker accepted an order."""

    cid: uuid.UUID
    broker_id: str
    ts: Timestamps


@dataclass(frozen=True, slots=True)
class OrderState:
    """Where an order is. The fields that do not apply to ``kind`` are ``None``."""

    kind: OrderStateKind
    cid: uuid.UUID
    broker_id: Optional[str] = None
    """Set when ``kind`` is ``ACKED``."""
    filled: Optional[Decimal] = None
    """Set when ``kind`` is ``PARTIALLY_FILLED``: how much is done."""
    left: Optional[Decimal] = None
    """Set when ``kind`` is ``PARTIALLY_FILLED``: how much is left."""
    reason: Optional[str] = None
    """Set when ``kind`` is ``REJECTED``."""

    @property
    def is_final(self) -> bool:
        """The order cannot change any more: filled, cancelled or rejected."""
        return self.kind in (OrderStateKind.FILLED, OrderStateKind.CANCELLED, OrderStateKind.REJECTED)

    @classmethod
    def _from_json(cls, value: Any) -> "OrderState":
        ((name, body),) = value.items()
        kind = OrderStateKind(name)
        return cls(
            kind=kind,
            cid=uuid.UUID(body["cid"]),
            broker_id=body.get("broker_id"),
            filled=Decimal(body["filled"]) if "filled" in body else None,
            left=Decimal(body["left"]) if "left" in body else None,
            reason=body.get("reason"),
        )


@dataclass(frozen=True, slots=True)
class OrderSnapshot:
    """An order as the broker has it. Also the event for a change of an order."""

    cid: uuid.UUID
    instrument: InstrumentId
    state: OrderState
    side: Side
    order_type: OrderType
    qty: Decimal
    """In units of the instrument (shares, currency units, contracts), not lots."""
    filled: Decimal
    ts: Timestamps
    broker_id: Optional[str] = None
    price: Optional[Decimal] = None


@dataclass(frozen=True, slots=True)
class Execution:
    """A fill of one of your orders."""

    broker_order_id: str
    instrument: InstrumentId
    side: Side
    price: Decimal
    qty: Decimal
    commission: Decimal
    trade_id: str
    ts: Timestamps
    cid: Optional[uuid.UUID] = None


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    """What ``reconcile`` found."""

    confirmed: list[OrderSnapshot]
    """Orders of your ledger that the broker has."""
    closed_offline: list[uuid.UUID]
    """Orders of your ledger that ended while you were away."""
    foreign: list[OrderSnapshot]
    """Active orders of the account that are not in your ledger."""


@dataclass(frozen=True, slots=True)
class ShutdownReport:
    """What a stop of one connector did."""

    unsubscribed: int
    unsubscribe_failed: int
    inflight_abandoned: list[uuid.UUID]


# ---------------------------------------------------------------------------------------------------- events


@dataclass(frozen=True, slots=True)
class PositionsUpdate:
    """The positions of the account changed."""

    positions: list[Position]


@dataclass(frozen=True, slots=True)
class BalancesUpdate:
    """The balances of the account changed."""

    balances: list[Balance]


@dataclass(frozen=True, slots=True)
class Resync:
    """The order book of this instrument must be taken again (take a snapshot, then follow increments)."""

    sub: int
    instrument: InstrumentId
    reason: ResyncReason


@dataclass(frozen=True, slots=True)
class Overflow:
    """The connector lost events of a lossless subscription since ``lost_from``.

    Trading is closed (:class:`~brokapi.Degraded`) until you read the stream to the end and call ``reconcile``.
    """

    sub: int
    lost_from: datetime


@dataclass(frozen=True, slots=True)
class UnknownEvent:
    """An event of a kind this version of the Python package does not know (the core is newer than the package).

    The stream does not break on it: ``kind`` is the name the core gave it and ``data`` its content as it came
    (JSON values, money as text). Update the package to get a typed class. An event about the reconnect of an order
    stream, when the core has one, means: call ``reconcile`` before trading further.
    """

    kind: str
    data: Any


Event = Union[
    OrderBookSnapshot,
    OrderBookIncrement,
    MarketTrade,
    Quote,
    Candle,
    OrderSnapshot,
    Execution,
    Portfolio,
    PositionsUpdate,
    BalancesUpdate,
    Resync,
    Overflow,
    UnknownEvent,
]
"""What an :class:`Envelope` carries."""


def _decode_event(value: dict[str, Any]) -> Event:
    ((variant, body),) = value.items()
    if variant == "OrderBook":
        return _decode_book(body)
    if variant == "Positions":
        return PositionsUpdate(decode(list[Position], body))
    if variant == "Balances":
        return BalancesUpdate(decode(list[Balance], body))
    known = {
        "MarketTrade": MarketTrade,
        "Quote": Quote,
        "Candle": Candle,
        "Order": OrderSnapshot,
        "Execution": Execution,
        "Portfolio": Portfolio,
        "Resync": Resync,
        "Overflow": Overflow,
    }
    if variant not in known:
        return UnknownEvent(variant, body)
    return decode(known[variant], body)


@dataclass(frozen=True, slots=True)
class Envelope:
    """One event with its address."""

    source: str
    """Which connector sent it: ``<name>-md`` for market data, ``<name>-ud`` for the account."""
    sub: Optional[int]
    """The subscription it belongs to, if any (what ``subscribe_*`` returned)."""
    ts: Timestamps
    payload: Event
    schema: int = 1

    @classmethod
    def _from_json(cls, value: dict[str, Any]) -> "Envelope":
        return cls(
            schema=value["schema"],
            source=value["source"],
            sub=value.get("sub"),
            ts=Timestamps._from_json(value["ts"]),
            payload=_decode_event(value["payload"]),
        )


# ---------------------------------------------------------------------------------------------------- options


@dataclass(frozen=True, slots=True)
class Backpressure:
    """What a subscription does when the reader is slow. Build it with the class methods."""

    kind: str
    cap: int = 0

    @classmethod
    def conflate(cls) -> "Backpressure":
        """Keep only the latest value. For books, quotes and candles, where the old value is useless."""
        return cls("Conflate")

    @classmethod
    def lossless(cls, cap: int) -> "Backpressure":
        """Never drop. At ``cap`` events in the buffer an :class:`Overflow` is sent and trading closes."""
        return cls("Lossless", cap)

    @classmethod
    def drop_newest(cls, cap: int) -> "Backpressure":
        """Keep up to ``cap`` events and drop the newest beyond that (and count them)."""
        return cls("DropNewest", cap)

    def _to_json(self) -> Any:
        if self.kind == "Conflate":
            return "Conflate"
        return {self.kind: {"cap": self.cap}}


# ---------------------------------------------------------------------------------------------------- capabilities


@dataclass(frozen=True, slots=True)
class StreamingCaps:
    """Which market data streams the broker has."""

    order_book: bool
    market_trades: bool
    quotes: bool
    candles: bool


@dataclass(frozen=True, slots=True)
class MarketDataCaps:
    """What the market data side can do."""

    max_order_book_depth: int
    incremental_order_book: bool
    streaming: StreamingCaps


@dataclass(frozen=True, slots=True)
class PlaceIdempotency:
    """What the broker does with a second ``place`` that has the same ``cid``."""

    kind: str
    """``LabelOnly`` (the broker does not look), ``RejectDuplicate`` or ``Idempotent``."""
    window: Optional[timedelta] = None

    @classmethod
    def _from_json(cls, value: Any) -> "PlaceIdempotency":
        if isinstance(value, str):
            return cls(value)
        ((kind, body),) = value.items()
        return cls(kind, timedelta(seconds=body["window"]))


@dataclass(frozen=True, slots=True)
class UserDataCaps:
    """What the account side can do."""

    place_idempotency: PlaceIdempotency
    ws_rpc: bool
    ordered_command_channel: bool
    supports_replace: bool
    """``replace`` works. Without it ``replace`` raises :class:`~brokapi.Unsupported`."""
    supports_stop_orders: bool
    streams_executions: bool
    sandbox: bool


@dataclass(frozen=True, slots=True)
class Caps:
    """What a connection can do (``client.caps``)."""

    md: MarketDataCaps
    ud: UserDataCaps


