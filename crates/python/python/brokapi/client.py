"""The connection to a broker account: :func:`connect`, :class:`Client` and the stream of events."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Optional, Sequence, overload

from . import _codec, _native
from .models import (
    Backpressure,
    Balance,
    Candle,
    Caps,
    Envelope,
    Execution,
    Instrument,
    InstrumentId,
    InstrumentQuery,
    MarketTrade,
    OrderAck,
    OrderBook,
    OrderSnapshot,
    OrderType,
    Portfolio,
    Position,
    ReconcileReport,
    Schedule,
    ShutdownReport,
    Side,
    Timeframe,
    TradingParams,
    TradingStatus,
)

__all__ = ["CandlePager", "Client", "EventStream", "Limiter", "connect", "merge", "new_cid"]

_BROKERS = {
    "bcs": {"refresh_token", "write", "endpoint"},
    "tbank": {"token", "sandbox", "account_id", "endpoint", "app_name"},
    "finam": {"secret", "account_id", "endpoint"},
}


def new_cid() -> uuid.UUID:
    """A new client order id. The bot makes it, not the library, and writes it to its journal BEFORE ``place``."""
    return uuid.uuid4()


class Limiter:
    """The rate limit counter of one broker area.

    A broker counts its limits per token or per user, so everything that talks to the broker with that token must
    share one counter: two counters together exceed the quota and the broker starts to refuse. Every
    :func:`connect` makes its own. To put several clients on one counter, pass ``limiter=first.limiter`` to the
    next ones. A limiter is shared only inside one broker.
    """

    def __init__(self, native: _native.NativeLimiter) -> None:
        self._native = native

    @property
    def broker(self) -> str:
        """The broker whose limits it counts: ``bcs``, ``tbank`` or ``finam``."""
        return self._native.broker

    def __repr__(self) -> str:
        return f"Limiter(broker={self.broker!r})"


class EventStream:
    """The events of one client, in order: ``async for envelope in client.events``.

    One stream per client, for market data and for the account together. :attr:`Envelope.source` says which
    connector sent an event, :attr:`Envelope.sub` which subscription. The stream ends when the client is closed.

    Read it as fast as you can. Do slow work (a database, a model) in another task and hand it the events through
    your own queue: when this stream is not read, the connector applies the rule of each subscription, and a lossless
    one closes trading after it overflows (see ``Overflow``).
    """

    def __init__(self, native: _native.NativeClient) -> None:
        self._native = native

    def __aiter__(self) -> "EventStream":
        return self

    async def __anext__(self) -> Envelope:
        text = await self._native.next_event()
        if text is None:
            raise StopAsyncIteration
        return Envelope._from_json(_codec.loads(text))

    async def get(self, timeout: Optional[float] = None) -> Envelope:
        """The next event. Raises ``asyncio.TimeoutError`` after ``timeout`` seconds, ``EOFError`` at the end."""
        try:
            if timeout is None:
                return await self.__anext__()
            return await asyncio.wait_for(self.__anext__(), timeout)
        except StopAsyncIteration:
            raise EOFError("the client is closed, there are no more events") from None

    def drain(self, limit: int = 10_000) -> list[Envelope]:
        """The events that are already waiting, without waiting for more."""
        return [Envelope._from_json(_codec.loads(t)) for t in self._native.drain_events(limit)]


class CandlePager:
    """A long candle history, one window per step: ``async for bars in client.candle_pages(...)``.

    A window is as long as the broker accepts, reads wait out ``RateLimited`` and repeat after a network error
    (reading is safe to repeat). Bars come in time order, each one once. A page can be empty (a weekend): that is
    not the end. After an error, :attr:`resume_from` is where to start again, and nothing before it is read twice.
    """

    def __init__(self, native: _native.NativePager, start: datetime) -> None:
        self._native = native
        self.resume_from: datetime = start
        """The start of the next window: save it after each page you wrote."""

    def __aiter__(self) -> "CandlePager":
        return self

    async def __anext__(self) -> list[Candle]:
        try:
            text = await self._native.next_page()
        except Exception as error:
            ns = getattr(error, "resume_from_ns", None)
            if ns is not None:
                self.resume_from = _codec.from_nanos(ns)
            raise
        if text is None:
            raise StopAsyncIteration
        page = _codec.loads(text)
        self.resume_from = _codec.from_nanos(page["resume_from"])
        return _codec.decode(list[Candle], page["bars"])


def _opts(depth: Optional[int], backpressure: Optional[Backpressure]) -> dict[str, Any]:
    return {"backpressure": backpressure or Backpressure.conflate(), "depth": depth}


class Client:
    """A connection to one broker account: market data, orders, the account, and the stream of events.

    Make one with :func:`connect`. Every method is a coroutine. A client is an ``async with`` context: it is closed
    when the block ends.

    The rules of the core hold here as they do in Rust. Trading starts after :meth:`reconcile`. The ``cid`` of an
    order is made by the bot (:func:`new_cid`). :class:`~brokapi.Unknown` is never repeated: ask :meth:`order_state`.
    A command that you stop waiting for (a cancelled task, a timeout) still runs to its end in the library, so after
    that, too, ask the broker what became of it.
    """

    def __init__(self, native: _native.NativeClient) -> None:
        self._native = native
        self._events = EventStream(native)
        self._closed = False

    def __repr__(self) -> str:
        return f"Client(broker={self.broker!r}, name={self.name!r})"

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def broker(self) -> str:
        """``bcs``, ``tbank`` or ``finam``."""
        return self._native.broker

    @property
    def name(self) -> str:
        """The name given to :func:`connect`. Events say ``<name>-md`` and ``<name>-ud`` in ``source``."""
        return self._native.name

    @property
    def limiter(self) -> Limiter:
        """The rate limit counter of this client. Pass it to the next :func:`connect` to share it."""
        return Limiter(self._native.limiter)

    @property
    def events(self) -> EventStream:
        """The stream of events of this client (the same object every time)."""
        return self._events

    async def _call(self, method: str, **args: Any) -> Any:
        text = await self._native.call(method, _codec.dumps(args))
        return _codec.loads(text)

    async def close(self, deadline: Optional[float] = 5.0) -> list[ShutdownReport]:
        """Stops the connectors and ends the event stream. Safe to call twice.

        ``deadline`` is how many seconds a graceful stop may take, ``None`` stops at once. **Orders at the exchange are
        not cancelled**: cancel them yourself before, if the bot should leave none. Returns the reports of the two
        connectors (empty if the client was closed already).
        """
        if self._closed:
            return []
        self._closed = True
        text = await self._native.close(deadline)
        report = _codec.loads(text)
        if report is None:
            return []
        return [_codec.decode(ShutdownReport, report["md"]), _codec.decode(ShutdownReport, report["ud"])]

    # ------------------------------------------------------------------------------------------ instruments

    async def caps(self) -> Caps:
        """What this connection can do: streams, ``replace``, stop orders, how a repeated ``cid`` is treated."""
        return _codec.decode(Caps, await self._call("caps"))

    async def instrument(self, ticker: str, board: Optional[str] = None) -> InstrumentId:
        """The instrument for a ticker, in the form the broker wants.

        ``board`` is the class code of BCS and T-Bank (default ``TQBR``) or the MIC of Finam (default ``MISX``).
        ``"SBER@TQBR"`` works as the ticker too. BCS and Finam build the id from the text. T-Bank looks it up in
        its directory (one request) and raises :class:`~brokapi.Rejected` with code ``not_found`` if there is none.
        """
        return _codec.decode(InstrumentId, await self._call("instrument", ticker=ticker, board=board))

    async def find(self, query: InstrumentQuery) -> list[Instrument]:
        """Looks in the directory of the broker."""
        return _codec.decode(list[Instrument], await self._call("find", query=query))

    async def get_instrument(self, instrument: InstrumentId) -> Instrument:
        """The directory entry of an instrument."""
        return _codec.decode(Instrument, await self._call("get_instrument", instrument=instrument))

    async def trading_params(self, instrument: InstrumentId) -> TradingParams:
        """The price step, the smallest quantity and the lot of an instrument.

        Also teaches the connector the price step, so that a price off the step is refused at once with
        :class:`~brokapi.Rejected` (``price_off_step``), before anything is sent.
        """
        return _codec.decode(TradingParams, await self._call("trading_params", instrument=instrument))

    async def trading_status(self, instrument: InstrumentId) -> TradingStatus:
        """Does the instrument trade now."""
        return _codec.decode(TradingStatus, await self._call("trading_status", instrument=instrument))

    async def schedule(self, exchange: str, start: datetime, end: datetime) -> Schedule:
        """Trading sessions of an exchange in a period."""
        return _codec.decode(Schedule, await self._call("schedule", exchange=exchange, start=start, end=end))

    # ------------------------------------------------------------------------------------------ market data

    async def order_book(self, instrument: InstrumentId, depth: int = 10) -> OrderBook:
        """The order book now, ``depth`` levels on each side (one request)."""
        text = await self._call("order_book", instrument=instrument, depth=depth)
        return _book(text)

    async def last_trades(self, instrument: InstrumentId, limit: int = 100) -> list[MarketTrade]:
        """The last deals of the tape. How deep it goes depends on the broker."""
        return _codec.decode(list[MarketTrade], await self._call("last_trades", instrument=instrument, limit=limit))

    async def candles(
        self, instrument: InstrumentId, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]:
        """Bars of a period in one request. A broker accepts only a limited span per request: for a long history use
        :meth:`candle_pages` or :meth:`load_candles`, which cut the period into windows."""
        args = {"instrument": instrument, "timeframe": timeframe, "start": start, "end": end}
        return _codec.decode(list[Candle], await self._call("candles", **args))

    def candle_pages(
        self,
        instrument: InstrumentId,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        *,
        window: Optional[timedelta] = None,
        max_rate_waits: Optional[int] = None,
        transport_retries: Optional[int] = None,
    ) -> CandlePager:
        """A long history, one window per step. ``window`` overrides the span the broker accepts per request."""
        args: dict[str, Any] = {"instrument": instrument, "timeframe": timeframe, "start": start, "end": end}
        if window is not None:
            args["window_secs"] = window.total_seconds()
        if max_rate_waits is not None:
            args["max_rate_waits"] = max_rate_waits
        if transport_retries is not None:
            args["transport_retries"] = transport_retries
        return CandlePager(self._native.candle_pager(_codec.dumps(args)), start)

    async def load_candles(
        self, instrument: InstrumentId, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]:
        """The whole history in memory. Suits a few thousand bars; for more write page by page."""
        bars: list[Candle] = []
        async for page in self.candle_pages(instrument, timeframe, start, end):
            bars.extend(page)
        return bars

    async def subscribe_order_book(
        self,
        instrument: InstrumentId,
        *,
        depth: Optional[int] = None,
        backpressure: Optional[Backpressure] = None,
    ) -> int:
        """Order book events for an instrument. Returns the subscription id (``Envelope.sub``).

        By default ``Backpressure.conflate()``: only the latest book matters, a slow reader just misses old ones.
        """
        return await self._call("subscribe_order_book", instrument=instrument, opts=_opts(depth, backpressure))

    async def subscribe_trades(self, instrument: InstrumentId, *, backpressure: Backpressure) -> int:
        """Deals of the tape. ``backpressure`` has no default: every deal is an event, so ``conflate()`` would lose
        deals. Choose ``drop_newest(cap)`` if losing some is acceptable, ``lossless(cap)`` if not."""
        return await self._call("subscribe_trades", instrument=instrument, opts=_opts(None, backpressure))

    async def subscribe_quotes(
        self, instrument: InstrumentId, *, backpressure: Optional[Backpressure] = None
    ) -> int:
        """Best bid and ask events."""
        return await self._call("subscribe_quotes", instrument=instrument, opts=_opts(None, backpressure))

    async def subscribe_candles(
        self, instrument: InstrumentId, timeframe: Timeframe, *, backpressure: Optional[Backpressure] = None
    ) -> int:
        """Candle events."""
        return await self._call(
            "subscribe_candles", instrument=instrument, timeframe=timeframe, opts=_opts(None, backpressure)
        )

    async def unsubscribe(self, sub: int) -> None:
        """Ends a market data subscription."""
        await self._call("unsubscribe", sub=sub)

    # ------------------------------------------------------------------------------------------ account

    async def portfolio(self) -> Portfolio:
        """Money and holdings."""
        return _codec.decode(Portfolio, await self._call("portfolio"))

    async def positions(self) -> list[Position]:
        """Holdings."""
        return _codec.decode(list[Position], await self._call("positions"))

    async def balances(self) -> list[Balance]:
        """Money by currency."""
        return _codec.decode(list[Balance], await self._call("balances"))

    async def executions(self, start: datetime, end: datetime) -> list[Execution]:
        """Your fills in a period."""
        return _codec.decode(list[Execution], await self._call("executions", start=start, end=end))

    async def subscribe_order_state(self) -> int:
        """Events about your orders (``OrderSnapshot``). Always lossless. Subscribe BEFORE :meth:`reconcile`, so no
        event between the snapshot of the reconcile and the subscription is lost."""
        return await self._call("subscribe_order_state")

    async def subscribe_executions(self) -> int:
        """Events about your fills (``Execution``)."""
        return await self._call("subscribe_executions")

    async def subscribe_portfolio(self, *, backpressure: Optional[Backpressure] = None) -> int:
        """Events about money and holdings."""
        return await self._call("subscribe_portfolio", opts=_opts(None, backpressure))

    # ------------------------------------------------------------------------------------------ orders

    async def reconcile(self, ledger: Sequence[uuid.UUID | str] = ()) -> ReconcileReport:
        """Compares your ledger with the account and opens the trading gate.

        ``ledger`` is the cids your bot believes are alive (an empty list for a new bot). Until a reconcile has
        succeeded, ``place`` raises :class:`~brokapi.NotReconciled`. After :class:`~brokapi.Degraded` call it again.
        """
        cids = [_codec.cid_text(c) for c in ledger]
        return _codec.decode(ReconcileReport, await self._call("reconcile", ledger=cids))

    async def place(
        self,
        *,
        cid: uuid.UUID,
        instrument: InstrumentId,
        side: Side,
        qty: Decimal | str | int,
        price: Decimal | str | int | None = None,
        order_type: OrderType = OrderType.LIMIT,
    ) -> OrderAck:
        """Places an order.

        ``cid`` is made by the bot (:func:`new_cid`) and belongs in its journal before this call. ``qty`` is in
        units of the instrument (shares, currency units, contracts), never in lots: a broker that counts in lots
        gets the conversion from the connector, and a quantity that is not a whole number of lots is refused with
        :class:`~brokapi.Rejected` (``invalid_qty``) before anything is sent. ``qty`` and ``price`` are ``Decimal`` (a ``float`` is refused). A limit order needs a ``price``, else the call raises
        :class:`~brokapi.Rejected` (``missing_price``).

        Raises :class:`~brokapi.NotReconciled` before the first :meth:`reconcile`, :class:`~brokapi.Rejected` when the
        broker or the connector says no (nothing was done), :class:`~brokapi.Unknown` when the outcome is not known:
        do not repeat, call :meth:`order_state`.
        """
        request = {
            "cid": _codec.cid_text(cid),
            "instrument": instrument,
            "side": side,
            "order_type": order_type,
            "qty": _codec.decimal_text(qty, "qty"),
            "price": None if price is None else _codec.decimal_text(price, "price"),
        }
        return _codec.decode(OrderAck, await self._call("place", request=request))

    async def cancel(self, cid: uuid.UUID) -> None:
        """Cancels an order. Raises :class:`~brokapi.Unknown` when the outcome is not known."""
        await self._call("cancel", cid=_codec.cid_text(cid))

    async def replace(
        self,
        *,
        cid: uuid.UUID,
        original_cid: uuid.UUID,
        qty: Decimal | str | int | None = None,
        price: Decimal | str | int | None = None,
    ) -> OrderAck:
        """Changes the price or quantity of an order. The new order has its own ``cid``.

        Only where the broker can: ``(await client.caps()).ud.supports_replace``. Elsewhere it raises
        :class:`~brokapi.Unsupported` and the order is not touched.
        """
        request = {
            "cid": _codec.cid_text(cid),
            "original_cid": _codec.cid_text(original_cid),
            "qty": None if qty is None else _codec.decimal_text(qty, "qty"),
            "price": None if price is None else _codec.decimal_text(price, "price"),
        }
        return _codec.decode(OrderAck, await self._call("replace", request=request))

    async def order_state(self, cid: uuid.UUID) -> OrderSnapshot:
        """What the broker says about an order. The way out of :class:`~brokapi.Unknown`."""
        return _codec.decode(OrderSnapshot, await self._call("order_state", cid=_codec.cid_text(cid)))

    async def active_orders(self) -> list[OrderSnapshot]:
        """Orders that are alive at the broker."""
        return _codec.decode(list[OrderSnapshot], await self._call("active_orders"))


def _book(value: Any) -> OrderBook:
    from .models import _decode_book

    return _decode_book(value)


@overload
async def connect(
    broker: Literal["bcs"],
    *,
    refresh_token: Optional[str] = None,
    write: Optional[bool] = None,
    endpoint: Optional[str] = None,
    name: Optional[str] = None,
    limiter: Optional[Limiter] = None,
    event_buffer: int = 8192,
) -> Client: ...
@overload
async def connect(
    broker: Literal["tbank"],
    *,
    token: Optional[str] = None,
    sandbox: Optional[bool] = None,
    account_id: Optional[str] = None,
    endpoint: Optional[str] = None,
    app_name: Optional[str] = None,
    name: Optional[str] = None,
    limiter: Optional[Limiter] = None,
    event_buffer: int = 8192,
) -> Client: ...
@overload
async def connect(
    broker: Literal["finam"],
    *,
    secret: Optional[str] = None,
    account_id: Optional[str] = None,
    endpoint: Optional[str] = None,
    name: Optional[str] = None,
    limiter: Optional[Limiter] = None,
    event_buffer: int = 8192,
) -> Client: ...
async def connect(
    broker: str,
    *,
    name: Optional[str] = None,
    limiter: Optional[Limiter] = None,
    event_buffer: int = 8192,
    **settings: Any,
) -> Client:
    """Connects to a broker account.

    What you do not pass is read from the environment (and a ``.env`` file next to the program), as in the Rust
    library:

    - ``bcs``: ``refresh_token`` (``BCS_REFRESH_TOKEN``, required), ``write`` (``BCS_IS_WRITE``, ``true``: the token can
      trade).
    - ``tbank``: ``token`` (``TBANK_TOKEN``, required), ``sandbox`` (``TBANK_SANDBOX``, without it the address is the real
      one), ``account_id`` (``TBANK_ACCOUNT_ID``, needed when the token has several), ``endpoint`` and ``app_name``
      (``TBANK_ENDPOINT``, ``TBANK_APP_NAME``).
    - ``finam``: ``secret`` (``FINAM_SECRET``, required), ``account_id`` (``FINAM_ACCOUNT_ID``, required), ``endpoint``
      (``FINAM_API_URL``).

    ``name`` labels the events (``<name>-md``, ``<name>-ud``) and defaults to the broker. Give each client of a
    process its own name. ``limiter`` puts this client on the rate limit counter of another one (see
    :class:`Limiter`). ``event_buffer`` is how many events wait for the reader before the connector applies the
    rule of each subscription.

    Raises :class:`~brokapi.Config` for a missing or wrong setting, :class:`~brokapi.Transport` when the broker cannot
    be reached or refuses the token. BCS has no sandbox: the account is real.
    """
    if broker not in _BROKERS:
        raise ValueError(f"unknown broker {broker!r}: use 'bcs', 'tbank' or 'finam'")
    unknown = set(settings) - _BROKERS[broker]
    if unknown:
        raise TypeError(
            f"connect({broker!r}) does not take {', '.join(sorted(unknown))}; "
            f"its settings are {', '.join(sorted(_BROKERS[broker]))}"
        )
    given = {k: v for k, v in settings.items() if v is not None}
    native = await _native.connect(
        broker,
        name or broker,
        _codec.dumps(given),
        None if limiter is None else limiter._native,
        event_buffer,
    )
    return Client(native)


async def merge(*sources: EventStream | Client) -> AsyncIterator[Envelope]:
    """One stream out of the streams of several clients: ``async for envelope in brokapi.merge(a, b)``.

    ``Envelope.source`` still says whose each event is. It ends when all the clients are closed. Stop reading
    it only when you are done with those clients: an event that was just fetched when you stop is lost.
    """
    streams = [s.events if isinstance(s, Client) else s for s in sources]
    pending: dict[asyncio.Future[Envelope], EventStream] = {
        asyncio.ensure_future(s.__anext__()): s for s in streams
    }
    try:
        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                stream = pending.pop(task)
                try:
                    envelope = task.result()
                except StopAsyncIteration:
                    continue
                pending[asyncio.ensure_future(stream.__anext__())] = stream
                yield envelope
    finally:
        for task in pending:
            task.cancel()
