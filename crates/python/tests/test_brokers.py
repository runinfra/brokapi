"""The same day on every imitation: the interface does not depend on the broker, nor on whether the bot reaches it directly
or through a gateway."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

import brokapi
from brokapi import testing

BROKERS = ["bcs", "tbank", "finam", "alor"]
# Each scenario runs twice: with the connection in this process, and through a gateway.
ROADS = BROKERS + [f"gw_{b}" for b in BROKERS]


@pytest.fixture(params=ROADS)
def pair(request: pytest.FixtureRequest) -> tuple[testing.MockBroker, brokapi.Client]:
    return request.getfixturevalue(request.param)


async def collect(client: brokapi.Client, done: callable, timeout: float = 10.0) -> list[brokapi.Event]:  # type: ignore[valid-type]
    """Reads events until `done(events)` is true. Waits for the events, not for time."""
    seen: list[brokapi.Event] = []

    async def read() -> None:
        async for envelope in client.events:
            seen.append(envelope.payload)
            if done(seen):
                return

    await asyncio.wait_for(read(), timeout)
    return seen


async def test_read_place_fill(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    mock, client = pair
    sber = await client.instrument("SBER")
    book = await client.order_book(sber, depth=3)
    assert isinstance(book, brokapi.OrderBookSnapshot)
    assert all(isinstance(level.price, Decimal) for level in book.bids + book.asks)
    params = await client.trading_params(sber)

    await client.subscribe_order_state()
    await client.subscribe_executions()
    await client.reconcile([])

    cid = brokapi.new_cid()
    price = Decimal("200.00")
    ack = await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=price)
    assert ack.cid == cid and ack.broker_id

    mock.fill(cid, qty=params.min_qty, price=price)

    events = await collect(
        client,
        lambda seen: any(isinstance(e, brokapi.Execution) for e in seen)
        and any(isinstance(e, brokapi.OrderSnapshot) and e.state.is_final for e in seen),
    )
    fills = [e for e in events if isinstance(e, brokapi.Execution)]
    assert fills[0].cid == cid
    assert fills[0].price == price
    assert fills[0].qty == params.min_qty
    assert (await client.order_state(cid)).state.kind is brokapi.OrderStateKind.FILLED

    history = await client.executions(
        datetime(2020, 1, 1, tzinfo=timezone.utc), datetime.now(timezone.utc) + timedelta(days=1)
    )
    # The fill is in the history (an imitation may keep older trades of the account there too).
    mine = [e for e in history if e.price == price and e.qty == params.min_qty]
    assert len(mine) == 1 and isinstance(mine[0].price, Decimal)


async def test_cancel(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    await client.reconcile([])
    cid = brokapi.new_cid()
    await client.place(cid=cid, instrument=sber, side=brokapi.Side.SELL, qty=params.min_qty, price=Decimal("500.00"))
    assert [o.cid for o in await client.active_orders()] == [cid]
    await client.cancel(cid)
    assert (await client.order_state(cid)).state.kind is brokapi.OrderStateKind.CANCELLED


async def test_reconcile_names_foreign_and_closed_orders(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    await client.reconcile([])
    mine = brokapi.new_cid()
    await client.place(cid=mine, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00"))
    never_sent = brokapi.new_cid()
    report = await client.reconcile([mine, never_sent])
    assert [o.cid for o in report.confirmed] == [mine]
    assert report.closed_offline == [never_sent]
    assert report.foreign == []


async def test_caps_tell_whether_replace_works(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    caps = await client.caps()
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    await client.reconcile([])
    first = brokapi.new_cid()
    await client.place(cid=first, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00"))
    second = brokapi.new_cid()
    if caps.ud.supports_replace:
        ack = await client.replace(cid=second, original_cid=first, price=Decimal("101.00"))
        assert ack.cid == second
        assert (await client.order_state(second)).price == Decimal("101.00")
    else:
        with pytest.raises(brokapi.Unsupported) as caught:
            await client.replace(cid=second, original_cid=first, price=Decimal("101.00"))
        assert str(caught.value) == f"unsupported capability: {caught.value.capability}"
        # The order was not touched.
        assert (await client.order_state(first)).price == Decimal("100.00")


async def test_book_stream(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    sber = await client.instrument("SBER")
    sub = await client.subscribe_order_book(sber, depth=5)

    async def first_book() -> brokapi.Envelope:
        async for envelope in client.events:
            if isinstance(envelope.payload, brokapi.OrderBookSnapshot):
                return envelope
        raise AssertionError("the stream ended")

    envelope = await asyncio.wait_for(first_book(), 10)
    assert envelope.sub == sub
    assert envelope.source.endswith("-md")
    await client.unsubscribe(sub)


async def test_the_same_cid_is_not_resent_by_the_library(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    """A repeated place with the same cid is refused: the library never repeats a command by itself."""
    _, client = pair
    sber = await client.instrument("SBER")
    params = await client.trading_params(sber)
    await client.reconcile([])
    cid = brokapi.new_cid()
    await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00"))
    with pytest.raises(brokapi.Rejected) as caught:
        await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=params.min_qty, price=Decimal("100.00"))
    assert caught.value.code == "duplicate_place"


def test_the_cids_of_two_orders_differ() -> None:
    assert brokapi.new_cid() != brokapi.new_cid()
    assert isinstance(brokapi.new_cid(), uuid.UUID)


async def test_the_root_certificate_of_tbank_is_a_setting_and_a_bad_path_is_named() -> None:
    # The endpoints of T-Bank are signed by the root of the Ministry of Digital Development: without it UnknownIssuer.
    with pytest.raises(brokapi.Config, match="/no/such/root.pem"):
        await brokapi.connect("tbank", token="t.token", account_id="x", root_certificate="/no/such/root.pem")


async def test_request_timeout_is_a_setting_and_a_bad_one_is_named() -> None:
    with pytest.raises(brokapi.Config, match="request_timeout"):
        await brokapi.connect("tbank", token="t.token", account_id="x", request_timeout=-1)


async def test_request_timeout_takes_a_fraction_of_a_second() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect(request_timeout=0.5)
        assert str(await client.instrument("SBER")) == "SBER@TQBR"


async def test_finam_without_an_account_id_names_the_accounts_of_the_secret() -> None:
    # The imitation's secret sees two accounts: the error lists them, so the person can pick one without the cabinet.
    async with await testing.mock_finam() as mock:
        with pytest.raises(brokapi.Config, match="sees 2 accounts: ACC1, ACC2"):
            await mock.connect(account_id=None)


@pytest.mark.parametrize("broker", ["bcs", "tbank", "finam", "alor", "bybit"])
async def test_a_price_off_the_step_is_refused_before_it_is_sent_without_a_call_of_trading_params(broker: str) -> None:
    # The check does not hang on the order of the calls: the client learns the step itself before the first order.
    ticker = "MOCKUSDT" if broker == "bybit" else "SBER"
    async with await getattr(testing, f"mock_{broker}")() as mock:
        client = await mock.connect()
        instrument = await client.instrument(ticker)
        await client.reconcile([])
        qty = Decimal(1) if broker == "bcs" else Decimal(10)  # one lot of the imitation, a whole number of lots
        with pytest.raises(brokapi.Rejected) as caught:
            await client.place(cid=brokapi.new_cid(), instrument=instrument, side=brokapi.Side.BUY, qty=qty, price=Decimal("100.005"))
        assert caught.value.code == "price_off_step"
        assert await client.active_orders() == []


async def test_a_new_sandbox_token_of_tbank_gets_an_account_from_python() -> None:
    # A new sandbox token sees no account, connect refuses it. One call opens the account, then connect finds it.
    async with await testing.mock_tbank(new_sandbox=True) as mock:
        with pytest.raises(brokapi.Config, match="no open account"):
            await mock.connect()
        account = await brokapi.open_tbank_sandbox_account(money="100000", **mock.settings)
        assert account == "acc-a"
        client = await mock.connect()
        sber = await client.instrument("SBER")
        await client.reconcile([])
        ack = await client.place(cid=brokapi.new_cid(), instrument=sber, side=brokapi.Side.BUY, qty=10, price=Decimal("100.00"))
        assert ack.broker_id


async def test_a_sandbox_account_is_not_opened_on_the_real_address() -> None:
    with pytest.raises(brokapi.Config, match="nothing was sent"):
        await brokapi.open_tbank_sandbox_account(token="t.not-a-token", sandbox=False)
    with pytest.raises(TypeError, match="float"):
        await brokapi.open_tbank_sandbox_account(money=1.5, token="t.x", sandbox=True)
