"""`client.raw`: any method of a broker by name, on every imitation."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest

import brokapi
from brokapi import testing

BROKERS = ["bcs", "tbank", "finam", "alor"]

# One read that each imitation answers, and the typed call that spends the same quota.
READ: dict[str, tuple[str, dict[str, Any]]] = {
    "bcs": ("GetPortfolio", {}),
    "tbank": ("GetPortfolio", {"account_id": "acc-a"}),
    "finam": ("GetAccount", {"account_id": "ACC1"}),
    "alor": ("GET /md/v2/Clients/MOEX/D00001/positions", {}),
}


@pytest.fixture(params=BROKERS)
def pair(request: pytest.FixtureRequest) -> tuple[testing.MockBroker, brokapi.Client]:
    return request.getfixturevalue(request.param)


async def test_a_read_returns_the_reply_of_the_broker_as_plain_data(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    mock, client = pair
    method, args = READ[mock.broker]
    reply = await client.raw(method, args)
    if mock.broker == "bcs":
        assert {row["type"] for row in reply} == {"depoLimit", "moneyLimit"}
        assert all(isinstance(row["quantity"], (int, Decimal)) for row in reply)
    elif mock.broker == "alor":
        assert {row["symbol"] for row in reply} == {"RUB", "SBER"}
        assert all(isinstance(row["qtyUnits"], (int, Decimal)) for row in reply)
    elif mock.broker == "tbank":
        assert reply["account_id"] == "acc-a"
        assert isinstance(reply["positions"], list)
    else:
        assert reply["account_id"] == "ACC1"
        assert isinstance(reply["positions"], list)


async def test_tbank_money_and_64_bit_numbers_come_as_the_wire_form_text(
    tbank: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    _, client = tbank
    reply = await client.raw("GetLastPrices", {"instrument_id": ["SBER_TQBR"]})
    (last,) = reply["last_prices"]
    assert last["ticker"] == "SBER"
    assert last["price"] == {"units": "274", "nano": 50_000_000}  # int64 is text, nano is a number
    assert last["time"].endswith("Z")  # a Timestamp is RFC 3339


async def test_finam_decimal_is_text(finam: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = finam
    reply = await client.raw("LastQuote", {"symbol": "SBER@MISX"})
    assert reply["symbol"] == "SBER@MISX"
    assert Decimal(reply["quote"]["last"]["value"]) > 0


async def test_bcs_takes_the_name_or_the_path_and_the_same_reply_comes_back(
    bcs: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    _, client = bcs
    by_name = await client.raw("GetPortfolio")
    by_path = await client.raw("GET /trade-api-bff-portfolio/api/v1/portfolio")
    assert by_name == by_path
    quotes = await client.raw("GetQuotes", {"body": {"instruments": [{"ticker": "SBER", "classCode": "TQBR"}]}})
    assert quotes["records"][0]["ticker"] == "SBER"
    assert isinstance(quotes["records"][0]["last"], Decimal)  # a number with a fraction is a Decimal, never a float


async def test_bcs_query_and_path_arguments(bcs: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = bcs
    with pytest.raises(brokapi.Rejected) as error:  # the id goes into the path, the imitation knows no such order
        await client.raw("GetOrderStatus", {"path": {"id": "00000000-0000-0000-0000-000000000000"}})
    assert error.value.code == "not_found"
    with pytest.raises(brokapi.Config, match="path"):
        await client.raw("GetOrderStatus")  # the id is missing
    with pytest.raises(brokapi.Config, match="not valid"):
        await client.raw("GetOrderStatus", {"path": {"id": "a/../b"}})


async def test_an_unknown_method_is_refused_and_nothing_is_sent(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    with pytest.raises(brokapi.Config) as error:
        await client.raw("NoSuchMethodAtAll")
    assert "NoSuchMethodAtAll" in str(error.value)
    assert "nothing was sent" in str(error.value)
    assert error.value.error_class is brokapi.ErrorClass.FATAL


async def test_arguments_that_do_not_fit_the_method_are_refused(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    mock, client = pair
    if mock.broker == "bcs":
        with pytest.raises(brokapi.Config, match="verb"):
            await client.raw("DELETE /x")
        with pytest.raises(brokapi.Config, match="query"):
            await client.raw("GetQuotes", {"query": [1]})
    elif mock.broker == "alor":
        with pytest.raises(brokapi.Config, match="verb"):
            await client.raw("PATCH /x")
        with pytest.raises(brokapi.Config, match="query"):
            await client.raw("GET /md/v2/time", {"query": [1]})
    else:
        method, args = READ[mock.broker]
        with pytest.raises(brokapi.Config, match="do not fit"):
            await client.raw(method, {**args, "no_such_field": 1})
        with pytest.raises(brokapi.Config, match="do not fit"):
            await client.raw(method, {"account_id": ["a list"]})


async def test_a_float_is_refused_like_everywhere(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = pair
    with pytest.raises(TypeError, match="float"):
        await client.raw("GetQuotes", {"body": {"x": 1.5}})


async def test_a_method_with_a_stream_is_refused(
    tbank: tuple[testing.MockBroker, brokapi.Client], finam: tuple[testing.MockBroker, brokapi.Client]
) -> None:
    """The quota covers only the opening of a stream, and the stream is then yours: a Python call cannot hand it over."""
    for (_, client), method in ((tbank, "MarketDataStream"), (finam, "SubscribeQuote")):
        with pytest.raises(brokapi.Unsupported) as error:
            await client.raw(method, {})
        assert error.value.capability == "raw_streaming"


async def test_tbank_names_the_service_when_two_have_the_same_method(
    tbank: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    _, client = tbank
    plain = await client.raw("GetAccounts", {})
    qualified = await client.raw("UsersService/GetAccounts", {})
    assert plain == qualified and len(plain["accounts"]) == 2


# ---------------------------------------------------------------------------------------------------- the quota


async def exhaust(client: brokapi.Client, method: str, args: dict[str, Any]) -> brokapi.RateLimited:
    for _ in range(500):
        try:
            await client.raw(method, args)
        except brokapi.RateLimited as error:
            return error
    raise AssertionError("500 calls and the quota is not spent")


async def test_raw_spends_the_same_quota_as_the_calls_of_the_interface(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    mock, client = pair
    method, args = READ[mock.broker]
    limited = await exhaust(client, method, args)
    assert limited.retry_after > timedelta(0)
    # The interface finds the quota spent by raw: portfolio() is the same route.
    with pytest.raises(brokapi.RateLimited):
        await client.portfolio()


async def test_raw_of_two_clients_on_one_limiter_counts_together(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    mock, first = pair
    second = await mock.connect(limiter=first.limiter, name="second")
    method, args = READ[mock.broker]
    await exhaust(first, method, args)
    # The quota was spent by the other client. A bucket of a fast broker (ALOR: 100 a second) refills between two calls,
    # so the second client may get one or two through: still far from the full bucket a client of its own would have.
    with pytest.raises(brokapi.RateLimited):
        for _ in range(5):
            await second.raw(method, args)


async def test_a_call_without_a_route_spends_the_default_quota(pair: tuple[testing.MockBroker, brokapi.Client]) -> None:
    mock, client = pair
    # A method the library has no route for: it is never free.
    method, args = {
        "bcs": ("GET /trade-api-bff-portfolio/api/v1/portfolio", {}),
        "tbank": ("GetAccounts", {}),
        "finam": ("LastQuote", {"symbol": "SBER@MISX"}),
        "alor": ("GET /md/v2/time", {}),
    }[mock.broker]
    await exhaust(client, method, args)


# ---------------------------------------------------------------------------------------------------- commands


async def test_a_command_is_refused_by_the_broker_as_a_definite_no(
    pair: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    mock, client = pair
    if mock.broker == "tbank":
        with pytest.raises(brokapi.Rejected) as error:
            await client.raw("CancelOrder", {"account_id": "acc-a", "order_id": "no-such-order"}, command=True)
        assert error.value.code == "not_found"
    elif mock.broker == "finam":
        with pytest.raises(brokapi.BrokapiError) as error:
            await client.raw("CancelOrder", {"account_id": "ACC1", "order_id": "no-such-order"}, command=True)
        assert not isinstance(error.value, brokapi.Unknown)  # an answer is not an unknown outcome
    elif mock.broker == "alor":
        with pytest.raises(brokapi.Rejected) as error:
            await client.raw(
                "DELETE /commandapi/warptrans/TRADE/v2/client/orders/1",
                {"query": {"portfolio": "D00001", "exchange": "MOEX", "stop": False}},
                command=True,
            )
        assert error.value.code == "not_found"
    else:
        with pytest.raises(brokapi.Rejected):
            await client.raw("POST /trade-api-bff-operations/api/v1/orders/cancel/sync", {"body": {}}, command=True)


async def test_a_read_through_command_mode_works_too(bcs: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = bcs
    assert await client.raw("GetPortfolio", command=True) == await client.raw("GetPortfolio")


async def test_alor_takes_the_verb_and_the_path_and_a_request_id_for_a_command(
    alor: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    _, client = alor
    assert isinstance(await client.raw("GET /md/v2/time"), int)
    body = {
        "side": "buy",
        "quantity": 1,
        "price": Decimal("100.50"),
        "instrument": {"symbol": "SBER", "exchange": "MOEX", "instrumentGroup": "TQBR"},
        "user": {"portfolio": "D00001"},
    }
    path = "POST /commandapi/warptrans/TRADE/v2/client/orders/actions/limit"
    first = await client.raw(path, {"body": body, "request_id": "my-request-1"}, command=True)
    # The same request id: the broker answers with a copy of the first answer and does not place a second order.
    again = await client.raw(path, {"body": body, "request_id": "my-request-1"}, command=True)
    assert first == again and first["orderNumber"]
    assert len(await client.active_orders()) == 1


async def test_a_read_through_command_mode_works_too_on_alor(alor: tuple[testing.MockBroker, brokapi.Client]) -> None:
    _, client = alor
    assert await client.raw("GET /md/v2/time", command=True) > 0
