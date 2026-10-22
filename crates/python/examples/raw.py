"""Any method of a broker, outside the common interface: `client.raw`.

The code of the page "Любой метод брокера, Python" of the book. The tests run it against the imitations.
"""

import asyncio
from typing import Any

import brokapi


# ANCHOR: tbank
async def tbank_prices(client: brokapi.Client) -> dict[str, str]:
    """T-Bank (gRPC): the method is named as in the API description, the arguments are the request message."""
    reply = await client.raw("GetLastPrices", {"instrument_id": ["SBER_TQBR", "GAZP_TQBR"]})
    # int64 is text, money is {"units": "274", "nano": 50000000}: the wire form of the broker.
    return {p["ticker"]: f'{p["price"]["units"]}.{p["price"]["nano"]:09d}' for p in reply["last_prices"]}
    # ANCHOR_END: tbank


# ANCHOR: bcs
async def bcs_quotes(client: brokapi.Client) -> dict[str, Any]:
    """BCS (REST): a name from its rate limit file, `query` for the URL and `body` for the JSON of a POST."""
    reply = await client.raw("GetQuotes", {"body": {"instruments": [{"ticker": "SBER", "classCode": "TQBR"}]}})
    return {record["ticker"]: record["last"] for record in reply["records"]}  # prices are Decimal
    # ANCHOR_END: bcs


# ANCHOR: finam
async def finam_quote(client: brokapi.Client) -> str:
    """Finam (gRPC): the same as T-Bank. `google.type.Decimal` is {"value": "100.05"}."""
    reply = await client.raw("LastQuote", {"symbol": "SBER@MISX"})
    return reply["quote"]["last"]["value"]
    # ANCHOR_END: finam


# ANCHOR: wait
async def raw_read(client: brokapi.Client, method: str, args: dict[str, Any], tries: int = 3) -> Any:
    """A read through `raw` spends the same quota as every other call: when it is spent, wait and ask again."""
    for _ in range(tries - 1):
        try:
            return await client.raw(method, args)
        except brokapi.RateLimited as error:
            await asyncio.sleep(error.retry_after.total_seconds())
    return await client.raw(method, args)
    # ANCHOR_END: wait


# ANCHOR: command
async def raw_command(client: brokapi.Client, method: str, args: dict[str, Any]) -> Any:
    """Anything that changes the account goes with command=True: a lost answer is Unknown and it is never repeated."""
    try:
        return await client.raw(method, args, command=True)
    except brokapi.Unknown:
        raise  # ask the broker what became of it (a report, a list): do not call this again
    # ANCHOR_END: command


async def main() -> None:
    client = await brokapi.connect("bcs")
    try:
        print(await bcs_quotes(client))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
