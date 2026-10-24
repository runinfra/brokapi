"""Moving from tinkoff-investments: the two conversions that every ported bot needs, lots to units and Quotation to Decimal.

The code of the page "Если вы пришли с tinkoff-investments" of the book. The tests run it against the imitation of T-Bank.
"""

import asyncio
import uuid
from decimal import Decimal

import brokapi


# ANCHOR: lots
async def place_lots(
    client: brokapi.Client, ticker: str, lots: int, price: Decimal, side: brokapi.Side = brokapi.Side.BUY
) -> uuid.UUID:
    """The order of the old code was `quantity=lots`. Here the quantity is in units of the instrument: lots times the lot size."""
    instrument = await client.instrument(ticker)  # an InstrumentId (the uid inside), not a figi string
    params = await client.trading_params(instrument)
    cid = brokapi.new_cid()  # the old `order_id`: made by the bot, in its journal before the call
    await client.place(cid=cid, instrument=instrument, side=side, qty=params.lot_size * lots, price=price)
    return cid
    # ANCHOR_END: lots


# ANCHOR: quotation
def quotation_to_decimal(units: int, nano: int) -> Decimal:
    """`Quotation(units, nano)` and the `units` and `nano` of `MoneyValue` of the old code, for the data you keep in files.

    Exact: no float on the way. Negative prices have both parts negative, as in the API of the broker."""
    return Decimal(units) + Decimal(nano) / Decimal(1_000_000_000)
    # ANCHOR_END: quotation


async def main() -> None:
    client = await brokapi.connect("tbank", sandbox=True)
    try:
        await client.reconcile([])
        cid = await place_lots(client, "SBER", 1, Decimal("200.00"))
        print(f"placed {cid}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
