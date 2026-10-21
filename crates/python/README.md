# brokapi for Python

One interface to the APIs of Russian brokers (BCS, T-Bank, Finam): market data, orders, reconciliation and rate limits.
A thin layer over the Rust core of [brokapi](https://brokapi.ru/): `asyncio` for the events, `decimal.Decimal` for money,
the same errors and the same rules as the core.

**Closed alpha.** Checked against the imitations of the brokers, not yet on live accounts. The interface can change
before the beta.

```python
import asyncio
import brokapi

async def main():
    client = await brokapi.connect("bcs")  # BCS_REFRESH_TOKEN from the environment
    sber = await client.instrument("SBER")
    book = await client.order_book(sber, depth=5)
    print(book.bids[0].price, book.asks[0].price)
    await client.close()

asyncio.run(main())
```

Stop orders, `client.raw()` (any method of a broker's API by name) and the event log with replay (`brokapi.record`) work as in
the Rust library.

Documentation: https://brokapi.ru/docs/python/. Licence: MIT or Apache-2.0.
