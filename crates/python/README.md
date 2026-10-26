# brokapi for Python

One interface to the APIs of Russian brokers (BCS, T-Bank, Finam, ALOR) and the Bybit exchange: market data, orders, reconciliation and rate limits.
A thin layer over the Rust core of [brokapi](https://brokapi.ru/): `asyncio` for the events, `decimal.Decimal` for money,
the same errors and the same rules as the core.

**Open alpha.** The interface can change before the beta. Read "Before you trade with real money" first (in Russian):
<https://brokapi.ru/docs/before-live.html>.

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

Documentation: https://brokapi.ru/docs/python/ (in Russian). For AI agents that write code with the package: the index in text,
https://brokapi.ru/docs/python/llms.txt (and the whole book as one file, https://brokapi.ru/docs/llms-full.txt). Licence: MIT or Apache-2.0.
