Answers of the Bybit API V5 kept as test data.

rest/*.json without a prefix and ws/orderbook*.json, ws/public_trade.json, ws/kline.json, ws/ack_*.json, ws/pong.json, ws/auth_failed.json
    Recorded from the public endpoints of the main exchange (api.bybit.com, stream.bybit.com) on 2026-10-23. Prices and
    sizes are the market of that moment. Nothing in them belongs to an account (the login refused with a made-up key is the only answer of a private stream).

rest/doc_*.json and ws/doc_private_*.json
    The response examples of the Bybit documentation (https://bybit-exchange.github.io/docs/v5/). The private endpoints
    need a key, so no recording of them exists yet. When one is made, it replaces the example under the same name
    without the prefix.
