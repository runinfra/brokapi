# Справочник API: Gateway (gRPC)

Контракт между ботом и гейтвеем: сервисы, вызовы, сообщения и перечисления. Страница собрана из файлов `.proto` в каталоге
`proto/` репозитория, руками не правится: сборка книги сравнивает ее с файлами и падает, если они разошлись.

Как подключиться, какие заголовки слать и в каком порядке вызывать: [Подключить бота](gateway/bots.md). Как устроены события и
что делать с `Gap` и `Lagged`: [События и сверка после перезапуска](gateway/events.md). Что значит каждая ошибка и что с ней делать:
[Ошибки и что с ними делать](errors.md).

В файлах три пакета описаний:

| Файл | Для кого | Что в нем |
|---|---|---|
| `gateway.proto` | боты | сервис `Gateway`: заявки, счет, рынок, подписки, `Health` и поток `Events` |
| `types.proto` | боты | значения, которыми обмениваются вызовы: инструменты, заявки, портфель, рыночные данные |
| `service.proto` | команды `brokapi` | сервис `Service` у службы `brokapi up`. Боту он не нужен |

Описания и комментарии в файлах английские, так они и показаны ниже: это тот самый текст, который увидит ваш генератор кода.

<!-- proto:begin -->

## Gateway: служба для ботов и события

Файл `proto/brokapi/gateway/v1/gateway.proto`, пакет `brokapi.gateway.v1`.

```text
The brokapi gateway: one process that holds the connection to one broker account and serves any number of bots.

How a bot uses it:

  1. Connect to the Unix socket of the gateway (or 127.0.0.1 on Windows) and send two metadata headers with every
     call: `authorization: Bearer <the key from the access.key file>` and `x-brokapi-bot: <name of the bot>`.
  2. Open `Events` first, then subscribe. The first frame of `Events` says the stream is registered. Events that
     happen after it are yours.
  3. Call `Reconcile` once before any trading command.

An error of the library comes back as a gRPC status with the `brokapi-error-bin` trailer, an `ErrorDetail`. The
status code is a hint for generic tools. The detail is the whole error: what a bot needs to decide whether to retry
is in `kind`.
```

### Сервис Gateway

<a id="brokapi-gateway-v1-gateway"></a>

A call named like the type it returns (Portfolio, OrderBook, TradingParams, TradingStatus, Schedule) names the type with its full name: inside the service the short name is the call, and protoc and buf refuse it. tools/proto-check.

**Orders: a command is never repeated by the gateway, and it runs to its end if you stop waiting**

| Вызов | Запрос | Ответ | Описание |
|---|---|---|---|
| `Place` | [`PlaceOrderRequest`](#brokapi-gateway-v1-placeorderrequest) | [`OrderAck`](#brokapi-gateway-v1-orderack) |  |
| `Cancel` | [`CancelRequest`](#brokapi-gateway-v1-cancelrequest) | [`CancelResponse`](#brokapi-gateway-v1-cancelresponse) |  |
| `Replace` | [`ReplaceOrderRequest`](#brokapi-gateway-v1-replaceorderrequest) | [`OrderAck`](#brokapi-gateway-v1-orderack) |  |
| `OrderState` | [`OrderStateRequest`](#brokapi-gateway-v1-orderstaterequest) | [`OrderSnapshot`](#brokapi-gateway-v1-ordersnapshot) |  |
| `ActiveOrders` | [`ActiveOrdersRequest`](#brokapi-gateway-v1-activeordersrequest) | [`ActiveOrdersResponse`](#brokapi-gateway-v1-activeordersresponse) |  |
| `Reconcile` | [`ReconcileRequest`](#brokapi-gateway-v1-reconcilerequest) | [`ReconcileReport`](#brokapi-gateway-v1-reconcilereport) |  |
| `SubscribeOrderState` | [`SubscribeRequest`](#brokapi-gateway-v1-subscriberequest) | [`SubscribeResponse`](#brokapi-gateway-v1-subscriberesponse) |  |

**Stop orders**

| Вызов | Запрос | Ответ | Описание |
|---|---|---|---|
| `PlaceStop` | [`PlaceStopOrderRequest`](#brokapi-gateway-v1-placestoporderrequest) | [`OrderAck`](#brokapi-gateway-v1-orderack) |  |
| `CancelStop` | [`CancelStopRequest`](#brokapi-gateway-v1-cancelstoprequest) | [`CancelResponse`](#brokapi-gateway-v1-cancelresponse) |  |
| `ActiveStopOrders` | [`ActiveOrdersRequest`](#brokapi-gateway-v1-activeordersrequest) | [`ActiveStopOrdersResponse`](#brokapi-gateway-v1-activestopordersresponse) |  |
| `SubscribeStopOrders` | [`SubscribeRequest`](#brokapi-gateway-v1-subscriberequest) | [`SubscribeResponse`](#brokapi-gateway-v1-subscriberesponse) |  |

**Account**

| Вызов | Запрос | Ответ | Описание |
|---|---|---|---|
| `Portfolio` | [`AccountRequest`](#brokapi-gateway-v1-accountrequest) | [`Portfolio`](#brokapi-gateway-v1-portfolio) |  |
| `Positions` | [`AccountRequest`](#brokapi-gateway-v1-accountrequest) | [`PositionsResponse`](#brokapi-gateway-v1-positionsresponse) |  |
| `Balances` | [`AccountRequest`](#brokapi-gateway-v1-accountrequest) | [`BalancesResponse`](#brokapi-gateway-v1-balancesresponse) |  |
| `Executions` | [`ExecutionsRequest`](#brokapi-gateway-v1-executionsrequest) | [`ExecutionsResponse`](#brokapi-gateway-v1-executionsresponse) |  |
| `SubscribeExecutions` | [`SubscribeRequest`](#brokapi-gateway-v1-subscriberequest) | [`SubscribeResponse`](#brokapi-gateway-v1-subscriberesponse) |  |
| `SubscribePortfolio` | [`SubscribeRequest`](#brokapi-gateway-v1-subscriberequest) | [`SubscribeResponse`](#brokapi-gateway-v1-subscriberesponse) |  |

**Market data**

| Вызов | Запрос | Ответ | Описание |
|---|---|---|---|
| `OrderBook` | [`OrderBookRequest`](#brokapi-gateway-v1-orderbookrequest) | [`OrderBook`](#brokapi-gateway-v1-orderbook) |  |
| `LastTrades` | [`LastTradesRequest`](#brokapi-gateway-v1-lasttradesrequest) | [`LastTradesResponse`](#brokapi-gateway-v1-lasttradesresponse) |  |
| `Candles` | [`CandlesRequest`](#brokapi-gateway-v1-candlesrequest) | [`CandlesResponse`](#brokapi-gateway-v1-candlesresponse) |  |
| `SubscribeOrderBook` | [`SubscribeInstrumentRequest`](#brokapi-gateway-v1-subscribeinstrumentrequest) | [`SubscribeResponse`](#brokapi-gateway-v1-subscriberesponse) |  |
| `SubscribeMarketTrades` | [`SubscribeInstrumentRequest`](#brokapi-gateway-v1-subscribeinstrumentrequest) | [`SubscribeResponse`](#brokapi-gateway-v1-subscriberesponse) |  |
| `SubscribeQuotes` | [`SubscribeInstrumentRequest`](#brokapi-gateway-v1-subscribeinstrumentrequest) | [`SubscribeResponse`](#brokapi-gateway-v1-subscriberesponse) |  |
| `SubscribeCandles` | [`SubscribeCandlesRequest`](#brokapi-gateway-v1-subscribecandlesrequest) | [`SubscribeResponse`](#brokapi-gateway-v1-subscriberesponse) |  |
| `Unsubscribe` | [`UnsubscribeRequest`](#brokapi-gateway-v1-unsubscriberequest) | [`UnsubscribeResponse`](#brokapi-gateway-v1-unsubscriberesponse) | Lets go of a subscription this bot holds. The same request of two bots is one subscription at the broker with two owners: it is closed when the last owner lets go, and a bot that does not hold it gets the refusal `not_your_subscription`. The subscriptions of a bot are also let go when the bot is gone (its event stream ended and did not come back within the grace period of the gateway). The events of a subscription go to every bot that reads the stream, whoever asked for it. |

**Instruments**

| Вызов | Запрос | Ответ | Описание |
|---|---|---|---|
| `ResolveInstrument` | [`ResolveInstrumentRequest`](#brokapi-gateway-v1-resolveinstrumentrequest) | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) |  |
| `Find` | [`FindRequest`](#brokapi-gateway-v1-findrequest) | [`FindResponse`](#brokapi-gateway-v1-findresponse) |  |
| `GetInstrument` | [`InstrumentRequest`](#brokapi-gateway-v1-instrumentrequest) | [`Instrument`](#brokapi-gateway-v1-instrument) |  |
| `TradingParams` | [`InstrumentRequest`](#brokapi-gateway-v1-instrumentrequest) | [`TradingParams`](#brokapi-gateway-v1-tradingparams) |  |
| `TradingStatus` | [`InstrumentRequest`](#brokapi-gateway-v1-instrumentrequest) | [`TradingStatus`](#brokapi-gateway-v1-tradingstatus) |  |
| `Schedule` | [`ScheduleRequest`](#brokapi-gateway-v1-schedulerequest) | [`Schedule`](#brokapi-gateway-v1-schedule) |  |

**The rest**

| Вызов | Запрос | Ответ | Описание |
|---|---|---|---|
| `Raw` | [`RawRequest`](#brokapi-gateway-v1-rawrequest) | [`RawResponse`](#brokapi-gateway-v1-rawresponse) | Any method of the broker's own API by name, arguments and reply as JSON text. See the page of the broker. |
| `Caps` | [`CapsRequest`](#brokapi-gateway-v1-capsrequest) | [`CapsResponse`](#brokapi-gateway-v1-capsresponse) |  |
| `Health` | [`HealthRequest`](#brokapi-gateway-v1-healthrequest) | [`HealthResponse`](#brokapi-gateway-v1-healthresponse) | Works while the gateway starts: this is how a bot waits for it. |
| `Events` | [`EventsRequest`](#brokapi-gateway-v1-eventsrequest) | поток [`EventFrame`](#brokapi-gateway-v1-eventframe) | The events of the connection. See `EventsRequest`. |

<a id="brokapi-gateway-v1-cancelrequest"></a>

### CancelRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `cid` | `string` | 1 |  |

<a id="brokapi-gateway-v1-cancelresponse"></a>

### CancelResponse

Без полей.

<a id="brokapi-gateway-v1-orderstaterequest"></a>

### OrderStateRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `cid` | `string` | 1 |  |

<a id="brokapi-gateway-v1-activeordersrequest"></a>

### ActiveOrdersRequest

Без полей.

<a id="brokapi-gateway-v1-activeordersresponse"></a>

### ActiveOrdersResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `orders` | список [`OrderSnapshot`](#brokapi-gateway-v1-ordersnapshot) | 1 |  |

<a id="brokapi-gateway-v1-activestopordersresponse"></a>

### ActiveStopOrdersResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `stop_orders` | список [`StopOrderSnapshot`](#brokapi-gateway-v1-stopordersnapshot) | 1 |  |

<a id="brokapi-gateway-v1-reconcilerequest"></a>

### ReconcileRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `ledger` | список `string` | 1 | The ids of the orders you placed and have not seen end. |
| `include_journal` | `bool` | 2 | Add the orders of the gateway's own journal (all bots) to the ledger. Without it the orders of the other bots come back as `foreign`. |

<a id="brokapi-gateway-v1-cancelstoprequest"></a>

### CancelStopRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `stop` | [`StopOrderRef`](#brokapi-gateway-v1-stoporderref) | 1 |  |

<a id="brokapi-gateway-v1-accountrequest"></a>

### AccountRequest

Без полей.

<a id="brokapi-gateway-v1-positionsresponse"></a>

### PositionsResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `positions` | список [`Position`](#brokapi-gateway-v1-position) | 1 |  |

<a id="brokapi-gateway-v1-balancesresponse"></a>

### BalancesResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `balances` | список [`Balance`](#brokapi-gateway-v1-balance) | 1 |  |

<a id="brokapi-gateway-v1-executionsrequest"></a>

### ExecutionsRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `range` | [`TimeRange`](#brokapi-gateway-v1-timerange) | 1 |  |

<a id="brokapi-gateway-v1-executionsresponse"></a>

### ExecutionsResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `executions` | список [`Execution`](#brokapi-gateway-v1-execution) | 1 |  |

<a id="brokapi-gateway-v1-orderbookrequest"></a>

### OrderBookRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `depth` | `uint32` | 2 |  |

<a id="brokapi-gateway-v1-lasttradesrequest"></a>

### LastTradesRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `limit` | `uint32` | 2 |  |

<a id="brokapi-gateway-v1-lasttradesresponse"></a>

### LastTradesResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `trades` | список [`MarketTrade`](#brokapi-gateway-v1-markettrade) | 1 |  |

<a id="brokapi-gateway-v1-candlesrequest"></a>

### CandlesRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `timeframe` | [`Timeframe`](#brokapi-gateway-v1-timeframe) | 2 |  |
| `range` | [`TimeRange`](#brokapi-gateway-v1-timerange) | 3 |  |

<a id="brokapi-gateway-v1-candlesresponse"></a>

### CandlesResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `candles` | список [`Candle`](#brokapi-gateway-v1-candle) | 1 |  |

<a id="brokapi-gateway-v1-subscriberequest"></a>

### SubscribeRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `opts` | [`SubscribeOpts`](#brokapi-gateway-v1-subscribeopts) | 1 | Used by SubscribePortfolio. SubscribeOrderState, SubscribeStopOrders and SubscribeExecutions do not use it: the gateway holds one such subscription for all bots, and every bot gets each event of it once, whatever it sends here. |

<a id="brokapi-gateway-v1-subscribeinstrumentrequest"></a>

### SubscribeInstrumentRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `opts` | [`SubscribeOpts`](#brokapi-gateway-v1-subscribeopts) | 2 |  |

<a id="brokapi-gateway-v1-subscribecandlesrequest"></a>

### SubscribeCandlesRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `timeframe` | [`Timeframe`](#brokapi-gateway-v1-timeframe) | 2 |  |
| `opts` | [`SubscribeOpts`](#brokapi-gateway-v1-subscribeopts) | 3 |  |

<a id="brokapi-gateway-v1-subscriberesponse"></a>

### SubscribeResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `sub` | `uint64` | 1 | The id that marks the events of this subscription (`Event.sub`). |

<a id="brokapi-gateway-v1-unsubscriberequest"></a>

### UnsubscribeRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `sub` | `uint64` | 1 |  |

<a id="brokapi-gateway-v1-unsubscriberesponse"></a>

### UnsubscribeResponse

Без полей.

<a id="brokapi-gateway-v1-resolveinstrumentrequest"></a>

### ResolveInstrumentRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `ticker` | `string` | 1 |  |

<a id="brokapi-gateway-v1-findrequest"></a>

### FindRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `query` | [`InstrumentQuery`](#brokapi-gateway-v1-instrumentquery) | 1 |  |

<a id="brokapi-gateway-v1-findresponse"></a>

### FindResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instruments` | список [`Instrument`](#brokapi-gateway-v1-instrument) | 1 |  |

<a id="brokapi-gateway-v1-instrumentrequest"></a>

### InstrumentRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |

<a id="brokapi-gateway-v1-schedulerequest"></a>

### ScheduleRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `exchange` | `string` | 1 |  |
| `range` | [`TimeRange`](#brokapi-gateway-v1-timerange) | 2 |  |

<a id="brokapi-gateway-v1-rawrequest"></a>

### RawRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `method` | `string` | 1 | The name of the method of the broker's API, as the broker names it. |
| `args_json` | `string` | 2 | The arguments as a JSON object, empty for none. |
| `command` | `bool` | 3 | True when the call changes something at the broker: a lost reply is then `Unknown` and the call is not repeated. |

<a id="brokapi-gateway-v1-rawresponse"></a>

### RawResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `reply_json` | `string` | 1 |  |

<a id="brokapi-gateway-v1-capsrequest"></a>

### CapsRequest

Без полей.

<a id="brokapi-gateway-v1-capsresponse"></a>

### CapsResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `md` | [`MarketDataCaps`](#brokapi-gateway-v1-marketdatacaps) | 1 |  |
| `ud` | [`UserDataCaps`](#brokapi-gateway-v1-userdatacaps) | 2 |  |
| `candle_spans` | список [`CandleSpan`](#brokapi-gateway-v1-candlespan) | 3 | The longest span one Candles request may cover, per timeframe, as the broker documents it. A timeframe that is not listed has no known limit. A client that reads a long history cuts it into windows of this span. |

<a id="brokapi-gateway-v1-candlespan"></a>

### CandleSpan

| Поле | Тип | № | Описание |
|---|---|---|---|
| `timeframe` | [`Timeframe`](#brokapi-gateway-v1-timeframe) | 1 |  |
| `span_ns` | `uint64` | 2 |  |

<a id="brokapi-gateway-v1-healthrequest"></a>

### HealthRequest

Без полей.

<a id="brokapi-gateway-v1-botinfo"></a>

### BotInfo

| Поле | Тип | № | Описание |
|---|---|---|---|
| `name` | `string` | 1 |  |
| `first_seen_ns` | `int64` | 2 | When the gateway first heard from the bot (since the gateway started). |
| `last_seen_ns` | `int64` | 3 |  |
| `calls` | `uint64` | 4 |  |
| `streaming` | `bool` | 5 | The bot has an `Events` stream open. |
| `queued` | `uint64` | 6 | Events waiting in the buffer of its stream. |
| `lagged` | `uint64` | 7 | How many times its stream was closed because the bot read too slowly. |

<a id="brokapi-gateway-v1-healthresponse"></a>

### HealthResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `state` | [`GatewayState`](#brokapi-gateway-v1-gatewaystate) | 1 |  |
| `name` | `string` | 2 | The name of the gateway and the broker it serves. |
| `broker` | `string` | 3 |  |
| `version` | `string` | 4 |  |
| `epoch` | `uint64` | 5 | Chosen at random each time the gateway starts. A bot keeps it with its last event number. |
| `last_seq` | `uint64` | 6 | The number of the last event. |
| `detail` | `string` | 7 | While STARTING: what the gateway is doing or what went wrong the last time. |
| `open_orders` | `uint64` | 8 | Orders of the journal that are not known to have ended. |
| `bots` | список [`BotInfo`](#brokapi-gateway-v1-botinfo) | 9 |  |
| `uptime_ns` | `uint64` | 10 | How long this run of the gateway has been up. |
| `pid` | `uint32` | 11 | The process id of the gateway. |
| `events_total` | `uint64` | 12 | The number of events since the start, and how many per second over the last ten seconds. |
| `events_per_sec` | `double` | 13 |  |
| `limits` | список [`LimitInfo`](#brokapi-gateway-v1-limitinfo) | 14 | How much of each bucket of the rate limit of the broker is left. |
| `subscriptions` | список [`SubscriptionInfo`](#brokapi-gateway-v1-subscriptioninfo) | 15 | The subscriptions at the broker and who holds them. |

<a id="brokapi-gateway-v1-limitinfo"></a>

### LimitInfo

One bucket of the rate limit of the broker.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `bucket` | `string` | 1 |  |
| `available` | `uint32` | 2 |  |
| `capacity` | `uint32` | 3 |  |

<a id="brokapi-gateway-v1-subscriptioninfo"></a>

### SubscriptionInfo

A subscription at the broker. The same request of several bots is one subscription with several owners. Only an owner can close it. The gateway's own subscriptions (the states of orders) have the owner "(gateway)".

| Поле | Тип | № | Описание |
|---|---|---|---|
| `sub` | `uint64` | 1 |  |
| `what` | `string` | 2 | What it is: the kind and the instrument. |
| `owners` | список `string` | 3 |  |

<a id="brokapi-gateway-v1-eventsrequest"></a>

### EventsRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `epoch` | `uint64` | 1 | Where to continue. Without `epoch` the stream is live only. |
| `last_seq` | `uint64` | 2 | The number of the last event you handled. |
| `own_orders_only` | `bool` | 3 | Orders and executions of the orders that this bot placed (by the journal), not those of the other bots. Everything else comes to all. |

<a id="brokapi-gateway-v1-eventframe"></a>

### EventFrame

A frame of the `Events` stream. The first is always `ready`.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `ready` | [`Ready`](#brokapi-gateway-v1-ready) | 1 | Одно из группы `frame`. |
| `event` | [`Event`](#brokapi-gateway-v1-event) | 2 | Одно из группы `frame`. |
| `gap` | [`Gap`](#brokapi-gateway-v1-gap) | 3 | Одно из группы `frame`. |
| `lagged` | [`Lagged`](#brokapi-gateway-v1-lagged) | 4 | Одно из группы `frame`. |

<a id="brokapi-gateway-v1-ready"></a>

### Ready

| Поле | Тип | № | Описание |
|---|---|---|---|
| `epoch` | `uint64` | 1 | The epoch of this gateway run and the number the next live event will have. |
| `next_seq` | `uint64` | 2 |  |
| `replaying` | `uint64` | 3 | How many events follow from the ring before the live ones begin. |

<a id="brokapi-gateway-v1-gap"></a>

### Gap

The events after `last_seq` are no longer there. Call Reconcile and read the account again: the stream goes on from now.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `reason` | [`Reason`](#brokapi-gateway-v1-gap-reason) | 1 |  |
| `requested_after` | `uint64` | 2 |  |
| `first_available` | `uint64` | 3 | The oldest event still in the ring (the next one to come if the ring is empty). |

<a id="brokapi-gateway-v1-lagged"></a>

### Lagged

The stream is closed because this bot read too slowly and its buffer filled. The events in the buffer were delivered first. Reconnect with the last number you got.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `last_seq` | `uint64` | 1 |  |

<a id="brokapi-gateway-v1-errordetail"></a>

### ErrorDetail

The error of the library, whole. Sent in the `brokapi-error-bin` trailer of a failed call.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `kind` | [`Kind`](#brokapi-gateway-v1-errordetail-kind) | 1 |  |
| `code` | `string` | 2 | REJECTED: the code of the refusal ("not_found", "price_off_step", ...). |
| `message` | `string` | 3 | CONFIG, TRANSPORT, REJECTED, GATEWAY_STARTING, BAD_REQUEST: the text. |
| `retry_after_ns` | `uint64` | 4 | RATE_LIMITED. |
| `capability` | `string` | 5 | UNSUPPORTED. |

<a id="brokapi-gateway-v1-gatewaystate"></a>

### GatewayState

| Значение | Число | Описание |
|---|---|---|
| `GATEWAY_STATE_UNSPECIFIED` | 0 |  |
| `GATEWAY_STATE_STARTING` | 1 | The gateway runs the reconcile of its journal. Every call but Health and Caps is answered UNAVAILABLE. |
| `GATEWAY_STATE_READY` | 2 |  |
| `GATEWAY_STATE_STOPPING` | 3 | The gateway is stopping: no new commands. |

<a id="brokapi-gateway-v1-gap-reason"></a>

### Gap.Reason

| Значение | Число | Описание |
|---|---|---|
| `REASON_UNSPECIFIED` | 0 |  |
| `REASON_TOO_OLD` | 1 | The ring has already dropped them (by count or by age). |
| `REASON_NEW_EPOCH` | 2 | The gateway was restarted: its numbers begin again. |
| `REASON_AHEAD` | 3 | The number is ahead of the gateway. |

<a id="brokapi-gateway-v1-errordetail-kind"></a>

### ErrorDetail.Kind

| Значение | Число | Описание |
|---|---|---|
| `KIND_UNSPECIFIED` | 0 |  |
| `KIND_CONFIG` | 1 |  |
| `KIND_TRANSPORT` | 2 |  |
| `KIND_RATE_LIMITED` | 3 |  |
| `KIND_REJECTED` | 4 |  |
| `KIND_UNKNOWN` | 5 | The command may or may not have reached the broker: look at the order before anything else. |
| `KIND_UNSUPPORTED` | 6 |  |
| `KIND_NOT_RECONCILED` | 7 |  |
| `KIND_DEGRADED` | 8 |  |
| `KIND_SHUTTING_DOWN` | 9 |  |
| `KIND_GATEWAY_STARTING` | 10 | The gateway is not ready yet. The command was not run. |
| `KIND_BAD_REQUEST` | 11 | The request could not be read: a bad id, a bad number. The command was not run. |

## Типы: значения, заявки, портфель, рынок

Файл `proto/brokapi/gateway/v1/types.proto`, пакет `brokapi.gateway.v1`.

```text
The types of the brokapi gateway: the same values as the types of `brokapi-core`, written for the wire.

Rules that hold for every message in this file:

  * A price, a quantity, an amount of money is a string with a decimal number ("101.25", "-0.5"). Never a double:
    a double cannot hold 0.1 and a bot that adds prices loses kopecks. Parse it into a decimal type of your language.
  * A moment of time is an integer of nanoseconds since 1970-01-01 UTC, in a field that ends with `_ns`.
  * An order id (`cid`) is a UUID in the text form with hyphens, made by the bot.
  * `optional` marks a value that may be absent: absent is not the same as an empty string or zero.
```

<a id="brokapi-gateway-v1-timestamps"></a>

### Timestamps

When the library saw something. `broker_ts_ns` is the broker's own time when the broker gives one.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `rx_ts_ns` | `int64` | 1 |  |

<a id="brokapi-gateway-v1-timerange"></a>

### TimeRange

| Поле | Тип | № | Описание |
|---|---|---|---|
| `from_ns` | `int64` | 1 |  |
| `to_ns` | `int64` | 2 |  |

<a id="brokapi-gateway-v1-instrumentid"></a>

### InstrumentId

An instrument: the same in every broker (`symbol` and `mic`) and the way this broker names it (`native`). Take it from `ResolveInstrument`, `Find` or `GetInstrument` and pass it back as it is.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `symbol` | `string` | 1 |  |
| `mic` | `string` | 2 |  |
| `native` | [`NativeId`](#brokapi-gateway-v1-nativeid) | 3 |  |

<a id="brokapi-gateway-v1-nativeid"></a>

### NativeId

| Поле | Тип | № | Описание |
|---|---|---|---|
| `symbol_mic` | [`SymbolMic`](#brokapi-gateway-v1-symbolmic) | 1 | Одно из группы `id`. |
| `ticker_class` | [`TickerClass`](#brokapi-gateway-v1-tickerclass) | 2 | Одно из группы `id`. |
| `figi` | `string` | 3 | Одно из группы `id`. |
| `uid` | `string` | 4 | Одно из группы `id`. |

<a id="brokapi-gateway-v1-symbolmic"></a>

### SymbolMic

| Поле | Тип | № | Описание |
|---|---|---|---|
| `symbol` | `string` | 1 |  |
| `mic` | `string` | 2 |  |

<a id="brokapi-gateway-v1-tickerclass"></a>

### TickerClass

| Поле | Тип | № | Описание |
|---|---|---|---|
| `ticker` | `string` | 1 |  |
| `class_code` | `string` | 2 |  |

<a id="brokapi-gateway-v1-subscribeopts"></a>

### SubscribeOpts

| Поле | Тип | № | Описание |
|---|---|---|---|
| `backpressure` | [`Backpressure`](#brokapi-gateway-v1-backpressure) | 1 |  |
| `cap` | `uint64` | 2 | The buffer size for LOSSLESS and DROP_NEWEST. |

<a id="brokapi-gateway-v1-placeorderrequest"></a>

### PlaceOrderRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `cid` | `string` | 1 |  |
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 2 |  |
| `side` | [`Side`](#brokapi-gateway-v1-side) | 3 |  |
| `order_type` | [`OrderType`](#brokapi-gateway-v1-ordertype) | 4 |  |
| `qty` | `string` | 5 |  |

<a id="brokapi-gateway-v1-replaceorderrequest"></a>

### ReplaceOrderRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `cid` | `string` | 1 | The id of the new order. |
| `original_cid` | `string` | 2 |  |

<a id="brokapi-gateway-v1-orderack"></a>

### OrderAck

| Поле | Тип | № | Описание |
|---|---|---|---|
| `cid` | `string` | 1 |  |
| `broker_id` | `string` | 2 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 3 |  |

<a id="brokapi-gateway-v1-orderstate"></a>

### OrderState

| Поле | Тип | № | Описание |
|---|---|---|---|
| `kind` | [`OrderStateKind`](#brokapi-gateway-v1-orderstatekind) | 1 |  |
| `cid` | `string` | 2 |  |
| `broker_id` | `string` | 3 | ACKED. |
| `filled` | `string` | 4 | PARTIALLY_FILLED. |
| `left` | `string` | 5 |  |
| `reason` | `string` | 6 | REJECTED. |

<a id="brokapi-gateway-v1-ordersnapshot"></a>

### OrderSnapshot

| Поле | Тип | № | Описание |
|---|---|---|---|
| `cid` | `string` | 1 |  |
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 3 |  |
| `state` | [`OrderState`](#brokapi-gateway-v1-orderstate) | 4 |  |
| `side` | [`Side`](#brokapi-gateway-v1-side) | 5 |  |
| `order_type` | [`OrderType`](#brokapi-gateway-v1-ordertype) | 6 |  |
| `qty` | `string` | 7 |  |
| `filled` | `string` | 8 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 10 |  |

<a id="brokapi-gateway-v1-reconcilereport"></a>

### ReconcileReport

| Поле | Тип | № | Описание |
|---|---|---|---|
| `confirmed` | список [`OrderSnapshot`](#brokapi-gateway-v1-ordersnapshot) | 1 | In your ledger and active at the broker. |
| `closed_offline` | список `string` | 2 | In your ledger, not active at the broker: filled, cancelled or refused while you were away. Ask OrderState. |
| `foreign` | список [`OrderSnapshot`](#brokapi-gateway-v1-ordersnapshot) | 3 | Active at the broker, not in your ledger: somebody else's orders. Do not touch them. |

<a id="brokapi-gateway-v1-stopexpiry"></a>

### StopExpiry

| Поле | Тип | № | Описание |
|---|---|---|---|
| `kind` | [`Kind`](#brokapi-gateway-v1-stopexpiry-kind) | 1 |  |
| `date_ns` | `int64` | 2 | KIND_GOOD_TILL_DATE. |

<a id="brokapi-gateway-v1-placestoporderrequest"></a>

### PlaceStopOrderRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `cid` | `string` | 1 |  |
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 2 |  |
| `side` | [`Side`](#brokapi-gateway-v1-side) | 3 |  |
| `kind` | [`StopOrderKind`](#brokapi-gateway-v1-stoporderkind) | 4 |  |
| `qty` | `string` | 5 |  |
| `stop_price` | `string` | 6 |  |
| `expiry` | [`StopExpiry`](#brokapi-gateway-v1-stopexpiry) | 8 |  |

<a id="brokapi-gateway-v1-stoporderstate"></a>

### StopOrderState

| Поле | Тип | № | Описание |
|---|---|---|---|
| `kind` | [`Kind`](#brokapi-gateway-v1-stoporderstate-kind) | 1 |  |
| `other` | `string` | 2 |  |

<a id="brokapi-gateway-v1-stoporderref"></a>

### StopOrderRef

A stop order by your id or by the broker's id.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `cid` | `string` | 1 | Одно из группы `ref`. |
| `broker_id` | `string` | 2 | Одно из группы `ref`. |

<a id="brokapi-gateway-v1-stopordersnapshot"></a>

### StopOrderSnapshot

| Поле | Тип | № | Описание |
|---|---|---|---|
| `broker_id` | `string` | 2 |  |
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 3 |  |
| `side` | [`Side`](#brokapi-gateway-v1-side) | 4 |  |
| `kind` | [`StopOrderKind`](#brokapi-gateway-v1-stoporderkind) | 5 |  |
| `qty` | `string` | 6 |  |
| `stop_price` | `string` | 7 |  |
| `expiry` | [`StopExpiry`](#brokapi-gateway-v1-stopexpiry) | 9 |  |
| `state` | [`StopOrderState`](#brokapi-gateway-v1-stoporderstate) | 10 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 11 |  |

<a id="brokapi-gateway-v1-booklevel"></a>

### BookLevel

| Поле | Тип | № | Описание |
|---|---|---|---|
| `price` | `string` | 1 |  |
| `qty` | `string` | 2 |  |

<a id="brokapi-gateway-v1-orderbook"></a>

### OrderBook

A full book (`incremental` false) or a change of it (`incremental` true, then `seq` is the number of the change).

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `incremental` | `bool` | 2 |  |
| `seq` | `uint64` | 3 |  |
| `bids` | список [`BookLevel`](#brokapi-gateway-v1-booklevel) | 4 |  |
| `asks` | список [`BookLevel`](#brokapi-gateway-v1-booklevel) | 5 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 6 |  |

<a id="brokapi-gateway-v1-markettrade"></a>

### MarketTrade

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `price` | `string` | 2 |  |
| `qty` | `string` | 3 |  |
| `side` | [`Side`](#brokapi-gateway-v1-side) | 4 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 6 |  |

<a id="brokapi-gateway-v1-quote"></a>

### Quote

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 4 |  |

<a id="brokapi-gateway-v1-candle"></a>

### Candle

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `timeframe` | [`Timeframe`](#brokapi-gateway-v1-timeframe) | 2 |  |
| `open` | `string` | 3 |  |
| `high` | `string` | 4 |  |
| `low` | `string` | 5 |  |
| `close` | `string` | 6 |  |
| `volume` | `string` | 7 |  |
| `open_ts_ns` | `int64` | 8 |  |
| `close_ts_ns` | `int64` | 9 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 10 |  |

<a id="brokapi-gateway-v1-balance"></a>

### Balance

| Поле | Тип | № | Описание |
|---|---|---|---|
| `currency` | `string` | 1 |  |
| `available` | `string` | 2 |  |
| `blocked` | `string` | 3 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 4 |  |

<a id="brokapi-gateway-v1-position"></a>

### Position

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `qty` | `string` | 2 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 5 |  |

<a id="brokapi-gateway-v1-portfolio"></a>

### Portfolio

| Поле | Тип | № | Описание |
|---|---|---|---|
| `balances` | список [`Balance`](#brokapi-gateway-v1-balance) | 1 |  |
| `positions` | список [`Position`](#brokapi-gateway-v1-position) | 2 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 3 |  |

<a id="brokapi-gateway-v1-execution"></a>

### Execution

| Поле | Тип | № | Описание |
|---|---|---|---|
| `broker_order_id` | `string` | 2 |  |
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 3 |  |
| `side` | [`Side`](#brokapi-gateway-v1-side) | 4 |  |
| `price` | `string` | 5 |  |
| `qty` | `string` | 6 |  |
| `commission` | `string` | 7 |  |
| `trade_id` | `string` | 8 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 9 |  |

<a id="brokapi-gateway-v1-instrument"></a>

### Instrument

| Поле | Тип | № | Описание |
|---|---|---|---|
| `id` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `name` | `string` | 2 |  |
| `kind` | [`InstrumentKind`](#brokapi-gateway-v1-instrumentkind) | 4 |  |
| `currency` | `string` | 5 |  |
| `exchange` | `string` | 6 |  |

<a id="brokapi-gateway-v1-instrumentquery"></a>

### InstrumentQuery

| Поле | Тип | № | Описание |
|---|---|---|---|
| `by_ticker` | [`ByTicker`](#brokapi-gateway-v1-instrumentquery-byticker) | 1 | Одно из группы `query`. |
| `by_isin` | `string` | 2 | Одно из группы `query`. |
| `by_kind` | [`InstrumentKind`](#brokapi-gateway-v1-instrumentkind) | 3 | Одно из группы `query`. |

<a id="brokapi-gateway-v1-instrumentquery-byticker"></a>

### InstrumentQuery.ByTicker

| Поле | Тип | № | Описание |
|---|---|---|---|
| `ticker` | `string` | 1 |  |

<a id="brokapi-gateway-v1-tradingparams"></a>

### TradingParams

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `lot_size` | `string` | 2 |  |
| `min_qty` | `string` | 3 |  |
| `price_step` | `string` | 4 |  |
| `price_step_value` | `string` | 5 |  |
| `currency` | `string` | 6 |  |

<a id="brokapi-gateway-v1-tradingstatus"></a>

### TradingStatus

| Поле | Тип | № | Описание |
|---|---|---|---|
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 1 |  |
| `availability` | [`TradingAvailability`](#brokapi-gateway-v1-tradingavailability) | 2 |  |

<a id="brokapi-gateway-v1-sessioninterval"></a>

### SessionInterval

| Поле | Тип | № | Описание |
|---|---|---|---|
| `start_ns` | `int64` | 1 |  |
| `end_ns` | `int64` | 2 |  |
| `kind` | [`SessionKind`](#brokapi-gateway-v1-sessionkind) | 3 |  |

<a id="brokapi-gateway-v1-schedule"></a>

### Schedule

| Поле | Тип | № | Описание |
|---|---|---|---|
| `exchange` | `string` | 1 |  |
| `sessions` | список [`SessionInterval`](#brokapi-gateway-v1-sessioninterval) | 2 |  |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 3 |  |

<a id="brokapi-gateway-v1-streamingcaps"></a>

### StreamingCaps

| Поле | Тип | № | Описание |
|---|---|---|---|
| `order_book` | `bool` | 1 |  |
| `market_trades` | `bool` | 2 |  |
| `quotes` | `bool` | 3 |  |
| `candles` | `bool` | 4 |  |

<a id="brokapi-gateway-v1-marketdatacaps"></a>

### MarketDataCaps

| Поле | Тип | № | Описание |
|---|---|---|---|
| `max_order_book_depth` | `uint32` | 1 |  |
| `incremental_order_book` | `bool` | 2 |  |
| `streaming` | [`StreamingCaps`](#brokapi-gateway-v1-streamingcaps) | 3 |  |

<a id="brokapi-gateway-v1-placeidempotency"></a>

### PlaceIdempotency

| Поле | Тип | № | Описание |
|---|---|---|---|
| `kind` | [`Kind`](#brokapi-gateway-v1-placeidempotency-kind) | 1 |  |
| `window_secs` | `uint64` | 2 | How long the broker remembers an id (the two last kinds). |

<a id="brokapi-gateway-v1-userdatacaps"></a>

### UserDataCaps

| Поле | Тип | № | Описание |
|---|---|---|---|
| `place_idempotency` | [`PlaceIdempotency`](#brokapi-gateway-v1-placeidempotency) | 1 |  |
| `ws_rpc` | `bool` | 2 |  |
| `ordered_command_channel` | `bool` | 3 |  |
| `supports_replace` | `bool` | 4 |  |
| `supports_stop_orders` | `bool` | 5 |  |
| `streams_executions` | `bool` | 6 |  |
| `sandbox` | `bool` | 7 |  |

<a id="brokapi-gateway-v1-positionlist"></a>

### PositionList

| Поле | Тип | № | Описание |
|---|---|---|---|
| `positions` | список [`Position`](#brokapi-gateway-v1-position) | 1 |  |

<a id="brokapi-gateway-v1-balancelist"></a>

### BalanceList

| Поле | Тип | № | Описание |
|---|---|---|---|
| `balances` | список [`Balance`](#brokapi-gateway-v1-balance) | 1 |  |

<a id="brokapi-gateway-v1-resync"></a>

### Resync

| Поле | Тип | № | Описание |
|---|---|---|---|
| `sub` | `uint64` | 1 |  |
| `instrument` | [`InstrumentId`](#brokapi-gateway-v1-instrumentid) | 2 |  |
| `reason` | [`ResyncReason`](#brokapi-gateway-v1-resyncreason) | 3 |  |

<a id="brokapi-gateway-v1-overflow"></a>

### Overflow

| Поле | Тип | № | Описание |
|---|---|---|---|
| `sub` | `uint64` | 1 |  |
| `lost_from_ns` | `int64` | 2 |  |

<a id="brokapi-gateway-v1-reconnected"></a>

### Reconnected

| Поле | Тип | № | Описание |
|---|---|---|---|
| `sub` | `uint64` | 1 |  |
| `gap_from_ns` | `int64` | 2 |  |

<a id="brokapi-gateway-v1-event"></a>

### Event

One event of the broker connection. `seq` counts the events of this gateway from 1, without holes, in the order the gateway got them: keep the last one you handled and give it to `Events` when you reconnect.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `seq` | `uint64` | 1 |  |
| `source` | `string` | 2 | The connector that produced it: "NAME-md" or "NAME-ud", NAME being the name of the gateway. |
| `ts` | [`Timestamps`](#brokapi-gateway-v1-timestamps) | 4 |  |
| `order_book` | [`OrderBook`](#brokapi-gateway-v1-orderbook) | 10 | Одно из группы `payload`. |
| `market_trade` | [`MarketTrade`](#brokapi-gateway-v1-markettrade) | 11 | Одно из группы `payload`. |
| `quote` | [`Quote`](#brokapi-gateway-v1-quote) | 12 | Одно из группы `payload`. |
| `candle` | [`Candle`](#brokapi-gateway-v1-candle) | 13 | Одно из группы `payload`. |
| `order` | [`OrderSnapshot`](#brokapi-gateway-v1-ordersnapshot) | 14 | Одно из группы `payload`. Your order changed state. |
| `execution` | [`Execution`](#brokapi-gateway-v1-execution) | 15 | Одно из группы `payload`. |
| `portfolio` | [`Portfolio`](#brokapi-gateway-v1-portfolio) | 16 | Одно из группы `payload`. |
| `positions` | [`PositionList`](#brokapi-gateway-v1-positionlist) | 17 | Одно из группы `payload`. |
| `balances` | [`BalanceList`](#brokapi-gateway-v1-balancelist) | 18 | Одно из группы `payload`. |
| `resync` | [`Resync`](#brokapi-gateway-v1-resync) | 19 | Одно из группы `payload`. A stream of order book changes lost its place: ask for a snapshot. |
| `overflow` | [`Overflow`](#brokapi-gateway-v1-overflow) | 20 | Одно из группы `payload`. A lossless subscription of the connector overflowed. Trading stops until you call Reconcile. |
| `reconnected` | [`Reconnected`](#brokapi-gateway-v1-reconnected) | 21 | Одно из группы `payload`. A stream was cut and came back: whatever happened from `gap_from_ns` may be missing. |

<a id="brokapi-gateway-v1-side"></a>

### Side

| Значение | Число | Описание |
|---|---|---|
| `SIDE_UNSPECIFIED` | 0 |  |
| `SIDE_BUY` | 1 |  |
| `SIDE_SELL` | 2 |  |

<a id="brokapi-gateway-v1-ordertype"></a>

### OrderType

| Значение | Число | Описание |
|---|---|---|
| `ORDER_TYPE_UNSPECIFIED` | 0 |  |
| `ORDER_TYPE_MARKET` | 1 |  |
| `ORDER_TYPE_LIMIT` | 2 |  |

<a id="brokapi-gateway-v1-timeframe"></a>

### Timeframe

| Значение | Число | Описание |
|---|---|---|
| `TIMEFRAME_UNSPECIFIED` | 0 |  |
| `TIMEFRAME_M1` | 1 |  |
| `TIMEFRAME_M5` | 2 |  |
| `TIMEFRAME_M10` | 3 |  |
| `TIMEFRAME_M15` | 4 |  |
| `TIMEFRAME_M30` | 5 |  |
| `TIMEFRAME_H1` | 6 |  |
| `TIMEFRAME_H4` | 7 |  |
| `TIMEFRAME_D1` | 8 |  |
| `TIMEFRAME_W1` | 9 |  |
| `TIMEFRAME_MN` | 10 |  |

<a id="brokapi-gateway-v1-instrumentkind"></a>

### InstrumentKind

| Значение | Число | Описание |
|---|---|---|
| `INSTRUMENT_KIND_UNSPECIFIED` | 0 |  |
| `INSTRUMENT_KIND_STOCK` | 1 |  |
| `INSTRUMENT_KIND_BOND` | 2 |  |
| `INSTRUMENT_KIND_OPTION` | 3 |  |
| `INSTRUMENT_KIND_FUTURE` | 4 |  |
| `INSTRUMENT_KIND_ETF` | 5 |  |
| `INSTRUMENT_KIND_MUTUAL_FUND` | 6 |  |
| `INSTRUMENT_KIND_CURRENCY` | 7 |  |
| `INSTRUMENT_KIND_OTHER` | 8 |  |
| `INSTRUMENT_KIND_PERPETUAL` | 9 |  |

<a id="brokapi-gateway-v1-tradingavailability"></a>

### TradingAvailability

| Значение | Число | Описание |
|---|---|---|
| `TRADING_AVAILABILITY_UNSPECIFIED` | 0 |  |
| `TRADING_AVAILABILITY_OPEN` | 1 |  |
| `TRADING_AVAILABILITY_CLOSED` | 2 |  |
| `TRADING_AVAILABILITY_BREAK` | 3 |  |
| `TRADING_AVAILABILITY_UNKNOWN` | 4 |  |

<a id="brokapi-gateway-v1-sessionkind"></a>

### SessionKind

| Значение | Число | Описание |
|---|---|---|
| `SESSION_KIND_UNSPECIFIED` | 0 |  |
| `SESSION_KIND_PRE_MARKET` | 1 |  |
| `SESSION_KIND_MAIN` | 2 |  |
| `SESSION_KIND_POST_MARKET` | 3 |  |
| `SESSION_KIND_AUCTION` | 4 |  |
| `SESSION_KIND_OTHER` | 5 |  |

<a id="brokapi-gateway-v1-resyncreason"></a>

### ResyncReason

| Значение | Число | Описание |
|---|---|---|
| `RESYNC_REASON_UNSPECIFIED` | 0 |  |
| `RESYNC_REASON_SEQUENCE_GAP` | 1 |  |
| `RESYNC_REASON_SUBSCRIPTION_RESET` | 2 |  |
| `RESYNC_REASON_MANUAL` | 3 |  |

<a id="brokapi-gateway-v1-stoporderkind"></a>

### StopOrderKind

| Значение | Число | Описание |
|---|---|---|
| `STOP_ORDER_KIND_UNSPECIFIED` | 0 |  |
| `STOP_ORDER_KIND_STOP_LOSS` | 1 |  |
| `STOP_ORDER_KIND_TAKE_PROFIT` | 2 |  |
| `STOP_ORDER_KIND_STOP_LIMIT` | 3 |  |

<a id="brokapi-gateway-v1-backpressure"></a>

### Backpressure

| Значение | Число | Описание |
|---|---|---|
| `BACKPRESSURE_UNSPECIFIED` | 0 |  |
| `BACKPRESSURE_CONFLATE` | 1 | The latest value replaces older ones that nobody has read yet. For prices. |
| `BACKPRESSURE_LOSSLESS` | 2 | Every value is kept up to `cap`. When the buffer is full the stream is marked lost and trading stops until Reconcile. |
| `BACKPRESSURE_DROP_NEWEST` | 3 | At `cap` the newest value is dropped. |

<a id="brokapi-gateway-v1-orderstatekind"></a>

### OrderStateKind

| Значение | Число | Описание |
|---|---|---|
| `ORDER_STATE_KIND_UNSPECIFIED` | 0 |  |
| `ORDER_STATE_KIND_IN_FLIGHT` | 1 | Sent, no answer yet. |
| `ORDER_STATE_KIND_UNKNOWN` | 2 | No answer and the outcome is not known: ask with OrderState, do not send again. |
| `ORDER_STATE_KIND_ACKED` | 3 | Accepted by the broker (`broker_id`). |
| `ORDER_STATE_KIND_PARTIALLY_FILLED` | 4 | Partly executed (`filled`, `left`). |
| `ORDER_STATE_KIND_FILLED` | 5 |  |
| `ORDER_STATE_KIND_CANCELLED` | 6 |  |
| `ORDER_STATE_KIND_REJECTED` | 7 | Refused (`reason`). |

<a id="brokapi-gateway-v1-stopexpiry-kind"></a>

### StopExpiry.Kind

| Значение | Число | Описание |
|---|---|---|
| `KIND_UNSPECIFIED` | 0 |  |
| `KIND_GOOD_TILL_CANCEL` | 1 |  |
| `KIND_END_OF_DAY` | 2 |  |
| `KIND_GOOD_TILL_DATE` | 3 |  |

<a id="brokapi-gateway-v1-stoporderstate-kind"></a>

### StopOrderState.Kind

| Значение | Число | Описание |
|---|---|---|
| `KIND_UNSPECIFIED` | 0 |  |
| `KIND_ACTIVE` | 1 |  |
| `KIND_TRIGGERED` | 2 |  |
| `KIND_CANCELLED` | 3 |  |
| `KIND_EXPIRED` | 4 |  |
| `KIND_OTHER` | 5 | A state this version does not know: the broker's word is in `other`. |

<a id="brokapi-gateway-v1-placeidempotency-kind"></a>

### PlaceIdempotency.Kind

| Значение | Число | Описание |
|---|---|---|
| `KIND_UNSPECIFIED` | 0 |  |
| `KIND_LABEL_ONLY` | 1 | The broker only keeps the label: a second place with the same id may open a second order. |
| `KIND_REJECT_DUPLICATE` | 2 |  |
| `KIND_IDEMPOTENT` | 3 |  |

## Service: служба `brokapi up` и ее команды

Файл `proto/brokapi/service/v1/service.proto`, пакет `brokapi.service.v1`.

```text
The API of the brokapi service: the commands of the program `brokapi` talk to it. The service runs the gateways of one
config file, one process each, and watches them.

It is for the person who runs the machine, not for bots: bots talk to a gateway (brokapi.gateway.v1). The service
listens on a Unix socket (or on 127.0.0.1 on Windows) in its state directory, and a call carries the key from
`service.key` of that directory in `authorization: Bearer <key>`, as a call to a gateway does.
```

### Сервис Service

<a id="brokapi-service-v1-service"></a>

| Вызов | Запрос | Ответ | Описание |
|---|---|---|---|
| `Info` | [`InfoRequest`](#brokapi-service-v1-inforequest) | [`ServiceInfo`](#brokapi-service-v1-serviceinfo) | What the service is. |
| `List` | [`ListRequest`](#brokapi-service-v1-listrequest) | [`ListResponse`](#brokapi-service-v1-listresponse) | The gateways, with what the service knows of each (the state of the process) and what the gateway says of itself. Without names: all of them. |
| `Start` | [`GatewayRef`](#brokapi-service-v1-gatewayref) | [`GatewayInfo`](#brokapi-service-v1-gatewayinfo) | Starts a gateway that is stopped or failed. A gateway that runs is left alone. |
| `Stop` | [`StopRequest`](#brokapi-service-v1-stoprequest) | [`StopResponse`](#brokapi-service-v1-stopresponse) | Stops a gateway and keeps it stopped until `Start`. With `cancel_orders` the active orders and stop orders of the account are cancelled first. Orders at the exchange are otherwise not touched. |
| `Restart` | [`GatewayRef`](#brokapi-service-v1-gatewayref) | [`GatewayInfo`](#brokapi-service-v1-gatewayinfo) | Stops a gateway and starts it again. |
| `Add` | [`GatewayRef`](#brokapi-service-v1-gatewayref) | [`GatewayInfo`](#brokapi-service-v1-gatewayinfo) | Reads the config file again and starts the gateway of this name from it, if it is not running. |
| `Remove` | [`GatewayRef`](#brokapi-service-v1-gatewayref) | [`GatewayInfo`](#brokapi-service-v1-gatewayinfo) | Stops a gateway and forgets it until `Add` or `Reload` brings it back from the config. |
| `Reload` | [`ReloadRequest`](#brokapi-service-v1-reloadrequest) | [`ReloadResponse`](#brokapi-service-v1-reloadresponse) | Reads the config file again and brings the running gateways in line with it: starts the new ones, stops the ones that are gone from the file, restarts the ones whose section changed. |
| `Shutdown` | [`ShutdownRequest`](#brokapi-service-v1-shutdownrequest) | [`ShutdownResponse`](#brokapi-service-v1-shutdownresponse) | Stops the service. The gateways are stopped too, unless `leave_gateways` is set. |

<a id="brokapi-service-v1-inforequest"></a>

### InfoRequest

Без полей.

<a id="brokapi-service-v1-serviceinfo"></a>

### ServiceInfo

| Поле | Тип | № | Описание |
|---|---|---|---|
| `pid` | `uint32` | 1 |  |
| `version` | `string` | 2 |  |
| `config` | `string` | 3 | The config file the service was started with, and its state directory. |
| `state_dir` | `string` | 4 |  |
| `address` | `string` | 5 | Where the service listens. |
| `started_ns` | `uint64` | 6 |  |
| `uptime_ns` | `uint64` | 7 |  |
| `gateways` | `uint32` | 8 |  |
| `running` | `uint32` | 9 |  |

<a id="brokapi-service-v1-listrequest"></a>

### ListRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `names` | список `string` | 1 |  |

<a id="brokapi-service-v1-listresponse"></a>

### ListResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `gateways` | список [`GatewayInfo`](#brokapi-service-v1-gatewayinfo) | 1 |  |

<a id="brokapi-service-v1-gatewayref"></a>

### GatewayRef

| Поле | Тип | № | Описание |
|---|---|---|---|
| `name` | `string` | 1 |  |

<a id="brokapi-service-v1-gatewayinfo"></a>

### GatewayInfo

A gateway as the service sees it.

| Поле | Тип | № | Описание |
|---|---|---|---|
| `name` | `string` | 1 |  |
| `broker` | `string` | 2 |  |
| `state` | `string` | 3 | starting, running, restarting, stopping, stopped or failed. |
| `pid` | `uint32` | 4 | The process id, 0 if there is no process. |
| `uptime_ns` | `uint64` | 5 | How long the process has run. |
| `restarts` | `uint32` | 6 | How many times the service started it again after it stopped on its own. |
| `last_error` | `string` | 7 | The last time it stopped on its own or could not start, and why. Empty if it never did. |
| `restart_in_ns` | `uint64` | 8 | While restarting: how long until the next start. |
| `cpu_percent` | `float` | 9 | The CPU use of the process in percent of one core, and its memory in bytes. |
| `memory_bytes` | `uint64` | 10 |  |
| `account` | `string` | 11 | The account and whether it is a sandbox, from the config. |
| `sandbox` | `bool` | 12 |  |
| `adopted` | `bool` | 13 | The service found this process running when it started, instead of starting it. |
| `address` | `string` | 14 | Where the gateway listens, and its directory. |
| `state_dir` | `string` | 15 |  |
| `last_error_age_ns` | `uint64` | 17 | How long ago last_error happened. 0 when there is none. |

<a id="brokapi-service-v1-stoprequest"></a>

### StopRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `name` | `string` | 1 |  |
| `cancel_orders` | `bool` | 2 |  |

<a id="brokapi-service-v1-stopresponse"></a>

### StopResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `gateway` | [`GatewayInfo`](#brokapi-service-v1-gatewayinfo) | 1 |  |
| `cancelled_orders` | `uint32` | 2 | How many orders and stop orders were cancelled before the stop. |
| `cancelled_stops` | `uint32` | 3 |  |
| `errors` | список `string` | 4 | Orders that could not be cancelled, with the reason. |

<a id="brokapi-service-v1-reloadrequest"></a>

### ReloadRequest

Без полей.

<a id="brokapi-service-v1-reloadresponse"></a>

### ReloadResponse

| Поле | Тип | № | Описание |
|---|---|---|---|
| `started` | список `string` | 1 |  |
| `stopped` | список `string` | 2 |  |
| `restarted` | список `string` | 3 |  |
| `unchanged` | список `string` | 4 |  |

<a id="brokapi-service-v1-shutdownrequest"></a>

### ShutdownRequest

| Поле | Тип | № | Описание |
|---|---|---|---|
| `leave_gateways` | `bool` | 1 |  |

<a id="brokapi-service-v1-shutdownresponse"></a>

### ShutdownResponse

Без полей.

<!-- proto:end -->
