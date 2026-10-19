# Покрытие API брокеров

Собрано скриптом по коду, руками не правится. Цель: что поддерживает API брокера, то доступно из библиотеки.

Колонка **Где**:

- **общий** - метод вызывает коннектор, он доступен через общие трейты (`brokapi-core`).
- **клиент** - метод есть в клиенте брокера (`*-protocol`), общего трейта для него нет. Он доступен через `connector.raw()` (`TbankRaw`, `FinamRaw`, `BcsRaw`): тот же токен, адрес и общий счетчик лимитов, квота берется по имени метода. Это не недоделка, а способ дать все остальное без раздувания общего интерфейса.
- **нет** - метода нет в библиотеке вообще. Сейчас таких нет. Если появится, рядом будет причина, почему его сознательно не берем.

Что считается вызовом: в коде коннектора (`crates/{bcs,tbank,finam}/src`, без комментариев) метод клиента вызван как `.имя_метода(`. Вызовы через `raw()` сюда не попадают по построению: там имя метода приходит замыканием у пользователя.

## Т-Банк

Источник: официальные .proto в `crates/tbank-protocol/proto`.

Всего 101: в общем интерфейсе 30, только в клиенте брокера 71, нет 0.

| Сервис | Метод | Поток | Где |
|---|---|---|---|
| InstrumentsService | TradingSchedules |  | общий |
| InstrumentsService | BondBy |  | клиент |
| InstrumentsService | Bonds |  | общий |
| InstrumentsService | GetBondCoupons |  | клиент |
| InstrumentsService | GetBondEvents |  | клиент |
| InstrumentsService | CurrencyBy |  | клиент |
| InstrumentsService | Currencies |  | общий |
| InstrumentsService | EtfBy |  | клиент |
| InstrumentsService | Etfs |  | общий |
| InstrumentsService | FutureBy |  | клиент |
| InstrumentsService | Futures |  | общий |
| InstrumentsService | OptionBy |  | клиент |
| InstrumentsService | Options |  | общий |
| InstrumentsService | OptionsBy |  | клиент |
| InstrumentsService | ShareBy |  | клиент |
| InstrumentsService | Shares |  | общий |
| InstrumentsService | Indicatives |  | клиент |
| InstrumentsService | GetAccruedInterests |  | клиент |
| InstrumentsService | GetFuturesMargin |  | клиент |
| InstrumentsService | GetInstrumentBy |  | общий |
| InstrumentsService | GetDividends |  | клиент |
| InstrumentsService | GetAssetBy |  | клиент |
| InstrumentsService | GetAssets |  | клиент |
| InstrumentsService | GetFavorites |  | клиент |
| InstrumentsService | EditFavorites |  | клиент |
| InstrumentsService | CreateFavoriteGroup |  | клиент |
| InstrumentsService | DeleteFavoriteGroup |  | клиент |
| InstrumentsService | GetFavoriteGroups |  | клиент |
| InstrumentsService | GetCountries |  | клиент |
| InstrumentsService | FindInstrument |  | общий |
| InstrumentsService | GetBrands |  | клиент |
| InstrumentsService | GetBrandBy |  | клиент |
| InstrumentsService | GetAssetFundamentals |  | клиент |
| InstrumentsService | GetAssetReports |  | клиент |
| InstrumentsService | GetConsensusForecasts |  | клиент |
| InstrumentsService | GetForecastBy |  | клиент |
| InstrumentsService | GetRiskRates |  | клиент |
| InstrumentsService | GetInsiderDeals |  | клиент |
| InstrumentsService | StructuredNoteBy |  | клиент |
| InstrumentsService | StructuredNotes |  | клиент |
| MarketDataService | GetCandles |  | общий |
| MarketDataService | GetLastPrices |  | клиент |
| MarketDataService | GetOrderBook |  | общий |
| MarketDataService | GetTradingStatus |  | общий |
| MarketDataService | GetTradingStatuses |  | клиент |
| MarketDataService | GetLastTrades |  | общий |
| MarketDataService | GetClosePrices |  | клиент |
| MarketDataService | GetTechAnalysis |  | клиент |
| MarketDataService | GetMarketValues |  | клиент |
| MarketDataStreamService | MarketDataStream | да | общий |
| MarketDataStreamService | MarketDataServerSideStream | да | клиент |
| OperationsService | GetOperations |  | клиент |
| OperationsService | GetPortfolio |  | общий |
| OperationsService | GetPositions |  | общий |
| OperationsService | GetWithdrawLimits |  | клиент |
| OperationsService | GetBrokerReport |  | клиент |
| OperationsService | GetDividendsForeignIssuer |  | клиент |
| OperationsService | GetOperationsByCursor |  | общий |
| OperationsStreamService | PortfolioStream | да | общий |
| OperationsStreamService | PositionsStream | да | общий |
| OrdersStreamService | TradesStream | да | общий |
| OrdersStreamService | OrderStateStream | да | общий |
| OrdersService | PostOrder |  | общий |
| OrdersService | PostOrderAsync |  | клиент |
| OrdersService | CancelOrder |  | общий |
| OrdersService | GetOrderState |  | общий |
| OrdersService | GetOrders |  | общий |
| OrdersService | ReplaceOrder |  | общий |
| OrdersService | GetMaxLots |  | клиент |
| OrdersService | GetOrderPrice |  | клиент |
| SandboxService | OpenSandboxAccount |  | клиент |
| SandboxService | GetSandboxAccounts |  | клиент |
| SandboxService | CloseSandboxAccount |  | клиент |
| SandboxService | PostSandboxOrder |  | клиент |
| SandboxService | PostSandboxOrderAsync |  | клиент |
| SandboxService | ReplaceSandboxOrder |  | клиент |
| SandboxService | GetSandboxOrders |  | клиент |
| SandboxService | CancelSandboxOrder |  | клиент |
| SandboxService | GetSandboxOrderState |  | клиент |
| SandboxService | GetSandboxOrderPrice |  | клиент |
| SandboxService | GetSandboxPositions |  | клиент |
| SandboxService | GetSandboxOperations |  | клиент |
| SandboxService | GetSandboxOperationsByCursor |  | клиент |
| SandboxService | GetSandboxPortfolio |  | клиент |
| SandboxService | SandboxPayIn |  | клиент |
| SandboxService | GetSandboxWithdrawLimits |  | клиент |
| SandboxService | GetSandboxMaxLots |  | клиент |
| SandboxService | PostSandboxStopOrder |  | клиент |
| SandboxService | GetSandboxStopOrders |  | клиент |
| SandboxService | CancelSandboxStopOrder |  | клиент |
| SignalService | GetStrategies |  | клиент |
| SignalService | GetSignals |  | клиент |
| StopOrdersService | PostStopOrder |  | общий |
| StopOrdersService | GetStopOrders |  | общий |
| StopOrdersService | CancelStopOrder |  | общий |
| UsersService | GetAccounts |  | общий |
| UsersService | GetMarginAttributes |  | клиент |
| UsersService | GetUserTariff |  | клиент |
| UsersService | GetInfo |  | клиент |
| UsersService | GetBankAccounts |  | клиент |
| UsersService | CurrencyTransfer |  | клиент |

## Финам

Источник: официальные .proto в `crates/finam-protocol/proto`.

Всего 42: в общем интерфейсе 21, только в клиенте брокера 21, нет 0.

| Сервис | Метод | Поток | Где |
|---|---|---|---|
| AccountsService | GetAccount |  | общий |
| AccountsService | Trades |  | общий |
| AccountsService | Transactions |  | клиент |
| AccountsService | SubscribeAccount | да | общий |
| AssetsService | Exchanges |  | клиент |
| AssetsService | Assets |  | общий |
| AssetsService | AllAssets |  | общий |
| AssetsService | GetAsset |  | общий |
| AssetsService | GetAssetParams |  | клиент |
| AssetsService | OptionsChain |  | клиент |
| AssetsService | Schedule |  | общий |
| AssetsService | Clock |  | клиент |
| AssetsService | GetConstituents |  | клиент |
| AuthService | Auth |  | клиент |
| AuthService | TokenDetails |  | клиент |
| AuthService | SubscribeJwtRenewal | да | клиент |
| CorporateActionsService | GetFutureSplits |  | клиент |
| CorporateActionsService | GetPastSplits |  | клиент |
| CorporateActionsService | GetFutureBondsEvents |  | клиент |
| CorporateActionsService | GetPastBondsEvents |  | клиент |
| CorporateActionsService | GetFutureDividends |  | клиент |
| CorporateActionsService | GetPastDividends |  | клиент |
| MarketDataService | Bars |  | общий |
| MarketDataService | LastQuote |  | клиент |
| MarketDataService | OrderBook |  | общий |
| MarketDataService | LatestTrades |  | общий |
| MarketDataService | SubscribeQuote | да | общий |
| MarketDataService | SubscribeOrderBook | да | общий |
| MarketDataService | SubscribeLatestTrades | да | общий |
| MarketDataService | SubscribeBars | да | общий |
| UsageMetricsService | GetUsageMetrics |  | клиент |
| OrdersService | PlaceOrder |  | общий |
| OrdersService | CancelOrder |  | общий |
| OrdersService | GetOrders |  | общий |
| OrdersService | GetOrder |  | общий |
| OrdersService | SubscribeOrderTrade | да | клиент |
| OrdersService | SubscribeOrders | да | общий |
| OrdersService | SubscribeTrades | да | общий |
| OrdersService | PlaceSLTPOrder |  | общий |
| ReportsService | CreateAccountReport |  | клиент |
| ReportsService | GetAccountReportInfo |  | клиент |
| ReportsService | SubscribeAccountReportInfo | да | клиент |

## БКС

Источник: документация БКС (trade-api.bcs.ru). Страницы сопоставлены с функциями клиента вручную, использование коннектором ищется по коду. Устаревшие методы (`deprecated`) не считаем.

Всего 33: в общем интерфейсе 22, только в клиенте брокера 11, нет 0.

| Сервис | Метод | Поток | Где |
|---|---|---|---|
| REST | POST /trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token (authenticate) |  | общий |
| REST | GET /api/v1/instruments-discounts (get_instruments_discounts) |  | клиент |
| REST | POST /api/v1/operations/search (search_operations) |  | клиент |
| REST | GET /api/v1/trading-schedule/daily-schedule (get_daily_schedule) |  | общий |
| REST | POST /api/v1/instruments/by-isins (get_instruments_by_isins) |  | общий |
| REST | POST /api/v1/instruments/by-tickers (get_instruments_by_tickers) |  | общий |
| REST | GET /api/v1/instruments/by-type (get_instruments_by_type) |  | общий |
| REST | GET /api/v1/trading-schedule/status (get_trading_status) |  | общий |
| REST | GET /api/v1/limits (get_limits) |  | клиент |
| REST | GET /api/v1/candles-chart (get_candles) |  | общий |
| REST | POST /api/v1/last-trades (get_last_trades) |  | общий |
| REST | GET /api/v1/order-book (get_order_book) |  | общий |
| REST | POST /api/v1/quotes (get_quotes) |  | клиент |
| REST | POST /api/v1/orders/search (search_orders, search_orders_simple) |  | общий |
| REST | POST /api/v1/orders/cancel (cancel_order_by_id) |  | клиент |
| REST | POST /api/v1/orders (create_order) |  | клиент |
| REST | GET /api/v1/orders (get_order) |  | общий |
| REST | POST /api/v1/orders/cancel/sync (cancel_order_by_id_sync) |  | общий |
| REST | POST /api/v1/orders/sync (create_order_sync) |  | общий |
| REST | POST /api/v1/orders/edit/sync (edit_order_sync) |  | общий |
| REST | POST /api/v1/orders/edit (edit_order) |  | клиент |
| REST | GET /api/v1/portfolio (get_portfolio) |  | общий |
| REST | POST /api/v1/trades/search (search_trades) |  | общий |
| WebSocket | market-data/order-book (connect_market_data) | да | общий |
| WebSocket | market-data/quotes (connect_market_data) | да | общий |
| WebSocket | market-data/trades (connect_market_data) | да | общий |
| WebSocket | market-data/last-candle (connect_market_data) | да | общий |
| WebSocket | operations/execution-status (connect_order_execution) | да | клиент |
| WebSocket | operations/status (connect_order_events) | да | общий |
| WebSocket | operations/transaction-status (connect_order_transaction) | да | клиент |
| WebSocket | portfolio (connect_portfolio) | да | общий |
| WebSocket | limits (connect_limits) | да | клиент |
| WebSocket | marginal-indicators (connect_marginal_indicators) | да | клиент |

