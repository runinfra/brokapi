# Покрытие API брокеров

Собрано скриптом по коду, руками не правится. Цель: что поддерживает API брокера, то доступно из библиотеки.

Колонка **Где**:

- **общий** - метод вызывает коннектор, он доступен через общие трейты (`brokapi-core`).
- **клиент** - метод есть в клиенте брокера (`*-protocol`), общего трейта для него нет. Он доступен через `connector.raw()` (`TbankRaw`, `FinamRaw`, `BcsRaw`, `AlorRaw`): тот же токен, адрес и общий счетчик лимитов, квота берется по имени метода. Это не недоделка, а способ дать все остальное без раздувания общего интерфейса.
- **нет** - метода нет в библиотеке вообще. Сейчас таких нет. Если появится, рядом будет причина, почему его сознательно не берем.

Что считается вызовом: в коде коннектора (`crates/{bcs,tbank,finam,alor}/src`, без комментариев) метод клиента вызван как `.имя_метода(`. Вызовы через `raw()` сюда не попадают по построению: там имя метода приходит замыканием у пользователя.

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

Всего 42: в общем интерфейсе 22, только в клиенте брокера 20, нет 0.

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
| AssetsService | GetAssetParams |  | общий |
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

## АЛОР

Источник: описания API брокера (OpenAPI для HTTP и AsyncAPI для WebSocket, alor.dev). Методы сопоставлены с функциями клиента вручную, использование коннектором ищется по коду. Команды канала `/cws` в клиенте есть (`CwsSession`), коннектор ходит за теми же командами по HTTP.

Всего 84: в общем интерфейсе 25, только в клиенте брокера 59, нет 0.

| Сервис | Метод | Поток | Где |
|---|---|---|---|
| REST | POST /refresh (get_access_token, refresh_access_token) |  | общий |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/orders (get_orders) |  | общий |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/orders/{orderId} (get_order) |  | общий |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/stoporders (get_stop_orders) |  | общий |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/stoporders/{orderId} (get_stop_order) |  | клиент |
| REST | GET /md/v2/Clients/legacy/{exchange}/{portfolio}/money (get_money_legacy) |  | клиент |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/summary (get_summary) |  | клиент |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/positions (get_positions) |  | общий |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/positions/{symbol} (get_position) |  | клиент |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/trades (get_trades) |  | общий |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/{symbol}/trades (get_symbol_trades) |  | клиент |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/fortsrisk (get_forts_risk) |  | клиент |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/risk (get_risk) |  | клиент |
| REST | GET /md/v2/Clients/{login}/positions (get_login_positions) |  | клиент |
| REST | GET /md/stats/{exchange}/{portfolio}/history/trades (get_trade_stats) |  | клиент |
| REST | GET /md/stats/{exchange}/{portfolio}/history/trades/{symbol} (get_trade_stats_by_symbol) |  | клиент |
| REST | GET /md/v2/Stats/{exchange}/{portfolio}/history/trades (get_history_trades) |  | общий |
| REST | GET /md/v2/Stats/{exchange}/{portfolio}/history/trades/{symbol} (get_history_trades_by_symbol) |  | клиент |
| REST | GET /md/v2/Securities (get_securities) |  | общий |
| REST | GET /md/v2/Securities/{exchange} (get_securities_by_exchange) |  | клиент |
| REST | GET /md/v2/Securities/{exchange}/{symbol} (get_security) |  | общий |
| REST | GET /md/v2/Securities/{exchange}/{symbol}/availableBoards (get_available_boards) |  | клиент |
| REST | GET /md/v2/Securities/{exchange}/{symbol}/alltrades (get_all_trades) |  | общий |
| REST | GET /md/v2/Securities/{exchange}/{symbol}/alltrades/history (get_all_trades_history) |  | клиент |
| REST | GET /md/v2/Securities/{exchange}/{symbol}/actualFuturesQuote (get_actual_futures_quote) |  | клиент |
| REST | GET /md/v2/Securities/{symbols}/quotes (get_quotes) |  | клиент |
| REST | GET /md/v2/Securities/currencyPairs (get_currency_pairs) |  | клиент |
| REST | GET /md/v2/orderbooks/{exchange}/{symbol} (get_order_book) |  | общий |
| REST | GET /md/v2/time (get_time) |  | клиент |
| REST | GET /md/v2/risk/rates (get_risk_rates) |  | клиент |
| REST | GET /md/v2/history (get_history) |  | общий |
| REST | GET /md/status/{exchange}/{market} (get_market_status) |  | клиент |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/actions/market (create_market_order) |  | общий |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/actions/limit (create_limit_order) |  | общий |
| REST | POST /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLoss (create_stop_loss) |  | клиент |
| REST | POST /warptrans/{tradeServerCode}/v2/client/orders/actions/takeProfit (create_take_profit) |  | клиент |
| REST | POST /warptrans/{tradeServerCode}/v2/client/orders/actions/takeProfitLimit (create_take_profit_limit) |  | клиент |
| REST | POST /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLossLimit (create_stop_loss_limit) |  | клиент |
| REST | PUT /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLoss/{orderId} (update_stop_loss) |  | клиент |
| REST | PUT /warptrans/{tradeServerCode}/v2/client/orders/actions/takeProfit/{orderId} (update_take_profit) |  | клиент |
| REST | PUT /warptrans/{tradeServerCode}/v2/client/orders/actions/takeProfitLimit/{orderId} (update_take_profit_limit) |  | клиент |
| REST | PUT /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLossLimit/{orderId} (update_stop_loss_limit) |  | клиент |
| REST | PUT /commandapi/warptrans/TRADE/v2/client/orders/actions/market/{orderId} (update_market_order) |  | клиент |
| REST | PUT /commandapi/warptrans/TRADE/v2/client/orders/actions/limit/{orderId} (update_limit_order) |  | общий |
| REST | DELETE /warptrans/{tradeServerCode}/v2/client/orders/{orderId} (cancel_order_legacy) |  | клиент |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/estimate (estimate_order) |  | клиент |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/estimate/all (estimate_orders) |  | клиент |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/actions/stopLimit (create_stop_limit_order) |  | общий |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/actions/stop (create_stop_order) |  | общий |
| REST | PUT /commandapi/warptrans/TRADE/v2/client/orders/actions/stop/{stopOrderId} (update_stop_order) |  | клиент |
| REST | PUT /commandapi/warptrans/TRADE/v2/client/orders/actions/stopLimit/{stopOrderId} (update_stop_limit_order) |  | клиент |
| REST | DELETE /commandapi/warptrans/TRADE/v2/client/orders/{orderId} (cancel_order) |  | общий |
| REST | DELETE /commandapi/warptrans/TRADE/v2/client/orders/all (cancel_all_orders) |  | клиент |
| REST | GET /commandapi/api/orderGroups (get_order_groups) |  | клиент |
| REST | POST /commandapi/api/orderGroups (create_order_group) |  | клиент |
| REST | GET /commandapi/api/orderGroups/{orderGroupId} (get_order_group) |  | клиент |
| REST | PUT /commandapi/api/orderGroups/{orderGroupId} (update_order_group) |  | клиент |
| REST | DELETE /commandapi/api/orderGroups/{orderGroupId} (delete_order_group) |  | клиент |
| WebSocket | subscribeOrderBook /ws (WsSubscribe::order_book) | да | общий |
| WebSocket | subscribeBars /ws (WsSubscribe::bars) | да | общий |
| WebSocket | subscribeQuotes /ws (WsSubscribe::quotes) | да | общий |
| WebSocket | subscribeTrades /ws (WsSubscribe::trades) | да | общий |
| WebSocket | subscribeAllTrades /ws (WsSubscribe::all_trades) | да | общий |
| WebSocket | subscribePositions /ws (WsSubscribe::positions) | да | общий |
| WebSocket | subscribeSummaries /ws (WsSubscribe::summaries) | да | клиент |
| WebSocket | subscribeRisks /ws (WsSubscribe::risks) | да | клиент |
| WebSocket | subscribeSpectraRisks /ws (WsSubscribe::spectra_risks) | да | клиент |
| WebSocket | subscribeInstruments /ws (WsSubscribe::instruments) | да | клиент |
| WebSocket | subscribeOrders /ws (WsSubscribe::orders) | да | общий |
| WebSocket | subscribeStopOrders /ws (WsSubscribe::stop_orders) | да | клиент |
| WebSocket | unsubscribe /ws (WsSubscribe::unsubscribe) | да | клиент |
| WebSocket | authorize /cws (CwsSession::authorize) | да | клиент |
| WebSocket | createMarketOrder /cws (CREATE_MARKET) | да | клиент |
| WebSocket | createLimitOrder /cws (CREATE_LIMIT) | да | клиент |
| WebSocket | createStopMarketOrder /cws (CREATE_STOP) | да | клиент |
| WebSocket | createStopLimitOrder /cws (CREATE_STOP_LIMIT) | да | клиент |
| WebSocket | updateMarketOrder /cws (UPDATE_MARKET) | да | клиент |
| WebSocket | updateLimitOrder /cws (UPDATE_LIMIT) | да | клиент |
| WebSocket | updateStopMarketOrder /cws (UPDATE_STOP) | да | клиент |
| WebSocket | updateStopLimitOrder /cws (UPDATE_STOP_LIMIT) | да | клиент |
| WebSocket | deleteMarketOrder /cws (DELETE_MARKET) | да | клиент |
| WebSocket | deleteLimitOrder /cws (DELETE_LIMIT) | да | клиент |
| WebSocket | deleteStopMarketOrder /cws (DELETE_STOP) | да | клиент |
| WebSocket | deleteStopLimitOrder /cws (DELETE_STOP_LIMIT) | да | клиент |

