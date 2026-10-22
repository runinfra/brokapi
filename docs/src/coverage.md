# Покрытие API брокеров

Какие методы API брокера закрыты общим интерфейсом. Колонка **Где**:

- **в интерфейсе** - метод доступен через общие трейты (`brokapi-core`).
- **через raw()** - общего трейта нет, метод доступен через `connector.raw()` с теми же токеном, адресом и общим счетчиком лимитов.
- **нет** - метода нет в библиотеке.

Все методы брокера доступны через `raw()`, даже если общего трейта для них нет.

## Т-Банк

Источник: официальные .proto в `crates/tbank-protocol/proto`.

Всего 101: в интерфейсе 30, через `raw()` 71.

| Сервис | Метод | Поток | Где |
|---|---|---|---|
| InstrumentsService | TradingSchedules |  | в интерфейсе |
| InstrumentsService | BondBy |  | через raw() |
| InstrumentsService | Bonds |  | в интерфейсе |
| InstrumentsService | GetBondCoupons |  | через raw() |
| InstrumentsService | GetBondEvents |  | через raw() |
| InstrumentsService | CurrencyBy |  | через raw() |
| InstrumentsService | Currencies |  | в интерфейсе |
| InstrumentsService | EtfBy |  | через raw() |
| InstrumentsService | Etfs |  | в интерфейсе |
| InstrumentsService | FutureBy |  | через raw() |
| InstrumentsService | Futures |  | в интерфейсе |
| InstrumentsService | OptionBy |  | через raw() |
| InstrumentsService | Options |  | в интерфейсе |
| InstrumentsService | OptionsBy |  | через raw() |
| InstrumentsService | ShareBy |  | через raw() |
| InstrumentsService | Shares |  | в интерфейсе |
| InstrumentsService | Indicatives |  | через raw() |
| InstrumentsService | GetAccruedInterests |  | через raw() |
| InstrumentsService | GetFuturesMargin |  | через raw() |
| InstrumentsService | GetInstrumentBy |  | в интерфейсе |
| InstrumentsService | GetDividends |  | через raw() |
| InstrumentsService | GetAssetBy |  | через raw() |
| InstrumentsService | GetAssets |  | через raw() |
| InstrumentsService | GetFavorites |  | через raw() |
| InstrumentsService | EditFavorites |  | через raw() |
| InstrumentsService | CreateFavoriteGroup |  | через raw() |
| InstrumentsService | DeleteFavoriteGroup |  | через raw() |
| InstrumentsService | GetFavoriteGroups |  | через raw() |
| InstrumentsService | GetCountries |  | через raw() |
| InstrumentsService | FindInstrument |  | в интерфейсе |
| InstrumentsService | GetBrands |  | через raw() |
| InstrumentsService | GetBrandBy |  | через raw() |
| InstrumentsService | GetAssetFundamentals |  | через raw() |
| InstrumentsService | GetAssetReports |  | через raw() |
| InstrumentsService | GetConsensusForecasts |  | через raw() |
| InstrumentsService | GetForecastBy |  | через raw() |
| InstrumentsService | GetRiskRates |  | через raw() |
| InstrumentsService | GetInsiderDeals |  | через raw() |
| InstrumentsService | StructuredNoteBy |  | через raw() |
| InstrumentsService | StructuredNotes |  | через raw() |
| MarketDataService | GetCandles |  | в интерфейсе |
| MarketDataService | GetLastPrices |  | через raw() |
| MarketDataService | GetOrderBook |  | в интерфейсе |
| MarketDataService | GetTradingStatus |  | в интерфейсе |
| MarketDataService | GetTradingStatuses |  | через raw() |
| MarketDataService | GetLastTrades |  | в интерфейсе |
| MarketDataService | GetClosePrices |  | через raw() |
| MarketDataService | GetTechAnalysis |  | через raw() |
| MarketDataService | GetMarketValues |  | через raw() |
| MarketDataStreamService | MarketDataStream | да | в интерфейсе |
| MarketDataStreamService | MarketDataServerSideStream | да | через raw() |
| OperationsService | GetOperations |  | через raw() |
| OperationsService | GetPortfolio |  | в интерфейсе |
| OperationsService | GetPositions |  | в интерфейсе |
| OperationsService | GetWithdrawLimits |  | через raw() |
| OperationsService | GetBrokerReport |  | через raw() |
| OperationsService | GetDividendsForeignIssuer |  | через raw() |
| OperationsService | GetOperationsByCursor |  | в интерфейсе |
| OperationsStreamService | PortfolioStream | да | в интерфейсе |
| OperationsStreamService | PositionsStream | да | в интерфейсе |
| OrdersStreamService | TradesStream | да | в интерфейсе |
| OrdersStreamService | OrderStateStream | да | в интерфейсе |
| OrdersService | PostOrder |  | в интерфейсе |
| OrdersService | PostOrderAsync |  | через raw() |
| OrdersService | CancelOrder |  | в интерфейсе |
| OrdersService | GetOrderState |  | в интерфейсе |
| OrdersService | GetOrders |  | в интерфейсе |
| OrdersService | ReplaceOrder |  | в интерфейсе |
| OrdersService | GetMaxLots |  | через raw() |
| OrdersService | GetOrderPrice |  | через raw() |
| SandboxService | OpenSandboxAccount |  | через raw() |
| SandboxService | GetSandboxAccounts |  | через raw() |
| SandboxService | CloseSandboxAccount |  | через raw() |
| SandboxService | PostSandboxOrder |  | через raw() |
| SandboxService | PostSandboxOrderAsync |  | через raw() |
| SandboxService | ReplaceSandboxOrder |  | через raw() |
| SandboxService | GetSandboxOrders |  | через raw() |
| SandboxService | CancelSandboxOrder |  | через raw() |
| SandboxService | GetSandboxOrderState |  | через raw() |
| SandboxService | GetSandboxOrderPrice |  | через raw() |
| SandboxService | GetSandboxPositions |  | через raw() |
| SandboxService | GetSandboxOperations |  | через raw() |
| SandboxService | GetSandboxOperationsByCursor |  | через raw() |
| SandboxService | GetSandboxPortfolio |  | через raw() |
| SandboxService | SandboxPayIn |  | через raw() |
| SandboxService | GetSandboxWithdrawLimits |  | через raw() |
| SandboxService | GetSandboxMaxLots |  | через raw() |
| SandboxService | PostSandboxStopOrder |  | через raw() |
| SandboxService | GetSandboxStopOrders |  | через raw() |
| SandboxService | CancelSandboxStopOrder |  | через raw() |
| SignalService | GetStrategies |  | через raw() |
| SignalService | GetSignals |  | через raw() |
| StopOrdersService | PostStopOrder |  | в интерфейсе |
| StopOrdersService | GetStopOrders |  | в интерфейсе |
| StopOrdersService | CancelStopOrder |  | в интерфейсе |
| UsersService | GetAccounts |  | в интерфейсе |
| UsersService | GetMarginAttributes |  | через raw() |
| UsersService | GetUserTariff |  | через raw() |
| UsersService | GetInfo |  | через raw() |
| UsersService | GetBankAccounts |  | через raw() |
| UsersService | CurrencyTransfer |  | через raw() |

## Финам

Источник: официальные .proto в `crates/finam-protocol/proto`.

Всего 42: в интерфейсе 22, через `raw()` 20.

| Сервис | Метод | Поток | Где |
|---|---|---|---|
| AccountsService | GetAccount |  | в интерфейсе |
| AccountsService | Trades |  | в интерфейсе |
| AccountsService | Transactions |  | через raw() |
| AccountsService | SubscribeAccount | да | в интерфейсе |
| AssetsService | Exchanges |  | через raw() |
| AssetsService | Assets |  | в интерфейсе |
| AssetsService | AllAssets |  | в интерфейсе |
| AssetsService | GetAsset |  | в интерфейсе |
| AssetsService | GetAssetParams |  | в интерфейсе |
| AssetsService | OptionsChain |  | через raw() |
| AssetsService | Schedule |  | в интерфейсе |
| AssetsService | Clock |  | через raw() |
| AssetsService | GetConstituents |  | через raw() |
| AuthService | Auth |  | через raw() |
| AuthService | TokenDetails |  | через raw() |
| AuthService | SubscribeJwtRenewal | да | через raw() |
| CorporateActionsService | GetFutureSplits |  | через raw() |
| CorporateActionsService | GetPastSplits |  | через raw() |
| CorporateActionsService | GetFutureBondsEvents |  | через raw() |
| CorporateActionsService | GetPastBondsEvents |  | через raw() |
| CorporateActionsService | GetFutureDividends |  | через raw() |
| CorporateActionsService | GetPastDividends |  | через raw() |
| MarketDataService | Bars |  | в интерфейсе |
| MarketDataService | LastQuote |  | через raw() |
| MarketDataService | OrderBook |  | в интерфейсе |
| MarketDataService | LatestTrades |  | в интерфейсе |
| MarketDataService | SubscribeQuote | да | в интерфейсе |
| MarketDataService | SubscribeOrderBook | да | в интерфейсе |
| MarketDataService | SubscribeLatestTrades | да | в интерфейсе |
| MarketDataService | SubscribeBars | да | в интерфейсе |
| UsageMetricsService | GetUsageMetrics |  | через raw() |
| OrdersService | PlaceOrder |  | в интерфейсе |
| OrdersService | CancelOrder |  | в интерфейсе |
| OrdersService | GetOrders |  | в интерфейсе |
| OrdersService | GetOrder |  | в интерфейсе |
| OrdersService | SubscribeOrderTrade | да | через raw() |
| OrdersService | SubscribeOrders | да | в интерфейсе |
| OrdersService | SubscribeTrades | да | в интерфейсе |
| OrdersService | PlaceSLTPOrder |  | в интерфейсе |
| ReportsService | CreateAccountReport |  | через raw() |
| ReportsService | GetAccountReportInfo |  | через raw() |
| ReportsService | SubscribeAccountReportInfo | да | через raw() |

## БКС

Источник: документация БКС (trade-api.bcs.ru). Устаревшие методы (`deprecated`) не считаем.

Всего 33: в интерфейсе 22, через `raw()` 11.

| Сервис | Метод | Поток | Где |
|---|---|---|---|
| REST | POST /trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token (authenticate) |  | в интерфейсе |
| REST | GET /api/v1/instruments-discounts (get_instruments_discounts) |  | через raw() |
| REST | POST /api/v1/operations/search (search_operations) |  | через raw() |
| REST | GET /api/v1/trading-schedule/daily-schedule (get_daily_schedule) |  | в интерфейсе |
| REST | POST /api/v1/instruments/by-isins (get_instruments_by_isins) |  | в интерфейсе |
| REST | POST /api/v1/instruments/by-tickers (get_instruments_by_tickers) |  | в интерфейсе |
| REST | GET /api/v1/instruments/by-type (get_instruments_by_type) |  | в интерфейсе |
| REST | GET /api/v1/trading-schedule/status (get_trading_status) |  | в интерфейсе |
| REST | GET /api/v1/limits (get_limits) |  | через raw() |
| REST | GET /api/v1/candles-chart (get_candles) |  | в интерфейсе |
| REST | POST /api/v1/last-trades (get_last_trades) |  | в интерфейсе |
| REST | GET /api/v1/order-book (get_order_book) |  | в интерфейсе |
| REST | POST /api/v1/quotes (get_quotes) |  | через raw() |
| REST | POST /api/v1/orders/search (search_orders, search_orders_simple) |  | в интерфейсе |
| REST | POST /api/v1/orders/cancel (cancel_order_by_id) |  | через raw() |
| REST | POST /api/v1/orders (create_order) |  | через raw() |
| REST | GET /api/v1/orders (get_order) |  | в интерфейсе |
| REST | POST /api/v1/orders/cancel/sync (cancel_order_by_id_sync) |  | в интерфейсе |
| REST | POST /api/v1/orders/sync (create_order_sync) |  | в интерфейсе |
| REST | POST /api/v1/orders/edit/sync (edit_order_sync) |  | в интерфейсе |
| REST | POST /api/v1/orders/edit (edit_order) |  | через raw() |
| REST | GET /api/v1/portfolio (get_portfolio) |  | в интерфейсе |
| REST | POST /api/v1/trades/search (search_trades) |  | в интерфейсе |
| WebSocket | market-data/order-book (connect_market_data) | да | в интерфейсе |
| WebSocket | market-data/quotes (connect_market_data) | да | в интерфейсе |
| WebSocket | market-data/trades (connect_market_data) | да | в интерфейсе |
| WebSocket | market-data/last-candle (connect_market_data) | да | в интерфейсе |
| WebSocket | operations/execution-status (connect_order_execution) | да | через raw() |
| WebSocket | operations/status (connect_order_events) | да | в интерфейсе |
| WebSocket | operations/transaction-status (connect_order_transaction) | да | через raw() |
| WebSocket | portfolio (connect_portfolio) | да | в интерфейсе |
| WebSocket | limits (connect_limits) | да | через raw() |
| WebSocket | marginal-indicators (connect_marginal_indicators) | да | через raw() |

## АЛОР

Источник: описания API брокера (OpenAPI для HTTP и AsyncAPI для WebSocket, alor.dev). Команды канала `/cws` в клиенте есть (`CwsSession`), коннектор ходит за теми же командами по HTTP.

Всего 84: в интерфейсе 25, через `raw()` 59.

| Сервис | Метод | Поток | Где |
|---|---|---|---|
| REST | POST /refresh (get_access_token, refresh_access_token) |  | в интерфейсе |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/orders (get_orders) |  | в интерфейсе |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/orders/{orderId} (get_order) |  | в интерфейсе |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/stoporders (get_stop_orders) |  | в интерфейсе |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/stoporders/{orderId} (get_stop_order) |  | через raw() |
| REST | GET /md/v2/Clients/legacy/{exchange}/{portfolio}/money (get_money_legacy) |  | через raw() |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/summary (get_summary) |  | через raw() |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/positions (get_positions) |  | в интерфейсе |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/positions/{symbol} (get_position) |  | через raw() |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/trades (get_trades) |  | в интерфейсе |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/{symbol}/trades (get_symbol_trades) |  | через raw() |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/fortsrisk (get_forts_risk) |  | через raw() |
| REST | GET /md/v2/Clients/{exchange}/{portfolio}/risk (get_risk) |  | через raw() |
| REST | GET /md/v2/Clients/{login}/positions (get_login_positions) |  | через raw() |
| REST | GET /md/stats/{exchange}/{portfolio}/history/trades (get_trade_stats) |  | через raw() |
| REST | GET /md/stats/{exchange}/{portfolio}/history/trades/{symbol} (get_trade_stats_by_symbol) |  | через raw() |
| REST | GET /md/v2/Stats/{exchange}/{portfolio}/history/trades (get_history_trades) |  | в интерфейсе |
| REST | GET /md/v2/Stats/{exchange}/{portfolio}/history/trades/{symbol} (get_history_trades_by_symbol) |  | через raw() |
| REST | GET /md/v2/Securities (get_securities) |  | в интерфейсе |
| REST | GET /md/v2/Securities/{exchange} (get_securities_by_exchange) |  | через raw() |
| REST | GET /md/v2/Securities/{exchange}/{symbol} (get_security) |  | в интерфейсе |
| REST | GET /md/v2/Securities/{exchange}/{symbol}/availableBoards (get_available_boards) |  | через raw() |
| REST | GET /md/v2/Securities/{exchange}/{symbol}/alltrades (get_all_trades) |  | в интерфейсе |
| REST | GET /md/v2/Securities/{exchange}/{symbol}/alltrades/history (get_all_trades_history) |  | через raw() |
| REST | GET /md/v2/Securities/{exchange}/{symbol}/actualFuturesQuote (get_actual_futures_quote) |  | через raw() |
| REST | GET /md/v2/Securities/{symbols}/quotes (get_quotes) |  | через raw() |
| REST | GET /md/v2/Securities/currencyPairs (get_currency_pairs) |  | через raw() |
| REST | GET /md/v2/orderbooks/{exchange}/{symbol} (get_order_book) |  | в интерфейсе |
| REST | GET /md/v2/time (get_time) |  | через raw() |
| REST | GET /md/v2/risk/rates (get_risk_rates) |  | через raw() |
| REST | GET /md/v2/history (get_history) |  | в интерфейсе |
| REST | GET /md/status/{exchange}/{market} (get_market_status) |  | через raw() |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/actions/market (create_market_order) |  | в интерфейсе |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/actions/limit (create_limit_order) |  | в интерфейсе |
| REST | POST /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLoss (create_stop_loss) |  | через raw() |
| REST | POST /warptrans/{tradeServerCode}/v2/client/orders/actions/takeProfit (create_take_profit) |  | через raw() |
| REST | POST /warptrans/{tradeServerCode}/v2/client/orders/actions/takeProfitLimit (create_take_profit_limit) |  | через raw() |
| REST | POST /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLossLimit (create_stop_loss_limit) |  | через raw() |
| REST | PUT /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLoss/{orderId} (update_stop_loss) |  | через raw() |
| REST | PUT /warptrans/{tradeServerCode}/v2/client/orders/actions/takeProfit/{orderId} (update_take_profit) |  | через raw() |
| REST | PUT /warptrans/{tradeServerCode}/v2/client/orders/actions/takeProfitLimit/{orderId} (update_take_profit_limit) |  | через raw() |
| REST | PUT /warptrans/{tradeServerCode}/v2/client/orders/actions/stopLossLimit/{orderId} (update_stop_loss_limit) |  | через raw() |
| REST | PUT /commandapi/warptrans/TRADE/v2/client/orders/actions/market/{orderId} (update_market_order) |  | через raw() |
| REST | PUT /commandapi/warptrans/TRADE/v2/client/orders/actions/limit/{orderId} (update_limit_order) |  | в интерфейсе |
| REST | DELETE /warptrans/{tradeServerCode}/v2/client/orders/{orderId} (cancel_order_legacy) |  | через raw() |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/estimate (estimate_order) |  | через raw() |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/estimate/all (estimate_orders) |  | через raw() |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/actions/stopLimit (create_stop_limit_order) |  | в интерфейсе |
| REST | POST /commandapi/warptrans/TRADE/v2/client/orders/actions/stop (create_stop_order) |  | в интерфейсе |
| REST | PUT /commandapi/warptrans/TRADE/v2/client/orders/actions/stop/{stopOrderId} (update_stop_order) |  | через raw() |
| REST | PUT /commandapi/warptrans/TRADE/v2/client/orders/actions/stopLimit/{stopOrderId} (update_stop_limit_order) |  | через raw() |
| REST | DELETE /commandapi/warptrans/TRADE/v2/client/orders/{orderId} (cancel_order) |  | в интерфейсе |
| REST | DELETE /commandapi/warptrans/TRADE/v2/client/orders/all (cancel_all_orders) |  | через raw() |
| REST | GET /commandapi/api/orderGroups (get_order_groups) |  | через raw() |
| REST | POST /commandapi/api/orderGroups (create_order_group) |  | через raw() |
| REST | GET /commandapi/api/orderGroups/{orderGroupId} (get_order_group) |  | через raw() |
| REST | PUT /commandapi/api/orderGroups/{orderGroupId} (update_order_group) |  | через raw() |
| REST | DELETE /commandapi/api/orderGroups/{orderGroupId} (delete_order_group) |  | через raw() |
| WebSocket | subscribeOrderBook /ws (WsSubscribe::order_book) | да | в интерфейсе |
| WebSocket | subscribeBars /ws (WsSubscribe::bars) | да | в интерфейсе |
| WebSocket | subscribeQuotes /ws (WsSubscribe::quotes) | да | в интерфейсе |
| WebSocket | subscribeTrades /ws (WsSubscribe::trades) | да | в интерфейсе |
| WebSocket | subscribeAllTrades /ws (WsSubscribe::all_trades) | да | в интерфейсе |
| WebSocket | subscribePositions /ws (WsSubscribe::positions) | да | в интерфейсе |
| WebSocket | subscribeSummaries /ws (WsSubscribe::summaries) | да | через raw() |
| WebSocket | subscribeRisks /ws (WsSubscribe::risks) | да | через raw() |
| WebSocket | subscribeSpectraRisks /ws (WsSubscribe::spectra_risks) | да | через raw() |
| WebSocket | subscribeInstruments /ws (WsSubscribe::instruments) | да | через raw() |
| WebSocket | subscribeOrders /ws (WsSubscribe::orders) | да | в интерфейсе |
| WebSocket | subscribeStopOrders /ws (WsSubscribe::stop_orders) | да | через raw() |
| WebSocket | unsubscribe /ws (WsSubscribe::unsubscribe) | да | через raw() |
| WebSocket | authorize /cws (CwsSession::authorize) | да | через raw() |
| WebSocket | createMarketOrder /cws (CREATE_MARKET) | да | через raw() |
| WebSocket | createLimitOrder /cws (CREATE_LIMIT) | да | через raw() |
| WebSocket | createStopMarketOrder /cws (CREATE_STOP) | да | через raw() |
| WebSocket | createStopLimitOrder /cws (CREATE_STOP_LIMIT) | да | через raw() |
| WebSocket | updateMarketOrder /cws (UPDATE_MARKET) | да | через raw() |
| WebSocket | updateLimitOrder /cws (UPDATE_LIMIT) | да | через raw() |
| WebSocket | updateStopMarketOrder /cws (UPDATE_STOP) | да | через raw() |
| WebSocket | updateStopLimitOrder /cws (UPDATE_STOP_LIMIT) | да | через raw() |
| WebSocket | deleteMarketOrder /cws (DELETE_MARKET) | да | через raw() |
| WebSocket | deleteLimitOrder /cws (DELETE_LIMIT) | да | через raw() |
| WebSocket | deleteStopMarketOrder /cws (DELETE_STOP) | да | через raw() |
| WebSocket | deleteStopLimitOrder /cws (DELETE_STOP_LIMIT) | да | через raw() |

