# brokapi

[Что это](README.md)

# Как устроено

- [Карта: сущности и три круга](architecture.md)
  - [Круг событий: подписки, буферы, обрывы](streams.md)
  - [Круг команд: заявки](orders.md)
  - [Круг жизни: шлюз и сверка](reconcile.md)
  - [Лимиты запросов](limits.md)
  - [Деньги, цены и количества](numbers.md)

# Брокеры

- [Брокеры](brokers/README.md)
  - [БКС](brokers/bcs.md)
  - [Т-Банк](brokers/tbank.md)
  - [Финам](brokers/finam.md)

# Rust

- [brokapi на Rust](rust/index.md)
  - [Быстрый старт](rust/quickstart.md)
  - [Торговый бот на событиях](rust/howto-trading-bot.md)
  - [Несколько брокеров и счетов: инфраструктура](rust/howto-infrastructure.md)
  - [Исследовательский движок](rust/howto-research.md)
  - [Запуск, рестарт и остановка бота](rust/lifecycle.md)
  - [Если что-то пошло не так](rust/recovery.md)
  - [Несколько счетов и брокеров](rust/accounts.md)
  - [Стоп-заявки](rust/stop-orders.md)
  - [Любой метод брокера: raw](rust/raw.md)
  - [Тесты против имитаций](rust/mocks.md)

# Python

- [brokapi для Python](python/index.md)

# Gateway

- [brokapi gateway](gateway/index.md)

# MCP

- [brokapi MCP](mcp/index.md)

# Справочник

- [Контракт для агентов](agent-contract.md)
- [Ошибки и что с ними делать](errors.md)
- [События и возможности](events.md)
- [Покрытие API](coverage.md)
- [Справочник API]()
  - [Rust](api.md)
  - [Python](api-python.md)
  - [Gateway (gRPC)](api-gateway.md)

# Состояние

- [Готовность](readiness.md)
- [Живые прогоны](live.md)
- [Изменения](changelog.md)
