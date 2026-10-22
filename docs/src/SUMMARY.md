# brokapi

[Что это](README.md)

# Начать

- [Установка](install.md)

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
  - [АЛОР](brokers/alor.md)

# Rust

- [brokapi на Rust](rust/index.md)
  - [Быстрый старт на Rust](rust/quickstart.md)
  - [Торговый бот на событиях, Rust](rust/howto-trading-bot.md)
  - [Несколько брокеров и счетов: инфраструктура, Rust](rust/howto-infrastructure.md)
  - [Исследовательский движок](rust/howto-research.md)
  - [Запуск, рестарт и остановка бота](rust/lifecycle.md)
  - [Если что-то пошло не так](rust/recovery.md)
  - [Несколько счетов и брокеров](rust/accounts.md)
  - [Стоп-заявки](rust/stop-orders.md)
  - [Любой метод брокера: raw](rust/raw.md)
  - [Тесты против имитаций, Rust](rust/mocks.md)

# Python

- [brokapi для Python](python/index.md)
  - [Быстрый старт на Python](python/quickstart.md)
  - [Торговый бот на событиях, Python](python/howto-trading-bot.md)
  - [Несколько брокеров и счетов, Python](python/howto-infrastructure.md)
  - [История свечей](python/howto-history.md)
  - [Стоп-заявки, Python](python/stop-orders.md)
  - [Любой метод брокера, Python](python/raw.md)
  - [Запись событий и проигрыш, Python](python/record.md)
  - [Тесты против имитаций, Python](python/testing.md)
  - [Как Python связан с ядром](python/design.md)

# Gateway

- [Gateway](gateway/index.md)

# CLI

- [CLI](cli/index.md)

# Справочник

- [Контракт для агентов](agent-contract.md)
- [Ошибки и что с ними делать](errors.md)
- [События и возможности](events.md)
- [Покрытие API](coverage.md)
- [Справочник API]()
  - [Справочник Rust](api.md)
  - [Справочник Python](api-python.md)
  - [Gateway (gRPC)](api-gateway.md)

# Состояние

- [Готовность](readiness.md)
- [Живые прогоны](live.md)
- [Изменения](changelog.md)
