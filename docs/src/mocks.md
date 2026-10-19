# Как тестировать код против имитаций брокеров

Инструкция. Крейт `brokapi-mocks` запускает в вашем процессе имитации БКС, Т-Банка и Финама. Они говорят на настоящем
протоколе брокера (HTTP и WebSocket у БКС, gRPC у Т-Банка и Финама), поэтому коннектор работает с ними так же, как с брокером:
без сети, без токенов, без денег. Все примеры книги и все программы в `examples/` запускаются против них.

**Имитация проверяет, что ваш код правильно разбирает то, что брокер описал в документации. Она не доказывает, что настоящий
брокер отвечает так же.** Что проверено вживую, а что нет: [Готовность](readiness.md).

`brokapi-mocks` не входит в обещания библиотеки (`publish = false`): его интерфейс может меняться в любой версии. Он нужен
тестам и примерам.

## Подключение

```toml
[dev-dependencies]
brokapi       = { path = "../brokapi", features = ["bcs", "tbank", "finam"] }
brokapi-mocks = { path = "../brokapi/crates/mocks" }
tokio         = { version = "1", features = ["full"] }
```

Путь указывает на клон репозитория. Тесты на `tokio` с многопоточной средой: `#[tokio::test(flavor = "multi_thread")]`.

## Запуск каждой имитации

Имитация живет, пока жива переменная `MockServer`, и слушает случайный порт на `127.0.0.1`. Адрес и токен подставляются в
настройку коннектора специальным конструктором `for_mock`.

| | БКС | Т-Банк | Финам |
|---|---|---|---|
| Запуск | `brokapi_mocks::bcs::MockServer::start(Default::default()).await` | `brokapi_mocks::tbank::MockServer::start(Default::default()).await` | `brokapi_mocks::finam::MockServer::start().await` |
| Адрес | `server.http_base` | `server.endpoint` | `server.url` |
| Настройка клиента | `BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true)` | `TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A)` | `FinamApiConfig::new(SECRET).with_endpoint(server.url.clone())` |
| Счета | один счет на refresh-токен: другое имя после `mock-refresh-` дает отдельный счет | `ACCOUNT_A` и `ACCOUNT_B` с разными данными | `"ACC1"` и `"ACC2"` |
| Инструменты | любой тикер, класс `TQBR` | `SBER` и `GAZP` (`uid-sber`, `uid-gazp`), лот 10 | `SBER@MISX`, `GAZP@MISX` |

```rust,ignore
use std::sync::Arc;
use brokapi::bcs::protocol::config::BcsConfig;
use brokapi::bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi::bcs::limits::default_limits;
use brokapi::runtime::BrokerLimiter;
use tokio::sync::mpsc;

let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
let limiter = BrokerLimiter::new(&default_limits());
let (tx, mut events) = mpsc::channel(1024);
let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, tx).await?;
ud.reconcile(&[]).await?;   // обязательно, как и с настоящим брокером
```

Чтобы читать счетчики и менять рычаги работающей имитации (`event_sockets()`, `synthetic_candles` и другие), создайте настройки сами и
передайте `Arc` в `start`. Тип надо назвать: `Arc::new(Default::default())` компилятор не выведет.

```rust,ignore
use std::sync::Arc;
use brokapi_mocks::bcs::{MockOptions, MockServer};

let opts: Arc<MockOptions> = Arc::default();
let server = MockServer::start(opts.clone()).await;
// ... после подписки на статусы дождаться, пока сокет событий подключится:
while opts.event_sockets() == 0 {
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}
```

`BrokerLimiter::new` возвращает `Arc<BrokerLimiter>`: передавайте его дальше через `.clone()`.

Готовые рабочие программы с запуском всех трех имитаций: `examples/trading_bot.rs` (БКС и Т-Банк), `examples/infrastructure.rs`
и `examples/research.rs` (все три). Тесты на них: `tests/examples.rs`.

## Константы и пути

| Что | Где | Значение |
|---|---|---|
| Токен Т-Банка | `brokapi_mocks::tbank::TOKEN` | `"mock-token"` |
| Счета Т-Банка | `brokapi_mocks::tbank::{ACCOUNT_A, ACCOUNT_B}` | `"acc-a"`, `"acc-b"` |
| Секрет Финама | `brokapi_mocks::finam::SECRET` | `"mock-secret"` |
| Счета Финама | строки | `"ACC1"`, `"ACC2"` |
| Refresh-токен БКС | строка | `"mock-refresh-token"` (токен доступа имитации `brokapi_mocks::bcs::DEFAULT_ACCESS`) |
| Цена, которую имитации отклоняют | `brokapi_conformance::REJECT_PRICE` | `13.37` (можно писать число) |
| Цена, отклоняемая в теле ответа (БКС) | `brokapi_mocks::bcs::REJECT_IN_BODY_PRICE` | `13.38` |

## Какие данные отдают имитации

- **Стакан.** БКС (чтение по REST и поток): лучшая цена покупки 244.30, продажи 244.35. **Подписка на стакан БКС дает один снимок**
  и дальше тихо: бот, которому нужен повтор по таймеру, не дождется второго снимка (настоящий брокер шлет снимок при каждом изменении стакана, не проверено вживую). Т-Банк: покупка около 273.90, продажа около 274.00, при подписке
  приходит несколько снимков. Финам (чтение по REST): покупка 100.0, продажа 100.1, поток снимков с периодом `tick_ms`. Имитации принимают любую цену, кроме отклоняемой.
- **Свечи и бары.** По умолчанию фиксированный короткий ответ. Включите `synthetic_candles` (опция имитации), и имитация отдаст ряд под
  заданный период по одному бару на шаг таймфрейма и **отклонит период длиннее предела брокера** (БКС: больше 1440 баров, Т-Банк:
  `INVALID_ARGUMENT 30014` по таблице пределов). Так проверяется выгрузка окнами.
- **Заявки.** Имитации помнят заявки: их можно поставить, прочитать, снять, заменить (где брокер умеет). Данные счета отдельные.

## Рычаги, которыми вызывают нужный исход

| Что получить | БКС | Т-Банк | Финам |
|---|---|---|---|
| Исполнить заявку | `server.fill(&cid.hyphenated().to_string(), qty, price)` | `server.fill(ACCOUNT_A, &cid.hyphenated().to_string())` | вручную: `server.state.push_order(счет, ...)` |
| `Rejected` на постановке (цена задается в тесте, не берется из стакана) | лимитная цена `brokapi_conformance::REJECT_PRICE` (13.37), в теле ответа 200 - цена 13.38 (`mbcs::REJECT_IN_BODY_PRICE`) | `REJECT_PRICE` (13.37) | `REJECT_PRICE` (13.37) |
| `Unknown` на постановке | **рычага нет**. Проверяйте этот путь на имитации Т-Банка или Финама | `opts.post_delay_ms` больше, чем `with_request_timeout(...)` клиента. Либо `opts.post_fail` с кодом gRPC | `opts.place_delay_ms` больше таймаута клиента, либо `opts.place_error` |
| Обрыв потока | `opts.disconnect_after_data_frames` | `opts.md_cut_after` | `opts.book_drop_after` |
| `Overflow` | канал на одно место, никто не читает, `cfg.order_state_buffer = 2` и несколько исполнений подряд | то же | то же |
| `RateLimited` | малый бюджет: свои числа в `BrokerLimiter::new` ([формат файла](limits.md#свои-числа-формат-файла)). «Один отказ, потом проходит»: `burst = 1`, длинный `per` | то же | то же |
| Свечи по заданному периоду | `opts.synthetic_candles` | `opts.synthetic_candles` | `server.state.options.synthetic_candles` |

`opts` - это `Arc<MockOptions>`, который передается в `start`. Для Финама рычаги лежат в `server.state.options`. Все поля - атомарные
числа и флаги, их можно менять, пока имитация работает.

## Ловушки тестов

- **Подписка на статусы заявок у БКС подключается в фоне.** Событие, случившееся до подключения сокета, теряется. Перед заявкой
  дождитесь `opts.event_sockets() > 0` (так сделано в примерах из справочника), либо читайте состояние через `order_state`.
- **Имитация сервера должна жить столько же, сколько коннекторы.** Положите ее в переменную, которая не уходит из области раньше.
- **Рестарт процесса** проверяется двумя жизнями коннекторов при одной имитации: имитация помнит заявки между ними
  ([инфраструктура](howto-infrastructure.md)).
- **Таймауты в тестах** задавайте с запасом на медленный CI, но не бесконечные: зависший тест хуже упавшего.
- Имитации не знают реальных лимитов брокеров и реальной скорости. Лимиты запросов проверяйте своим малым бюджетом.

## Чего имитации не покажут

Реальные форматы чисел и времени, реальные лимиты и ошибки, поведение брокера на повтор `cid`, единицы количества у БКС. Это
вопросы к живому счету: [Живые прогоны](live.md).
