# Как тестировать код против имитаций брокеров (Rust)

Инструкция. Крейт `brokapi-mocks` запускает в вашем процессе имитации БКС, Т-Банка, Финама, АЛОРа и Bybit. Они говорят на настоящем
протоколе брокера (HTTP и WebSocket у БКС, АЛОРа и Bybit, gRPC у Т-Банка и Финама), поэтому коннектор работает с ними так же, как с брокером:
без сети, без токенов, без денег. Все примеры книги и все программы в `examples/` запускаются против них.

**Имитация проверяет, что ваш код правильно разбирает то, что брокер описал в документации. Она не доказывает, что настоящий
брокер отвечает так же.**

`brokapi-mocks` опубликован на crates.io (с версии 0.1.0-alpha.9) для ваших тестов, но его интерфейс может меняться в любой версии, даже после беты:
рычаги имитаций добавляются по мере того, как появляются сбои, которые стоит проверять. Берите его той же версии, что `brokapi`.

## Подключение

```toml
# edition = "2024" в [package], Rust 1.88 или новее
[dependencies]
brokapi       = { version = "{{brokapi_version}}", features = ["tbank"] }

[dev-dependencies]
brokapi-mocks = "{{brokapi_version}}"
tokio         = { version = "1", features = ["full", "test-util"] }
```

Номер `brokapi-mocks` тот же, что у `brokapi`: имитации и коннекторы одного выпуска знают друг друга. В более ранних выпусках крейта на crates.io нет, тогда
из git по тегу выпуска: `brokapi-mocks = { git = "https://github.com/runinfra/brokapi", tag = "v<версия brokapi>" }`. Так он соберется рядом со своей копией всех крейтов
`brokapi` из git (сборка дольше), и типы двух копий между собой не совместимы. Имитации это не мешает: от нее нужен только адрес сервера и рычаги, а не типы
библиотеки. Из клона репозитория - путь: `brokapi-mocks = { path = "../brokapi/crates/mocks" }`. Тесты - обычные `#[tokio::test]` (среда в одном потоке). Как обращаться со временем в таких
тестах, написано в разделе [Время в тестах](#время-в-тестах).

## Весь тест целиком

Заявка на имитации Т-Банка, исполнение и событие сделки. Его можно скопировать как есть: это файл `tests/mocks_guide.rs`, он запускается в CI.
Частая первая ошибка - забыть `subscribe_executions`: без подписки событий сделок нет, и тест молча ждет до конца таймаута.

```rust
{{#include ../../../tests/mocks_guide.rs:test}}
```

## Запуск каждой имитации

Имитация живет, пока жива переменная `MockServer`, и слушает случайный порт на `127.0.0.1`. Адрес и токен подставляются в
настройку коннектора специальным конструктором `for_mock`.

| | БКС | Т-Банк | Финам | АЛОР |
|---|---|---|---|---|
| Запуск | `brokapi_mocks::bcs::MockServer::start(Default::default()).await` | `brokapi_mocks::tbank::MockServer::start(Default::default()).await` | `brokapi_mocks::finam::MockServer::start().await` | `brokapi_mocks::alor::MockServer::start(Default::default()).await` |
| Адрес | `server.http_base` | `server.endpoint` | `server.url` | `server.http_base` (сервер авторизации по тому же адресу) |
| Настройка клиента | `BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true)` | `TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A)` | `FinamApiConfig::new(SECRET).with_endpoint(server.url.clone())` | `AlorConfig::for_mock("mock-refresh-token", &server.http_base)` (портфель `D00001`, биржа `MOEX`) |
| Счета | один счет на refresh-токен: другое имя после `mock-refresh-` дает отдельный счет | `ACCOUNT_A` и `ACCOUNT_B` с разными данными | `"ACC1"` и `"ACC2"` | один портфель `D00001`, другой refresh-токен дает отдельный счет с пустыми заявками |
| Инструменты | любой тикер, класс `TQBR` | справочник (`find`) знает `SBER` и `GAZP` (`uid-sber`, `uid-gazp`), лот 10. Для неизвестного тикера `find` отвечает **пустым списком**, не ошибкой. Свечи с `synthetic_candles` отдаются и для идентификатора `TickerClass` любого тикера | справочник знает `SBER@MISX`, `GAZP@MISX`. Свечи с `synthetic_candles` отдаются для любого символа | - |

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
// ... рычаги и счетчики лежат в `opts`, например `opts.event_sockets()` - сколько сокетов событий открыто.
```

`BrokerLimiter::new` возвращает `Arc<BrokerLimiter>`: передавайте его дальше через `.clone()`.

Готовые рабочие программы с запуском имитаций: `examples/trading_bot.rs` (БКС и Т-Банк), `examples/infrastructure.rs`
и `examples/research.rs` (все три). Тесты на них: `tests/examples.rs`.

## Время в тестах

Тесты без настоящих пауз: ни `sleep`, ни ожидания «на всякий случай». Часы коннекторов - часы `tokio`: все они берут текущее
время из `brokapi::runtime::now()`, поэтому лимит запросов, таймаут команды и пауза переподключения идут по тем же часам, что и
тест.

Нельзя только одного: останавливать часы на весь тест (`#[tokio::test(start_paused = true)]`), когда имитация слушает настоящий
порт. Пока запрос идет по сокету, у среды нет задач, и остановленные часы сами перескакивают к ближайшему таймеру. Таймаут запроса
срабатывает раньше, чем прочитан ответ. Поэтому у тестов с имитациями часы настоящие, а вместо пауз - два помощника в
`brokapi_mocks::time`:

| Помощник | Зачем |
|---|---|
| `jump(d)` | сдвигает часы вперед на `d` сразу. Лимит запросов, который восстанавливается за секунду, полон после `jump(1 с)`. Таймеры, которые ждали, срабатывают |
| `jump_until("что ждем", шаг, \|\| условие)` | сдвигает часы на шаг снова и снова, пока условие не станет истинным. Для ожидания того, что перед действием спит, например пауза 0,5 с перед переоткрытием потока. Вызывать, когда запросов в пути нет |
| `wait_until("что ждем", \|\| условие)` | ждет, пока условие станет истинным (счетчик имитации, событие), без пауз. Через 10 секунд роняет тест и называет, чего не дождался. `wait_until_async` - то же для условия, которое спрашивают вызовом коннектора |
| `elapse_when("что ждем", \|\| запрос дошел, d, вызов)` | выполняет вызов, а когда запрос дошел до имитации, сдвигает часы на `d`. Так проверяют `Unknown` по таймауту без ожидания таймаута: имитация держит ответ дольше (`post_delay_ms`), чем таймаут клиента, а часы перескакивают таймаут за ноль секунд |

Про «ничего не произошло» (чужой счет не получил событие, повтора не было) вместо «подождали и не увидели» проверяют так: после
действия делают что-то, что приходит заведомо позже (событие по своему счету, круг запроса к брокеру), и смотрят, что
между ними лишнего нет.

В готовых тестах книги так и сделано: `crates/*/tests/`.

## Как оборвать запись после заданного байта

С версии 0.1.0-alpha.10 `brokapi_mocks::io::FaultyWrite::new(stream)` оборачивает ваш поток Tokio и возвращает поток с
рычагом ошибки. Вызовите `fault.fail_after(n)` перед нужной записью: первые `n` байтов пройдут, следующая запись вернет
`BrokenPipe`. Счетчики `written()` и `failures()` показывают отправленные байты и ошибки этого рычага.

Так проверяют собственный код на `AsyncRead`/`AsyncWrite`: что он делает после частичного кадра, закрывает ли соединение
и не повторяет ли торговую команду. Полный запускаемый пример - в [справочнике модуля `io`](../api/brokapi_mocks/io/index.html).
Рычаг работает и с TCP, и с потоком `tokio::io::duplex`, поэтому момент ошибки не зависит от планировщика сети.

## Константы и пути

| Что | Где | Значение |
|---|---|---|
| Токен Т-Банка | `brokapi_mocks::tbank::TOKEN` | `"mock-token"` |
| Счета Т-Банка | `brokapi_mocks::tbank::{ACCOUNT_A, ACCOUNT_B}` | `"acc-a"`, `"acc-b"` |
| Секрет Финама | `brokapi_mocks::finam::SECRET` | `"mock-secret"` |
| Счета Финама | строки | `"ACC1"`, `"ACC2"` |
| Refresh-токен БКС | строка | `"mock-refresh-token"` (токен доступа имитации `brokapi_mocks::bcs::DEFAULT_ACCESS`) |
| Refresh-токен и портфель АЛОРа | `brokapi_mocks::alor::PORTFOLIO` | `"mock-refresh-token"`, `"D00001"` (токен доступа `brokapi_mocks::alor::DEFAULT_ACCESS`, лот бумаг `brokapi_mocks::alor::LOT` = 10) |
| Цена, которую имитации отклоняют | `brokapi_conformance::REJECT_PRICE` | `13.37` (можно писать число) |
| Цена, отклоняемая в теле ответа (БКС) | `brokapi_mocks::bcs::REJECT_IN_BODY_PRICE` | `13.38` |

## Какие данные отдают имитации

- **Стакан.** БКС (чтение по REST и поток): лучшая цена покупки 244.30, продажи 244.35. **Подписка на стакан БКС дает один снимок**
  и дальше тихо: бот, которому нужен повтор по таймеру, не дождется второго снимка. Т-Банк: покупка около 273.90, продажа около 274.00, при подписке
  приходит несколько снимков. Финам (чтение по REST): покупка 100.0, продажа 100.1, поток снимков с периодом `tick_ms`. Имитации принимают любую цену, кроме отклоняемой.
- **Свечи и бары.** По умолчанию фиксированный короткий ответ. Включите `synthetic_candles` (опция имитации), и имитация отдаст ряд под
  заданный период по одному бару на шаг таймфрейма и **отклонит период длиннее предела брокера** (БКС: больше 1440 баров, Т-Банк:
  `INVALID_ARGUMENT 30014` по таблице пределов). Так проверяется выгрузка окнами.
- **АЛОР.** Стакан: покупка 244.30, продажа 244.31, подписка на стакан дает один снимок. Бумаги `SBER`, `GAZP` (режим `TQBR`, лот 10, шаг 0.01) и фьючерс `SiZ6`, тикер `NOPE` неизвестен. Портфель: рубли и 30 штук `SBER`. Свечи всегда рядом по сетке таймфрейма на весь запрошенный период. Повтор `X-REQID` получает копию первого ответа, как сказано в документации брокера.
- **Заявки.** Имитации помнят заявки: их можно поставить, прочитать, снять, заменить (где брокер умеет). Данные счета отдельные.
- **Исполнения двигают счет.** После `fill` позиция по инструменту меняется (чтение портфеля и поток портфеля), каждое исполнение - своя сделка
  истории со своим номером. Деньги не меняются.
- **Цена по команде теста.** `prices.set(тикер, цена)` (модуль `brokapi_mocks::market`): стакан строится вокруг цены (лучшая покупка на 0.01 ниже,
  продажа на 0.01 выше), сделки и бары идут по цене. Без цены остаются записанные числа выше.

## Имитация Bybit

Таблицы выше про четырех российских брокеров. Имитация Bybit устроена так же, отличия коротко.

```rust,ignore
use brokapi::bybit::protocol::config::BybitConfig;

let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
let cfg = BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
// MarketDataConfig::new(cfg.clone(), "bybit-md"), UserDataConfig::new(cfg, "bybit-ud"): дальше как у остальных.
```

- **Подпись настоящая.** Имитация проверяет подпись каждого приватного запроса и окно времени (`recv_window`), как биржа: клиент, который подписывает неверно, получает отказ. Ключ `brokapi_mocks::bybit::DEFAULT_KEY` (`"mock-key"`), секрет `SECRET` (`"mock-secret"`). Другой ключ - отдельный счет со своими заявками.
- **Инструменты.** `MOCKUSDT` (бессрочный, шаг количества и самое малое количество 10, шаг цены 0.01, `LOT` = 10), `BTCUSDT` (числа настоящего контракта: шаг 0.001, цена 0.10) и срочный фьючерс `BTC-26DEC26`. Тикер `NOPEUSDT` (`UNKNOWN_SYMBOL`) неизвестен: чтение дает код 10001.
- **Заявки и рычаги.** `server.fill(&cid.hyphenated().to_string(), qty, price)` исполняет заявку (статус, исполнение, события). `server.cut_event_sockets()` обрывает приватные сокеты (бот получает `Event::Reconnected`), `server.push_event(текст)` пишет сырое сообщение в приватные сокеты. Лимитная заявка по цене `brokapi_conformance::REJECT_PRICE` (13.37) отвергается кодом 110003.
- **Настройки** (`MockOptions`): `rate_limit_order_book` (чтение стакана отвечает HTTP 429), `order_delay_ms` (команда ждет столько по часам теста, для `Unknown` по таймауту), `stop_answer_lost` (стоп принят, ответ потерян: HTTP 503), `order_answer_lost` (то же с обычной заявкой), `prices` (цена тикера, за ней идут стакан, сделки и бары), `clock_offset_ms` (часы биржи уходят вперед), `refuse_ws_login`, `fail_subscribe_text`, `stream_gate` (биржа медленно открывает потоки), `disconnect_after_data_frames`.
- **Свечи.** Ряд под запрошенный период, по одной свече на шаг интервала (`synthetic_candles` не нужен).

## Рычаги, которыми вызывают нужный исход

| Что получить | БКС | Т-Банк | Финам | АЛОР |
|---|---|---|---|---|
| Исполнить заявку | `server.fill(&cid.hyphenated().to_string(), qty, price)` | `server.fill(ACCOUNT_A, &cid.hyphenated().to_string())` | `server.state.fill(счет, комментарий заявки, qty, price)`: комментарий - `cid` в коде Финама (`FinamCidCodec`) | `server.fill(&cid.hyphenated().to_string(), qty, price)`: сначала событие заявки, потом сделка |
| `Rejected` на постановке (цена задается в тесте, не берется из стакана) | лимитная цена `brokapi_conformance::REJECT_PRICE` (13.37), в теле ответа 200 - цена 13.38 (`mbcs::REJECT_IN_BODY_PRICE`) | `REJECT_PRICE` (13.37) | `REJECT_PRICE` (13.37) | `REJECT_PRICE` (13.37), ответ HTTP 400 |
| Брокер принял заявку, ответ потерян (`Unknown`, заявка у брокера) | `opts.order_answer_lost` (HTTP 503) | `opts.order_answer_lost` (`UNAVAILABLE`) | `server.state.options.order_answer_lost` | `opts.order_answer_lost` (HTTP 503) |
| Цена тикера, за которой идут стакан, сделки и бары | `opts.prices.set("SBER", цена)`: открытые подписки на тикер получают новое сообщение | `opts.prices.set(...)` | `server.state.options.prices.set(...)`: следующее сообщение потока по таймеру | `opts.prices.set(...)` |
| `Unknown` на постановке | `opts.order_delay_ms` больше, чем `cfg.request_timeout` клиента (имитация держит команду это время по часам теста). Пример: `a_place_without_an_answer_is_unknown` в `crates/bcs/tests/conformance.rs` | `opts.post_delay_ms` больше, чем `with_request_timeout(...)` клиента. Либо `opts.post_fail` с кодом gRPC | `opts.place_delay_ms` больше таймаута клиента, либо `opts.place_error` | `opts.order_delay_ms` больше, чем `request_timeout` клиента |
| Обрыв потока стакана | `opts.disconnect_after_data_frames` | `opts.md_cut_after` | `opts.book_drop_after` | `opts.disconnect_after_data_frames` |
| Брокер медленно открывает поток (`subscribe_*` должен ждать, а не вернуться раньше) | `opts.stream_gate.hold()` до старта коннектора: рукопожатие сокетов заявок и портфеля не отвечается. `opts.stream_gate.release()` отпускает. `opts.stream_gate.waiting()` - сколько запросов держится. Подписка рынка держится так же | `opts.stream_gate.hold()`: потоки заявок, исполнений и портфеля не открываются. Рынок: `opts.md_silent = true`, подтверждения подписок не приходят | `server.state.options.stream_gate.hold()`: открытие любого потока не возвращается | `opts.stream_gate.hold()`: подписки на сокете не обрабатываются и не подтверждаются |
| Обрыв потоков своих заявок и исполнений (`Event::Reconnected`) | `server.cut_event_sockets()`, подключения считает `opts.ws_connections()` | `server.cut_user_streams()`, открытия считает `opts.user_stream_opens` | `server.state.cut_user_streams()`, открытия считает `server.state.order_stream_opens` | `server.cut_event_sockets()`, подключения считает `opts.ws_connections()` |
| Статус 6 «Отменяется» | `opts.cancel_pending`, закончить отмену `server.finish_cancel(cid)` | нет | нет | - |
| Исполнения с накопленными итогами | `server.fill(cid, qty, price)` шлет `executedQuantity`, `executionValue`, `averagePrice` как брокер | нет | нет | - |
| Длинная история сделок | `server.add_history_trades(n, цена, количество)` (список листается по `page` и `size`, не больше 1000 последних) | `opts.operations_pages` | `server.state.push_trade(...)`, ответ режется по `limit` и интервалу | `server.add_history_trades(n, цена, количество)` (один ответ до 1000) |
| Живая заявка, которой нет в списке | `server.hide_from_list(cid)` | нет | нет | - |
| Заявка, поставленная кем-то другим | `server.fill`/ручной вызов | `server.place_foreign(счет, request_id, инструмент, лоты)` | `server.state.push_order(...)` | `raw().call_command(...)` с чужим комментарием |
| Лот инструмента нельзя прочитать | нет | `opts.instrument_by_fail`, `opts.omit_lot_size` | нет | - |
| `Overflow` | канал на одно место, никто не читает, `cfg.order_state_buffer = 2` и несколько исполнений подряд | то же | то же | то же |
| `RateLimited` | малый бюджет: свои числа в `BrokerLimiter::new` ([формат файла](../limits.md#свои-числа-формат-файла)). «Один отказ, потом проходит»: `burst = 1`, длинный `per` | то же | то же | то же |
| Чтение свечей падает | не предусмотрено | `opts.candle_fail`: `1` - отказ навсегда (`Rejected`), `2` - сбой сети (`Transport`), `0` - норма. `opts.candle_network_failures`: сколько следующих чтений упадут по сети, потом снова норма. Только с `synthetic_candles` | не предусмотрено | - |
| Свечи по заданному периоду | `opts.synthetic_candles` | `opts.synthetic_candles` | `server.state.options.synthetic_candles` | всегда: ряд по сетке, `opts.history_requests` хранит запросы |

`opts` - это `Arc<MockOptions>`, который передается в `start`. Для Финама рычаги лежат в `server.state.options`. Все поля - атомарные
числа и флаги, их можно менять, пока имитация работает.

## Ловушки тестов

- **`subscribe_*` возвращается, когда поток открыт.** Ждать подключения сокета (`event_sockets()`) после `subscribe_order_state` и
  `subscribe_executions` не нужно: заявка, поставленная сразу после возврата, не пропадет. Событие, случившееся до подписки, теряется,
  его читают через `order_state` или `active_orders`. Подписка раньше заявки - правило и для имитации.
- **Имитация сервера должна жить столько же, сколько коннекторы.** Положите ее в переменную, которая не уходит из области раньше.
- **Рестарт процесса** проверяется двумя жизнями коннекторов при одной имитации: имитация помнит заявки между ними
  ([инфраструктура](howto-infrastructure.md)).
- **Таймауты ожидания в тестах** задавайте с запасом на медленный CI, но не бесконечные: зависший тест хуже упавшего. Они
  срабатывают только когда тест упал, поэтому на скорость прогона не влияют.
- Имитации не знают реальных лимитов брокеров и реальной скорости. Лимиты запросов проверяйте своим малым бюджетом.

## Чего имитации не покажут

Реальные форматы чисел и времени, реальные лимиты и ошибки, поведение брокера на повтор `cid`, единицы количества у БКС. Проверяйте эти места
на минимальной заявке.
