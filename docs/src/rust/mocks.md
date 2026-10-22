# Как тестировать код против имитаций брокеров (Rust)

Инструкция. Крейт `brokapi-mocks` запускает в вашем процессе имитации БКС, Т-Банка, Финама и АЛОРа. Они говорят на настоящем
протоколе брокера (HTTP и WebSocket у БКС и АЛОРа, gRPC у Т-Банка и Финама), поэтому коннектор работает с ними так же, как с брокером:
без сети, без токенов, без денег. Все примеры книги и все программы в `examples/` запускаются против них.

**Имитация проверяет, что ваш код правильно разбирает то, что брокер описал в документации. Она не доказывает, что настоящий
брокер отвечает так же.**

`brokapi-mocks` не входит в обещания библиотеки (`publish = false`): его интерфейс может меняться в любой версии. Он нужен
тестам и примерам.

## Подключение

```toml
# edition = "2024" в [package], Rust 1.88 или новее
[dev-dependencies]
brokapi       = { path = "../brokapi", features = ["bcs", "tbank", "finam", "alor"] }
brokapi-mocks = { path = "../brokapi/crates/mocks" }
tokio         = { version = "1", features = ["full", "test-util"] }
```

Путь указывает на клон репозитория. Тесты - обычные `#[tokio::test]` (среда в одном потоке). Как обращаться со временем в таких
тестах, написано в разделе [Время в тестах](#время-в-тестах).

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

## Рычаги, которыми вызывают нужный исход

| Что получить | БКС | Т-Банк | Финам | АЛОР |
|---|---|---|---|---|
| Исполнить заявку | `server.fill(&cid.hyphenated().to_string(), qty, price)` | `server.fill(ACCOUNT_A, &cid.hyphenated().to_string())` | вручную: `server.state.push_order(счет, ...)` | `server.fill(&cid.hyphenated().to_string(), qty, price)`: сначала событие заявки, потом сделка |
| `Rejected` на постановке (цена задается в тесте, не берется из стакана) | лимитная цена `brokapi_conformance::REJECT_PRICE` (13.37), в теле ответа 200 - цена 13.38 (`mbcs::REJECT_IN_BODY_PRICE`) | `REJECT_PRICE` (13.37) | `REJECT_PRICE` (13.37) | `REJECT_PRICE` (13.37), ответ HTTP 400 |
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
