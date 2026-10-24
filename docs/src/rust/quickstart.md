# Быстрый старт на Rust

Цель страницы: за один заход получить работающую программу, которая подключается к брокеру, смотрит стакан,
ставит одну заявку далеко от рынка, читает события и снимает заявку. Для каждого брокера свой короткий `main`,
а общая часть одна.

> На настоящем счете библиотеку пока не запускали. Начинайте с песочницы Т-Банка. Для БКС и Финама песочницы нет, заявка уйдет на
> настоящий счет (у АЛОРа есть тестовый контур со своим счетом): ставьте минимальное количество и цену **заведомо ниже рынка**, кратную шагу цены.
> Подробнее: [Перед боевым счетом](../before-live.md).

## Шаг 1. Получить библиотеку

Нужен Rust 1.88 или новее (редакция 2024). Подключается один крейт `brokapi`, брокеры включаются флагами, как у `tokio`
(`tbank`, `bcs`, `finam`, `alor`, можно несколько):

```bash
cargo add brokapi --features tbank
cargo add tokio --features full
cargo add dotenvy
```

Самая свежая версия из репозитория - вместо первой строки (подробности и проверка - на странице [Установка](../install.md)):

```bash
cargo add --git https://github.com/runinfra/brokapi brokapi --features tbank
```

Должно получиться в `Cargo.toml`:

```toml
[dependencies]
brokapi = { git = "https://github.com/runinfra/brokapi", version = "{{brokapi_version}}", features = ["tbank"] }   # после беты без git
tokio   = { version = "1", features = ["full"] }
dotenvy = "0.15"
```

Из клона репозитория подключают по пути: `brokapi = { path = "../brokapi", features = ["tbank"] }`. В коде все лежит под
`brokapi::`: общее в `brokapi::core` и `brokapi::runtime`, брокеры в `brokapi::bcs`, `brokapi::tbank`, `brokapi::finam`, `brokapi::alor`, их
клиенты нижнего слоя в `brokapi::bcs::protocol` и так далее. `cargo build` должен собрать проект без ошибок. Не собрал:
таблица «Если что-то пошло не так» на странице [Установка](../install.md#если-что-то-пошло-не-так).

Что делает каждый крейт: [Карта](../architecture.md#слои).

## Сначала без токена: на имитации

Токен и счет нужны не сразу. В клоне репозитория (`git clone https://github.com/runinfra/brokapi`) пример БКС запускается на имитации, которая
живет в вашем процессе и говорит на протоколе БКС: без сети, без счета, без денег.

```bash
cargo run --example quickstart_bcs                 # имитация: токен не нужен
BROKAPI_LIVE=1 cargo run --example quickstart_bcs  # настоящий БКС: токен на запись и слово владельца счета
```

Пример читает портфель, стакан и свечи, ставит заявку далеко от рынка и снимает ее: ровно то, что делают программы ниже. **Цена в имитации стоит на
месте**, заявка сама не исполнится, ее исполняет тест (`server.fill`). Имитации всех четырех брокеров, рычаги и ограничения описаны на странице
[Тесты против имитаций, Rust](mocks.md). Крейт `brokapi-mocks` в crates.io не публикуется, поэтому в своем проекте его подключают по пути к клону.
Токены и счет для следующих шагов выдает человек, чей это счет: [правило 0](../agent-contract.md#правило-0-токен-и-первая-боевая-заявка-только-от-человека).

## Шаг 2. Переменные `.env`

Файл `.env` лежит рядом с программой, в git не попадает. Токены не печатайте в логи и не кладите в коммиты.

| Брокер | Переменная | Для чего |
|---|---|---|
| Т-Банк | `TBANK_TOKEN` | токен (обязательно). Для первого запуска - токен песочницы |
| | `TBANK_SANDBOX` | `true` или `1` - песочница. Без нее адрес боевой |
| | `TBANK_ACCOUNT_ID` | счет. Если не задан, берется единственный открытый счет токена, а при нескольких счетах коннектор не запустится |
| | `TBANK_ENDPOINT`, `TBANK_APP_NAME` | необязательно: свой адрес, имя приложения в заголовке |
| БКС | `BCS_REFRESH_TOKEN` | refresh-токен (обязательно). Счет определяет сам токен |
| | `BCS_IS_WRITE` | `true` - токен на запись. Без нее торговые команды брокер не примет |
| Финам | `FINAM_SECRET` | секрет Trade API (обязательно) |
| | `FINAM_ACCOUNT_ID` | счет (обязательно для UD) |
| | `FINAM_API_URL` | необязательно: другой адрес |
| АЛОР | `ALOR_REFRESH_TOKEN` | refresh-токен с портала разработчика (обязательно) |
| | `ALOR_PORTFOLIO` | портфель (обязательно, если токен называет несколько) |
| | `ALOR_EXCHANGE`, `ALOR_ENV` | необязательно: биржа `MOEX` (по умолчанию) или `SPBX`, `ALOR_ENV=test` - тестовый контур |
| Пример | `EXAMPLE_PRICE` | цена лимитной заявки в примерах ниже. Это переменная примера, библиотека ее не читает |

Счет Т-Банка в песочнице нужно открыть. Список своих счетов: `TbankClient::new(&cfg)?.list_accounts().await`
(`brokapi::tbank::protocol::TbankClient`). Открывает счет песочницы метод `open_sandbox_account` клиента `sandbox()`,
пример в `crates/tbank/tests/live_sandbox.rs`.

## Шаг 3. Общая часть

Эта функция одинакова для всех брокеров, потому что работает через общие трейты `OrderBookSource` и
`OrderExecutor`. Положите ее в тот же файл, что и `main` нужного брокера.

```rust
use std::time::Duration;

use brokapi::core::{
    Backpressure, ClientOrderId, Envelope, Error, Event, InstrumentId, OrderBook, OrderBookSource,
    OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, Side, SubscribeOpts,
};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Читает события из общего канала заданное время и печатает самое интересное.
async fn show_events(events: &mut mpsc::Receiver<Envelope>, how_long: Duration) {
    let _ = timeout(how_long, async {
        while let Some(env) = events.recv().await {
            match env.payload {
                Event::OrderBook(OrderBook::Snapshot { bids, asks, .. }) => println!(
                    "[{} sub {:?}] стакан: покупка {:?}, продажа {:?}",
                    env.source,
                    env.sub,
                    bids.first().map(|l| l.price),
                    asks.first().map(|l| l.price),
                ),
                Event::Order(order) => println!("заявка {}: {:?}", order.cid, order.state),
                Event::Execution(fill) => {
                    println!("исполнение {}: {} по {}", fill.trade_id, fill.qty, fill.price)
                }
                Event::Resync { .. } => println!("стакан надо брать заново"),
                Event::Overflow { .. } => println!("буфер переполнился, нужна сверка"),
                _ => {}
            }
        }
    })
    .await;
}

/// Сверка, подписки, одна заявка, события, отмена.
async fn trade_once<M, U>(
    md: &M,
    ud: &U,
    instrument: InstrumentId,
    qty: Qty,
    price: Price,
    events: &mut mpsc::Receiver<Envelope>,
) -> Result<(), Box<dyn std::error::Error>>
where
    M: OrderBookSource,
    U: OrderExecutor,
{
    // 1. Подписки, ДО сверки: событие между снимком сверки и подпиской иначе теряется. Стакан можно
    //    склеивать: важен только свежий. Статусы своих заявок всегда идут без потерь, режим выбирает
    //    сама библиотека.
    let opts = SubscribeOpts { backpressure: Backpressure::Conflate, depth: Some(10) };
    let book = md.subscribe_order_book(&instrument, opts).await?;
    ud.subscribe_order_state().await?;

    // 2. Сверка. Пока она не прошла, UD отвечает NotReconciled на любую торговую команду.
    //    Журнал бота пуст, поэтому список своих заявок пустой.
    let report = ud.reconcile(&[]).await?;
    println!("чужих заявок на счете: {}", report.foreign.len());

    // 3. Одна заявка. Номер cid придумывает бот, не библиотека.
    let cid = ClientOrderId::new_v4();
    let request = PlaceOrderRequest {
        cid,
        instrument: instrument.clone(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty,
        price: Some(price),
    };
    match ud.place(request).await {
        Ok(ack) => println!("заявка принята, номер брокера {}", ack.broker_id.as_str()),
        Err(Error::Rejected { code, message }) => {
            println!("брокер отказал: {code}, {message}");
            return Ok(());
        }
        Err(Error::Unknown) => {
            // Неизвестно, дошла ли заявка. Повторять нельзя: спрашиваем брокера.
            let snapshot = ud.order_state(cid).await?;
            println!("итог был неясен, брокер говорит: {:?}", snapshot.state);
        }
        Err(other) => return Err(other.into()),
    }

    // 4. События: стакан, статус заявки.
    show_events(events, Duration::from_secs(5)).await;

    // 5. Снимаем заявку, дочитываем события, отписываемся.
    ud.cancel(cid).await?;
    show_events(events, Duration::from_secs(2)).await;
    md.unsubscribe(book).await?;
    Ok(())
}
```

Разбор шагов: сверка - [Круг жизни](../reconcile.md), заявка и ее исходы - [Круг команд](../orders.md), режимы подписки -
[Круг событий](../streams.md), события и их поля - [События и возможности](../events.md).

## Шаг 4. Программа для Т-Банка

Количество в **штуках**, как у любого брокера в библиотеке: у Т-Банка API считает лотами, но коннектор переводит сам, лот должен
укладываться в заявку целое число раз (лот `SBER` 10, поэтому ниже заявка на одну бумагу была бы отказана `invalid_qty`).
Цена за одну бумагу. Инструмент ищем через справочник: он отдает `uid`, которым пользуется брокер.

```rust
use brokapi::core::{InstrumentQuery, InstrumentSource, ShutdownMode};
use brokapi::runtime::BrokerLimiter;
use brokapi::tbank::limits::default_limits;
use brokapi::tbank::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi::tbank::protocol::TbankConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TBANK_TOKEN, TBANK_SANDBOX, TBANK_ACCOUNT_ID берутся из .env
    let cfg = TbankConfig::from_env()?;
    let price: Price = std::env::var("EXAMPLE_PRICE")?.parse()?;

    // Один счетчик лимитов на пользователя: Т-Банк считает лимит на все счета и токены сразу.
    let limiter = BrokerLimiter::new(&default_limits());
    // Один канал событий на оба коннектора. Кто прислал, видно по полю source.
    let (tx, mut events) = mpsc::channel::<Envelope>(1024);

    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "tbank-md"),
        limiter.clone(),
        tx.clone(),
    )
    .await?;
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), limiter, tx).await?;

    let sber = md
        .find(InstrumentQuery::ByTicker { ticker: "SBER".into(), exchange: Some("TQBR".into()) })
        .await?
        .into_iter()
        .next()
        .ok_or("SBER не найден")?
        .id;

    // один лот SBER: 10 штук
    trade_once(&md, &ud, sber, Qty::from(10), price, &mut events).await?;

    let stop = ShutdownMode::Graceful { deadline: Duration::from_secs(5) };
    ud.shutdown(stop).await;
    md.shutdown(stop).await;
    Ok(())
}
```

Что важно у Т-Банка: [страница Т-Банка](../brokers/tbank.md).

## Шаг 4. Программа для БКС

Счет определяет токен, поэтому `account_id` нет. Лимиты встроены в крейт: `brokapi::bcs::limits::default_limits()`, как у двух
других брокеров (числа из таблицы ограничений брокера). Инструмент БКС задается тикером и классом (`SBER`, `TQBR`),
справочник для этого не нужен.

```rust
use brokapi::bcs::protocol::config::BcsConfig;
use brokapi::bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi::bcs::limits::default_limits;
use brokapi::core::{InstrumentKey, NativeId, ShutdownMode};
use brokapi::runtime::BrokerLimiter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // BCS_REFRESH_TOKEN и BCS_IS_WRITE берутся из .env
    let cfg = BcsConfig::from_env()?;
    let price: Price = std::env::var("EXAMPLE_PRICE")?.parse()?;

    // Один счетчик лимитов на токен: оба коннектора пользуются одним.
    let limiter = BrokerLimiter::new(&default_limits());
    let (tx, mut events) = mpsc::channel::<Envelope>(1024);

    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "bcs-md"),
        limiter.clone(),
        tx.clone(),
    )
    .await?;
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, tx).await?;

    let sber = InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
    );

    trade_once(&md, &ud, sber, Qty::from(1), price, &mut events).await?;

    let stop = ShutdownMode::Graceful { deadline: Duration::from_secs(5) };
    ud.shutdown(stop).await;
    md.shutdown(stop).await;
    Ok(())
}
```

У БКС заявка идет в штуках (так в документации брокера): [страница БКС](../brokers/bcs.md). Лот инструмента передайте
коннектору (`ud.price_grid().learn(&params)`), тогда некратное количество будет отказано до отправки.

## Шаг 4. Программа для Финама

Количество в штуках. Инструмент задается строкой `SBER@MISX`. Чтобы MD и UD не авторизовались дважды,
создаем один менеджер токенов и отдаем его обоим.

```rust
use brokapi::core::ShutdownMode;
use brokapi::finam::limits::default_limits;
use brokapi::finam::util::instrument_from_symbol;
use brokapi::finam::{api_config_from_env, MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi::runtime::BrokerLimiter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // api_config_from_env сам .env не читает, поэтому загружаем его вручную
    dotenvy::dotenv().ok();
    let (api, account) = api_config_from_env()?;
    let account = account.ok_or("FINAM_ACCOUNT_ID не задан")?;
    let price: Price = std::env::var("EXAMPLE_PRICE")?.parse()?;

    // Один JWT на оба коннектора
    let tokens = api.shared_tokens().await?;
    let api = api.with_tokens(tokens);

    let limiter = BrokerLimiter::new(&default_limits());
    let (tx, mut events) = mpsc::channel::<Envelope>(1024);

    let md = MarketDataConnector::start(
        MarketDataConfig::new(api.clone(), "finam-md"),
        limiter.clone(),
        tx.clone(),
    )
    .await?;
    let ud = UserDataConnector::start(UserDataConfig::new(api, account, "finam-ud"), limiter, tx).await?;

    let sber = instrument_from_symbol("SBER@MISX");

    trade_once(&md, &ud, sber, Qty::from(1), price, &mut events).await?;

    let stop = ShutdownMode::Graceful { deadline: Duration::from_secs(5) };
    ud.shutdown(stop).await;
    md.shutdown(stop).await;
    Ok(())
}
```

Что важно у Финама: [страница Финама](../brokers/finam.md).

## Что произошло по шагам

1. `start` поднял коннектор. У БКС и Финама он сразу проходит авторизацию и возвращает ошибку, если токен не
   подошел. У Т-Банка при заданном счете `start` в сеть не ходит, о неверном токене скажет первый запрос.
2. Подписки дали `SubscriptionId`. По `source` и `sub` в конверте видно, чье это событие. Они сделаны до сверки нарочно.
3. `reconcile(&[])` открыл шлюз команд. До него `place` вернул бы `NotReconciled`.
4. `place` вернул `OrderAck` (или один из исходов, разобранных в [Заявках](../orders.md)).
5. События идут в канал сами. Программа их читает, как только они появились.
6. `cancel` снял заявку, `shutdown` закрыл подписки. **Заявки на бирже при остановке не снимаются**: если
   `cancel` не прошел, заявка останется.
