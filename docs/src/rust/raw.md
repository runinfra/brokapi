# Любой метод брокера: raw

Общий интерфейс закрывает то, что умеют все брокеры. Но у каждого брокера есть много своего: купоны и
дивиденды, отчеты, избранное, корпоративные действия, свежие методы. Чтобы они не оказались закрыты, у каждого
коннектора (MD и UD, у всех брокеров) есть метод `raw()`. Он позволяет вызвать **любой метод API брокера**
с теми же токеном, адресом и счетчиком лимитов, что у самого коннектора.

| Брокер | Тип | Что получает ваш код в замыкании |
|---|---|---|
| Т-Банк | `TbankRaw` | `TbankClient` с клиентами всех сервисов (`users()`, `market_data()`, `instruments()`...) |
| БКС | `BcsRaw` | `BcsHttpClient` (REST), плюс `connect_ws` для WebSocket-каналов |
| Финам | `FinamRaw` | `FinamApi` с клиентами всех сервисов |
| АЛОР | `AlorRaw` | `AlorHttpClient` (REST, по методу на операцию описания API), плюс `connect_ws` и `connect_cws` для каналов WebSocket |

Сколько методов брокера есть и какие из них доступны только так: [Покрытие API](../coverage.md). Это не недоделка,
а способ дать все остальное, не раздувая общий интерфейс.

## Два вызова

- **`call(имя, замыкание)`** - для чтения. Сетевой сбой и таймаут - ошибка, которую можно повторить.
  У Финама при просроченном токене запрос один раз повторяется после обновления JWT. У БКС и АЛОРа клиент сам повторяет
  запрос один раз после ответа 401 (команды заявок этого не делают).
- **`call_command(имя, замыкание)`** - для всего, что **меняет счет**. Если ответа нет (таймаут, обрыв, ошибка
  сервера), результат `Unknown`: брокер мог выполнить команду. Библиотека такой вызов никогда не повторяет.

Если сомневаетесь, меняет ли метод счет, берите `call_command`.

## Что происходит при вызове

1. **Берется квота.** Имя метода (`"GetUserTariff"`) ищется в файле лимитов (`limits.toml` крейта брокера). Нет такого
   имени - тратится общее ведро брокера (маршрут `"*"`). Бесплатно не уходит ни один вызов. Квоты нет - вызов **не
   отправляется**, возвращается `RateLimited`.
2. **Идет вызов** с токеном коннектора. Время ожидания то же, что у самого коннектора.
3. **Ошибка брокера переводится** в те же ошибки библиотеки, что и везде ([Круг команд](../orders.md)).

Правила:

- **Один вызов `raw()` - один метод API.** Не вызывайте несколько методов в одном замыкании: квота берется за один.
- **Имя метода** - это имя метода из описания API (для Т-Банка и Финама имя метода .proto, для БКС имя операции из
  файла лимитов). Хотите точный учет - добавьте маршрут для метода в файл лимитов.
- **Потоки.** Если метод возвращает поток (server-streaming), квота покрывает только открытие. Дальше поток ваш:
  читайте его сами. Поток Финама хранит JWT, с которым он открыт, и после смены токена его надо открыть заново
  (коннектор делает это для своих потоков, для ваших - вы).
- **WebSocket БКС.** `connect_ws(канал, отправитель)` открывает один из каналов (лимиты, маржинальные показатели)
  с тем же токеном. Квота называется `Connect<Канал>`. Сессия сама переподключается и подписывается заново.

## Чего `raw()` не делает

`raw()` **не знает про заявки коннектора.** Заявка, поставленная через `raw()`, не попадает ни в события, ни в
`reconcile`, и коннектор не следит за ее состоянием. Заявки ставьте обычными методами. Через `raw()` вызывайте то,
чего в общем интерфейсе нет.

## Примеры

Т-Банк: тариф и последние цены нескольких инструментов сразу (нужен крейт `brokapi-tbank-protocol`).

```rust
use brokapi::tbank::MarketDataConnector;
use brokapi::tbank::protocol::v1 as pb;

async fn tbank_raw(md: &MarketDataConnector) -> Result<(), brokapi::core::Error> {
    let raw = md.raw();

    // Квота берется по имени метода, вызов идет с токеном коннектора
    let tariff = raw
        .call("GetUserTariff", |c| async move { c.users().get_user_tariff(pb::GetUserTariffRequest {}).await })
        .await?;
    println!("лимитов в тарифе: {}", tariff.unary_limits.len());

    let prices = raw
        .call("GetLastPrices", |c| async move {
            c.market_data()
                .get_last_prices(pb::GetLastPricesRequest {
                    instrument_id: vec!["SBER_TQBR".into(), "GAZP_TQBR".into()],
                    ..Default::default()
                })
                .await
        })
        .await?;
    for p in prices.last_prices {
        println!("{} {:?}", p.ticker, p.price);
    }
    Ok(())
}
```

БКС: котировки и история операций (нужен крейт `brokapi-bcs-protocol`).

```rust
use brokapi::bcs::protocol::http::models::{OperationHistoryRequest, QuoteInstrument, QuotesRequest};
use brokapi::bcs::MarketDataConnector;

async fn bcs_raw(md: &MarketDataConnector) -> Result<(), brokapi::core::Error> {
    let raw = md.raw();

    let quotes = raw
        .call("GetQuotes", |http| async move {
            http.get_quotes(&QuotesRequest { instruments: vec![QuoteInstrument::new("SBER", "TQBR")] }).await
        })
        .await?;
    for q in quotes.records {
        println!("{:?} покупка {:?} продажа {:?} последняя {:?}", q.ticker, q.bid, q.offer, q.last);
    }

    // Операции без сделок (пополнения, дивиденды, комиссии): в общем интерфейсе их нет
    let history = raw
        .call("GetOperationHistory", |http| async move {
            http.search_operations(&OperationHistoryRequest::default(), Some(10), None).await
        })
        .await?;
    println!("операций: {}", history.records.len());
    Ok(())
}
```

Финам: последняя котировка и время биржи (нужен крейт `brokapi-finam-protocol`). Замыкание у Финама может
вызываться дважды (после обновления токена), поэтому оно типа `Fn`, а не `FnOnce`.

```rust
use brokapi::finam::MarketDataConnector;
use brokapi::finam::protocol::pb::grpc::tradeapi::v1::assets as ps;
use brokapi::finam::protocol::pb::grpc::tradeapi::v1::marketdata as pm;

async fn finam_raw(md: &MarketDataConnector) -> Result<(), brokapi::core::Error> {
    let raw = md.raw();

    let quote = raw
        .call("LastQuote", |api| async move {
            api.market_data().last_quote(pm::QuoteRequest { symbol: "SBER@MISX".into() }).await
        })
        .await?;
    println!("{} {:?}", quote.symbol, quote.quote);

    let clock = raw.call("Clock", |api| async move { api.assets().clock(ps::ClockRequest {}).await }).await?;
    println!("время биржи: {:?}", clock.timestamp);
    Ok(())
}
```

АЛОР: котировки нескольких бумаг сразу и сводка портфеля (оба метода в общем интерфейсе не нужны).

```rust
use brokapi::alor::MarketDataConnector;

async fn alor_raw(md: &MarketDataConnector, portfolio: &str) -> Result<(), brokapi::core::Error> {
    let raw = md.raw();

    let quotes = raw
        .call("GetQuotes", |http| async move { http.get_quotes(&[("MOEX", "SBER"), ("MOEX", "GAZP")]).await })
        .await?;
    for q in quotes {
        println!("{} покупка {:?} продажа {:?} последняя {:?}", q.symbol, q.bid, q.ask, q.last_price);
    }

    let portfolio = portfolio.to_string();
    let summary = raw
        .call("GetSummary", |http| async move { http.get_summary("MOEX", &portfolio).await })
        .await?;
    println!("оценка портфеля: {:?}", summary.portfolio_evaluation);
    Ok(())
}
```

Остальные методы описания API (группы заявок, оценка заявки, риски, остальные подписки WebSocket, канал `/cws`) устроены так же:
метод клиента `AlorHttpClient` в замыкании, имя для квоты первым аргументом.

Все примеры только читают. Все, что меняет счет (отчет, SL/TP-заявка Финама, группа заявок АЛОРа и подобное), идет через
`call_command`. Готовые полные примеры лежат в `examples/*_raw_access.rs` соответствующих крейтов.

## Как проверено

На имитации у всех брокеров: вызов проходит авторизацию, тратит квоту по имени метода, отказ по квоте не
доходит до брокера, счетчик общий с соседним коннектором, метод без маршрута идет по маршруту по умолчанию.
У Финама еще проверено обновление токена при чтении и отсутствие повтора у команды. У Т-Банка и БКС неясный ответ
команды дает `Unknown`. У БКС проверено открытие WebSocket-канала через `raw()`.
