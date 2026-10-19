# brokapi

Библиотека на Rust: один интерфейс к API брокеров БКС, Т-Банк и Финам. Рыночные данные, заявки, сверка со счетом,
лимиты запросов. Сеть и деньги в тестах не используются: все примеры идут против имитаций брокеров.

**Стадия: закрытая альфа.** Проверено только на имитациях, вживую нет, интерфейс до беты может меняться.
Что проверено и что нет: [Готовность](https://brokapi.runinfra.ru/readiness.html).

Документация: <https://brokapi.runinfra.ru>. Справочник API собирается из кода (rustdoc) и лежит рядом с книгой.
Для ИИ-агентов: [llms.txt](https://brokapi.runinfra.ru/llms.txt) и [llms-full.txt](https://brokapi.runinfra.ru/llms-full.txt), страницы книги в markdown рядом с HTML.

## Подключение

Один крейт, брокеры включаются флагами, как у `tokio`:

```toml
[dependencies]
brokapi = { version = "0.1.0-alpha.1", features = ["bcs"] }   # или "tbank", "finam"
```

В crates.io крейт пока не опубликован. Быстрый старт с пошаговым примером для БКС и Т-Банка:
[книга](https://brokapi.runinfra.ru/quickstart.html).

## Состав

| Крейт | Что это |
|---|---|
| `brokapi` | главный крейт, реэкспортирует остальные |
| `brokapi-core`, `brokapi-runtime`, `brokapi-sansio` | типы и трейты, лимиты и сверка, ядро без сети |
| `brokapi-bcs`, `brokapi-tbank`, `brokapi-finam` | коннекторы брокеров |
| `brokapi-bcs-protocol`, `brokapi-tbank-protocol`, `brokapi-finam-protocol` | клиенты протоколов брокеров |
| `brokapi-mocks`, `brokapi-conformance` | имитации и общие сценарии для тестов, не публикуются |

## Проверка перед пушем

```bash
tools/check   # fmt, clippy, тесты, doctest-ы, rustdoc, примеры, сборка книги
```

Нужен Rust 1.85 или новее, [mdBook](https://rust-lang.github.io/mdBook/) с `mdbook-mermaid` и python 3 (файлы для ИИ-агентов собирает `docs/llms.py`, только стандартная библиотека).

## Лицензия

`MIT OR Apache-2.0`, на выбор: [LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE).
