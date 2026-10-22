# brokapi

Библиотека на Rust и пакет для Python: один интерфейс к API брокеров (БКС, Т-Банк, Финам). Рыночные данные, заявки, сверка со счетом,
лимиты запросов. Сеть и деньги в тестах не используются: все примеры идут против имитаций брокеров.

**Стадия: открытая альфа.** Проверено только на имитациях, вживую нет, интерфейс до беты может меняться.
Что проверено и что нет: [Готовность](https://brokapi.ru/docs/readiness.html).

Документация: <https://brokapi.ru/docs/>. Справочник API собирается из кода (rustdoc) и лежит рядом с книгой.
Для ИИ-агентов: [llms.txt](https://brokapi.ru/docs/llms.txt) и [llms-full.txt](https://brokapi.ru/docs/llms-full.txt), страницы книги в markdown рядом с HTML.

## Подключение

Rust: один крейт, брокеры включаются флагами, как у `tokio`:

```bash
cargo add brokapi --features bcs   # или tbank, finam, можно несколько
```

Python 3.10 или новее (готовые пакеты для Linux x86_64, macOS и Windows):

```bash
pip install brokapi
```

Самая свежая версия - из этого репозитория, команды на странице [Установка](https://brokapi.ru/docs/install.html).
Быстрый старт с пошаговым примером: [Rust](https://brokapi.ru/docs/rust/quickstart.html),
[Python](https://brokapi.ru/docs/python/quickstart.html).

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

Нужен Rust 1.88 или новее, [mdBook](https://rust-lang.github.io/mdBook/) с `mdbook-mermaid` и python 3 (файлы для ИИ-агентов собирает `docs/llms.py`, только стандартная библиотека).

## Безопасность

Уязвимость - не в открытые issues: [SECURITY.md](SECURITY.md).

## Лицензия

`MIT OR Apache-2.0`, на выбор: [LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE).
