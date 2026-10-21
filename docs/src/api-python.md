# Справочник API: Python

Справочник по каждому классу, методу, полю и ошибке пакета `brokapi` для Python собирается из docstring в коде (pdoc), поэтому он не
расходится с кодом. Он на английском. Книга объясняет, как это устроено и что делать, справочник отвечает на вопрос «что делает
этот метод и какие ошибки поднимет».

Справочник лежит рядом с книгой: **[Справочник API Python](api/python/brokapi.html)**. Он собирается скриптом `docs/build.sh` вместе
с книгой, поэтому ссылка работает на сайте и после локальной сборки, но не в `mdbook serve`. Страница не делает запросов к
внешним адресам: шрифты и стили свои.

Тот же справочник текстом, одним файлом, для чтения агентом: [python/api-reference.md](python/api-reference.md) (сигнатуры и
описания классов и методов без оформления).

| Модуль | Что в нем | Справочник |
|---|---|---|
| `brokapi` | `connect`, `Client`, `EventStream`, `merge`, `new_cid`: все, что нужно для работы, и общий обзор | [brokapi](api/python/brokapi.html) |
| `brokapi.models` | данные: инструменты, стакан, заявки, события, возможности | [brokapi.models](api/python/brokapi/models.html) |
| `brokapi.errors` | исключения с теми же именами, что у `Error` в Rust, и `ErrorClass` | [brokapi.errors](api/python/brokapi/errors.html) |
| `brokapi.numbers` | точное округление до шага цены (`round_to_step`) | [brokapi.numbers](api/python/brokapi/numbers.html) |
| `brokapi.testing` | имитации брокеров для тестов | [brokapi.testing](api/python/brokapi/testing.html) |

Примеры в docstring, помеченные `>>>`, запускаются тестом (`pytest --doctest-modules`), так что справочник не может тихо устареть.

Справочник Rust: [Справочник API: Rust](api.md). Имена ошибок, событий и возможностей у Python те же, что у ядра, поэтому
[Ошибки и что с ними делать](errors.md) и [События и возможности](events.md) верны для обоих.
