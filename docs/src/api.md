# Справочник API: Rust

Справочник по каждому публичному типу, трейту, методу, полю и ошибке собирается из кода (rustdoc), поэтому он не
расходится с кодом. Он на английском, как принято у библиотек Rust. Книга объясняет, как это устроено и что делать,
справочник отвечает на вопрос «что делает этот метод и какие ошибки вернет».

Справочник лежит рядом с книгой, в папке `api/`. Ссылки ниже ведут в него, а вверху его страниц есть путь назад в книгу.

Тот же справочник текстом, одним файлом, для чтения агентом: [api-reference.md](api-reference.md) (объявления типов, поля, варианты, методы трейтов с разделами Errors и Contract, без примеров кода).

Каждый пример в справочнике - тест, он запускается против имитации брокера (`cargo test --doc`). Сеть и деньги в них не
задействованы.

| Крейт | Что в нем | Справочник |
|---|---|---|
| `brokapi` | главный крейт: все остальное под одним именем, брокеры включаются флагами `bcs`, `tbank`, `finam`, `alor` | [brokapi](api/brokapi/index.html) |
| `brokapi-core` | типы, трейты возможностей, ошибки, `Envelope`. С него начинать | [brokapi_core](api/brokapi_core/index.html) |
| `brokapi-runtime` | лимиты запросов, буферы подписок, сверка, остановка | [brokapi_runtime](api/brokapi_runtime/index.html) |
| `brokapi-sansio` | ядро коннектора без сети, стенд для записанных кадров | [brokapi_sansio](api/brokapi_sansio/index.html) |
| `brokapi-bcs` | коннекторы БКС | [brokapi_bcs](api/brokapi_bcs/index.html) |
| `brokapi-tbank` | коннекторы Т-Банка | [brokapi_tbank](api/brokapi_tbank/index.html) |
| `brokapi-finam` | коннекторы Финама | [brokapi_finam](api/brokapi_finam/index.html) |
| `brokapi-alor` | коннекторы АЛОРа | [brokapi_alor](api/brokapi_alor/index.html) |
| `brokapi-bcs-protocol` | нижний слой БКС: REST и WebSocket | [brokapi_bcs_protocol](api/brokapi_bcs_protocol/index.html) |
| `brokapi-tbank-protocol` | нижний слой Т-Банка: gRPC из официальных `.proto` | [brokapi_tbank_protocol](api/brokapi_tbank_protocol/index.html) |
| `brokapi-finam-protocol` | нижний слой Финама: gRPC из официальных `.proto`, JWT | [brokapi_finam_protocol](api/brokapi_finam_protocol/index.html) |
| `brokapi-alor-protocol` | нижний слой АЛОРа: HTTP и WebSocket, все методы описания API | [brokapi_alor_protocol](api/brokapi_alor_protocol/index.html) |
| `brokapi-conformance` | общие сценарии, которые проходит каждый коннектор | [brokapi_conformance](api/brokapi_conformance/index.html) |

Имитации брокеров (`brokapi-mocks`) нужны только тестам и примерам. Они не часть библиотеки, в справочнике их нет.
