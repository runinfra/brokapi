# Установка

Как получить библиотеку и убедиться, что она встала. Это первый шаг обоих быстрых стартов: [Rust](rust/quickstart.md),
[Python](python/quickstart.md).

> Открытая альфа: интерфейс до беты может меняться. Перед первой заявкой на счете прочитайте [Перед боевым счетом](before-live.md).
> Код открыт: `https://github.com/runinfra/brokapi`. Ниже в каждом разделе команда для пакета и для самой свежей версии из репозитория.

## Что понадобится

| | Rust | Python |
|---|---|---|
| Язык | Rust 1.88 или новее (редакция 2024) | Python 3.10 или новее. Пакет один на все версии (`abi3`) |
| Инструменты | `cargo` (идет вместе с Rust, ставится через [rustup](https://rustup.rs)) | `pip`, лучше виртуальное окружение |
| Для установки из репозитория | `git` | `git` и Rust 1.88 или новее: расширение собирается на месте, сборщик `maturin` подтянется сам. Из PyPI Rust не нужен на Linux x86_64, macOS и Windows: там готовые пакеты |

Брокеры включаются флагами, как у `tokio`: `bcs`, `tbank`, `finam`, `alor`, можно несколько. В Python все брокеры уже в пакете.

## Rust

Флаг выбирает брокера, здесь БКС:

```bash
cargo add brokapi --features bcs
```

Самая свежая версия, из репозитория:

```bash
cargo add --git https://github.com/runinfra/brokapi brokapi --features bcs
```

Обе команды добавляют зависимость в `Cargo.toml`. Для программ из быстрого старта нужны еще `tokio` и `dotenvy`:

```bash
cargo add tokio --features full
cargo add dotenvy
```

Из клона репозитория (без доступа в сеть к GitHub во время сборки, например на закрытой машине):

```toml
[dependencies]
brokapi = { path = "../brokapi", features = ["bcs"] }
```

Проверка: `cargo build` собирает проект без ошибок (первая сборка занимает несколько минут: компилируются зависимости).
В коде все лежит под `brokapi::`: общее в `brokapi::core` и `brokapi::runtime`, брокеры в `brokapi::bcs`,
`brokapi::tbank`, `brokapi::finam`, `brokapi::alor`. Дальше: [быстрый старт на Rust](rust/quickstart.md).

## Python

Готовые пакеты есть для Linux x86_64, macOS и Windows, на остальных системах `pip` соберет расширение сам (нужен Rust):

```bash
python -m venv .venv && . .venv/bin/activate
pip install brokapi
```

Самая свежая версия, из репозитория (нужен Rust: расширение собирается при установке, это несколько минут):

```bash
python -m venv .venv && . .venv/bin/activate
pip install "git+https://github.com/runinfra/brokapi#subdirectory=crates/python"
```

Из клона репозитория: `pip install ./crates/python`.

Проверка:

```bash
python -c "import brokapi; print(brokapi.__version__)"
```

Должен напечататься номер версии. Не напечатала: смотрите таблицу ниже. Дальше: [быстрый старт на Python](python/quickstart.md).

## Если что-то пошло не так

| Что видно | Что значит | Что делать |
|---|---|---|
| Git спрашивает имя и пароль или пишет `Repository not found` | Опечатка в адресе репозитория | Сверить адрес: `https://github.com/runinfra/brokapi`. Или ставить пакет: `cargo add brokapi`, `pip install brokapi` |
| `error: package ... requires rustc 1.88` | Версия Rust старее нужной | `rustup update` |
| `pip` пишет, что не нашел `cargo` или `rustc`, при установке из репозитория | Расширение Python собирается из исходников, нужен Rust | Поставить Rust через [rustup](https://rustup.rs) и повторить |
| `ModuleNotFoundError: No module named 'brokapi'` | Пакет поставлен в другое окружение | Активировать то окружение, в которое ставили (`. .venv/bin/activate`), и проверить `python -m pip show brokapi` |
| `the crate brokapi could not be found in registry index` у `cargo add brokapi` | Устарел индекс crates.io на машине или старый `cargo` | `rustup update` и повторить. Или ставить из репозитория (команда выше) |

## Gateway и CLI

[Gateway](gateway/index.md) и [CLI](cli/index.md) пока не выпущены: ставить нечего. Страницы продуктов
рассказывают, что будет и что можно использовать уже сейчас.
