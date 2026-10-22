# Установка

Как получить библиотеку и убедиться, что она встала. Это первый шаг обоих быстрых стартов: [Rust](rust/quickstart.md),
[Python](python/quickstart.md).

> **Стадия: закрытая альфа.** Пакетов `brokapi` на crates.io и PyPI пока нет, они появятся в открытой бете, и тогда
> подойдут короткие команды `cargo add brokapi` и `pip install brokapi`. До этого библиотеку ставят из репозитория
> `https://github.com/runinfra/brokapi`. Он откроется вместе с открытой бетой, а до тех пор поставить из него можно только
> с выданным доступом. Ниже в каждом разделе обе команды: целевая (когда пакет опубликован) и та, что нужна сейчас.

## Что понадобится

| | Rust | Python |
|---|---|---|
| Язык | Rust 1.88 или новее (редакция 2024) | Python 3.10 или новее. Пакет один на все версии (`abi3`) |
| Инструменты | `cargo` (идет вместе с Rust, ставится через [rustup](https://rustup.rs)) | `pip`, лучше виртуальное окружение |
| Для установки из репозитория | `git` | `git` и Rust 1.88 или новее: расширение собирается на месте, сборщик `maturin` подтянется сам |

Брокеры включаются флагами, как у `tokio`: `bcs`, `tbank`, `finam`, можно несколько. В Python все брокеры уже в пакете.

## Rust

Целевая команда, когда пакет опубликован (открытая бета). Флаг выбирает брокера, здесь БКС:

```bash
cargo add brokapi --features bcs
```

Сейчас, из репозитория:

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
`brokapi::tbank`, `brokapi::finam`. Дальше: [быстрый старт на Rust](rust/quickstart.md).

## Python

Целевая команда, когда пакет опубликован (открытая бета):

```bash
python -m venv .venv && . .venv/bin/activate
pip install brokapi
```

Сейчас, из репозитория (нужен Rust: расширение собирается при установке, это несколько минут):

```bash
python -m venv .venv && . .venv/bin/activate
pip install "git+https://github.com/runinfra/brokapi#subdirectory=crates/python"
```

Из клона репозитория: `pip install ./crates/python`.

Проверка:

```bash
python -c "import brokapi; print(brokapi.__version__)"
```

Должна напечататься версия `0.1.0a1`. Не напечатала: смотрите таблицу ниже. Дальше: [быстрый старт на Python](python/quickstart.md).
Готовые пакеты под Linux, macOS и Windows будут к бете.

## Если что-то пошло не так

| Что видно | Что значит | Что делать |
|---|---|---|
| Git спрашивает имя и пароль или пишет `Repository not found` | Репозиторий пока закрыт, или у вас нет доступа | Дождаться открытой беты или получить доступ к альфе. Пакетов на crates.io и PyPI до беты нет |
| `error: package ... requires rustc 1.88` | Версия Rust старее нужной | `rustup update` |
| `pip` пишет, что не нашел `cargo` или `rustc`, при установке из репозитория | Расширение Python собирается из исходников, нужен Rust | Поставить Rust через [rustup](https://rustup.rs) и повторить |
| `ModuleNotFoundError: No module named 'brokapi'` | Пакет поставлен в другое окружение | Активировать то окружение, в которое ставили (`. .venv/bin/activate`), и проверить `python -m pip show brokapi` |
| `the crate brokapi could not be found in registry index` у `cargo add brokapi` | Пакета на crates.io еще нет, он появится в открытой бете | Ставить из репозитория (команда выше) |

## Gateway и CLI

[Gateway](gateway/index.md) и [CLI](cli/index.md) пока не выпущены: ставить нечего. Страницы продуктов
рассказывают, что будет и что можно использовать уже сейчас.
