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
| `could not find brokapi-gateway in registry` у `cargo install brokapi-gateway` | Не указана версия: у альфы нет стабильного выпуска | Добавить `--version`, как в разделе «Через cargo» |
| `the crate brokapi could not be found in registry index` у `cargo add brokapi` | Устарел индекс crates.io на машине или старый `cargo` | `rustup update` и повторить. Или ставить из репозитория (команда выше) |

## Программа brokapi (Gateway)

Gateway - это программа `brokapi`: служба и команды в одном бинарнике ([что это](gateway/index.md)). Ставится готовым файлом (Rust не нужен)
или через `cargo install`.

### Готовый файл

Файлы лежат на [странице выпусков](https://github.com/runinfra/brokapi/releases), в выпуске `v{{brokapi_version}}` и в каждом следующем. В выпуске пять архивов
и файл с суммами `SHA256SUMS`:

| Система | Архив |
|---|---|
| Linux x86_64 | `brokapi-{{brokapi_version}}-linux-x86_64.tar.gz` |
| Linux aarch64 (ARM) | `brokapi-{{brokapi_version}}-linux-aarch64.tar.gz` |
| macOS, процессор Intel | `brokapi-{{brokapi_version}}-macos-x86_64.tar.gz` |
| macOS, Apple silicon (M1 и новее) | `brokapi-{{brokapi_version}}-macos-arm64.tar.gz` |
| Windows x86_64 | `brokapi-{{brokapi_version}}-windows-x86_64.zip` |

Файлы для Linux статические: им не нужна определенная версия `glibc`, они запускаются на Debian, Ubuntu, RHEL, Alpine и в пустом контейнере.
Файл для Windows не просит установить Visual C++. Файлы не подписаны сертификатом разработчика, поэтому macOS и Windows могут предупредить о неизвестном
издателе: сверьте сумму (ниже) и подтвердите запуск.

Linux и macOS, на примере Linux x86_64 (для другой системы замените имя архива):

```bash
V={{brokapi_version}}
F=brokapi-$V-linux-x86_64.tar.gz
curl -fLO https://github.com/runinfra/brokapi/releases/download/v$V/$F
curl -fLO https://github.com/runinfra/brokapi/releases/download/v$V/SHA256SUMS
grep "$F" SHA256SUMS | sha256sum -c -        # на macOS: shasum -a 256 -c -
tar -xzf $F
mkdir -p ~/.local/bin && cp brokapi-$V-linux-x86_64/brokapi ~/.local/bin/
```

Последняя команда кладет программу в `~/.local/bin`: этот каталог должен быть в `PATH` (можно любой другой каталог из `PATH`). Строка с `sha256sum`
должна ответить `OK`. Если на macOS браузер скачал архив сам и система не дает запустить `brokapi`, снимите пометку скачанного файла:
`xattr -d com.apple.quarantine ~/.local/bin/brokapi`. Через `curl` пометки нет.

Windows, PowerShell:

```powershell
$V = "{{brokapi_version}}"
$F = "brokapi-$V-windows-x86_64.zip"
curl.exe -fLO "https://github.com/runinfra/brokapi/releases/download/v$V/$F"
curl.exe -fLO "https://github.com/runinfra/brokapi/releases/download/v$V/SHA256SUMS"
(Get-FileHash $F -Algorithm SHA256).Hash.ToLower()   # эта сумма должна совпасть
Select-String -Path SHA256SUMS -Pattern $F           # с суммой в этой строке
Expand-Archive $F -DestinationPath .
```

Дальше положите `brokapi-{{brokapi_version}}-windows-x86_64\brokapi.exe` в каталог из `PATH` (или вызывайте по полному пути). В архиве рядом лежат `README.md` и лицензии.

### Через cargo

Нужен Rust 1.88 или новее. Версия предварительная (альфа), поэтому номер указывается явно: без `--version` `cargo install` ищет только стабильные выпуски
и ответит, что крейта нет.

```bash
cargo install brokapi-gateway --version {{brokapi_version}} --locked
```

Сборка занимает несколько минут. Бинарник `brokapi` попадает в `~/.cargo/bin`.

Самая свежая версия, из клона репозитория:

```bash
git clone https://github.com/runinfra/brokapi
cd brokapi
cargo install --path crates/gateway --locked
```

### Проверка

```bash
brokapi --version
```

Должно напечататься `brokapi {{brokapi_version}}` (номер как у библиотеки). Не напечатало: убедитесь, что каталог с файлом есть в `PATH` (для `cargo install`
это `~/.cargo/bin`), а при ошибке сборки смотрите таблицу выше (`rustc`, `cargo`).
Дальше: [быстрый старт Gateway](gateway/quickstart.md).

Если вы пишете бота на Rust и вам нужен только клиент гейтвея, программа не нужна: [Подключить бота](gateway/bots.md#бот-на-rust).
