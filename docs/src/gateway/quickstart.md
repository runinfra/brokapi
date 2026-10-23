# Быстрый старт: служба brokapi

Что получится за десять минут: служба `brokapi` с одним гейтвеем на песочнице Т-Банка, и вы читаете из нее стакан.
Заявок мы не ставим, только читаем, поэтому на этом пути нельзя навредить счету.

Что нужно заранее:

- Программа `brokapi` установлена: [Установка](../install.md#программа-brokapi-gateway). Проверка: `brokapi --version` печатает номер версии.
- Токен песочницы Т-Банка и номер счета в песочнице. Где их взять: [Т-Банк, раздел «Вход»](../brokers/tbank.md#вход).

## 1. Конфиг

Создайте каталог для службы и образец конфига в нем:

```text
$ brokapi init
wrote /home/me/bots/brokapi.toml
state directory /home/me/bots/brokapi.state
Next: put the account and the token of a broker in the file, then `brokapi check`, then `brokapi up`.
```

`init` пишет `brokapi.toml` с комментариями и рядом каталог `brokapi.state`, в нем служба держит свои файлы. Если `brokapi.toml` уже
есть, команда откажется и ничего не затрет (затереть можно флагом `--force`).

Откройте `brokapi.toml`. В образце уже есть раздел гейтвея на песочнице Т-Банка, вам нужно подставить номер счета:

```toml
[[gateway]]
name = "tbank-sandbox"
broker = "tbank"
account = "2000000000"
sandbox = true
token_env = "TBANK_TOKEN"
```

`name` - имя гейтвея, по нему к нему обращаются все команды. `token_env` - имя переменной окружения, в которой лежит токен. Сам токен в файл
не пишется: конфиг попадет в копию или в репозиторий, а токен туда попадать не должен. Остальные поля: [Конфиг brokapi.toml](config.md).

## 2. Проверка

Передайте токен службе через окружение и проверьте конфиг:

```text
$ export TBANK_TOKEN=токен-песочницы
$ brokapi check
config /home/me/bots/brokapi.toml
state directory /home/me/bots/brokapi.state (writable)

GATEWAY        BROKER  CONFIG  ACCESS
tbank-sandbox  tbank   ok      ok
```

`check` разбирает файл и читает счет у брокера (портфель и активные заявки). Он ничего не ставит. В колонке `ACCESS` должно быть `ok`.
Если там `FAILED` с причиной, команда завершится с кодом 1: исправьте причину и повторите. Частые причины: [в конце страницы](#если-что-то-пошло-не-так).

## 3. Запуск

```text
$ brokapi up -d
service started in the background, pid 4121
log /home/me/bots/brokapi.state/logs/service.log
```

Служба запущена в фоне и запустила гейтвей. Посмотрите на него:

```text
$ brokapi ps
NAME           BROKER  STATE    PID   UPTIME  RESTARTS  BOTS  EVENTS/S  ORDERS  LIMIT  LAST ERROR
tbank-sandbox  tbank   running  4133  3s      0         0     0.0       0       0%
```

Колонка `STATE` должна показать `running`. Если там `starting`, гейтвей еще сверяет свой журнал с брокером, повторите команду через секунду.
Что значат остальные колонки: [Команды](commands.md#колонки-ps).

## 4. Читаем котировку

```text
$ brokapi quote tbank-sandbox SBER
SBER TQBR
bid   281.35 x 10
ask   281.40 x 12
last  281.35
```

Команда спросила гейтвей `tbank-sandbox`, а гейтвей - брокера. Если на месте цен прочерки (`-`), стакан сейчас пуст: так бывает, когда торгов нет.
Стакан целиком и портфель читаются командами `book` и `portfolio`, ключ `--json` делает вывод пригодным для скрипта.

## 5. Остановка

```text
$ brokapi down
service stopped
```

Служба остановила гейтвей и вышла. Заявок на бирже `down` не трогает, а в этом пути их и нет.

## Если что-то пошло не так

| Что видно | Что значит | Что делать |
|---|---|---|
| `no config file found` | Команда не нашла `brokapi.toml` | Запускайте команды в каталоге с файлом или укажите путь: `brokapi -c путь/к/brokapi.toml ...` |
| `the environment variable TBANK_TOKEN (token_env) is not set` | Переменной с токеном нет в этом терминале | `export TBANK_TOKEN=...` в том же терминале, где запускаете `brokapi` |
| В `ACCESS` написано `FAILED` | Брокер не принял токен или номер счета | Сверьте токен песочницы и `account`. Токен боевого счета в песочницу не подходит и наоборот |
| `no service is running for ...` (код 3) | Служба не запущена | `brokapi up -d` |
| `ps` показывает `failed` | Гейтвей несколько раз подряд упал, служба остановила попытки | В колонке `LAST ERROR` причина, подробности в `brokapi logs tbank-sandbox`. Исправьте причину и запустите `brokapi start tbank-sandbox` |

## Дальше

- Подключить бота к этому гейтвею: [Подключить бота](bots.md).
- Узнать, зачем нужен гейтвей и что он гарантирует: [Gateway](index.md).
- Все команды: [Команды](commands.md). Все поля конфига: [Конфиг](config.md).
- Держать службу постоянно, на нескольких счетах: [Запуск и обслуживание](operations.md).
