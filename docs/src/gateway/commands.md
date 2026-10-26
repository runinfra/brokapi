# Команды brokapi

Справочник по программе `brokapi`: все команды, общие флаги, коды выхода. Как начать работать: [Быстрый старт](quickstart.md). Зачем она нужна и как
устроена: [Gateway](index.md).

Таблица ниже написана по задачам, а в конце страницы стоит точный текст `brokapi <команда> --help` каждой команды. Он собран из самой
программы: сборка книги падает, если команда или флаг изменились, а страница нет.

## Общие правила

- **Конфиг.** Каждая команда берет конфиг из `-c <путь>`. Без него: переменная `BROKAPI_CONFIG`, потом `./brokapi.toml`, потом
  `brokapi.toml` в каталоге настроек пользователя (на Linux `~/.config/brokapi`). Конфиг задает каталог состояния и сокет службы, поэтому
  на одной машине можно держать сколько угодно служб: `brokapi -c prod.toml up` и `brokapi -c test.toml up` не мешают друг другу.
- **`--json`.** У каждой команды, которая что-то печатает, есть ключ `--json`: тот же ответ в JSON, для скрипта или ИИ-агента. Ключ ставится
  и до, и после команды.
- **Заявок команды не ставят.** Из программы можно только читать счет и снимать заявки (`stop --cancel-orders`, `cancel-all`). Ставят заявки
  боты: [Подключить бота](bots.md).

## Какая команда для чего

| Команда | Что делает | Нужна ли служба |
|---|---|---|
| `brokapi init [путь]` | Пишет образец `brokapi.toml` и каталог состояния. Существующий файл не затирает без `--force` | нет |
| `brokapi check [гейтвей ...]` | Разбирает конфиг и читает счет у брокера (портфель и активные заявки). Ничего не ставит. `--offline` - только файл и токены, без брокера. Предупреждает, если файл с токеном читается другими пользователями | нет |
| `brokapi up [-d]` | Запускает службу этого конфига со всеми ее гейтвеями. Без `-d` работает на переднем плане до Ctrl+C. С `-d` - в фоне, команда возвращается, когда служба ответила | это она |
| `brokapi down [--leave-gateways]` | Останавливает службу и ее гейтвеи. С `--leave-gateways` гейтвеи остаются жить, следующий `up` подхватит их | да |
| `brokapi services` | Все запущенные на машине службы, каждая со своим конфигом: pid, сколько гейтвеев работает, аптайм | нет |
| `brokapi ps` | Гейтвеи службы: состояние, pid, аптайм, перезапуски, боты, расход лимита, последняя ошибка. `--check` - код 1, если гейтвей не работает | да |
| `brokapi top [-i сек] [-n кадров]` | То же вживую: таблица обновляется на месте, плюс CPU и память. В терминале перерисовывается, в трубе печатает кадры подряд, с `--json` - по объекту в строке | да |
| `brokapi status <гейтвей>` | Подробно об одном гейтвее: процесс, журнал, подписки, боты, остатки ведер лимита | да |
| `brokapi start <гейтвей ...> [--wait]` | Запускает остановленный или упавший (`failed`) гейтвей. `--wait` ждет, пока он начнет принимать команды | да |
| `brokapi stop <гейтвей ...> [--cancel-orders]` | Останавливает гейтвей и держит его остановленным до `start`. Заявки на бирже не трогает. С `--cancel-orders` сначала снимает активные и стоп-заявки счета | да |
| `brokapi restart <гейтвей ...> [--wait]` | Останавливает и запускает снова | да |
| `brokapi add <гейтвей>` | Перечитывает конфиг и запускает этот гейтвей из него, не останавливая службу. Уже работающий и неизмененный - ошибка | да |
| `brokapi remove <гейтвей>` | Останавливает гейтвей и забывает его до `add` или `reload`. Файл конфига не правит | да |
| `brokapi reload` | Перечитывает конфиг целиком: новые гейтвеи запускает, исчезнувшие из файла останавливает, измененные перезапускает, остальные не трогает | да |
| `brokapi logs [гейтвей] [-f] [-n строк]` | Журнал гейтвея, а без имени - службы. `-f` показывает, что дописывается. По умолчанию 50 последних строк | нет |
| `brokapi portfolio <гейтвей>` | Деньги и позиции счета | нет, нужен запущенный гейтвей |
| `brokapi orders <гейтвей>` | Активные заявки счета | нет, нужен запущенный гейтвей |
| `brokapi stops <гейтвей>` | Активные стоп-заявки счета | нет, нужен запущенный гейтвей |
| `brokapi book <гейтвей> <тикер> [--board] [--depth]` | Стакан, по умолчанию 5 уровней с каждой стороны | нет, нужен запущенный гейтвей |
| `brokapi quote <гейтвей> <тикер> [--board]` | Лучшая покупка, лучшая продажа и последняя сделка | нет, нужен запущенный гейтвей |
| `brokapi direct <брокер> portfolio` и `orders`, `stops`, `book`, `quote` | То же напрямую у брокера, без службы и гейтвея: проверить токен или задать разовый вопрос. `--from <гейтвей>` берет настройки и токен из раздела этого гейтвея в конфиге (брокер должен совпасть), без него - из переменных окружения библиотеки и файла `.env` | нет |
| `brokapi cancel-all <гейтвей>` | Аварийная кнопка: снять все активные и стоп-заявки счета. На отказ по лимиту ждет и повторяет, пока не снимет | нет, нужен запущенный гейтвей |
| `brokapi gateway run <гейтвей>` | Один гейтвей на переднем плане, без службы. Так его запускает и сама служба | нет |

Команды чтения (`portfolio`, `orders`, `stops`, `book`, `quote`, `cancel-all`) ходят прямо к гейтвею по каталогу из конфига, поэтому
работают и тогда, когда гейтвей запущен вручную через `gateway run`, а не службой. Для них в программе заводится бот с именем,
начинающимся на `brokapi-`: в `ps` он не считается ботом, в `status` помечен как `(tool)`.

## Колонки ps

| Колонка | Что значит |
|---|---|
| `NAME`, `BROKER` | Имя гейтвея из конфига и брокер |
| `STATE` | `starting` (процесс идет, гейтвей сверяет журнал с брокером), `running` (принимает команды), `restarting` (пауза перед новым запуском, в скобках сколько осталось), `stopping`, `stopped` (ждет `start`), `failed` (служба перестала его перезапускать, ждет `start`) |
| `PID`, `UPTIME` | Процесс гейтвея и сколько он работает |
| `RESTARTS` | Сколько раз служба запускала гейтвей заново после того, как он остановился сам. Считает с запуска службы: `start` и `restart` обнуляют только счет к пределу `max_restarts` ([Конфигурация](config.md#servicerestart)), а не эту колонку. Новая служба начинает с нуля |
| `BOTS` | Сколько ботов сейчас читает его поток событий. Инструменты самой программы не считаются |
| `EVENTS/S` | События в секунду за последние десять секунд |
| `ORDERS` | Заявки в журнале гейтвея, про которые не известно, что они закончились |
| `LIMIT` | Насколько заполнено самое занятое ведро лимита брокера, в процентах |
| `CPU`, `MEM` | Только в `top`: нагрузка и память процесса |
| `LAST ERROR` | Сколько времени назад гейтвей последний раз остановился сам, чем закончился этот запуск и последняя строка `ERROR` или `WARN` из его журнала (`3m05s ago: killed by signal 9`, время назад - с версии 0.1.0-alpha.6). Остается и после того, как гейтвей снова работает: так видно, почему вырос `RESTARTS`. Новая служба начинает с пустой колонки |

Где значение неизвестно (гейтвей не отвечает), стоит `-`. Память процесса служба читает раз в секунду (`check_interval`), поэтому в первую секунду после запуска вместо нее тоже `-`, а не `0 B`.

## Коды выхода

Скрипт и ИИ-агент отличают причины по коду выхода, текст ошибки при этом читать не нужно.

| Код | Что значит | Что делать |
|---|---|---|
| `0` | Успех | |
| `1` | Команда не удалась: брокер или гейтвей отказал, `check` нашел проблему, файл не записался | Прочитать сообщение, оно называет причину |
| `2` | Неверный вызов или конфиг, который нельзя использовать | Исправить команду или `brokapi.toml`, `brokapi check --offline` покажет место |
| `3` | Для этого конфига нет службы | `brokapi up -d` |
| `4` | Нет такого гейтвея или он не запущен | Сверить имя с `[[gateway]]` в конфиге, посмотреть `brokapi ps` |

Эти же пять кодов и порядок поиска конфига печатает `brokapi --help`.

## Примеры вывода

Службу и гейтвей из [быстрого старта](quickstart.md) считаем запущенными. Числа и пути у вас будут свои.

```text
$ brokapi services
PID   GATEWAYS       UPTIME  CONFIG                          STATE DIRECTORY
4121  1/1 running    1m12s   /home/me/bots/brokapi.toml      /home/me/bots/brokapi.state
```

```text
$ brokapi status tbank-sandbox
gateway       tbank-sandbox (tbank, sandbox, account 2000000000)
state         running
process       pid 4133, up 1m12s, 0.3% cpu, 9.1 MB
restarts      0
listens at    unix:/home/me/bots/brokapi.state/gateways/tbank-sandbox/gateway.sock
directory     /home/me/bots/brokapi.state/gateways/tbank-sandbox
gateway says  ready
version       {{brokapi_version}} (commit 1a2b3c4d5)
events        0 so far, 0.0 per second, last number 0
journal       0 order(s) not known to have ended

bots
BOT                     CONNECTED  CALLS  QUEUED  CUT OFF
brokapi-service (tool)  no         3      0       0

subscriptions
SUB  WHAT         HELD BY
1    order_state  (gateway)

rate limit of the broker
LIMIT BUCKET  LEFT  OF
accounts      100   100
default       100   100
global        50    50
...
```

Строка `process` в `status` показывает память так же, как `top`: `9.1 MB`, а до первого чтения `-`.

В `status` три таблицы. `bots` - кто обращался к гейтвею и читает ли сейчас его поток событий (`brokapi-service (tool)` - сама программа, не ваш бот). `subscriptions` -
подписки у брокера и кто их держит: `(gateway)` - подписка самого гейтвея на состояния заявок, ее не снять. `rate limit of the broker` - сколько осталось в каждом ведре лимита
(таблица длиннее, чем показано: у Т-Банка ведер около двадцати, здесь первые строки).

```text
$ brokapi portfolio tbank-sandbox
CURRENCY  AVAILABLE  BLOCKED
RUB       100000     0

INSTRUMENT  QTY  AVG PRICE  PRICE
SBER TQBR   10   250.10     281.35
```

```text
$ brokapi book tbank-sandbox SBER --depth 2
SBER TQBR
BID QTY  BID     ASK     ASK QTY
10       281.35  281.40  12
5        281.30  281.45  8
```

```text
$ brokapi orders tbank-sandbox
ORDER  INSTRUMENT  SIDE  TYPE  QTY  FILLED  PRICE  STATE
```

С `--json` те же данные идут в JSON, числа и цены в нем строками, как везде в библиотеке:

```text
$ brokapi quote tbank-sandbox SBER --json
{
  "ask": {
    "price": "281.40",
    "qty": "12"
  },
  "bid": {
    "price": "281.35",
    "qty": "10"
  },
  "instrument": "SBER TQBR",
  "last": "281.35"
}
```

## Справка по каждой команде

<!-- help:begin -->

### brokapi

```text
$ brokapi --help
The brokapi gateway: one process per broker account that many bots share. `brokapi up` runs the
service that keeps the gateways alive, the other commands look after them.

Usage: brokapi [OPTIONS] <COMMAND>

Commands:
  init        Write a sample config and make the state directory
  check       Check the config and the access of every gateway. Only reads: no order is placed
  up          Start the service of this config: it runs the gateways and keeps them alive
  down        Stop the service of this config, and its gateways
  services    List the services running on this machine, whatever their config
  ps          List the gateways of the service: state, pid, uptime, restarts, bots, events, orders,
              limit use, last error
  top         Like `ps`, with CPU and memory, refreshed in place until Ctrl+C
  start       Start gateways that are stopped or failed
  stop        Stop gateways and keep them stopped. Orders at the exchange are not touched, unless
              asked
  restart     Stop gateways and start them again
  add         Read the config again and start this gateway of it, without stopping the service
  remove      Stop a gateway and forget it, without stopping the service. `add` or `reload` bring it
              back from the config
  reload      Read the config again: start the new gateways, stop the removed ones, restart the
              changed ones
  status      Everything about one gateway: connection, journal, subscriptions, bots, limits
  logs        Show the log of a gateway, or of the service without a name
  gateway     Run one gateway in this terminal, without the service. The service starts its gateways
              this way
  portfolio   Money and positions of the account behind a gateway
  orders      Active orders of the account behind a gateway
  stops       Active stop orders of the account behind a gateway
  book        The order book of an instrument, through a gateway
  quote       Best bid, best ask and the last trade of an instrument, through a gateway
  direct      Read straight from a broker, without a service or a gateway: to try a token, or for a
              one-off question
  cancel-all  Emergency: cancel every active order and stop order of the account behind a gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
  -V, --version        Print version

Exit codes:
  0  success
  1  the command failed: a broker or a gateway refused, a check found a problem, a file could not be
  written
  2  wrong usage or a config that cannot be used
  3  no service is running for this config (start it with `brokapi up`)
  4  no such gateway, or the gateway is not running

The config is looked for in: -c FILE, the variable BROKAPI_CONFIG, ./brokapi.toml, then brokapi.toml
in the config
directory of the user (~/.config/brokapi on Linux). Two config files are two independent services,
each with its own
state directory and its own socket, so any number of services can run on one machine.
```

### brokapi init

```text
$ brokapi init --help
Write a sample config and make the state directory

Usage: brokapi init [OPTIONS] [PATH]

Arguments:
  [PATH]  Where to write the config (default ./brokapi.toml)

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --force          Replace the file if it is there
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi check

```text
$ brokapi check --help
Check the config and the access of every gateway. Only reads: no order is placed

Usage: brokapi check [OPTIONS] [NAMES]...

Arguments:
  [NAMES]...  Only these gateways

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --offline        Do not connect to the brokers, check the file and the tokens only
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi up

```text
$ brokapi up --help
Start the service of this config: it runs the gateways and keeps them alive

Usage: brokapi up [OPTIONS]

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
  -d, --detach         Run in the background (a process without a terminal) and return when the
                       service answers
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi down

```text
$ brokapi down --help
Stop the service of this config, and its gateways

Usage: brokapi down [OPTIONS]

Options:
  -c, --config <FILE>   The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                        directory of the user) [env: BROKAPI_CONFIG=]
      --leave-gateways  Leave the gateway processes running. A service started later takes them over
      --json            Print JSON instead of text (every command that prints something)
  -h, --help            Print help
```

### brokapi services

```text
$ brokapi services --help
List the services running on this machine, whatever their config

Usage: brokapi services [OPTIONS]

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi ps

```text
$ brokapi ps --help
List the gateways of the service: state, pid, uptime, restarts, bots, events, orders, limit use,
last error

Usage: brokapi ps [OPTIONS]

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --check          Exit with 1 when a gateway is not running (starting, restarting, stopped or
                       failed): for cron and monitoring
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi top

```text
$ brokapi top --help
Like `ps`, with CPU and memory, refreshed in place until Ctrl+C

Usage: brokapi top [OPTIONS]

Options:
  -c, --config <FILE>        The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the
                             config directory of the user) [env: BROKAPI_CONFIG=]
  -i, --interval <INTERVAL>  Seconds between refreshes [default: 1]
      --json                 Print JSON instead of text (every command that prints something)
  -n, --count <COUNT>        Stop after this many refreshes (default: until Ctrl+C)
  -h, --help                 Print help
```

### brokapi start

```text
$ brokapi start --help
Start gateways that are stopped or failed

Usage: brokapi start [OPTIONS] <NAMES>...

Arguments:
  <NAMES>...  The gateways

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --wait           Wait until they take commands
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi stop

```text
$ brokapi stop --help
Stop gateways and keep them stopped. Orders at the exchange are not touched, unless asked

Usage: brokapi stop [OPTIONS] <NAMES>...

Arguments:
  <NAMES>...  The gateways

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --cancel-orders  Cancel the active orders and stop orders of the account first
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi restart

```text
$ brokapi restart --help
Stop gateways and start them again

Usage: brokapi restart [OPTIONS] <NAMES>...

Arguments:
  <NAMES>...  The gateways

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --wait           Wait until they take commands
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi add

```text
$ brokapi add --help
Read the config again and start this gateway of it, without stopping the service

Usage: brokapi add [OPTIONS] <NAME>

Arguments:
  <NAME>  The gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi remove

```text
$ brokapi remove --help
Stop a gateway and forget it, without stopping the service. `add` or `reload` bring it back from the
config

Usage: brokapi remove [OPTIONS] <NAME>

Arguments:
  <NAME>  The gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi reload

```text
$ brokapi reload --help
Read the config again: start the new gateways, stop the removed ones, restart the changed ones

Usage: brokapi reload [OPTIONS]

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi status

```text
$ brokapi status --help
Everything about one gateway: connection, journal, subscriptions, bots, limits

Usage: brokapi status [OPTIONS] <NAME>

Arguments:
  <NAME>  The gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi logs

```text
$ brokapi logs --help
Show the log of a gateway, or of the service without a name

Usage: brokapi logs [OPTIONS] [NAME]

Arguments:
  [NAME]  The gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
  -f, --follow         Keep showing what is added
      --json           Print JSON instead of text (every command that prints something)
  -n, --lines <LINES>  How many lines from the end [default: 50]
  -h, --help           Print help
```

### brokapi gateway

```text
$ brokapi gateway --help
Run one gateway in this terminal, without the service. The service starts its gateways this way

Usage: brokapi gateway [OPTIONS] <COMMAND>

Commands:
  run  Run the gateway until Ctrl+C or SIGTERM

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi gateway run

```text
$ brokapi gateway run --help
Run the gateway until Ctrl+C or SIGTERM

Usage: brokapi gateway run [OPTIONS] <NAME>

Arguments:
  <NAME>  The gateway

Options:
  -c, --config <FILE>    The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                         directory of the user) [env: BROKAPI_CONFIG=]
      --log-file <FILE>  Write the log to this file instead of the terminal
      --json             Print JSON instead of text (every command that prints something)
  -h, --help             Print help
```

### brokapi portfolio

```text
$ brokapi portfolio --help
Money and positions of the account behind a gateway

Usage: brokapi portfolio [OPTIONS] <GATEWAY>

Arguments:
  <GATEWAY>  The gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi orders

```text
$ brokapi orders --help
Active orders of the account behind a gateway

Usage: brokapi orders [OPTIONS] <GATEWAY>

Arguments:
  <GATEWAY>  The gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi stops

```text
$ brokapi stops --help
Active stop orders of the account behind a gateway

Usage: brokapi stops [OPTIONS] <GATEWAY>

Arguments:
  <GATEWAY>  The gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi book

```text
$ brokapi book --help
The order book of an instrument, through a gateway

Usage: brokapi book [OPTIONS] <GATEWAY> <TICKER>

Arguments:
  <GATEWAY>  The gateway
  <TICKER>   The ticker, like SBER

Options:
      --board <BOARD>  The board or market, like TQBR (default: the broker's choice)
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --depth <DEPTH>  How many levels on each side [default: 5]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi quote

```text
$ brokapi quote --help
Best bid, best ask and the last trade of an instrument, through a gateway

Usage: brokapi quote [OPTIONS] <GATEWAY> <TICKER>

Arguments:
  <GATEWAY>  The gateway
  <TICKER>   The ticker, like SBER

Options:
      --board <BOARD>  The board or market, like TQBR
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

### brokapi direct

```text
$ brokapi direct --help
Read straight from a broker, without a service or a gateway: to try a token, or for a one-off
question

Usage: brokapi direct [OPTIONS] <BROKER> <COMMAND>

Commands:
  portfolio  Money and positions
  orders     Active orders
  stops      Active stop orders
  book       The order book
  quote      Best bid, best ask, last trade

Arguments:
  <BROKER>  The broker: bcs, tbank, finam, alor or bybit

Options:
  -c, --config <FILE>   The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                        directory of the user) [env: BROKAPI_CONFIG=]
      --from <GATEWAY>  Take the settings and the token of this gateway of the config (its broker
                        must be the same). Without it the settings come from the environment
                        variables of the library (BCS_REFRESH_TOKEN, TBANK_TOKEN, ...) and a .env
                        file in the directory
      --json            Print JSON instead of text (every command that prints something)
  -h, --help            Print help
```

### brokapi direct portfolio

```text
$ brokapi direct portfolio --help
Money and positions

Usage: brokapi direct <BROKER> portfolio [OPTIONS]

Options:
  -c, --config <FILE>   The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                        directory of the user) [env: BROKAPI_CONFIG=]
      --from <GATEWAY>  Take the settings and the token of this gateway of the config (its broker
                        must be the same). Without it the settings come from the environment
                        variables of the library (BCS_REFRESH_TOKEN, TBANK_TOKEN, ...) and a .env
                        file in the directory
      --json            Print JSON instead of text (every command that prints something)
  -h, --help            Print help
```

### brokapi direct orders

```text
$ brokapi direct orders --help
Active orders

Usage: brokapi direct <BROKER> orders [OPTIONS]

Options:
  -c, --config <FILE>   The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                        directory of the user) [env: BROKAPI_CONFIG=]
      --from <GATEWAY>  Take the settings and the token of this gateway of the config (its broker
                        must be the same). Without it the settings come from the environment
                        variables of the library (BCS_REFRESH_TOKEN, TBANK_TOKEN, ...) and a .env
                        file in the directory
      --json            Print JSON instead of text (every command that prints something)
  -h, --help            Print help
```

### brokapi direct stops

```text
$ brokapi direct stops --help
Active stop orders

Usage: brokapi direct <BROKER> stops [OPTIONS]

Options:
  -c, --config <FILE>   The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                        directory of the user) [env: BROKAPI_CONFIG=]
      --from <GATEWAY>  Take the settings and the token of this gateway of the config (its broker
                        must be the same). Without it the settings come from the environment
                        variables of the library (BCS_REFRESH_TOKEN, TBANK_TOKEN, ...) and a .env
                        file in the directory
      --json            Print JSON instead of text (every command that prints something)
  -h, --help            Print help
```

### brokapi direct book

```text
$ brokapi direct book --help
The order book

Usage: brokapi direct <BROKER> book [OPTIONS] <TICKER>

Arguments:
  <TICKER>  The ticker

Options:
      --board <BOARD>   The board
  -c, --config <FILE>   The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                        directory of the user) [env: BROKAPI_CONFIG=]
      --from <GATEWAY>  Take the settings and the token of this gateway of the config (its broker
                        must be the same). Without it the settings come from the environment
                        variables of the library (BCS_REFRESH_TOKEN, TBANK_TOKEN, ...) and a .env
                        file in the directory
      --depth <DEPTH>   How many levels on each side [default: 5]
      --json            Print JSON instead of text (every command that prints something)
  -h, --help            Print help
```

### brokapi direct quote

```text
$ brokapi direct quote --help
Best bid, best ask, last trade

Usage: brokapi direct <BROKER> quote [OPTIONS] <TICKER>

Arguments:
  <TICKER>  The ticker

Options:
      --board <BOARD>   The board
  -c, --config <FILE>   The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                        directory of the user) [env: BROKAPI_CONFIG=]
      --from <GATEWAY>  Take the settings and the token of this gateway of the config (its broker
                        must be the same). Without it the settings come from the environment
                        variables of the library (BCS_REFRESH_TOKEN, TBANK_TOKEN, ...) and a .env
                        file in the directory
      --json            Print JSON instead of text (every command that prints something)
  -h, --help            Print help
```

### brokapi cancel-all

```text
$ brokapi cancel-all --help
Emergency: cancel every active order and stop order of the account behind a gateway

Usage: brokapi cancel-all [OPTIONS] <GATEWAY>

Arguments:
  <GATEWAY>  The gateway

Options:
  -c, --config <FILE>  The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config
                       directory of the user) [env: BROKAPI_CONFIG=]
      --json           Print JSON instead of text (every command that prints something)
  -h, --help           Print help
```

<!-- help:end -->
