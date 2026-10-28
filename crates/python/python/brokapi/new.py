"""``python -m brokapi new <folder>``: a ready project of a trading bot, for a person or for the AI agent working for them.

The project has a bot with a signal, one buy and a protective stop (``bot.py``), its tests on the imitation of the broker
(``test_bot.py``), ``.env.example`` with the read-only mode on and a cap on one order, and ``AGENTS.md`` with the rules an
agent keeps in every session. The book: https://brokapi.ru/docs/agent-setup.html
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

TEMPLATES = Path(__file__).with_name("project")
DOCS = "https://brokapi.ru/docs/brokers"


@dataclass(frozen=True)
class Kind:
    name: str
    ticker: str
    mock: str
    price: str
    env: str
    cap: str
    token_help: str


BROKERS = {
    "tbank": Kind(
        "Т-Банк", "SBER", "mock_tbank", "250",
        "TBANK_TOKEN=\n# Песочница: игровые деньги. Настоящий счет - false и боевой токен.\nTBANK_SANDBOX=true\n",
        "10000",
        f"токен песочницы Т-Банка, как выпустить - {DOCS}/tbank.html#вход. У нового токена песочницы нет счета, откройте его один раз:\n"
        "   `python -c \"import asyncio, brokapi; print(asyncio.run(brokapi.open_tbank_sandbox_account(money='100000', sandbox=True)))\"`",
    ),
    "bcs": Kind(
        "БКС", "SBER", "mock_bcs", "250",
        "BCS_REFRESH_TOKEN=\n# Токен только на чтение. Для торговли нужен токен на запись и true.\nBCS_IS_WRITE=false\n",
        "10000",
        f"refresh-токен БКС, как выпустить - {DOCS}/bcs.html. Песочницы у БКС нет: первый запуск только с `BROKAPI_READ_ONLY=1`.",
    ),
    "finam": Kind(
        "Финам", "SBER", "mock_finam", "250",
        "FINAM_SECRET=\n# Нужен, только если у секрета несколько счетов: без него ошибка назовет их номера.\n# FINAM_ACCOUNT_ID=\n",
        "10000",
        f"секрет Trade API Финама, где взять - {DOCS}/finam.html. Песочницы у Финама нет: первый запуск только с `BROKAPI_READ_ONLY=1`.",
    ),
    "alor": Kind(
        "АЛОР", "SBER", "mock_alor", "250",
        "ALOR_REFRESH_TOKEN=\nALOR_PORTFOLIO=\n# Тестовый контур: тестовый счет выдает брокер. Настоящий счет - false.\nALOR_TEST=true\n",
        "10000",
        f"refresh-токен АЛОР, как выпустить - {DOCS}/alor.html. Для первого запуска есть тестовый контур.",
    ),
    "bybit": Kind(
        "Bybit", "BTCUSDT", "mock_bybit", "100000",
        "# Демо-счет: виртуальные деньги, своя пара ключей.\nBYBIT_ENV=demo\nBYBIT_DEMO_API_KEY=\nBYBIT_DEMO_API_SECRET=\n",
        "200",
        f"ключ и секрет демо-счета Bybit, как выпустить - {DOCS}/bybit.html.",
    ),
}

ENV_TAIL = (
    "\n# Бот только смотрит и пишет, что купил бы: ничего не отправляется. Торговлю включает владелец счета, убрав эту строку.\n"
    "BROKAPI_READ_ONLY=1\n"
    "# Самая большая стоимость одной заявки, в деньгах цены. Код бота ее не поднимет.\n"
    "BROKAPI_MAX_ORDER_VALUE={cap}\n"
)

FILES = {  # template -> file of the project
    "bot.py.tmpl": "bot.py",
    "test_bot.py.tmpl": "test_bot.py",
    "agent-rules.md.tmpl": "AGENTS.md",  # the template is not named AGENTS.md: such files of ours never leave the repository
    "README.md.tmpl": "README.md",
    "gitignore.tmpl": ".gitignore",
    "pyproject.toml.tmpl": "pyproject.toml",
}


def create(folder: Path, broker: str, ticker: str | None = None) -> list[Path]:
    """Writes the project into `folder` (new or empty) and returns the files written."""
    if broker not in BROKERS:
        raise ValueError(f"unknown broker {broker!r}: use {', '.join(sorted(BROKERS))}")
    kind = BROKERS[broker]
    if folder.exists() and any(folder.iterdir()):
        raise FileExistsError(f"{folder} is not empty: the project goes into a new or an empty folder, nothing was written")
    folder.mkdir(parents=True, exist_ok=True)
    marks = {
        "__BROKER__": broker,
        "__BROKER_NAME__": kind.name,
        "__TICKER__": ticker or kind.ticker,
        "__MOCK__": kind.mock,
        "__PRICE__": kind.price,
        "__TOKEN_HELP__": kind.token_help,
    }
    written = []
    for template, name in FILES.items():
        text = (TEMPLATES / template).read_text(encoding="utf-8")
        for mark, value in marks.items():
            text = text.replace(mark, value)
        path = folder / name
        path.write_text(text, encoding="utf-8")
        written.append(path)
    env = folder / ".env.example"
    env.write_text(kind.env + ENV_TAIL.format(cap=kind.cap), encoding="utf-8")
    written.append(env)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m brokapi")
    commands = parser.add_subparsers(dest="command", required=True)
    new = commands.add_parser("new", help="a ready project of a trading bot: the bot, its tests on the imitation, .env.example, AGENTS.md")
    new.add_argument("folder", type=Path, help="a new or an empty folder")
    new.add_argument("--broker", required=True, choices=sorted(BROKERS))
    new.add_argument("--ticker", help="what the bot trades (default: SBER, or BTCUSDT on Bybit)")
    args = parser.parse_args(argv)
    try:
        files = create(args.folder, args.broker, args.ticker)
    except (FileExistsError, ValueError) as error:
        parser.error(str(error))
    print(f"made {args.folder}: {', '.join(p.name for p in files)}")
    print(f"next: cd {args.folder} && pip install pytest pytest-asyncio && pytest && python bot.py --mock")
    return 0
