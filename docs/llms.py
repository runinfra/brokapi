#!/usr/bin/env python3
"""Builds the files for AI agents next to the HTML book (called by docs/build.sh, python 3 standard library only).

  llms.txt        the index of the documentation for agents (https://llmstxt.org): a title, one sentence, sections of
                  links with one line about each link
  llms-full.txt   the whole book as one markdown file, in the order of SUMMARY.md
  <page>.md       the markdown of every page next to its <page>.html, with {{#include}} lines replaced by the code
  <old>.md        for every page moved by [output.html.redirect] in book.toml: a short note with the new address, so the
                  old markdown address of a page does not end in 404 either

usage: llms.py <docs dir> <book dir>
       <docs dir> holds book.toml and src/, <book dir> is the output of `mdbook build` (it gets the files).

Every page of SUMMARY.md must have a line in DESCRIPTIONS below: a new page without one fails the build, so the index
for agents cannot fall behind the book.
"""
import os
import re
import sys
import textwrap

try:
    import tomllib  # python 3.11+
except ImportError:  # pragma: no cover
    tomllib = None

BASE_URL = os.environ.get("BROKAPI_DOCS_URL", "https://brokapi.ru/docs").rstrip("/")

TITLE = "brokapi"
SUMMARY_LINE = (
    "brokapi - единый интерфейс к торговым API: один и тот же код бота работает с любым брокером. Начинаем с API брокеров "
    "российского рынка (БКС, Т-Банк, Финам), дальше другие брокеры и биржевые протоколы. Писать можно на Rust (ядро) или на "
    "Python (пакет brokapi, тонкая обертка над ядром): рыночные данные, заявки, сверка со счетом, лимиты запросов. Gateway и "
    "CLI еще не вышли, их разделы - заглушки. Стадия: закрытая альфа, проверено на имитациях, вживую пока нет. Документация: %s/" % BASE_URL
)
INTRO = """Как пользоваться этим набором. Книга написана по задачам и состоит из общей части (как устроено, брокеры, \
ошибки, события, контракт для агентов) и разделов продуктов: Rust, Python, Gateway, CLI. Писать код можно на Rust или на \
Python, оба работают сейчас (альфа). Если пользователь пишет на Python, берите отдельный индекс для Python: \
%s/python/llms.txt, в нем только то, что нужно для Python, и справочник пакета одним файлом. Страницы с пометкой "еще нет" \
(Gateway, CLI) - заглушки: кода и пакетов там нет, не пытайтесь обращаться к службе Gateway. Если нужно написать код: \
прочитайте "Контракт для агентов" (все обязательные правила на одной странице, они одни для любого языка), затем инструкцию \
из раздела нужного языка, ближайшую к задаче (торговый бот, несколько брокеров и счетов, история или исследовательский \
движок), и копируйте код оттуда: каждый пример - рабочий тест против имитаций. Общие страницы (круги механики, заявки, \
сверка) показывают код вкладками Rust и Python. Точные объявления типов и сигнатуры методов - в api-reference.md (Rust, из \
rustdoc) и python/api-reference.md (Python, из docstring), а также в самих справочниках. Весь текст книги одним файлом: \
llms-full.txt. У каждой страницы есть markdown-исходник рядом с HTML (замените .html на .md). Все адреса ниже - на \
https://brokapi.ru/docs/, справочник API Rust - на https://brokapi.ru/docs/api/, справочник Python - на \
https://brokapi.ru/docs/api/python/.""" % BASE_URL

# part titles of SUMMARY.md that are not ready, with the note added to the section title in llms.txt
SECTION_NOTES = {
    "Gateway": "еще нет, заглушка",
    "CLI": "еще нет, заглушка",
}

# path of the page in src/ -> one line for llms.txt
DESCRIPTIONS = {
    "README.md": "Что такое brokapi, выбор пути (Rust, Python, Gateway, CLI) и статус каждого, три главных правила, честный статус",
    "install.md": "Установка: cargo add brokapi и pip install brokapi (пакеты появятся в открытой бете), а сейчас установка из репозитория; проверка и частые ошибки",
    "architecture.md": "Карта: коннекторы MD и UD, счетчик лимитов, канал событий и три круга механики со схемами",
    "streams.md": "Круг событий: подписки, режимы буфера (Conflate, DropNewest, Lossless), Overflow, Resync, обрывы",
    "orders.md": "Круг команд: cid, порядок проверок place, исходы команды, почему нет автоповтора, машина состояний заявки",
    "reconcile.md": "Круг жизни: шлюз торговых команд и сверка reconcile, три списка отчета, что хранит бот",
    "limits.md": "Лимиты запросов: BrokerLimiter, ведра, RateLimited, почему отмена в отдельном ведре",
    "numbers.md": "Price, Qty, Money на Decimal, округление до шага цены, PriceGrid, перевод форматов брокеров",
    "brokers/README.md": "Сравнение трех брокеров: идентификаторы инструментов, единицы, особенности",
    "brokers/bcs.md": "БКС: токены, идентификаторы, единицы количества, ограничения",
    "brokers/tbank.md": "Т-Банк: песочница, счета, лоты, uid инструмента, ограничения",
    "brokers/finam.md": "Финам: секрет и JWT, символ SBER@MISX, количество в штуках, нет replace",
    "rust/index.md": "Раздел Rust (работает сейчас, альфа): как подключить крейт и таблица инструкций по задачам",
    "rust/quickstart.md": "Rust: от нуля до первой заявки на БКС, Т-Банке и Финаме: подключение, сверка, подписки, заявка, отмена",
    "rust/howto-trading-bot.md": "Rust: как написать торгового бота на событиях: стакан, сигнал, заявка, статусы, выход из Unknown, остановка",
    "rust/howto-infrastructure.md": "Rust: как собрать один процесс на три брокера и несколько счетов: журнал заявок в файле, рестарт со сверкой",
    "rust/howto-research.md": "Rust: как собрать исследовательский движок: выгрузка длинной истории свечей по лимитам, запись и воспроизведение событий",
    "rust/lifecycle.md": "Rust: как запустить, перезапустить и остановить бота: порядок start, подписка, reconcile, shutdown",
    "rust/recovery.md": "Rust: что делать при Unknown, Overflow, Degraded и долгом обрыве связи, с кодом",
    "rust/accounts.md": "Rust: несколько счетов и брокеров: UD на счет, MD на брокера, один счетчик лимитов на область квоты",
    "rust/stop-orders.md": "Rust: стоп-лосс, тейк-профит и стоп-лимит: place_stop, cancel_stop, active_stop_orders",
    "rust/raw.md": "Rust: любой метод брокера через raw(): те же токен, адрес и счетчик лимитов",
    "rust/mocks.md": "Rust: как тестировать код против имитаций брокеров brokapi-mocks: запуск, настройка, подтверждение исхода",
    "python/index.md": "Раздел Python (работает, альфа): как поставить пакет, таблица инструкций, что умеет первая версия, чего нет",
    "python/quickstart.md": "Python: от нуля до первой заявки на БКС: connect, чтение, reconcile, place, cancel, close. Т-Банк и Финам тем же кодом",
    "python/howto-trading-bot.md": "Python: как написать торгового бота на событиях: журнал, сверка, исходы place, Unknown, Overflow, остановка",
    "python/howto-infrastructure.md": "Python: несколько брокеров и счетов в одном процессе: общий limiter, merge событий, журнал в файле, рестарт со сверкой",
    "python/howto-history.md": "Python: выгрузка длинной истории свечей в файлы по лимитам брокера с продолжением после сбоя (candle_pages)",
    "python/testing.md": "Python: тесты против имитаций brokapi.testing (mock_bcs, mock_tbank, mock_finam), fill, ожидание событий без sleep",
    "python/design.md": "Объяснение: как пакет Python связан с ядром, мост tokio и asyncio, Decimal, ошибки, отличия от Rust",
    "gateway/index.md": "ЕЩЕ НЕТ, заглушка: Gateway, служба с gRPC для любого языка и командная строка brokapi, стадия к бете",
    "cli/index.md": "ЕЩЕ НЕТ, заглушка: CLI, командная строка для людей и ИИ-агентов (status, book, portfolio, вывод --json), через Gateway или напрямую",
    "agent-contract.md": "Контракт для агентов: обязательные правила с причинами, последствиями и кодом, частые ошибки",
    "errors.md": "Ошибки и состояния: что значат, почему бывают, что делать (Unknown, Rejected, RateLimited, Degraded...)",
    "events.md": "Конверт Envelope, события Event, возможности коннекторов caps()",
    "coverage.md": "Какие методы API каждого брокера закрыты общим интерфейсом, а какие только через raw()",
    "api.md": "Справочник API Rust (rustdoc): список крейтов и ссылки на страницы справочника",
    "api-python.md": "Справочник API Python (pdoc, из docstring): модули brokapi, models, errors, numbers, testing и ссылки на страницы справочника",
    "api-gateway.md": "ЕЩЕ НЕТ, заглушка: справочник gRPC-интерфейса Gateway (из .proto), появится вместе со службой",
    "readiness.md": "Готовность: что проверено в коде, на имитации и вживую",
    "live.md": "Живые прогоны: вопросы, на которые ответит только настоящий счет",
    "changelog.md": "Журнал изменений для пользователей, в том числе когда появятся Gateway и CLI",
}

# the reference (rustdoc) pages for the "Справочник API" section of llms.txt
API_CRATES = [
    ("brokapi", "Главный крейт: все под одним именем, брокеры включаются флагами bcs, tbank, finam"),
    ("brokapi_core", "Типы, трейты возможностей (OrderExecutor, OrderBookSource...), события, ошибки. С него начинать"),
    ("brokapi_runtime", "Лимиты (BrokerLimiter), PriceGrid, history (CandlePages), record (запись и воспроизведение событий)"),
    ("brokapi_bcs", "Коннекторы БКС и их настройка"),
    ("brokapi_tbank", "Коннекторы Т-Банка и их настройка"),
    ("brokapi_finam", "Коннекторы Финама и их настройка"),
]

INCLUDE = re.compile(r"\{\{#include\s+([^\s}:]+)(?::([^\s}]+))?\s*\}\}")
ANCHOR_LINE = re.compile(r"^\s*(//|#|<!--)\s*ANCHOR(_END)?:\s*\S+")


def read_summary(src):
    """Returns [(section title or None, [(page title, path, depth)])] in order, from SUMMARY.md."""
    sections, current, first = [], ("Начало", []), True
    for line in open(os.path.join(src, "SUMMARY.md"), encoding="utf-8"):
        line = line.rstrip("\n")
        m = re.match(r"^#\s+(.*\S)\s*$", line)
        if m and first:  # the first heading is the title of the book
            first = False
            continue
        if m:
            if current[1]:
                sections.append(current)
            current = (m.group(1), [])
            continue
        m = re.match(r"^(\s*)[-*]?\s*\[([^\]]+)\]\(([^)]+)\)", line)
        if m:
            depth = len(m.group(1)) // 2
            current[1].append((m.group(2), m.group(3), depth))
    if current[1]:
        sections.append(current)
    return sections


def expand(text, page_dir):
    """Replaces every {{#include file[:anchor]}} with the file, or the part between ANCHOR: and ANCHOR_END:."""

    def one(m):
        path = os.path.normpath(os.path.join(page_dir, m.group(1)))
        anchor = m.group(2)
        lines = open(path, encoding="utf-8").read().split("\n")
        if anchor and re.fullmatch(r"[A-Za-z_][\w\-]*", anchor):
            out, on = [], False
            for line in lines:
                if re.search(r"ANCHOR:\s*%s\s*$" % re.escape(anchor), line):
                    on = True
                elif re.search(r"ANCHOR_END:\s*%s\s*$" % re.escape(anchor), line):
                    on = False
                elif on and not ANCHOR_LINE.match(line):
                    out.append(line)
            if not out:
                raise SystemExit("llms.py: anchor %r not found in %s" % (anchor, path))
            return textwrap.dedent("\n".join(out))  # an anchor inside a function is indented: as in the HTML book, take the common indentation off
        if anchor:  # start:end line range, 1-based, either end may be empty
            a, _, b = anchor.partition(":")
            lo = int(a) - 1 if a else 0
            hi = int(b) if b else len(lines)
            return "\n".join(lines[lo:hi])
        return "\n".join(line for line in lines if not ANCHOR_LINE.match(line)).rstrip("\n")

    return INCLUDE.sub(one, text)


def html_name(path):
    if path == "README.md":
        return "index.html"
    if path.endswith("/README.md"):
        return path[: -len("README.md")] + "index.html"
    return path[:-3] + ".html"


def read_redirects(docs):
    """Returns {old markdown path: new markdown path} from [output.html.redirect] of book.toml.

    Only moves of one html page to another are taken (the target is a relative .html). Entries that point to index.html
    from README.html or to a file that is not a page are not moves: the markdown of a README.md is the page itself."""
    if tomllib is None:
        return {}
    with open(os.path.join(docs, "book.toml"), "rb") as f:
        table = tomllib.load(f).get("output", {}).get("html", {}).get("redirect", {})
    result = {}
    for old, new in table.items():
        if not old.endswith(".html") or not new.endswith(".html") or "://" in new or new.startswith("/"):
            continue
        if os.path.basename(old) == "README.html":
            continue
        old = old.lstrip("/")
        new = os.path.normpath(os.path.join(os.path.dirname(old), new))
        result[old[: -len(".html")] + ".md"] = new[: -len(".html")] + ".md"
    return result


# pages of the common part that hold for Python as they do for Rust (the code on them is in tabs or does not depend on the language)
PYTHON_COMMON = [
    "install.md", "agent-contract.md", "architecture.md", "streams.md", "orders.md", "reconcile.md", "limits.md", "numbers.md",
    "errors.md", "events.md", "brokers/README.md", "brokers/bcs.md", "brokers/tbank.md", "brokers/finam.md",
    "coverage.md", "readiness.md", "live.md", "changelog.md",
]


def python_index(sections, book):
    """python/llms.txt: the index for an agent that writes in Python. Only what it needs, the Rust pages are left out."""
    titles = {p: t for _, items in sections for t, p, _ in items}
    out = [
        "# brokapi для Python",
        "",
        "> Пакет brokapi для Python (pip install brokapi, Python 3.10+) - тонкая обертка над ядром brokapi на Rust: БКС, Т-Банк, "
        "Финам. События через async for, деньги и цены через decimal.Decimal, ошибки с теми же именами и текстом, что у ядра "
        "(Unknown, Rejected, RateLimited, NotReconciled, Degraded). Стадия: закрытая альфа, проверено на имитациях, вживую нет. "
        "На PyPI пакета пока нет (появится в открытой бете): сейчас ставится из репозитория (pip install \"git+https://github.com/runinfra/brokapi#subdirectory=crates/python\" или из клона: pip install ./crates/python, нужен Rust). Документация: %s/" % BASE_URL,
        "",
        "Как писать код. 1) Прочитайте \"Контракт для агентов\": обязательные правила одни для любого языка (торговля только после "
        "reconcile, cid придумывает бот и пишет в журнал до place, Unknown не повторять, один счетчик лимитов на область брокера: "
        "limiter=, цены только Decimal). 2) Возьмите ближайшую инструкцию из раздела Python и копируйте код: каждый пример - тест "
        "против имитаций brokapi.testing. 3) Сигнатуры и поля - в python/api-reference.md. Что пока нет в пакете: стоп-заявки, "
        "raw(), запись событий в файл. Сеть и деньги в тестах не трогайте: mock_bcs(), mock_tbank(), mock_finam().",
        "",
        "## Python",
        "",
    ]
    for _, items in sections:
        for page_title, path, _ in items:
            if path.startswith("python/"):
                out.append("- [%s](%s/%s): %s" % (page_title, BASE_URL, path, DESCRIPTIONS[path]))
    out += ["", "## Общее, что верно и для Python", ""]
    for path in PYTHON_COMMON:
        md = "index.md" if path == "README.md" else path
        out.append("- [%s](%s/%s): %s" % (titles[path], BASE_URL, md, DESCRIPTIONS[path]))
    out += [
        "",
        "## Справочник API Python (английский)",
        "",
        "- [python/api-reference.md](%s/python/api-reference.md): справочник одним markdown-файлом: сигнатуры и описания" % BASE_URL,
        "- [brokapi (HTML)](%s/api/python/brokapi.html): тот же справочник страницами (pdoc)" % BASE_URL,
        "",
        "## Optional",
        "",
        "- [llms.txt](%s/llms.txt): индекс всей книги, включая Rust" % BASE_URL,
        "- [llms-full.txt](%s/llms-full.txt): вся книга одним markdown-файлом" % BASE_URL,
        "",
    ]
    os.makedirs(os.path.join(book, "python"), exist_ok=True)
    with open(os.path.join(book, "python", "llms.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(out))


def main():
    docs, book = sys.argv[1], sys.argv[2]
    src = os.path.join(docs, "src")
    sections = read_summary(src)
    pages = [p for _, items in sections for p in items]

    missing = [p for _, p, _ in pages if p not in DESCRIPTIONS]
    if missing:
        raise SystemExit("llms.py: add a line to DESCRIPTIONS for: " + ", ".join(missing))
    unused = [p for p in DESCRIPTIONS if p not in {q for _, q, _ in pages}]
    if unused:
        raise SystemExit("llms.py: DESCRIPTIONS has pages that are not in SUMMARY.md: " + ", ".join(unused))

    for old, new in read_redirects(docs).items():
        if new not in {p for _, p, _ in pages}:
            raise SystemExit("llms.py: redirect %s points to %s, which is not a page of SUMMARY.md" % (old, new))
        with open(os.path.join(book, old), "w", encoding="utf-8") as f:
            f.write("# Страница переехала\n\nЭта страница теперь здесь: %s/%s\n" % (BASE_URL, new))

    full = []
    for _, path, _ in pages:
        page_src = os.path.join(src, path)
        text = expand(open(page_src, encoding="utf-8").read(), os.path.dirname(page_src))
        out = os.path.join(book, path)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            f.write(text if text.endswith("\n") else text + "\n")
        if path == "README.md":  # the HTML of README.md is index.html: give the markdown that name too
            with open(os.path.join(book, "index.md"), "w", encoding="utf-8") as f:
                f.write(text if text.endswith("\n") else text + "\n")
        full.append("<!-- page: %s | %s/%s -->\n\n%s" % (path, BASE_URL, html_name(path), text.strip("\n")))

    # llms.txt
    out = ["# %s" % TITLE, "", "> " + SUMMARY_LINE, "", INTRO, ""]
    for title, items in sections:
        out.append("## %s" % (title + (" (%s)" % SECTION_NOTES[title] if title in SECTION_NOTES else "")))
        out.append("")
        for page_title, path, _ in items:
            md = "index.md" if path == "README.md" else path
            out.append("- [%s](%s/%s): %s" % (page_title, BASE_URL, md, DESCRIPTIONS[path]))
        out.append("")
    out.append("## Справочник API (rustdoc, английский)")
    out.append("")
    out.append(
        "- [api-reference.md](%s/api-reference.md): справочник API одним markdown-файлом: объявления типов, поля, "
        "варианты, методы трейтов с разделами Errors и Contract (без примеров кода)" % BASE_URL
    )
    for crate, about in API_CRATES:
        out.append("- [%s](%s/api/%s/index.html): %s" % (crate, BASE_URL, crate, about))
    out.append("")
    out.append(
        "- [python/api-reference.md](%s/python/api-reference.md): справочник API пакета Python одним markdown-файлом: "
        "сигнатуры и описания классов, методов, ошибок" % BASE_URL
    )
    out.append("- [brokapi (Python)](%s/api/python/brokapi.html): справочник пакета для Python в HTML (pdoc)" % BASE_URL)
    out.append("")
    out.append("## Optional")
    out.append("")
    out.append("- [llms-full.txt](%s/llms-full.txt): вся книга одним markdown-файлом" % BASE_URL)
    out.append("")
    with open(os.path.join(book, "llms.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(out))

    header = "# brokapi: вся книга одним файлом\n\n%s\n\nКаждая страница начинается со строки `<!-- page: файл | адрес -->`.\n" % INTRO
    with open(os.path.join(book, "llms-full.txt"), "w", encoding="utf-8") as f:
        f.write(header + "\n\n---\n\n" + "\n\n---\n\n".join(full) + "\n")

    python_index(sections, book)
    print("llms: %d pages, llms.txt, python/llms.txt, llms-full.txt" % len(pages))


if __name__ == "__main__":
    main()
