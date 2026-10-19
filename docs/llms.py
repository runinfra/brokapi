#!/usr/bin/env python3
"""Builds the files for AI agents next to the HTML book (called by docs/build.sh, python 3 standard library only).

  llms.txt        the index of the documentation for agents (https://llmstxt.org): a title, one sentence, sections of
                  links with one line about each link
  llms-full.txt   the whole book as one markdown file, in the order of SUMMARY.md
  <page>.md       the markdown of every page next to its <page>.html, with {{#include}} lines replaced by the code

usage: llms.py <docs dir> <book dir>
       <docs dir> holds book.toml and src/, <book dir> is the output of `mdbook build` (it gets the files).

Every page of SUMMARY.md must have a line in DESCRIPTIONS below: a new page without one fails the build, so the index
for agents cannot fall behind the book.
"""
import os
import re
import sys

BASE_URL = os.environ.get("BROKAPI_DOCS_URL", "https://brokapi.runinfra.ru").rstrip("/")

TITLE = "brokapi"
SUMMARY_LINE = (
    "brokapi - библиотека на Rust: один интерфейс к API брокеров БКС, Т-Банк и Финам (рыночные данные, заявки, "
    "сверка со счетом, лимиты запросов). Стадия: закрытая альфа, проверено на имитациях."
)
INTRO = """Как пользоваться этим набором. Книга написана по задачам. Если нужно написать код: прочитайте "Контракт для \
агентов" (все обязательные правила на одной странице), затем инструкцию, ближайшую к задаче (торговый бот, несколько \
брокеров и счетов, исследовательский движок), и копируйте код оттуда: каждый пример - рабочий тест против имитаций. \
Точные сигнатуры типов и методов - в справочнике API (rustdoc), он ссылается на ту же логику. Весь текст книги одним \
файлом: llms-full.txt. У каждой страницы есть markdown-исходник рядом с HTML (замените .html на .md)."""

# path of the page in src/ -> one line for llms.txt
DESCRIPTIONS = {
    "README.md": "Что такое brokapi, три главных правила, что библиотека не делает, честный статус",
    "quickstart.md": "От нуля до первой заявки на БКС, Т-Банке и Финаме: подключение, сверка, подписки, заявка, отмена",
    "architecture.md": "Карта: коннекторы MD и UD, счетчик лимитов, канал событий и три круга механики со схемами",
    "streams.md": "Круг событий: подписки, режимы буфера (Conflate, DropNewest, Lossless), Overflow, Resync, обрывы",
    "orders.md": "Круг команд: cid, порядок проверок place, исходы команды, почему нет автоповтора, машина состояний заявки",
    "reconcile.md": "Круг жизни: шлюз торговых команд и сверка reconcile, три списка отчета, что хранит бот",
    "limits.md": "Лимиты запросов: BrokerLimiter, ведра, RateLimited, почему отмена в отдельном ведре",
    "numbers.md": "Price, Qty, Money на Decimal, округление до шага цены, PriceGrid, перевод форматов брокеров",
    "lifecycle.md": "Как запустить, перезапустить и остановить бота: порядок start, подписка, reconcile, shutdown",
    "recovery.md": "Что делать при Unknown, Overflow, Degraded и долгом обрыве связи, с кодом",
    "accounts.md": "Несколько счетов и брокеров: UD на счет, MD на брокера, один счетчик лимитов на область квоты",
    "stop-orders.md": "Стоп-лосс, тейк-профит и стоп-лимит: place_stop, cancel_stop, active_stop_orders",
    "raw.md": "Любой метод брокера через raw(): те же токен, адрес и счетчик лимитов",
    "howto-trading-bot.md": "Как написать торгового бота на событиях: стакан, сигнал, заявка, статусы, выход из Unknown, остановка",
    "howto-infrastructure.md": "Как собрать один процесс на три брокера и несколько счетов: журнал заявок в файле, рестарт со сверкой",
    "howto-research.md": "Как собрать исследовательский движок: выгрузка длинной истории свечей по лимитам, запись и воспроизведение событий",
    "mocks.md": "Как тестировать код против имитаций брокеров brokapi-mocks: запуск, настройка, подтверждение исхода",
    "agent-contract.md": "Контракт для агентов: обязательные правила с причинами, последствиями и кодом, частые ошибки",
    "errors.md": "Ошибки и состояния: что значат, почему бывают, что делать (Unknown, Rejected, RateLimited, Degraded...)",
    "events.md": "Конверт Envelope, события Event, возможности коннекторов caps()",
    "brokers/README.md": "Сравнение трех брокеров: идентификаторы инструментов, единицы, особенности",
    "brokers/bcs.md": "БКС: токены, идентификаторы, единицы количества, ограничения",
    "brokers/tbank.md": "Т-Банк: песочница, счета, лоты, uid инструмента, ограничения",
    "brokers/finam.md": "Финам: секрет и JWT, символ SBER@MISX, количество в штуках, нет replace",
    "api.md": "Справочник API (rustdoc): список крейтов и ссылки на страницы справочника",
    "coverage.md": "Какие методы API каждого брокера закрыты общим интерфейсом, а какие только через raw()",
    "readiness.md": "Готовность: что проверено в коде, на имитации и вживую",
    "live.md": "Живые прогоны: вопросы, на которые ответит только настоящий счет",
    "changelog.md": "Журнал изменений для пользователей",
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
            return "\n".join(out)
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
        out.append("## %s" % title)
        out.append("")
        for page_title, path, _ in items:
            md = "index.md" if path == "README.md" else path
            out.append("- [%s](%s/%s): %s" % (page_title, BASE_URL, md, DESCRIPTIONS[path]))
        out.append("")
    out.append("## Справочник API (rustdoc, английский)")
    out.append("")
    for crate, about in API_CRATES:
        out.append("- [%s](%s/api/%s/index.html): %s" % (crate, BASE_URL, crate, about))
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

    print("llms: %d pages, llms.txt, llms-full.txt" % len(pages))


if __name__ == "__main__":
    main()
