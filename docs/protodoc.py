#!/usr/bin/env python3
"""Makes the reference of the gRPC API (docs/src/api-gateway.md) from the .proto files in crates/gateway/proto/.

The page is not written by hand: the text between the lines `<!-- proto:begin -->` and `<!-- proto:end -->` is made here from the
files and their comments, the text around it is the introduction. Python 3 standard library only.

  python3 docs/protodoc.py           rewrites the generated part of docs/src/api-gateway.md
  python3 docs/protodoc.py --check   exits with 1 if the page is not what the files would give (tools/check and docs/build.sh run it)

The .proto files are the contract, the comments in them are English, and so is the generated reference. Only what the files
hold goes in: services with their calls, messages with fields and numbers, enums with values.
"""
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
PAGE = os.path.join(ROOT, "docs", "src", "api-gateway.md")
FILES = [
    ("brokapi/gateway/v1/gateway.proto", "Gateway: служба для ботов и события"),
    ("brokapi/gateway/v1/types.proto", "Типы: значения, заявки, портфель, рынок"),
    ("brokapi/service/v1/service.proto", "Service: служба `brokapi up` и ее команды"),
]
BEGIN, END = "<!-- proto:begin -->", "<!-- proto:end -->"
SCALARS = {"string", "bool", "bytes", "double", "float", "int32", "int64", "uint32", "uint64", "sint32", "sint64", "fixed32", "fixed64"}

RULE = re.compile(r"^-{5,}")


def read_proto(path):
    """Returns (file comment, [service], [message], [enum]); every item has a name, its comment and its parts."""
    lines = open(path, encoding="utf-8").read().split("\n")
    file_doc, items, stack = [], {"service": [], "message": [], "enum": []}, []
    pending, group = [], None  # comment lines right above the next declaration; the heading of a group of rpc
    blank_before_comment = True
    seen_decl = False
    package = ""
    oneof = None

    def doc():
        nonlocal pending
        text = " ".join(p.strip() for p in pending if p.strip())
        pending = []
        return text

    for raw in lines:
        line = raw.strip()
        if line.startswith("//"):
            text = line[2:]
            text = text[1:] if text.startswith(" ") else text
            body = text.strip()
            if RULE.match(body):
                pending = []
                continue
            if body.startswith("----"):  # "---- orders: ..." heading of a group of calls
                pending = []
                group = body.strip("- ").strip()
                continue
            pending.append(text.rstrip())
            continue
        if not line:
            if not seen_decl and pending:  # the comment at the top of the file
                file_doc = list(pending)
                pending = []
            else:
                pending = []  # a comment separated by a blank line belongs to nothing
            continue
        m = re.match(r"^package\s+([\w.]+);", line)
        if m:
            package = m.group(1)
            if pending and not file_doc:  # the comment between `syntax` and `package` describes the file
                file_doc = list(pending)
            pending = []
            continue
        if line.startswith(("syntax", "import", "option")):
            seen_decl = True
            pending = []
            continue
        m = re.match(r"^(service|message|enum)\s+(\w+)\s*\{(\s*\})?", line)
        if m:
            seen_decl = True
            kind, name, closed = m.group(1), m.group(2), m.group(3)
            if kind in ("message", "enum") and stack and stack[-1]["kind"] == "message":
                full = stack[-1]["name"] + "." + name
            else:
                full = name
            item = {"kind": kind, "name": full, "doc": doc(), "fields": [], "values": [], "calls": []}
            items[kind].append(item)
            if not closed:
                stack.append(item)
            continue
        m = re.match(r"^oneof\s+(\w+)\s*\{", line)
        if m:
            oneof = m.group(1)
            pending = []
            continue
        if line == "}":
            if oneof is not None:
                oneof = None
            elif stack:
                stack.pop()
            pending = []
            continue
        if not stack:
            continue
        top = stack[-1]
        if top["kind"] == "service":
            m = re.match(r"^rpc\s+(\w+)\((\.?\w[\w.]*)\)\s+returns\s+\((stream\s+)?(\.?\w[\w.]*)\);", line)
            if m:
                # A type written with the full name (`.brokapi.gateway.v1.Portfolio`, where a call of the same name hides the
                # type) is shown by its short name, as the other calls are.
                own = "." + package + "."
                short_name = lambda t: t[len(own):] if t.startswith(own) else t.lstrip(".")
                top["calls"].append(
                    {"name": m.group(1), "request": short_name(m.group(2)), "reply": short_name(m.group(4)), "stream": bool(m.group(3)), "doc": doc(), "group": group}
                )
            continue
        if top["kind"] == "enum":
            m = re.match(r"^(\w+)\s*=\s*(-?\d+);", line)
            if m:
                top["values"].append({"name": m.group(1), "number": m.group(2), "doc": doc()})
            continue
        m = re.match(r"^(repeated\s+|optional\s+)?([\w.]+)\s+(\w+)\s*=\s*(\d+);", line)
        if m:
            top["fields"].append(
                {
                    "label": (m.group(1) or "").strip(),
                    "type": m.group(2),
                    "name": m.group(3),
                    "number": m.group(4),
                    "oneof": oneof,
                    "doc": doc(),
                }
            )
    return package, "", file_doc, items


def anchor(package, name):
    return (package + "." + name).lower().replace(".", "-")


def render():
    parsed = [(path, title, read_proto(os.path.join(ROOT, "crates", "gateway", "proto", path))) for path, title in FILES]
    # every type name -> (package, full name), so that a field type becomes a link
    known = {}
    for _, _, (package, _, _, items) in parsed:
        for kind in ("message", "enum"):
            for it in items[kind]:
                known[(package, it["name"])] = True
    simple = {}
    for (package, name) in known:
        simple.setdefault(name.split(".")[-1], []).append((package, name))

    def link(package, type_name, owner):
        """A field type as text, a link when it is a type of these files."""
        if type_name in SCALARS:
            return f"`{type_name}`"
        parts = type_name.split(".")
        # nested type of the owner: Kind -> Owner.Kind
        for cand in (owner + "." + type_name, type_name):
            if (package, cand) in known:
                return f"[`{type_name}`](#{anchor(package, cand)})"
        # a type of another file, written with the package: brokapi.gateway.v1.HealthResponse
        for pkg_len in range(len(parts) - 1, 0, -1):
            pkg, name = ".".join(parts[:pkg_len]), ".".join(parts[pkg_len:])
            if (pkg, name) in known:
                return f"[`{parts[-1]}`](#{anchor(pkg, name)})"
        if len(simple.get(parts[-1], [])) == 1:
            pkg, name = simple[parts[-1]][0]
            return f"[`{type_name}`](#{anchor(pkg, name)})"
        return f"`{type_name}`"

    def cell(text):
        return text.replace("|", "\\|").replace("\n\n", " ").strip()

    out = []
    for path, title, (package, short, file_doc, items) in parsed:
        out.append(f"## {title}\n")
        out.append(f"Файл `proto/{path}`, пакет `{package}`.\n")
        if file_doc:
            # the comment at the top of the file as it is: it is the contract in the words of its authors
            out.append("```text\n" + "\n".join(file_doc).strip("\n") + "\n```\n")
        for svc in items["service"]:
            out.append(f"### Сервис {svc['name']}\n")
            anchor_line = f'<a id="{anchor(package, svc["name"])}"></a>\n'
            out.append(anchor_line)
            if svc["doc"]:
                out.append(svc["doc"] + "\n")
            group = object()
            rows = []  # the table of the group being written
            for call in svc["calls"]:
                if call["group"] != group:
                    group = call["group"]
                    if rows:
                        out.append("\n".join(rows) + "\n")
                        rows = []
                    if group:
                        out.append(f"**{group[0].upper() + group[1:]}**\n")
                    rows = ["| Вызов | Запрос | Ответ | Описание |", "|---|---|---|---|"]
                req = link(package, call["request"], "")
                rep = link(package, call["reply"], "")
                if call["stream"]:
                    rep = "поток " + rep
                rows.append(f"| `{call['name']}` | {req} | {rep} | {cell(call['doc'])} |")
            if rows:
                out.append("\n".join(rows) + "\n")
        for kind, heading in (("message", "Сообщение"), ("enum", "Перечисление")):
            for it in items[kind]:
                out.append(f'<a id="{anchor(package, it["name"])}"></a>\n')
                out.append(f"### {it['name']}\n")
                if it["doc"]:
                    out.append(it["doc"] + "\n")
                if kind == "message":
                    if not it["fields"]:
                        out.append("Без полей.\n")
                        continue
                    out.append("| Поле | Тип | № | Описание |")
                    out.append("|---|---|---|---|")
                    for f in it["fields"]:
                        t = link(package, f["type"], it["name"])
                        if f["label"] == "repeated":
                            t = "список " + t
                        elif f["label"] == "optional":
                            t = t + ", необязательное"
                        d = cell(f["doc"])
                        if f["oneof"]:
                            d = (f"Одно из группы `{f['oneof']}`. " + d).strip()
                        out.append(f"| `{f['name']}` | {t} | {f['number']} | {d} |")
                    out.append("")
                else:
                    out.append("| Значение | Число | Описание |")
                    out.append("|---|---|---|")
                    for v in it["values"]:
                        out.append(f"| `{v['name']}` | {v['number']} | {cell(v['doc'])} |")
                    out.append("")
    return "\n".join(out).rstrip() + "\n"


def build_page():
    page = open(PAGE, encoding="utf-8").read()
    if BEGIN not in page or END not in page:
        sys.exit(f"{PAGE}: no {BEGIN} / {END} lines")
    head, rest = page.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    return head + BEGIN + "\n\n" + render() + "\n" + END + tail


def main():
    new = build_page()
    if "--check" in sys.argv:
        if open(PAGE, encoding="utf-8").read() != new:
            sys.exit("docs/src/api-gateway.md is not what crates/gateway/proto/ gives: run `python3 docs/protodoc.py`")
        print("api-gateway.md matches the .proto files")
    else:
        open(PAGE, "w", encoding="utf-8").write(new)
        print("wrote", os.path.relpath(PAGE, ROOT))


if __name__ == "__main__":
    main()
