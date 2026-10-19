#!/usr/bin/env python3
"""Turns the rustdoc reference into one markdown file for agents (python 3 standard library only).

  usage: apidigest.py <book dir>      reads <book dir>/api/<crate>/**.html, writes <book dir>/api-reference.md

An agent that reads text, not HTML, still needs the exact declaration of a type, its fields and variants, the methods
of a trait with their `# Errors` and `# Contract` sections. This file has them for the crates a program uses: the item
declaration, the description, the fields or variants or methods with their descriptions. The code examples of rustdoc
are left out: the book has the examples that run. Called by docs/build.sh after the reference is built.
"""
import html
import os
import re
import sys
from html.parser import HTMLParser

# crate -> None (every item) or a set of item names (only these)
CRATES = [
    ("brokapi_core", None),
    ("brokapi_runtime", None),
    ("brokapi_bcs", None),
    ("brokapi_tbank", None),
    ("brokapi_finam", None),
    ("brokapi_bcs_protocol", {"BcsConfig"}),
    ("brokapi_tbank_protocol", {"TbankConfig"}),
]
KINDS = ("struct", "enum", "trait", "fn", "type", "constant", "macro")
SKIP_SECTIONS = {
    "synthetic-implementations", "blanket-implementations", "trait-implementations",
    "implementors", "foreign-impls", "synthetic-implementors", "implementations",
}


class Page(HTMLParser):
    """Collects the declaration and the documented members of one rustdoc item page as markdown lines."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []          # finished markdown lines
        self.title = ""
        self.decl = ""
        self.in_main = False
        self.stack = []        # (tag, classes, id) of open elements
        self.capture = None    # what the text goes to: "title", "decl", "member", "doc"
        self.buf = []
        self.skip_depth = 0    # inside an example or a skipped section
        self.skip_section = False
        self.doc_depth = 0
        self.pending_member = None
        self.first_doc = True

    # --- helpers
    def _flush(self, kind):
        text = "".join(self.buf)
        self.buf = []
        return text

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        cls = (a.get("class") or "").split()
        ident = a.get("id") or ""
        if tag == "section" and ident == "main-content":
            self.in_main = True
        if not self.in_main:
            return
        self.stack.append((tag, cls, ident))
        if self.skip_depth:
            self.skip_depth += 1
            return
        if tag == "h2" and ident in SKIP_SECTIONS:
            self.skip_section = True
        elif tag == "h2" and self.skip_section:
            self.skip_section = False
        if self.skip_section:
            return
        if tag == "h1" and "section-header" not in cls and not self.title and self.capture is None:
            self.capture = "title"
            self.buf = []
            return
        if tag == "pre" and "item-decl" in cls:
            self.capture = "decl"
            self.buf = []
            return
        if tag == "pre" or (tag == "div" and "example-wrap" in cls):
            self.skip_depth = 1
            return
        if tag == "div" and "docblock" in cls and self.doc_depth == 0:
            self.doc_depth = 1
            self.capture = "doc"
            self.buf = []
            return
        if self.doc_depth and tag == "div":
            self.doc_depth += 1
        if tag == "h4" and "code-header" in cls and not self.doc_depth:
            self.capture = "member"
            self.buf = []
            return
        if tag == "span" and ident.startswith(("structfield.", "variant.")) and not self.doc_depth:
            self.capture = "member"
            self.buf = []
            return
        if tag == "section" and ident.startswith("variant.") and not self.doc_depth:
            self.capture = "member"
            self.buf = []
            return
        if self.capture == "doc":
            if tag == "code":
                self.buf.append("`")
            elif tag == "li":
                self.buf.append("\n- ")
            elif tag in ("p", "br"):
                self.buf.append("\n")
            elif tag in ("h1", "h2", "h3", "h4"):
                self.buf.append("\n\n**")

    def handle_endtag(self, tag):
        if not self.in_main or not self.stack:
            return
        top = self.stack.pop() if self.stack else None
        if self.skip_depth:
            self.skip_depth -= 1
            return
        if self.skip_section:
            return
        if self.capture == "title" and tag == "h1":
            self.title = re.sub(r"\s+", " ", self._flush("title")).replace("Copy item path", "").strip()
            self.capture = None
        elif self.capture == "decl" and tag == "pre":
            self.decl = self._flush("decl").strip("\n")
            self.capture = None
        elif self.capture == "member" and (
            (tag == "h4") or (tag in ("span", "section") and top and top[2].startswith(("structfield.", "variant.")))
        ):
            text = re.sub(r"\s+", " ", self._flush("member")).strip().replace("§", "")
            text = re.sub(r"\s*Source\s*$", "", text)
            if text:
                self.out.append("\n- `%s`" % text)
            self.capture = None
        elif self.capture == "doc":
            if tag == "code":
                self.buf.append("`")
            elif tag in ("h1", "h2", "h3", "h4"):
                self.buf.append("**\n")
            elif tag == "div":
                self.doc_depth -= 1
                if self.doc_depth == 0:
                    text = self._flush("doc")
                    self.capture = None
                    self._emit_doc(text)
            elif tag == "p":
                self.buf.append("\n")

    def handle_data(self, data):
        if self.skip_depth or self.skip_section or self.capture is None:
            return
        if self.capture == "member" and data.strip() in ("Source", "§"):
            return
        self.buf.append(data)

    def _emit_doc(self, text):
        text = text.replace("§", "")
        lines = [re.sub(r"[ \t]+", " ", l).strip() for l in text.split("\n")]
        # drop a section whose body was only code examples
        cleaned, i = [], 0
        while i < len(lines):
            l = lines[i]
            if l.startswith("**Examples") or l.startswith("**Example"):
                i += 1
                while i < len(lines) and not lines[i].startswith("**"):
                    i += 1
                continue
            cleaned.append(l)
            i += 1
        body = re.sub(r"\n{3,}", "\n\n", "\n".join(cleaned)).strip()
        if not body:
            return
        if self.out and self.out[-1].startswith("\n- "):
            indent = "\n".join("  " + l if l else l for l in body.split("\n"))
            self.out.append(indent)
        else:
            self.out.append(body)


FUTURE = re.compile(r"-> Pin<Box<dyn Future<Output = (.*?)> \+ Send \+ 'async_trait>>", re.S)


def tidy(text):
    """Undoes what #[async_trait] does to a signature: `async fn name(&self, ...) -> T` is what the code says."""
    if "'async_trait" not in text:
        return text
    text = re.sub(r"\s*where\s+Self: 'async_trait(?:,\s*'life\d+: 'async_trait)*,?", "", text)
    text = re.sub(r"<'life\d+(?:, 'life\d+)*(?:, 'async_trait)?>|<'async_trait>", "", text)
    text = re.sub(r"&'life\d+ ", "&", text)
    text = re.sub(r"(\bfn\b)", r"async fn", text) if FUTURE.search(text) else text
    return FUTURE.sub(r"-> \1", text)


def parse(path):
    p = Page()
    p.feed(open(path, encoding="utf-8").read())
    return p


def item_pages(crate_dir):
    seen = {}
    for root, _, files in os.walk(crate_dir):
        if "/src" in root:
            continue
        for f in files:
            m = re.match(r"^(%s)\.(.+)\.html$" % "|".join(KINDS), f)
            if not m:
                continue
            key = (m.group(1), m.group(2))
            path = os.path.join(root, f)
            if key not in seen or len(path) < len(seen[key]):
                seen[key] = path
    return seen


def main():
    book = sys.argv[1]
    out = [
        "# brokapi: справочник API одним файлом",
        "",
        "Текст справочника для агентов: объявления типов, поля, варианты, методы трейтов и их описание (разделы Errors и "
        "Contract). Собран из rustdoc, примеры кода опущены: рабочие примеры в книге. Описание в справочнике на английском. "
        "Полный справочник с исходниками и ссылками: api/<крейт>/index.html.",
        "",
    ]
    total = 0
    for crate, only in CRATES:
        cdir = os.path.join(book, "api", crate)
        if not os.path.isdir(cdir):
            raise SystemExit("apidigest.py: no %s" % cdir)
        out.append("\n## Крейт %s\n" % crate)
        pages = item_pages(cdir)
        for (kind, name), path in sorted(pages.items(), key=lambda kv: (kv[0][0] != "trait", kv[0][1])):
            if only is not None and name not in only:
                continue
            page = parse(path)
            out.append("\n### %s\n" % (page.title or "%s %s" % (kind, name)))
            if page.decl:
                out.append("```rust\n%s\n```" % tidy(page.decl))
            out.extend(tidy(x) for x in page.out)
            total += 1
    text = "\n".join(out) + "\n"
    with open(os.path.join(book, "api-reference.md"), "w", encoding="utf-8") as f:
        f.write(text)
    print("apidigest: %d items, %d bytes" % (total, len(text)))


if __name__ == "__main__":
    main()
