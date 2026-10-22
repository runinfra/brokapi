#!/usr/bin/env python3
"""The version of the library in the book comes from one place: `version` in [workspace.package] of ../Cargo.toml.

Pages write `{{brokapi_version}}` (Cargo form, 0.1.0-alpha.3) or `{{brokapi_version_py}}` (PyPI form, 0.1.0a3) where the
number must appear, and the build puts the real one in:

  - mdBook preprocessor `version` (book.toml): `brokapi_version.py` is called as `supports <renderer>` and then with
    `[context, book]` on stdin, and replaces the two placeholders in every chapter;
  - docs/llms.py replaces them in the markdown it writes for agents (llms-full.txt, page.md);
  - `brokapi_version.py stamp <book dir>` replaces the token @@BROKAPI_VERSION@@ in the html of the book: the theme
    (theme/index.hbs) is not a chapter, so the preprocessor does not see it. docs/build.sh and tools/check call it after
    `mdbook build`. In `mdbook serve` the token is left as it is.

`brokapi_version.py print [--py]` prints the number. tests/version_sync.rs fails if a number is written by hand anywhere.
"""
import json
import os
import re
import sys

CARGO_TOML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Cargo.toml")
TOKEN = "@@BROKAPI_VERSION@@"


def cargo_version(path=CARGO_TOML):
    """`version` from the [workspace.package] table."""
    text = open(path, encoding="utf-8").read()
    table = re.search(r"^\[workspace\.package\]\n(.*?)(?=^\[|\Z)", text, re.S | re.M)
    if not table:
        raise SystemExit("brokapi_version.py: no [workspace.package] in %s" % path)
    m = re.search(r'^version\s*=\s*"([^"]+)"', table.group(1), re.M)
    if not m:
        raise SystemExit("brokapi_version.py: no version in [workspace.package] of %s" % path)
    return m.group(1)


def pep440(version):
    """0.1.0-alpha.3 -> 0.1.0a3, 0.2.0-rc.1 -> 0.2.0rc1: the form PyPI and `brokapi.__version__` use."""
    m = re.fullmatch(r"(\d+\.\d+\.\d+)(?:-(alpha|beta|rc)\.(\d+))?", version)
    if not m:
        raise SystemExit("brokapi_version.py: cannot turn %r into a Python version" % version)
    base, kind, number = m.groups()
    return base if kind is None else base + {"alpha": "a", "beta": "b", "rc": "rc"}[kind] + number


def substitute(text, version=None):
    version = version or cargo_version()
    return text.replace("{{brokapi_version_py}}", pep440(version)).replace("{{brokapi_version}}", version)


def _walk(items, version):
    for item in items:
        chapter = item.get("Chapter") if isinstance(item, dict) else None
        if chapter:
            chapter["content"] = substitute(chapter["content"], version)
            _walk(chapter.get("sub_items", []), version)


def stamp(book_dir, version=None):
    version = version or cargo_version()
    count = 0
    for root, _dirs, files in os.walk(book_dir):
        for name in files:
            if not name.endswith(".html"):
                continue
            path = os.path.join(root, name)
            text = open(path, encoding="utf-8").read()
            if TOKEN in text:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(text.replace(TOKEN, version))
                count += 1
    if count == 0:
        raise SystemExit("brokapi_version.py: no page of %s has %s (theme/index.hbs lost it?)" % (book_dir, TOKEN))
    return count


def main():
    args = sys.argv[1:]
    if args[:1] == ["supports"]:
        sys.exit(0)
    if args[:1] == ["print"]:
        print(pep440(cargo_version()) if "--py" in args[1:] else cargo_version())
        return
    if args[:1] == ["stamp"] and len(args) == 2:
        stamp(args[1])
        return
    if args:
        raise SystemExit("usage: brokapi_version.py print [--py] | stamp <book dir> | (mdBook preprocessor)")
    _context, book = json.load(sys.stdin)
    _walk(book.get("items", book.get("sections", [])), cargo_version())
    json.dump(book, sys.stdout)


if __name__ == "__main__":
    main()
