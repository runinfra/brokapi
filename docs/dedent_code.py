#!/usr/bin/env python3
"""mdBook preprocessor: takes the common indentation off the code blocks.

`{{#include file:anchor}}` puts the lines of an anchor as they stand in the file. An anchor inside a function is
indented, and an indented Python snippet is not valid Python when it is copied. This runs after the `links` preprocessor
(which makes the includes) and removes the indentation that all the lines of a fenced block share. Blocks that start at
the left edge, and diagrams, are not touched.

mdBook calls it as `dedent_code.py supports <renderer>` (answer: exit code 0) and then with `[context, book]` on stdin.
"""
import json
import re
import sys
import textwrap

FENCE = re.compile(r"^(?P<indent>[ ]{0,3})(?P<fence>`{3,})(?P<info>[^`\n]*)\n(?P<body>.*?)^(?P=indent)(?P=fence)[ ]*$", re.S | re.M)


def dedent_blocks(text: str) -> str:
    def one(m: re.Match) -> str:
        info = m.group("info").strip().split(",")[0].split(" ")[0]
        if info in ("mermaid", "text", "console"):
            return m.group(0)
        body = m.group("body")
        fixed = textwrap.dedent(body)
        if fixed == body:
            return m.group(0)
        return f"{m.group('indent')}{m.group('fence')}{m.group('info')}\n{fixed}{m.group('indent')}{m.group('fence')}"

    return FENCE.sub(one, text)


def walk(items: list) -> None:
    for item in items:
        chapter = item.get("Chapter") if isinstance(item, dict) else None
        if chapter:
            chapter["content"] = dedent_blocks(chapter["content"])
            walk(chapter.get("sub_items", []))


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "supports":
        sys.exit(0)
    _context, book = json.load(sys.stdin)
    walk(book.get("items", book.get("sections", [])))
    json.dump(book, sys.stdout)


if __name__ == "__main__":
    main()
