#!/usr/bin/env python3
"""Removes the preload of rustdoc's own fonts from the generated reference.

rustdoc puts a small script into the head of every page. It adds `<link rel="preload" as="font">` for
Fira Sans, Source Serif 4 and Source Code Pro, and the browser downloads about 400 KB of fonts. The
reference of this project sets its own fonts in docs/rustdoc-runinfra.css, so those downloads are
wasted. The @font-face rules of rustdoc stay: a font that no text uses is never fetched.

    python3 docs/rustdoc_nofonts.py target/doc      # rewrites the pages in place
    python3 docs/rustdoc_nofonts.py --selftest      # checks the pattern on a sample, no files

Exit code 1 when a page still preloads a font after the rewrite (rustdoc changed the script: update
PATTERN). Standard library only.
"""
import pathlib
import re
import sys

# The script rustdoc writes: `if(window.location.protocol!=="file:")document.head.insertAdjacentHTML(
#   "beforeend","<font files, comma separated>".split(",").map(f=>`<link rel="preload" as="font" ...>`).join(""))`
PATTERN = re.compile(
    r'<script>if\(window\.location\.protocol!=="file:"\)document\.head\.insertAdjacentHTML\('
    r'"beforeend","[^"]*"\.split\(","\)\.map\(f=>`<link rel="preload" as="font"[^`]*`\)\.join\(""\)\)</script>'
)
# Whatever else still asks the browser for a font before the page needs it.
LEFTOVER = re.compile(r'rel="preload"[^>]*as="font"|as="font"[^>]*rel="preload"')

SAMPLE = (
    '<head><title>x</title><script>if(window.location.protocol!=="file:")document.head.insertAdjacentHTML('
    '"beforeend","FiraSans-Regular-0fe48ade.woff2,SourceSerif4-Regular-6b053e98.ttf.woff2".split(",").map('
    'f=>`<link rel="preload" as="font" type="font/woff2"href="../static.files/${f}">`).join(""))</script>'
    '<link rel="stylesheet" href="a.css"></head>'
)


def strip(text: str) -> str:
    return PATTERN.sub("", text)


def selftest() -> None:
    out = strip(SAMPLE)
    assert "FiraSans" not in out and "SourceSerif" not in out, out
    assert '<link rel="stylesheet" href="a.css">' in out, "the rest of the head stays"
    assert strip(out) == out, "a second run changes nothing"
    assert strip("<p>no fonts here</p>") == "<p>no fonts here</p>"
    print("selftest ok")


def main(argv: list) -> int:
    if argv[1:] == ["--selftest"]:
        selftest()
        return 0
    if len(argv) != 2:
        print(__doc__)
        return 2
    changed = 0
    left = []
    for page in pathlib.Path(argv[1]).rglob("*.html"):
        text = page.read_text(encoding="utf-8")
        new = strip(text)
        if new != text:
            page.write_text(new, encoding="utf-8")
            changed += 1
        if LEFTOVER.search(new):
            left.append(str(page))
    print(f"font preload removed from {changed} pages")
    if left:
        print(f"still preloading fonts: {len(left)} pages, for example {left[0]}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
