#!/usr/bin/env python3
"""Builds the Python API reference (pdoc) from the docstrings of the `brokapi` package.

    python3 docs/pydoc.py <output folder> [<digest.md>]

With a second argument it also writes the reference as one markdown file for AI agents: the signature and the docstring
of every public class, function and method, no styling.

The reference is made from the Python layer only, so the native extension is not needed: a stand-in for
`brokapi._native` is put in its place before pdoc imports the package (the layer touches it only inside functions).
Needs pdoc (`pip install pdoc`), nothing else, and makes no request to the network. The pages use the fonts and the
colors of the book (docs/reference/custom.css, the bar: docs/reference/bar.html), the fonts are copied next to them by docs/build.sh.
"""

import re
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
PACKAGE_SOURCE = HERE.parent / "crates" / "python" / "python"


def stand_in() -> types.ModuleType:
    """A `brokapi._native` with the same names and no behavior. Enough to import the package and read its docstrings."""
    module = types.ModuleType("brokapi._native")
    module.TESTING = True  # type: ignore[attr-defined]
    module.CORE_VERSION = "0"  # type: ignore[attr-defined]
    for name in ("NativeClient", "NativeLimiter", "NativePager", "NativeMock"):
        setattr(module, name, type(name, (), {}))
    return module


def digest(path: Path) -> None:
    """The public API as plain markdown: what an agent that writes code for brokapi needs to see."""
    import dataclasses
    import enum
    import inspect

    import brokapi
    import brokapi.testing

    def doc(obj: object) -> str:
        return inspect.cleandoc(inspect.getdoc(obj) or "").strip()

    def signature(obj: object) -> str:
        try:
            text = str(inspect.signature(obj))  # type: ignore[arg-type]
            # the annotations are strings (from __future__ import annotations): print them without quotes
            return re.sub(r"(: |-> )'([^']*)'", r"\1\2", text)
        except (TypeError, ValueError):
            return "(...)"

    lines = [
        "# brokapi for Python: API reference",
        "",
        "Made from the docstrings of the package (`pip install brokapi`), one file for agents. Money, prices and quantities",
        "are `decimal.Decimal`. Every method of `Client` is a coroutine. The errors are in `brokapi.errors` and also at the",
        "top level of the package.",
        "",
    ]

    def section(title: str, objs: list[tuple[str, object]]) -> None:
        lines.extend([f"## {title}", ""])
        for name, obj in objs:
            if inspect.isclass(obj):
                kind = "enum" if issubclass(obj, enum.Enum) else "exception" if issubclass(obj, BaseException) else "class"
                lines.append(f"### {kind} `{name}`")
                lines.append("")
                if doc(obj):
                    lines.extend([doc(obj), ""])
                if dataclasses.is_dataclass(obj):
                    lines.append("Fields:")
                    lines.append("")
                    for f in dataclasses.fields(obj):
                        lines.append(f"- `{f.name}`: `{f.type}`")
                    lines.append("")
                if issubclass(obj, enum.Enum):
                    lines.append("Members: " + ", ".join(f"`{m.name}`" for m in obj) + "")
                    lines.append("")
                for mname, member in vars(obj).items():
                    target = member.__func__ if isinstance(member, (classmethod, staticmethod)) else member
                    if mname.startswith("_") or not inspect.isfunction(target):
                        continue
                    lines.append(f"#### `{name}.{mname}{signature(target)}`")
                    lines.append("")
                    if doc(target):
                        lines.extend([doc(target), ""])
            elif inspect.isfunction(obj):
                lines.append(f"### `{name}{signature(obj)}`")
                lines.append("")
                if doc(obj):
                    lines.extend([doc(obj), ""])

    top = [(n, getattr(brokapi, n)) for n in ("connect", "merge", "new_cid", "Client", "EventStream", "CandlePager", "Limiter")]
    section("Connecting and working", top)
    section("brokapi.testing: imitations of the brokers", [(n, getattr(brokapi.testing, n)) for n in brokapi.testing.__all__])
    from brokapi import errors, models, numbers

    section("brokapi.errors", [(n, getattr(errors, n)) for n in errors.__all__])
    section("brokapi.numbers", [(n, getattr(numbers, n)) for n in numbers.__all__])
    section("brokapi.models: data", [(n, getattr(models, n)) for n in models.__all__ if inspect.isclass(getattr(models, n))])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print(__doc__)
        return 2
    out = Path(sys.argv[1])
    sys.path.insert(0, str(PACKAGE_SOURCE))
    sys.modules["brokapi._native"] = stand_in()

    import pdoc
    import pdoc.render

    pdoc.render.configure(
        docformat="restructuredtext",
        template_directory=HERE / "reference",
        footer_text="brokapi by RunInfra",
        search=True,
        show_source=False,
    )
    pdoc.pdoc("brokapi", output_directory=out)
    if len(sys.argv) == 3:
        digest(Path(sys.argv[2]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
