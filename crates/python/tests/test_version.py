"""The version of the package is the one of Cargo.toml: it is not written in the Python code."""

from __future__ import annotations

import re
from pathlib import Path

import brokapi

MANIFEST = Path(__file__).resolve().parents[1] / "Cargo.toml"


def pep440(version: str) -> str:
    """0.1.0-alpha.3 -> 0.1.0a3 (the form maturin writes into the metadata)."""
    m = re.fullmatch(r"(\d+\.\d+\.\d+)(?:-(alpha|beta|rc)\.(\d+))?", version)
    assert m, version
    base, kind, number = m.groups()
    return base if kind is None else base + {"alpha": "a", "beta": "b", "rc": "rc"}[kind] + number


def test_the_version_comes_from_the_manifest() -> None:
    declared = re.search(r'^version\s*=\s*"([^"]+)"', MANIFEST.read_text(encoding="utf-8"), re.M)
    assert declared is not None
    assert brokapi.__version__ == pep440(declared.group(1))
