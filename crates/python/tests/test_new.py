"""``python -m brokapi new``: the project it makes works as it is, for every broker.

Each project is made in a temporary folder, then its own tests and its demo on the imitation run there in a new process,
as a person (or their agent) runs them.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from brokapi import new

BROKERS = sorted(new.BROKERS)


def run(folder: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, *args], cwd=folder, capture_output=True, text=True, timeout=120)


@pytest.mark.parametrize("broker", BROKERS)
def test_the_project_of_each_broker_passes_its_own_tests_and_runs_its_demo(broker: str, tmp_path: Path) -> None:
    folder = tmp_path / "bot"
    made = run(tmp_path, "-m", "brokapi", "new", "bot", "--broker", broker)
    assert made.returncode == 0, made.stderr
    names = sorted(p.name for p in folder.iterdir())
    assert names == sorted([".env.example", ".gitignore", "AGENTS.md", "README.md", "bot.py", "pyproject.toml", "test_bot.py"])
    text = "".join(p.read_text(encoding="utf-8") for p in folder.iterdir())
    assert "__" + "BROKER__" not in text and "__" + "MOCK__" not in text, "every mark is filled"
    # The money is safe by default: the read-only mode and a cap on one order.
    env = (folder / ".env.example").read_text(encoding="utf-8")
    assert "BROKAPI_READ_ONLY=1" in env and "BROKAPI_MAX_ORDER_VALUE=" in env
    assert ".env" in (folder / ".gitignore").read_text(encoding="utf-8").splitlines()

    tests = run(folder, "-m", "pytest", "-q", "-p", "no:cacheprovider")
    assert tests.returncode == 0, tests.stdout + tests.stderr
    assert "2 passed" in tests.stdout
    demo = run(folder, "bot.py", "--mock")
    assert demo.returncode == 0, demo.stdout + demo.stderr
    assert "done: the imitation showed the signal, the buy, the fill and the stop" in demo.stdout
    assert not list(folder.glob("journal*.jsonl")), "the demo leaves no journal behind"


def test_a_folder_that_is_not_empty_is_left_alone(tmp_path: Path) -> None:
    (tmp_path / "mine.txt").write_text("mine")
    with pytest.raises(FileExistsError, match="nothing was written"):
        new.create(tmp_path, "tbank")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["mine.txt"]
    made = run(tmp_path, "-m", "brokapi", "new", ".", "--broker", "nope")
    assert made.returncode == 2 and "invalid choice" in made.stderr
