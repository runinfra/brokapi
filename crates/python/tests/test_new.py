"""``python -m brokapi new``: the project it makes works as it is, for every broker.

Each project is made in a temporary folder, then its own tests and its demo on the imitation run there in a new process,
as a person (or their agent) runs them.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from brokapi import new

BROKERS = sorted(new.BROKERS)


def run(folder: Path, *args: str, timeout: float = 120, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, *args], cwd=folder, capture_output=True, text=True, timeout=timeout, env=env)


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
    (folder / ".env").write_text(env, encoding="utf-8")

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


def test_the_demo_works_after_copying_dotenv_and_with_real_limits_inherited(tmp_path: Path) -> None:
    folder = tmp_path / "bot"
    new.create(folder, "tbank")
    dotenv = (folder / ".env.example").read_bytes()
    (folder / ".env").write_bytes(dotenv)
    env = {**os.environ, "BROKAPI_READ_ONLY": "1", "BROKAPI_MAX_ORDER_QTY": "0.001", "BROKAPI_MAX_ORDER_VALUE": "0.01"}
    demo = run(folder, "-u", "bot.py", "--mock", timeout=10, env=env)
    assert demo.returncode == 0, demo.stdout + demo.stderr
    assert "done: the imitation showed the signal, the buy, the fill and the stop" in demo.stdout
    assert "would buy" not in demo.stdout
    assert (folder / ".env").read_bytes() == dotenv, "the demo must not relax the real account's settings"
    assert not list(folder.glob("journal*.jsonl"))


@pytest.mark.parametrize("args", [("--help",), ("--mock", "--help"), ("--unknown",), ("--mock", "--unknown")])
def test_help_and_bad_arguments_do_not_start_the_bot(args: tuple[str, ...], tmp_path: Path) -> None:
    folder = tmp_path / "bot"
    new.create(folder, "tbank")
    result = run(folder, "bot.py", *args, timeout=5)
    if "--help" in args:
        assert result.returncode == 0, result.stdout + result.stderr
        assert "--mock" in result.stdout and "usage:" in result.stdout
    else:
        assert result.returncode == 2, result.stdout + result.stderr
        assert "unrecognized arguments: --unknown" in result.stderr
    assert "done: the imitation" not in result.stdout
    assert not list(folder.glob("journal*.jsonl")), "argument parsing must finish before any bot is started"


def test_a_strategy_error_ends_the_demo_with_its_cause(tmp_path: Path) -> None:
    folder = tmp_path / "bot"
    new.create(folder, "tbank")
    bot = folder / "bot.py"
    bot.write_text(bot.read_text().replace(
        'if __name__ == "__main__":',
        'def wants_to_buy(prices):\n    raise RuntimeError("the strategy failed")\n\nif __name__ == "__main__":',
    ))
    result = run(folder, "bot.py", "--mock", timeout=5)
    assert result.returncode != 0
    assert "RuntimeError: the strategy failed" in result.stderr
    assert "done: the imitation" not in result.stdout


def test_the_demo_can_import_a_strategy_next_to_the_bot(tmp_path: Path) -> None:
    folder = tmp_path / "bot"
    new.create(folder, "tbank")
    (folder / "strategy.py").write_text(
        'def wants_to_buy(prices):\n    print("strategy from sibling")\n    return prices[-1] > min(prices)\n'
    )
    bot = folder / "bot.py"
    bot.write_text(bot.read_text().replace(
        'if __name__ == "__main__":',
        'from strategy import wants_to_buy\n\nif __name__ == "__main__":',
    ))
    demo = run(folder, "bot.py", "--mock", timeout=10)
    assert demo.returncode == 0, demo.stdout + demo.stderr
    assert "strategy from sibling" in demo.stdout
    assert "done: the imitation showed the signal, the buy, the fill and the stop" in demo.stdout
