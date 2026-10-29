"""The native runtime must stop before CPython starts destroying the interpreter."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="observes native threads through /proc")
def test_native_threads_end_before_interpreter_finalization(tmp_path: Path) -> None:
    report = tmp_path / "threads.json"
    child = subprocess.run(
        [sys.executable, "-c", r'''
import atexit
import asyncio
import json
from pathlib import Path
import sys

report = Path(sys.argv[1])
baseline = {p.name for p in Path("/proc/self/task").iterdir()}
started = set()

# Registered before importing the extension: its shutdown must run before this audit.
def audit():
    remaining = []
    details = {}
    for tid in sorted(started):
        try:
            fields = Path("/proc/self/task", tid, "stat").read_text().rsplit(")", 1)[1].split()
        except FileNotFoundError:
            continue
        flags = int(fields[6])  # stat field 9, after pid and comm
        details[tid] = {"state": fields[0], "flags": flags}
        # Linux can still list a joined thread during kernel exit (PF_EXITING,
        # include/linux/sched.h). It has left user space and cannot call Python.
        # A parked worker of an unclosed runtime does not have this flag.
        if not flags & 0x00000004:
            remaining.append(tid)
    report.write_text(json.dumps({"started": sorted(started), "remaining": remaining, "details": details}))

atexit.register(audit)
from brokapi import testing

async def main():
    async with await testing.mock_finam() as mock:
        client = await mock.connect()
        await client.subscribe_order_state()
        # Cancel a pending read, as a bot does when its main loop finishes.
        pending = client._native.next_event()
        pending.cancel()
        started.update(p.name for p in Path("/proc/self/task").iterdir() if p.name not in baseline)

asyncio.run(main())
''', str(report)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.returncode == 0, child.stdout + child.stderr
    threads = json.loads(report.read_text())
    assert threads["started"], "the scenario must actually start native background threads"
    assert threads["remaining"] == [], threads


@pytest.mark.parametrize("started", [False, True])
def test_shutdown_is_idempotent_and_late_calls_cannot_restart_it(started: bool, tmp_path: Path) -> None:
    child = subprocess.run(
        [sys.executable, "-c", r'''
import asyncio
from pathlib import Path
import sys
from brokapi import _native, testing

async def main():
    path = str(Path(sys.argv[2]) / "empty.jsonl")
    _native.NativeLogWriter(path).close()
    mock = client = None
    if sys.argv[1] == "True":
        mock = await testing.mock_finam()
        client = await mock.connect()
        await client.subscribe_order_state()
        pending = client._native.next_event()
        pending.cancel()

    _native._shutdown()
    _native._shutdown()

    async def cannot_restart():
        await testing.mock_finam()

    try:
        await cannot_restart()
    except RuntimeError as error:
        assert "runtime is shut down" in str(error), str(error)
    else:
        raise AssertionError("a late connection restarted the runtime")

    late_calls = [lambda: _native.NativeReplay(path)]
    if client is not None:
        late_calls.extend([client._native.next_event, client._native.close])
    for call in late_calls:
        try:
            call()
        except RuntimeError as error:
            assert "runtime is shut down" in str(error), str(error)
        else:
            raise AssertionError("a late native operation restarted the runtime")

asyncio.run(main())
print("shutdown completed and late operations were rejected")
''', str(started), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.returncode == 0, child.stdout + child.stderr
    assert "shutdown completed and late operations were rejected" in child.stdout
