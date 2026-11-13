import json
import subprocess
import sys

import pytest

from lab.phase10 import repair_guard
from lab.phase15 import restart_preflight


@pytest.fixture
def repair_project(tmp_path, monkeypatch):
    for relative in (
        repair_guard.CONTROL, repair_guard.WORKER, repair_guard.ACTIVITY,
        *repair_guard.WRITE_LOCKS, "runs/phase13/observations/fixture/.capture.lock",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    (tmp_path / repair_guard.CONTROL).write_text(
        json.dumps({"paused": True, "monitor": True, "ideas": {"day": True}}), encoding="utf-8",
    )
    runtime = tmp_path / "data/phase10/runtime.json"
    runtime.write_text(json.dumps({"drain_guard_version": 1, "launch_id": "a" * 32}), encoding="utf-8")
    monkeypatch.setattr(restart_preflight, "verify", lambda root: None)
    return tmp_path


def _competing_lock(root, relative):
    source = """
import msvcrt
import sys
from pathlib import Path
with Path(sys.argv[1]).open('a+b') as handle:
    handle.seek(0)
    try:
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        raise SystemExit(2)
    print('LOCKED', flush=True)
    if len(sys.argv) > 2:
        sys.stdin.readline()
"""
    return [sys.executable, "-c", source, str(root / relative)]


@pytest.mark.parametrize("relative", [
    repair_guard.WORKER,
    "runs/phase10/.observer-cycle.lock",
    "runs/phase15/.writer.lock",
    "runs/phase13/observations/fixture/.capture.lock",
])
def test_quiescence_refuses_existing_writer_and_releases_its_partial_leases(repair_project, relative):
    process = subprocess.Popen(
        [*_competing_lock(repair_project, relative), "hold"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    try:
        assert process.stdout.readline().strip() == "LOCKED"
        with pytest.raises(repair_guard.Busy), repair_guard.quiescence(repair_project):
            pytest.fail("Quiescence must never admit an active writer")
        with repair_guard._lease(repair_project / repair_guard.CONTROL_LOCK):
            pass
    finally:
        process.communicate("\n", timeout=20)
    assert process.returncode == 0


def test_live_worker_requires_its_activity_guard(repair_project):
    (repair_project / "data/phase10/runtime.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="no verified activity guard"), repair_guard.quiescence(
        repair_project, allow_worker=True,
    ):
        pytest.fail("Legacy worker has no protected write boundary")


def test_quiescence_holds_journal_lease_for_its_entire_context(repair_project):
    command = _competing_lock(repair_project, "runs/phase15/.writer.lock")
    with repair_guard.quiescence(repair_project):
        blocked = subprocess.run(command, capture_output=True, text=True, timeout=20)
        assert blocked.returncode == 2
    released = subprocess.run(command, capture_output=True, text=True, timeout=20)
    assert released.returncode == 0
    assert released.stdout.strip() == "LOCKED"


def test_repair_pause_is_explicit_idempotent_and_preserves_controls(repair_project):
    repair_guard._state(repair_project, "pause", "Drain old launch")
    repair_guard._state(repair_project, "block", "Writer active")
    status = repair_guard._state(repair_project, "status")
    assert status["paused"] is True and status["monitor"] is True and status["worker_lock_held"] is False
    state = json.loads((repair_project / repair_guard.REPAIR).read_text(encoding="utf-8"))
    assert state == status["repair"]
    assert state["state"] == "BLOCKED" and state["reason"] == "Writer active" and state["launch_id"] == "a" * 32
    repair_guard._state(repair_project, "resume", "Prior ownership released")
    repair_guard._state(repair_project, "resume", "Prior ownership released")
    saved = json.loads((repair_project / repair_guard.CONTROL).read_text(encoding="utf-8"))
    assert saved == {"paused": False, "monitor": True, "ideas": {"day": True}}
