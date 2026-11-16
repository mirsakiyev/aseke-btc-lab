from contextlib import closing
from datetime import timedelta
import json
import sqlite3
import sys

import psutil
import pytest

from lab.paper_run import reload
from lab.storage import utc, write_json, writer_lock


NOW = utc("2026-11-16T20:00:00Z")
GENERATION = "a" * 64
AMENDMENT = "b" * 64


class Process:
    def __init__(self, pid, started, project, parent=None, username="test"):
        self.pid = pid
        self.started = started
        self.project = project
        self.parent_process = parent
        self.user = username
        self.arguments = ["python.exe", *reload.ARGS]
        self.terminated = False
        self.waited = False

    def create_time(self):
        return self.started.timestamp()

    def username(self):
        return self.user

    def exe(self):
        return sys._base_executable

    def cwd(self):
        return self.project

    def cmdline(self):
        return self.arguments

    def parent(self):
        return self.parent_process

    def terminate(self):
        self.terminated = True

    def wait(self, timeout):
        assert timeout == 5 and self.terminated
        self.waited = True


@pytest.fixture
def runner(tmp_path):
    root = tmp_path / "paper-run"
    root.mkdir()
    launch = NOW - timedelta(minutes=10)
    shim = Process(12, launch, tmp_path)
    child = Process(15, launch + timedelta(milliseconds=50), tmp_path, parent=shim)
    write_json(root / "last-launch.json", {"pid": shim.pid, "at": launch.isoformat()})
    write_json(root / "last-cycle.json", {
        "pid": child.pid, "state": "RUNNING", "generation": GENERATION, "at": NOW.isoformat(),
    })
    with closing(sqlite3.connect(root / "paper.sqlite3")) as db, db:
        db.execute("CREATE TABLE book(id TEXT PRIMARY KEY, state TEXT)")
        db.executemany("INSERT INTO book VALUES(?,?)",
                       [(str(i), json.dumps({"position": None, "pending": None})) for i in range(7)])
    return root, tmp_path, child


def _reload(runner, *, lookup=None, amendment=AMENDMENT):
    root, project, child = runner
    return reload.if_needed(root, GENERATION, amendment, project=project, clock=lambda: NOW,
                            lookup=lookup or (lambda pid=None: child))


def test_flat_verified_current_runner_reloads_only_actual_python_child(runner):
    _, _, child = runner
    original_parent = child.parent()
    result = _reload(runner)
    assert result == {"state": "RUNNER_RELOADED", "retired_pid": child.pid,
                      "source_amendment_id": AMENDMENT}
    assert child.terminated and child.waited
    assert not original_parent.terminated


@pytest.mark.parametrize("field", ["position", "pending"])
def test_keeps_existing_protection_and_pending_intent(runner, field):
    root, _, child = runner
    with closing(sqlite3.connect(root / "paper.sqlite3")) as db, db:
        state = {"position": None, "pending": None}
        state[field] = {"id": "current"}
        db.execute("UPDATE book SET state=? WHERE id='0'", (json.dumps(state),))
    assert _reload(runner)["reason"] == "CURRENT_BOOK_HAS_EXPOSURE_OR_PENDING_IDEA"
    assert not child.terminated


def test_matching_amendment_is_a_noop(runner):
    root, _, child = runner
    write_json(root / "last-cycle.json", {"source_amendment_id": AMENDMENT})
    assert _reload(runner, lookup=lambda *_: pytest.fail("Unnecessary process lookup")) is None
    assert not child.terminated


def test_flat_runner_with_same_amendment_and_old_build_can_recover_a_failed_cycle(runner):
    root, project, child = runner
    write_json(root / "last-cycle.json", {"pid": child.pid, "state": "RUNNING", "generation": GENERATION,
        "source_amendment_id": AMENDMENT, "runner_build_id": "old", "at": (NOW - timedelta(minutes=3)).isoformat()})
    result = reload.if_needed(root, GENERATION, AMENDMENT, project=project, clock=lambda: NOW,
                             lookup=lambda *_: child, expected_build="new")
    assert result["state"] == "RUNNER_RELOADED"
    assert child.terminated and child.waited


def test_windows_access_denied_fails_without_termination(runner):
    _, _, child = runner

    def inaccessible(pid=None):
        raise psutil.AccessDenied(pid)

    assert _reload(runner, lookup=inaccessible)["reason"] == "CURRENT_RUNNER_WINDOWS_ACCESS_DENIED"
    assert not child.terminated


def test_wrong_parent_or_runner_command_is_never_terminated(runner):
    _, _, child = runner
    parent = child.parent()
    child.parent_process = Process(18, parent.started, parent.project)
    assert _reload(runner)["reason"] == "CURRENT_RUNNER_ANCESTRY_MISMATCH"
    child.parent_process = parent
    child.arguments = ["python.exe", "-m", "lab.phase10", "worker"]
    assert _reload(runner)["reason"] == "CURRENT_RUNNER_OWNERSHIP_MISMATCH"
    assert not child.terminated


def test_queued_repeat_cannot_observe_account_while_writer_is_busy(runner):
    root, _, child = runner
    with writer_lock(root / ".writer.lock"):
        assert _reload(runner)["reason"] == "CURRENT_RUNNER_WRITER_BUSY"
    assert not child.terminated
