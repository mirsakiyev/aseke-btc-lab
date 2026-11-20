from contextlib import closing
from datetime import timedelta
import json
import sqlite3
import sys

import psutil
import pytest

from lab.config import fingerprint
from lab.paper_run import reload
from lab.phase15 import paper
from lab.storage import utc, write_json, writer_lock
from test_paper_capabilities_membership import seven, eighth


NOW = utc("2026-11-16T20:00:00Z")
REGISTERED_LANES = seven()
for number, value in enumerate(REGISTERED_LANES.values()):
    value['book_id'] = str(number)
RETIREMENT_BODY = {'fixture': 'reviewed retirement', 'slots': {slot: {'book_id': lane['book_id']}
                                                           for slot, lane in REGISTERED_LANES.items()}}
REGISTRATION_BODY = {'protocol': 'fresh-reference-paper-v2', 'archive_id': fingerprint(RETIREMENT_BODY),
                     'start': (NOW-timedelta(days=1)).isoformat(), 'lanes': REGISTERED_LANES}
GENERATION = fingerprint(REGISTRATION_BODY)
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
    write_json(root / 'current.json', {'id': GENERATION})
    write_json(root / 'registrations' / f'{GENERATION}.json', {'id': GENERATION, 'body': REGISTRATION_BODY})
    write_json(root / 'legacy-retired.json', {'id': fingerprint(RETIREMENT_BODY), 'body': RETIREMENT_BODY})
    launch = NOW - timedelta(minutes=10)
    shim = Process(12, launch, tmp_path)
    child = Process(15, launch + timedelta(milliseconds=50), tmp_path, parent=shim)
    write_json(root / "last-launch.json", {"pid": shim.pid, "at": launch.isoformat()})
    write_json(root / "last-cycle.json", {
        "pid": child.pid, "state": "RUNNING", "generation": GENERATION, "at": NOW.isoformat(),
    })
    with closing(sqlite3.connect(root / "paper.sqlite3")) as db, db:
        db.execute("CREATE TABLE book(id TEXT PRIMARY KEY, state TEXT)")
        db.execute('CREATE TABLE event(key TEXT PRIMARY KEY,book TEXT,kind TEXT,idea TEXT,source_at TEXT,recorded_at TEXT,body TEXT)')
        db.executemany("INSERT INTO book VALUES(?,?)",
                       [(value['book_id'], json.dumps(paper.initial(value))) for value in REGISTERED_LANES.values()])
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
        state = json.loads(db.execute("SELECT state FROM book WHERE id='0'").fetchone()[0])
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


def test_registered_eighth_flat_book_is_verified_without_weakening_identity_checks(runner):
    from lab.paper_run import membership

    root, _, child = runner
    record = {'id': GENERATION, 'body': REGISTRATION_BODY}
    request = eighth(record)
    with closing(sqlite3.connect(root/'paper.sqlite3')) as db, db:
        db.execute('BEGIN IMMEDIATE')
        # No source amendment lookup is required for the actual sealed identity
        # guard; the isolated setup has the same already approved WS scope.
        original_active = membership.source_amendment.active
        membership.source_amendment.active = lambda _: None
        try:
            membership.register(db, record, request, at=NOW, approved=True)
        finally:
            membership.source_amendment.active = original_active
    assert reload._flat(root, generation_id=GENERATION)
    assert _reload(runner)['state'] == 'RUNNER_RELOADED'
    assert child.terminated


def test_an_unregistered_extra_book_is_not_mistaken_for_future_membership(runner):
    root, _, child = runner
    with closing(sqlite3.connect(root/'paper.sqlite3')) as db, db:
        db.execute('INSERT INTO book VALUES(?,?)', ('rogue', json.dumps({'book_id':'rogue','position':None,'pending':None})))
    with pytest.raises(ValueError, match='REGISTERED_MEMBERSHIP_MISMATCH'):
        reload._flat(root, generation_id=GENERATION)
    assert _reload(runner)['reason'] == 'CURRENT_RUNNER_RELOAD_EVIDENCE_UNAVAILABLE'
    assert not child.terminated


def test_seven_rows_with_the_wrong_registered_identity_are_also_refused(runner):
    root, _, child = runner
    with closing(sqlite3.connect(root/'paper.sqlite3')) as db, db:
        db.execute("DELETE FROM book WHERE id='0'")
        db.execute('INSERT INTO book VALUES(?,?)', ('wrong', json.dumps({'book_id':'wrong','position':None,'pending':None})))
    assert _reload(runner)['reason'] == 'CURRENT_RUNNER_RELOAD_EVIDENCE_UNAVAILABLE'
    assert not child.terminated


def test_passed_root_seals_are_checked_instead_of_global_current_state(runner, tmp_path, monkeypatch):
    from lab.paper_run import gate

    root, _, child = runner
    monkeypatch.setattr(gate, 'ROOT', tmp_path/'different-global-root')
    assert reload._flat(root, generation_id=GENERATION)
    changed = dict(REGISTRATION_BODY)
    changed['archive_id'] = 'forged'
    write_json(root/'registrations'/f'{GENERATION}.json', {'id':GENERATION,'body':changed})
    assert _reload(runner)['reason'] == 'CURRENT_RUNNER_RELOAD_EVIDENCE_UNAVAILABLE'
    assert not child.terminated


def test_running_owner_exits_after_its_generation_stop_and_watchdog_does_not_relaunch(runner, monkeypatch):
    from lab.paper_run import gate, runtime

    root, _, _ = runner
    monkeypatch.setattr(gate, "ROOT", root)
    cycles = []

    def stop_requested(generation):
        assert generation == GENERATION
        return bool(cycles)

    def cycle(**_):
        cycles.append("committed")
        return {}

    monkeypatch.setattr(gate, "stop_requested", stop_requested)
    monkeypatch.setattr(runtime, "cycle", cycle)
    monkeypatch.setattr(runtime.time, "sleep", lambda _: None)
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **k: pytest.fail("Stopped generation relaunched"))

    runtime.run()
    assert cycles == ["committed"]
    with writer_lock(root / ".runner.lock"):
        pass
    assert runtime.ensure_runner() == {"state": "RUNNER_STOP_REQUESTED", "generation": GENERATION}


def test_watchdog_rechecks_generation_under_launch_lease(runner, monkeypatch):
    from lab.paper_run import gate, runtime

    root, _, _ = runner
    monkeypatch.setattr(gate, "ROOT", root)
    generations = iter(({"id": GENERATION}, {"id": "c" * 64}))
    monkeypatch.setattr(runtime, "current", lambda: next(generations))
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **k: pytest.fail("Old launch was superseded"))

    assert runtime.ensure_runner() == {"state": "GENERATION_CHANGED"}
