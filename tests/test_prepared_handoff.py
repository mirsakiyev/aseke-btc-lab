from contextlib import closing
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys

import pandas as pd
import pytest

from lab.paper_run import gate, reload, runtime, source_amendment
from lab import storage


HELPER = (Path(os.environ.get("LOCALAPPDATA", "")) / "ASEKE-BTC-Lab/protection-audit"
          / "deployment-helper-20261117T181648785Z/guarded_handoff.py")
GENERATION, BUILD = "a" * 64, "b" * 64
NOW = storage.utc("2026-11-17T18:00:00Z")


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    if not HELPER.is_file():
        pytest.skip("The private reviewed operator handoff artifact is not installed on this workstation")
    spec = importlib.util.spec_from_file_location("prepared_guarded_handoff", HELPER)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setattr(runtime, "PROJECT", project)
    monkeypatch.setattr(runtime, "RUNNER_BUILD", BUILD)
    monkeypatch.setattr(gate, "ROOT", project / "paper-run")
    monkeypatch.setattr(storage, "utc_now", lambda: NOW)
    cli = project / "lab/paper_run/__main__.py"
    cli.parent.mkdir(parents=True)
    cli.write_bytes(b"reviewed operator CLI")
    cli_sha = sha256(cli.read_bytes()).hexdigest()
    record = {"id": GENERATION, "body": {"lanes": {str(n): {"book_id": str(n)} for n in range(7)}}}
    monkeypatch.setattr(runtime, "current", lambda: record)
    monkeypatch.setattr(source_amendment, "active", lambda _: {"id": "amendment"})
    storage.write_json(gate.ROOT / "entries-paused", {
        "generation": GENERATION, "paused_at": (NOW - pd.Timedelta(minutes=1)).isoformat()})
    storage.write_json(gate.ROOT / "last-cycle.json", {
        "state": "RUNNING", "generation": GENERATION, "at": NOW.isoformat(), "pid": 42,
        "runner_build_id": "old", "source_amendment_id": "amendment",
    })
    with closing(sqlite3.connect(gate.ROOT / "paper.sqlite3")) as db, db:
        db.executescript(runtime.SQL)
        for n in range(7):
            book = {"book_id": str(n), "position": None, "pending": None,
                    "problem": "UNRESOLVED_PROTECTION_INTERVAL" if n == 0 else None,
                    "cash": 10010. if n == 0 else 10000., "equity": 10010. if n == 0 else 10000.,
                    "starting_equity": 10000., "gross": 12. if n == 0 else 0., "fees": 2. if n == 0 else 0.,
                    "funding": 0., "liquidation_cost": 0., "unfunded_deficit": 0.}
            db.execute("INSERT INTO book VALUES(?,?)", (str(n), json.dumps(book)))
        closed = {"net": 10., "gross_price_pnl": 12., "fees": 2., "funding": 0.,
                  "liquidation_cost": 0., "unfunded_deficit": 0., "provisional": True}
        for kind, minutes, body in (("HYPOTHETICAL_FILL", 5, {}), ("EXIT_OBSERVED", 4, {}), ("CLOSED", 4, closed)):
            at = (NOW - pd.Timedelta(minutes=minutes)).isoformat()
            db.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)", (kind, "0", kind, "position", at, at, json.dumps(body)))
    return helper, cli_sha


def mutate_book(identifier, **changes):
    with closing(sqlite3.connect(gate.ROOT / "paper.sqlite3")) as db, db:
        book = json.loads(db.execute("SELECT state FROM book WHERE id=?", (identifier,)).fetchone()[0])
        book.update(changes)
        db.execute("UPDATE book SET state=? WHERE id=?", (json.dumps(book), identifier))


def test_default_handoff_check_is_read_only_and_accepts_provisional_history(prepared, monkeypatch):
    helper, cli_sha = prepared
    monkeypatch.setattr(reload, "if_needed", lambda *_args, **_kwargs: pytest.fail("Read-only check retired a runner"))
    before = (gate.ROOT / "paper.sqlite3").read_bytes()
    report = helper.check(GENERATION, BUILD, cli_sha)
    assert report["state"] == "READY_FOR_OPERATOR_REVIEW"
    assert report["books"]["0"]["historical_problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    assert report["running_pid"] == 42
    assert (gate.ROOT / "paper.sqlite3").read_bytes() == before


@pytest.mark.parametrize("field", ["position", "pending"])
def test_handoff_refuses_any_open_or_pending_book(prepared, monkeypatch, field):
    helper, cli_sha = prepared
    mutate_book("3", **{field: {"id": "exposure"}})
    monkeypatch.setattr(reload, "if_needed", lambda *_args, **_kwargs: pytest.fail("Unsafe retirement"))
    with pytest.raises(ValueError, match="open exposure or a pending"):
        helper.check(GENERATION, BUILD, cli_sha, handoff=True)


def test_handoff_refuses_unknown_execution(prepared):
    helper, cli_sha = prepared
    with closing(sqlite3.connect(gate.ROOT / "paper.sqlite3")) as db, db:
        db.execute("DELETE FROM event WHERE kind='CLOSED'")
    with pytest.raises(ValueError, match="incomplete"):
        helper.check(GENERATION, BUILD, cli_sha)


@pytest.mark.parametrize("position", ["unknown-position", "position"])
def test_handoff_refuses_orphaned_or_unreconciled_funding(prepared, position):
    helper, cli_sha = prepared
    at = (NOW - pd.Timedelta(minutes=4)).isoformat()
    with closing(sqlite3.connect(gate.ROOT / "paper.sqlite3")) as db, db:
        db.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)", (
            "funding", "0", "FUNDING", position, at, at, json.dumps({"cash_flow": 1.})))
    with pytest.raises(ValueError, match="Funding"):
        helper.check(GENERATION, BUILD, cli_sha)


@pytest.mark.parametrize("changes", [{"cash": float("nan")}, {"fees": 3.}, {"cash": 10009., "equity": 10009.}])
def test_handoff_refuses_invalid_or_unreconciled_financial_state(prepared, changes):
    helper, cli_sha = prepared
    mutate_book("0", **changes)
    with pytest.raises(ValueError, match="financial|accounting"):
        helper.check(GENERATION, BUILD, cli_sha)


@pytest.mark.parametrize("generation,build,cli", [("different", BUILD, None), (GENERATION, "different", None),
                                                 (GENERATION, BUILD, "0" * 64)])
def test_handoff_refuses_generation_or_source_drift(prepared, generation, build, cli):
    helper, cli_sha = prepared
    with pytest.raises(ValueError, match="differs"):
        helper.check(generation, build, cli or cli_sha)


def test_guarded_retirement_holds_launch_fence_and_uses_existing_owner_checks(prepared, monkeypatch):
    helper, cli_sha = prepared
    calls = []

    def guarded(root, generation, amendment, **kwargs):
        calls.append((root, generation, amendment, kwargs))
        with pytest.raises(RuntimeError):
            with storage.writer_lock(gate.ROOT / ".launch.lock"):
                pass
        # The existing reload helper takes and repeats checks under this lease itself.
        with storage.writer_lock(gate.ROOT / ".writer.lock"):
            pass
        return {"state": "RUNNER_RELOAD_BLOCKED", "reason": "CURRENT_RUNNER_WINDOWS_ACCESS_DENIED"}

    monkeypatch.setattr(reload, "if_needed", guarded)
    monkeypatch.setattr(runtime, "ensure_runner", lambda: pytest.fail("Interactive runner launch"))
    backup = runtime.PROJECT.parent / "handoff-backup"
    result = helper.check(GENERATION, BUILD, cli_sha, handoff=True, backup_dir=backup)
    assert result["state"] == "RUNNER_RELOAD_BLOCKED"
    manifest = json.loads(Path(result["backup"]["manifest"]).read_text())
    assert manifest["journal_watermark"]["event_count"] == 3
    assert manifest["journal_watermark"]["max_rowid"] == 3
    assert manifest["generation"] == GENERATION
    assert sha256((backup / "paper.sqlite3").read_bytes()).hexdigest() == manifest["database_sha256"]
    assert calls[0][1:3] == (GENERATION, "amendment")
    assert calls[0][3]["expected_build"] == BUILD
    with storage.writer_lock(gate.ROOT / ".launch.lock"):
        pass


def test_backup_failure_aborts_before_retirement(prepared, monkeypatch):
    helper, cli_sha = prepared
    monkeypatch.setattr(reload, "if_needed", lambda *_args, **_kwargs: pytest.fail("Retirement without backup"))
    with pytest.raises(ValueError, match="outside"):
        helper.check(GENERATION, BUILD, cli_sha, handoff=True, backup_dir=runtime.PROJECT / "forbidden-backup")


def test_unpaused_or_busy_handoff_is_blocked(prepared):
    helper, cli_sha = prepared
    with storage.writer_lock(gate.ROOT / ".writer.lock"), pytest.raises(RuntimeError):
        helper.check(GENERATION, BUILD, cli_sha)
    (gate.ROOT / "entries-paused").unlink()
    with pytest.raises(ValueError, match="pause"):
        helper.check(GENERATION, BUILD, cli_sha)


def test_helper_cli_reports_corrupt_database_as_blocked_json(prepared, monkeypatch, capsys):
    helper, cli_sha = prepared
    (runtime.PROJECT / "lab/paper_run/runtime.py").write_text("reviewed runner")
    executable = runtime.PROJECT / ".venv/Scripts/python.exe"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"fixture")

    def corrupt(**_):
        raise sqlite3.DatabaseError("database disk image is malformed")

    monkeypatch.setattr(runtime, "connect", corrupt)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(sys, "argv", ["guarded_handoff.py", "--check", "--expected-generation", GENERATION,
                                     "--expected-build", BUILD, "--expected-cli-sha", cli_sha])
    with pytest.raises(SystemExit) as stopped:
        helper.main()
    assert stopped.value.code == 1
    output = json.loads(capsys.readouterr().out)
    assert output["state"] == "HANDOFF_BLOCKED" and not output["mutation_requested"]
    assert "malformed" in output["reason"]
