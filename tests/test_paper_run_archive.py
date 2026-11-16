from contextlib import closing
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
import sqlite3

import pytest

from lab.config import fingerprint
from lab.ema_paper import catalog as ema
from lab.health import fresh_paper, state as health
from lab.paper_run import __main__ as cli, archive, gate
from lab.storage import utc, write_json


NOW = utc("2026-11-16T19:00:00Z")


@pytest.fixture
def legacy(tmp_path, monkeypatch):
    project, parent = tmp_path / "worktree", tmp_path / "private"
    monkeypatch.setattr(archive, "_git", lambda _, action, *args: "f" * 40 if action == "rev-parse" else "")
    monkeypatch.setattr(archive, "_reference_check", lambda *args: [])
    phase = {"lanes": {slot: {"book_id": f"b/{slot}", "risk": {"starting_equity": 10000}} for slot in archive.BREAKOUT}}
    phase_record = {"id": fingerprint(phase), "body": phase}
    write_json(project / "runs/phase15/registration.json", {"id": phase_record["id"]})
    write_json(project / f"runs/phase15/registrations/{phase_record['id']}.json", phase_record)
    phase_db = sqlite3.connect(project / archive.PHASE15_DATABASE)
    phase_db.execute("PRAGMA journal_mode=WAL")
    phase_db.execute("PRAGMA wal_autocheckpoint=0")
    phase_db.executescript(ema.SCHEMA)
    raw = b'{"trade": "legacy"}'
    digest = sha256(raw).hexdigest()
    raw_path = project / f"runs/phase15/spot/raw/{digest}.json"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(raw)
    cleanup_path = project / "runs/phase15/first-managed.json"
    cleanup_path.write_bytes(b"legacy report")
    protected = project / "EMA_PAPER_SOURCE_APPROVAL_DRAFT.json"
    protected.write_bytes(b"shared source review")
    with phase_db:
        for key, lane in phase["lanes"].items():
            position = {"id": "open-short"} if key == "swing/fixed" else None
            pending = {"id": "wait-long"} if key == "day/fixed" else None
            _account(phase_db, lane["book_id"], position, pending)
        phase_db.execute("INSERT INTO event VALUES (?,?,?,?,?,?,?)",
                         ("legacy-event", "b/day/fixed", "PUBLISHED", "wait-long", NOW.isoformat(),
                          NOW.isoformat(), f'{{"raw_sha256": "{digest}"}}'))
    ema_path = project / archive.EMA_DATABASE
    ema_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(ema_path)) as db, db:
        db.executescript(ema.SCHEMA)
        for profile in archive.EMA:
            record = {"profile": profile, "book_id": f"ema/{profile}"}
            db.execute("INSERT INTO registration VALUES (?,?,?)", (profile, fingerprint(record), ema.paper.encode(record)))
            _account(db, record["book_id"], None, None)
    health_path = project / archive.HEALTH_DATABASE
    health_path.parent.mkdir(parents=True)
    monkeypatch.setattr(health, "ROOT", health_path.parent)
    with health.connect() as db, db:
        health.enqueue(db, "paper:legacy-alert", "paper_entry", "old idea", NOW.timestamp(), idea_id="wait-long")
    try:
        yield project, parent, phase_db, cleanup_path, protected
    finally:
        phase_db.close()


def _account(db, key, position, pending):
    db.execute("INSERT INTO book VALUES (?,?)", (key, ema.paper.encode({
        "book_id": key, "cash": 9980., "equity": 10040., "position": position, "pending": pending,
    })))


def test_verified_cutoff_is_resumable_and_preserves_wal_and_old_positions(legacy):
    project, parent, db, cleanup_path, protected = legacy
    assert (project / f"{archive.PHASE15_DATABASE}-wal").is_file()
    with archive.cutover_locks(project=project):
        record = archive.create_locked(parent, project=project, cutoff=NOW)
    body = record["body"]
    assert body["tables"][archive.PHASE15_DATABASE]["event"] == 1
    assert body["tables"][archive.HEALTH_DATABASE]["outbox"] == 1
    assert len(body["slots"]) == 7
    assert body["legacy_ideas"] == ["wait-long"]
    assert body["slots"]["breakout/day/fixed"]["disposition"]["pending"] == "CANCELLED_AT_CUTOVER"
    swing = body["slots"]["breakout/swing/fixed"]
    assert swing["disposition"]["exposure"] == "ADMINISTRATIVELY_ARCHIVED_UNRESOLVED"
    assert swing["disposition"]["execution_price"] is None and swing["disposition"]["strategy_result"] is None
    assert swing["account"]["position"] == {"id": "open-short"}
    assert len(body["raw_receipts"]["files"]) == 1
    assert not gate.legacy_entries_closed(root=project / archive.STATE_DIRECTORY)
    with archive.cutover_locks(project=project):
        assert archive.create_locked(parent, project=project) == record
        archive.retire_locked(record, project=project)
        gone = archive.apply_cleanup_locked(record, project=project)
        assert archive.retire_locked(record, project=project) == record
        assert archive.apply_cleanup_locked(record, project=project) == gone
    assert len(list(parent.iterdir())) == 1
    assert len(gone) == 1 and gone["runs/phase15/first-managed.json"]["state"] == "REMOVED"
    assert not cleanup_path.exists() and protected.is_file()
    assert gate.retired(root=project / archive.STATE_DIRECTORY) == record
    with pytest.raises(sqlite3.IntegrityError, match="legacy paper generation retired"):
        with db:
            db.execute("UPDATE book SET state='{}'")
    with closing(sqlite3.connect(project / archive.EMA_DATABASE)) as old_db:
        with pytest.raises(sqlite3.IntegrityError, match="legacy paper generation retired"):
            with old_db:
                _account(old_db, "new-legacy-book", None, None)
    assert archive.verify(body["path"]) == record


def test_tampered_backup_cannot_retire_or_clean(legacy):
    project, parent, _, cleanup_path, _ = legacy
    with archive.cutover_locks(project=project):
        record = archive.create_locked(parent, project=project, cutoff=NOW)
        backup = Path(record["body"]["path"]) / archive.EMA_DATABASE
        backup.write_bytes(b"invalid")
        with pytest.raises(ValueError, match="Archive content checksum"):
            archive.retire_locked(record, project=project)
    assert cleanup_path.is_file()
    assert not gate.legacy_entries_closed(root=project / archive.STATE_DIRECTORY)


def test_outbox_drift_refuses_the_old_cutoff(legacy):
    project, parent, _, _, _ = legacy
    with archive.cutover_locks(project=project):
        record = archive.create_locked(parent, project=project, cutoff=NOW)
        with closing(sqlite3.connect(project / archive.HEALTH_DATABASE)) as db, db:
            db.execute("UPDATE outbox SET status='SENT'")
        with pytest.raises(ValueError, match="Notification records advanced"):
            archive.retire_locked(record, project=project)
        next_run = archive.create_locked(parent, project=project, cutoff=NOW + timedelta(seconds=1))
        assert next_run != record
    assert not gate.legacy_entries_closed(root=project / archive.STATE_DIRECTORY)


def test_cutover_resumes_before_current_publication_without_replaying_outbox(legacy, monkeypatch):
    project, parent, _, cleanup_path, _ = legacy
    current = {}
    source_calls = []
    failures = []
    monkeypatch.setattr(gate, "ROOT", project / archive.STATE_DIRECTORY)
    monkeypatch.setattr(cli, "utc_now", lambda: NOW)
    monkeypatch.setattr(cli.archive, "cutover_locks", lambda: cutover_locks(project=project))
    monkeypatch.setattr(cli.archive, "create_locked", lambda root: create_locked(root, project=project, cutoff=NOW))
    monkeypatch.setattr(cli.archive, "retire_locked", lambda record: retire_locked(record, project=project))
    monkeypatch.setattr(cli.archive, "apply_cleanup_locked", lambda record: cleanup_locked(record, project=project))
    monkeypatch.setattr(cli.archive, "protected_hashes", lambda root=project: protected_hashes(root))
    monkeypatch.setattr(cli.runtime, "current", lambda: current.get("record"))
    monkeypatch.setattr(cli.runtime, "ensure_runner", lambda: {"state": "FIXTURE"})

    def probe():
        source_calls.append(True)
        return {"MARK": {"state": "QUALIFIED"}}

    def define(record, qualification, now):
        assert qualification == {"MARK": {"state": "QUALIFIED"}}
        return {"registered_at": now.isoformat(), "start": (now + timedelta(minutes=15)).isoformat(),
                "cutoff": record["body"]["cutoff"], "lanes": {
                    slot: {"book_id": f"fresh/{slot}"} for slot in record["body"]["slots"]}}

    def initialize(archived, qualification, *, prepared, before_publish):
        assert gate.retired() == archived
        record = {"id": fingerprint(prepared), "body": prepared}
        before_publish(record)
        with health.connect() as db:
            assert db.execute("SELECT status FROM outbox WHERE id='paper:legacy-alert'").fetchone()[0] == "SUPERSEDED"
        if not failures:
            failures.append(record["id"])
            raise RuntimeError("Fixture failed before publication")
        current["record"] = record
        return record

    monkeypatch.setattr(cli.runtime, "probe", probe)
    monkeypatch.setattr(cli.runtime, "definitions", define)
    monkeypatch.setattr(cli.runtime, "initialize", initialize)
    with health.connect() as db, db:
        health.enqueue(db, "health:test", "health", "incident", NOW.timestamp())
    with pytest.raises(RuntimeError, match="Fixture failed"):
        cli.cutover(parent)
    assert not current and cleanup_path.is_file()
    completed = cli.cutover(parent)
    assert completed["generation_id"] == failures[0] and len(completed["books"]) == 7
    assert completed["legacy_outbox_superseded"] == 1
    assert cli.cutover(parent) == completed
    assert source_calls == [True]
    assert not cleanup_path.exists()
    with health.connect() as db:
        assert db.execute("SELECT status FROM outbox WHERE id='health:test'").fetchone()[0] == "PENDING"
        assert db.execute("SELECT count(*) FROM meta WHERE key LIKE ?", (fresh_paper.CURSOR_PREFIX + "%",)).fetchone()[0] == 7


cutover_locks = archive.cutover_locks
create_locked = archive.create_locked
retire_locked = archive.retire_locked
cleanup_locked = archive.apply_cleanup_locked
protected_hashes = archive.protected_hashes
