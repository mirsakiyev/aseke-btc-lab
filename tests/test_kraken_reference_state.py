from contextlib import closing
from hashlib import sha256
import sqlite3
from types import SimpleNamespace

import psutil
import pytest

from lab.config import fingerprint
from lab.ema_paper import kraken_reference as reference, synthetic_protocol
from lab.health import __main__ as health, kraken_reference_state as recovery
from lab.paper_run import gate
from lab.storage import utc, write_json, writer_lock


NOW = utc("2026-11-17T01:35:29Z")
CREATED = utc("2026-11-17T01:32:28Z")
CORRUPT = b"\0" * 88
PID = 17352


def dead(pid):
    assert pid == PID
    raise psutil.NoSuchProcess(pid)


@pytest.fixture
def collector(tmp_path, monkeypatch):
    root, backup = tmp_path / "ema_kraken_reference", tmp_path / "private"
    scope = {"authorization_id": reference.AUTHORIZATION_ID}
    report = {"id": fingerprint(scope), "body": scope}
    write_json(root / reference.QUALIFICATION, report)
    write_json(root / reference.PROCESS, {"pid": PID, "created": CREATED.timestamp(), "started_at": CREATED.isoformat(),
                                        "qualification_id": report["id"]})
    heartbeat = root / reference.HEARTBEAT
    heartbeat.write_bytes(CORRUPT)
    store = root / "source"
    store.mkdir()
    database = store / recovery.source.FILE
    with closing(sqlite3.connect(database)) as db:
        db.execute("CREATE TABLE source_evidence (id TEXT)")
    authorized = []
    monkeypatch.setattr(reference, "_qualified_permission", lambda actual, now: authorized.append((actual, now)))
    return root, backup, heartbeat, database, report, authorized


def attempt(collector, process=dead):
    root, backup, *_ = collector
    return recovery.recover_heartbeat(root, backup_root=backup, clock=lambda: NOW, process_factory=process)


@pytest.mark.parametrize("process", [
    dead,
    lambda _: SimpleNamespace(create_time=lambda: CREATED.timestamp() + 1),
])
def test_recovers_a_corrupt_heartbeat_with_dead_or_reused_marker(collector, process):
    root, backup, heartbeat, database, report, authorized = collector
    before = database.read_bytes()
    process_marker = (root / reference.PROCESS).read_bytes()

    outcome = attempt(collector, process)

    assert outcome["state"] == "CORRUPT_HEARTBEAT_PRESERVED"
    assert outcome["heartbeat_error"]["path"] == str(heartbeat)
    assert outcome["heartbeat_error"]["error"] == "JSONDecodeError"
    assert outcome["invalid_sha256"] == sha256(CORRUPT).hexdigest()
    assert not heartbeat.exists()
    saved, = backup.glob("*.corrupt")
    assert str(saved) == outcome["preserved_file"] and saved.read_bytes() == CORRUPT
    assert authorized == [(report, NOW)]
    assert database.read_bytes() == before and (root / reference.PROCESS).read_bytes() == process_marker
    assert attempt(collector) is None


@pytest.mark.parametrize(("process", "reason"), [
    (lambda _: SimpleNamespace(create_time=lambda: CREATED.timestamp(), is_running=lambda: True),
     "OLD_PRODUCER_STILL_RUNNING"),
    (lambda pid: (_ for _ in ()).throw(psutil.AccessDenied(pid)), "OLD_PRODUCER_IDENTITY_UNVERIFIED"),
])
def test_unverified_or_live_producer_is_untouched(collector, process, reason):
    _, backup, heartbeat, *_ = collector
    with pytest.raises(recovery.RecoveryBlocked) as raised:
        attempt(collector, process)
    assert raised.value.evidence["reason"] == reason
    assert heartbeat.read_bytes() == CORRUPT and not backup.exists()


@pytest.mark.parametrize("lease", [".reference-dispatch.lock", ".reference-process.lock", "source/.kraken-source.lock"])
def test_each_existing_owner_lease_is_required(collector, lease):
    root, backup, heartbeat, *_ = collector
    with writer_lock(root / lease), pytest.raises(recovery.RecoveryBlocked) as raised:
        attempt(collector)
    assert raised.value.evidence["reason"] == "RECOVERY_LEASES_UNAVAILABLE"
    assert heartbeat.read_bytes() == CORRUPT and not backup.exists()


def test_revalidates_authorization_and_source_integrity_before_backup(collector, monkeypatch):
    _, backup, heartbeat, database, *_ = collector

    def deny(*_):
        raise PermissionError("Source qualification expired")

    with monkeypatch.context() as patch:
        patch.setattr(reference, "_qualified_permission", deny)
        with pytest.raises(recovery.RecoveryBlocked) as denied:
            attempt(collector)
    assert denied.value.evidence["reason"] == "QUALIFICATION_OR_AUTHORIZATION_UNVERIFIED"

    database.write_bytes(b"not SQLite")
    with pytest.raises(recovery.RecoveryBlocked) as damaged:
        attempt(collector)
    assert damaged.value.evidence["reason"] == "SOURCE_DATABASE_INTEGRITY_UNVERIFIED"
    assert heartbeat.read_bytes() == CORRUPT and not backup.exists()


def test_marker_must_be_present_and_match_qualification(collector):
    root, backup, heartbeat, *_ = collector
    process = root / reference.PROCESS
    process.unlink()
    with pytest.raises(recovery.RecoveryBlocked) as missing:
        attempt(collector)
    assert missing.value.evidence["reason"] == "OLD_PRODUCER_IDENTITY_UNVERIFIED"
    write_json(process, {"pid": PID, "created": CREATED.timestamp(), "started_at": CREATED.isoformat(),
                         "qualification_id": "a" * 64})
    with pytest.raises(recovery.RecoveryBlocked) as mismatch:
        attempt(collector)
    assert mismatch.value.evidence["reason"] == "OLD_PRODUCER_IDENTITY_UNVERIFIED"
    assert heartbeat.read_bytes() == CORRUPT and not backup.exists()


def test_failed_durable_write_leaves_original_in_place(collector, monkeypatch):
    _, _, heartbeat, *_ = collector

    def fail(_):
        raise OSError("Simulated flush failure")

    monkeypatch.setattr(recovery.os, "fsync", fail)
    with pytest.raises(recovery.RecoveryBlocked) as raised:
        attempt(collector)
    assert raised.value.evidence["reason"] == "PRESERVATION_OR_REMOVAL_FAILED"
    assert heartbeat.read_bytes() == CORRUPT


def test_read_only_health_exposes_path_and_exact_decode_error(collector, monkeypatch):
    root, backup, heartbeat, *_ = collector
    monkeypatch.setattr(health, "ROOT", root.parent / "health")
    monkeypatch.setattr(gate, "retired", lambda: None)
    current = health._operational_status()["kraken_reference"]
    assert current["state"] == "EVIDENCE_UNAVAILABLE"
    assert current["heartbeat_error"] == {
        "path": str(heartbeat), "error": "JSONDecodeError", "detail": "Expecting value: line 1 column 1 (char 0)",
    }
    assert heartbeat.read_bytes() == CORRUPT and not backup.exists()


def test_only_supervisor_uses_existing_ensure_after_verified_preservation(collector, monkeypatch):
    root, backup, heartbeat, *_ = collector
    recover = recovery.recover_heartbeat
    monkeypatch.setattr(reference, "ROOT", root)
    monkeypatch.setattr(synthetic_protocol, "active", lambda: True)
    monkeypatch.setattr(synthetic_protocol, "load", lambda: {})
    monkeypatch.setattr(health, "_kraken_reference_continuity", lambda *_: False)
    monkeypatch.setattr(recovery, "recover_heartbeat", lambda path: recover(
        path, backup_root=backup, clock=lambda: NOW, process_factory=dead))

    def ensure(*, root):
        assert not heartbeat.exists()
        path, = backup.glob("*.corrupt")
        assert path.read_bytes() == CORRUPT
        with writer_lock(root / ".reference-dispatch.lock"):
            return {"state": "STARTED"}

    monkeypatch.setattr(reference, "ensure", ensure)
    evidence = {}
    health._supervise_kraken_reference(evidence)
    assert evidence["ema_kraken_reference"]["state"] == "STARTED"
    assert evidence["ema_kraken_reference_recovery"]["invalid_sha256"] == sha256(CORRUPT).hexdigest()
