"""Watchdog recovery of a malformed, detached Kraken collector heartbeat."""

from contextlib import closing
from hashlib import sha256
import json
from math import isfinite
import os
from pathlib import Path
import sqlite3

import psutil

from lab.ema_paper import kraken_reference as reference
from lab.kraken_paper import source
from lab.storage import read_json, utc, utc_now, writer_lock


class RecoveryBlocked(RuntimeError):
    def __init__(self, problem, reason, error=None):
        super().__init__(reason)
        self.evidence = {"state": "RECOVERY_BLOCKED", "heartbeat_error": problem, "reason": reason}
        if error is not None:
            self.evidence["recovery_error"] = {"error": type(error).__name__, "detail": str(error)}


def _heartbeat(path):
    raw = None
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("Kraken reference heartbeat must be a JSON object")
    except FileNotFoundError:
        return None, None
    except (OSError, ValueError) as error:
        return raw, {"path": str(path), "error": type(error).__name__, "detail": str(error)}
    return raw, None


def heartbeat_problem(root):
    """Inspect the heartbeat without taking a lease or changing any files."""
    _, issue = _heartbeat(Path(root) / reference.HEARTBEAT)
    return issue


def _old_process_is_gone(root, report, now, process_factory):
    marker = read_json(root / reference.PROCESS)
    pid, created = marker["pid"], marker["created"]
    start = utc(marker["started_at"])
    if (type(pid) is not int or pid <= 0 or type(created) not in (int, float)
            or not isfinite(created) or not 0 < created <= now.timestamp()
            or not start <= now or marker["qualification_id"] != report["id"]):
        raise ValueError("Invalid Kraken reference process identity or qualification")
    try:
        process = process_factory(pid)
        return process.create_time() != created or not process.is_running()
    except psutil.NoSuchProcess:
        return True


def _healthy_database(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        return db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]


def _preserve(path, raw, backup_root):
    path = Path(path)
    folder = Path(backup_root).resolve()
    if folder == path.parent or folder.is_relative_to(path.parent):
        raise ValueError("Kraken recovery artifacts require a separate private backup tree")
    digest = sha256(raw).hexdigest()
    folder.mkdir(parents=True, exist_ok=True)
    saved = folder / f"reference-heartbeat-{digest}.corrupt"
    if saved.is_symlink():
        raise ValueError("Invalid Kraken heartbeat recovery artifact path")
    try:
        with saved.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        with saved.open("r+b") as stream:
            if stream.read() != raw:
                raise ValueError("Existing recovery artifact does not match the corrupt heartbeat")
            os.fsync(stream.fileno())
    if saved.read_bytes() != raw:
        raise ValueError("Durable recovery artifact failed verification")
    return saved, digest


def recover_heartbeat(root, *, backup_root=None, clock=utc_now, process_factory=psutil.Process):
    """Remove only a corrupt heartbeat with verified dead ownership and a durable copy."""
    root = reference._root(root)
    path = root / reference.HEARTBEAT
    raw, problem = _heartbeat(path)
    if not problem:
        return None
    if raw is None or path.is_symlink():
        raise RecoveryBlocked(problem, "HEARTBEAT_BYTES_UNAVAILABLE")
    store = root / "source"
    local = Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
    backup_root = backup_root if backup_root is not None else local / "ASEKE" / "kraken-reference-recovery"
    step = "RECOVERY_LEASES_UNAVAILABLE"
    try:
        with (writer_lock(root / ".reference-dispatch.lock"),
              writer_lock(root / ".reference-process.lock"),
              writer_lock(store / ".kraken-source.lock")):
            observed, latest = _heartbeat(path)
            if not latest:
                return None
            if observed != raw or path.is_symlink():
                raise RecoveryBlocked(problem, "HEARTBEAT_CHANGED_DURING_RECOVERY")
            step = "QUALIFICATION_OR_AUTHORIZATION_UNVERIFIED"
            report = reference.qualification(root=root)
            if not report:
                raise ValueError("Missing Kraken reference qualification")
            now = clock()
            reference._qualified_permission(report, now)
            step = "OLD_PRODUCER_IDENTITY_UNVERIFIED"
            if not _old_process_is_gone(root, report, now, process_factory):
                raise RecoveryBlocked(problem, "OLD_PRODUCER_STILL_RUNNING")
            step = "SOURCE_DATABASE_INTEGRITY_UNVERIFIED"
            if not _healthy_database(store / source.FILE):
                raise ValueError("Kraken reference SQLite integrity check failed")
            step = "PRESERVATION_OR_REMOVAL_FAILED"
            saved, digest = _preserve(path, raw, backup_root)
            if path.read_bytes() != raw:
                raise RecoveryBlocked(problem, "HEARTBEAT_CHANGED_DURING_RECOVERY")
            path.unlink()
    except RecoveryBlocked:
        raise
    except (OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error, psutil.Error) as error:
        raise RecoveryBlocked(problem, step, error) from error
    return {"state": "CORRUPT_HEARTBEAT_PRESERVED", "heartbeat_error": problem,
            "preserved_file": str(saved), "invalid_sha256": digest, "bytes": len(raw)}
