"""Append a source-operations amendment over the existing synthetic EMA study."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3

from lab.config import ROOT as PROJECT, fingerprint
from lab.ema_paper import catalog, synthetic_protocol
from lab.storage import file_hash, utc_now, writer_lock


VERSION = "EMA_KRAKEN_CHART_RECOVERY_V1"
FILENAME = "chart-recovery.sqlite3"
SCHEMA = """
CREATE TABLE IF NOT EXISTS operations_amendment(id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS ops_amendment_update BEFORE UPDATE ON operations_amendment
    BEGIN SELECT RAISE(ABORT, 'immutable EMA operations amendment'); END;
CREATE TRIGGER IF NOT EXISTS ops_amendment_delete BEFORE DELETE ON operations_amendment
    BEGIN SELECT RAISE(ABORT, 'immutable EMA operations amendment'); END;
"""


def _path(root):
    return (synthetic_protocol.ROOT if root is None else Path(root)) / FILENAME


def active(*, root=None):
    return _path(root).is_file()


def _pins():
    names = ("chart_recovery.py", "recovery_protocol.py", "recovery_runtime.py", "recovery_source.py")
    paths = tuple(f"lab/ema_paper/{name}" for name in names)
    return {path: file_hash(PROJECT / path) for path in paths}


def _scope(synthetic):
    return {profile: {key: record["body"][key] for key in (
        "book_id", "registration_id", "strategy_hash", "execution_model_id")}
        for profile, record in synthetic["body"]["books"].items()}


def load(synthetic, *, root=None, now=None):
    now = utc_now() if now is None else synthetic_protocol.utc(now)
    path = _path(root)
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        rows = db.execute("SELECT id,body FROM operations_amendment").fetchall()
    if len(rows) != 1:
        raise PermissionError("Exactly one frozen EMA chart-recovery amendment is required")
    identifier, payload = rows[0]
    body = json.loads(payload)
    if (fingerprint(body) != identifier or body["version"] != VERSION or body["predecessor"] != synthetic["id"]
            or body["books"] != _scope(synthetic) or body["source_id"] != synthetic["body"]["source_id"]
            or body["code_pins"] != _pins() or not synthetic_protocol.utc(synthetic["body"]["recorded_at"])
            <= synthetic_protocol.utc(body["recorded_at"]) <= now):
        raise PermissionError("EMA recovery amendment does not match its frozen synthetic predecessor or runtime")
    return {"id": identifier, "body": body}


def register(*, root=None, paper_root=None, now=None):
    """Bind an operations-only amendment without altering original registrations or clocks."""
    path = _path(root)
    paper_root = catalog.ROOT if paper_root is None else Path(paper_root)
    now = utc_now() if now is None else synthetic_protocol.utc(now)
    with writer_lock(path.parent / ".recovery-register.lock"), writer_lock(paper_root / ".cycle.lock"):
        synthetic = synthetic_protocol.load(root=root, now=now)
        if path.is_file():
            return load(synthetic, root=root, now=now)
        with closing(catalog.db_open(True, root=paper_root)) as db:
            originals = catalog.registrations(db)
            scope = _scope(synthetic)
            if set(originals) != set(scope) or any(
                row["id"] != scope[name]["registration_id"] or row["body"]["book_id"] != scope[name]["book_id"]
                or row["body"]["strategy_hash"] != scope[name]["strategy_hash"] for name, row in originals.items()
            ):
                raise PermissionError("Original EMA books differ from the proposed operations amendment")
        body = {"version": VERSION, "predecessor": synthetic["id"], "books": scope,
                "source_id": synthetic["body"]["source_id"], "recorded_at": now.isoformat(), "code_pins": _pins(),
                "scope": "Chart context repair only. Original strategy, economics, grants, start and horizon remain frozen."}
        identifier = fingerprint(body)
        with closing(sqlite3.connect(path, timeout=3)) as db:
            db.executescript(SCHEMA)
            with db:
                db.execute("INSERT INTO operations_amendment VALUES (?,?)", (identifier, json.dumps(body, sort_keys=True)))
        return {"id": identifier, "body": body}
