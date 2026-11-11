import json
import re
import sqlite3
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path

from lab.storage import RUNS, utc_now


ROOT = RUNS / "strategy_lab"
MODE = "MANUAL_OUTSIDE_LAB"
MODEL_VALIDATION = "No validated model"
PROFILES = {
    "position": ("SPOT", "BTC-USDT", "1D", 3600),
    "swing": ("FUTURES", "XBTUSDTM", "4h", 60),
    "day": ("FUTURES", "XBTUSDTM", "15min", 60),
}
SOURCE_TYPES = ("manual", "url", "document", "pine", "chart", "repository")
STATES = (
    "DRAFT", "SPECIFIED", "IMPLEMENTED", "UNIT_TESTED", "HISTORICAL_RESEARCH", "INCONCLUSIVE",
    "REJECTED", "SHORTLISTED", "PROSPECTIVE_PENDING", "PROSPECTIVE_RUNNING", "PROSPECTIVE_COMPLETE", "ARCHIVED",
)
TRANSITIONS = {
    "DRAFT": {"SPECIFIED", "REJECTED"},
    "SPECIFIED": {"IMPLEMENTED", "REJECTED"},
    "IMPLEMENTED": {"UNIT_TESTED", "REJECTED"},
    "UNIT_TESTED": {"HISTORICAL_RESEARCH", "REJECTED"},
    "HISTORICAL_RESEARCH": {"INCONCLUSIVE", "REJECTED", "SHORTLISTED"},
    "INCONCLUSIVE": {"HISTORICAL_RESEARCH", "REJECTED", "SHORTLISTED"},
    "SHORTLISTED": {"PROSPECTIVE_PENDING", "INCONCLUSIVE", "REJECTED"},
    "PROSPECTIVE_PENDING": {"SHORTLISTED", "REJECTED"},
    "PROSPECTIVE_RUNNING": {"PROSPECTIVE_COMPLETE", "INCONCLUSIVE"},
}
DEFAULT_FLAGS = {"evidence_quality": "UNASSESSED", "reproducibility": "UNVERIFIED", "data_completeness": "UNKNOWN"}
FLAGS = {**DEFAULT_FLAGS, "model_validation": MODEL_VALIDATION, "execution": MODE,
         "historical_permission": "EXPLICIT_PER_JOB", "prospective_permission": "DISABLED", "live_permission": "DISABLED"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS strategy_version (
    hash TEXT PRIMARY KEY, strategy_id TEXT NOT NULL, family_id TEXT NOT NULL, version INTEGER NOT NULL,
    created_at TEXT NOT NULL, spec TEXT NOT NULL, UNIQUE(strategy_id, version)
);
CREATE TABLE IF NOT EXISTS strategy_event (
    seq INTEGER PRIMARY KEY, version_hash TEXT NOT NULL REFERENCES strategy_version(hash),
    at TEXT NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL, flags TEXT NOT NULL, reference TEXT
);
CREATE INDEX IF NOT EXISTS version_events ON strategy_event(version_hash, seq);
CREATE TABLE IF NOT EXISTS dataset (hash TEXT PRIMARY KEY, admitted_at TEXT NOT NULL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS job (
    hash TEXT PRIMARY KEY, created_at TEXT NOT NULL, request TEXT NOT NULL,
    state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, deadline TEXT
);
CREATE TABLE IF NOT EXISTS job_event (
    seq INTEGER PRIMARY KEY, job_hash TEXT NOT NULL REFERENCES job(hash), at TEXT NOT NULL, message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS job_events ON job_event(job_hash, seq);
CREATE TABLE IF NOT EXISTS checkpoint (
    job_hash TEXT NOT NULL REFERENCES job(hash), stage TEXT NOT NULL, body TEXT NOT NULL,
    PRIMARY KEY(job_hash, stage)
);
CREATE TABLE IF NOT EXISTS proposal (hash TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_blob (hash TEXT PRIMARY KEY, body BLOB NOT NULL);
"""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    return sha256(canonical(value).encode("utf-8")).hexdigest()


def now():
    return utc_now().isoformat()


@contextmanager
def connect(root=None, *, write=False):
    path = (ROOT if root is None else Path(root)) / "registry.sqlite3"
    memory = not write and not path.exists()
    if write:
        path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(path, timeout=5)
    elif not memory:
        db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    else:
        db = sqlite3.connect(":memory:")
    try:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        if write or memory:
            db.executescript(SCHEMA)
        if write:
            for table in ("strategy_version", "strategy_event", "dataset", "job_event", "checkpoint", "proposal", "source_blob"):
                for action in ("UPDATE", "DELETE"):
                    db.execute(f"""CREATE TRIGGER IF NOT EXISTS immutable_{table}_{action}
                                   BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT, 'immutable'); END""")
            db.execute("BEGIN IMMEDIATE")
        else:
            db.execute("PRAGMA query_only=ON")
        with db:
            yield db
    finally:
        db.close()


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._/-]{0,95}", value):
        raise ValueError("Use a stable lowercase strategy and family identifier")
    return value


def event(db, version, state, reason, *, flags=None, reference=None):
    if state not in STATES or not isinstance(reason, str) or not reason.strip():
        raise ValueError("A recognized state and transition reason are required")
    prior = db.execute("SELECT flags FROM strategy_event WHERE version_hash=? ORDER BY seq DESC LIMIT 1",
                       (version,)).fetchone()
    evidence = json.loads(prior["flags"]) if prior else FLAGS.copy()
    if set(flags or {}) - DEFAULT_FLAGS.keys() or any(
        not isinstance(value, str) or not value.strip() for value in (flags or {}).values()
    ):
        raise ValueError("Only evidence, reproducibility and completeness flags are editable")
    evidence.update(flags or {})
    db.execute("INSERT INTO strategy_event(version_hash, at, state, reason, flags, reference) VALUES(?,?,?,?,?,?)",
               (version, now(), state, reason.strip(), canonical(evidence), canonical(reference)))


def register(db, spec, *, expected=None, initial="DRAFT", reason="Staged for review", flags=None, reference=None):
    strategy_id, family_id = identifier(spec.get("strategy_id")), identifier(spec.get("family_id"))
    body = canonical(spec)
    latest = db.execute("SELECT * FROM strategy_version WHERE strategy_id=? ORDER BY version DESC LIMIT 1",
                        (strategy_id,)).fetchone()
    if latest and latest["family_id"] != family_id:
        raise ValueError("Family identity is fixed for a strategy ID")
    if latest and latest["spec"] == body:
        return get(db, latest["hash"])
    if (latest["hash"] if latest else None) != expected:
        raise ValueError("Supply the current version hash to create a revision")
    number, created = (latest["version"] + 1 if latest else 1), now()
    version = digest({"version": number, "created_at": created, "spec": spec})
    db.execute("INSERT INTO strategy_version VALUES(?,?,?,?,?,?)",
               (version, strategy_id, family_id, number, created, body))
    event(db, version, initial, reason, flags=flags, reference=reference)
    return get(db, version)


def get(db, version):
    row = db.execute("SELECT * FROM strategy_version WHERE hash=?", (version,)).fetchone()
    if row is None:
        raise ValueError("Unknown strategy version hash")
    spec = json.loads(row["spec"])
    if digest({"version": row["version"], "created_at": row["created_at"], "spec": spec}) != version:
        raise ValueError("Strategy snapshot hash mismatch")
    history = []
    for item in db.execute("SELECT at,state,reason,flags,reference FROM strategy_event WHERE version_hash=? ORDER BY seq",
                           (version,)):
        history.append({**dict(item), "flags": json.loads(item["flags"]), "reference": json.loads(item["reference"])})
    return {**dict(row), "spec": spec, "state": history[-1]["state"], "flags": history[-1]["flags"], "history": history}


def inventory(db, *, all_versions=False):
    query = "SELECT hash FROM strategy_version"
    if not all_versions:
        query += " AS v WHERE version=(SELECT MAX(version) FROM strategy_version WHERE strategy_id=v.strategy_id)"
    return [get(db, row["hash"]) for row in db.execute(query + " ORDER BY strategy_id, version DESC")]


def source_content(db, key):
    row = db.execute("SELECT body FROM source_blob WHERE hash=?", (key,)).fetchone()
    if row is None or sha256(row["body"]).hexdigest() != key:
        raise ValueError("Unknown attachment or source checksum mismatch")
    return row["body"]


def transition(db, version, state, reason, *, flags=None):
    record = get(db, version)
    current = record["state"]
    if state == current:
        if not flags:
            raise ValueError("Supply evidence flags when recording an update in the same state")
    elif state in ("IMPLEMENTED", "UNIT_TESTED", "PROSPECTIVE_PENDING"):
        raise ValueError("Use the implementation, test, completed replay or proposal workflow for that state")
    elif state not in TRANSITIONS.get(current, set()) and (state != "ARCHIVED" or current == "ARCHIVED"):
        raise ValueError(f"Transition from {current} to {state} is unavailable")
    if state == "SPECIFIED" and state != current:
        from lab.strategy_lab.specification import validate
        if issues := validate(record["spec"]):
            raise ValueError(f"Specification requires review: {issues}")
    if state in ("HISTORICAL_RESEARCH", "SHORTLISTED", "INCONCLUSIVE") and not db.execute(
        "SELECT 1 FROM job WHERE state='COMPLETE' AND json_extract(request,'$.version')=?", (version,)
    ).fetchone():
        raise ValueError("A sandbox experiment result is required")
    if state == "SHORTLISTED" and not db.execute(
        """SELECT 1 FROM checkpoint AS c JOIN job AS j ON c.job_hash=j.hash
           WHERE j.state='COMPLETE' AND c.stage='verdict' AND json_extract(j.request,'$.version')=?
           AND json_extract(c.body,'$.status')='EXPLORATORY_SCREEN'""", (version,)
    ).fetchone():
        raise ValueError("Shortlisting requires an explicit review of a conclusive historical screen")
    event(db, version, state, reason, flags=flags)
    return get(db, version)
