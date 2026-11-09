from contextlib import closing, contextmanager
from datetime import timedelta
import json
import sqlite3

from lab.config import fingerprint
from lab.learning.design import PROFILE_IDS, STATE
from lab.storage import utc_now


DATABASE = STATE / "learning.sqlite3"
DEFAULT = {"mode": "off", "state": "OFF", "profiles": dict.fromkeys(PROFILE_IDS, True), "job": None,
           "last_check": None, "reason": "Learning disabled", "resume_mode": "continuous"}
TERMINAL_TRIALS = {"COMPLETED", "INSUFFICIENT_EVIDENCE", "BLOCKED_DATA", "REJECTED", "FAILED",
                   "WAITING_FOR_DATA", "DEDUPLICATED"}
SCHEMA = """
CREATE TABLE IF NOT EXISTS control (singleton INTEGER PRIMARY KEY CHECK(singleton=1), body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY, job TEXT, kind TEXT NOT NULL, trial TEXT, at TEXT NOT NULL, body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_job ON events(job, kind, trial, seq);
CREATE INDEX IF NOT EXISTS events_trial ON events(trial, kind, seq);
"""


def encode(record):
    return json.dumps(record, default=str, sort_keys=True, allow_nan=False)


@contextmanager
def connect(path=None, *, write=False):
    path = DATABASE if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path, timeout=8)) as db:
        db.row_factory = sqlite3.Row
        if write:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.executescript(SCHEMA)
            db.execute("BEGIN IMMEDIATE")
        try:
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise


def current(db):
    row = db.execute("SELECT body FROM control WHERE singleton=1").fetchone()
    return json.loads(row["body"]) if row else {**DEFAULT, "profiles": DEFAULT["profiles"].copy()}


def event(db, job, kind, body, trial=None):
    db.execute("INSERT INTO events(job,kind,trial,at,body) VALUES (?,?,?,?,?)",
               (job, kind, trial, utc_now().isoformat(), encode(body)))


def save(db, prior, **updates):
    updated = {**prior, **updates}
    db.execute("INSERT INTO control VALUES (1,?) ON CONFLICT(singleton) DO UPDATE SET body=excluded.body", (encode(updated),))
    if updated != prior:
        event(db, updated["job"], "STATE", {key: updated.get(key) for key in
                                            ("state", "mode", "reason", "profiles", "active_trial", "last_check")})
    return updated


def enqueue(db, state, manifest):
    enabled = sorted(key for key, value in state["profiles"].items() if value)
    if not enabled:
        return save(db, state, state="WAITING_FOR_DATA", reason="All research profiles disabled", last_check=utc_now().isoformat())
    design = manifest["design"]
    job_id = fingerprint([manifest["id"], enabled])[:24]
    existing = db.execute("""
        SELECT 1 FROM jobs WHERE id=? OR (json_extract(body,'$.manifest')=? AND json_extract(body,'$.profiles')=json(?))
    """, (job_id, manifest["id"], encode(enabled))).fetchone()
    if existing:
        return save(db, state, state="WAITING_FOR_DATA", reason="Campaign already registered for these immutable inputs",
                    last_check=utc_now().isoformat())
    now = utc_now()
    job = {"id": job_id, "manifest": manifest["id"], "profiles": enabled, "queued_at": now.isoformat(),
           "deadline": (now + timedelta(seconds=design["settings"]["budget"]["wall_seconds"])).isoformat(),
           "trial_ids": [trial["id"] for trial in design["trials"] if trial["profile"] in enabled]}
    db.execute("INSERT INTO jobs VALUES (?,?)", (job_id, encode(job)))
    event(db, job_id, "JOB_QUEUED", job)
    return save(db, state, job=job_id, state="QUEUED", reason="Immutable inputs and catalog pinned", last_check=now.isoformat())


def job(db, key):
    row = db.execute("SELECT body FROM jobs WHERE id=?", (key,)).fetchone()
    return json.loads(row["body"]) if row else None


def trials(db, job_id):
    rows = db.execute("SELECT trial,body FROM events WHERE job=? AND kind='TRIAL_RESULT' ORDER BY seq", (job_id,)).fetchall()
    return {row["trial"]: json.loads(row["body"]) for row in rows}


def events(db, job_id):
    return [{**dict(row), "body": json.loads(row["body"])} for row in
            db.execute("SELECT seq,job,kind,trial,at,body FROM events WHERE job=? ORDER BY seq", (job_id,))]


def latest_job(db):
    row = db.execute("SELECT body FROM jobs ORDER BY rowid DESC LIMIT 1").fetchone()
    return json.loads(row["body"]) if row else None


def status():
    if not DATABASE.exists():
        return {**DEFAULT, "latest_job": None, "campaigns": 0, "cumulative_fit_attempts": 0,
                "cumulative_candidate_attempts": 0, "latest_results": {}}
    with connect() as db:
        state = current(db)
        newest = latest_job(db)
        count = db.execute("SELECT count(*) FROM jobs").fetchone()[0]
        attempts = db.execute("SELECT count(*) FROM events WHERE kind='TRIAL_STARTED'").fetchone()[0]
        fits = db.execute("SELECT count(*) FROM events WHERE kind='FIT_STARTED'").fetchone()[0]
        return {**state, "latest_job": newest, "campaigns": count, "cumulative_candidate_attempts": attempts,
                "cumulative_fit_attempts": fits, "latest_results": trials(db, newest["id"]) if newest else {}}
