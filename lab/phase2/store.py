from contextlib import closing, contextmanager
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import zlib

import pandas as pd

from lab.data import OHLCV, SOURCE
from lab.storage import DATA, utc

FORWARD = DATA / "forward"
DATABASE = FORWARD / "observation.sqlite3"
STORE_LIMIT = 256 * 1024 * 1024
SCHEMA = """
CREATE TABLE IF NOT EXISTS payloads (sha TEXT PRIMARY KEY, zlib_body BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS responses (
    id INTEGER PRIMARY KEY, seconds INTEGER NOT NULL, start_open INTEGER NOT NULL,
    end_open INTEGER NOT NULL, received_at TEXT NOT NULL, url TEXT NOT NULL,
    sha TEXT NOT NULL REFERENCES payloads(sha), accepted INTEGER NOT NULL, problem TEXT
);
CREATE TABLE IF NOT EXISTS candles (
    id INTEGER PRIMARY KEY, seconds INTEGER NOT NULL, open_epoch INTEGER NOT NULL,
    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
    volume REAL NOT NULL, batch INTEGER NOT NULL REFERENCES responses(id),
    UNIQUE(seconds, open_epoch, batch)
);
CREATE INDEX IF NOT EXISTS candles_key ON candles(seconds, open_epoch, id DESC);
CREATE TABLE IF NOT EXISTS cursors (seconds INTEGER PRIMARY KEY, next_open INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS accounts (profile TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY, profile TEXT NOT NULL, key TEXT NOT NULL, kind TEXT NOT NULL,
    recorded_at TEXT NOT NULL, body TEXT NOT NULL, UNIQUE(profile, key)
);
CREATE INDEX IF NOT EXISTS events_profile ON events(profile, id DESC);
"""


def epoch(value: pd.Timestamp) -> int:
    return int(value.timestamp())


def from_epoch(value: int) -> pd.Timestamp:
    return pd.Timestamp(value, unit="s", tz="UTC")


def dump(record: object) -> str:
    return json.dumps(record, default=str, allow_nan=False, sort_keys=True, separators=(",", ":"))


@contextmanager
def connect(path: Path = DATABASE, *, readonly: bool = False):
    if not readonly:
        path.parent.mkdir(parents=True, exist_ok=True)
    target = path.resolve().as_uri() + "?mode=ro" if readonly else str(path)
    with closing(sqlite3.connect(target, uri=readonly, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        if readonly:
            db.execute("PRAGMA query_only = ON")
        else:
            db.execute("PRAGMA journal_mode = WAL")
            db.execute("PRAGMA synchronous = FULL")
            db.execute("PRAGMA foreign_keys = ON")
            db.execute("PRAGMA wal_autocheckpoint = 128")
            db.executescript(SCHEMA)
        yield db


def budget(path: Path = DATABASE) -> int:
    return sum(candidate.stat().st_size for candidate in (
        path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm"),
    ) if candidate.exists())


def checkpoint(db: sqlite3.Connection, seconds: int, default: pd.Timestamp) -> pd.Timestamp:
    row = db.execute("SELECT next_open FROM cursors WHERE seconds = ?", (seconds,)).fetchone()
    return from_epoch(row[0]) if row else default


def commit_response(db, seconds, start, end, received, url, body, normalized, problem=None) -> dict:
    """Atomically append evidence and unique price revisions before advancing the scan cursor."""
    digest = sha256(body).hexdigest()
    revisions, inserted = 0, 0
    with db:
        db.execute("INSERT OR IGNORE INTO payloads VALUES (?, ?)", (digest, zlib.compress(body)))
        batch = db.execute(
            """INSERT INTO responses(seconds, start_open, end_open, received_at, url, sha, accepted, problem)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (seconds, epoch(start), epoch(end), received.isoformat(), url, digest,
             len(normalized) if problem is None else 0, problem),
        ).lastrowid
        if problem is not None:
            return {"batch": batch, "error": problem, "raw_sha256": digest}
        for row in normalized[["open_at", *OHLCV]].itertuples(index=False, name=None):
            open_at, *prices = row
            previous = db.execute(
                """SELECT open, high, low, close, volume FROM candles
                   WHERE seconds = ? AND open_epoch = ? ORDER BY id DESC LIMIT 1""",
                (seconds, epoch(open_at)),
            ).fetchone()
            if previous is not None and tuple(previous) == tuple(prices):
                continue
            db.execute("""INSERT INTO candles(seconds, open_epoch, open, high, low, close, volume, batch)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (seconds, epoch(open_at), *prices, batch))
            revisions += int(previous is not None)
            inserted += 1
        db.execute("""INSERT INTO cursors VALUES (?, ?)
                      ON CONFLICT(seconds) DO UPDATE SET next_open = max(next_open, excluded.next_open)""",
                   (seconds, epoch(end)))
    return {"batch": batch, "accepted": len(normalized), "new_candles": inserted - revisions,
            "revisions": revisions, "raw_sha256": digest}


def bars(db, seconds: int, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    columns = ["open_epoch", *OHLCV, "revision_id", "batch", "received_at", "raw_sha256"]
    rows = db.execute("""
        SELECT c.open_epoch, c.open, c.high, c.low, c.close, c.volume, c.id, c.batch, r.received_at, r.sha
        FROM candles c JOIN responses r ON c.batch = r.id
        WHERE c.seconds = ? AND c.open_epoch >= ? AND c.open_epoch < ?
        AND c.id = (SELECT max(v.id) FROM candles v WHERE v.seconds = c.seconds AND v.open_epoch = c.open_epoch)
        ORDER BY c.open_epoch
    """, (seconds, epoch(start), epoch(end))).fetchall()
    frame = pd.DataFrame.from_records(rows, columns=columns)
    frame.index = pd.DatetimeIndex(pd.to_datetime(frame.pop("open_epoch"), unit="s", utc=True), name="open_at")
    frame["close_at"] = frame.index + pd.Timedelta(seconds=seconds)
    frame["received_at"] = pd.to_datetime(frame.received_at, utc=True, format="ISO8601")
    frame["source_id"] = SOURCE
    return frame


def get(db, key: str):
    row = db.execute("SELECT body FROM metadata WHERE key = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def set_meta(db, key: str, value) -> None:
    db.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)", (key, dump(value)))


def get_account(db, profile: str) -> dict:
    row = db.execute("SELECT body FROM accounts WHERE profile = ?", (profile,)).fetchone()
    return json.loads(row[0]) if row else {}


def set_account(db, profile: str, state: dict) -> None:
    db.execute("INSERT OR REPLACE INTO accounts VALUES (?, ?)", (profile, dump(state)))


def event(db, profile: str, key: str, kind: str, now: pd.Timestamp, body: dict) -> None:
    db.execute("INSERT INTO events(profile,key,kind,recorded_at,body) VALUES (?,?,?,?,?)",
               (profile, key, kind, now.isoformat(), dump(body)))


def events(db, profile: str, limit: int = 30, *, kind: str | None = None) -> list[dict]:
    return [{"kind": row["kind"], "recorded_at": row["recorded_at"], **json.loads(row["body"])}
            for row in db.execute(
                "SELECT * FROM events WHERE profile=? AND (? IS NULL OR kind=?) ORDER BY id DESC LIMIT ?",
                (profile, kind, kind, limit),
            )]


def feed_status(db) -> dict:
    out = {}
    for seconds in (300, 3600):
        cursor = db.execute("SELECT next_open FROM cursors WHERE seconds=?", (seconds,)).fetchone()
        last = db.execute("SELECT max(open_epoch) FROM candles WHERE seconds=?", (seconds,)).fetchone()[0]
        latest = db.execute("SELECT received_at,problem FROM responses WHERE seconds=? ORDER BY id DESC LIMIT 1",
                            (seconds,)).fetchone()
        out[str(seconds)] = {
            "scanned_through": from_epoch(cursor[0]) if cursor else None,
            "last_observed_close": from_epoch(last + seconds) if last is not None else None,
            "last_response_at": latest[0] if latest else None,
            "last_error": latest[1] if latest else None,
        }
    return out
