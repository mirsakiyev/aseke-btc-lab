"""Retirement of the three Kraken Spot paper books, with an immutable audit."""

from contextlib import closing
import json
from pathlib import Path

import psutil

from lab.config import fingerprint
from lab.kraken_paper import catalog
from lab.learning.control import process_identity
from lab.phase15.paper import encode
from lab.storage import read_json, utc, utc_now, writer_lock


STATE = "RETIRED"
REASON = "Operator retired the extra Kraken Spot paper accounts and retained Kraken for feature warmup."


def audit(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='retirement'").fetchone():
        return {}
    result = {}
    for key, identifier, payload in db.execute("SELECT book,id,body FROM retirement ORDER BY book"):
        body = json.loads(payload)
        if fingerprint(body) != identifier or body["book_id"] != key or body["profile"] in result:
            raise ValueError("Kraken paper retirement checksum failed")
        result[body["profile"]] = {**body, "retirement_id": identifier}
    return result


def history(*, root=None):
    root = catalog.ROOT if root is None else Path(root)
    if not (root / "paper.sqlite3").is_file():
        return {}
    with closing(catalog.db_open(True, root=root)) as db:
        return audit(db)


def _guard_ledger(db):
    for table, column, actions in (
        ("book", "id", ("INSERT", "UPDATE", "DELETE")),
        ("event", "book", ("INSERT",)),
        ("readiness", "book", ("INSERT", "UPDATE", "DELETE")),
        ("eligibility", "book", ("INSERT",)),
        ("equity_sample", "book", ("INSERT", "UPDATE", "DELETE")),
        ("warmup", "book", ("INSERT",)),
    ):
        for action in actions:
            trigger = f"{table}_retired_{action}"
            rows = ("OLD", "NEW") if action == "UPDATE" else ("OLD" if action == "DELETE" else "NEW",)
            protected_keys = ",".join(f"{row}.{column}" for row in rows)
            db.execute(f"DROP TRIGGER IF EXISTS {trigger}")
            db.execute(f"CREATE TRIGGER {trigger} BEFORE {action} ON {table} "
                       f"WHEN EXISTS (SELECT 1 FROM retirement WHERE book IN ({protected_keys})) "
                       "BEGIN SELECT RAISE(ABORT,'retired Kraken paper book'); END")


def _seal_retirement(db):
    sealed = db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='retirement_frozen_insert'").fetchone()
    if not sealed:
        _guard_ledger(db)
        db.execute("CREATE TRIGGER retirement_frozen_insert BEFORE INSERT ON retirement "
                   "BEGIN SELECT RAISE(ABORT,'immutable Kraken paper retirement'); END")


def _proof(db, profile, registration, retired_at):
    lane = registration["body"]
    key = lane["book_id"]
    saved = db.execute("SELECT state FROM book WHERE id=?", (key,)).fetchone()
    if not saved:
        raise ValueError(f"Missing Kraken {profile} ledger")
    account = json.loads(saved[0])
    events = db.execute("SELECT key,kind,idea,source_at,recorded_at,body FROM event WHERE book=? ORDER BY rowid", (key,)).fetchall()
    kinds = set()
    published, unresolved = set(), set()
    for _, kind, idea, *_ in events:
        kinds.add(kind)
        if kind == "PUBLISHED":
            published.add(idea)
        elif kind in {"EXPIRED", "INVALIDATED"}:
            published.discard(idea)
        elif kind == "UNRESOLVED":
            unresolved.add(idea)
        elif kind == "EVIDENCE_RECOVERED":
            unresolved.discard(idea)
    if (any(account[name] is not None for name in ("position", "pending", "problem"))
            or account["permanent_block"] or published or unresolved
            or kinds & {"HYPOTHETICAL_FILL", "ENTRY_OBSERVED", "CLOSED"}):
        raise ValueError(f"Kraken {profile} has a position, fill or unresolved lifecycle")
    readiness = db.execute("SELECT body FROM readiness WHERE book=?", (key,)).fetchone()
    eligibility = db.execute("SELECT body FROM eligibility WHERE book=?", (key,)).fetchone()
    return {"state": STATE, "reason": REASON, "retired_at": retired_at, "profile": profile,
            "registration_id": registration["id"], "book_id": key, "strategy_hash": lane["strategy_hash"],
            "code_pins_hash": fingerprint(lane["code_pins"]), "authorization_id": lane["authorization_id"],
            "starting_equity": lane["risk"]["starting_equity"], "final_account": account,
            "event_count": len(events), "event_hash": fingerprint(events),
            "prior_readiness": json.loads(readiness[0]) if readiness else None,
            "prior_eligibility": json.loads(eligibility[0]) if eligibility else None}


def retire(*, root=None, now=None):
    root = catalog.ROOT if root is None else Path(root)
    if not (root / "paper.sqlite3").is_file():
        raise ValueError("Kraken paper registrations are missing")
    with (writer_lock(root / ".cycle.lock"), writer_lock(root / ".producer-dispatch.lock"),
          closing(catalog.db_open(root=root)) as db, db):
        db.execute("BEGIN IMMEDIATE")
        existing = audit(db)
        if existing:
            if set(existing) != set(catalog.PROFILES):
                raise ValueError("Incomplete Kraken retirement audit")
            _seal_retirement(db)
            return existing
        registrations = catalog.registrations(db)
        if set(registrations) != set(catalog.PROFILES):
            raise ValueError("Exactly the three Kraken EMA registrations are required")
        keys = {row["body"]["book_id"] for row in registrations.values()}
        if keys != {row[0] for row in db.execute("SELECT id FROM book")} or len(keys) != len(registrations):
            raise ValueError("Kraken paper registrations and account ledgers differ")
        stamp = (utc_now() if now is None else utc(now)).isoformat()
        records = [_proof(db, profile, row, stamp) for profile, row in registrations.items()]
        db.execute("CREATE TABLE retirement(book TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, body TEXT NOT NULL)")
        for action in ("UPDATE", "DELETE"):
            db.execute(f"CREATE TRIGGER retirement_frozen_{action} BEFORE {action} ON retirement "
                       "BEGIN SELECT RAISE(ABORT,'immutable Kraken paper retirement'); END")
        db.executemany("INSERT INTO retirement VALUES (?,?,?)",
                       [(row["book_id"], fingerprint(row), encode(row)) for row in records])
        _seal_retirement(db)
        return audit(db)


def stop_producer(*, root=None):
    root = catalog.ROOT if root is None else Path(root)
    if set(history(root=root)) != set(catalog.PROFILES):
        raise ValueError("All three Kraken paper books must be retired first")
    path = root / "producer-process.json"
    if not path.is_file():
        return {"state": "NOT_STARTED"}
    record = read_json(path)
    process = process_identity(record.get("pid"), record.get("created"))
    if process is None:
        return {"state": "STOPPED", "pid": record.get("pid")}
    try:
        command = process.cmdline()
        expected = ["-m", "lab.kraken_paper", "_produce", "--root", str(root.resolve())]
        if command[-len(expected):] != expected:
            raise ValueError("Recorded Kraken producer command or root does not match")
        process.terminate()
        process.wait(timeout=5)
    except psutil.NoSuchProcess:
        pass
    return {"state": "STOPPED", "pid": record["pid"]}
