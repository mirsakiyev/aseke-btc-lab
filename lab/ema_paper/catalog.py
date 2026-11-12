"""Frozen EMA paper registrations and the independent journal."""
from contextlib import closing
from pathlib import Path
import json
import sqlite3

import pandas as pd

from lab.config import ROOT as PROJECT, fingerprint
from lab.learning.simulator import Contract
from lab.phase15 import paper
from lab.phase15.catalog import registered as phase15_registration
from lab.storage import RUNS, file_hash, utc_now, writer_lock
from lab.strategy_lab import ema_pullback, ema_registration
from lab.strategy_lab.evaluator import code_hash, compile_spec, test_version, tested
from lab.strategy_lab.registry import MODE, MODEL_VALIDATION, connect, get, source_content
from lab.strategy_lab.testing import signal_contract


ROOT = RUNS / "ema_paper"
PROFILES = ("day", "swing", "position")
VERSION = "ema-paper-independent-receipts-v1"
VERSION_PINS = {
    "position": "50a8b7c23ab5f428568c1d38bb5c8b37d23336b7f369096163429ed48dc3ada1",
    "swing": "f41153a53e627ee799940bdf007f84c5d0d142aaa68a929a074555004e3db4d4",
}
BENCHMARKS = {"day": ["day/fixed"], "swing": ["swing/fixed", "swing/trail"], "position": ["position/fixed"]}
ENTRY_SECONDS = {"day": 900, "swing": 14400, "position": 3600}
SOURCE_POLICY = {"scope": "EMA_PAPER_PROSPECTIVE_ONLY", "post_registration_only": True,
                 "decision_receipt_seconds": 120, "publication_max_seconds": 360,
                 "price_receipt_seconds": 1020, "funding_receipt_seconds": 7200,
                 "modeled_liquidation_fee": .003,
                 "handoff": "Independent public KuCoin first receipts with an explicit book-bound permission grant"}
LIMITS = {"min_available_mib": 1250, "min_free_disk_mib": 2048, "max_ledger_mib": 128,
          "worker_rss_mib": 640, "cycle_seconds": 45, "minutes_per_book": 60, "threads": 1}
SCHEMA = """
CREATE TABLE IF NOT EXISTS registration(profile TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS book(id TEXT PRIMARY KEY, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS event(key TEXT PRIMARY KEY, book TEXT NOT NULL, kind TEXT NOT NULL,
    idea TEXT NOT NULL, source_at TEXT NOT NULL, recorded_at TEXT NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS book_events ON event(book,kind,recorded_at);
CREATE TABLE IF NOT EXISTS readiness(book TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS eligibility(book TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS equity_sample(book TEXT NOT NULL, source_at TEXT NOT NULL, recorded_at TEXT NOT NULL,
    equity REAL NOT NULL, exposure REAL NOT NULL, PRIMARY KEY(book,source_at));
CREATE TRIGGER IF NOT EXISTS registration_frozen_update BEFORE UPDATE ON registration
    BEGIN SELECT RAISE(ABORT,'immutable registration'); END;
CREATE TRIGGER IF NOT EXISTS registration_frozen_delete BEFORE DELETE ON registration
    BEGIN SELECT RAISE(ABORT,'immutable registration'); END;
CREATE TRIGGER IF NOT EXISTS eligibility_frozen_update BEFORE UPDATE ON eligibility
    BEGIN SELECT RAISE(ABORT,'immutable eligibility'); END;
CREATE TRIGGER IF NOT EXISTS eligibility_frozen_delete BEFORE DELETE ON eligibility
    BEGIN SELECT RAISE(ABORT,'immutable eligibility'); END;
CREATE TRIGGER IF NOT EXISTS event_frozen_update BEFORE UPDATE ON event
    BEGIN SELECT RAISE(ABORT,'append-only EMA event'); END;
CREATE TRIGGER IF NOT EXISTS event_frozen_delete BEFORE DELETE ON event
    BEGIN SELECT RAISE(ABORT,'append-only EMA event'); END;
"""


def db_open(readonly=False, *, root=None):
    path = (ROOT if root is None else Path(root)) / "paper.sqlite3"
    if readonly:
        return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=3)
    db.executescript(SCHEMA)
    return db


def registrations(db=None, *, root=None):
    if db is None:
        if not ((ROOT if root is None else Path(root)) / "paper.sqlite3").exists():
            return {}
        with closing(db_open(True, root=root)) as reader:
            return registrations(reader)
    result = {}
    for profile, identifier, payload in db.execute("SELECT profile,id,body FROM registration ORDER BY profile"):
        body = json.loads(payload)
        if fingerprint(body) != identifier or body["profile"] != profile:
            raise ValueError("EMA prospective registration checksum mismatch")
        result[profile] = {"id": identifier, "body": body}
    return result


def code_pins():
    paths = (
        "lab/data.py", "lab/features.py", "lab/execution.py", "lab/learning/simulator.py",
        "lab/phase15/paper.py", "lab/strategy_lab/evaluator.py", "lab/strategy_lab/specification.py",
        "lab/strategy_lab/ema_pullback.py", "lab/strategy_lab/ema_execution.py",
        "lab/ema_paper/catalog.py", "lab/ema_paper/ledger.py", "lab/ema_paper/source.py", "lab/ema_paper/worker.py",
    )
    return {path: file_hash(PROJECT / path) for path in paths}


def initial_status(lane):
    return {"state": "BLOCKED_PERMISSION", "reason": "No independent forward receipt grant bound to this EMA book",
            "warmup": {"completed": 0, "required": ema_pullback.WARMUP_BARS},
            "source_ready_at": None, "first_eligible_at": None, "primary_end_exclusive": None,
            "last_evaluated_at": None, "next_evaluation_at": None, "last_signal": None}


def _register_body(record, operator, now, pins, comparison):
    spec = record["spec"]
    profile = spec["profile"]
    decision = int(pd.Timedelta(spec["timeframe"]).total_seconds())
    hold = spec["session"]["max_hold_minutes"]
    if (record["strategy_id"] != f"{ema_pullback.FAMILY}/{profile}"
            or spec["session"] != {"timezone": "UTC", "max_hold_minutes": {"day": 480, "swing": 10080, "position": 43200}[profile]}
            or spec["risk"]["starting_equity"] != 10000
            or spec["risk"]["leverage"] != (1 if profile == "position" else 3)):
        raise ValueError("EMA paper variant differs from the authorized session, capital or leverage")
    return {
        "policy": VERSION, "profile": profile, "book_id": fingerprint([VERSION, record["hash"], now.isoformat()]),
        "strategy_hash": record["hash"], "strategy_version": spec["implementation_version"],
        "registered_at": now.isoformat(), "spec": spec, "risk": spec["risk"], "cost": spec["cost"],
        "market": spec["market"], "instrument": spec["instrument"],
        "source": {**SOURCE_POLICY, "warmup_bars": ema_pullback.WARMUP_BARS},
        "execution_clock": {"decision_seconds": decision, "entry_seconds": ENTRY_SECONDS[profile], "protection_seconds": 60},
        "session": "24/7 UTC", "horizon_days": 90, "follow_through_minutes": hold + 120,
        "min_complete_trades": 25, "benchmark": BENCHMARKS[profile],
        "benchmark_registration": comparison["id"],
        "benchmark_books": {key: comparison["body"]["lanes"][key]["book_id"] for key in BENCHMARKS[profile]},
        "start_rule": "First strictly future UTC decision boundary after qualified source readiness is recorded",
        "end_rule": "First eligible decision plus 90 calendar days, exclusive. Entry opens must precede that end.",
        "permissions": {"operator": operator, "paper_only": True, "telegram_lifecycle": True,
                        "live_orders": False, "historical_labels": False, "protected_sources": False},
        "mode": MODE, "model_validation": MODEL_VALIDATION, "code_pins": pins, "limits": LIMITS,
    }


def register_books(operator, *, root=None):
    if not operator.strip():
        raise ValueError("Record the operator who authorized the three paper books")
    root = ROOT if root is None else Path(root)
    with writer_lock(root / ".cycle.lock"), closing(db_open(root=root)) as db:
        existing = registrations(db)
        if existing:
            if set(existing) != set(PROFILES):
                raise ValueError("Incomplete EMA registration group")
            return existing
        pins, now = code_pins(), utc_now()
        comparison = phase15_registration()
        if not comparison:
            raise ValueError("The existing frozen Phase 15 comparison registration is required")
        # Registry changes concern only the new Day snapshot. Prior strategy
        # versions are read and fixture-checked without changing lifecycle rows.
        with connect(write=True) as registry:
            day = ema_registration.register_ema_day(registry)
            day = day if tested(day, code_hash()) else test_version(registry, day["hash"])
            rows = {"day": day, **{name: get(registry, key) for name, key in VERSION_PINS.items()}}
            for row in rows.values():
                spec = row["spec"]
                profile, *_ = compile_spec(spec)
                signal_contract(spec, profile)
                for filename, key in ema_registration.ARTIFACTS:
                    if source_content(registry, key) != (ema_registration.SOURCE_DIR / filename).read_bytes():
                        raise ValueError("Frozen source artifact differs from registry bytes")
        with db:
            for name in PROFILES:
                lane = _register_body(rows[name], operator.strip(), now, pins, comparison)
                db.execute("INSERT INTO registration VALUES(?,?,?)",
                           (name, fingerprint(lane), paper.encode(lane)))
                book = paper.initial(lane)
                paper.save(db, book)
                db.execute("INSERT INTO readiness VALUES(?,?)", (book["book_id"], paper.encode(initial_status(lane))))
                db.execute("INSERT INTO equity_sample VALUES(?,?,?,?,?)",
                           (book["book_id"], now.isoformat(), now.isoformat(), book["equity"], 0.))
        return registrations(db)
