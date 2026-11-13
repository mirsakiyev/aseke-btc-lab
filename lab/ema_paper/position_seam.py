"""Append and run a future daily seam for the original EMA Position book."""

from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sqlite3
from tempfile import TemporaryDirectory
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.config import fingerprint
from lab.ema_paper import catalog, chart_recovery, kraken_reference, recovery_protocol, recovery_runtime
from lab.ema_paper import recovery_source, synthetic_protocol, synthetic_runtime
from lab.kraken_paper import source
from lab.phase15 import paper
from lab.storage import utc_now, writer_lock
from lab.strategy_lab.market_data import Purpose


VERSION = "EMA_KRAKEN_POSITION_SEAM_V1"
FILENAME = "position-seam.sqlite3"
DAY = pd.Timedelta(days=1)
SCHEMA = """
CREATE TABLE amendment(id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TRIGGER immutable_update BEFORE UPDATE ON amendment
    BEGIN SELECT RAISE(ABORT, 'immutable EMA Position seam amendment'); END;
CREATE TRIGGER immutable_delete BEFORE DELETE ON amendment
    BEGIN SELECT RAISE(ABORT, 'immutable EMA Position seam amendment'); END;
"""


def _path(root):
    return (synthetic_protocol.ROOT if root is None else Path(root)) / FILENAME


def active(*, root=None):
    return _path(root).is_file()


def _record(original, predecessor, at):
    scopes = {grant.purpose: grant for grant in map(synthetic_protocol._grant, predecessor["body"]["grants"])}
    warmup = scopes[Purpose.WARMUP]
    replacement = synthetic_protocol._lane(original, predecessor, warmup.source, at)
    mutable = {"predecessor_amendment_id", "seed_end", "registered_at", "grants"}
    if ({key: value for key, value in replacement["body"].items() if key not in mutable}
            != {key: value for key, value in predecessor["body"].items() if key not in mutable}):
        raise PermissionError("A future seam may only change Position clocks and their scoped grants")
    if (original["body"]["profile"] != "position"
            or original["body"]["execution_clock"]["decision_seconds"] != DAY.total_seconds()
            or synthetic_protocol.utc(replacement["body"]["seed_end"]) <= max(at, warmup.end)):
        raise PermissionError("Position replacement must name a new prospective UTC daily seam")
    return replacement


def _document(original, synthetic, recovery, now):
    prior = synthetic["body"]["books"]["position"]
    return {"version": VERSION, "recorded_at": now.isoformat(), "synthetic": synthetic["id"],
            "chart_recovery": recovery["id"], "source_id": synthetic["body"]["source_id"],
            "original_seam": prior["body"]["seed_end"], "position": _record(original, prior, now),
            "runtime_sha256": sha256(Path(__file__).read_text(encoding="utf-8").encode("utf-8")).hexdigest()}


def load(synthetic, recovery, original, *, root=None, now=None):
    path = _path(root)
    now = utc_now() if now is None else synthetic_protocol.utc(now)
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        rows = db.execute("SELECT id,body FROM amendment").fetchall()
    if len(rows) != 1:
        raise PermissionError("Exactly one frozen EMA Position seam amendment is required")
    identifier, raw = rows[0]
    body = json.loads(raw)
    recorded = synthetic_protocol.utc(body["recorded_at"])
    if (fingerprint(body) != identifier or not synthetic_protocol.utc(recovery["body"]["recorded_at"]) <= recorded <= now
            or body != _document(original, synthetic, recovery, recorded)):
        raise PermissionError("Position seam differs from its immutable book, predecessors, or source")
    return {"id": identifier, "body": body}


def register(*, root=None, paper_root=None, now=None):
    """Record a successor without changing the original ledger or either predecessor."""
    path = _path(root)
    paper_root = catalog.ROOT if paper_root is None else Path(paper_root)
    with writer_lock(path.parent / ".position-register.lock"), writer_lock(paper_root / ".cycle.lock"):
        now = utc_now() if now is None else synthetic_protocol.utc(now)
        synthetic = synthetic_protocol.load(root=root, now=now)
        recovery = recovery_protocol.load(synthetic, root=root, now=now)
        with closing(catalog.db_open(readonly=True, root=paper_root)) as db:
            original = catalog.registrations(db)["position"]
            if path.is_file():
                return load(synthetic, recovery, original, root=root, now=now)
            lane = original["body"]
            for table in ("event", "eligibility", "synthetic_seed", "chart_recovery_event"):
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                    if db.execute(f"SELECT 1 FROM {table} WHERE book=? LIMIT 1", (lane["book_id"],)).fetchone():
                        raise PermissionError("The original Position book already has prospective evidence")
            if not db.execute("SELECT 1 FROM book WHERE id=?", (lane["book_id"],)).fetchone():
                raise PermissionError("The original Position account is missing")
            if paper.load(db, lane) != paper.initial(lane):
                raise PermissionError("The original Position account is outside the unused-book scope")
        body = _document(original, synthetic, recovery, now)
        identifier = fingerprint(body)
        with TemporaryDirectory(dir=path.parent) as scratch:
            temporary = Path(scratch) / FILENAME
            with closing(sqlite3.connect(temporary)) as db, db:
                db.executescript(SCHEMA)
                db.execute("INSERT INTO amendment VALUES (?,?)", (identifier, json.dumps(body, sort_keys=True)))
            temporary.replace(path)
    return {"id": identifier, "body": body}


def prepare_history(*, root=None, paper_root=None, source_root=None):
    """Fetch only the originally missed seam as historical feature context."""
    now = utc_now()
    synthetic = synthetic_protocol.load(root=root, now=now)
    recovery = recovery_protocol.load(synthetic, root=root, now=now)
    original = catalog.registrations(root=paper_root)["position"]
    amendment = load(synthetic, recovery, original, root=root, now=now)
    body = amendment["body"]
    source_root = kraken_reference.ROOT if source_root is None else kraken_reference._root(source_root)
    proof = kraken_reference.qualification(root=source_root)
    if not proof or proof["id"] != synthetic["body"]["qualification_id"]:
        raise PermissionError("Position historical context requires the frozen Kraken qualification")
    scope = kraken_reference._qualified_permission(proof, now)
    record = body["position"]
    warmup = next(grant for grant in map(synthetic_protocol._grant, record["body"]["grants"])
                  if grant.purpose == Purpose.WARMUP)
    end = synthetic_protocol.utc(body["original_seam"])
    start = end - DAY
    if scope.source.id != warmup.source.id or not warmup.start <= start < end < warmup.end:
        raise PermissionError("Historical Position repair is outside the successor's feature grant")
    with source._writer(source_root / "source") as db:
        return {**recovery_source._repair_one(db, scope, start, end, DAY, rate_bound=True),
                "purpose": Purpose.WARMUP, "open_at": start.isoformat(), "close_at": end.isoformat()}


def cycle(*, now=None, root=None, protocol_root=None, source_root=None):
    """Reuse the frozen recovery and execution paths with only Position's scopes replaced."""
    if not active(root=protocol_root):
        return recovery_runtime.cycle(now=now, root=root, protocol_root=protocol_root, source_root=source_root)
    root = catalog.ROOT if root is None else Path(root)
    now = utc_now() if now is None else synthetic_protocol.utc(now)
    synthetic = synthetic_protocol.load(root=protocol_root, now=now)
    recovery = recovery_protocol.load(synthetic, root=protocol_root, now=now)
    source_root = kraken_reference.ROOT if source_root is None else Path(source_root)
    process, limits, unit = psutil.Process(), catalog.LIMITS, synthetic_runtime.MIB
    if (psutil.virtual_memory().available < limits["min_available_mib"] * unit
            or shutil.disk_usage(root).free < limits["min_free_disk_mib"] * unit
            or (root / "paper.sqlite3").stat().st_size >= limits["max_ledger_mib"] * unit
            or process.memory_info().rss >= limits["worker_rss_mib"] * unit):
        return {"state": "WAITING_FOR_RESOURCES", "synthetic": True}
    started = time.monotonic()
    deadline = started + limits["cycle_seconds"]
    with writer_lock(root / ".cycle.lock"), closing(catalog.db_open(root=root)) as db, threadpool_limits(limits=1):
        db.executescript(synthetic_runtime.SCHEMA)
        chart_recovery.install_schema(db)
        originals = catalog.registrations(db)
        records = dict(synthetic["body"]["books"])
        amendment = None
        try:
            amendment = load(synthetic, recovery, originals["position"], root=protocol_root, now=now)
            records["position"] = amendment["body"]["position"]
        except (OSError, ValueError, KeyError, sqlite3.Error):
            pass
        effective = {**synthetic, "body": {**synthetic["body"], "books": records}}
        grants = synthetic_protocol.grants(effective)
        data = kraken_reference.market_data(grants, root=source_root)
        results = {}
        # Keep existing evaluations and protective execution ahead of the new readiness check.
        for profile in ("day", "swing", "position"):
            if profile == "position" and amendment is None:
                results[profile] = {"state": "BLOCKED_PERMISSION", "reason": "Position seam amendment could not be verified"}
                continue
            if time.monotonic() >= deadline:
                results[profile] = {"state": "WAITING_FOR_RESOURCES"}
                continue
            scopes = {grant.purpose: grant for grant in grants if grant.book_id == originals[profile]["body"]["book_id"]}
            with db:
                results[profile] = recovery_runtime._book(db, originals[profile], records[profile],
                                                          recovery, scopes, data, now, deadline)
    repairs = None
    if time.monotonic() + 8 < deadline:
        repairs = recovery_source.repair_pending(results, grants, root=source_root, now=now)
    return {"state": "CHECKED", "synthetic": True, "revision_id": synthetic["id"], "recovery_amendment_id": recovery["id"],
            "position_seam_amendment_id": amendment["id"] if amendment else None,
            "books": results, "recovery_source": repairs, "wall_seconds": round(time.monotonic() - started, 3),
            "rss_mib": round(process.memory_info().rss / unit, 1)}


def protocol_status(*, registrations=None, runtime_books=None):
    """Expose the effective Position clock in the same views as the original books."""
    statuses = synthetic_runtime.protocol_status(registrations=registrations, runtime_books=runtime_books)
    if not active():
        return statuses
    from lab.ema_paper import worker

    registrations = catalog.registrations() if registrations is None else registrations
    runtime_books = worker.status()["books"] if runtime_books is None else runtime_books
    if "position" not in statuses or "position" not in registrations:
        return statuses
    try:
        synthetic = synthetic_protocol.load()
        recovery = recovery_protocol.load(synthetic)
        amendment = load(synthetic, recovery, registrations["position"])
    except (OSError, ValueError, KeyError, sqlite3.Error):
        statuses["position"].update(state="BLOCKED_PERMISSION", reason="Position seam amendment could not be verified")
        return statuses
    original = registrations["position"]["body"]
    record = amendment["body"]["position"]
    seam = synthetic_protocol.utc(record["body"]["seed_end"])
    _, processing = worker._clocks(original)
    runtime = runtime_books.get("position", {})
    detail = runtime.get("source_protocol") or {}
    if detail.get("amendment_id") != record["id"]:
        detail = {}
    statuses["position"].update(
        **synthetic_runtime._status_detail(record, original),
        seam=seam.isoformat(), state="WAITING_FOR_DATA", reason="Waiting for the new prospective Kraken daily seam",
        next_evaluation_at=(seam + processing).isoformat(), first_eligible_at=None)
    statuses["position"].update(detail, position_seam_amendment_id=amendment["id"],
                                earliest_possible_decision_at=(seam + DAY).isoformat())
    return statuses
