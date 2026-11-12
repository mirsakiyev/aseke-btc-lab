"""Immutable, consent-bound USD Spot paper registrations and account initialization."""

from contextlib import closing
from dataclasses import asdict
from hashlib import sha256
from math import isfinite
from pathlib import Path
import json
import sqlite3

import pandas as pd

from lab.config import ORIGIN, ROOT as PROJECT, fingerprint
from lab.ema_paper import catalog as frozen_catalog
from lab.kraken_paper import authorization
from lab.phase15 import paper
from lab.storage import RUNS, file_hash, utc_now, writer_lock
from lab.strategy_lab.market_data import ObservationKind, ProvenanceClass, Purpose, SourceGrant, SourceIdentity
from lab.strategy_lab.registry import MODE, MODEL_VALIDATION, digest
from lab.strategy_lab.spot_ema_research import proposals


ROOT = RUNS / "kraken_paper"
POLICY = "kraken-btc-usd-ema-spot-paper-v1"
PROFILES = frozen_catalog.PROFILES
LIMITS = dict(frozen_catalog.LIMITS)
PAPER_COST = {
    "id": "kraken-spot-tier1-taker-adverse-10bp-v1", "maker": .004, "taker": .008, "adverse_proxy": .001,
    "basis": "PUBLISHED_UNDISCOUNTED_TIER_1_TAKER_BOTH_SIDES",
    "checked_utc_date": "2026-11-12", "reference": "https://www.kraken.com/pro/fees",
}
PAIR_PATH = "https://api.kraken.com/0/public/AssetPairs"
PAIR_PARAMS = {"pair": "XBTUSD", "country_code": "CA"}
EXTRA_SCHEMA = """
CREATE TABLE IF NOT EXISTS authorization(id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_grant(id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS warmup(book TEXT PRIMARY KEY, body TEXT NOT NULL);
"""


def db_open(readonly=False, *, root=None):
    db = frozen_catalog.db_open(readonly, root=ROOT if root is None else root)
    if not readonly:
        db.executescript(EXTRA_SCHEMA)
        for table in ("authorization", "source_grant", "warmup"):
            for action in ("UPDATE", "DELETE"):
                db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_frozen_{action} BEFORE {action} ON {table} "
                           "BEGIN SELECT RAISE(ABORT,'immutable Kraken paper evidence'); END")
    return db


def registrations(db=None, *, root=None):
    return frozen_catalog.registrations(db, root=ROOT if root is None else root)


def code_pins():
    paths = (
        "lab/kraken_paper/authorization.py", "lab/kraken_paper/catalog.py", "lab/kraken_paper/source.py",
        "lab/kraken_paper/worker.py", "lab/kraken_paper/managed.py",
        "lab/phase15/paper.py", "lab/ema_paper/catalog.py", "lab/ema_paper/ledger.py",
        "lab/learning/simulator.py", "lab/execution.py", "lab/data.py", "lab/features.py",
        "lab/strategy_lab/market_data.py", "lab/strategy_lab/spot_sources.py", "lab/strategy_lab/feature_warmup.py",
        "lab/strategy_lab/technical_features.py", "lab/strategy_lab/ema_pullback.py", "lab/strategy_lab/evaluator.py",
        "lab/strategy_lab/ema_execution.py", "lab/strategy_lab/spot_ema_research.py",
    )
    return {path: file_hash(PROJECT / path) for path in paths}


def strategy_specs():
    """Prepare deterministic, venue-specific specifications without opening a ledger."""
    records = {}
    for profile, draft in proposals("kraken").items():
        body = draft["body"]
        chart = body["execution"]["decision_seconds"]
        spec = {
            "strategy_id": body["strategy_id"], "family_id": body["recipe"]["entry"], "profile": profile,
            "implementation_version": POLICY, "venue": "Kraken", "market": "SPOT", "instrument": "BTC-USD",
            "quote": "USD", "timeframe": {900: "15min", 14400: "4h", 86400: "1D"}[chart],
            "sides": ["long"], "short_handling": "SIGNAL_ONLY", "recipe": body["recipe"], "context": [],
            "session": dict(body["session"]),
            "risk": {**body["risk"], "starting_equity": 10000},
            "data": {"native_seconds": chart, "warmup_bars": 200, "maximum_seed_bars": 720,
                     "historical_use": "FEATURE_INITIALIZATION_ONLY"},
            "execution": {**body["execution"], "funding_reserve_bps": 0, "version": POLICY,
                          "intrabar_ambiguity": "UNRESOLVED"},
            "cost": dict(PAPER_COST),
            "comparison": {"policy": "MATCHED_WINDOW_KRAKEN_BTC_USD_SPOT_ONLY", "benchmark_id": None},
        }
        records[profile] = {"id": spec["strategy_id"], "hash": digest(spec), "spec": spec}
    return records


def instrument_terms(raw, received_at, now):
    """Validate captured public Canadian AssetPairs terms for the exact USD Spot product."""
    if type(raw) is not bytes or not 0 < len(raw) <= authorization.MAX_DOCUMENT:
        raise ValueError("Invalid public AssetPairs receipt size")
    received, now = map(authorization.utc, (received_at, now))
    if not now - pd.Timedelta(days=1) < received <= now:
        raise ValueError("A current, causal Kraken AssetPairs receipt is required")
    payload = json.loads(raw)
    if payload.get("error") != [] or set(payload.get("result", {})) != {"XXBTZUSD"}:
        raise ValueError("AssetPairs response lacks the exact Kraken BTC/USD pair")
    item = payload["result"]["XXBTZUSD"]
    if any(item.get(k) != v for k, v in {"altname": "XBTUSD", "wsname": "XBT/USD", "base": "XXBT",
                                          "quote": "ZUSD", "status": "online"}.items()):
        raise ValueError("Kraken BTC/USD Spot terms are unavailable or unsupported")
    decimals = item["lot_decimals"]
    if type(decimals) is not int or not 0 <= decimals <= 12:
        raise ValueError("Invalid Kraken BTC quantity precision")
    fields = {name: float(item[key]) for name, key in (("price_tick", "tick_size"),
                                                      ("min_quantity", "ordermin"), ("min_quote", "costmin"))}
    if not all(isfinite(value) and value > 0 for value in fields.values()):
        raise ValueError("Kraken Spot price tick and minimums must be positive")
    return {**fields, "quantity_step": 10. ** -decimals, "raw_sha256": sha256(raw).hexdigest(),
            "received_at": received.isoformat()}


def _source(record):
    return SourceIdentity("Kraken", "kraken-btc-usd-public-ohlc-v1", "BTC-USD", "SPOT",
                          record["evidence_reference"], authorization.evidence_id(record), ProvenanceClass.INDEPENDENT)


def approved_authorization(db, *, at):
    from lab.kraken_paper import retirement

    if retirement.audit(db):
        raise PermissionError("Kraken Spot paper books are retired")
    records = db.execute("SELECT id,body FROM authorization").fetchall()
    if len(records) != 1:
        raise PermissionError("Exactly one verified Kraken permission record is required")
    key, payload = records[0]
    record = json.loads(payload)
    if authorization.evidence_id(record) != key:
        raise PermissionError("Kraken authorization checksum failed")
    authorization.validate(record, at)
    existing = registrations(db)
    pins = code_pins()
    if set(existing) != set(PROFILES) or any(
        lane["body"]["authorization_id"] != key or lane["body"]["code_pins"] != pins
        for lane in existing.values()
    ):
        raise PermissionError("Kraken registrations or their code pins differ from the authorized group")
    return record


def grants(db):
    loaded = []
    for key, payload in db.execute("SELECT id,body FROM source_grant ORDER BY id"):
        body = json.loads(payload)
        src = body.pop("source")
        identity = SourceIdentity(**src)
        grant = SourceGrant(source=identity, **body)
        if grant.id != key:
            raise PermissionError("Kraken source grant checksum failed")
        loaded.append(grant)
    by_book = {row["body"]["book_id"]: row["body"] for row in registrations(db).values()}
    expected = {lane["source"][field] for lane in by_book.values()
                for field in ("warmup_grant_id", "forward_grant_id", "execution_grant_id")}
    if {grant.id for grant in loaded} != expected or any(
        grant.source.id != by_book[grant.book_id]["source"]["source_id"] for grant in loaded
    ):
        raise PermissionError("Source grants differ from the frozen Kraken paper books")
    return tuple(loaded)


def _grant_body(grant):
    body = asdict(grant)
    body.pop("id")
    body["source"].pop("id")
    body["kinds"] = sorted(grant.kinds)
    for field in ("start", "end", "approved_at", "registered_at"):
        body[field] = getattr(grant, field).isoformat()
    return body


def _prepare(record, terms, now, pins):
    source = _source(record)
    for profile, entry in strategy_specs().items():
        spec = entry["spec"]
        clock = {key: spec["execution"][key] for key in ("decision_seconds", "entry_seconds", "protection_seconds")}
        step = pd.Timedelta(seconds=clock["decision_seconds"])
        seam = ORIGIN + (now + pd.Timedelta(minutes=2) - ORIGIN).floor(step) + step
        follow = spec["session"]["max_hold_minutes"] + 120
        grant_end = seam + 3 * step + pd.Timedelta(days=90, minutes=follow)
        if record.get("expires_at") and authorization.utc(record["expires_at"]) < grant_end:
            raise PermissionError("Kraken authorization expires before the study and follow-through can end")
        book = fingerprint([POLICY, entry["hash"], now.isoformat(), source.id])
        scopes = {}
        for purpose, start, end in (
            (Purpose.WARMUP, seam - 720 * step, seam),
            (Purpose.FORWARD_DECISION, seam, grant_end),
            (Purpose.EXECUTION_EVIDENCE, seam, grant_end),
        ):
            scopes[purpose] = SourceGrant(source, book, purpose, frozenset({ObservationKind.TRADE_OHLCV}),
                                          start, end, now, now, record["evidence_reference"])
        lane = {
            "policy": POLICY, "profile": profile, "book_id": book, "strategy_hash": entry["hash"],
            "strategy_version": POLICY, "registered_at": now.isoformat(), "spec": spec,
            "venue": "Kraken", "market": "SPOT", "instrument": "BTC-USD", "currency": "USD",
            "risk": spec["risk"], "cost": spec["cost"], "instrument_terms": terms,
            "execution_clock": clock, "session": "24/7 UTC", "horizon_days": 90,
            "follow_through_minutes": follow, "min_complete_trades": 25, "comparison": spec["comparison"],
            "authorization_id": authorization.evidence_id(record), "limits": LIMITS, "code_pins": pins,
            "mode": MODE, "model_validation": MODEL_VALIDATION,
            "permissions": {"paper_only": True, "live_orders": False, "telegram_lifecycle": False,
                            "historical_labels": False, "protected_sources": False},
            "source": {
                "source_id": source.id, "warmup_grant_id": scopes[Purpose.WARMUP].id,
                "forward_grant_id": scopes[Purpose.FORWARD_DECISION].id,
                "execution_grant_id": scopes[Purpose.EXECUTION_EVIDENCE].id,
                "seed_end": seam.isoformat(), "history_seconds": 720 * clock["decision_seconds"],
                "native_seconds": clock["decision_seconds"], "warmup_bars": 200,
                "decision_receipt_seconds": 120, "publication_max_seconds": 360, "price_receipt_seconds": 120,
            },
        }
        yield lane, tuple(scopes.values())


def register_books(review_path, raw_pair, pair_received_at, *, now=None, root=None):
    """Atomically record reviewed consent, source grants and three new USD accounts."""
    root = ROOT if root is None else Path(root)
    now = authorization.utc(utc_now() if now is None else now)
    record = authorization.read_review(review_path, at=now)
    key = authorization.evidence_id(record)
    if (root / "paper.sqlite3").is_file():
        with closing(db_open(True, root=root)) as db:
            if existing := registrations(db):
                if authorization.evidence_id(approved_authorization(db, at=now)) != key:
                    raise PermissionError("Existing Kraken books are already bound to a different approval")
                return existing
    terms = instrument_terms(raw_pair, pair_received_at, now)
    pins = code_pins()
    planned = list(_prepare(record, terms, now, pins))
    with writer_lock(root / ".cycle.lock"), closing(db_open(root=root)) as db:
        if existing := registrations(db):
            approved_authorization(db, at=now)
            if any(lane["body"]["authorization_id"] != key for lane in existing.values()):
                raise PermissionError("Existing Kraken books are already bound to a different approval")
            return existing
        with db:
            db.execute("INSERT INTO authorization VALUES(?,?)", (key, paper.encode(record)))
            for lane, scopes in planned:
                db.execute("INSERT INTO registration VALUES(?,?,?)",
                           (lane["profile"], fingerprint(lane), paper.encode(lane)))
                db.executemany("INSERT INTO source_grant VALUES(?,?)",
                               [(grant.id, paper.encode(_grant_body(grant))) for grant in scopes])
                paper.save(db, paper.initial(lane))
                db.execute("INSERT INTO readiness VALUES(?,?)", (lane["book_id"], paper.encode({
                    "state": "WAITING_WARMUP", "reason": "Awaiting authorized Kraken history ending at the registered seam",
                    "warmup": {"completed": 0, "required": 200}, "first_eligible_at": None,
                    "primary_end_exclusive": None,
                })))
        return registrations(db)
