"""Bind the existing empty EMA books to reviewed Kraken-reference paper research."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3

import pandas as pd

from lab.config import ROOT as PROJECT, fingerprint
from lab.ema_paper import catalog, proxy_protocol
from lab.kraken_paper import authorization
from lab.kraken_paper.catalog import _grant_body
from lab.storage import file_hash, utc_now, writer_lock
from lab.strategy_lab.market_data import ObservationKind, Purpose, ProvenanceClass, SourceGrant, SourceIdentity


VERSION = "EMA_KRAKEN_SYNTHETIC_REFERENCE_V1"
SOURCE_MODE = "KRAKEN_SPOT_REFERENCE"
ROOT = catalog.ROOT / "synthetic"
FILENAME = "protocol.sqlite3"
TRADE = ObservationKind.TRADE_OHLCV
AUTHORIZATION_ID = "b6352954bc1250f7514df23d5ab812bb1e1c908f9f658a80e9c9bac25264c2e5"
SCHEMA = """
CREATE TABLE IF NOT EXISTS revision(id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS synthetic_revision_update BEFORE UPDATE ON revision
    BEGIN SELECT RAISE(ABORT, 'immutable Kraken synthetic EMA protocol'); END;
CREATE TRIGGER IF NOT EXISTS synthetic_revision_delete BEFORE DELETE ON revision
    BEGIN SELECT RAISE(ABORT, 'immutable Kraken synthetic EMA protocol'); END;
"""
utc = authorization.utc


def pins():
    paths = ("lab/ema_paper/synthetic_protocol.py", "lab/ema_paper/synthetic_runtime.py",
             "lab/ema_paper/kraken_reference.py", "lab/strategy_lab/synthetic_btc.py",
             "lab/strategy_lab/feature_warmup.py", "lab/strategy_lab/technical_features.py",
             "lab/strategy_lab/market_data.py", "lab/kraken_paper/source.py", "lab/kraken_paper/worker.py", "lab/strategy_lab/spot_sources.py",
             "lab/ema_paper/notifications.py")
    return {**catalog.code_pins(), **{path: file_hash(PROJECT / path) for path in paths}}


def active(*, root=None):
    return ((ROOT if root is None else Path(root)) / FILENAME).is_file()


def review_source(review, now):
    record = authorization.read_review(review, at=now)
    auth = authorization.evidence_id(record)
    if auth != AUTHORIZATION_ID:
        raise PermissionError("Kraken permission differs from the reviewed BTC/USD research consent")
    source = SourceIdentity("Kraken", "kraken-btc-usd-public-ohlc-v1", "BTC-USD", "SPOT",
                            record["evidence_reference"], auth, ProvenanceClass.INDEPENDENT)
    return source, record


def _read(root):
    path = root / FILENAME
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        rows = db.execute("SELECT id,body FROM revision").fetchall()
    if len(rows) != 1:
        raise ValueError("Exactly one frozen synthetic EMA revision is required")
    identifier, payload = rows[0]
    body = json.loads(payload)
    if fingerprint(body) != identifier or body["version"] != VERSION:
        raise ValueError("Synthetic EMA revision checksum failed")
    return {"id": identifier, "body": body}


def load(*, root=None, now=None):
    root = ROOT if root is None else Path(root)
    saved = _read(root)
    body = saved["body"]
    if body["code_pins"] != pins():
        raise PermissionError("Synthetic EMA runtime differs from the frozen source protocol")
    now = utc_now() if now is None else utc(now)
    source, _ = review_source(body["review_path"], now)
    if utc(body["recorded_at"]) > now or source.id != body["source_id"]:
        raise PermissionError("Synthetic EMA runtime authorization is outside its source or activation clock")
    from lab.ema_paper import kraken_reference

    qualified = kraken_reference.qualification()
    if not qualified or qualified["id"] != body["qualification_id"]:
        raise PermissionError("Shared Kraken reference qualification differs from the frozen protocol")
    return saved


def _grant(body):
    fields = dict(body)
    return SourceGrant(source=SourceIdentity(**fields.pop("source")), **fields)


def grants(saved):
    """Restore and verify all purpose-bound scopes before opening the source."""
    items = []
    for item in saved["body"]["books"].values():
        if fingerprint(item["body"]) != item["id"]:
            raise ValueError("Synthetic EMA book amendment checksum failed")
        record = item["body"]
        scopes = tuple(_grant(value) for value in record["grants"])
        if (set(grant.purpose for grant in scopes) != set(Purpose)
                or len(scopes) != len(Purpose) or any(grant.book_id != record["book_id"]
                or grant.source.id != saved["body"]["source_id"] for grant in scopes)):
            raise PermissionError("Synthetic Kraken grant does not match the immutable original book")
        items.extend(scopes)
    return tuple(items)


def _lane(record, predecessor, source, now):
    from lab.strategy_lab import synthetic_btc

    lane = record["body"]
    profile = lane["profile"]
    step = pd.Timedelta(seconds=lane["execution_clock"]["decision_seconds"])
    seam = now.floor(step) + step
    minute_start = now.floor("min") + pd.Timedelta(minutes=1)
    end = (seam + pd.Timedelta(days=130)).ceil(step)
    scope = {Purpose.WARMUP: (seam - 720 * step, seam), Purpose.FORWARD_DECISION: (seam, end),
             Purpose.EXECUTION_EVIDENCE: (minute_start, end)}
    model = synthetic_btc.model(profile)
    content = {
        "book_id": lane["book_id"], "registration_id": record["id"], "strategy_hash": lane["strategy_hash"],
        "predecessor_amendment_id": predecessor["id"], "seed_end": seam.isoformat(),
        "registered_at": now.isoformat(), "source_mode": SOURCE_MODE,
        "price_source": "KRAKEN_BTC_USD_SPOT", "execution_model_id": model.id, "execution_model": model.body(),
        "counterfactual_leverage": [3, 5] if profile == "position" else [5],
        "actual_futures_mark": "NOT_AVAILABLE", "actual_funding": "NOT_AVAILABLE",
        "local_telegram_only": True, "paper_only": True, "live_orders": False,
        "grants": [_grant_body(SourceGrant(source, lane["book_id"], purpose, {TRADE}, left, right, now, now,
                                           "operator-approved-kraken-reference-paper:" + AUTHORIZATION_ID))
                   for purpose, (left, right) in scope.items()],
    }
    return {"id": fingerprint(content), "body": content}


def register(review, *, root=None, paper_root=None, now=None):
    """Append an auditable future-only protocol without changing balances or ledgers."""
    root = ROOT if root is None else Path(root)
    paper_root = catalog.ROOT if paper_root is None else Path(paper_root)
    now = utc_now() if now is None else utc(now)
    source, permission = review_source(review, now)
    with writer_lock(root / ".protocol.lock"), writer_lock(paper_root / ".cycle.lock"):
        if active(root=root):
            return load(root=root, now=now)
        from lab.ema_paper import commissioning, kraken_reference

        if (proxy_protocol.ROOT / commissioning.FILENAME).exists():
            raise PermissionError("A separately commissioned KuCoin study requires explicit closeout before migration")
        proof = kraken_reference.qualification()
        if not proof or proof["body"]["source_id"] != source.id:
            raise PermissionError("Qualified Kraken source differs from the reviewed permission")
        with closing(catalog.db_open(True, root=paper_root)) as ledger:
            originals = proxy_protocol._original(catalog.registrations(ledger))
            if any(ledger.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() for table in ("event", "eligibility")):
                raise PermissionError("The original EMA ledgers have prospective activity that requires explicit migration review")
            for record in originals.values():
                lane = record["body"]
                row = ledger.execute("SELECT state FROM book WHERE id=?", (lane["book_id"],)).fetchone()
                balance = json.loads(row[0]) if row else {}
                if (balance.get("starting_equity") != 10000 or balance.get("cash") != 10000
                        or balance.get("equity") != 10000 or any(balance.get(key) for key in ("position", "pending", "cursor", "problem"))):
                    raise PermissionError("The original paper account is outside the empty-ledger migration scope")
        records = {profile: _lane(record, proxy_protocol.get_amendment(profile), source, now)
                   for profile, record in originals.items()}
        body = {"version": VERSION, "recorded_at": now.isoformat(), "authorization_id": AUTHORIZATION_ID,
                "qualification_id": proof["id"],
                "source_id": source.id, "review_path": str(Path(review).resolve()), "books": records,
                "permission_expires_at": permission.get("expires_at"), "code_pins": pins()}
        identifier = fingerprint(body)
        with closing(sqlite3.connect(root / FILENAME, timeout=3)) as db:
            db.executescript(SCHEMA)
            with db:
                db.execute("INSERT INTO revision VALUES (?,?)", (identifier, json.dumps(body, sort_keys=True)))
        return {"id": identifier, "body": body}
