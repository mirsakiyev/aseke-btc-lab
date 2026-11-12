from contextlib import closing
from hashlib import sha256
import json
import sqlite3

import pandas as pd
import pytest

from lab.kraken_paper import authorization, catalog, managed
from lab.phase15 import paper
from lab.strategy_lab.market_data import Purpose
from lab.strategy_lab.registry import digest


NOW = pd.Timestamp("2026-11-12T16:01:00Z")
PAIR = json.dumps({"error": [], "result": {"XXBTZUSD": {
    "altname": "XBTUSD", "wsname": "XBT/USD", "base": "XXBT", "quote": "ZUSD", "status": "online",
    "lot_decimals": 8, "tick_size": ".1", "ordermin": ".00006", "costmin": ".5",
}}}).encode()


def _review(tmp_path, **changes):
    root = tmp_path / "private"
    root.mkdir(exist_ok=True)
    request = b"fixture-only: private spot paper and retention"
    response = b"fixture-only: consent for that request"
    (root / "request.txt").write_bytes(request)
    (root / "response.txt").write_bytes(response)
    stamp = (NOW - pd.Timedelta(seconds=20)).isoformat()
    document = {
        "provider": "Kraken", "contact": authorization.CONTACT, "jurisdiction": "CA-ON", "market": "SPOT",
        "instrument": "BTC-USD", "authorization_type": "WRITTEN_PERMISSION",
        "approved_scope": sorted(authorization.SCOPE), "paper_only": True, "api_key_required": False,
        "evidence_reference": "fixture evidence", "reviewed_by": "fixture operator",
        "original_request_path": "request.txt", "provider_response_path": "response.txt",
        "original_request_sha256": sha256(request).hexdigest(), "provider_response_sha256": sha256(response).hexdigest(),
        "provider_granted_at": None, "operator_approved_at": stamp, "evidence_observed_at": stamp, "expires_at": None,
        **changes,
    }
    descriptor = root / "review.json"
    descriptor.write_text(json.dumps(document), encoding="utf-8")
    return descriptor


def test_reply_alone_scope_mismatch_and_modified_original_never_create_accounts(tmp_path):
    root = tmp_path / "paper"
    insufficient = sorted(authorization.SCOPE - {"local_raw_and_derived_retention"})
    with pytest.raises(PermissionError, match="no verified scope"):
        catalog.register_books(_review(tmp_path, approved_scope=insufficient), PAIR, NOW, now=NOW, root=root)
    review = _review(tmp_path)
    (review.parent / "request.txt").write_text("changed", encoding="utf-8")
    with pytest.raises(PermissionError, match="checksum"):
        catalog.register_books(review, PAIR, NOW, now=NOW, root=root)
    assert not root.exists()
    assert managed.ensure(root=root) == {"state": "NOT_REGISTERED"}


def test_independent_usd_registration_grants_and_restart_preservation(tmp_path):
    root = tmp_path / "paper"
    review = _review(tmp_path)
    assert not catalog.registrations(root=root)
    books = catalog.register_books(review, PAIR, NOW, now=NOW, root=root)
    assert set(books) == {"day", "swing", "position"}
    assert len({r["body"]["book_id"] for r in books.values()}) == 3
    with closing(catalog.db_open(True, root=root)) as db:
        auth = catalog.approved_authorization(db, at=NOW)
        scopes = catalog.grants(db)
        assert len(scopes) == 9 and len({g.source.id for g in scopes}) == 1
        assert db.execute("SELECT COUNT(*) FROM event").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM eligibility").fetchone()[0] == 0
        for profile, record in books.items():
            lane = record["body"]
            book = paper.load(db, lane)
            assert lane["strategy_hash"] == digest(lane["spec"])
            assert lane["authorization_id"] == authorization.evidence_id(auth)
            assert lane["currency"] == "USD" and lane["permissions"]["telegram_lifecycle"] is False
            assert book["cash"] == book["equity"] == 10000 and book["position"] is None
            scoped = {g.purpose: g for g in scopes if g.book_id == lane["book_id"]}
            assert scoped[Purpose.WARMUP].end == scoped[Purpose.FORWARD_DECISION].start > NOW
            assert scoped[Purpose.EXECUTION_EVIDENCE].start == scoped[Purpose.FORWARD_DECISION].start
            assert lane["instrument_terms"]["quantity_step"] == 1e-8
            assert lane["cost"]["taker"] == .008
    assert catalog.register_books(review, PAIR, NOW, now=NOW + pd.Timedelta(days=2), root=root) == books
    with closing(catalog.db_open(root=root)) as db:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db.execute("DELETE FROM authorization")


def test_private_material_inside_repo_and_noncausal_terms_are_rejected(tmp_path, monkeypatch):
    review = _review(tmp_path)
    monkeypatch.setattr(authorization, "PROJECT", tmp_path)
    with pytest.raises(ValueError, match="outside the repository"):
        authorization.read_review(review, at=NOW)
    with pytest.raises(ValueError, match="current, causal"):
        catalog.instrument_terms(PAIR, NOW + pd.Timedelta(seconds=1), NOW)
    assert catalog.instrument_terms(PAIR, NOW, NOW)["min_quantity"] == .00006
