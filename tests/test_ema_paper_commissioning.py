from contextlib import closing
from dataclasses import replace
from hashlib import sha256
import json
import sqlite3

import pandas as pd
import pytest

from lab.ema_paper import catalog, commissioning, proxy_native, proxy_protocol
from lab.strategy_lab.cross_venue_warmup import freeze_proxy_seed
from lab.strategy_lab.market_data import ObservationKind, Purpose, SourceGrant, SourceIdentity
from lab.strategy_lab.source_spool import SPOOL_FILENAME, SPOOL_SCHEMA, SPOOL_VERSION
from test_cross_venue_warmup import SEAM, STEP, snapshot
from test_ema_paper_worker import registered


NOW = SEAM + pd.Timedelta(hours=2)
MINUTE = pd.Timedelta(minutes=1)
TRADE = ObservationKind.TRADE_OHLCV
RIGHTS = b"fixture-only correspondence for independent Ontario paper evaluation"
RECEIPT = b"fixture-only original upstream KuCoin receipt"


@pytest.fixture
def commissioning_env(registered, tmp_path, monkeypatch):
    paper_root, _, _, _, registrations = registered
    root = tmp_path / "commission"
    folder = tmp_path / "private"
    folder.mkdir()
    project = tmp_path / "protected_project"
    project.mkdir()
    (folder / "rights.txt").write_bytes(RIGHTS)
    spool = folder / "independent"
    spool.mkdir()
    review_path = folder / "review.json"
    review = {
        "version": commissioning.VERSION, "reviewed_scope": sorted(commissioning.RIGHTS),
        "upstream_venue": "KuCoin", "protected_sources_excluded": True, "paper_only": True, "live_orders": False,
        "operator": "Synthetic operator", "provider": "Synthetic licensed producer", "namespace": "synthetic EMA source",
        "rights_reference": "Synthetic independent review", "approved_at": (SEAM - pd.Timedelta(days=1)).isoformat(),
        "provider_effective_at": (SEAM - pd.Timedelta(days=2)).isoformat(),
        "expires_at": (NOW + pd.Timedelta(days=180)).isoformat(), "rights_evidence_file": "rights.txt",
        "rights_evidence_sha256": sha256(RIGHTS).hexdigest(), "spool_directory": "independent", "books": {},
    }
    predecessors = {}
    monkeypatch.setattr(commissioning, "PROJECT", project)
    monkeypatch.setattr(commissioning, "_pins", lambda: {"fixture": "pinned independent commissioning"})
    monkeypatch.setattr(catalog, "registrations", lambda: registrations)
    monkeypatch.setattr(proxy_protocol, "ORIGINAL_HASHES", {
        profile: record["body"]["strategy_hash"] for profile, record in registrations.items()
    })
    monkeypatch.setattr(proxy_protocol, "get_amendment", lambda profile, *, root: predecessors[profile])

    def fresh_seed(lane, previous, settings, review_path, now, seam):
        interval = pd.Timedelta(seconds=lane["execution_clock"]["decision_seconds"])
        archived = snapshot(interval)
        assert seam == SEAM
        grant = SourceGrant(archived.source, lane["book_id"], Purpose.WARMUP, {TRADE}, archived.start, seam, now,
                            lane["registered_at"], "Fixture feature-only approval")
        seed = freeze_proxy_seed(archived, timeframe=interval)
        return replace(seed, as_of=now, book_id=lane["book_id"], grant_id=grant.id), grant

    monkeypatch.setattr(commissioning, "_fresh_seed", fresh_seed)
    database = spool / SPOOL_FILENAME
    with closing(sqlite3.connect(database)) as db, db:
        db.executescript(SPOOL_SCHEMA)
        raw_hash = sha256(RECEIPT).hexdigest()
        db.execute("INSERT INTO market_raw VALUES (?,?)", (raw_hash, RECEIPT))
        sources = set()
        for profile, record in registrations.items():
            lane = record["body"]
            interval = lane["execution_clock"]["decision_seconds"]
            monkeypatch.setitem(proxy_native.ORIGINAL_BOOKS, profile,
                                (record["id"], lane["book_id"], lane["market"], lane["instrument"], interval))
            source = SourceIdentity(review["provider"], review["namespace"], lane["instrument"], lane["market"],
                                    review["rights_reference"], review["rights_evidence_sha256"], "independent")
            if source.id not in sources:
                db.execute("INSERT INTO market_source VALUES (?,?,?,?,?,?)",
                           (source.id, SPOOL_VERSION, source.provider, source.instrument, source.market,
                            source.rights_evidence_sha256))
                sources.add(source.id)
            if profile == "day":
                prices = json.dumps({"open": 106., "high": 108., "low": 105., "close": 107., "volume": 1.})
                for at in pd.date_range(SEAM, periods=15, freq=MINUTE):
                    close = at + MINUTE
                    db.execute("INSERT INTO market_observation VALUES (?,?,?,?,?,?,?,?)",
                               (source.id, TRADE.value, at.value, close.value, close.value, close.value, raw_hash, prices))
            seed, grant = fresh_seed(lane, None, None, None, NOW, SEAM)
            predecessors[profile] = {
                "id": f"synthetic original {profile} amendment",
                "body": {"recorded_at": (SEAM - pd.Timedelta(days=2)).isoformat(), "seed": seed.body(),
                         "warmup_grant": commissioning._grant_body(grant)},
            }
            review["books"][profile] = {"registration_id": record["id"], "book_id": lane["book_id"],
                                        "predecessor_amendment_id": predecessors[profile]["id"],
                                        "seam": SEAM.isoformat()}
    review_path.write_text(json.dumps(review), encoding="utf-8")
    return root, review, review_path, database, project, paper_root, registrations


def test_commission_binds_original_books_and_read_only_spool(commissioning_env):
    root, _, path, spool, _, paper_root, registrations = commissioning_env
    ledger = paper_root / "paper.sqlite3"
    ledger_bytes, spool_bytes = ledger.read_bytes(), spool.read_bytes()
    assert commissioning.attachments(registrations, NOW, root=root) == {}
    first = commissioning.commission(path, now=NOW, root=root)
    assert first["state"] == "COMMISSIONED"
    assert commissioning.commission(path, now=NOW, root=root) == first
    attached = commissioning.attachments(registrations, NOW, root=root)
    assert set(attached) == set(registrations)
    for profile, (amendment, seed, reader) in attached.items():
        assert seed.end == SEAM and seed.count == 220
        assert seed.book_id == registrations[profile]["body"]["book_id"]
        assert amendment["body"]["registration_id"] == registrations[profile]["id"]
        assert reader._forward.start == reader._execution.start == SEAM
        assert reader._forward.purpose == Purpose.FORWARD_DECISION
        assert reader._execution.purpose == Purpose.EXECUTION_EVIDENCE
        assert reader._forward.source.id == reader._execution.source.id
    _, _, day = attached["day"]
    batch, proof = day.read_chart(SEAM + STEP, NOW)
    assert batch.complete and batch.bars.volume.iloc[-1] == 15.
    assert proof.observations[-1].raw_sha256 == sha256(RECEIPT).hexdigest()
    assert ledger.read_bytes() == ledger_bytes and spool.read_bytes() == spool_bytes


def test_books_can_commission_independently_and_fail_closed_per_review(commissioning_env):
    root, review, day_path, _, _, _, registrations = commissioning_env
    day_path.write_text(json.dumps({**review, "books": {"day": review["books"]["day"]}}), encoding="utf-8")
    commissioning.commission(day_path, now=NOW, root=root)
    assert set(commissioning.attachments(registrations, NOW, root=root)) == {"day"}
    spot_path = day_path.with_name("spot-review.json")
    spot_path.write_text(json.dumps({**review, "books": {"position": review["books"]["position"]}}), encoding="utf-8")
    commissioning.commission(spot_path, now=NOW, root=root)
    assert set(commissioning.attachments(registrations, NOW, root=root)) == {"day", "position"}
    day_path.write_text("{}", encoding="utf-8")
    errors = {}
    assert set(commissioning.attachments(registrations, NOW, root=root, errors=errors)) == {"position"}
    assert set(errors) == {"day"}


@pytest.mark.parametrize("change,match", [
    (lambda review: review.update(namespace="unreviewed alternate source"), "manifest differs"),
    (lambda review: review.update(rights_evidence_sha256="0" * 64), "checksum failed"),
    (lambda review: review["reviewed_scope"].remove("ontario"), "complete operator-reviewed"),
    (lambda review: review["books"]["swing"].update(seam=(SEAM + MINUTE).isoformat()), "approved exact seam"),
    (lambda review: review["books"]["day"].update(book_id="duplicate account"), "pin the original book"),
])
def test_commission_rejects_mismatched_scope_namespace_or_identity(commissioning_env, change, match):
    root, review, path, _, _, _, _ = commissioning_env
    change(review)
    path.write_text(json.dumps(review), encoding="utf-8")
    with pytest.raises(PermissionError, match=match):
        commissioning.commission(path, now=NOW, root=root)
    assert not (root / commissioning.FILENAME).exists()


def test_commission_rejects_refreshed_seed_with_gap_at_seam(commissioning_env, monkeypatch):
    root, _, path, _, _, _, _ = commissioning_env
    prepare = commissioning._fresh_seed

    def stale(lane, previous, settings, review_path, now, seam):
        seed, grant = prepare(lane, previous, settings, review_path, now, seam)
        step = pd.Timedelta(seconds=seed.timeframe_seconds)
        return replace(seed, start=seed.start - step, end=seed.end - step), grant

    monkeypatch.setattr(commissioning, "_fresh_seed", stale)
    with pytest.raises(ValueError, match="reviewed book and UTC seam"):
        commissioning.commission(path, now=NOW, root=root)
    assert not (root / commissioning.FILENAME).exists()


def test_commission_never_opens_protected_project_path_or_missing_rights(commissioning_env, monkeypatch):
    root, review, path, _, project, _, _ = commissioning_env
    review["spool_directory"] = str(project)
    path.write_text(json.dumps(review), encoding="utf-8")
    monkeypatch.setattr(commissioning, "_spool", lambda *args: pytest.fail("attempted to open protected source"))
    with pytest.raises(PermissionError, match="outside the project stores"):
        commissioning.commission(path, now=NOW, root=root)
    review.update(spool_directory="independent", rights_evidence_file="missing.txt")
    path.write_text(json.dumps(review), encoding="utf-8")
    with pytest.raises((FileNotFoundError, PermissionError)):
        commissioning.commission(path, now=NOW, root=root)
    assert not (root / commissioning.FILENAME).exists()
