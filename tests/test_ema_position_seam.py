from contextlib import closing
from contextlib import nullcontext
from dataclasses import replace

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.ema_paper import catalog, position_seam, private_notifications, recovery_protocol, synthetic_protocol, worker
from lab.health import state
from lab.phase15 import paper
from lab.storage import file_hash
from lab.strategy_lab.market_data import MarketData, Observation, Purpose
from test_cross_venue_warmup import RAW, RAW_HASH, SEAM, snapshot
from test_ema_paper_worker import registered


@pytest.fixture
def successor(registered, tmp_path, monkeypatch):
    root, _, _, _, originals = registered
    protocol_root = tmp_path / "protocol"
    archive = snapshot(position_seam.DAY)
    old_registration = SEAM - position_seam.DAY - pd.Timedelta(hours=1)
    books = {name: synthetic_protocol._lane(record, {"id": "fixture predecessor"}, archive.source, old_registration)
             for name, record in originals.items()}
    synthetic_body = {"books": books, "source_id": archive.source.id, "qualification_id": "fixture qualification"}
    synthetic = {"id": fingerprint(synthetic_body), "body": synthetic_body}
    recovery = {"id": "fixture recovery", "body": {"recorded_at": old_registration.isoformat()}}
    monkeypatch.setattr(synthetic_protocol, "load", lambda **kwargs: synthetic)
    monkeypatch.setattr(recovery_protocol, "load", lambda *args, **kwargs: recovery)
    approved = SEAM - pd.Timedelta(hours=1)
    before = file_hash(root / "paper.sqlite3")
    amendment = position_seam.register(root=protocol_root, paper_root=root, now=approved)
    assert file_hash(root / "paper.sqlite3") == before
    assert position_seam.register(root=protocol_root, paper_root=root, now=approved) == amendment
    assert amendment["body"]["original_seam"] == (SEAM - position_seam.DAY).isoformat()
    return root, protocol_root, originals, synthetic, archive, approved, amendment


def test_missed_old_seam_is_requested_only_as_successor_history(successor, monkeypatch):
    root, protocol_root, _, synthetic, archive, approved, amendment = successor
    reference = position_seam.kraken_reference
    source_root = root.parent / "source"
    clock = approved + pd.Timedelta(minutes=1)
    monkeypatch.setattr(position_seam, "utc_now", lambda: clock)
    proof = {"id": synthetic["body"]["qualification_id"]}
    monkeypatch.setattr(reference, "qualification", lambda **kwargs: proof)
    scope = reference._Collection(archive.source, SEAM - position_seam.DAY - pd.Timedelta(hours=1), None)
    monkeypatch.setattr(reference, "_qualified_permission", lambda qualification, at: scope)
    monkeypatch.setattr(position_seam.source, "_writer", lambda path: nullcontext(None))
    requests = []

    def fetch(_db, actual_scope, start, end, step, *, rate_bound):
        requests.append(actual_scope.grant(Purpose.WARMUP, start, end))
        assert step == position_seam.DAY and rate_bound
        return {"state": "CAPTURED", "available_at": clock.isoformat()}

    monkeypatch.setattr(position_seam.recovery_source, "_repair_one", fetch)
    saved = position_seam.prepare_history(root=protocol_root, paper_root=root, source_root=source_root)
    assert saved["purpose"] == Purpose.WARMUP and len(requests) == 1
    assert requests[0].end.isoformat() == amendment["body"]["original_seam"]
    assert requests[0].start < scope.approved_at < requests[0].end
    assert pd.Timestamp(saved["available_at"]) > requests[0].end + pd.Timedelta(seconds=120)


@pytest.mark.parametrize("seam_lag", [5, 121, None], ids=["timely", "late", "missing"])
def test_position_only_accepts_a_complete_timely_future_seam(successor, monkeypatch, seam_lag):
    root, protocol_root, originals, synthetic, archive, approved, amendment = successor
    day, minute = position_seam.DAY, pd.Timedelta(minutes=1)
    original = originals["position"]
    lane = original["body"]
    position = amendment["body"]["position"]
    assert position["body"]["book_id"] == lane["book_id"]
    assert position["body"]["execution_model"] == synthetic["body"]["books"]["position"]["body"]["execution_model"]
    assert position["body"]["execution_model"]["leverage"] == 1
    # Earlier candles remain history even when the missed prior seam was received late.
    rows = [replace(row, received_at=approved + minute, available_at=approved + minute)
            for row in archive.observations[:-1]]
    if seam_lag is not None:
        seen = SEAM + pd.Timedelta(seconds=seam_lag)
        rows.append(replace(archive.observations[-1], received_at=seen, available_at=seen))
    next_boundary = SEAM + day
    seen = next_boundary + pd.Timedelta(seconds=5)
    values = archive.observations[-1].values
    rows.append(Observation(archive.source.id, synthetic_protocol.TRADE, SEAM, next_boundary, seen, seen,
                            RAW_HASH, values))
    for boundary in (SEAM, next_boundary):
        for closed in pd.date_range(boundary - 4 * minute, boundary + 2 * minute, freq=minute):
            seen = closed + pd.Timedelta(seconds=3)
            rows.append(Observation(archive.source.id, synthetic_protocol.TRADE, closed - minute, closed, seen, seen,
                                    RAW_HASH, values))

    class Provider:
        def read(self, kind, start, end, as_of, interval):
            return (row for row in rows if row.kind == kind and row.end - row.start == interval and start <= row.start < end)

        def raw(self, digest):
            assert digest == RAW_HASH
            return RAW

    monkeypatch.setattr(position_seam.kraken_reference, "market_data",
                        lambda grants, **kwargs: MarketData(grants, {archive.source.id: Provider()}))
    monkeypatch.setattr(position_seam.recovery_source, "repair_pending", lambda *args, **kwargs: None)

    def run(now):
        return position_seam.cycle(root=root, protocol_root=protocol_root, now=now)["books"]["position"]

    assert run(SEAM - minute)["state"] == "WAITING_FOR_DATA"
    qualified = run(SEAM + 3 * minute)
    if seam_lag != 5:
        assert qualified["state"] == "WAITING_FOR_DATA"
        assert qualified["first_eligible_at"] is None
    else:
        assert qualified["state"] == "WAITING_FOR_START"
        assert qualified["first_eligible_at"] == next_boundary.isoformat()
        assert qualified["source_protocol"]["warmup_count"] == 220
        with closing(catalog.db_open(True, root=root)) as db:
            assert db.execute("SELECT COUNT(*) FROM event WHERE book=?", (lane["book_id"],)).fetchone() == (0,)
        assert run(next_boundary + 3 * minute)["state"] == "ACTIVE"
        assert run(next_boundary + 3 * minute)["state"] == "ACTIVE"
    with closing(catalog.db_open(True, root=root)) as db:
        assert catalog.registrations(db) == originals
        assert paper.load(db, lane) == paper.initial(lane)
        assert db.execute("SELECT COUNT(*) FROM event WHERE book=?", (lane["book_id"],)).fetchone() == (0,)
        if seam_lag == 5:
            saved = worker._record(db, "readiness", lane["book_id"])
            assert saved["last_evaluated_at"] == next_boundary.isoformat()
            assert saved["last_signal"]["decision"] == "NO_TRADE"


def test_position_successor_uses_original_private_route_and_cursors(successor, tmp_path, monkeypatch):
    root, protocol_root, originals, synthetic, _, approved, amendment = successor
    lane = originals["position"]["body"]
    effective = amendment["body"]["position"]
    predecessor = synthetic["body"]["books"]["position"]
    monkeypatch.setattr(synthetic_protocol, "ROOT", protocol_root)
    monkeypatch.setattr(state, "ROOT", tmp_path / "outbox")
    route_id = "a" * 64
    with closing(catalog.db_open(root=root)) as journal, state.connect() as outbox:
        route = private_notifications.enable(outbox, route_id, approved, originals, journal, revision=synthetic)
        at = SEAM + position_seam.DAY
        plan = {"evidence": {"source_mode": synthetic_protocol.SOURCE_MODE, "protocol_amendment_id": effective["id"]}}
        with journal:
            paper.event(journal, lane, "PUBLISHED", "fixture", at, at, {"direction": "LONG", "plan": plan})
        private_notifications.collect(outbox, at, originals, journal)
        private_notifications.collect(outbox, at, originals, journal)
        assert private_notifications.route(outbox) == route
        assert state.get_meta(outbox, private_notifications._cursor(predecessor["id"], ideas=True)) == "1"
        assert state.get_meta(outbox, private_notifications._cursor(effective["id"], ideas=True)) is None
        sent = outbox.execute("SELECT category,text FROM outbox").fetchall()
        assert len(sent) == 1
        assert sent[0]["category"] == "ideas" and "EMA Pullback Position 1D · LONG · 1×" in sent[0]["text"]
