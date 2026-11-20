from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
import json
import sqlite3

import pandas as pd
import pytest

from lab.learning.simulator import Position
from lab.paper_run import capabilities as caps, delivery, gate, membership, operational_status, runtime
from lab.paper_run import protection_incidents as incidents
from lab.phase15 import paper
from lab.storage import write_json
from test_paper_capabilities_membership import AT, apply, eighth, registered


@pytest.fixture
def monitor(registered, tmp_path, monkeypatch):
    db, record = registered
    for slot, lane in record["body"]["lanes"].items():
        lane["profile"] = slot.split("/")[1]
    db.execute("CREATE TABLE source_state(id TEXT PRIMARY KEY,state TEXT)")
    for lane in record["body"]["lanes"].values():
        point = {"source": lane["source"], "symbol": lane["symbol"], "observation_kind": lane["observation_kind"],
                 "event_id": "100", "source_at": (AT-pd.Timedelta(milliseconds=100)).isoformat(),
                 "received_at": (AT-pd.Timedelta(milliseconds=50)).isoformat(), "price": 100.}
        runtime._save_lane_source(db, lane, {"last_observation": point})
    db.commit()
    path = tmp_path / "paper.sqlite3"

    def publish():
        db.commit()
        with closing(sqlite3.connect(path)) as target:
            db.backup(target)

    publish()
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    monkeypatch.setattr(runtime, "current", lambda: record)
    monkeypatch.setattr(runtime, "connect", lambda *, readonly=False: sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True))
    now = [AT]
    monkeypatch.setattr(runtime, "utc_now", lambda: now[0])
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(runtime.monitoring, "controls", lambda: {
        "monitor": True, "paused": False, "ideas": {"day": True, "swing": True, "position": True}})
    monkeypatch.setattr(runtime, "_source_health", lambda _db, lane, _at, stream=None: {
        "available": True, "reason_code": None, "source_scope_id": caps.source_scope_id(lane)})
    heartbeat = {"state": "RUNNING", "at": AT.isoformat(), "generation": record["id"],
                 "runner_build_id": runtime.RUNNER_BUILD, "pid": 123}
    write_json(tmp_path / "last-cycle.json", heartbeat)
    write_json(tmp_path / "ema-live-status.json", {"state": "STREAMING", "at": AT.isoformat(),
               "last_usable_observation": {"source": caps.KRAKEN, "symbol": "BTC/USD", "observation_kind": "WEBSOCKET_TRADE",
                                            "event_id": "999", "source_at": AT.isoformat(), "received_at": AT.isoformat(), "price": 102.}})
    return db, record, publish, now, path


def test_all_seven_status_contracts_preserve_aggregate_and_statistics(monitor):
    db, record, _, _, _ = monitor
    before = db.total_changes
    result = operational_status.read_status()
    assert set(result["books"]) == set(record["body"]["lanes"])
    assert result["monitor_state"] == "OK"
    for slot, value in result["books"].items():
        assert value["state"] == "ACTIVE" and value["prospective_entry_permission"]
        assert value["operational"]["entry_permission"]["available"]
        assert value["operational"]["exposure"]["state"] == "FLAT"
        assert value["financial_reconciliation_complete"]
        assert value["cash"] == value["equity"] == 10000.
        assert value["validated_closed_net"] == value["closed_net"] == 0.
        assert value["capabilities"]["generation"] == record["id"]
        assert value["capabilities"]["slot"] == slot
        assert value["source_scope_id"] == caps.source_scope_id(record["body"]["lanes"][slot])
        assert value["aggregate_state_semantics"]
    assert db.total_changes == before


def test_status_projection_releases_live_reader_before_a_writer_commit(monitor, monkeypatch):
    db, record, _, _, path = monitor
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    book["last_evaluation"] = {"state": "EVALUATED", "reason": "NEXT_CYCLE", "at": AT.isoformat(),
                               "boundary": AT.isoformat()}
    original_view_book = operational_status._view_book
    writer_results = []

    def project_book(*args, **kwargs):
        if not writer_results:
            try:
                with closing(sqlite3.connect(path, timeout=0)) as writer:
                    assert writer.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
                    paper.save(writer, book)
                    writer.commit()
            except sqlite3.OperationalError as error:
                writer_results.append(str(error))
            else:
                writer_results.append("COMMITTED")
        return original_view_book(*args, **kwargs)

    monkeypatch.setattr(operational_status, "_view_book", project_book)
    earlier = operational_status.read_status()
    assert writer_results == ["COMMITTED"]
    assert earlier["books"]["ema/day"].get("last_evaluation") is None
    later = operational_status.read_status()
    assert later["books"]["ema/day"]["last_evaluation"]["reason"] == "NEXT_CYCLE"


def test_reset_status_reports_the_immediate_retirement_archive_and_book(monitor):
    _, record, _, _, _ = monitor
    record["body"].update(archive="first-cutover", retired_generation_archive="administratively-retired",
                          retired_generation="previous-generation")
    record["body"]["lanes"]["ema/day"].update(legacy_book_id="first-day-book", retired_book_id="previous-day-book")

    status = operational_status.read_status()
    assert status["archive"] == "administratively-retired"
    assert status["retired_generation"] == "previous-generation"
    assert status["books"]["ema/day"]["retired_book_id"] == "previous-day-book"


def test_release_readiness_removes_only_the_global_pause(monitor, monkeypatch):
    db, record, publish, _, _ = monitor
    (gate.ROOT / "entries-paused").touch()
    monkeypatch.setattr(runtime.monitoring, "controls", lambda: {
        "monitor": True, "paused": False, "ideas": {"day": True, "swing": True, "position": False}})
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    book["problem"] = "UNRESOLVED_PROTECTION_INTERVAL"
    paper.save(db, book)
    publish()

    status = operational_status.read_status()
    assert status["entries_paused"] is True
    views = status["books"]
    assert len(views) == 7
    assert all(not view["prospective_entry_permission"] for view in views.values())
    assert views["breakout/day/fixed"]["admission_after_global_resume"]["available"]
    assert views["breakout/position/fixed"]["admission_after_global_resume"]["reason_code"] == "PROFILE_ENTRY_CONTROL_DISABLED"
    assert views["ema/day"]["admission_after_global_resume"]["reason_code"] == "HISTORICAL_REVIEW_REQUIRED"


def test_a_newly_registered_nonstandard_eighth_appears_without_an_operational_name_branch(monitor):
    db, record, publish, _, _ = monitor
    before = operational_status.read_status()
    request = eighth(record)
    apply(db, record, request)
    publish()
    after = operational_status.read_status()
    assert len(before["books"]) == 7 and len(after["books"]) == 8
    value = after["books"][request["slot"]]
    assert value["state"] == "ACTIVE" and value["prospective_entry_permission"]
    assert value["operational"]["capabilities"]["strategy"]["name"] == request["lane"]["strategy"]
    assert request["lane"]["family"] not in ("EMA", "BREAKOUT")


def test_a_previously_registered_eighth_with_now_unsupported_adapter_is_isolated(monitor, monkeypatch):
    db, record, publish, _, _ = monitor
    request = eighth(record)
    apply(db, record, request)
    publish()
    original = caps.contract

    def reviewed(lane, **kw):
        if lane["book_id"] == request["lane"]["book_id"]:
            raise caps.UnsupportedCapability("UNSUPPORTED_SIGNAL_ADAPTER", "later implementation")
        return original(lane, **kw)

    monkeypatch.setattr(caps, "contract", reviewed)
    result = operational_status.read_status()
    assert len(result["books"]) == 8
    value = result["books"][request["slot"]]
    assert value["state"] == "BLOCKED" and not value["prospective_entry_permission"]
    assert value["reason"] == "UNSUPPORTED_SIGNAL_ADAPTER"
    assert value["exposure_state"] == "UNKNOWN"
    assert all(item["state"] == "ACTIVE" for slot, item in result["books"].items() if slot != request["slot"])


@pytest.mark.parametrize("kind", ["position", "pending"])
def test_active_occupied_books_never_report_overlapping_entry_permission(monitor, kind):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    if kind == "position":
        book["position"] = {"id": "open", "position": asdict(Position(1, 1., 100., 10000., 100., 0., 100.)),
                            "planned_risk": 1., "last_mark": 100.}
    else:
        book["pending"] = {"id": "intention"}
    paper.save(db, book)
    publish()
    value = operational_status.read_status()["books"]["ema/day"]
    assert value["state"] == "ACTIVE"
    assert not value["prospective_entry_permission"]
    assert value["operational"]["entry_permission"]["reason_code"] == ("POSITION_OPEN" if kind == "position" else "PENDING_ENTRY")


@pytest.mark.parametrize("guard", ["clock", "source", "pause"])
def test_current_clock_source_and_pause_gates_are_explicit_without_protection_incidents(monitor, monkeypatch, guard):
    db, _, _, _, _ = monitor
    if guard == "clock":
        monkeypatch.setattr(runtime, "synchronized", lambda _: False)
    elif guard == "source":
        monkeypatch.setattr(runtime, "_source_health", lambda *_args, **_kw: {
            "available": False, "reason_code": "SOURCE_RESTRICTED"})
    else:
        write_json(gate.ROOT / "entries-paused", {"generation": "test", "paused_at": AT.isoformat()})
    result = operational_status.read_status()
    assert all(not item["prospective_entry_permission"] for item in result["books"].values())
    expected = {"clock": "CLOCK_UNVERIFIED", "source": "SOURCE_RESTRICTED", "pause": "INTENTIONAL_ENTRY_PAUSE"}[guard]
    assert result["books"]["ema/day"]["operational"]["entry_permission"]["reason_code"] == expected
    assert db.execute("SELECT count(*) FROM event WHERE kind='PROTECTION_INCIDENT_OPENED'").fetchone()[0] == 0


@pytest.mark.parametrize("damage", ["missing", "json", "identity", "nonfinite"])
def test_one_bad_registered_book_does_not_suppress_the_others_or_create_a_balance(monitor, damage):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    if damage == "missing":
        db.execute("DELETE FROM book WHERE id=?", (lane["book_id"],))
    elif damage == "json":
        db.execute("UPDATE book SET state=? WHERE id=?", ('\x00\x00', lane["book_id"]))
    else:
        value = paper.load(db, lane)
        value.update(book_id="wrong") if damage == "identity" else value.update(cash=float("nan"))
        db.execute("UPDATE book SET state=? WHERE id=?", (json.dumps(value), lane["book_id"]))
    publish()
    count = db.execute("SELECT count(*) FROM book").fetchone()[0]
    result = operational_status.read_status()
    bad = result["books"]["ema/day"]
    assert bad["state"] == "BLOCKED" and not bad["prospective_entry_permission"]
    assert bad["operational"]["exposure"]["state"] == "UNKNOWN"
    assert bad["cash"] is None and bad["inspection_error"]
    assert all(value["state"] == "ACTIVE" for slot, value in result["books"].items() if slot != "ema/day")
    assert db.execute("SELECT count(*) FROM book").fetchone()[0] == count
    json.dumps(result, allow_nan=False)


def test_corrupt_source_evidence_preserves_known_exposure_and_isolates_other_sources(monitor, monkeypatch):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    book["position"] = {"id": "protected-open", "position": asdict(Position(1, 1., 100., 10000., 100., 0., 100.)),
                        "planned_risk": 1., "last_mark": 100.}
    paper.save(db, book)
    publish()

    def health(_db, source_lane, _at, stream=None):
        if source_lane["source"] == caps.KRAKEN:
            raise ValueError("source evidence inaccessible")
        return {"available": True, "reason_code": None}

    monkeypatch.setattr(runtime, "_source_health", health)
    value = operational_status.read_status()
    assert value["books"]["ema/day"]["operational"]["exposure"]["state"] == "OPEN"
    assert value["books"]["ema/day"]["operational"]["current_protection"] == "UNAVAILABLE"
    assert value["books"]["breakout/day/fixed"]["prospective_entry_permission"]


def test_canonical_durable_reference_wins_over_live_status_fallback(monitor):
    _, _, _, _, _ = monitor
    value = operational_status.read_status()["books"]["ema/day"]
    assert value["last_observation"]["event_id"] == "100"


def test_malformed_observation_keeps_known_exposure_instead_of_reinitializing_it(monitor):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    book["position"] = {"id": "protected-open", "position": asdict(Position(1, 1., 100., 10000., 100., 0., 100.)),
                        "planned_risk": 1., "last_mark": 100.}
    paper.save(db, book)
    runtime._save_lane_source(db, lane, {"last_observation": "corrupt object"})
    publish()
    value = operational_status.read_status()["books"]["ema/day"]
    assert value["exposure_state"] == "OPEN" and value["position"]["id"] == "protected-open"
    assert not value["prospective_entry_permission"] and value["last_observation"] is None
    assert value["operational"]["source_health"]["reason_code"] == "SOURCE_OBSERVATION_INSPECTION_FAILED"
    assert value["operational"]["current_protection"] == "UNAVAILABLE"


def test_ws_status_reference_is_used_only_when_canonical_observation_is_missing(monitor):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    scope = caps.source_scope_id(lane)
    db.execute("UPDATE source_state SET state='{}' WHERE id IN (?,?)", ("SCOPE:"+scope, lane["source"]))
    publish()
    assert operational_status.read_status()["books"]["ema/day"]["last_observation"]["event_id"] == "999"


def reviewed_permit(db, record, lane, future):
    book = paper.load(db, lane)
    incidents.append(db, book, "PROTECTION_POLICY_ACTIVATED", record["id"], AT,
                     {"generation": record["id"]}, identity="policy")
    incidents.append(db, book, "PROTECTION_INCIDENT_IMPORTED", "past", AT,
                     {"generation": record["id"], "incident_id": "past-incident", "position_id": "past",
                      "historical_coverage": "UNRESOLVED", "current_feed_health": "UNKNOWN_HISTORICAL",
                      "outcome": "INSUFFICIENT_EVIDENCE"}, identity="past-incident")
    dep = {"incident_ids": ["past-incident"], "position_ids": ["past"], "segment_id": "segment"}
    key = incidents.append(db, book, "BOOK_PROSPECTIVE_RESUME", "past", AT,
                           {"generation": record["id"], "effective_at": future.isoformat(), "capital_dependency": dep}, identity="resume")
    book.update(problem="UNRESOLVED_PROTECTION_INTERVAL", prospective_admission={
        "policy": incidents.POLICY, "event_id": key, "effective_at": future.isoformat(), "capital_dependency": dep})
    paper.save(db, book)
    return book


def test_future_authorized_admission_waits_without_reclassifying_historical_uncertainty_as_a_current_block(monitor):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    reviewed_permit(db, record, lane, AT+pd.Timedelta(minutes=15))
    publish()
    value = operational_status.read_status()["books"]["ema/day"]
    assert value["state"] == "WAITING"
    assert value["operational"]["entry_permission"]["reason_code"] == "WAIT_FUTURE_ADMISSION"
    assert value["historical_problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    assert value["research_state"] == "QUARANTINED"
    assert value["financial_reconciliation_complete"]


def test_newer_incident_revokes_an_old_saved_permission_in_read_only_projection(monitor):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    book = reviewed_permit(db, record, lane, AT)
    incidents.append(db, book, "PROTECTION_INCIDENT_IMPORTED", "later", AT,
                     {"generation": record["id"], "incident_id": "new-incident", "position_id": "later",
                      "historical_coverage": "UNRESOLVED", "current_feed_health": "UNKNOWN_HISTORICAL"}, identity="later")
    publish()
    before = db.execute("SELECT state FROM book WHERE id=?", (lane["book_id"],)).fetchone()[0]
    value = operational_status.read_status()["books"]["ema/day"]
    assert value["state"] == "BLOCKED" and not value["prospective_entry_permission"]
    assert value["operational"]["entry_permission"]["reason_code"] == "HISTORICAL_REVIEW_REQUIRED"
    assert db.execute("SELECT state FROM book WHERE id=?", (lane["book_id"],)).fetchone()[0] == before


def test_missing_problem_projection_does_not_hide_a_journal_incident_or_rewrite_account(monitor):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    assert book.get("problem") is None
    incidents.append(db, book, "PROTECTION_INCIDENT_IMPORTED", "past", AT,
                     {"generation": record["id"], "incident_id": "legacy-lost-problem", "position_id": "past",
                      "historical_coverage": "UNRESOLVED", "current_feed_health": "UNKNOWN_HISTORICAL"}, identity="legacy-lost-problem")
    publish()
    before = db.execute("SELECT state FROM book WHERE id=?", (lane["book_id"],)).fetchone()[0]
    value = operational_status.read_status()["books"]["ema/day"]
    assert value["state"] == "BLOCKED" and not value["prospective_entry_permission"]
    assert value["historical_problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    assert not value["accounting_complete"] and value["financial_reconciliation_complete"]
    assert value["research_state"] == "QUARANTINED"
    assert db.execute("SELECT state FROM book WHERE id=?", (lane["book_id"],)).fetchone()[0] == before


@pytest.mark.parametrize("damage", ["stale", "wrong_build", "corrupt"])
def test_unverified_managed_runner_cannot_admit_entries(monitor, damage):
    if damage == "corrupt":
        (gate.ROOT/"last-cycle.json").write_bytes(b'\x00\x00')
    else:
        value = json.loads((gate.ROOT/"last-cycle.json").read_text())
        value.update(at=(AT-pd.Timedelta(minutes=1)).isoformat()) if damage == "stale" else value.update(runner_build_id="older")
        write_json(gate.ROOT/"last-cycle.json", value)
    result = operational_status.read_status()
    assert all(not value["prospective_entry_permission"] for value in result["books"].values())
    assert all(value["state"] == "WAITING" for value in result["books"].values())


def test_undrained_delivery_is_not_reported_as_financial_or_entry_readiness(monitor, monkeypatch):
    _, record, _, _, _ = monitor
    lane = record["body"]["lanes"]["ema/day"]
    monkeypatch.setattr(delivery, "pending_books", lambda _: [{"book_id": lane["book_id"], "pending_count": 1, "loss_count": 0}])
    result = operational_status.read_status()
    assert not result["books"]["ema/day"]["prospective_entry_permission"]
    assert result["books"]["ema/day"]["operational"]["entry_permission"]["reason_code"] == "PROTECTION_DELIVERY_PENDING"
    assert result["books"]["ema/swing"]["prospective_entry_permission"]
