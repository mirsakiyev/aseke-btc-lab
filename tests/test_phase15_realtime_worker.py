from contextlib import closing
from copy import deepcopy

import pandas as pd
import pytest

from lab.learning.design import config as learning_config
from lab.phase9.reference import reference
from lab.phase15 import catalog, paper, realtime, realtime_worker as runner, worker
from lab.storage import file_hash, utc, write_json
from test_phase15_realtime import BOUNDARY, COMMITTED, CONFIRMED, DECIDED, FIRST_MINUTE, MARK, analysis, minute_bar, observation


@pytest.fixture
def registered(tmp_path, monkeypatch):
    frozen_root = tmp_path / "frozen"
    new_root = tmp_path / "new"
    monkeypatch.setattr(catalog, "ROOT", frozen_root)
    monkeypatch.setattr(runner, "ROOT", new_root)
    monkeypatch.setattr(runner.monitoring, "RUNTIME", tmp_path / "shared-runtime.json")
    config = reference()
    lanes = catalog.rules(config, {"design": {"settings": learning_config()}})
    for key, lane in lanes.items():
        lane["book_id"] = f"frozen-{key}"
    original = {"source_protocol": "observed", "phase14_amendment": "amendment", "lanes": lanes, "config": config,
                "start": "2026-11-09T21:45:00Z", "follow_end_exclusive": "2027-01-09T21:45:00Z",
                "holdout_end_exclusive": "2026-11-08T00:00:00Z", "max_live_delay_seconds": 300,
                "max_batch_minutes": 180, "gap_grace_seconds": 2700, "funding_grace_seconds": 7200}
    baseline = {"id": "frozen-registration", "body": original}
    monkeypatch.setattr(catalog, "registered", lambda: baseline)
    monkeypatch.setattr(runner, "futures_source", lambda body: {"id": body["phase14_amendment"]})
    monkeypatch.setattr(runner.protocol, "active", lambda: {"id": "observed", "body": {"end_exclusive": "2027-02-01T00:00:00Z"}})
    monkeypatch.setattr(runner.protocol, "control", lambda: {"enabled": True})
    monkeypatch.setattr(worker, "monitoring_controls", lambda: {"monitor": True, "ideas": {"day": True, "swing": True}})
    rules = {"as_of": BOUNDARY.isoformat(), "contract": {"status": "Open", "symbol": "XBTUSDTM", "multiplier": .001,
             "lotSize": 1., "tickSize": .1, "maxLeverage": 100., "marketMaxOrderQty": 2000.},
             "isolated": [{"level": 1, "maxRiskLimit": 250000., "maxLeverage": 100., "maintainMargin": .004}]}
    monkeypatch.setattr(runner.protocol, "current_metadata", lambda: rules)
    record = runner.register(clock=lambda: BOUNDARY - pd.Timedelta(minutes=16))
    return frozen_root, new_root, lanes, rules, record


def draft(record, rules, *, expiry=None):
    config = record["body"]["config"]
    options = {"expiry": expiry} if expiry else {}
    signal = analysis(rules, config, 1, **options)
    receipt = {"at": BOUNDARY.isoformat(), "received_at": BOUNDARY.isoformat()}
    signal["plan"].update(quote_receipt=receipt, mark_receipt=receipt)
    return signal


def test_default_off_and_new_accounts_leave_frozen_ledger_identical(registered, monkeypatch):
    frozen_root, new_root, lanes, rules, record = registered
    assert not runner.status()["entries_enabled"]
    assert not (new_root / "paper.sqlite3").exists()
    assert record["body"]["mark_source_policy"] == realtime.SOURCE_POLICY
    assert set(record["body"]["lanes"]) == set(runner.LANES)
    assert all(lane["book_id"] != lanes[name]["book_id"] for name, lane in record["body"]["lanes"].items())
    assert utc(record["body"]["registered_at"]) < utc(record["body"]["start"])
    assert runner.register(clock=lambda: COMMITTED) == record

    baseline_lanes = deepcopy(lanes)
    with closing(catalog.db_open()) as db:
        for lane in lanes.values():
            account = paper.initial(lane)
            account["cash"] -= 40.
            paper.save(db, account)
            paper.event(db, account, "CLOSED", "historical", BOUNDARY, BOUNDARY, {"fixture": True})
        db.commit()
    baseline_hash = file_hash(frozen_root / "paper.sqlite3")
    request_times = []

    def observe(db, root, *, idea_ids, clock, transport):
        assert root == new_root and not db.in_transaction
        assert db.execute("SELECT 1 FROM event WHERE kind='PUBLISHED'").fetchone()
        request_times.append(clock())
        return observation()

    monkeypatch.setattr(realtime, "observe", observe)
    monkeypatch.setattr(worker, "opportunity_rows", lambda body, *_: {
        key: draft(record, rules) for key in body["body"]["lanes"] if key == "day/fixed"})
    assert not runner.cycle(clock=lambda: CONFIRMED)["confirmed"]
    runner.set_entries(True, clock=lambda: CONFIRMED)
    assert list(runner.cycle(clock=lambda: CONFIRMED)["confirmed"]) == ["day/fixed"]
    assert not request_times
    assert runner.cycle(clock=lambda: DECIDED)["entry_outcomes"] == {"day/fixed": "HYPOTHETICAL_FILL"}
    assert request_times == [DECIDED]
    day = runner.status()["books"]["day/fixed"]
    assert day["position"]["position"]["entry"] == MARK
    assert utc(day["position"]["entry_at"]) == DECIDED
    assert day["cash_reconciliation_difference"] == pytest.approx(0., abs=1e-9)
    assert file_hash(frozen_root / "paper.sqlite3") == baseline_hash
    assert lanes == baseline_lanes


def test_existing_position_stops_before_entry_feed_io_failure(registered, monkeypatch):
    _, _, _, rules, record = registered
    runner.set_entries(True, clock=lambda: CONFIRMED)
    day = record["body"]["lanes"]["day/fixed"]
    swing = record["body"]["lanes"]["swing/fixed"]
    config = record["body"]["config"]
    late = utc("2026-11-16T06:30:51Z")
    with closing(runner.connect()) as db:
        day_book, swing_book = (paper.initial(lane) for lane in (day, swing))
        realtime.confirm(db, day_book, day, draft(record, rules), CONFIRMED)
        realtime.confirm(db, swing_book, swing, draft(record, rules, expiry=late + pd.Timedelta(minutes=2)), CONFIRMED)
        assert realtime.consider(db, day_book, day, rules, config, observation(), clock=lambda: COMMITTED) == "HYPOTHETICAL_FILL"
        for book in (day_book, swing_book):
            paper.save(db, book)
        db.commit()

    partial = minute_bar(FIRST_MINUTE, MARK)
    next_at = FIRST_MINUTE + catalog.STEP
    stop = minute_bar(next_at, 94.)
    paired = {basis: {FIRST_MINUTE: partial, next_at: stop} for basis in ("trade", "mark")}
    for module in (runner, worker):
        monkeypatch.setattr(module, "futures_window", lambda *_: (paired, {}, {}, True))

    def failed_observe(db, *_args, **_kwargs):
        assert not db.in_transaction
        assert db.execute("SELECT 1 FROM event WHERE book=? AND kind='CLOSED'", (day["book_id"],)).fetchone()
        raise OSError("fixture outage")

    monkeypatch.setattr(realtime, "observe", failed_observe)
    result = runner.cycle(clock=lambda: late)
    assert result["diagnostics"] == {"mark_source": "OSError"}
    books = runner.status()["books"]
    assert books["swing/fixed"]["pending"]["last_mark_rejection"] == "ENTRY_SOURCE_IO_ERROR"
    assert books["day/fixed"]["recent_closed"][0]["reason"] == "stop"
    assert books["day/fixed"]["cash_reconciliation_difference"] == pytest.approx(0., abs=1e-9)

    runner.set_entries(False, clock=lambda: late)
    monkeypatch.setattr(realtime, "observe", lambda *_a, **_k: pytest.fail("disabled protocol cannot request a mark"))
    runner.cycle(clock=lambda: late)
    swing_book = runner.status()["books"]["swing/fixed"]
    assert swing_book["pending"] is None
    assert swing_book["counts"]["NO_FILL"] == 1
    assert swing_book["entry_evidence_reasons"]["MARK_REFERENCE_ENTRIES_DISABLED_OR_PERMISSION_CLOSED"] == 1


def test_shared_monitor_pause_and_retry_suppress_extra_requests(registered, monkeypatch):
    _, _, _, rules, record = registered
    runner.set_entries(True, clock=lambda: CONFIRMED)
    monkeypatch.setattr(worker, "monitoring_controls", lambda: {"monitor": True, "paused": True})
    assert runner.allowed_profiles(record, CONFIRMED) == set()
    monkeypatch.setattr(worker, "monitoring_controls", lambda: {"monitor": True, "ideas": {"day": True, "swing": True}})
    monkeypatch.setattr(worker, "opportunity_rows", lambda *_: {})
    monkeypatch.setattr(realtime, "observe", lambda *_a, **_k: pytest.fail("shared backoff must suppress the GET"))
    write_json(runner.monitoring.RUNTIME, {"retry_at": (DECIDED + pd.Timedelta(seconds=30)).isoformat()})
    lane = record["body"]["lanes"]["day/fixed"]
    with closing(runner.connect()) as db:
        book = paper.initial(lane)
        realtime.confirm(db, book, lane, draft(record, rules), CONFIRMED)
        paper.save(db, book)
        db.commit()
    runner.cycle(clock=lambda: DECIDED)
    pending = runner.status()["books"]["day/fixed"]["pending"]
    assert pending["last_mark_rejection"] == "SHARED_SOURCE_BACKOFF"
    assert pending.get("mark_attempts", 0) == 0


def test_ambiguous_entry_minute_is_visible_and_never_used_for_exit(registered, monkeypatch):
    _, _, _, rules, record = registered
    day = record["body"]["lanes"]["day/fixed"]
    late = utc("2026-11-16T06:30:51Z")
    with closing(runner.connect()) as db:
        book = paper.initial(day)
        realtime.confirm(db, book, day, draft(record, rules), CONFIRMED)
        assert realtime.consider(db, book, day, rules, record["body"]["config"], observation(), clock=lambda: COMMITTED)
        paper.save(db, book)
        db.commit()
    partial = {**minute_bar(FIRST_MINUTE, MARK), "low": 94.}
    next_at = FIRST_MINUTE + catalog.STEP
    take = minute_bar(next_at, 112.)
    paired = {basis: {FIRST_MINUTE: partial, next_at: take} for basis in ("trade", "mark")}
    for module in (runner, worker):
        monkeypatch.setattr(module, "futures_window", lambda *_: (paired, {}, {}, True))
    runner.cycle(clock=lambda: late)
    book = runner.status()["books"]["day/fixed"]
    assert book["book_status"] == "UNRESOLVED_ENTRY_INTERVAL" and not book["accounting_complete"]
    assert book["entry_interval_unresolved"]["possible_events"] == ["stop"]
    closed, = book["recent_closed"]
    assert utc(closed["exit_at"]) == next_at and closed["provisional"]
    assert closed["reason"] == "target"
