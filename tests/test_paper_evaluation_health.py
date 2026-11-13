from contextlib import closing
import json
import sqlite3

import pandas as pd
import pytest

from lab.health import evaluation, state
from lab.kraken_paper import source
from lab.storage import utc, write_json


FIRST = utc("2026-11-13T00:00:00Z")
ROUTE = "a" * 64


def _receipt(db, close, step, name, delay):
    opened = close - step
    available = close + pd.Timedelta(seconds=delay)
    db.execute("INSERT INTO receipt VALUES(?,?,?,?,?,?,?,?,?,?)",
               (name, "stream", int(step.total_seconds() // 60), close.value, available.value,
                available.value, opened.value, close.value, 200, "raw"))
    db.execute("INSERT INTO observation VALUES(?,?,?,?,?)",
               ("stream", int(step.total_seconds() // 60), opened.value, "{}", name))


@pytest.mark.parametrize("profile,minutes", [("day", 15), ("swing", 240)])
def test_ema_missing_boundaries_alert_once_and_recover_after_real_no_trade(tmp_path, monkeypatch, profile, minutes):
    monkeypatch.setattr(state, "ROOT", tmp_path)
    step = pd.Timedelta(minutes=minutes)
    grace = pd.Timedelta(minutes=6)
    lane = {"profile": profile, "execution_clock": {"decision_seconds": minutes * 60},
            "source": {"decision_receipt_seconds": 120, "publication_max_seconds": 360}}
    ready = {"source_id": "stream", "first_eligible_at": FIRST.isoformat(),
             "primary_end_exclusive": (FIRST + 20 * step).isoformat()}
    saved = {**ready, "last_evaluated_at": FIRST.isoformat(),
             "last_signal": {"decision": "NO_TRADE", "observed_at": (FIRST + pd.Timedelta(minutes=2)).isoformat()}}
    name = "ema/" + profile
    with closing(sqlite3.connect(":memory:")) as market, state.connect() as db:
        market.executescript(source.SCHEMA)
        db.execute(evaluation.SCHEMA)
        for index in range(4):
            _receipt(market, FIRST + index * step, step, f"receipt-{index}", 30)

        now = FIRST + step + grace
        status = evaluation._record_ema(db, market, lane, saved, now)
        assert len(status["overdue"]) == 1
        evaluation._alerts(db, {name: status}, now, ROUTE)
        assert db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 0

        now = FIRST + 2 * step + grace
        status = evaluation._record_ema(db, market, lane, saved, now)
        assert status["state"] == "STALE"
        assert [row["receipt_id"] for row in status["overdue"]] == ["receipt-1", "receipt-2"]
        evaluation._alerts(db, {name: status}, now, ROUTE)
        evaluation._alerts(db, {name: status}, now, ROUTE)
        db.commit()

    # Reopen the watchdog to test episode durability. An elapsed-window marker
    # is not an evaluation, even though its status says NO_TRADE.
    with state.connect() as db, closing(sqlite3.connect(":memory:")) as market:
        market.executescript(source.SCHEMA)
        _receipt(market, FIRST + 3 * step, step, "receipt-3", 30)
        now = FIRST + 3 * step + pd.Timedelta(minutes=2)
        expired = {**ready, "last_evaluated_at": (FIRST + 2 * step).isoformat(),
                   "last_signal": {"decision": "NO_TRADE", "reason": "Publication window elapsed"}}
        assert evaluation._record_ema(db, market, lane, expired, now)["state"] == "STALE"
        resumed = {**ready, "last_evaluated_at": (FIRST + 3 * step).isoformat(),
                   "last_signal": {"decision": "NO_TRADE", "observed_at": now.isoformat()}}
        status = evaluation._record_ema(db, market, lane, resumed, now)
        assert status["state"] == "TRACKING"
        evaluation._alerts(db, {name: status}, now, ROUTE)
        evaluation._alerts(db, {name: status}, now, ROUTE)
        queued = [json.loads(row[0]) for row in db.execute("SELECT event_json FROM outbox ORDER BY rowid")]
        assert [row["event"] for row in queued] == ["STALE", "RECOVERED"]
        assert [row["receipt_id"] for row in queued[0]["overdue"]] == ["receipt-1", "receipt-2"]
        assert all(row["private_route_id"] == ROUTE for row in queued)


def test_phase15_freshness_requires_prospective_source_and_managed_decision(tmp_path, monkeypatch):
    from lab.phase10.worker import decision_key

    monkeypatch.setattr(state, "ROOT", tmp_path / "health")
    step = pd.Timedelta(minutes=15)
    lane = {"profile": "day", "profile_rules": {"decision": "15min"}}
    record = {"body": {"config": {"ideas": {"version": "v"}}, "max_live_delay_seconds": 300,
                       "start": FIRST.isoformat(), "publication_end_exclusive": (FIRST + 10 * step).isoformat(),
                       "lanes": {"day/fixed": lane}}}
    managed = {"at": (FIRST + pd.Timedelta(minutes=1)).isoformat(), "opportunities": {
        "day/fixed": {"source_cutoff": FIRST.isoformat(), "latest_candle_receipt": FIRST.isoformat(),
                      "gates": {"feature readiness": "PASSED"}, "decision": "NO_TRADE"},
    }}
    with state.connect() as db:
        db.execute(evaluation.SCHEMA)
        for index in (1, 2, 3, 4):
            at = FIRST + index * step
            path = tmp_path / "phase10" / "decisions" / "day" / (decision_key("day", at, "v") + ".json")
            write_json(path, {"status": "CATCH_UP_ONLY" if index == 2 else "NO_TRADE",
                              "source_receipt": (at + pd.Timedelta(seconds=50)).isoformat(),
                              "idea": {"source_last_close": at.isoformat()}})
            result = evaluation._record_breakout(db, tmp_path, record, managed, at + pd.Timedelta(minutes=5))
            evaluation._alerts(db, result, at + pd.Timedelta(minutes=5), None)
            assert result["breakout/day/fixed"]["state"] == ("STALE" if index == 4 else "TRACKING")
        assert db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1


def test_first_watchdog_run_retains_pre_restart_decision_and_missed_boundaries(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path)
    step = pd.Timedelta(minutes=15)
    delay = pd.Timedelta(minutes=2)
    original = {"last_evaluated_at": FIRST.isoformat(),
                "last_signal": {"decision": "NO_TRADE", "observed_at": (FIRST + delay).isoformat()}}
    path = tmp_path / "ema_paper" / "paper.sqlite3"
    path.parent.mkdir()
    with closing(sqlite3.connect(path)) as ledger, ledger:
        ledger.execute("CREATE TABLE registration(profile TEXT, body TEXT)")
        ledger.execute("CREATE TABLE readiness(book TEXT, body TEXT)")
        ledger.execute("INSERT INTO registration VALUES('day','{\"book_id\":\"original\"}')")
        ledger.execute("INSERT INTO readiness VALUES('original',?)", (json.dumps(original),))
    before = evaluation.baseline(tmp_path)
    assert before == {"day": (FIRST, FIRST + delay, "NO_TRADE")}
    resumed = FIRST + 3 * step
    now = resumed + delay
    lane = {"profile": "day", "execution_clock": {"decision_seconds": 900},
            "source": {"decision_receipt_seconds": 120, "publication_max_seconds": 360}}
    latest = {"source_id": "stream", "first_eligible_at": FIRST.isoformat(),
              "primary_end_exclusive": (FIRST + 20 * step).isoformat(),
              "last_evaluated_at": resumed.isoformat(),
              "last_signal": {"decision": "NO_TRADE", "observed_at": now.isoformat()}}
    with closing(sqlite3.connect(":memory:")) as market, state.connect() as db:
        market.executescript(source.SCHEMA)
        db.execute(evaluation.SCHEMA)
        for index in range(4):
            _receipt(market, FIRST + index * step, step, f"receipt-{index}", 30)
        status = evaluation._record_ema(db, market, lane, latest, now, before["day"])
        assert status["counts"] == {"COMPLETED": 2, "MISSED_QUALIFIED_SOURCE": 2}
        assert status["state"] == "TRACKING"
