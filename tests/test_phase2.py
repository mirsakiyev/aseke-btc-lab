import httpx
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from lab.config import ROOT, load_profiles
from lab.data import normalize
from lab.execution import Setup, known_exit, trade_return
from lab.storage import utc

from lab.phase2 import store
from lab.phase2.audit_data import gaps, reconciliation
from lab.phase2.collector import collect
from lab.phase2.shadow import Shadow, commit_decision, new_account, nominal_open
from lab.phase2.shadow_ledger import account_view, advance


def raw_row(at, price=100, volume=2):
    return [int(at.timestamp()), price - 1, price + 1, price, price, volume]


def ingest(db, at, received, rows, seconds=300):
    end = at + pd.Timedelta(seconds=seconds)
    frame = normalize(rows, at, end, seconds, received, "fixture")
    return store.commit_response(db, seconds, at, end, received, "fixture", str(rows).encode(), frame)


def sample_intent(profile, close):
    fill = nominal_open(profile, close)
    deadline, time_exit, reason = known_exit(fill, profile, profile.costs)
    return {"fill_at": fill.isoformat(), "deadline": deadline.isoformat(), "time_exit": time_exit.isoformat(),
            "time_reason": reason, "units_btc": 0.25, "stop": 95, "costs": profile.costs.model_dump(),
            "risk_budget_usd": 25, "allocation_cap_usd": 2500}


def test_reconciliation_excludes_partial_fine_buckets_and_classifies_differences():
    start = utc("2025-01-01")
    fine_index = pd.date_range(start, periods=36, freq="5min")
    fine = pd.DataFrame({"open": 100, "close": 100, "high": 101, "low": 99, "volume": 2}, index=fine_index)
    hourly = pd.DataFrame({"open": [100, 100, 100], "close": [100, 101, 100],
                           "high": 101, "low": 99, "volume": 25},
                          index=pd.date_range(start, periods=3, freq="1h"))
    missing = start + pd.Timedelta("2h5min")
    fine = fine.drop(missing)
    result, detail = reconciliation(hourly, fine)
    assert result["overlapping_complete_hours"] == 2
    assert result["ohlc_hours"] == result["volume_only_hours"] == 1
    assert detail.loc[start, "relative_to_native_volume"] == pytest.approx(1 / 25)
    assert gaps(fine, start, start + pd.Timedelta("3h"), 300)[0]["start"] == missing


def test_flat_raw_price_still_charges_two_fees_and_two_adverse_prices():
    p = load_profiles()["day"]
    close = utc("2026-11-08T17:00Z")
    setup = Setup(close, close, close, 100, 95, close + pd.Timedelta("8h"), entry_raw=100, exit_raw=100)
    expected = (99.95 - 100.05 - 0.005 * (99.95 + 100.05)) / 100.05
    assert trade_return(setup, p.costs) == pytest.approx(expected)
    assert trade_return(setup, p.costs) * 100 == pytest.approx(-1.09945027486)


def test_native_bar_collection_resumes_and_publishes_revisions_without_rewriting(tmp_path, monkeypatch):
    from lab import data

    monkeypatch.setattr(data.time, "sleep", lambda _: None)
    start = utc("2025-01-01")
    responses = [
        [raw_row(start), raw_row(start + pd.Timedelta("5min"))],
        [raw_row(start, 101), raw_row(start + pd.Timedelta("5min")), raw_row(start + pd.Timedelta("10min"))],
    ]
    received = []

    def endpoint(request):
        received.append(request)
        return httpx.Response(200, json=responses.pop(0))

    db_path = tmp_path / "forward.sqlite3"
    with httpx.Client(transport=httpx.MockTransport(endpoint)) as client:
        with store.connect(db_path) as db:
            first = collect(db, client, start, start + pd.Timedelta("10min41s"), tmp_path)
        with store.connect(db_path) as db:
            assert collect(db, client, start, start + pd.Timedelta("11min"), tmp_path) == []
            second = collect(db, client, start, start + pd.Timedelta("15min41s"), tmp_path)
            assert second[0]["new_candles"] == second[0]["revisions"] == 1
            assert db.execute("SELECT open FROM candles WHERE open_epoch=? ORDER BY id",
                              (store.epoch(start),)).fetchall()[0][0] == 100
            assert store.bars(db, 300, start, start + pd.Timedelta("15min")).iloc[0].open == 101
    assert first[0]["new_candles"] == 2 and len(received) == 2


def test_bad_schema_keeps_raw_evidence_without_advancing_cursor(tmp_path, monkeypatch):
    from lab import data

    monkeypatch.setattr(data.time, "sleep", lambda _: None)
    start = utc("2025-01-01")
    with httpx.Client(transport=httpx.MockTransport(
        lambda _: httpx.Response(200, json=[[int(start.timestamp()), 1, 2, 3, 4]])
    )) as client, store.connect(tmp_path / "forward.sqlite3") as db:
        result = collect(db, client, start, start + pd.Timedelta("5min41s"), tmp_path)
        assert "error" in result[0]
        assert store.checkpoint(db, 300, start) == start
        assert db.execute("SELECT count(*) FROM payloads").fetchone()[0] == 1


def test_holdout_and_actual_activation_gate_precede_feature_or_model_access():
    shadow = Shadow.__new__(Shadow)
    shadow.boundary = utc("2026-11-08")
    p = load_profiles()["day"]
    experiment = {"observation_start_utc": "2026-11-08T00:15:00Z"}
    for close in (utc("2026-11-07T23:45Z"), utc("2026-11-08T00:00Z")):
        with pytest.raises(ValueError, match="permitted observation"):
            shadow.observe_decision(None, p, {}, close, experiment, {})


def test_intent_late_after_initial_commit_cannot_fill(tmp_path):
    p = load_profiles()["day"]
    close = utc("2026-11-08T16:00Z")
    intent = sample_intent(p, close)
    before, open_at = close + pd.Timedelta("40s"), utc(intent["fill_at"])
    clock = iter([before, open_at]).__next__
    state = new_account(p, close)
    with store.connect(tmp_path / "forward.sqlite3") as db:
        commit_decision(db, p, state, close, clock, {"decision_close": close, "outcome": "entry_intent"},
                        intent, "entry")
        assert store.get_account(db, p.id)["pending_entry"] is None
        assert store.events(db, p.id)[0]["kind"] == "intent_too_late_no_fill"


def test_late_decisions_are_separate_from_prospective_counts(tmp_path):
    p = load_profiles()["day"]
    close = utc("2026-11-08T16:00Z")
    state = new_account(p, close)
    with store.connect(tmp_path / "forward.sqlite3") as db:
        commit_decision(db, p, state, close, lambda: nominal_open(p, close),
                        {"decision_close": close, "outcome": "late_catch_up_no_intent"})
        assert state["prospective_decision_count"] == 0 and state["late_catchup_count"] == 1
        assert store.events(db, p.id)[0]["observation_quality"] == "late_catch_up"


def test_restart_resolves_one_precommitted_entry_and_entry_bar_stop(tmp_path):
    p = load_profiles()["day"]
    close = utc("2026-11-08T16:00Z")
    intent = sample_intent(p, close)
    fill = utc(intent["fill_at"])
    now = fill + p.step + pd.Timedelta("40s")
    state = new_account(p, close)
    path = tmp_path / "forward.sqlite3"
    with store.connect(path) as db:
        commit_decision(db, p, state, close, lambda: close + pd.Timedelta("40s"),
                        {"decision_close": close, "outcome": "entry_intent"}, intent, "entry")
        row = raw_row(fill)
        row[1] = 94
        ingest(db, fill, now, [row])
        advance(db, p, state, close, fill + p.step, fill + pd.Timedelta("2min"))
        assert store.get_account(db, p.id)["entry_fills"] == 0
    with store.connect(path) as db:
        state = store.get_account(db, p.id)
        advance(db, p, state, close, now, now)
        advance(db, p, state, close, now, now)
        kinds = [row["kind"] for row in store.events(db, p.id)]
        assert kinds.count("entry_fill") == kinds.count("exit_fill") == 1
        assert store.events(db, p.id)[0]["reason"] == "stop_intrabar"
        expected = .25 * (95 * .9995 * .995 - 100 * 1.0005 * 1.005)
        assert account_view(state)["equity_usd_at_last_mark"] == pytest.approx(10000 + expected)


def test_missing_day_deadline_keeps_open_position_and_equity_uncertain(tmp_path):
    p = load_profiles()["day"]
    close = utc("2026-11-08T21:45Z")
    intent = sample_intent(p, close)
    fill, deadline = utc(intent["fill_at"]), utc(intent["deadline"])
    assert deadline == utc("2026-11-08T23:55Z")
    state = new_account(p, close)
    path = tmp_path / "forward.sqlite3"
    with store.connect(path) as db:
        commit_decision(db, p, state, close, lambda: close + pd.Timedelta("40s"),
                        {"decision_close": close, "outcome": "entry_intent"}, intent, "entry")
        for at in pd.date_range(fill, deadline, freq=p.step, inclusive="left"):
            ingest(db, at, at + p.step + pd.Timedelta("40s"), [raw_row(at)])
        end = utc("2026-11-09T00:02Z")
        advance(db, p, state, close, end, end)
        assert state["position"] and state["closed_trades"] == 0
        assert state["unresolved"]["deadline_status"] == "unresolved_at_deadline"
        assert account_view(state)["equity_uncertain"]


def test_stale_heartbeat_blocks_entries_before_model_access():
    p = load_profiles()["swing"]
    shadow = Shadow.__new__(Shadow)
    now = utc("2026-11-08T20:00:40Z")
    shadow.clock = lambda: now
    close = now.floor("4h")
    assert shadow.entry_gate(p, {}, close, nominal_open(p, close), {}, None, {},
                             {"healthy": True, "received_at": str(now - pd.Timedelta("16s"))}) == (
        "public_heartbeat_stale", None,
    )


def test_dashboard_mode_changes_preserve_shadow_position_and_pnl(tmp_path, monkeypatch):
    from lab import data, research
    from lab.phase2 import collector

    p = load_profiles()["day"]
    at = utc("2026-11-08T16:00Z")
    path = tmp_path / "forward.sqlite3"
    state = new_account(p, at)
    state.update(cash=9974.875, realized_usd=12, mark_price=110, mark_at=at.isoformat(), entry_fills=1)
    state["position"] = {"intent": sample_intent(p, at), "entry_notional": 25, "entry_price": 100, "entry_fee": .125}
    with store.connect(path) as db, db:
        store.set_meta(db, "experiment", {"observation_start_utc": at.isoformat()})
        store.set_account(db, p.id, state)
        store.event(db, p.id, "activation", "observation_activated", at, {})
    monkeypatch.setattr(store, "DATABASE", path)
    monkeypatch.setattr(collector, "status", lambda: {"process_alive": False, "runner_stale": False})
    monkeypatch.setattr(data, "snapshot", lambda _: (pd.DataFrame(), {}))
    monkeypatch.setattr(research, "latest_run", lambda _: None)
    app = AppTest.from_file(str(ROOT / "dashboard.py")).run()
    for key in ("day", "swing", "day"):
        app.radio(key="profile").set_value(key).run()
        assert not app.exception
    assert any(m.label == "Realized P&L" and m.value == "$12.00" for m in app.metric)
    with store.connect(path, readonly=True) as db:
        assert store.get_account(db, p.id) == state
        assert len(store.events(db, p.id)) == 1
