import numpy as np
import pandas as pd
import pytest

from lab.config import Execution, Profile, load_profiles
from lab.data import normalize
from lab.execution import known_exit, plan_setups
from lab.features import feature_columns
from lab.phase2 import shadow, store
from lab.phase2.shadow_ledger import advance
from lab.phase3.design import calendar
from lab.phase3.observation import session_entry_allowed
from lab.phase3.portfolio import fixed_ledger_fee, run_scenario
from lab.storage import utc


def market(start, end, step, price=100.0):
    index = pd.date_range(start, end, freq=step, inclusive="left")
    return pd.DataFrame({"open": price, "high": price + 1, "low": price - 1, "close": price,
                         "volume": 1.0, "close_at": index + step}, index=index)


def test_frozen_calendar_is_consecutive_and_holdout_exclusive():
    first = utc("2020-08-18")
    final = utc("2025-11-08")
    folds = calendar(first, final)
    assert len(folds) == 6
    assert utc(folds[0]["training_cutoff_utc"]) == utc("2023-01-01")
    assert all(a["evaluation_end_exclusive_utc"] == b["evaluation_start_utc"] for a, b in zip(folds, folds[1:]))
    assert utc(folds[-1]["evaluation_end_exclusive_utc"]) == final and folds[-1]["partial"]
    assert len(calendar(utc("2010-01-01"), final)) == 12


@pytest.mark.parametrize(("decision", "delay", "opening", "allowed"), [
    ("21:50", 0, "21:55", True),
    ("21:55", 0, "22:00", False),
    ("22:30", 0, "22:35", False),
    ("21:50", 1, "22:00", False),
])
def test_day_gate_with_positive_candidate_and_baseline(decision, delay, opening, allowed, monkeypatch):
    day = load_profiles()["day"]
    profile = Profile(**{**day.model_dump(), "costs": Execution(delay_bars=delay).model_dump()})
    close = utc(f"2026-11-09T{decision}Z")
    expected = utc(f"2026-11-09T{opening}Z")
    end = utc("2026-11-10")
    candles = market(close.floor(profile.step), end, profile.step)
    signals = pd.DataFrame({"enter": [True], "exit": [False], "close": [100.0], "stop": [95.0]}, index=[close])
    historical = plan_setups(candles, signals, profile, profile.costs, end)[0]
    assert historical.entry_at == expected and (historical.entry_raw is not None) == allowed
    assert session_entry_allowed(profile, close, expected) == allowed
    assert shadow.nominal_open(profile, close) == expected
    monkeypatch.setattr(shadow, "load_profiles", lambda: {"day": profile})
    runner = shadow.Shadow.__new__(shadow.Shadow)
    now = close + pd.Timedelta("40s")
    runner.clock = lambda: now

    class PositiveModel:
        def predict(self, values):
            return np.repeat(.01, len(values))

    runner.predictors, runner.model_issues = {"day": PositiveModel()}, {}
    features = {name: .1 for name in feature_columns(profile)}
    row = pd.Series({**features, "baseline_ready": True, "ready": True, "enter": True, "close": 100.0, "stop": 95.0})
    record = {}
    reason, intent = runner.entry_gate(profile, shadow.new_account(profile, close), close, expected,
                                      {"entry_threshold": 0, "model_version": "v", "artifact_sha256": "h"}, row,
                                      record, {"healthy": True, "received_at": now})
    assert record["score_decimal"] == .01
    assert reason == ("entry_intent" if allowed else "entry_cutoff")
    assert (intent is not None) == allowed


def test_restart_with_existing_position_and_pending_exit_after_day_cutoff(tmp_path):
    profile = load_profiles()["day"]
    close = utc("2026-11-09T21:50Z")
    due = shadow.nominal_open(profile, close)
    deadline, time_exit, reason = known_exit(due, profile, profile.costs)
    intent = {
        "fill_at": due.isoformat(), "deadline": deadline.isoformat(), "time_exit": time_exit.isoformat(),
        "time_reason": reason, "units_btc": .25, "stop": 95, "costs": profile.costs.model_dump(),
        "risk_budget_usd": 25, "allocation_cap_usd": 2500, "model_version": "pinned-v1",
        "profile_hash": profile.identity, "strategy_version": profile.strategy_version,
    }
    path = tmp_path / "shadow.sqlite3"
    state = shadow.new_account(profile, close)
    exit_signal = utc("2026-11-09T22:30Z")
    exit_due = shadow.nominal_open(profile, exit_signal)
    with store.connect(path) as db:
        shadow.commit_decision(db, profile, state, close, lambda: close + pd.Timedelta("40s"),
                               {"decision_close": close, "outcome": "entry_intent"}, intent, "entry")
        insert_bars(db, profile, due, exit_signal, 100)
        advance(db, profile, state, close, exit_signal, exit_signal + pd.Timedelta("40s"))
        runner = shadow.Shadow.__new__(shadow.Shadow)
        runner.clock = lambda: exit_signal + pd.Timedelta("40s")
        runner.evaluate_row(db, profile, state, exit_signal, exit_due, {},
                            pd.Series({"exit": True}), {"decision_close": exit_signal}, {"healthy": False})
        saved = store.get_account(db, "day")
        keys = [row[0] for row in db.execute("SELECT key FROM events ORDER BY id")]
    assert not session_entry_allowed(profile, exit_signal, exit_due)
    assert saved["position"]["signal_exit"]["fill_at"] == exit_due.isoformat()
    assert saved["position"]["intent"]["model_version"] == "pinned-v1"
    assert saved["position"]["intent"]["deadline"] == "2026-11-09T23:55:00+00:00"
    entry_notional = .25 * 100 * 1.0005
    buy_fee = entry_notional * .005
    assert saved["cash"] == pytest.approx(10000 - entry_notional - buy_fee)
    assert saved["fees_usd"] == pytest.approx(buy_fee)
    assert saved["realized_usd"] == 0
    assert saved["position"]["intent"]["units_btc"] == .25
    with store.connect(path) as db:
        restored = store.get_account(db, "day")
        assert restored == saved
        insert_bars(db, profile, exit_signal, exit_due + profile.step, 110)
        now = exit_due + profile.step + pd.Timedelta("40s")
        advance(db, profile, restored, close, now, now)
        advance(db, profile, restored, close, now, now)
        all_keys = [row[0] for row in db.execute("SELECT key FROM events ORDER BY id")]
        events = store.events(db, "day")
    sell = .25 * 110 * .9995
    net = sell * .995 - entry_notional - buy_fee
    assert all_keys[:len(keys)] == keys and len(all_keys) == len(set(all_keys)) == len(keys) + 1
    assert restored["closed_trades"] == 1 and restored["entry_fills"] == 1
    assert restored["fees_usd"] == pytest.approx(buy_fee + sell * .005)
    assert restored["cash"] == pytest.approx(10000 + net)
    assert restored["realized_usd"] == pytest.approx(net) and restored["position"] is None
    assert events[0]["originating_intent"]["model_version"] == "pinned-v1" and events[0]["reason"] == "signal"


def insert_bars(db, profile, start, end, price):
    payload = [[int(at.timestamp()), price - 1, price + 1, price, price, 1]
               for at in pd.date_range(start, end, freq=profile.step, inclusive="left")]
    receipt = end + pd.Timedelta("40s")
    data = normalize(payload, start, end, profile.seconds, receipt, "fixture")
    store.commit_response(db, profile.seconds, start, end, receipt, "fixture", str(payload).encode(), data)


@pytest.mark.parametrize(("boundary", "carried"), [("21:00", 0), ("23:00", 1)])
def test_portfolio_carries_pending_and_open_trade_with_original_fold(tmp_path, boundary, carried):
    profile = load_profiles()["swing"]
    start = utc("2025-01-01T20:00Z")
    split = utc(f"2025-01-01T{boundary}Z")
    end = utc("2025-01-02T12:00Z")
    second = utc("2025-01-02T00:00Z")
    exit_at = utc("2025-01-02T04:00Z")
    candles = market(start, end, profile.step)
    signals = pd.DataFrame({"enter": [True, True, False], "exit": [False, False, True],
                            "close": 100.0, "stop": 95.0}, index=[start, second, exit_at])
    forecasts = pd.DataFrame({"fold": ["f01", "f02"], "fit_eligible": True,
                              "ridge_accepted": [True, False], "mean_accepted": [True, False],
                              "ridge_score": [.01, -.01], "mean_score": [.01, -.01],
                              "ridge_model_id": ["old", "new"], "mean_model_id": ["mean-old", "mean-new"]},
                             index=[start, second])
    folds = [{"id": key, "fit_state": "fitted", "evaluation_start_utc": left, "evaluation_end_exclusive_utc": right}
             for key, left, right in [("f01", start, split), ("f02", split, end)]]
    report, simulations = run_scenario(candles, signals, forecasts, folds, profile, profile.costs, tmp_path / "result",
                                      "reference")
    journal = simulations["ridge"].trades
    assert len(journal) == 1 and journal.iloc[0].model_version == "old"
    assert journal.iloc[0].signal_fold == "f01"
    assert report["folds"]["f02"]["methods"]["ridge"]["positions_carried_in"] == carried
    left = report["folds"]["f01"]["methods"]["ridge"]["ending_equity_usd"]
    assert report["folds"]["f02"]["methods"]["ridge"]["starting_equity_usd"] == left


def test_fixed_ledger_fee_uses_net_before_fees_and_actual_notional():
    assert fixed_ledger_fee(90, 10, 20000)["break_even_fee_bps_per_side"] == 40
    assert fixed_ledger_fee(5, 10, 20000)["status"] == "no_feasible_nonnegative_fee"
