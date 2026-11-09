import pandas as pd
import pytest

from lab.backtest import Simulation, benchmark, simulate, size_order
from lab.config import Execution, load_profiles
from lab.data import normalize
from lab.execution import Setup, plan_setups
from lab.phase2 import store
from lab.phase2.shadow import nominal_open
from lab.phase5.metrics import benchmark_metrics, calendar_years, geometric
from lab.phase5.observations import candle_receipts, classify, profile_observations
from lab.phase5.replays import validate_timing
from lab.phase5.sizing import quantity_plan
from lab.storage import utc


def test_independent_sizing_charges_both_sides_and_caps_notional_without_future_fill():
    profile = load_profiles()["swing"]
    costs = Execution(fee_bps=5, proxy_bps=5)
    close = utc("2025-01-01")
    estimated_entry, estimated_stop = 100 * 1.0005, 95 * .9995
    expected = estimated_entry * 1.0005 - estimated_stop * .9995
    plan = quantity_plan(100, 95, 10000, costs, profile)
    assert plan["planned_cost_inclusive_loss_per_btc_usd"] == pytest.approx(expected)
    assert plan["uncapped_risk_quantity_btc"] == pytest.approx(25 / expected)
    setup = Setup(close, close + pd.Timedelta("30s"), close + profile.step, 100., 95.,
                  close + pd.Timedelta("7d"), entry_raw=110.)
    assert size_order(setup, profile, costs, 10000)[0] == pytest.approx(plan["planned_quantity_btc"])
    cap = quantity_plan(100, 99.98, 10000, costs, profile)
    assert cap["binding_constraint"] == "entry_notional"
    assert cap["planned_entry_notional_usd"] == pytest.approx(2500)
    assert cap["planned_cost_inclusive_risk_usd"] < 25
    assert cap["planned_cash_outlay_usd"] <= 10000


def test_extra_hour_replays_precommitted_entry_without_prior_bar_stop_or_future_sizing():
    profile = load_profiles()["swing"]
    start = utc("2025-01-01")
    idx = pd.date_range(start, periods=8, freq=profile.step)
    end = idx[-1] + profile.step
    frame = pd.DataFrame({"open": 100., "low": 99., "high": 101., "close": 100., "volume": 1.,
                          "close_at": idx + profile.step}, index=idx)
    frame.loc[idx[1], "low"] = 90
    frame.loc[idx[2], ["open", "high"]] = 110
    signals = pd.DataFrame({"enter": [True, False], "exit": [False, True], "close": 100., "stop": 95.},
                           index=[idx[0], idx[4]])
    ref = Execution(fee_bps=5, proxy_bps=5)
    stress = Execution(fee_bps=5, proxy_bps=5, delay_bars=1)
    first = simulate(frame, plan_setups(frame, signals, profile, ref, end), profile, ref, start, end)
    delayed = simulate(frame, plan_setups(frame, signals, profile, stress, end), profile, stress, start, end)
    validate_timing(delayed, profile, stress, end)
    assert first.trades.entry_at.iloc[0] == idx[1] and first.trades.reason.iloc[0] == "stop_intrabar"
    assert delayed.trades.entry_at.iloc[0] == idx[2] and delayed.trades.exit_at.iloc[0] == idx[6]
    assert delayed.trades.available_at.iloc[0] == first.trades.available_at.iloc[0]
    assert delayed.trades.units_btc.iloc[0] == first.trades.units_btc.iloc[0]
    assert delayed.trades.entry_price.iloc[0] == pytest.approx(110 * 1.0005)


def test_calendar_benchmark_fees_are_only_allocated_at_initial_entry_and_truncation_has_no_cagr():
    start = utc("2023-12-31T22:00Z")
    cutoff = utc("2024-01-01")
    end = utc("2024-01-01T02:00Z")
    idx = pd.date_range(start, end, freq="1h", inclusive="left")
    candles = pd.DataFrame({"open": 100., "close": [100, 125, 120, 150]}, index=idx)
    costs = Execution(fee_bps=5)
    cash = 10000.
    curve = {name: benchmark(candles, cash, costs, allocation)[0]
             for name, allocation in (("cash_benchmark", 0), ("full_btc", 1), ("passive_25pct", .25))}
    sim = Simulation({}, pd.DataFrame(curve, index=idx + pd.Timedelta("1h")), pd.DataFrame(), pd.DataFrame())
    assert list(calendar_years(start, end)) == [("2023", start, cutoff), ("2024", cutoff, end)]
    whole = benchmark_metrics(sim, costs, costs, 100., cash, start, start, end, False)
    first = benchmark_metrics(sim, costs, costs, 100., cash, start, start, cutoff, False)
    later = benchmark_metrics(sim, costs, costs, 100., cash, start, cutoff, end, False)
    for key in curve:
        assert first[key]["net_pnl_usd"] + later[key]["net_pnl_usd"] == pytest.approx(whole[key]["net_pnl_usd"])
        assert later[key]["fees_usd"] == 0
        assert later[key]["btc_units"] == first[key]["btc_units"]
    assert geometric(.0423, utc("2023-07-01"), utc("2025-11-08"), False) is None
    assert geometric(.0423, utc("2023-07-01"), utc("2025-11-08"), True) == pytest.approx(.01773, abs=.0001)


def test_prospective_timing_separates_pre_open_late_causal_catchup_and_unknown():
    profile = load_profiles()["day"]
    close = utc("2026-11-08T22:30Z")
    cutoff = nominal_open(profile, close)
    now = close + pd.Timedelta("400s")
    observed = close + pd.Timedelta("180s")
    evidence = {"first_locally_available_at": observed, "selected_revisions_received_by": observed}

    def record(record_lag, input_lag, reason):
        actual = close + pd.Timedelta(seconds=record_lag)
        return {"decision_recorded_at": actual, "recorded_at": actual, "modeled_fill_at": cutoff,
                "feature_input_delay_seconds": input_lag, "decision_outcome": reason, "feature_ready": True}

    timely = record(220, 210, "no_baseline_breakout_or_context_trend")
    result = classify(profile, close, now, timely, evidence)
    assert result["classification"] == "timely_before_modeled_open"  # No 120s decision SLA.
    assert result["no_signal"] and not result["session_allows_entry_at_open"]
    assert classify(profile, close, now, timely)["classification"] == "unknown_timing"
    assert classify(profile, close, now, record(310, 210, "missed_execution_open"), evidence)["classification"] == (
        "late_causal_computation"
    )
    assert classify(profile, close, now, record(320, 320, "late_catch_up_no_intent"))["classification"] == (
        "late_catchup_reconstruction"
    )
    assert classify(profile, close, cutoff, None)["classification"] == "missing"
    assert classify(profile, close, cutoff - pd.Timedelta("1s"), None)["classification"] == "pending_deadline"


def test_timing_uses_original_receipts_and_excludes_unregistered_time(tmp_path):
    profile = load_profiles()["day"]
    close = utc("2026-11-08T22:30Z")
    activation = close - pd.Timedelta("3min")
    boundary = close.normalize()
    opens = pd.date_range(close - pd.Timedelta("15min"), close, freq=profile.step, inclusive="left")
    path = tmp_path / "timing.sqlite3"
    with store.connect(path) as db:
        for i, at in enumerate(opens):
            end = at + profile.step
            receipt = end + pd.Timedelta(seconds=40 + i)
            raw = [[store.epoch(at), 99, 101, 100, 100, 1]]
            candles = normalize(raw, at, end, profile.seconds, receipt, f"fixture{i}")
            store.commit_response(db, profile.seconds, at, end, receipt, "fixture", str(raw).encode(), candles)
        at = opens[-1]
        later = close + pd.Timedelta("12min")
        raw = [[store.epoch(at), 98, 102, 100, 101, 2]]
        revised = normalize(raw, at, close, profile.seconds, later, "revision")
        store.commit_response(db, profile.seconds, at, close, later, "fixture", str(raw).encode(), revised)
        data = {"latest_forward_revision_id": 3, "last_observed_close": close}
        stamp = close + pd.Timedelta("45s")
        experiment = {"profile_hashes": {"day": profile.identity}, "model_versions": {"day": "fixture"}}
        row = {"decision_close": close, "decision_recorded_at": stamp, "modeled_fill_at": nominal_open(profile, close),
               "feature_input_delay_seconds": 44, "profile_hash": profile.identity, "model_version": "fixture",
               "observation_start_utc": activation, "data": data, "feature_ready": False,
               "decision_outcome": "baseline_warmup_or_context_gap"}
        with db:
            store.event(db, "day", f"decision:{close.isoformat()}", "decision", stamp, row)
        receipt = candle_receipts(db, profile, close, row, boundary)
        assert receipt["selected_revisions_received_by"] == close + pd.Timedelta("42s")
        report = profile_observations(db, profile, activation, boundary, boundary,
                                      close + pd.Timedelta("7min"), experiment)
    assert report["assessable_opportunities"] == report["timely_verified_records"] == 1
    assert report["data_blocked_records"] == 1 and report["timely_feature_ready_records"] == 0
    assert report["preactivation_or_preholdout_opportunities_excluded"] > 80
