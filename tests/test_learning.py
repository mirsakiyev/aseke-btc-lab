from dataclasses import replace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from lab.learning import control, state
from lab.learning.data import Market
from lab.learning.design import catalog, config, window
from lab.learning.diagnostics import diagnostics, fixture
from lab.learning.evaluation import matured, outcome_identity
from lab.learning.features import build_features, confirmed_pivot
from lab.learning.simulator import Cost, Position, entry_fill, run_trade, size_order, triggers
from lab.learning.study import observer_snapshot
from lab.phase2 import store


def test_confirmed_pivots_and_context_use_only_completed_inputs():
    values = pd.Series([1., 2., 5., 3., 2., 6., 2., 1.])
    confirmed = confirmed_pivot(values, True)
    assert confirmed.iloc[:4].isna().all()
    assert confirmed.iloc[4] == 5
    for length in range(2, len(values)):
        pd.testing.assert_series_equal(confirmed.iloc[:length], confirmed_pivot(values.iloc[:length], True))
    index = pd.date_range("2024-01-01", periods=100, freq="1D", tz="UTC")
    candles = pd.DataFrame({"open": 100., "high": 105., "low": 95., "close": range(100, 200),
                            "volume": 1., "close_at": index + pd.Timedelta("1D")}, index=index)
    candles["high"] = candles.close + 5
    profile = {"decision": "1D", "context": [{"frame": "7D", "sma": 2}]}
    full, _ = build_features(candles, profile, pd.Timedelta("1D"))
    prefix, _ = build_features(candles.iloc[:66], profile, pd.Timedelta("1D"))
    pd.testing.assert_frame_equal(prefix, full.loc[prefix.index])
    assert prefix["context_distance_0"].iloc[-1] == full["context_distance_0"].iloc[62]


def test_maturity_purge_and_common_btc_holdout():
    cutoff = pd.Timestamp("2025-11-08T00:00Z")
    decisions = pd.date_range("2025-11-01", periods=4, freq="D", tz="UTC")
    labels = pd.DataFrame({"net_r": [1, 2, 3, 4], "mature_at": [cutoff - pd.Timedelta("1h"), cutoff,
                           cutoff + pd.Timedelta("1min"), cutoff - pd.Timedelta("1min")]}, index=decisions)
    assert matured(labels, decisions[0], cutoff).net_r.tolist() == [1, 4]
    boundary = {"holdout": {"start": cutoff.isoformat()}}
    assert window(decisions[0], cutoff, boundary)[1] == cutoff
    with pytest.raises(ValueError, match="pre-holdout"):
        window(decisions[0], cutoff + pd.Timedelta("1min"), boundary)


def test_outcome_identity_normalizes_timestamp_representations():
    close = pd.Timestamp("2025-01-01T00:00Z")
    matured_at = pd.Timestamp("2025-01-02T01:00Z")
    assert outcome_identity({"decision_at": close, "mature_at": matured_at}) == outcome_identity(
        {"decision_at": "2024-12-31T19:00-05:00", "mature_at": "2025-01-02T01:00:00Z"}
    )


@pytest.mark.parametrize("direction", [1, -1])
def test_margin_posting_pnl_and_actual_funding_sign(direction):
    position = Position(direction, 2., 100., 1000., 50., 0., 200.)
    assert position.free == 950.
    position.fund(.01, 100.)
    assert position.funding == -2. * direction
    assert position.free == 950.
    assert position.unrealized(110.) == 20. * direction
    position.reduce(1., 110., Cost(.001, 0.))
    assert position.remaining == 1.
    position.reduce(9., 110., Cost(.001, 0.))
    assert position.remaining == position.reserved == 0.
    assert position.fees == pytest.approx(.22)
    assert position.wallet == pytest.approx(1000 + 18 * direction - .22)


def test_ioc_quantity_is_fixed_and_exit_order_is_conservative():
    settings = config()
    trade, mark, funding, setup, spec, risk, cost = fixture(settings)
    rules = settings["execution"]
    order, reason = size_order(setup, spec, cost, risk, 10000, rules)
    assert reason is None and order.planned_risk <= 25.
    assert entry_fill(order, 110., 1, spec, cost, 1)[0] == 0
    assert entry_fill(order, 100., 1, spec, cost, .5)[0] < order.contracts
    assert order == size_order(setup, spec, cost, risk, 10000, rules)[0]
    assert [event for event, _ in triggers([100., 110., 80., 100.], 1, 95., 105., 90., False)] == [
        "liquidation", "stop", "target"
    ]
    gap = Market(trade.drop(index=trade.index[4]), mark, funding, pd.Timedelta("1min"), "SIMULATOR_DIAGNOSTIC")
    result = run_trade(gap, setup, spec, cost, risk, 10000, rules, trade.index[-1] + pd.Timedelta("2min"))
    assert result["status"] == "INCOMPLETE" and result["remaining_base"] > 0 and "net" not in result
    high = replace(risk, leverage=50)
    assert size_order(setup, spec, cost, high, 10000, rules)[1] == "selected_leverage_outside_policy"
    assert size_order(setup, spec, cost, high, 10000, rules, evaluation_mode="stress")[1] == "stop_to_liquidation_buffer"


def test_synthetic_instrument_and_partial_reduce_only_remainder():
    result = diagnostics(config())
    assert result["scope"] == "SIMULATOR_DIAGNOSTIC"
    assert len(result["stress_grid"]) == 8
    assert result["ordinary_long"]["funding"] < 0 < result["ordinary_short"]["funding"]
    partial = result["stresses"]["partial_rejected"]["partial"]
    assert partial["ioc_cancelled_contracts"] > 0
    assert len(partial["fills"]) == 2
    assert all(fill["reduce_only"] and fill["stop_target_siblings_cancelled"] for fill in partial["fills"])
    assert sum(fill["base"] for fill in partial["fills"]) == pytest.approx(partial["signed_base_quantity"])
    divergent = result["stresses"]["fair_last_divergence"]["fills"][0]
    assert divergent["reason"] == "stop" and divergent["trigger_mark"] != divergent["raw_trade_price"]


def test_gap_liquidation_uses_mark_trigger_and_trade_fill_with_isolated_deficit():
    settings = config()
    trade, mark, funding, setup, spec, risk, cost = fixture(settings)
    at = trade.index[5]
    trade.loc[at, ["open", "high", "low", "close"]] = [45., 46., 44., 45.]
    mark.loc[at, ["open", "high", "low", "close"]] = [55., 56., 54., 55.]
    market = Market(trade, mark, funding, pd.Timedelta("1min"), "SIMULATOR_DIAGNOSTIC")
    result = run_trade(market, setup, spec, cost, risk, 10000., settings["execution"],
                       trade.index[-1] + pd.Timedelta("2min"))
    fill = result["fills"][0]
    assert fill["reason"] == "liquidation"
    assert fill["raw_trade_price"] == 45. < 55. < fill["trigger_mark"]
    assert result["liquidation_cost"] > 0 and result["unfunded_deficit"] > 0
    assert result["net"] == pytest.approx(result["gross_realized"] + result["funding"] - result["fees"]
                                         - result["liquidation_cost"] + result["unfunded_deficit"])


def test_pause_resume_stop_and_identical_job_dedup(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "DATABASE", tmp_path / "control.sqlite3")
    monkeypatch.setattr(control, "STATE", tmp_path)
    monkeypatch.setattr(control, "OUTPUT", tmp_path / "runs")
    settings = config()
    manifest = {"id": "fixture", "design": {"settings": settings, "trials": list(catalog(settings))}}
    monkeypatch.setattr(control, "read_manifest", lambda *_, **__: manifest)
    queued = control.change("run-once", manifest=manifest)
    assert queued["state"] == "QUEUED"
    with pytest.raises(ValueError, match="already active"):
        control.change("run-once", manifest=manifest)
    with state.connect(write=True) as db:
        state.save(db, state.current(db), state="TRAINING")
    assert control.change("pause")["state"] == "PAUSING"
    control.serve(single=True)
    assert state.status()["state"] == "PAUSED"
    assert control.change("resume")["job"] == queued["job"]
    assert control.change("stop")["state"] == "OFF"
    assert state.status()["mode"] == "off"
    with state.connect(write=True) as db:
        state.save(db, state.current(db), job=None)
    assert control.change("run-once", manifest=manifest)["state"] == "OFF"
    assert state.status()["campaigns"] == 1


def test_online_observer_backup_does_not_modify_live_store(tmp_path, monkeypatch):
    active, snapshot = tmp_path / "forward.sqlite3", tmp_path / "snapshot.sqlite3"
    connect = store.connect
    with connect(active) as db:
        with db:
            store.set_meta(db, "experiment", {"existing_model": "immutable"})
        monkeypatch.setattr(store, "connect", lambda readonly=True: connect(active, readonly=readonly))
        assert observer_snapshot(snapshot)["learning_input"] is False
        assert store.get(db, "experiment") == {"existing_model": "immutable"}
    with connect(snapshot, readonly=True) as db:
        assert store.get(db, "experiment") == {"existing_model": "immutable"}


def test_learning_dashboard_uses_the_durable_profile_flag(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "DATABASE", tmp_path / "control.sqlite3")
    profile = "day_perp_research_v1"
    widget = f"learning-profile-{profile}"
    app = AppTest.from_string("from lab.learning.dashboard import panel\npanel()").run()
    assert not app.exception
    assert app.checkbox(key=widget).value
    app.checkbox(key=widget).uncheck().run()
    assert not app.exception and not state.status()["profiles"][profile]
    control.change("profile", profile=profile, enabled=True)
    app.run()
    assert not app.exception and app.checkbox(key=widget).value
