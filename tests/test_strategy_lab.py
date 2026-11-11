import sqlite3

import numpy as np
import pandas as pd
import psutil
import pytest
from streamlit.testing.v1 import AppTest

from lab.learning.simulator import make_setup
from lab.phase10.trading import run_trade as futures_trade
from lab.storage import file_hash
from lab.strategy_lab import comparison, jobs, prospective
from lab.strategy_lab.datasets import admit, bounds, load_market, settlements
from lab.strategy_lab.evaluator import causal_bars, code_hash, compile_spec, evaluate, feature_frame, test_version as check_version
from lab.strategy_lab.registry import connect, get, inventory, register, transition
from lab.strategy_lab.seed import recipe_spec
from lab.strategy_lab.specification import INDICATORS, stage, validate


def spec(profile="position", *, baseline=False):
    short = profile != "position"
    settings = {"context": [{"frame": "1h" if profile == "day" else "7D", "sma": 3}],
                "long_only": not short, "max_hold_minutes": 120 if profile == "day" else 2880}
    risk = {"starting_equity": 10000, "risk_fraction": .0025, "notional_cap": .25, "leverage": 1,
            "ordinary_cap": 1, "liquidation_buffer_atr": 0}
    execution = {"version": "closed-input-ioc-onebar-v1", "processing_seconds": 30, "entry_limit_atr": .25,
                 "funding_reserve_bps": 10}
    result = recipe_spec(profile, "breakout_v1", "fixed_r_v1", {"prior_range": 20},
                         {"initial_atr": 2, "target_r": 2}, settings, risk, execution, {"taker": .001, "adverse_proxy": .0005})
    result.update(strategy_id=f"phase15/{profile}/fixed" if baseline else f"test/{profile}",
                  family_id="breakout_v1", name="Controlled fixture", description="Deterministic test recipe",
                  origin="CONTROLLED_FIXTURE", implementation_version="test-v1",
                  source={"kind": "manual", "author": "fixture", "reference": "tests/test_strategy_lab.py", "license": "local"})
    return result


def candles(start, periods, frequency):
    index = pd.date_range(start, periods=periods, freq=frequency, tz="UTC", name="open_at")
    step = pd.Timedelta(frequency)
    sample = np.arange(len(index)) * step / pd.Timedelta("1D")
    prices = 100 + sample * .2 + 4 * np.sin(sample * np.pi / 12) + np.sin(sample * np.pi / 3)
    return pd.DataFrame({"open": prices, "high": prices + .16, "low": prices - .16, "close": prices,
                         "volume": 1 + (sample % 7), "close_at": index + step, "received_at": index + step}, index=index)


def contract(futures=False):
    return {"family": "linear_perp" if futures else "spot", "quote": "USDT", "base_per_contract": .001 if futures else 1.,
            "quantity_step": 1 if futures else .001, "price_tick": .01, "min_contracts": 1 if futures else .001,
            "tier_notional": 1e7, "max_leverage": 5 if futures else 1, "maintenance_rate": .005 if futures else 0.,
            "liquidation_fee_rate": .001 if futures else 0.}


def data_fixture(directory, *, profile="position"):
    directory.mkdir()
    strategy = spec(profile)
    futures = strategy["market"] == "FUTURES"
    frame = candles("2024-01-01", 60 * 24 * 4 if futures else 24 * 270, "1min" if futures else "1h")
    left, right = frame.index[0].isoformat(), frame.close_at.iloc[-1].isoformat()
    body = {**{k: strategy[k] for k in ("profile", "market", "instrument", "venue")},
            "native_seconds": strategy["data"]["native_seconds"], "start": left, "end_exclusive": right,
            "availability": "RECORDED_RECEIPT", "use": "DEVELOPMENT_ONLY",
            "provenance": {"reference": "isolated test", "origin": "CONTROLLED_FIXTURE", "license": "fixture",
                           "exclusion_attestation": "generated from numeric functions"},
            "contract": {"reference": "fixture terms", "effective_from": left, "effective_until": right,
                         "terms": contract(futures)}, "assets": {}}
    for basis in ("trade", "mark") if futures else ("trade",):
        path = directory / f"{basis}.parquet"
        basis_frame = frame.copy()
        if basis == "mark":
            basis_frame[["open", "high", "low", "close"]] *= 1.001
        basis_frame.reset_index().to_parquet(path)
        body["assets"][basis] = {"file": path.name, "sha256": file_hash(path)}
    if futures:
        body["funding_schedule"] = [{"start": left, "end_exclusive": right, "anchor": left,
                                     "interval_minutes": 480, "reference": "fixture"}]
        body["funding"] = [{"settlement_at": at.isoformat(), "received_at": (at + pd.Timedelta("1min")).isoformat(),
                            "rate": .0001} for at in pd.date_range(left, right, freq="8h", inclusive="left")]
    return body


def test_registry_immutable_revision_and_transitions(tmp_path):
    with connect(tmp_path, write=True) as db:
        original = stage(db, spec())
        changed = {**spec(), "description": "New version"}
        with pytest.raises(ValueError, match="current version"):
            stage(db, changed)
        next_version = stage(db, changed, expected=original["hash"])
        assert next_version["version"] == 2
        assert get(db, original["hash"])["spec"] == spec()
        assert stage(db, changed)["hash"] == next_version["hash"]
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db.execute("UPDATE strategy_version SET spec='{}'")
        with pytest.raises(ValueError, match="workflow"):
            transition(db, next_version["hash"], "UNIT_TESTED", "skip review")
        transition(db, next_version["hash"], "SPECIFIED", "reviewed")
        tested = check_version(db, next_version["hash"])
        assert tested["state"] == "UNIT_TESTED"
        assert tested["flags"]["model_validation"] == "No validated model"
        assert tested["flags"]["live_permission"] == "DISABLED"
        assert [row["version"] for row in inventory(db, all_versions=True)] == [2, 1]


def test_staging_flags_ambiguous_future_and_unsupported_rules(tmp_path):
    draft = spec()
    draft["entry_rules"] = ["Enter a short if it looks strong"]
    draft["exit_rules"] = []
    draft["invalidation_rules"] = ["Stop based on the future candle"]
    draft["indicators"]["rsi"] = {"period": None}
    draft["data"]["dependencies"] += ["order_book"]
    issues = validate(draft)
    assert {"entry_rules", "exit_rules", "invalidation_rules", "sides", "indicators", "data"} <= {
        item["field"] for item in issues}
    assert any(row["code"] == "UNSUPPORTED_DATA" for row in issues)
    draft["source"] = {"kind": "pine", "author": "fixture", "reference": "fixture", "license": "reference"}
    with connect(tmp_path, write=True) as db:
        row = stage(db, draft, source_upload=("ref.pine", b"lookahead=barmerge.lookahead_on"))
        assert row["state"] == "DRAFT" and row["spec"]["source_review_flags"]
        assert db.execute("SELECT COUNT(*) FROM source_blob").fetchone()[0] == 1
        with pytest.raises(ValueError, match="requires review"):
            transition(db, row["hash"], "SPECIFIED", "review")


def test_features_use_completed_context_and_reject_revisions():
    strategy = spec()
    strategy["indicators"]["bollinger"] = {"definition": INDICATORS["bollinger"]}
    frame = candles("2024-01-01", 130 * 24, "1h")
    at = pd.Timestamp("2024-04-02T00:00Z")
    as_of = at + pd.Timedelta("30s")
    signal = evaluate(frame, strategy, at, as_of=as_of, data_hash="fixture")
    changed = frame.copy()
    changed.loc[changed.index >= at, ["open", "high", "low", "close"]] *= 50
    assert evaluate(changed, strategy, at, as_of=as_of, data_hash="fixture") == signal
    assert signal["context_closes"]["7D"] == "2024-04-01T00:00:00+00:00"
    expected = frame.loc[at - pd.Timedelta(days=20): at - pd.Timedelta(hours=1)].resample("1D").close.last()
    assert signal["values"]["bollinger_mid"] == pytest.approx(expected.mean())
    assert signal["values"]["bollinger_upper"] == pytest.approx(expected.mean() + 2 * expected.std(ddof=0))
    assert evaluate(frame, strategy, at, as_of=at, data_hash="fixture")["decision"] == "NO_TRADE"
    frame.loc[at - pd.Timedelta("1h"), "received_at"] = at + pd.Timedelta("31s")
    assert len(causal_bars(frame, strategy)) == len(frame) - 1
    assert at not in feature_frame(frame, strategy, pd.Timedelta("1h")).index


def test_vwap_requires_continuous_intraday_volume():
    strategy = spec("day")
    strategy["indicators"]["candle_vwap"] = {"definition": INDICATORS["candle_vwap"]}
    frame = candles("2024-01-01", 24 * 60 * 3, "1min")
    at = pd.Timestamp("2024-01-02T00:15Z")
    features = feature_frame(frame, strategy, pd.Timedelta("1min"))
    prefix = frame.loc["2024-01-02T00:00Z":"2024-01-02T00:14Z"]
    assert features.loc[at, "candle_vwap"] == pytest.approx(np.average(prefix.close, weights=prefix.volume))
    assert pd.isna(feature_frame(frame.drop(prefix.index[0]), strategy, pd.Timedelta("1min")).loc[
        at + pd.Timedelta("15min"), "candle_vwap"])


def test_data_partition_funding_and_spot_compatibility(tmp_path):
    body = data_fixture(tmp_path / "source", profile="day")
    with pytest.raises(ValueError, match="protected"):
        bounds({**body, "end_exclusive": "2025-11-08T00:01:00Z"})
    with pytest.raises(ValueError, match="funding"):
        settlements({**body, "funding": body["funding"][:-1]})
    assert len(settlements(body)) == 12
    with connect(tmp_path / "lab", write=True) as db:
        admitted = admit(db, tmp_path / "lab", body, tmp_path / "source", "test operator")
        market, terms, coverage = load_market(tmp_path / "lab", admitted["body"])
        assert terms.family == "linear_perp" and coverage["settlements"] == 12
        assert not np.array_equal(market.trade_prices[:, :4], market.mark_prices)
    wrong = {**spec(), "sides": ["short"]}
    with pytest.raises(ValueError, match="Spot is long-only"):
        compile_spec(wrong)


def test_futures_path_uses_mark_for_stop_and_trade_for_cash(tmp_path):
    draft = data_fixture(tmp_path / "source", profile="day")
    with connect(tmp_path / "lab", write=True) as db:
        admitted = admit(db, tmp_path / "lab", draft, tmp_path / "source", "test operator")
    market, terms, _ = load_market(tmp_path / "lab", admitted["body"])
    at = pd.Timestamp("2024-01-01T07:45Z")
    strategy = spec("day")
    profile, rules, recipe, risk, cost = compile_spec(strategy)
    profile["max_hold_minutes"] = 60
    row = pd.Series({"close": 100., "atr": 3., "pivot_low": 90.})
    setup = make_setup(at, row, 1, "breakout_v1", "fixed_r_v1", recipe["exit_parameters"], profile, rules, market.step)
    market.trade_prices = market.trade_prices.copy()
    market.mark_prices = market.mark_prices.copy()
    entry = market.index.get_loc(setup.submit_at)
    mark_hit = market.index.get_loc(pd.Timestamp("2024-01-01T08:01Z"))
    market.trade_prices[entry:mark_hit + 1, :4] = [100, 102, 98, 100]
    market.mark_prices[entry:mark_hit + 1] = [100, 102, 98, 100]
    market.mark_prices[mark_hit, 2] = 93
    result = futures_trade(market, setup, terms, cost, risk, 10000, rules, at + pd.Timedelta("2h"))
    assert result["status"] == "COMPLETE" and result["reason"] == "stop"
    assert result["settlements"][0]["settlement_at"] == pd.Timestamp("2024-01-01T08:00Z")
    assert result["funding"] < 0 and result["fills"][0]["raw_trade_price"] == 98
    assert result["net"] == pytest.approx(result["gross_realized"] - result["fees"] + result["funding"])
    market.mark_prices[mark_hit, :] = np.nan
    assert futures_trade(market, setup, terms, cost, risk, 10000, rules, at + pd.Timedelta("2h"))["status"] == "INCOMPLETE"


def test_completed_isolated_replay_and_recovery(tmp_path, monkeypatch):
    root, inputs = tmp_path / "research", tmp_path / "intake"
    body = data_fixture(inputs)
    monkeypatch.setattr(jobs, "SHARED_STATE", tmp_path / "shared-locks")
    with connect(root, write=True) as db:
        register(db, spec(baseline=True), initial="IMPLEMENTED")
        candidate = register(db, spec(), initial="IMPLEMENTED")
        version = check_version(db, candidate["hash"])["hash"]
        dataset = admit(db, root, body, inputs, "fixture operator")["hash"]
        development = ["2024-03-15T00:00:00Z", "2024-05-24T00:00:00Z"]
        validation = ["2024-05-24T00:00:00Z", "2024-09-01T00:00:00Z"]
        with pytest.raises(ValueError, match="chronological"):
            jobs.request_job(db, version, dataset, validation, development, "fixture operator")
        job = jobs.request_job(db, version, dataset, development, validation, "fixture operator")
        key = job["hash"]
        assert jobs.request_job(db, version, dataset, development, validation, "different operator")["hash"] == key
        jobs.update(db, key, "RUNNING", "simulated interrupted claim")
        jobs.recover(db)
        assert jobs.resume(db, key)["state"] == "QUEUED"
    proc = psutil.Process()
    jobs.work(root, key, f"{proc.pid}/{proc.create_time()}")
    with connect(root) as db:
        report = comparison.report(db, key)
        base = report["cases"]["validation/base"]["metrics"]
        assert base["accounting_complete"] and base["closed_trades"] > 0
        assert report["cases"]["validation/missing_data"]["metrics"]["return"] is None
        assert report["cases"]["verdict"]["status"] == "INCONCLUSIVE"
        assert report["job"]["attempts"] == 1 and len(report["cases"]) == 9
        assert report["selection"]["registered_requests"] == 1
        assert comparison.compare(db, [key])["ranked_job_ids"] == []
        assert report["job"]["request"]["identity"]["code"] == code_hash()
        assert prospective.preview(db, version, key)["activation"] == "DISABLED"
        assert get(db, version)["state"] == "UNIT_TESTED"
        assert report["cases"]["validation/base"]["metrics"] == report["cases"]["validation/baseline"]["metrics"]
    with connect(root, write=True) as db:
        assert jobs.cancel(db, key)["state"] == "COMPLETE"
        with pytest.raises(ValueError, match="sufficient evidence"):
            prospective.propose(db, version, key, {})
        another = jobs.request_job(db, version, dataset, development, [validation[0], "2024-08-01T00:00:00Z"], "operator")
        assert jobs.cancel(db, another["hash"])["state"] == "CANCELLED"
    print(f"Isolated Spot replay: {base['closed_trades']} closed, {len(report['cases']) - 1} cases, {report['cases']['verdict']['status']}")


def test_prospective_registration_stays_disabled_after_review(tmp_path):
    root, inputs = tmp_path / "research", tmp_path / "intake"
    body = data_fixture(inputs)
    with connect(root, write=True) as db:
        register(db, spec(baseline=True), initial="IMPLEMENTED")
        version = check_version(db, register(db, spec(), initial="IMPLEMENTED")["hash"])["hash"]
        dataset = admit(db, root, body, inputs, "test operator")["hash"]
        dev, val = ["2024-03-15T00:00Z", "2024-05-24T00:00Z"], ["2024-05-24T00:00Z", "2024-09-01T00:00Z"]
        key = jobs.request_job(db, version, dataset, dev, val, "test operator")["hash"]
        jobs.update(db, key, "RUNNING", "injected fixture evidence")
        jobs.checkpoint(db, key, "verdict", {"status": "EXPLORATORY_SCREEN"})
        jobs.update(db, key, "COMPLETE", "injected fixture completion")
        transition(db, version, "HISTORICAL_RESEARCH", "Operator reviewed test evidence")
        transition(db, version, "SHORTLISTED", "Operator shortlisted fixture")
        settings = {"start": "2099-01-01T00:00Z", "horizon_days": 30, "minimum_closed_trades": 25,
                    "execution_model": "standalone fixture book",
                    "data_availability": dict.fromkeys(("source", "receipt_rule", "gap_policy", "economic_path"), "fixture")}
        result = prospective.propose(db, version, key, settings)
        assert prospective.propose(db, version, key, settings) == result
        reviewed = prospective.review(db, result["hash"], "operator", "REVIEWED", "fixture review")
        assert reviewed["manifest"]["approval"] == "AWAITING_EXPLICIT_APPROVAL"
        assert reviewed["manifest"]["activation"] == "DISABLED"
        assert get(db, version)["state"] == "PROSPECTIVE_PENDING"
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db.execute("DELETE FROM proposal")


def test_dashboard_is_readonly_on_render(tmp_path, monkeypatch):
    from lab.strategy_lab import registry
    root = tmp_path / "unset"
    monkeypatch.setattr(registry, "ROOT", root)
    app = AppTest.from_string("from lab.strategy_lab.dashboard import panel\npanel()").run()
    assert not app.exception
    assert not root.exists()
    app.radio[0].set_value("Strategy Library").run()
    assert not app.exception and not root.exists()
