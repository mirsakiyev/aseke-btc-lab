from copy import deepcopy
from contextlib import closing
from dataclasses import asdict
import json
import subprocess

import joblib
import numpy as np
import pandas as pd
import pytest

from lab.data import OHLCV
from lab.learning.design import config
from lab.learning.features import REGISTRY, build_features
from lab.phase11 import campaign
from lab.phase13 import observation
from lab.phase14 import collection, context, features, outcomes, replay
from lab.storage import file_hash, immutable_bytes, utc


def native(start, periods, step):
    index = pd.date_range(start, periods=periods, freq=step)
    t = np.arange(periods, dtype=float)
    price = 100 + np.sin(t/140) + t/1e5
    return pd.DataFrame({"open": price, "high": price+.1, "low": price-.1, "close": price,
                         "volume": 1+t%7, "close_at": index+step,
                         "received_at": index+step+pd.Timedelta(seconds=50), "evidence": "fixture"}, index=index)


def test_coarse_seed_preserves_frozen_day_feature_values():
    start = utc("2026-10-26T00:00Z")
    raw = native(start, 52*240, context.MINUTE)
    quarter = context.resample(raw, context.QUARTER, context.MINUTE)
    cut = quarter.close_at.iloc[-1]
    daily = native(cut.floor("1D")-201*context.DAILY, 201, context.DAILY)
    seed = {"body": {"scope": context.SCOPE, "admitted_at": cut+pd.Timedelta(minutes=1), "cutoff": cut,
                     "bars": {"quarter": context.encode(quarter), "daily": context.encode(daily)}}}
    rules = config()["profiles"]["day_perp_research_v1"]
    expected, columns = build_features(raw, rules, context.MINUTE)
    frames = features.inputs(seed, context.combine([]), cut+pd.Timedelta(minutes=1))
    actual, received = features.seeded_features({"profile_rules": rules, "feature_columns": columns}, frames,
                                               utc(seed["body"]["admitted_at"]))
    pd.testing.assert_frame_equal(actual, expected, check_freq=False, atol=1e-12)
    assert actual.ready.iloc[-1] and received.iloc[-1] == seed["body"]["admitted_at"]


def test_seed_rejects_as_of_leaks_and_does_not_fill_missing_buckets():
    start = utc("2026-11-08T00:00Z")
    seed_bars = native(start, 2, context.QUARTER)
    cut = seed_bars.close_at.iloc[-1]
    admitted = cut + pd.Timedelta(minutes=1)
    daily = native(start-context.DAILY, 1, context.DAILY)
    seed = {"body": {"scope": context.SCOPE, "admitted_at": admitted, "cutoff": cut,
                     "bars": {"quarter": context.encode(seed_bars), "daily": context.encode(daily)}}}
    with pytest.raises(ValueError, match="not admitted"):
        features.inputs(seed, context.combine([]), admitted-pd.Timedelta(seconds=1))
    with pytest.raises(ValueError, match="cannot replace"):
        features.inputs(seed, native(cut-context.MINUTE, 2, context.MINUTE), admitted)
    next_minute = native(cut, 30, context.MINUTE)
    next_minute = next_minute.drop(next_minute.index[2])
    before = features.inputs(seed, next_minute, cut+pd.Timedelta(minutes=29))
    after = features.inputs(seed, next_minute, cut+pd.Timedelta(minutes=31))
    assert len(before[context.QUARTER]) == 2
    assert after[context.QUARTER].index[-1] == cut + context.QUARTER
    assert cut not in after[context.QUARTER].index


def test_context_scope_source_and_immutable_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(context, "ROOT", tmp_path)
    left = utc("2026-04-22T00:00Z")
    spec = context.page("1day", left, left+context.DAILY, "test")
    assert context.source_spec(spec) == "1day"
    for key, value in (("symbol", "BTC-USDT"), ("klineType", "MARK_PRICE"), ("tradeType", "SPOT")):
        bad = deepcopy(spec)
        bad["params"][key] = value
        with pytest.raises(ValueError, match="restricted"):
            context.source_spec(bad)
    body = {"scope": context.SCOPE, "admitted_at": left, "warmup_account_replays": 0}
    first = context.save("seed", body)
    assert context.pointer("seed") == context.save("seed", body) == first
    with pytest.raises(ValueError, match="not admitted"):
        features.inputs({"body": {**body, "scope": "HISTORICAL_EVALUATION"}}, context.combine([]), left)


def test_context_rows_never_enter_setup_or_outcome_paths(monkeypatch):
    at = utc("2026-11-09T20:00Z")
    clocks = pd.date_range(at-context.QUARTER, periods=3, freq=context.QUARTER)
    settings = config()
    item = {"profile_rules": settings["profiles"]["day_perp_research_v1"], "execution_rules": settings["execution"],
            "trial": {"entry": "breakout_v1", "exit_policy": "fixed_r_v1"},
            "strategy": settings["strategies"]["breakout_v1"], "exit_parameters": settings["exits"]["fixed_r_v1"]}
    frame = pd.DataFrame({"ready": True, "uptrend": True, "downtrend": False, "close": 100., "prior_high": 99.,
                          "prior_low": 98., "atr": 1., "pivot_low": 97., "pivot_high": 103.}, index=clocks)
    received = pd.Series(clocks+pd.Timedelta(seconds=45), index=clocks)
    received.iloc[-1] += pd.Timedelta(minutes=2)
    setups, trails, counts = replay.causal_setups(item, frame, received, at, at+context.FOUR, pd.Timedelta(minutes=2))
    assert list(setups) == [at] and setups[at].submit_at == at+pd.Timedelta(minutes=2)
    assert list(trails) == [setups[at].submit_at] and counts["late_or_unavailable_ready_rows"] == 1
    end = at + context.MINUTE
    prestart = (at-context.MINUTE, at, at, None, dict.fromkeys(OHLCV, 100.))
    monkeypatch.setattr(outcomes, "market_evidence", lambda amendment, start, end, basis: [prestart] if basis != "funding" else [])
    monkeypatch.setattr(outcomes, "checked", lambda *args: None)
    monkeypatch.setattr(outcomes, "price_frame", lambda rows, basis, verify: native(at-context.MINUTE, 1, context.MINUTE))
    with pytest.raises(ValueError, match="post-start paired native minutes"):
        outcomes.execution_market({"body": {"start": at, "end_exclusive": end}}, {"slots": []})


def test_funding_schedule_and_five_outcome_states():
    start = utc("2026-11-10T00:00Z")
    end = start + pd.Timedelta(hours=8)
    slot = start + pd.Timedelta(hours=4)
    duration = pd.Timedelta(hours=2)
    def view(expected, settled=(), queries=(), now=end):
        return outcomes.funding_status(expected, settled, queries, start, end, now, duration)
    assert view([])["state"] == "NO_SETTLEMENT_EXPECTED"
    assert view([slot], now=slot+context.MINUTE)["slots"][0]["state"] == "EXPECTED_NOT_YET_PUBLISHED"
    assert view([slot], queries=[(start,end,end,"RECEIVED")])["slots"][0]["state"] == "EXPECTED_SETTLEMENT_MISSING"
    assert view([slot], queries=[(start,end,end,"HTTP_ERROR")])["slots"][0]["state"] == "FUNDING_QUERY_FAILED"
    zero = view([slot], [(slot, slot+context.MINUTE, 0.)])
    assert zero["complete"] and zero["slots"][0]["state"] == "VERIFIED_ZERO_RATE"
    first = {"contract": {"currentFundingRateGranularity": 28800000, "nextFundingRateDateTime": int(start.timestamp()*1000)}}
    second = {"contract": {"currentFundingRateGranularity": 14400000, "nextFundingRateDateTime": int(slot.timestamp()*1000)}}
    snapshots = [(start-context.DAILY, start-context.DAILY, first), (slot-context.MINUTE, slot-context.MINUTE, second)]
    assert outcomes.dated_schedule(snapshots, start, end) == [start, slot]


def test_managed_dispatch_preserves_original_and_routes_only_tail(monkeypatch):
    start = utc("2026-11-09T20:00Z")
    old = {"id": "parent", "body": {"end_exclusive": start+context.DAILY}}
    new = {"id": "amendment", "body": {"start": start, "end_exclusive": start+context.DAILY+context.FOUR,
           "original_protocol": "parent", "acquisition": {"step_minutes": 15, "close_grace_seconds": 45},
           "rules": {"funding_receipt_grace_seconds": 7200}}}
    monkeypatch.setattr(collection, "pointer", lambda name: new)
    monkeypatch.setattr(collection.protocol, "active", lambda: old)
    monkeypatch.setattr(collection.observation, "observe", lambda **opts: opts)
    monkeypatch.setattr(collection, "original_complete", lambda _: False)
    pre = collection.managed_observe("scheduled:fixture")
    assert "study" not in pre and pre["after_horizon"] > old["body"]["end_exclusive"]
    monkeypatch.setattr(collection, "original_complete", lambda study: study["id"] == "parent")
    tail = collection.managed_observe("scheduled:fixture")["study"]
    assert tail["id"] == "amendment" and tail["pulse_id"] == "parent"
    assert utc(tail["body"]["start"]) == old["body"]["end_exclusive"]


def test_final_funding_closeout_waits_for_publication_window(monkeypatch):
    from lab.phase14 import outcomes

    end = utc("2027-02-07T20:00Z")
    tail = {"body": {"end_exclusive": end, "rules": {"funding_receipt_grace_seconds": 7200}}}
    wake = []
    monkeypatch.setattr(collection.observation, "schedule", lambda study, time: wake.append(time))
    monkeypatch.setattr(outcomes, "audit", lambda *args: pytest.fail("Closeout has to wait for the funding receipt window"))
    result = collection.funding_closeout({}, tail, end+pd.Timedelta(minutes=30))
    assert result["state"] == "WAITING_FOR_FINAL_FUNDING_RECEIPTS"
    assert wake == [end+pd.Timedelta(hours=2)]


def test_both_profiles_reach_persisted_evaluation_with_midnight_funding(tmp_path, monkeypatch):
    from sklearn.dummy import DummyRegressor

    start = utc("2033-06-01T20:00Z")
    midnight = start + context.FOUR
    end = midnight + pd.Timedelta(minutes=30)
    cutoff = start - pd.Timedelta(minutes=30)
    clock = {"now": end + pd.Timedelta(minutes=2)}
    settings = config()
    monkeypatch.setattr(context, "ROOT", tmp_path)
    monkeypatch.setattr(replay, "ROOT", tmp_path)
    monkeypatch.setattr(replay, "STATE", tmp_path / "locks")
    monkeypatch.setattr(observation, "ROOT", tmp_path)
    monkeypatch.setattr(campaign, "utc_now", lambda: clock["now"])
    monkeypatch.setattr(replay, "utc_now", lambda: clock["now"])
    monkeypatch.setattr(outcomes, "utc_now", lambda: clock["now"])
    monkeypatch.setattr(replay, "gates_off", lambda: True)
    monkeypatch.setattr(replay, "admission", lambda budget: None)
    parent_record = context.save("parent", {"design": {"seed": 308}})
    parent_path = tmp_path / "parent" / (parent_record["id"] + ".json")
    monkeypatch.setattr(replay, "CAMPAIGN", tmp_path / "campaign")
    (tmp_path / "campaign").mkdir()
    (tmp_path / "campaign" / "manifest.json").write_bytes(parent_path.read_bytes())
    lanes = {}
    for name, entry, exit_policy, estimator in (("day", "pullback_v1", "fixed_r_v1", "ridge_v1"),
                                                ("swing", "breakout_v1", "atr_trail_v1", "hist_v1")):
        profile_id = f"{name}_perp_research_v1"
        profile = settings["profiles"][profile_id]
        columns = [key for key in REGISTRY if not key.startswith("context_")]
        columns += [f"context_{feature}_{i}" for i in range(len(profile["context"])) for feature in ("distance", "slope")]
        for role, model in (("estimator", estimator), ("unfiltered", "rules_v1"), ("mean", "mean_control_v1"), ("cash", "cash")):
            lane = f"{name}/{role}"
            trial = {"id": lane, "entry": entry, "exit_policy": exit_policy, "model": model, "cost": "fixture"}
            artifact = None
            if role == "estimator":
                path = tmp_path / f"{name}-fixture.joblib"
                version = f"synthetic-{name}"
                # Fixture-only artifact. Each synthetic target is positive and constant.
                x = pd.DataFrame([dict.fromkeys(columns, 0.)])
                predictor = DummyRegressor(strategy="constant", constant=.5).fit(x, [.5])
                joblib.dump({"version": version, "trial": trial, "fit": "outer", "features": columns, "estimator": predictor}, path)
                artifact = {"version": version, "path": str(path), "sha256": file_hash(path), "use": "RESEARCH_ONLY",
                            "fit_manifest": "fixture"}
            lanes[lane] = {"id": lane, "role": role, "trial": trial, "fixed_artifact": artifact, "feature_columns": columns,
                           "fixed_mean_net_r": -.3 if name == "day" else .3, "threshold_net_r": 0.,
                           "parent_fitting_fingerprint": "fixture", "profile_rules": profile, "execution_rules": settings["execution"],
                           "strategy": settings["strategies"][entry], "exit_parameters": settings["exits"][exit_policy],
                           "cost": {"taker": .0006, "adverse_proxy": .0005}, "risk": settings["risk"][profile["risk"]]}
    registry = context.save("registry-fixture", {"candidates": lanes, "parent_campaign": parent_record["id"],
        "parent_manifest_sha256": file_hash(parent_path), "policy": {"budget": {"wall_seconds": 90, "child_rss_mib": 640,
                                                                              "output_cap_mib": 32}}})
    monkeypatch.setattr(replay, "freeze", lambda: registry)
    original = {"id": "synthetic-parent-store", "body": {"end_exclusive": end.isoformat()}}
    monkeypatch.setattr(outcomes.protocol, "active", lambda: original)
    quarter = native(cutoff.floor("1D")-9*context.DAILY, 9*96+78, context.QUARTER)
    qprice = 30000+3*np.arange(len(quarter))+100*np.sin(np.arange(len(quarter))/14)
    quarter[OHLCV[:4]] = np.column_stack([qprice, qprice+8, qprice-8, qprice])
    daily = native(cutoff.floor("1D")-201*context.DAILY, 201, context.DAILY)
    dprice = 27500+8*np.arange(len(daily))
    daily[OHLCV[:4]] = np.column_stack([dprice, dprice+8, dprice-8, dprice])
    seed = context.save("seed", {"registry": registry["id"], "scope": context.SCOPE, "cutoff": cutoff,
                            "admitted_at": cutoff+context.MINUTE, "bars": {"quarter": context.encode(quarter), "daily": context.encode(daily)}})
    contract_record = {"status": "Open", "multiplier": .001, "lotSize": 1, "tickSize": .1, "maxLeverage": 100,
                       "currentFundingRateGranularity": 28800000, "nextFundingRateDateTime": int(midnight.timestamp()*1000)}
    metadata = {"as_of": (start-pd.Timedelta(hours=1)).isoformat(), "contract": contract_record,
                "isolated": [{"level": 1, "maxRiskLimit": 250000, "maxLeverage": 100, "maintainMargin": .004}]}
    from lab.phase9.ideas import current_rules
    instrument = asdict(current_rules("FUTURES", metadata, {"ideas": {"modeled_liquidation_fee": .003}}))
    amendment = context.save("amendment", {"registry": registry["id"], "seed": seed["id"], "original_protocol": original["id"],
        "start": start, "end_exclusive": end, "registered_at": start-pd.Timedelta(minutes=10), "technical_review_due": end,
        "source_scope": "SYNTHETIC_FIXTURE", "candidates": lanes,
        "instrument": {"contract": instrument, "metadata_at_registration": metadata},
        "acquisition": {"step_minutes": 15, "close_grace_seconds": 45, "decision_max_receipt_delay_seconds": 120},
        "rules": {"funding_receipt_grace_seconds": 7200, "sample_floors": {"outer_forecast_mature": 10,
                                                                        "uncertainty_closed_trades": 25}}})
    root = observation.directory(original) / "raw"
    raw = {kind: immutable_bytes(root, kind.encode(), "json") for kind in ("trade", "mark", "funding")}
    with closing(observation.db_open(original)) as db:
        for at in context.intervals(cutoff, end, context.MINUTE):
            close = at+context.MINUTE
            receipt = close.ceil("15min") + pd.Timedelta(seconds=45)
            trade = (32300 if start+pd.Timedelta(minutes=15) <= at < start+pd.Timedelta(minutes=30) else
                     32900 if start+pd.Timedelta(minutes=30) <= at < start+pd.Timedelta(hours=1) else
                     33400 if start+pd.Timedelta(hours=1) <= at else 33000)
            mark = 33400 if start+pd.Timedelta(hours=1) <= at else 33000
            if at == midnight + pd.Timedelta(minutes=10):
                mark = trade = 32950
            for kind, price in (("trade", trade), ("mark", mark)):
                price = float(price)
                values = dict(zip(OHLCV, (price, price+2, price-2, price, 2.)))
                db.execute("INSERT INTO item VALUES (?,?,?,?,?,?,?)", (kind, at.isoformat(), close.isoformat(),
                           receipt.isoformat(), raw[kind], json.dumps(values), "ON_CADENCE"))
        db.execute("INSERT INTO item VALUES (?,?,?,?,?,?,?)", ("funding", midnight.isoformat(), midnight.isoformat(),
                   (midnight+context.MINUTE).isoformat(), raw["funding"], '{"rate":0.0001}', "SETTLED_RECEIPT"))
        db.commit()
    # Scope the clock to this isolated protocol and prove the real qualification gate still holds.
    clock["now"] = midnight
    assert replay.evaluate()["state"] == "PRIMARY_HORIZON_OPEN"
    assert context.pointer("snapshot") is None
    clock["now"] = end+pd.Timedelta(minutes=2)
    assert outcomes.audit()["state"] == "QUALIFIED_FOR_OFFLINE_REPLAY"
    popen = subprocess.Popen
    launch = []
    bootstrap = """
from pathlib import Path
import runpy, sys
from lab.phase13 import observation
from lab.phase14 import context, outcomes, replay, snapshot
from lab.storage import utc
root, when, identifier = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
context.ROOT = snapshot.ROOT = replay.ROOT = observation.ROOT = root
replay.STATE = root / 'locks'
replay.CAMPAIGN = root / 'campaign'
replay.freeze = lambda: context.pointer('registry-fixture')
replay.gates_off = lambda: True
replay.utc_now = outcomes.utc_now = lambda: utc(when)
outcomes.protocol.active = lambda: {'id': 'synthetic-parent-store', 'body': {'end_exclusive': context.pointer('amendment')['body']['end_exclusive']}}
sys.argv = ['phase14', '_evaluate', identifier]
runpy.run_module('lab.phase14', run_name='__main__')
"""

    def isolated_child(command, **kwargs):
        assert command[-3:] == ["lab.phase14", "_evaluate", amendment["id"]]
        launch.append(command)
        return popen([command[0], "-X", "utf8", "-c", bootstrap, str(tmp_path), clock["now"].isoformat(), amendment["id"]], **kwargs)

    monkeypatch.setattr(replay.subprocess, "Popen", isolated_child)
    completed = replay.evaluate()
    assert completed["state"] == "COMPLETE_RESEARCH_REPLAY", (tmp_path / "evaluation" / "worker.log").read_text()
    result = context.pointer("evaluation")
    snapshot = context.pointer("snapshot")
    assert result["body"]["snapshot"] == snapshot["id"]
    assert snapshot["body"]["assets"]["trade"]["rows"] == 270
    assert snapshot["body"]["assets"]["feature_bridge"]["rows"] == 30
    result_hash = file_hash(tmp_path / "evaluation" / (result["id"] + ".json"))
    assert replay.evaluate()["reused"] and len(launch) == 1
    assert file_hash(tmp_path / "evaluation" / (result["id"] + ".json")) == result_hash
    profiles = result["body"]["profiles"]
    assert set(profiles) == {"day", "swing"}
    for name, profile in profiles.items():
        lanes = profile["lanes"]
        assert set(lanes) == {"estimator", "unfiltered", "mean", "cash"}
        assert all(utc(order["decision_at"]) >= start for lane in lanes.values() for order in lane["orders"])
        assert lanes["cash"]["account"]["closed_trades"] == 0 and lanes["cash"]["account"]["return"] == 0
        executed = [order for order in lanes["estimator"]["orders"] if order["status"] == "COMPLETE"]
        assert executed, (name, profile["feature_readiness"], lanes["estimator"]["orders"])
        assert executed[0]["model_version"] == f"synthetic-{name}"
        expected_labels = 1 if name == "day" else 2
        assert lanes["estimator"]["forecast"]["matched_mature_labels"] == expected_labels
        assert lanes["mean"]["forecast"]["matched_mature_labels"] == expected_labels
    day = profiles["day"]["lanes"]
    assert day["mean"]["account"]["closed_trades"] == 0
    day_order = next(order for order in day["estimator"]["orders"] if order["status"] == "COMPLETE")
    assert day["unfiltered"]["account"]["closed_trades"] == 1
    assert day["unfiltered"]["orders"][0]["exit_at"] == day_order["exit_at"]
    assert day_order["target_r"] == 2 and day_order["reason"] == "target"
    assert utc(day_order["exit_at"]) < midnight and utc(day_order["deadline"]) == midnight-pd.Timedelta(minutes=5)
    swing = profiles["swing"]["lanes"]
    for role in ("estimator", "unfiltered", "mean"):
        order = next(order for order in swing[role]["orders"] if order["status"] == "COMPLETE")
        assert utc(order["entry_at"]) < midnight < utc(order["exit_at"])
        assert order["trail_atr"] == 2.5 and order["target_r"] is None
        assert utc(order["deadline"]) > midnight+6*context.DAILY
        assert order["reason"] == "stop" and order["fills"][0]["trigger_mark"] > order["stop"]
        assert [utc(item["settlement_at"]) for item in order["settlements"]] == [midnight]
        assert order["funding"] < 0 and swing[role]["account"]["funding"] == order["funding"]
        assert any(row["reason"] == "account_occupied_at_sizing" for row in swing[role]["orders"])
