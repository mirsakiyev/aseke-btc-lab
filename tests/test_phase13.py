from contextlib import closing
from hashlib import sha256
from pathlib import Path

import httpx
import joblib
import numpy as np
import pandas as pd
import pytest

from lab.learning.design import config
from lab.learning.simulator import Contract
from lab.phase13 import diagnostics, future, observation, protocol, replay
from lab.phase13.inference import decision, fitted
from lab.phase13.registry import sealed
from lab.storage import file_hash, utc, write_json


class FixedPredictor:
    def predict(self, frame):
        assert list(frame.columns) == ["feature"]
        return np.full(len(frame), 1.25)


def candidate(tmp_path):
    settings = config()
    profile = settings["profiles"]["day_perp_research_v1"]
    trial = {"id": "fixture", "entry": "breakout_v1", "exit_policy": "fixed_r_v1", "model": "ridge_v1", "cost": "fixture"}
    path = tmp_path / "pinned.joblib"
    joblib.dump({"version": "synthetic-pin", "trial": trial, "fit": "outer", "features": ["feature"],
                 "estimator": FixedPredictor()}, path)
    return {"id": "fixture", "role": "estimator", "trial": trial, "profile_rules": profile,
            "strategy": settings["strategies"]["breakout_v1"], "exit_parameters": settings["exits"]["fixed_r_v1"],
            "execution_rules": settings["execution"], "feature_columns": ["feature"], "threshold_net_r": 0,
            "fixed_artifact": {"path": str(path), "sha256": file_hash(path), "version": "synthetic-pin",
                               "use": "RESEARCH_ONLY", "fit_manifest": "fixture-manifest"},
            "parent_fitting_fingerprint": "fixture-manifest", "cost": {"taker": .0006, "adverse_proxy": .0005},
            "risk": settings["risk"][profile["risk"]], "contract": settings["instruments"][profile["instrument"]]}


def feature_row(at):
    return pd.DataFrame({"ready": True, "uptrend": True, "downtrend": False, "close": 100., "prior_high": 99.,
                         "prior_low": 98., "atr": 1., "pivot_low": 97., "pivot_high": 103., "feature": 1.}, index=[at])


def test_inference_enforces_receipt_readiness_and_pinned_schema(tmp_path):
    item = candidate(tmp_path)
    at = utc("2026-11-10T12:00Z")
    frame = feature_row(at)
    receipt = {"scope": "SYNTHETIC", "coverage_complete": True, "latest_required_source_close": at,
               "latest_required_receipt": at + pd.Timedelta(seconds=45)}
    result = decision(item, frame, at, at + pd.Timedelta(seconds=75), receipt)
    assert result["raw_prediction"] == 1.25 and result["artifact"] == "synthetic-pin"
    assert result["gates"]["threshold"] and result["scope"] == "RESEARCH_ONLY" and not result["actionable_approval"]
    assert result["setup"]["submit_at"] == at + pd.Timedelta(minutes=2)
    early = decision(item, frame, at, at + pd.Timedelta(seconds=60), receipt)
    assert early["raw_prediction"] is None and "processing" in early["reason"]
    for modified in ({**receipt, "coverage_complete": False},
                     {**receipt, "latest_required_receipt": at + pd.Timedelta(seconds=121)}):
        refused = decision(item, frame, at, at + pd.Timedelta(minutes=3), modified)
        assert refused["state"] == "NO_TRADE" and not refused["gates"]["availability"]
    unavailable = decision(item, frame.assign(ready=False), at, at + pd.Timedelta(seconds=75), receipt)
    assert "not ready" in unavailable["reason"]
    with pytest.raises(ValueError, match="feature schema mismatch"):
        fitted({**item, "feature_columns": ["wrong"]})
    with pytest.raises(ValueError, match="input hash changed"):
        fitted({**item, "fixed_artifact": {**item["fixed_artifact"], "sha256": "0" * 64}})


def test_registration_is_immutable_and_after_clock(tmp_path, monkeypatch):
    at = utc("2026-11-09T18:55:18Z")
    item = {**candidate(tmp_path), "fixed_mean_net_r": None,
            "fixed_training_cutoff_exclusive": "2025-01-01T00:00:00Z"}
    monkeypatch.setattr(protocol, "ROOT", tmp_path)
    monkeypatch.setattr(protocol, "POINTER", tmp_path / "registration.json")
    monkeypatch.setattr(protocol, "freeze", lambda: {"id": "registry", "body": {
        "candidates": {"day/estimator": item, "swing/estimator": item}}})
    monkeypatch.setattr(protocol, "current_metadata", lambda: {"as_of": at.isoformat()})
    monkeypatch.setattr(protocol, "reference", lambda: {})
    monkeypatch.setattr(protocol, "current_rules", lambda *args: Contract.load(item["contract"]))
    registered = protocol.register(at)
    body = registered["body"]
    assert utc(body["start"]) == at.ceil("min")
    assert utc(body["end_exclusive"]) - utc(body["start"]) == pd.Timedelta(days=90)
    assert utc(body["technical_review_due"]) - utc(body["start"]) == pd.Timedelta(days=7)
    assert body["candidates"]["day/estimator"]["fixed_artifact"]["sha256"] == item["fixed_artifact"]["sha256"]
    assert protocol.register(at + pd.Timedelta(days=1)) == registered


def test_observation_reserves_deduplicates_and_retains_source_evidence(tmp_path, monkeypatch):
    start = utc("2026-11-09T18:59Z")
    now = start + pd.Timedelta(minutes=1, seconds=46)
    registered = {"id": "fixture", "body": {"start": start.isoformat(),
                  "end_exclusive": (start + pd.Timedelta(days=90)).isoformat(),
                  "acquisition": {**protocol.OBSERVATION, "min_spacing_seconds": 0},
                  "instrument": {"contract": {"base_per_contract": .001}}}}
    monkeypatch.setattr(observation, "ROOT", tmp_path)
    monkeypatch.setattr(observation, "PULSE", tmp_path / "pulse.json")
    monkeypatch.setattr(observation, "active", lambda: registered)
    monkeypatch.setattr(observation, "control", lambda: {"enabled": True})
    monkeypatch.setattr(observation, "current_metadata", lambda: {"as_of": start.isoformat()})
    calls = []

    def respond(request):
        calls.append(request)
        basis = request.url.params.get("klineType")
        row = [int(start.timestamp()), "100", "101", "99", "100"]
        rows = [row + ["1", "100"]] if basis == "TRADE" else [row] if basis == "MARK_PRICE" else []
        return httpx.Response(200, json={"code": "200000", "data": {
            "symbol": "XBTUSDTM", "tradeType": "FUTURES", "list": rows}})

    transport = httpx.MockTransport(respond)
    assert observation.scheduled_due(now)
    cycle = observation.observe(now=now, transport=transport)
    assert cycle["state"] == "COMPLETE" and cycle["metadata"] == "CURRENT"
    assert cycle["received"] == {"trade": 1, "mark": 1} and cycle["funding_query_success"]
    assert cycle["metadata_snapshots"] == 1
    for receipt in cycle["receipts"]:
        path = observation.directory(registered) / "raw" / (receipt["raw_sha256"] + ".json")
        assert sha256(path.read_bytes()).hexdigest() == receipt["raw_sha256"]
        assert utc(receipt["received_at"]) >= start + pd.Timedelta(minutes=1)
    assert observation.observe(now=now, transport=transport)["state"] == "WAITING_FOR_NEXT_WINDOW"
    assert len(calls) == 3 and not observation.scheduled_due(now)
    with closing(observation.db_open(registered, readonly=True)) as db:
        assert db.execute("SELECT count(*) FROM item WHERE basis='funding'").fetchone()[0] == 0


def test_funding_slots_use_frozen_cadence_and_half_open_bounds():
    start, end = utc("2026-11-09T12:00Z"), utc("2026-11-10T00:00Z")
    metadata = {"contract": {"currentFundingRateGranularity": 28_800_000,
                              "nextFundingRateDateTime": int(end.timestamp() * 1000)}}
    assert list(future.expected_funding(metadata, start, end)) == [utc("2026-11-09T16:00Z")]
    assert future.expected_funding({}, start, end) is None


def test_future_gates_before_snapshot_or_score(monkeypatch):
    monkeypatch.setattr(future, "technical", lambda: {"state": "WAITING_FOR_PRIMARY_HORIZON"})
    monkeypatch.setattr(future, "active", lambda: pytest.fail("Readiness must be checked before opening the snapshot"))
    assert future.prepare_evaluation()["state"] == "WAITING_FOR_PRIMARY_HORIZON"
    assert replay.evaluate()["state"] == "WAITING_FOR_PRIMARY_HORIZON"


def test_receipt_adjusted_day_and_exposure_at_horizon(tmp_path, monkeypatch):
    item = candidate(tmp_path)
    first = utc("2026-11-10T11:58Z")
    index = pd.date_range(first, periods=6, freq="1min")
    close = pd.Series(index + replay.STEP, index=index)
    market = type("MarketFixture", (), {"trade": pd.DataFrame({"close_at": close}), "step": replay.STEP,
                                        "times": index.as_unit("ns").asi8})()
    at = index[2]
    frame = feature_row(at)
    monkeypatch.setattr(replay, "build_features", lambda *args: (frame, ["feature"]))
    receipts = (index + replay.STEP + pd.Timedelta(seconds=45)).as_unit("ns").asi8.copy()
    _, setups, counts = replay.receipt_setups(item, market, receipts)
    assert setups[at].submit_at == at + pd.Timedelta(minutes=2)
    assert counts["feature_ready"] == 1
    cutoff = close.iloc[-1]

    def open_position(_, bounded, *args, **kwargs):
        assert cutoff - replay.STEP < bounded.deadline < cutoff
        return {"remaining_base": .01, "deadline": bounded.deadline}

    monkeypatch.setattr(replay, "run_trade", open_position)
    row = replay.horizon_trade(market, setups[at], None, None, None, 10000., {}, cutoff)
    assert row["reason"] == "PRIMARY_HORIZON_EXPOSURE_UNRESOLVED" and row["deadline"] == setups[at].deadline
    receipts[1] = (at + pd.Timedelta(seconds=121)).value
    _, setups, counts = replay.receipt_setups(item, market, receipts)
    assert not setups and counts["unavailable_or_late"] == 1


def test_completed_diagnostic_never_starts_again(tmp_path, monkeypatch):
    monkeypatch.setattr(diagnostics, "folder", lambda identifier: tmp_path)
    result = {"state": "COMPLETE", "new_stress_replays": 12, "historical_estimator_fits": 0}
    write_json(tmp_path / "result.json", result)
    monkeypatch.setattr(diagnostics, "admission", lambda *args: pytest.fail("Completed work may not be admitted again"))
    assert diagnostics.supervise({"id": "fixture"}) == {**result, "reused": True}


def test_frozen_snapshot_feeds_the_future_replay_once(tmp_path, monkeypatch):
    start = utc("2026-11-10T11:58Z")
    end = start + pd.Timedelta(minutes=8)
    instrument = candidate(tmp_path)
    protocol_body = {"registry": "fixture", "start": start.isoformat(), "end_exclusive": end.isoformat(),
                     "instrument": {"contract": instrument["contract"]}, "acquisition": {"min_free_disk_bytes": 0},
                     "rules": {"sample_floors": {"uncertainty_closed_trades": 25, "outer_forecast_mature": 10}}}
    registered = {"id": "capture-test", "body": protocol_body}
    monkeypatch.setattr(observation, "ROOT", tmp_path)
    monkeypatch.setattr(future, "ROOT", tmp_path)
    monkeypatch.setattr(future, "active", lambda: registered)
    monkeypatch.setattr(future, "technical", lambda: {"state": "READY_FOR_FROZEN_OFFLINE_SNAPSHOT", "expected_funding_slots": 0})
    root = observation.directory(registered)
    (root / "raw").mkdir(parents=True)
    payload = b"synthetic source bytes"
    digest = sha256(payload).hexdigest()
    (root / "raw" / (digest + ".json")).write_bytes(payload)
    index = pd.date_range(start, end, freq="1min", inclusive="left")
    with closing(observation.db_open(registered)) as db:
        for basis in ("trade", "mark"):
            data = {"open": 100., "high": 101., "low": 99., "close": 100., "volume": 1.}
            for minute in index:
                close = minute + replay.STEP
                db.execute("INSERT INTO item VALUES (?,?,?,?,?,?,?)", (basis, minute.isoformat(), close.isoformat(),
                           (close + pd.Timedelta(seconds=45)).isoformat(), digest, observation.encode(data), "ON_CADENCE"))
        db.commit()
    snapshot = future.prepare_evaluation()
    assert snapshot["state"] == "FROZEN_OFFLINE_SNAPSHOT" and future.prepare_evaluation()["reused"]
    package = sealed(Path(snapshot["path"]), snapshot["snapshot"])["body"]
    market, receipts = replay.native_market(package)
    at = index[2]
    monkeypatch.setattr(replay, "build_features", lambda *args: (feature_row(at), ["feature"]))
    lanes = {f"day/{role}": {**instrument, "role": role, "fixed_mean_net_r": -.1}
             for role in ("estimator", "unfiltered", "mean", "cash")}
    result = replay.day_comparison({"body": {"candidates": lanes}}, registered, market, receipts, seed=308)
    assert result["readiness"]["feature_ready"] == 1
    assert result["lanes"]["estimator"]["forecast"]["state"] == "INSUFFICIENT_EVIDENCE"
    assert result["lanes"]["mean"]["account"]["closed_trades"] == 0
    assert result["lanes"]["cash"]["account"]["return"] == 0
