from dataclasses import asdict

import pandas as pd
import pytest

from lab.phase11.campaign import BudgetExhausted, start_clock
from lab.learning.design import config
from lab.learning.simulator import Contract
from lab.phase16 import allocation, control, evaluate, plan
from lab.storage import file_hash, read_json, utc, utc_now, write_json


def test_incremental_gates_require_unique_mature_labels_and_elapsed_interval():
    at = utc("2026-11-09T00:00Z")
    assert allocation.eligibility("same", "same", ["a","b"], [], 2, 1, at, at)["state"] == "NO_NEW_INPUTS"
    assert allocation.eligibility("new", "old", ["a","a","b"], ["a"], 2, 1, at, at+pd.Timedelta(days=2))["state"] == "WAITING_FOR_NEW_MATURE_LABELS"
    early = allocation.eligibility("new", "old", ["a","b","c"], ["a"], 2, 1, at, at)
    assert early["state"] == "WAITING_FOR_INTERVAL" and early["new_mature_labels"] == 2
    assert allocation.eligibility("new", "old", ["a","b","c"], ["a"], 2, 1, at, at+pd.Timedelta(days=1))["state"] == "QUEUED"


def test_label_ending_on_cutoff_is_excluded_from_training():
    at = utc("2026-11-01T00:00Z")
    days = pd.date_range(at, periods=12, freq="1D")
    data = pd.DataFrame({"net_r": [0., 1., 2., 3.], "mature_at": [days[2], days[3], days[6], days[9]]},
                        index=pd.DatetimeIndex([days[0],days[1],days[4],days[7]]))
    fold = {"inner_start": days[3], "outer_start": days[6], "outer_end": days[9]}
    slices = evaluate.fold_samples(data, at, fold)
    assert len(slices["inner_train"]) == 1 and len(slices["outer_train"]) == 2
    assert slices["inner_validation"].empty and slices["outer_mature"].empty


def test_incremental_worker_never_fits_when_input_has_no_mature_increment(tmp_path, monkeypatch):
    start = utc("2027-03-01T00:00Z")
    end = start + pd.Timedelta(days=30)
    labels = pd.DataFrame({"net_r": [1.], "mature_at": [start+pd.Timedelta(days=1)]}, index=pd.DatetimeIndex([start]))
    manifest = {"id": "fixture", "body": {"kind": "INCREMENTAL_ALLOCATION", "policy": {}, "profiles": {"day": {
                "train_start": start, "exclusive_data_end": end, "data_manifest": "new", "minimum_new_mature": 40, "interval_days": 1}},
                "previous": {"data_manifest": "old", "mature_ids": evaluate.mature_ids(labels), "completed_at": start.isoformat()}}}
    monkeypatch.setattr(allocation, "folder", lambda _: tmp_path)
    write_json(tmp_path/"allocation-maturity.json", {"campaign":"fixture", "new_decision_mature_ids":[]})
    monkeypatch.setattr(evaluate, "open_profile", lambda *_: ({}, {}, (None,None,labels,None,None), None))
    monkeypatch.setattr(evaluate, "forecast_folds", lambda *_: pytest.fail("Immature inputs must never reach fitting"))
    result = evaluate.evaluate_profile(tmp_path, manifest, "day")
    assert result["state"] == "WAITING_FOR_NEW_MATURE_LABELS" and result["new_mature_labels"] == 0
    assert evaluate.counts(tmp_path)["fit_attempts"] == 0


def test_allocated_permissions_reject_protected_windows_and_lab_roots(tmp_path, monkeypatch):
    start = utc("2027-02-07T20:00Z")
    descriptor = {"allocation_role": "CHALLENGER_DEVELOPMENT_ONLY", "profile": "swing", "start": start,
                  "inner_start": start+pd.Timedelta(days=205), "outer_start": start+pd.Timedelta(days=222),
                  "end_exclusive": start+pd.Timedelta(days=230), "source": {"venue": "KuCoin", "symbol": "XBTUSDTM",
                  "settlement": "USDT", "scope": "RESEARCH_ONLY", "permission": "public", "evidence": "fixture", "terms": "fixture"}}
    now = start+pd.Timedelta(days=231)
    assert allocation.permitted_interval(descriptor, [start], now)[0] == start
    with pytest.raises(ValueError, match="protected windows"):
        allocation.permitted_interval({**descriptor, "start": start-pd.Timedelta(minutes=1)}, [start], now)
    with pytest.raises(ValueError, match="permissions"):
        allocation.permitted_interval({**descriptor, "source": {**descriptor["source"], "venue": "Binance"}}, [start], now)
    monkeypatch.setattr(allocation, "PROJECT", tmp_path)
    with pytest.raises(ValueError, match="protected lab store"):
        allocation.outside_protected_roots(tmp_path, "runs/phase14/seed.json")


def test_version_receipt_and_attempt_budget_survive_resume(tmp_path):
    at = utc_now()
    clock = start_clock(tmp_path, 7200, at)
    times = pd.date_range("2025-01-01", periods=3, freq="1D", tz="UTC")
    labels = pd.DataFrame({"net_r": [0.,1.,-1.], "mature_at": times+pd.Timedelta(hours=1)}, index=times)
    features = {"features": pd.DataFrame({"a": [1.,2.,3.]}, index=times), "columns": ["a"]}
    fold = {"id":"unit", "inner_start":"2025-01-05T00:00Z", "outer_start":"2025-02-01T00:00Z"}
    policy = {"max_fit_attempts": 2, "max_attempts_per_fit": 2, "alpha": 10.}
    manifest = {"id":"fixture"}
    model, receipt = evaluate.fitted_model(tmp_path, manifest, "day", fold, "inner", labels, features, policy, 308)
    artifact = tmp_path/"models"/(receipt["version"]+".joblib")
    saved = file_hash(artifact)
    restarted, repeated = evaluate.fitted_model(tmp_path, manifest, "day", fold, "inner", labels, features, policy, 308)
    assert receipt == repeated and file_hash(artifact) == saved
    assert restarted.predict(features["features"]).tolist() == model.predict(features["features"]).tolist()
    assert evaluate.counts(tmp_path)["fit_attempts"] == 1
    evaluate.attempt(tmp_path, "fits", ["day","unit","outer"], 2, 2)
    with pytest.raises(BudgetExhausted):
        evaluate.attempt(tmp_path, "fits", ["day","unit","outer"], 2, 2)
    assert start_clock(tmp_path, 7200, at+pd.Timedelta(minutes=30)) == clock


def test_recurring_enable_pause_resume_stop_and_single_dispatch(tmp_path, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(control, "ROOT", tmp_path)
    monkeypatch.setattr(plan, "ROOT", tmp_path)
    monkeypatch.setattr(control, "active", lambda: None)
    monkeypatch.setattr(control, "process_identity", lambda pid, created: pid == 123 and created == 1.)
    monkeypatch.setattr(control.psutil, "Process", lambda pid: SimpleNamespace(pid=pid, create_time=lambda: 1.))
    spawned = []
    monkeypatch.setattr(control.subprocess, "Popen", lambda args, **kwargs: spawned.append(args) or SimpleNamespace(pid=123))
    now = utc("2027-03-01T00:00Z")
    mature_ids = [str(i) for i in range(40)]
    due = allocation.eligibility("new", "old", mature_ids, [], 40, 1, now, now)
    campaign = "a" * 64
    write_json(tmp_path/"queue"/f"{campaign}.json", {"id": campaign, "at": now.isoformat(),
               "not_before": due["next_eligible_check"]})
    monkeypatch.setattr(control, "utc_now", lambda: now)
    protected = tmp_path/"phase15-registration.json"
    write_json(protected, {"id": "preserved"})
    original = protected.read_bytes()
    control.managed_tick()
    assert not spawned and control.change("enable")["recurring"]
    control.managed_tick()
    assert not spawned and read_json(tmp_path/"last-managed-check.json")["state"] == "WAITING_FOR_INTERVAL"
    now += pd.Timedelta(days=1)
    assert allocation.eligibility("new", "old", mature_ids, [], 40, 1, now-pd.Timedelta(days=1), now)["state"] == "QUEUED"
    control.managed_tick()
    control.managed_tick()
    assert len(spawned) == 1 and spawned[0][-2:] == ["_supervise", campaign]
    assert not control.change("pause")["recurring"]
    assert control.change("pause")["resume_recurring"]
    assert control.resume()["recurring"]
    assert not control.change("stop")["recurring"] and protected.read_bytes() == original


def test_public_allocation_checks_native_data_and_logs_zero_labels_without_fitting(tmp_path, monkeypatch):
    start = utc("2027-02-08T00:00Z")
    inner, outer, end = (start+pd.Timedelta(days=days) for days in (160, 170, 180))
    clock = end+pd.Timedelta(days=1)
    research = tmp_path/"research"
    for module in (allocation, plan):
        monkeypatch.setattr(module, "ROOT", research)
    monkeypatch.setattr(allocation, "utc_now", lambda: clock)
    monkeypatch.setattr(allocation, "phase14", lambda _: {"body": {"end_exclusive": start.isoformat()}})
    monkeypatch.setattr(allocation, "phase15", lambda: {"body": {"follow_end_exclusive": start.isoformat()}})
    previous = {"data_manifest":"old", "mature_ids":[], "campaign":"prior", "cutoff":"2025-11-08T00:00Z",
                "completed_at":(start-pd.Timedelta(days=40)).isoformat()}
    monkeypatch.setattr(allocation, "completed_for", lambda _: previous)
    settings = config()
    monkeypatch.setattr(allocation, "parent_design", lambda: {"design": {"settings":settings,
                                                                         "feature_version": "fixture", "label_version": "fixture"}})
    profile_id = "position_spot_research_v1"
    trial = {"profile":profile_id, "entry":"breakout_v1", "exit_policy":"fixed_r_v1", "cost":"phase16-fixture"}
    cfg = {"trial":trial, "profile":settings["profiles"][profile_id], "risk":settings["risk"]["spot_equal_risk_v1"],
           "costs":{"taker_bps":10,"adverse_bps":5}, "minimum_new_mature":10, "interval_days":30}
    monkeypatch.setattr(allocation, "freeze_initial", lambda: {"body":{"profiles":{"position":cfg},
                        "holdout":{"end_exclusive":"2026-11-08T00:00Z"}}})
    hours = pd.date_range(start, end, freq="1h", inclusive="left")
    file = tmp_path/"public-spot.parquet"
    pd.DataFrame({"open_at":hours, "close_at":hours+pd.Timedelta(hours=1), "open":100., "high":110., "low":90.,
                  "close":100., "volume":5.}).to_parquet(file, index=False)
    contract = Contract("spot","USDT",1.,.0001,.1,.0001,1000000.,1.,0.,0.)
    descriptor = tmp_path/"input.json"
    write_json(descriptor, {"allocation_role":"CHALLENGER_DEVELOPMENT_ONLY", "profile":"position", "start":start,
        "inner_start":inner, "outer_start":outer, "end_exclusive":end, "contract":asdict(contract), "contract_evidence":"fixture",
        "source":{"venue":"KuCoin", "symbol":"BTC-USDT", "settlement":"USDT", "scope":"RESEARCH_ONLY", "permission":"fixture",
                  "terms":"fixture", "evidence":"fixture"}, "chunks":[{"trade":file.name, "trade_sha256":file_hash(file)}]})
    row = allocation.allocate(descriptor)
    assert row["state"] == "QUEUED" and allocation.allocate(descriptor)["reused"]
    root = plan.folder(row["id"])
    manifest = read_json(root/"manifest.json")
    result = evaluate.evaluate_profile(root, manifest, "position")
    assert result["state"] == "WAITING_FOR_NEW_MATURE_LABELS" and result["new_mature_labels"] == 0
    assert evaluate.counts(root)["fit_attempts"] == 0
