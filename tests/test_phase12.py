import numpy as np
import pandas as pd
import pytest

from lab.config import fingerprint
from lab.execution import execution_open
from lab.learning.design import config
from lab.learning.evaluation import forecasts
from lab.learning.simulator import Contract, Risk
from lab.phase10.evaluation import account_replay, label_batch
from lab.phase10.partition import Chunk, PartitionedMarket
from lab.phase11 import campaign
from lab.phase12 import study, view
from lab.storage import file_hash, utc, write_json


def fixture_market(tmp_path, *, gap=False):
    left = utc("2021-01-01")
    index = pd.date_range(left, periods=6 * 1440, freq="1min")
    clocks = index[120:-60:30]
    price = np.full(len(index), 100.)
    price[np.searchsorted(index, clocks + pd.Timedelta("2min"))] = 105.
    frame = pd.DataFrame({"open_at": index, "open": price, "high": price + .1,
                          "low": price - .1, "close": price, "volume": 5.})
    chunks = []
    for i, (start, stop) in enumerate(((0, 1700), (1700, 5200), (5200, len(index)))):
        trade, mark = tmp_path / f"t{i}.parquet", tmp_path / f"m{i}.parquet"
        rows = frame.iloc[start:stop]
        rows.to_parquet(trade, index=False)
        fair = rows.drop(columns="volume")
        if i == 0 and gap:
            fair = fair[fair.open_at.ne(clocks[4] + pd.Timedelta("2min"))]
        fair.to_parquet(mark, index=False)
        chunks.append(Chunk(trade, mark, file_hash(trade), file_hash(mark)))
    right = left + pd.Timedelta("6D")
    fund = {index[122] + pd.Timedelta(milliseconds=12): .0001}
    market = PartitionedMarket(chunks, fund, right, len(index))
    settings = config()
    profile_id = "day_perp_research_v1"
    profile = {**settings["profiles"][profile_id], "max_hold_minutes": 15}
    features = pd.DataFrame({"ready": True, "uptrend": True, "downtrend": False, "close": 100., "prior_high": 99.,
                             "prior_low": 98., "atr": 1., "pivot_low": 97., "pivot_high": 103.,
                             "x": np.arange(len(clocks), dtype=float) % 12}, index=clocks)
    contract = settings["instruments"]["linear_usdt_perp_v1"]
    data = {"market": market, "profile": profile, "features": features, "columns": ["x"], "trail": {},
            "rules": settings["execution"], "contract": Contract.load(contract),
            "risk": Risk(**settings["risk"][profile["risk"]])}
    fold = {"id": "synthetic", "inner_start": (left + pd.Timedelta("2D")).isoformat(),
            "outer_start": (left + pd.Timedelta("4D")).isoformat(), "outer_end": right.isoformat()}
    design = {"settings": settings, "seed": 308, "folds": [fold], "feature_version": "fixture", "label_version": "fixture",
              "available_at_cutoff_exclusive": right.isoformat(), "holdout": {"start": right.isoformat()}}
    trial = {"id": "fixture/ridge_v1", "profile": profile_id, "entry": "breakout_v1", "exit_policy": "fixed_r_v1",
             "model": "ridge_v1", "cost": profile["primary_cost"]}
    return left, right, design, data, fold, trial


def test_synthetic_admission_labels_fit_artifact_and_account_replay(tmp_path):
    left, _, design, data, fold, trial = fixture_market(tmp_path)
    batch = label_batch(design, data, trial)
    evidence = study.lane_evidence(design, data, batch, batch[3], left)["synthetic"]
    coverage = {"eligible": True, "missing_trade_or_mark_minutes": 0}
    ready = study.feature_bounds(data, left)
    verdict = study.verdict(trial, fold, evidence, ready["first_feature_ready"], coverage, design["settings"])
    assert verdict["status"] == "ELIGIBLE" and verdict["sample_counts"]["inner_train"] >= 35
    assert verdict["sample_counts"]["outer_mature"] >= 10
    assert all(at >= pd.Timestamp(ready["first_feature_ready"]) for at in batch[2].index)
    assert all(t < pd.Timestamp(fold["outer_end"]) for t in batch[2].mature_at)
    execution = {"id": "synthetic", "body": {"frozen_parent_code": {"fixture": "digest"},
                                              "campaign_code": "test", "eligibility_sha256": "test"}}
    qualified = {"id": "synthetic-view", "body": {"coverage_policy": {"version": "synthetic"}}}
    fit_design = study.model_design(design, execution, {"folds": {"synthetic": verdict}}, qualified)
    stages = []
    forecasts_out, rows = forecasts(fit_design, data, trial, batch, tmp_path / "models",
                                   lambda stage, details=None: stages.append((stage, details)))
    study.verify_artifacts(rows, trial, execution)
    assert rows[0]["status"] == "EVALUATED" and len(list((tmp_path / "models").glob("*.joblib"))) == 2
    assert sum(stage == "TRAINING" for stage, _ in stages) == 2
    metrics, orders = account_replay(design, data, trial, batch, forecasts_out,
                                     {"synthetic": rows[0]["model_artifacts"]["outer"]["version"]})
    assert metrics["full_window_observed"] and metrics["closed_trades"] > 0 and metrics["return"] > 0
    assert any(row["status"] == "COMPLETE" for row in orders)
    compact = study.EquityCurve(design, data["market"].step)
    compact_metrics, compact_orders = account_replay(design, data, trial, batch, forecasts_out,
                                    {"synthetic": rows[0]["model_artifacts"]["outer"]["version"]}, curve=compact)
    assert compact_metrics == metrics and compact_orders == orders


def test_view_cutoff_gap_and_holdout_and_maturity(tmp_path):
    start, end, design, data, fold, trial = fixture_market(tmp_path, gap=True)
    idx = data["market"].index
    assert view.cutoff([{"last": "2020-01-19T13:37:00Z"}, {"last": "2020-12-17T07:55:00Z"}]) == utc("2020-12-18")
    with pytest.raises(ValueError, match="Out-of-partition"):
        view.gaps(idx.append(pd.DatetimeIndex([end])), start, end)
    batch = label_batch(design, data, trial)
    proof = study.lane_evidence(design, data, batch, batch[3], start)["synthetic"]
    assert proof["required_execution_path_issues"] == {"mark_feed_gap_while_exposed": 1}
    state = study.verdict(trial, fold, proof, start.isoformat(),
                          {"eligible": True, "missing_trade_or_mark_minutes": 0}, design["settings"])
    assert state["status"] == "INELIGIBLE"
    thin = {**proof, "required_execution_path_issues": {}, "sample_counts": {**proof["sample_counts"], "outer_mature": 9}}
    state = study.verdict(trial, fold, thin, start.isoformat(),
                          {"eligible": True, "missing_trade_or_mark_minutes": 0}, design["settings"])
    assert "outer_mature: 9 mature, requires 10" in state["reasons"]


def test_policy_identity_and_completed_amended_run_dedup(tmp_path, monkeypatch):
    monkeypatch.setattr(campaign, "ROOT", tmp_path / "phase11")
    policy = {"version": view.POLICY, "cutoff": "2020-12-18T00:00:00Z"}
    original = {"coverage_policy": policy}
    ident = fingerprint(original)
    different = {**original, "coverage_policy": {**policy, "cutoff": "2020-12-19T00:00:00Z"}}
    assert ident != fingerprint(different)
    folder = tmp_path / "phase12" / "campaigns" / ident
    write_json(folder / "manifest.json", {"id": ident, "body": original})
    write_json(folder / "result.json", {"campaign": ident, "actual_estimator_fits": 2, "state": "COMPLETE"})
    assert campaign.resume(ident)["reused"]
    assert not (folder / "deadline.json").exists()


def test_eligibility_freezes_before_fit_and_reuses_hashed_labels(tmp_path, monkeypatch):
    _, _, design, data, _, trial = fixture_market(tmp_path)
    monkeypatch.setattr(study, "profile_data", lambda *args: data)
    folder = tmp_path / "campaign"
    manifest = {"id": "synthetic-preflight", "body": {"profiles": ["day"], "design": design, "contract": {},
                                                        "trial_order": {"day": [trial]}, "code": "fixture"}}
    resource = {"id": "budget", "body": {"replay": {"max_profile_trial_attempts": 2,
                       "trade_kernel_visits_all_attempts_ceiling": 100000,
                       "profiles": {"day": {"profile": trial["profile"], "maximum_hold_minutes": 15,
                                            "decision_bar_ceiling": 500}}}}}
    history = {"id": "view", "body": {"folds": {"synthetic": {"eligible": True, "missing_trade_or_mark_minutes": 0}}}}
    frozen, table = study.prepare(folder, manifest, data["market"], history, resource, lambda: None)
    row = table["trials"][trial["id"]]
    assert row["folds"]["synthetic"]["status"] == "ELIGIBLE"
    assert study.prepare(folder, manifest, data["market"], history, resource, lambda: pytest.fail("reran eligibility")) == (frozen, table)
    cached = study.read_lane(folder, "day", design, data, trial, row["lane"]["hash"])
    assert len(cached[2]) > 0 and campaign.ledger(folder)["actual_estimator_fits"] == 0
    expected = label_batch(design, data, trial)
    pd.testing.assert_frame_equal(cached[2], expected[2])
    assert cached[4] == expected[4]
    receipt = study.lane_path(folder, "day", trial) / "mature-labels.parquet"
    receipt.write_bytes(b"changed")
    with pytest.raises(ValueError, match="labels changed"):
        study.read_lane(folder, "day", design, data, trial, row["lane"]["hash"])


def test_compact_trail_matches_frozen_execution_clock(tmp_path):
    _, _, _, data, _, _ = fixture_market(tmp_path)
    frame = data["features"].iloc[:9].copy()
    frame.iloc[2, frame.columns.get_loc("atr")] = 0.
    frame.iloc[4, frame.columns.get_loc("atr")] = np.nan
    step = data["market"].step
    wait = pd.Timedelta(seconds=30)
    expected = {execution_open(at + wait, step): (row.close, row.atr)
                for at, row in frame[frame.atr.gt(0)].iterrows()}
    compact = study.TrailUpdates(frame, wait, step)
    assert len(compact) == len(expected)
    assert {at: compact[at] for at in expected if at in compact} == expected
    assert frame.index[0] not in compact


def test_memory_continuation_keeps_frozen_campaign_and_deadline(tmp_path, monkeypatch):
    monkeypatch.setattr(campaign, "code_fingerprint", lambda coverage: "revised")
    body = {"code": "original", "coverage_policy": {"cutoff": "unchanged"}}
    manifest = {"id": fingerprint(body), "body": body}
    write_json(tmp_path / "manifest.json", manifest)
    clock = campaign.start_clock(tmp_path, 7200, utc("2026-11-09T17:45Z"))
    with pytest.raises(ValueError, match="identity changed"):
        campaign.runtime_manifest(tmp_path, manifest)
    evidence = {"campaign": manifest["id"], "previous_code": "original", "current_code": "revised",
                "original_manifest_sha256": file_hash(tmp_path / "manifest.json")}
    write_json(tmp_path / "code-continuations" / "revised.json", {"body": evidence, "id": fingerprint(evidence)})
    resumed = campaign.runtime_manifest(tmp_path, manifest)
    assert resumed["id"] == manifest["id"] and resumed["body"]["coverage_policy"] == body["coverage_policy"]
    assert campaign.start_clock(tmp_path, 7200, utc("2026-11-09T18:00Z")) == clock


def test_compact_curve_keeps_exposure_and_missing_native_intervals():
    from types import SimpleNamespace

    start = utc("2024-01-01T00:00Z")
    step = pd.Timedelta("1min")
    clocks = pd.date_range(start, periods=6, freq=step)
    market = SimpleNamespace(times=clocks[[0, 1, 4]].as_unit("ns").asi8)
    design = {"folds": [{"outer_start": clocks[1].isoformat(), "outer_end": clocks[5].isoformat()}]}
    curve = study.EquityCurve(design, step)
    curve.post(clocks[1], 100., False)
    curve.post(clocks[3], 92., True)
    curve.post(clocks[3], 91., False)
    result = curve.equity(market, clocks[5])
    pd.testing.assert_series_equal(result, pd.Series([100., 100., 91., 91.], index=clocks[[1, 2, 3, 5]]),
                                   check_freq=False)
    assert curve.exposure_count == 1 and curve.latest == clocks[3]
