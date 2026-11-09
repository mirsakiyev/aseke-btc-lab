from contextlib import suppress
from dataclasses import replace
from pathlib import Path
import subprocess
import sys
import time

import numpy as np
import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.config import ROOT as PROJECT, fingerprint
from lab.execution import execution_open
from lab.learning.control import admission, cancel_tree
from lab.learning.features import directions
from lab.learning.simulator import Contract, Cost, Risk, make_setup
from lab.phase10.evaluation import account_replay
from lab.phase11.archive import size_under
from lab.phase11.campaign import start_clock
from lab.phase12.study import EquityCurve
from lab.phase13.diagnostics import STATE, gates_off
from lab.phase13.inference import fitted, inferred_score
from lab.phase13.registry import CAMPAIGN, checked, freeze, sealed
from lab.phase13.replay import horizon_trade
from lab.phase14.context import MINUTE, ROOT, combine, pointer, save
from lab.phase14.features import inputs, seeded_features
from lab.phase14.outcomes import audit
from lab.phase14.snapshot import freeze_snapshot, load_snapshot
from lab.storage import file_hash, utc, utc_now, write_json, writer_lock


def causal_setups(item, features, receipts, start, end, allowance):
    profile, rules = item["profile_rules"], item["execution_rules"]
    entry, exit_name = item["trial"]["entry"], item["trial"]["exit_policy"]
    window = features.loc[(features.index >= start) & (features.index < end)]
    eligible_receipt = receipts.reindex(window.index).le(window.index + allowance)
    sides = directions(window, entry, item["strategy"], profile["long_only"])
    setups, trails = {}, {}
    for at, row in window.loc[eligible_receipt].iterrows():
        available = max(at, receipts.loc[at]) + pd.Timedelta(seconds=rules["processing_seconds"])
        submit = execution_open(available, MINUTE)
        if submit >= end:
            continue
        if row.atr > 0:
            trails[submit] = (float(row.close), float(row.atr))
        side = int(sides.loc[at])
        if not side:
            continue
        plan = make_setup(at, row, side, entry, exit_name, item["exit_parameters"], profile, rules, MINUTE)
        if plan is None or submit >= plan.deadline:
            continue
        if "entry_cutoff_minute_utc" in profile and submit >= at.normalize()+pd.Timedelta(minutes=profile["entry_cutoff_minute_utc"]):
            continue
        deadline = min(plan.deadline, submit + pd.Timedelta(minutes=profile["max_hold_minutes"]))
        setups[at] = replace(plan, available_at=available, submit_at=submit, expires_at=submit+MINUTE, deadline=deadline)
    return setups, trails, {"complete_feature_rows": int(window.ready.sum()),
                            "late_or_unavailable_ready_rows": int((window.ready & ~eligible_receipt).sum()),
                            "eligible_setups": len(setups)}


def compare(profile, registry, amendment, features, receipts, market, seed):
    body = amendment["body"]
    lanes = registry["body"]["candidates"]
    item = lanes[f"{profile}/estimator"]
    start, end = utc(body["start"]), utc(body["end_exclusive"])
    allowance = pd.Timedelta(seconds=body["acquisition"]["decision_max_receipt_delay_seconds"])
    setups, trail, coverage = causal_setups(item, features, receipts, start, end, allowance)
    contract = Contract.load(body["instrument"]["contract"])
    cost = Cost(item["cost"]["taker"], item["cost"]["adverse_proxy"])
    risk = Risk(**item["risk"])
    rules = item["execution_rules"]
    data = {"market": market, "contract": contract, "risk": risk, "trail": trail, "rules": rules}
    floors = body["rules"]["sample_floors"]
    evaluation = {"threshold_net_r": item["threshold_net_r"], "minimum_trades_for_uncertainty": floors["uncertainty_closed_trades"],
                  "monthly_block_bootstrap_samples": 500}
    fold = "amended_primary"
    design = {"settings": {"evaluation": evaluation}, "seed": seed,
              "folds": [{"id": fold, "outer_start": start.isoformat(), "outer_end": end.isoformat()}],
              "feature_version": "frozen_phase12_seeded", "label_version": "phase14_registered_receipts"}
    labels = {}
    for at, setup in setups.items():
        row = horizon_trade(market, setup, contract, cost, risk, rules["label_equity"], rules, end, trail_updates=trail)
        if row["status"] == "COMPLETE" and row["mature_at"] < end:
            labels[at] = row["net_r"]
    results = {}
    for role in ("estimator", "unfiltered", "mean", "cash"):
        candidate = lanes[f"{profile}/{role}"]
        model = fitted(candidate) if role == "estimator" else None
        scores = {at: inferred_score(candidate, features.loc[[at]], model=model)[0] for at in setups} if role != "cash" else {}
        versions = {fold: model[0]["version"]} if model else {}
        metrics, orders = account_replay(design, data, candidate["trial"], (setups, cost, None, [], {}),
                                         {fold: pd.Series(scores, dtype=float)}, versions,
                                         curve=EquityCurve(design, MINUTE), executor=horizon_trade)
        forecast = None
        if role in ("estimator", "mean"):
            forecast = {"matched_mature_labels": len(labels), "state": "INSUFFICIENT_EVIDENCE"}
            if len(labels) >= floors["outer_forecast_mature"]:
                errors = np.array([scores[at] - value for at, value in labels.items()])
                forecast.update(state="DESCRIPTIVE_REGISTERED_REPLAY", mae_R=float(abs(errors).mean()),
                                rmse_R=float(np.sqrt((errors**2).mean())))
        results[role] = {"candidate": candidate["id"], "forecast": forecast, "account": metrics, "orders": orders}
    return {"feature_readiness": coverage, "lanes": results}


def code_version():
    paths = [*Path(__file__).parent.glob("*.py"), *(PROJECT / name for name in (
        "lab/phase10/evaluation.py", "lab/phase10/partition.py", "lab/phase10/trading.py", "lab/phase13/inference.py",
        "lab/phase13/replay.py", "lab/learning/features.py", "lab/learning/simulator.py"))]
    return fingerprint({str(path.relative_to(PROJECT)): file_hash(path) for path in paths})


def work(identifier):
    amendment, seed = pointer("amendment"), pointer("seed")
    if not amendment or not seed or amendment["id"] != identifier or seed["id"] != amendment["body"]["seed"]:
        raise ValueError("Wrong registered amendment or immutable feature seed")
    report = audit(amendment)
    if report["state"] != "QUALIFIED_FOR_OFFLINE_REPLAY" or not gates_off():
        raise ValueError("Outcome evidence or training controls are no longer eligible")
    registry = freeze()
    body = amendment["body"]
    lanes = registry["body"]["candidates"]
    if (registry["id"] != body["registry"] or seed["body"]["registry"] != registry["id"]
            or set(body["candidates"]) != {f"{profile}/{role}" for profile in ("day", "swing")
                                          for role in ("estimator", "unfiltered", "mean", "cash")}
            or any(any(record[key] != lanes[lane][key] for key in ("id", "role", "trial", "fixed_artifact",
                   "fixed_mean_net_r", "threshold_net_r", "feature_columns")) for lane, record in body["candidates"].items())):
        raise ValueError("Registered comparison lanes differ from the frozen registry")
    parent = checked(CAMPAIGN / "manifest.json", registry["body"]["parent_manifest_sha256"])
    parent_seed = sealed(parent, registry["body"]["parent_campaign"])["body"]["design"]["seed"]
    with writer_lock(STATE / ".fit.lock"), threadpool_limits(limits=2):
        if result := pointer("evaluation"):
            if result["body"]["amendment"] != identifier:
                raise ValueError("Completed result belongs to another amendment")
            return
        snapshot = freeze_snapshot(amendment, seed, report, utc_now())
        if audit(amendment)["state"] != "QUALIFIED_FOR_OFFLINE_REPLAY":
            raise ValueError("Outcome evidence changed while freezing the evaluation snapshot")
        market, bridge = load_snapshot(snapshot)
        minute_features = combine([bridge, market.trade])
        frames = inputs(seed, minute_features, utc(snapshot["body"]["as_of"]))
        profiles = {}
        for profile in ("day", "swing"):
            item = registry["body"]["candidates"][f"{profile}/estimator"]
            features, receipts = seeded_features(item, frames, utc(seed["body"]["admitted_at"]))
            profiles[profile] = compare(profile, registry, amendment, features, receipts, market, parent_seed)
        save("evaluation", {"amendment": identifier, "seed": seed["id"], "snapshot": snapshot["id"], "code": code_version(),
                            "coverage": snapshot["body"]["qualification"],
                            "profiles": profiles, "scope": "REGISTERED_RECEIPT_OFFLINE_REPLAY", "approval": "No validated model",
                            "historical_estimator_fits": 0, "warmup_outcomes": 0, "future_evaluations": 1})


def evaluate():
    if result := pointer("evaluation"):
        return {"state": "COMPLETE_RESEARCH_REPLAY", "id": result["id"], "reused": True}
    report = audit()
    if report["state"] != "QUALIFIED_FOR_OFFLINE_REPLAY":
        return report
    registry = freeze()
    budget = registry["body"]["policy"]["budget"]
    amendment = pointer("amendment")
    with writer_lock(STATE / ".supervisor.lock"), writer_lock(ROOT / ".evaluation.lock"):
        if result := pointer("evaluation"):
            return {"state": "COMPLETE_RESEARCH_REPLAY", "id": result["id"], "reused": True}
        reason = admission(budget)
        if reason or not gates_off():
            return {"state": "WAITING_FOR_RESEARCH_ADMISSION", "reason": reason or "Continuous training is enabled"}
        output = ROOT / "evaluation"
        output.mkdir(parents=True, exist_ok=True)
        clock = start_clock(output, budget["wall_seconds"])
        deadline = utc(clock["deadline"])
        if utc_now() >= deadline:
            return {"state": "INCOMPLETE", "reason": "Original research deadline elapsed", **clock}
        with (output / "worker.log").open("ab") as log:
            child = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase14", "_evaluate", amendment["id"]],
                                     cwd=PROJECT, stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW)
            process = psutil.Process(child.pid)
            peak = 0
            reason = None
            try:
                while child.poll() is None:
                    with suppress(psutil.NoSuchProcess):
                        peak = max(peak, sum(p.memory_info().rss for p in [process, *process.children(recursive=True)]))
                    reason = ("RSS ceiling" if peak >= budget["child_rss_mib"] * 1024**2 else
                              "Output ceiling" if size_under(output) + size_under(ROOT / "snapshot-assets") >= budget["output_cap_mib"] * 1024**2 else
                              "Wall deadline" if utc_now() >= deadline else "Continuous training enabled" if not gates_off() else
                              admission(budget))
                    if reason:
                        break
                    time.sleep(2)
            finally:
                if child.poll() is None:
                    cancel_tree(process)
        result = pointer("evaluation")
        evidence = {"amendment": amendment["id"], "peak_rss_bytes": peak, "deadline": deadline.isoformat(), "stop_reason": reason}
        write_json(ROOT / "evaluation-resources.json", evidence)
        return {"state": "COMPLETE_RESEARCH_REPLAY" if result else "INCOMPLETE", "id": result["id"] if result else None,
                "resources": evidence}
