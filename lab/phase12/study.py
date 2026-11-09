from collections import Counter
from bisect import bisect_left
import gc
import gzip
from importlib.metadata import version
import json
from pathlib import Path
import platform
from uuid import uuid4

import joblib
import numpy as np
import pandas as pd
import pyarrow as pa

from lab.config import fingerprint
from lab.learning.design import code_hashes
from lab.learning.evaluation import candidates, matured, outcome_identity
from lab.learning.simulator import Contract, Cost, Risk
from lab.phase10.partition import partition_features
from lab.phase10.trading import run_trade
from lab.storage import file_hash, read_json, utc, utc_now, write_json


CONTROLS = {"cash", "passive_25pct"}
PATH_ERRORS = {"missing_ioc_bar", "missing_entry_mark", "trade_feed_gap_while_exposed", "mark_feed_gap_while_exposed",
               "unsupported_margin_tier_transition", "no_resolving_bar"}


def runtime_versions():
    return {"python": platform.python_version(),
            **{name: version(name) for name in ("pandas", "numpy", "scikit-learn", "joblib", "pyarrow")}}


class TrailUpdates:
    """The frozen trailing updates in compact arrays instead of Python objects per bar."""

    def __init__(self, frame, allowance, step):
        valid = frame.atr.gt(0).to_numpy()
        clocks = frame.index.as_unit("ns").asi8[valid]
        self.times = ((clocks + allowance.value + step.value - 1) // step.value) * step.value
        self.values = frame[["close", "atr"]].to_numpy()[valid]

    def __len__(self):
        return len(self.times)

    def __contains__(self, at):
        pos = bisect_left(self.times, at.value)
        return pos < len(self.times) and self.times[pos] == at.value

    def __getitem__(self, at):
        pos = bisect_left(self.times, at.value)
        if pos == len(self.times) or self.times[pos] != at.value:
            raise KeyError(at)
        close, atr = self.values[pos]
        return float(close), float(atr)


class EquityCurve:
    """Keep the frozen last-write equity and sticky exposure semantics on a native grid."""

    def __init__(self, design, step):
        self.first = utc(design["folds"][0]["outer_start"])
        end = utc(design["folds"][-1]["outer_end"])
        self.step = step
        cells = (end - self.first) // step + 1
        self.values = np.full(cells, np.nan)
        self.exposed = np.zeros(cells, dtype=bool)
        self.latest = self.first

    def post(self, at, value, holding):
        offset, remainder = divmod(at.value - self.first.value, self.step.value)
        if remainder or not 0 <= offset < len(self.values):
            raise ValueError("Account mark is outside the frozen native replay grid")
        self.values[offset] = value
        if holding:
            self.exposed[offset] = True
        if at > self.latest:
            self.latest = at

    @property
    def exposure_count(self):
        return int(np.count_nonzero(self.exposed))

    def equity(self, market, last):
        end = (last - self.first) // self.step + 1
        values = self.values[:end]
        observed = np.isfinite(values)
        first_open = self.first.value - self.step.value
        past_last_open = last.value
        lo, hi = market.times.searchsorted([first_open, past_last_open])
        positions = (market.times[lo:hi] - first_open) // self.step.value
        observed[positions] = True
        offsets = np.flatnonzero(observed)
        index = pd.to_datetime(self.first.value + offsets * self.step.value, utc=True)
        return pd.Series(values[offsets], index=index, dtype=float).ffill()


def release_buffers():
    gc.collect()
    pa.default_memory_pool().release_unused()


def profile_data(market, settings, profile_id, instrument):
    policy = settings["profiles"][profile_id]
    features, columns = partition_features(market, policy)
    rules = settings["execution"]
    trail = TrailUpdates(features, pd.Timedelta(seconds=rules["processing_seconds"]), market.step)
    release_buffers()
    return {"market": market, "profile": policy, "features": features, "columns": columns, "trail": trail,
            "rules": rules, "contract": Contract.load(instrument), "risk": Risk(**settings["risk"][policy["risk"]])}


def write_orders(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{uuid4().hex}.tmp")
    try:
        with temporary.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=5, mtime=0) as output:
            for row in rows:
                output.write((json.dumps(row, default=str, allow_nan=False, separators=(",", ":")) + "\n").encode())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def lane_path(folder, name, trial):
    return folder / "lanes" / name / fingerprint([trial["entry"], trial["exit_policy"]])


def copy_parquet(path, frame):
    tmp = path.with_name(f".{uuid4().hex}.tmp")
    try:
        frame.to_parquet(tmp, index=True, compression="zstd")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def replay_counts(records, start, end):
    states, issues = Counter(), Counter()
    for row in records:
        if start <= utc(row["decision_at"]) < end:
            states[row["status"]] += 1
            if row["reason"] in PATH_ERRORS:
                issues[row["reason"]] += 1
    return dict(states), dict(issues)


def spool_lane(design, data, trial, destination):
    settings = design["settings"]
    entry, exit_name = trial["entry"], trial["exit_policy"]
    cost = Cost.load(settings["costs"][trial["cost"]])
    setups, skipped = candidates(data["market"], data["profile"], entry, settings["strategies"][entry], exit_name,
                                 settings["exits"][exit_name], data["features"], data["rules"])
    left, right = utc(settings["evaluation"]["train_start"]), utc(design["available_at_cutoff_exclusive"])
    setups = {at: setup for at, setup in setups.items() if left <= at < right}
    label_equity = data["rules"]["label_equity"]
    mature, ids, projection = [], [], []

    def events():
        for setup in setups.values():
            result = run_trade(data["market"], setup, data["contract"], cost, data["risk"], label_equity,
                               data["rules"], right, trail_updates=data["trail"])
            if result["status"] == "COMPLETE":
                mature.append((result["decision_at"], result["mature_at"], result["net_r"]))
                ids.append(outcome_identity(result))
            yield result
        yield from (row for row in skipped if left <= row["decision_at"] < right)

    def projected_events():
        for row in events():
            projection.append({key: row[key] for key in ("decision_at", "status", "reason")})
            yield row

    write_orders(destination, projected_events())
    labels = pd.DataFrame(mature, columns=["decision_at", "mature_at", "net_r"])
    for field in ("decision_at", "mature_at"):
        labels[field] = pd.to_datetime(labels[field], utc=True)
    labels = labels.set_index("decision_at")
    meta = {"observed_label_states": dict(Counter(row["status"] for row in projection)),
            "non_complete_reasons": dict(Counter(row["reason"] for row in projection if row["status"] != "COMPLETE")),
            "complete_mature_outcomes": len(mature), "label_outcome_ids": ids, "reference_equity": label_equity,
            "selected_leverage": data["risk"].leverage, "features": data["columns"], "fixed_label_capital": True}
    return setups, cost, labels, projection, meta


def lane_evidence(design, data, batch, records, market_start):
    setups, _, labels, _, _ = batch
    start = max(market_start, utc(design["settings"]["evaluation"]["train_start"]))
    output = {}
    setup_times = pd.DatetimeIndex(setups.keys(), tz="UTC")
    for fold in design["folds"]:
        inner, outer, end = (utc(fold[k]) for k in ("inner_start", "outer_start", "outer_end"))
        states, paths = replay_counts(records, start, end)
        inner_train = matured(labels, start, inner)
        outer_train = matured(labels, start, outer)
        validation, eval_labels = matured(labels, inner, outer), matured(labels, outer, end)
        boundaries = {"inner_training": [start.isoformat(), inner.isoformat()],
                      "outer_training": [start.isoformat(), outer.isoformat()],
                      "inner_validation": [inner.isoformat(), outer.isoformat()],
                      "outer_evaluation": [outer.isoformat(), end.isoformat()]}
        output[fold["id"]] = {"boundaries": boundaries,
            "sample_counts": {"inner_train": len(inner_train), "outer_train": len(outer_train),
                              "inner_validation": len(validation), "outer_mature": len(eval_labels)},
            "planned_outer_setups": int(((setup_times >= outer) & (setup_times < end)).sum()),
            "maturity_purges": {"inner_train": int(((labels.index < inner) & (labels.mature_at >= inner)).sum()),
                                "outer_train": int(((labels.index < outer) & (labels.mature_at >= outer)).sum()),
                                "inner_validation": int(((labels.index >= inner) & (labels.index < outer)
                                                         & (labels.mature_at >= outer)).sum()),
                                "outer_evaluation": int(((labels.index >= outer) & (labels.index < end)
                                                         & (labels.mature_at >= end)).sum())},
            "observed_states_through_fold": states, "required_execution_path_issues": paths}
    return output


def build_lane(folder, name, design, data, trial, cap, profile):
    from lab.phase11.campaign import BudgetExhausted, reserve

    path = lane_path(folder, name, trial)
    proof = path / "prepared.json"
    if proof.exists():
        return read_json(proof)
    attempt = 1 + len(list(path.glob("attempt-*.json")))
    if attempt > cap["max_profile_trial_attempts"]:
        raise BudgetExhausted("Frozen label lane attempts exhausted")
    reserve(folder, "kernel_visits", [name, trial["entry"], trial["exit_policy"], "labels", attempt],
            (profile["maximum_hold_minutes"] + 3) * profile["decision_bar_ceiling"],
            cap["trade_kernel_visits_all_attempts_ceiling"])
    write_json(path / f"attempt-{attempt}.json", {"at": utc_now().isoformat(), "entry": trial["entry"],
                                                 "exit": trial["exit_policy"]})
    label_log = path / "labels.jsonl.gz"
    batch = spool_lane(design, data, trial, label_log)
    records = batch[3]
    labels = batch[2]
    observations = lane_evidence(design, data, batch, records, data["market"].index[0])
    data_path = path / "mature-labels.parquet"
    copy_parquet(data_path, labels)
    info = {"label_sha256": file_hash(data_path), "evidence_sha256": file_hash(label_log),
            "meta": batch[4], "folds": observations,
            "first_mature_label_decision": labels.index[0].isoformat() if len(labels) else None,
            "first_label_resolution": labels.mature_at.min().isoformat() if len(labels) else None,
            "last_mature_label_decision": labels.index[-1].isoformat() if len(labels) else None}
    write_json(proof, info)
    return info


def read_lane(folder, name, design, data, trial, expected):
    path = lane_path(folder, name, trial)
    proof = read_json(path / "prepared.json")
    data_path = path / "mature-labels.parquet"
    if fingerprint(proof) != expected or file_hash(data_path) != proof["label_sha256"]:
        raise ValueError("Preregistered lane labels changed")
    # The original compressed event log remains independently verifiable in the manifest.
    labels = pd.read_parquet(data_path)
    settings = design["settings"]
    entry, exit_name = trial["entry"], trial["exit_policy"]
    setups, _ = candidates(data["market"], data["profile"], entry, settings["strategies"][entry], exit_name,
                           settings["exits"][exit_name], data["features"], data["rules"])
    left, right = utc(settings["evaluation"]["train_start"]), utc(design["available_at_cutoff_exclusive"])
    setups = {at: setup for at, setup in setups.items() if left <= at < right}
    return setups, Cost.load(settings["costs"][trial["cost"]]), labels, [], proof["meta"]


def verdict(trial, fold, history, ready_at, observed, settings):
    reasons, floors = [], {}
    model = trial["model"]
    if not observed["eligible"] or observed["missing_trade_or_mark_minutes"]:
        reasons.append("Required price or funding coverage incomplete")
    if model == "passive_25pct":
        reasons.append("Frozen passive spot control is inapplicable to this perpetual")
    if model not in CONTROLS:
        if ready_at is None or utc(ready_at) >= utc(fold["inner_start"]):
            reasons.append("No feature-ready decision before the frozen inner split")
        if history["required_execution_path_issues"]:
            reasons.append("One or more required native execution paths are unresolved")
        if model != "rules_v1":
            evaluation = settings["evaluation"]
            required = settings["models"].get(model, {"minimum_train": evaluation["minimum_inner"]})["minimum_train"]
            floors = {"inner_train": required, "outer_train": required, "inner_validation": evaluation["minimum_inner"],
                      "outer_mature": evaluation["minimum_outer"]}
            for key, limit in floors.items():
                if history["sample_counts"][key] < limit:
                    reasons.append(f"{key}: {history['sample_counts'][key]} mature, requires {limit}")
    return {**history, "market_coverage": observed, "first_feature_ready": ready_at, "sample_floors": floors,
            "status": "INELIGIBLE" if reasons else "ELIGIBLE", "reasons": reasons}


def feature_bounds(data, market_start):
    frame = data["features"]
    ready = frame.index[frame.ready]
    ready_at = ready[0].isoformat() if len(ready) else None
    return {"input_start": market_start.isoformat(), "first_complete_decision": frame.index[0].isoformat(),
            "first_feature_ready": ready_at, "feature_ready_count": len(ready), "derived_decisions": len(frame),
            "last_feature_ready": ready[-1].isoformat() if len(ready) else None,
            "feature_columns": list(data["columns"])}


def prepare(folder, manifest, market, view, resource, guard):
    path = folder / "fit-manifest.json"
    if path.exists():
        execution = read_json(path)
        table = read_json(folder / "eligibility.json")
        if (fingerprint(execution["body"]) != execution["id"] or execution["body"]["campaign"] != manifest["id"]
                or execution["body"]["eligibility_sha256"] != file_hash(folder / "eligibility.json")
                or execution["body"]["runtime_versions"] != runtime_versions()):
            raise ValueError("Frozen eligibility, runtime or Phase 12 fit provenance changed")
        return execution, table
    body = manifest["body"]
    design = body["design"]
    caps = resource["body"]["replay"]
    table = {"view": view["id"], "profiles": {}, "trials": {}}
    market_start = market.index[0]
    for name in body["profiles"]:
        guard()
        budget = caps["profiles"][name]
        print(f"Building {name} causal features", flush=True)
        data = profile_data(market, design["settings"], budget["profile"], body["contract"])
        print(f"Prepared {name} feature state", flush=True)
        evidence = feature_bounds(data, market_start)
        ready = evidence["first_feature_ready"]
        table["profiles"][name] = evidence
        lanes = {}
        for trial in body["trial_order"][name]:
            guard()
            model = trial["model"]
            entry = None
            if model not in CONTROLS:
                lane = trial["entry"], trial["exit_policy"]
                if lane not in lanes:
                    proof = build_lane(folder, name, design, data, trial, caps, budget)
                    print(f"Prepared {name} {lane[0]} {lane[1]}: {proof['meta']['complete_mature_outcomes']} mature labels", flush=True)
                    lanes[lane] = {"hash": fingerprint(proof), "folds": proof["folds"],
                                   "first_mature_label_decision": proof["first_mature_label_decision"],
                                   "first_label_resolution": proof["first_label_resolution"],
                                   "last_mature_label_decision": proof["last_mature_label_decision"]}
                entry = lanes[lane]
            rows = {fold["id"]: verdict(trial, fold, entry["folds"][fold["id"]] if entry else {}, ready,
                                        view["body"]["folds"][fold["id"]], design["settings"]) for fold in design["folds"]}
            table["trials"][trial["id"]] = {"model": model, "profile": name, "folds": rows,
                                            "lane": {k: v for k, v in entry.items() if k != "folds"} if entry else None}
        del data, lanes
        release_buffers()
        # The cache is bounded by the existing market and keeps no historical features between profiles.
    write_json(folder / "eligibility.json", table)
    execution = {"campaign": manifest["id"], "view": view["id"], "resource": resource["id"],
                 "eligibility_sha256": file_hash(folder / "eligibility.json"), "runtime_versions": runtime_versions(),
                 "frozen_parent_code": code_hashes(), "campaign_code": body["code"],
                 "feature_and_label_boundaries": table["profiles"],
                 "model_artifact_scope": "RESEARCH_ONLY", "frozen_trial_order": body["trial_order"]}
    if body.get("code_continuation"):
        execution["code_continuation"] = body["code_continuation"]
    record = {"id": fingerprint(execution), "body": execution, "at": utc_now().isoformat()}
    write_json(path, record)
    return record, table


def model_design(design, execution, trial_info, view):
    chosen = [fold for fold in design["folds"] if trial_info["folds"][fold["id"]]["status"] == "ELIGIBLE"]
    return {**design, "folds": chosen,
            "code_sha256": {"parent": execution["body"]["frozen_parent_code"],
                            "phase12": execution["body"]["campaign_code"]},
            "data": {profile: {"view": view["id"], "policy": view["body"]["coverage_policy"],
                               "fit_manifest": execution["id"], "eligibility": execution["body"]["eligibility_sha256"]}
                     for profile in design["settings"]["profiles"]}}


def verify_artifacts(folds, trial, execution, runtime=None):
    for fold in folds:
        for name, artifact in fold.get("model_artifacts", {}).items():
            path = Path(artifact["path"])
            if file_hash(path) != artifact["sha256"]:
                raise ValueError("Model artifact checksum disagrees with its research receipt")
            model = joblib.load(path)
            if (model["version"] != artifact["version"] or model["trial"] != trial or model["fit"] != name
                    or model["fold"]["id"] != fold["fold"]):
                raise ValueError("Wrong trial or fold in model artifact")
            artifact.update({"fit_manifest": execution["id"], "use": "RESEARCH_ONLY",
                             "estimator": type(model["estimator"]).__name__})
            if runtime:
                artifact["runtime_code"] = runtime
