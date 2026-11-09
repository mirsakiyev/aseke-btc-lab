from dataclasses import asdict, replace

import joblib
import numpy as np
import pandas as pd

from lab.execution import execution_open
from lab.learning.features import directions
from lab.learning.simulator import Contract, Cost, Risk, make_setup, size_order
from lab.phase13.registry import checked
from lab.storage import utc


def fitted(item, fold=None):
    if item["role"] != "estimator":
        raise ValueError("A fitted estimator candidate is required")
    receipt = item["fixed_artifact"] if fold is None else item["historical_walk_forward"][fold]["outer_artifact"]
    if not receipt or receipt["use"] != "RESEARCH_ONLY" or receipt["fit_manifest"] != item["parent_fitting_fingerprint"]:
        raise ValueError("Wrong research artifact receipt")
    model = joblib.load(checked(receipt["path"], receipt["sha256"]))
    if (model["version"] != receipt["version"] or model["trial"] != item["trial"] or model["fit"] != "outer"
            or model["features"] != item["feature_columns"] or (fold and model["fold"]["id"] != fold)):
        raise ValueError("Artifact trial, version or feature schema mismatch")
    return receipt, model["estimator"]


def inferred_score(item, frame, fold=None, model=None):
    if item["role"] == "unfiltered":
        return 1.0, None
    if item["role"] == "mean":
        mean = item["fixed_mean_net_r"] if fold is None else item["historical_walk_forward"][fold]["outer_mean_net_r"]
        if mean is None or not np.isfinite(mean):
            raise ValueError("Frozen mean control missing")
        return float(mean), None
    receipt, estimator = model if model is not None else fitted(item, fold)
    features = item["feature_columns"]
    if not set(features) <= set(frame.columns):
        raise ValueError("Expected frozen feature schema is unavailable")
    values = frame.loc[:, features]
    if not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError("Nonfinite forecast feature")
    score = float(estimator.predict(values)[0])
    if not np.isfinite(score):
        raise ValueError("Nonfinite model prediction")
    return score, receipt["version"]


def decision(item, frame, cutoff, decision_at, evidence, *, fold=None, contract=None, equity=None, model=None):
    cutoff, decision_at = utc(cutoff), utc(decision_at)
    scope = evidence.get("scope")
    report = {"candidate": item["id"], "artifact": None, "input_cutoff": cutoff.isoformat(),
              "decision_at": decision_at.isoformat(), "feature_ready": False, "raw_prediction": None,
              "prediction_unit": "net return per unit of planned risk (R)",
              "threshold_net_r": item["threshold_net_r"], "gates": {}, "scope": "RESEARCH_ONLY",
              "evidence_state": scope, "state": "NO_TRADE", "actionable_approval": False}
    if item["role"] == "estimator":
        artifact = item["fixed_artifact"] if fold is None else item["historical_walk_forward"][fold]["outer_artifact"]
        report["artifact"] = artifact["version"]
    if item["role"] == "cash":
        return {**report, "reason": "Frozen cash control"}
    historical = scope == "CONSUMED_PHASE12_MODELED_AVAILABILITY"
    report["gates"]["availability"] = bool(
        scope in ("CONSUMED_PHASE12_MODELED_AVAILABILITY", "REGISTERED_RECEIPT_OFFLINE_REPLAY", "SYNTHETIC")
        and evidence.get("coverage_complete") and cutoff <= decision_at
        and evidence.get("latest_required_source_close")
        and utc(evidence["latest_required_source_close"]) == cutoff
        and evidence.get("latest_required_receipt") and utc(evidence["latest_required_receipt"]) <= decision_at
        and (historical or utc(evidence["latest_required_receipt"]) <= cutoff + pd.Timedelta(seconds=120))
    )
    if not report["gates"]["availability"]:
        return {**report, "reason": "Required closed observations were missing or arrived too late"}
    if frame is None or cutoff not in frame.index or not bool(frame.loc[cutoff, "ready"]):
        return {**report, "reason": "Frozen causal feature state is not ready"}
    report["feature_ready"] = True
    one = frame.loc[[cutoff]]
    profile = item["profile_rules"]
    rule = item["trial"]["entry"]
    side = int(directions(one, rule, item["strategy"], profile["long_only"]).iloc[0])
    report["gates"]["setup"] = side != 0
    if not side:
        return {**report, "reason": "Frozen strategy found no setup"}
    settings = item["execution_rules"]
    step = pd.Timedelta(minutes=1)
    plan = make_setup(cutoff, one.iloc[0], side, rule, item["trial"]["exit_policy"],
                      item["exit_parameters"], profile, settings, step)
    if plan is None:
        return {**report, "reason": "Frozen UTC session excludes entry"}
    available_at = max(cutoff, utc(evidence["latest_required_receipt"])) + pd.Timedelta(seconds=settings["processing_seconds"])
    if decision_at < available_at:
        return {**report, "reason": "Decision precedes the frozen processing allowance"}
    if not historical:
        submit = execution_open(available_at, step)
        deadline = min(plan.deadline, submit + pd.Timedelta(minutes=profile["max_hold_minutes"]))
        if "entry_cutoff_minute_utc" in profile and submit >= cutoff.normalize() + pd.Timedelta(minutes=profile["entry_cutoff_minute_utc"]):
            return {**report, "reason": "Receipt-adjusted submission exceeds the Day entry cutoff"}
        if submit >= deadline:
            return {**report, "reason": "Receipt-adjusted submission leaves no eligible holding interval"}
        plan = replace(plan, available_at=available_at, submit_at=submit, expires_at=submit+step, deadline=deadline)
    score, version = inferred_score(item, one, fold, model)
    report.update(raw_prediction=score if item["role"] != "unfiltered" else None, artifact=version)
    report["gates"]["threshold"] = score > item["threshold_net_r"]
    if not report["gates"]["threshold"]:
        return {**report, "reason": "Saved forecast at or below the frozen threshold"}
    risk = Risk(**item["risk"])
    costs = Cost(item["cost"]["taker"], item["cost"]["adverse_proxy"])
    instrument = Contract.load(contract or item["contract"])
    order, reason = size_order(plan, instrument, costs, risk, risk.starting_equity if equity is None else equity, settings)
    report["gates"]["sizing"] = order is not None
    if not order:
        return {**report, "reason": reason}
    return {**report, "state": "CONDITIONAL_RESEARCH_SETUP", "reason": "Frozen research gates passed",
            "side": "LONG" if side == 1 else "SHORT", "setup": asdict(plan),
            "conditional_limit": order.limit_price, "size": asdict(order),
            "stop_trigger_basis": "MARK", "target_after_fill_r": plan.target_r, "trailing_atr": plan.trail_atr}
