import pandas as pd

from lab.phase13.observation import PULSE
from lab.phase16.control import settings
from lab.phase17.pilot_registry import PILOT, current, find
from lab.phase17.registry import ROOT
from lab.storage import read_json, utc, utc_now


def read(path):
    return read_json(path) if path.exists() else None


def record(path, kind):
    data = read(path)
    return find(kind,data["id"]) if data else None


def managed_capture_view(capture):
    if not capture or not (spot := capture.get("spot_coverage")):
        return capture
    return {**capture, "spot_coverage": {**spot, "bases": {"trade": spot["bases"]["trade"]},
            "mark_gaps": None, "paired": None,
            "applicability": {"trade": "REQUIRED", "mark": "NOT_APPLICABLE", "funding": "NOT_APPLICABLE"}}}


def status(now=None):
    registration = current()
    if not registration:
        return {"state":"PILOT_UNREGISTERED"}
    now = utc_now() if now is None else utc(now)
    body = registration["body"]
    recurring = settings()["recurring"]
    pulse = (read(PULSE) or {}).get("wake")
    guard = read(PILOT / "last-guard.json")
    profiles = {}
    for name,cfg in body["profiles"].items():
        root = PILOT / "profiles" / name
        pin = record(PILOT / "pins" / f"{name}.json", "context")
        proof = pin or record(PILOT / "proofs" / f"{name}.json", "context")
        saved = read(root / "readiness.json") or {}
        candidate = record(root / "candidate.json", "candidates")
        evaluation = record(root / "evaluation.json", "evaluations")
        start = utc(cfg["label_start"])
        if now < start:
            reason = ("Protected Phase 14 futures window and original native source boundary" if cfg["market"] == "FUTURES" else
                      "Next UTC day is the registered prospective Spot boundary")
            state = "WAITING_FOR_LABEL_START"
        elif not pin:
            state = "WAITING_FOR_CAUSAL_BRIDGE"
            blocker = read(PILOT / "context-wait" / f"{name}.json")
            reason = blocker["reason"] if blocker else "Waiting for the completed operational context at a managed check"
        else:
            state = saved.get("state", "WAITING_FOR_CLOSED_DAILY_CHECK")
            reason = saved.get("reason", "Wait for a closed day and the two-hour receipt grace. Actual maturity depends on execution and resolved paths.")
            if not saved and now >= start+pd.Timedelta(hours=26):
                state = "WAITING_FOR_SUPERVISED_LABEL_CHECK"
                reason = guard["reason"] if guard and utc(guard["at"]) >= start else "A closed-day cutoff awaits the supervised managed label check"
        fit_status = ("BASELINE_ONLY" if not cfg["queue"] else "CANDIDATE_FROZEN" if candidate else
                      "ELIGIBLE" if state in {"FIT_ELIGIBLE", "FIT_ELIGIBLE_TRAINING_OFF"} else "WAITING")
        if state == "FIT_ELIGIBLE_TRAINING_OFF" and recurring:
            reason = "The qualified sample awaits the next managed fit and resource check"
        if not cfg["queue"]:
            forward = {"state":"BASELINE_ONLY", "reason":"The shelved Swing challenger has no fitting or evaluation queue"}
        elif evaluation:
            cohort = evaluation["body"]
            end = utc(cohort["end_exclusive"])
            output = read(PILOT / "pilot-evaluation" / name / end.strftime("%Y%m%dT%H%M%S") / "result.json")
            forward = {"id":evaluation["id"], "state":output["state"] if output else
                       "MANUAL_EVALUATION_DUE" if now >= utc(cohort["scoring_available_at"]) else "FORWARD_HORIZON_OPEN",
                       **{k:cohort[k] for k in ("start","end_exclusive","scoring_available_at")}}
        else:
            forward = {"state":"NEEDS_SEPARATE_SOURCE_COVERAGE" if candidate else "WAITING_FOR_FROZEN_CANDIDATE"}
        evidence = {key:proof["body"][key] for key in ("rows","start","cutoff","receipt_last","admitted_at","parity","scope")} if proof else None
        if evidence:
            evidence["id"] = proof["id"]
            evidence["available_as_of"] = max(utc(evidence["receipt_last"]), utc(evidence["admitted_at"])).isoformat()
        last = saved.get("checked_cutoff")
        next_cut = max(start,utc(last) if last else start)+pd.Timedelta(days=1)
        profiles[name] = {"label_start":cfg["label_start"], "state":state, "reason":reason,
                          "feature_readiness":"PINNED_CAUSAL_CONTEXT" if pin else "VERIFIED_CONTEXT_BRIDGE_PENDING" if proof else "WAITING",
                          "feature_evidence":evidence, "train":saved.get("mature_train",0), "train_required":cfg["minimum_train"] if cfg["queue"] else None,
                          "inner":saved.get("mature_inner",0), "inner_required":cfg["minimum_inner"] if cfg["queue"] else None,
                          "new_mature":saved.get("mature_total",0), "new_mature_required":cfg["minimum_new_mature"] if cfg["queue"] else None,
                          "inner_boundary":saved.get("training_split"), "fit_status":fit_status,
                          "candidate_version":candidate["body"]["fit"]["version"] if candidate else None,
                          "evaluation":forward,"next_managed_check":pulse,"next_daily_sample_review":(next_cut+pd.Timedelta(hours=2)).isoformat(),
                          "fit_date":"Depends on eligible mature outcomes" if cfg["queue"] else None}
    return {"state":"PILOT_REGISTERED", "id":registration["id"], "parent_phase17":body["parent_phase17"], "activation":body["activation"],
            "recurring_training":recurring,"profiles":profiles,"first_spot_managed":managed_capture_view(read(ROOT / "first-spot-managed.json")),
            "first_complete_futures":read(ROOT / "first-complete-managed.json"),"last_resource_guard":guard,
            "actual_pilot_fits":len(list((PILOT / "profiles").glob("*/fit/fits/*.json"))),
            "approval":"No validated model","execution":"MANUAL_OUTSIDE_LAB"}
