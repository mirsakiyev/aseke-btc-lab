from pathlib import Path

from lab.config import ROOT as PROJECT, fingerprint
from lab.phase9.reference import OUTPUT as PHASE9
from lab.phase13.registry import CAMPAIGN, checked, load_parent, sealed
from lab.phase12.study import lane_path
from lab.storage import RUNS, file_hash, read_json, utc, write_json, writer_lock


ROOT = RUNS / "phase16"
SPOT = "cffb471b2e886f6aca8bfb79e590c17e46efae5dd144dc175dbfc65fa652cd42"
POLICY = {
    "version": "phase16-economic-challengers-v1", "alpha": 10., "entry_threshold_net_r": 0.,
    "risk_control_multiplier": .5, "minimum_closed": 25, "max_fit_attempts": 20, "max_attempts_per_fit": 2,
    "max_account_attempts": 42, "max_attempts_per_account": 2,
    "budget": {"wall_seconds": 7200, "max_threads": 2, "child_rss_mib": 640, "output_cap_mib": 256,
               "min_free_disk_mib": 2048, "min_available_ram_mib": 1250},
    "cases": ["base", "double_cost", "delay"],
    "controls": ["unfiltered", "training_mean", "half_risk_rule", "cash"],
    "cost_stress": "Two times reference taker fee and two times adverse price adjustment, both per side",
    "delay_stress": "Entry submission and expiry one native bar later, non-liquidation exit one native bar later. Preserve original deadline and Day session. Futures 1 minute, Spot 1 hour.",
    "target": "Mature simulator net return in R after fees, adverse execution, and applicable funding. Threshold strictly greater than zero.",
    "decision": "Descriptive only. Require 25 closed baseline and stress trades, all original folds, resolved accounting, and a positive dependence-aware lower expectancy bound. Improvement also requires candidate return above cash, rule, half-risk rule and mean in all cases, with base drawdown no worse than full rule. A fully sampled lower base return than both rule controls is deterioration. All other cases inconclusive.",
}
HYPOTHESES = {
    "day": ("day_perp_research_v1", "fixed_r_v1", "Cost load and volatility identify economically viable intraday breakouts",
            ["atr_fraction", "realized_vol", "relative_volume", "context_distance_0", "context_distance_1", "modeled_cost_to_stop"]),
    "swing": ("swing_perp_research_v1", "atr_trail_v1", "Directional breakout strength and trend persistence add value beyond a smaller account risk budget",
              ["directional_breakout_atr", "return_5", "relative_volume", "atr_fraction", "context_slope_0"]),
    "position": ("position_spot_research_v1", "fixed_r_v1", "Trend persistence and breakout quality improve long-only Spot entry selection",
                 ["directional_breakout_atr", "relative_volume", "ema_slope", "realized_vol", "context_distance_0", "context_slope_0"]),
}


def code_version():
    paths = [Path(__file__), Path(__file__).with_name("evaluate.py")] + [PROJECT / name for name in (
        "lab/phase10/evaluation.py", "lab/phase10/partition.py", "lab/phase10/trading.py", "lab/phase12/study.py",
        "lab/learning/evaluation.py", "lab/learning/features.py", "lab/learning/simulator.py")]
    return fingerprint({str(p.relative_to(PROJECT)): file_hash(p) for p in paths})


def folder(identifier):
    if len(identifier) != 64 or any(c not in "0123456789abcdef" for c in identifier):
        raise ValueError("Invalid Phase 16 campaign identity")
    return ROOT / "campaigns" / identifier


def recipe(name, trial, design):
    pid, exit_id, question, columns = HYPOTHESES[name]
    if (trial["profile"], trial["entry"], trial["exit_policy"]) != (pid, "breakout_v1", exit_id):
        raise ValueError("The matching admitted label recipe changed")
    settings = design["settings"]
    profile = settings["profiles"][pid]
    return {"trial": trial, "hypothesis": question, "features": columns, "profile": profile, "folds": design["folds"],
            "train_start": settings["evaluation"]["train_start"], "exclusive_data_end": design["available_at_cutoff_exclusive"],
            "risk": settings["risk"][profile["risk"]], "costs": settings["costs"][trial["cost"]],
            "minimum_train": settings["models"]["ridge_v1"]["minimum_train"],
            "minimum_inner": settings["evaluation"]["minimum_inner"], "minimum_outer": settings["evaluation"]["minimum_outer"],
            "interval_days": profile["interval_days"], "minimum_new_mature": profile["minimum_new_mature"]}


def freeze_initial():
    with writer_lock(ROOT / ".initial.lock"):
        pointer = ROOT / "initial.json"
        if pointer.exists():
            identifier = read_json(pointer)["id"]
            return sealed(folder(identifier) / "manifest.json", identifier)
        parent, result, fit_manifest, view, resource = load_parent()
        body = parent["body"]
        holdout = body["design"]["holdout"]
        embargo = utc(holdout["start"])
        if view["body"]["use"] != "RESEARCH_ONLY" or utc(view["body"]["source_end_exclusive"]) > embargo:
            raise ValueError("Futures view is outside admitted historical research")
        profiles = {}
        for name in ("day", "swing"):
            pid, exit_id, *_ = HYPOTHESES[name]
            trial = next(t for t in body["trial_order"][name] if t["profile"] == pid and t["entry"] == "breakout_v1"
                         and t["exit_policy"] == exit_id and t["model"] == "ridge_v1")
            lane = lane_path(CAMPAIGN, name, trial)
            proof = read_json(lane / "prepared.json")
            checked(lane / "mature-labels.parquet", proof["label_sha256"])
            checked(lane / "labels.jsonl.gz", proof["evidence_sha256"])
            profiles[name] = {**recipe(name, trial, body["design"]), "source": "PHASE12_PARTITIONED", "data_manifest": view["id"],
                              "source_scope": "BINANCE_DEVELOPMENT_RESEARCH_ONLY", "use": "RESEARCH_ONLY",
                              "label_sha256": proof["label_sha256"], "preparation": fingerprint(proof),
                              "prior_admission": {fold: row["sample_counts"] for fold,row in proof["folds"].items()}}
        spot_folder = PHASE9 / "campaigns" / SPOT
        spot = sealed(spot_folder / "manifest.json", SPOT)["body"]
        erratum = read_json(spot_folder / "lineage-erratum.json")
        design = spot["design"]
        trial = next(t for t in design["trials"] if t["entry"] == "breakout_v1" and t["exit_policy"] == "fixed_r_v1"
                     and t["model"] == "ridge_v1")
        records = design["data"][trial["profile"]]["trade"]
        if any(item["descriptor"]["venue"] != "KuCoin" or utc(item["descriptor"]["end_exclusive"]) > embargo for item in records):
            raise ValueError("Spot archive does not match the admitted KuCoin development scope")
        profiles["position"] = {**recipe("position", trial, design), "source": "PHASE9_SPOT", "data_manifest": SPOT,
            "source_scope": "KUCOIN_DEVELOPMENT_RESEARCH_ONLY", "use": "RESEARCH_ONLY", "assets": [row["id"] for row in records],
            "lineage_sha256": file_hash(spot_folder / "lineage-erratum.json"), "lineage_fields": sorted(erratum),
            "prior_admission": {"y2023": "Insufficient inner", "y2024": "Insufficient inner", "y2025": "Evaluated"}}
        data = {"kind": "INITIAL_DEVELOPMENT", "profiles": profiles, "policy": POLICY, "holdout": holdout, "code": code_version(),
                "source_futures_campaign": parent["id"], "source_futures_result_sha256": file_hash(CAMPAIGN/"result.json"),
                "source_futures_fit_manifest": fit_manifest["id"], "resource_manifest": resource["id"],
                "source_spot_result_sha256": file_hash(spot_folder / "result.json"),
                "scope": "RESEARCH_ONLY", "evidence": "CONSUMED_DEVELOPMENT", "phase14_and_phase15_used": False,
                "note": "Earlier full-feature Ridge, histogram, rules and mean trials exist. The fixed subset and alpha 10 are new, selected before this campaign. No new source requests."}
        identifier = fingerprint(data)
        write_json(folder(identifier) / "manifest.json", {"id": identifier, "body": data})
        write_json(pointer, {"id": identifier})
        return sealed(folder(identifier) / "manifest.json", identifier)
