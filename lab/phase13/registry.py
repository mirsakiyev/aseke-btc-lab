import gzip
import json
from pathlib import Path

from lab.config import ROOT as PROJECT, fingerprint
from lab.storage import RUNS, file_hash, read_json, utc, utc_now, write_json, writer_lock


ROOT = RUNS / "phase13"
PARENT = "a2846a8de85d2da4466f53d237ee1b7760945c0b44f0f0f72d255bc3a8edf44e"
CAMPAIGN = RUNS / "phase12" / "campaigns" / PARENT
CATALOG = (("day", "day_perp_research_v1", "pullback_v1", "fixed_r_v1", "ridge_v1"),
           ("swing", "swing_perp_research_v1", "breakout_v1", "atr_trail_v1", "hist_v1"))
STRESSES = {"double_cost": {"taker_multiplier": 2, "adverse_multiplier": 2, "delay_bars": 0},
            "delayed_entry_exit": {"taker_multiplier": 1, "adverse_multiplier": 1,
                                   "entry_ioc_and_expiry_shift_bars": 1, "delay_exit_bars": 1,
                                   "execution_bar_seconds": 60, "original_holding_deadline_preserved": True}}


def checked(path, digest):
    path = Path(path)
    if file_hash(path) != digest:
        raise ValueError(f"Frozen input hash changed for {path.name}")
    return path


def sealed(path, identifier=None):
    record = read_json(path)
    if (identifier is not None and record["id"] != identifier) or fingerprint(record["body"]) != record["id"]:
        raise ValueError(f"Invalid immutable identity for {path.name}")
    return record


def orders(path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for row in handle:
            yield json.loads(row)


def load_parent():
    manifest = sealed(CAMPAIGN / "manifest.json", PARENT)
    result = read_json(CAMPAIGN / "result.json")
    execution = sealed(CAMPAIGN / "fit-manifest.json", result["fit_manifest"])
    body = manifest["body"]
    view_path = RUNS / "phase12" / "views" / (body["qualification"] + ".json")
    view = sealed(view_path, body["qualification"])
    budget_path = RUNS / "phase11" / "resource-amendments" / (body["amendment"] + ".json")
    budget = sealed(budget_path, body["amendment"])
    if result["campaign"] != PARENT or result["state"] != "COMPLETE" or result["candidate_results"] != 52:
        raise ValueError("Expected the completed Phase 12 campaign")
    return manifest, result, execution, view, budget


def freeze():
    pointer = ROOT / "registry.json"
    with writer_lock(ROOT / ".registry.lock"):
        if pointer.exists():
            return sealed(ROOT / "registries" / (read_json(pointer)["id"] + ".json"))
        manifest, result, execution, view, budget = load_parent()
        parent = manifest["body"]
        design = parent["design"]
        settings = design["settings"]
        fitted = {trial["id"]: trial for rows in parent["trial_order"].values() for trial in rows}
        rows = {}
        for path in (CAMPAIGN / "trials").glob("*/result.json"):
            record = read_json(path)
            name = record["trial"]["id"]
            if name in rows or fitted.get(name) != record["trial"]:
                raise ValueError(f"Phase 12 trial does not match frozen catalog: {name}")
            rows[name] = path, record
        if len(rows) != 52:
            raise ValueError("Expected all 52 Phase 12 candidate/control results")
        registered = {}
        for profile, prefix, entry, exit_policy, selected in CATALOG:
            for role, model in (("estimator", selected), ("unfiltered", "rules_v1"),
                                ("mean", "mean_control_v1"), ("cash", "cash")):
                trial_id = f"{prefix}/{entry}/{exit_policy}/{model}" if role != "cash" else f"{prefix}/cash"
                if trial_id not in rows:
                    raise ValueError(f"Exact requested {profile} {role} trial is absent: {trial_id}")
                result_path, record = rows[trial_id]
                entry_path = result_path.parent / "orders.jsonl.gz"
                historical, means = {}, {}
                if role == "mean":
                    for row in orders(entry_path):
                        score = row["score_net_r"]
                        old = means.setdefault(row["fold"], score)
                        if old != score:
                            raise ValueError(f"Non-constant saved mean control for {trial_id}")
                eligible = []
                for fold in design["folds"]:
                    name = fold["id"]
                    info = next((row for row in record.get("folds", []) if row["fold"] == name), None)
                    if role != "cash" and (not info or info["status"] not in ("RULES", "EVALUATED")):
                        continue
                    eligible.append(name)
                    outer = info.get("model_artifacts", {}).get("outer") if info else None
                    inner = info.get("model_artifacts", {}).get("inner") if info else None
                    if role == "estimator":
                        for artifact in (inner, outer):
                            if not artifact or artifact["fit_manifest"] != execution["id"] or artifact["use"] != "RESEARCH_ONLY":
                                raise ValueError(f"Unverified artifact identity for {trial_id} {name}")
                            checked(artifact["path"], artifact["sha256"])
                    historical[name] = {"activation": [fold["outer_start"], fold["outer_end"]],
                                        "inner_training_cutoff_exclusive": fold["inner_start"],
                                        "outer_training_cutoff_exclusive": fold["outer_start"],
                                        "inner_artifact": inner, "outer_artifact": outer,
                                        "outer_mean_net_r": means.get(name)}
                if eligible != [fold["id"] for fold in design["folds"]]:
                    raise ValueError(f"Unexpected eligibility for required Phase 13 lane: {trial_id}")
                leaf = historical[eligible[-1]]
                trial = record["trial"]
                item = {"parent_campaign": PARENT, "profile": profile, "trial": trial, "role": role,
                        "result": str(result_path), "result_sha256": file_hash(result_path),
                        "orders": str(entry_path) if entry_path.exists() else None,
                        "orders_sha256": file_hash(entry_path) if entry_path.exists() else None,
                        "historical_walk_forward": historical, "eligible_folds": eligible,
                        "fixed_artifact": leaf["outer_artifact"], "fixed_mean_net_r": leaf["outer_mean_net_r"],
                        "fixed_training_cutoff_exclusive": leaf["outer_training_cutoff_exclusive"] if role in ("mean", "estimator") else None,
                        "feature_columns": execution["body"]["feature_and_label_boundaries"][profile]["feature_columns"],
                        "warmup": execution["body"]["feature_and_label_boundaries"][profile],
                        "training_maturity": "Completed labels resolving strictly before each training boundary",
                        "threshold_net_r": settings["evaluation"]["threshold_net_r"],
                        "estimator_parameters": settings["models"].get(model),
                        "strategy": settings["strategies"].get(trial.get("entry")),
                        "exit_parameters": settings["exits"].get(trial.get("exit_policy")),
                        "profile_rules": settings["profiles"][prefix],
                        "risk": settings["risk"][settings["profiles"][prefix]["risk"]],
                        "execution_rules": settings["execution"], "contract": parent["contract"],
                        "cost": parent["cost_evidence"], "funding": view["body"]["funding"],
                        "source": view["body"]["source"], "source_view": view["id"],
                        "runtime_code": record.get("runtime_code", execution["body"]["campaign_code"]),
                        "parent_fitting_fingerprint": execution["id"],
                        "research_evidence": "DESCRIPTIVE_CONSUMED_DEVELOPMENT_ONLY", "data_admission": "HISTORICAL_COMPLETE",
                        "permitted_use": "RESEARCH_ONLY", "venue_compatibility": "KUCOIN_TRANSFER_UNTESTED",
                        "actionable_approval": "NO", "validation": "No validated model",
                        "comparator": f"{profile}/unfiltered" if role == "estimator" else f"{profile}/cash",
                        "selection": "Exact operator shortlist after observing Phase 12. No substitution.",
                        "known_limitations": "Historical Binance source, assumed single-tier futures replay and KuCoin reference fees"}
                if item["fixed_training_cutoff_exclusive"]:
                    item["fixed_age_days_at_freeze"] = (utc_now() - utc(item["fixed_training_cutoff_exclusive"])).days
                registered[f"{profile}/{role}"] = {"id": fingerprint(item), **item}
        all_trials = {name: {"status": row["status"], "result": str(path), "sha256": file_hash(path)}
                      for name, (path, row) in sorted(rows.items())}
        mechanical = [str(path) for path in RUNS.glob("learning/**/simulator-diagnostics.json")]
        body = {"parent_campaign": PARENT, "parent_manifest_sha256": file_hash(CAMPAIGN / "manifest.json"),
                "parent_result_sha256": file_hash(CAMPAIGN / "result.json"), "parent_fit_manifest": execution["id"],
                "view": view["id"], "resource_profile": budget["id"], "folds": design["folds"],
                "holdout": design["holdout"], "candidates": registered, "all_trials": all_trials,
                "selection_timing": "POST_PHASE12", "policy": {"version": "phase13-two-frozen-stresses-v1",
                    "maximum_additional_continuous_replays": 12, "new_historical_fits": 0, "stresses": STRESSES,
                    "historical_scores": "Exact saved per-order prediction at the original cutoff. No threshold search.",
                    "reused_exact_account_stresses": [], "mechanical_only_stress_evidence": mechanical,
                    "mechanical_definition_sha256": file_hash(PROJECT / "lab/learning/diagnostics.py"),
                    "budget": {"wall_seconds": 7200, "max_threads": 2, "child_rss_mib": 640,
                               "output_cap_mib": 256, "min_free_disk_mib": 2048, "min_available_ram_mib": 1250}},
                "note": "Existing synthetic stress fixtures use different data and do not replace these account stresses."}
        record = {"id": fingerprint(body), "body": body, "frozen_at": utc_now().isoformat()}
        write_json(ROOT / "registries" / (record["id"] + ".json"), record)
        write_json(pointer, {"id": record["id"]})
        return record
