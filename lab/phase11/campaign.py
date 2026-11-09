from collections import Counter
from contextlib import suppress
from pathlib import Path
import subprocess
import sys
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.config import ROOT as PROJECT, fingerprint
from lab.learning.control import admission, cancel_tree
from lab.learning.design import MIB, STATE as LEARNING_STATE
from lab.learning.evaluation import benchmarks, forecasts
from lab.learning.state import status as legacy_status
from lab.phase9.reference import controls as phase9_controls
from lab.phase10.evaluation import account_replay, label_batch
from lab.phase10.partition import Chunk, PartitionedMarket
from lab.phase10.worker import controls as phase10_controls
from lab.phase11.archive import size_under
from lab.phase11.benchmark import jsonl_bytes, profile_data, reference_design
from lab.phase11.plan import ROOT, freeze_plan
from lab.phase11.qualify import matching_funding, qualify, read_values
from lab.phase11.resources import freeze as freeze_resources
from lab.storage import file_hash, put_bytes, read_json, utc, utc_now, write_json, writer_lock


class BudgetExhausted(RuntimeError):
    pass


def code_fingerprint(coverage=False):
    files = list(Path(__file__).parent.glob("*.py"))
    files.extend(PROJECT / "lab" / "phase10" / name for name in ("partition.py", "trading.py", "evaluation.py"))
    if coverage:
        files.extend(PROJECT / "lab" / "phase12" / name for name in ("view.py", "study.py"))
    return fingerprint({str(path.relative_to(PROJECT)): file_hash(path) for path in files})


def campaign_folder(identifier):
    if len(identifier) != 64 or any(char not in "0123456789abcdef" for char in identifier):
        raise ValueError("Invalid campaign ID")
    amended = ROOT.parent / "phase12" / "campaigns" / identifier
    return amended if (amended / "manifest.json").exists() else ROOT / "campaigns" / identifier


def gates_off():
    return (legacy_status()["mode"] == "off" and not phase9_controls()["training"]
            and not any(phase10_controls()["training"].values()))


def freeze_campaign(quality, amendment, plan):
    design, cfg, fee = reference_design()
    body = quality["body"]
    if fingerprint(body) != quality["id"] or not body["eligible"]:
        raise ValueError("A checked qualified research-only bundle is required")
    if body["amendment"] != amendment["id"] or body["plan"] != plan["id"]:
        raise ValueError("Qualification lineage mismatch")
    trials = {trial["id"]: {**trial, "cost": fee} for trial in design["trials"]}
    settings = design["settings"]
    # Keep the parent's hypothesis for linear USDT contract rules. Published Binance BTC quantity
    # is expressed in base units; this representation uses one 0.001 BTC simulator unit.
    contract = settings["instruments"]["linear_usdt_perp_v1"]
    order = {name: [trials[key] for key in plan["body"]["profiles"][name]["trial_order"]]
             for name in plan["body"]["profile_order"]}
    design = {key: design[key] for key in ("feature_version", "label_version", "settings", "folds",
                                          "seed", "holdout", "available_at_cutoff_exclusive")}
    design["folds"] = [fold for fold in design["folds"] if fold["id"] in body["eligible"]]
    if body["source_end_exclusive"] != design["folds"][-1]["outer_end"]:
        # The freeze uses a narrower available data prefix when later folds are unqualified.
        if utc(body["source_end_exclusive"]) != utc(design["folds"][-1]["outer_end"]):
            raise ValueError("Archive prefix and last eligible evaluation boundary disagree")
    design["available_at_cutoff_exclusive"] = body["source_end_exclusive"]
    coverage = body.get("coverage_policy")
    stable_input = fingerprint([amendment["id"], quality["id"], order, design, contract, cfg["fee_evidence"]])
    manifest_body = {"protocol": plan["body"]["protocol"], "input_id": stable_input, "plan": plan["id"],
                     "qualification": quality["id"], "amendment": amendment["id"], "design": design,
                     "use": "RESEARCH_ONLY", "scope": "HYPOTHETICAL_REPLAY", "source": body["source"],
                     "profiles": list(order), "trial_order": order, "contract": contract,
                     "rules_evidence": "Phase 8 frozen single-tier diagnostic assumption. Binance is the data venue. "
                                       "No point-in-time venue margin, size or liquidation reconstruction is claimed.",
                     "cost_evidence": {"id": fee, **cfg["fees"][fee], **cfg["fee_evidence"],
                                       "scope": "KuCoin reference fees on Binance history"},
                     "code": code_fingerprint(bool(coverage))}
    registry = ROOT / "campaigns"
    if coverage:
        manifest_body["coverage_policy"] = coverage
        manifest_body["original_qualification"] = body["original"]["qualification"]
        registry = ROOT.parent / "phase12" / "campaigns"
    with writer_lock(ROOT / ".campaign-registry.lock"):
        for path in registry.glob("*/manifest.json"):
            prior = read_json(path)
            if prior["body"]["input_id"] == stable_input:
                return prior
        identifier = fingerprint(manifest_body)
        manifest = {"id": identifier, "body": manifest_body, "created_at": utc_now().isoformat()}
        write_json(registry / identifier / "manifest.json", manifest)
    return manifest


def start_clock(folder, seconds, now=None):
    path = folder / "deadline.json"
    if not path.exists():
        start = now if now is not None else utc_now()
        write_json(path, {"started_at": start.isoformat(),
                          "deadline": (start + pd.Timedelta(seconds=seconds)).isoformat()})
    return read_json(path)


def reserve(folder, namespace, key, amount, limit):
    directory = folder / "reservations" / namespace
    path = directory / (fingerprint(key) + ".json")
    if path.exists():
        raise BudgetExhausted(f"Reservation already consumed: {namespace} {key}")
    used = sum(read_json(record)["amount"] for record in directory.glob("*.json"))
    if amount < 0 or used + amount > limit:
        raise BudgetExhausted(f"Frozen {namespace} ceiling reached")
    write_json(path, {"at": utc_now().isoformat(), "key": key, "amount": amount})
    return path


def ledger(folder):
    used = {}
    for directory in (folder / "reservations").glob("*"):
        used[directory.name] = sum(read_json(record)["amount"] for record in directory.glob("*.json"))
    results = [read_json(path) for path in folder.glob("trials/*/result.json")]
    return {"reservations": used, "actual_estimator_fits": len(list((folder / "completed-fits").glob("*.json"))),
            "candidate_results": len(results), "candidate_states": dict(Counter(row["status"] for row in results)),
            "forecast_evaluation_windows": sorted({item["fold"] for row in results for item in row.get("folds", [])
                                                  if item["status"] == "EVALUATED"}),
            "rule_evaluation_windows": sorted({fold for row in results if row["trial"]["model"] == "rules_v1"
                                              for fold in row.get("metrics", {}).get("completed_folds", {})})}


def load_market(quality, resource, design):
    body, cap = quality["body"], resource["body"]
    limit = cap["unique_observation_allowance"]
    if limit["trade"] != limit["mark"] or limit["index"] != 0:
        raise ValueError("Unexpected frozen resource allowances")
    partitions = [Chunk(Path(row["trade"]), Path(row["mark"]), row["trade_sha256"], row["mark_sha256"])
                  for row in body["chunks"]]
    market = PartitionedMarket(partitions, {}, design["holdout"]["start"], limit["trade"],
                               chunk_limit=cap["max_native_partition_rows"])
    fund = pd.concat([read_values(item, ["settlement_at", "rate", "interval_minutes"]) for item in body["funding_chunks"]],
                     ignore_index=True)
    fund, _ = matching_funding(fund, market.index[0], utc(body["source_end_exclusive"]))
    if len(fund) > limit["funding"]:
        raise ValueError("Funding exceeds the amended ceiling")
    market.set_funding(fund.set_index("settlement_at").rate)
    return market


def checked_json(path, expected):
    result = read_json(path)
    if result["id"] != expected or fingerprint(result["body"]) != expected:
        raise ValueError("Frozen input checksum mismatch")
    return result


def runtime_manifest(folder, manifest):
    body = manifest["body"]
    coverage = bool(body.get("coverage_policy"))
    current = code_fingerprint(coverage)
    if current == body["code"]:
        return manifest
    path = folder / "code-continuations" / f"{current}.json"
    if not coverage or not path.exists():
        raise ValueError("Frozen campaign code identity changed")
    revision = read_json(path)
    if (fingerprint(revision["body"]) != revision["id"] or revision["body"]["campaign"] != manifest["id"]
            or revision["body"]["original_manifest_sha256"] != file_hash(folder / "manifest.json")
            or revision["body"]["previous_code"] != body["code"] or revision["body"]["current_code"] != current):
        raise ValueError("Code continuation is not linked to this immutable campaign")
    frozen_preflight = revision["body"].get("fit_manifest_sha256")
    if frozen_preflight and file_hash(folder / "fit-manifest.json") != frozen_preflight:
        raise ValueError("Code continuation changed the prepared fit manifest")
    return {**manifest, "body": {**body, "code": current, "code_continuation": revision["id"]}}


def fit(identifier):
    folder = campaign_folder(identifier)
    manifest = runtime_manifest(folder, checked_json(folder / "manifest.json", identifier))
    body = manifest["body"]
    coverage = bool(body.get("coverage_policy"))
    resource = checked_json(ROOT / "resource-amendments" / f'{body["amendment"]}.json', body["amendment"])
    qualification_dir = ROOT.parent / "phase12" / "views" if coverage else ROOT / "qualification"
    quality = checked_json(qualification_dir / f'{body["qualification"]}.json', body["qualification"])
    caps, design = resource["body"]["replay"], body["design"]
    budget = resource["body"]["unchanged_guards"]
    deadline = utc(read_json(folder / "deadline.json")["deadline"])

    def guard():
        if utc_now() >= deadline:
            raise BudgetExhausted("Frozen campaign deadline reached")
        if not gates_off():
            raise BudgetExhausted("An independent continuous training gate was enabled")

    with writer_lock(LEARNING_STATE / ".fit.lock"), threadpool_limits(limits=budget["max_threads"]):
        market = load_market(quality, resource, design)
        if coverage:
            from lab.phase12 import study
            study.release_buffers()
            print(f"Loaded {len(market.index)} aligned native minutes", flush=True)
            execution, eligibility = study.prepare(folder, manifest, market, quality, resource, guard)
        for name in body["profiles"]:
            profile = caps["profiles"][name]
            build_profile = study.profile_data if coverage else profile_data
            data = build_profile(market, design["settings"], profile["profile"], body["contract"])
            active_lane = None
            batch = None
            for trial in body["trial_order"][name]:
                key = fingerprint(trial["id"])
                trial_dir = folder / "trials" / key
                path = trial_dir / "result.json"
                if path.exists():
                    continue
                guard()
                attempt = len(list(trial_dir.glob("attempt-*.json"))) + 1
                row = {"profile": name, "trial": trial, "status": "COMPLETED", "folds": []}
                if attempt > caps["max_profile_trial_attempts"]:
                    write_json(path, {**row, "status": "SKIPPED", "reason": "Frozen trial attempt limit"})
                    continue
                write_json(trial_dir / f"attempt-{attempt}.json", {"at": utc_now().isoformat(), "trial": trial})
                if coverage:
                    print(f"Evaluating {trial['id']}", flush=True)
                    row["runtime_code"] = body["code"]
                    row["code_continuation"] = body.get("code_continuation")
                    trial_info = eligibility["trials"][trial["id"]]
                    trial_design = study.model_design(design, execution, trial_info, quality)
                    excluded = [{"fold": fold, "status": "INSUFFICIENT_EVIDENCE", "reason": ". ".join(info["reasons"]),
                                 "sample_counts": info.get("sample_counts", {})}
                                for fold, info in trial_info["folds"].items() if info["status"] != "ELIGIBLE"]
                    if not trial_design["folds"] and trial["model"] not in ("cash", "passive_25pct"):
                        write_json(path, {**row, "status": "INSUFFICIENT_EVIDENCE", "folds": excluded})
                        continue
                else:
                    trial_design, excluded = design, []
                if trial["model"] in ("cash", "passive_25pct"):
                    metrics = benchmarks(design, data, trial["cost"], trial["model"])
                    write_json(path, {**row, "status": metrics.get("status", "COMPLETED"), "metrics": metrics})
                    continue
                lane = (trial["entry"], trial["exit_policy"])
                if lane != active_lane:
                    if coverage:
                        batch = study.read_lane(folder, name, design, data, trial, trial_info["lane"]["hash"])
                        active_lane = lane
                    else:
                        batch = None
                if lane != active_lane:
                    label_count = len(list((folder / "lanes" / name / fingerprint(lane)).glob("attempt-*.json"))) + 1
                    if label_count > caps["max_profile_trial_attempts"]:
                        write_json(path, {**row, "status": "SKIPPED", "reason": "Frozen label replay attempt limit"})
                        continue
                    count = (profile["maximum_hold_minutes"] + 3) * profile["decision_bar_ceiling"]
                    reserve(folder, "kernel_visits", [name, list(lane), "labels", label_count], count,
                            caps["trade_kernel_visits_all_attempts_ceiling"])
                    label_dir = folder / "lanes" / name / fingerprint(lane)
                    write_json(label_dir / f"attempt-{label_count}.json", {"at": utc_now().isoformat(), "lane": lane})
                    setup, cost, labels, evidence, summary = label_batch(design, data, trial)
                    if not (label_dir / "labels.jsonl.gz").exists():
                        put_bytes(label_dir / "labels.jsonl.gz", jsonl_bytes(evidence))
                    batch = (setup, cost, labels, [], summary)
                    del evidence
                    active_lane = lane
                pending = None

                def stage(_phase, details=None):
                    nonlocal pending
                    if pending:
                        write_json(folder / "completed-fits" / pending.name, read_json(pending))
                        pending = None
                    guard()
                    if details is None:
                        return
                    mean = trial["model"] == "mean_control_v1"
                    pending = reserve(folder, "mean_fits" if mean else "estimator_fits",
                                      [trial["id"], details["fold"], details["partition"]], 1,
                                      caps["max_baseline_mean_computations"] if mean else caps["max_estimator_fits"])
                    if mean:
                        pending = None

                try:
                    predictions, folds = forecasts(trial_design, data, trial, batch, folder / "models", stage)
                except BudgetExhausted as exc:
                    if not str(exc).startswith("Reservation already consumed:"):
                        raise
                    write_json(path, {**row, "status": "SKIPPED", "reason": str(exc)})
                    continue
                if coverage:
                    study.verify_artifacts(folds, trial, execution, body["code"])
                    by_id = {entry["fold"]: entry for entry in [*folds, *excluded]}
                    folds = [by_id[entry["id"]] for entry in design["folds"]]
                row.update(folds=folds, labels={k: v for k, v in batch[4].items() if k != "label_outcome_ids"},
                           outcome_identity_digest=fingerprint(batch[4]["label_outcome_ids"]))
                if predictions:
                    available = profile["full_equity_timeline_observation_ceiling"] // profile["account_passes"]
                    count = (profile["maximum_hold_minutes"] + 3) * (
                        (available + profile["native_minutes_per_decision"] - 1) // profile["native_minutes_per_decision"])
                    reserve(folder, "kernel_visits", [trial["id"], "account", attempt], count,
                            caps["trade_kernel_visits_all_attempts_ceiling"])
                    reserve(folder, "equity_visits", [trial["id"], attempt], available,
                            caps["full_equity_visits_all_attempts_ceiling"])
                    reserve(folder, "native_clock_filter_rows", [trial["id"], attempt], len(market.index),
                            caps["native_clock_filter_rows_all_attempts_ceiling"])
                    versions = {item["fold"]: item["model_artifacts"]["outer"]["version"]
                                for item in folds if item.get("model_artifacts")}
                    curve = study.EquityCurve(design, market.step) if coverage else None
                    metrics, orders = account_replay(design, data, trial, batch, predictions, versions, curve=curve)
                    if coverage:
                        study.write_orders(trial_dir / "orders.jsonl.gz", orders)
                        metrics["coverage_scope"] = "ALL_FROZEN_OUTER_WINDOWS" if metrics["full_window_observed"] else "PARTIAL_OUTER_WINDOWS"
                    else:
                        put_bytes(trial_dir / "orders.jsonl.gz", jsonl_bytes(orders))
                    row["metrics"] = metrics
                    del orders, curve
                if (not row.get("metrics", {}).get("full_window_observed")
                        or any(item["status"] not in ("EVALUATED", "RULES") for item in folds)):
                    row["status"] = "INSUFFICIENT_EVIDENCE"
                write_json(path, row)
            del data, batch
            if coverage:
                study.release_buffers()
    summary = ledger(folder)
    all_folds = set(quality["body"]["eligible"]) == set(quality["body"]["folds"])
    write_json(folder / "result.json", {"campaign": identifier, "at": utc_now().isoformat(),
               "state": "COMPLETE" if all_folds and summary["candidate_results"] == caps["max_trials"] else "PARTIAL",
               "scope": body["scope"], "use": body["use"], "qualified_folds": quality["body"]["eligible"],
               **({"fit_manifest": execution["id"], "coverage_policy": body["coverage_policy"]} if coverage else {}), **summary})


def supervise(manifest):
    folder = campaign_folder(manifest["id"])
    result = folder / "result.json"
    if result.exists():
        return {**read_json(result), "reused": True}
    body = manifest["body"]
    campaign_storage = ROOT.parent / "phase12" / "campaigns" if body.get("coverage_policy") else ROOT / "campaigns"
    resource = checked_json(ROOT / "resource-amendments" / f'{body["amendment"]}.json', body["amendment"])["body"]
    budget = resource["unchanged_guards"]
    if not resource["fit_projection_within_existing_caps"]:
        return {"state": "WAITING_FOR_RESOURCES", "reason": "Measured projection exceeds the existing guard", **ledger(folder)}
    with writer_lock(LEARNING_STATE / ".supervisor.lock"), writer_lock(ROOT / ".research.lock"):
        if result.exists():
            return {**read_json(result), "reused": True}
        reason = admission(budget)
        if not gates_off() or reason:
            return {"state": "WAITING_FOR_ADMISSION", "reason": reason or "Continuous training gates must be OFF",
                    **ledger(folder)}
        clock = start_clock(folder, budget["wall_seconds"])
        deadline = utc(clock["deadline"])
        if utc_now() >= deadline:
            return {"state": "PARTIAL", "reason": "Original campaign deadline expired", **clock, **ledger(folder)}
        peak_rss = 0
        with (folder / "research.log").open("ab") as log:
            child = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase11", "_fit", manifest["id"]],
                                     cwd=PROJECT, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                     creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
            root = psutil.Process(child.pid)
            write_json(folder / "active.json", {"pid": root.pid, "created": root.create_time(), "at": utc_now().isoformat()})
            try:
                while child.poll() is None:
                    with suppress(psutil.NoSuchProcess):
                        peak_rss = max(peak_rss, sum(p.memory_info().rss for p in [root, *root.children(recursive=True)]))
                    output = size_under(campaign_storage)
                    reason = ("Campaign wall clock" if utc_now() >= deadline else "Child RSS" if peak_rss > budget["child_rss_mib"]*MIB
                              else "Research output ceiling" if output >= budget["output_cap_mib"]*MIB else
                              "Continuous training enabled" if not gates_off() else admission(budget))
                    if reason:
                        cancel_tree(root)
                        break
                    time.sleep(2)
            finally:
                (folder / "active.json").unlink(missing_ok=True)
                if child.poll() is None:
                    cancel_tree(root)
        observation = {"at": utc_now().isoformat(), "peak_child_rss_bytes_sampled": peak_rss,
                       "outputs_bytes": size_under(campaign_storage), "stop_reason": reason, **clock}
        path = folder / "observations" / (fingerprint(observation) + ".json")
        write_json(path, observation)
        return ({**read_json(result), "observation": observation} if result.exists() else
                {"state": "PARTIAL", "reason": reason or f"Child exited {child.poll()}", **observation, **ledger(folder)})


def run_once():
    plan, resource = freeze_plan(), freeze_resources()
    quality = qualify(plan, resource)
    if not quality["body"]["eligible"]:
        path = ROOT / "admission-attempts" / f'{quality["id"]}.json'
        with writer_lock(ROOT / ".campaign-registry.lock"):
            if path.exists():
                return {**read_json(path), "reused": True}
            evidence = {"state": "WAITING_FOR_QUALIFIED_FUTURES_BUNDLE", "qualification": quality["id"],
                        "resource_amendment": resource["id"], "at": utc_now().isoformat(), "deadline_started": False,
                        "eligible_folds": [], "actual_estimator_fits": 0, "candidate_attempts": 0,
                        "skipped_before_fitting": resource["body"]["replay"]["max_trials"]}
            write_json(path, evidence)
            return evidence
    return supervise(freeze_campaign(quality, resource, plan))


def resume(identifier):
    return supervise(checked_json(campaign_folder(identifier) / "manifest.json", identifier))
