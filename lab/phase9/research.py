from collections import Counter
from copy import deepcopy
import subprocess
import sys
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.config import ROOT, fingerprint
from lab.learning import state as learning_state
from lab.learning.control import admission, cancel_tree
from lab.learning.data import Market
from lab.learning.design import MIB, STATE as LEARNING_STATE
from lab.learning.evaluation import account_replay, benchmarks, forecasts, label_batch, prepare
from lab.phase9.data import checked_duplicates, metadata, read_import_json
from lab.phase9.ideas import current_rules
from lab.phase9.public import run_folder
from lab.phase9.reference import NAMES, OUTPUT, code_id, controls, freeze_reference, parent_design, reference
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


def imported(asset_id):
    if len(asset_id) != 64 or any(char not in "0123456789abcdef" for char in asset_id):
        raise ValueError("Invalid imported asset ID")
    path = OUTPUT / "imports" / asset_id
    record = read_json(path / "manifest.json")
    original = {key: value for key, value in record.items() if key not in ("id", "normalized_sha256", "published_at")}
    if record["id"] != asset_id or fingerprint(original) != asset_id:
        raise ValueError("Imported dataset manifest hash changed")
    return record


def assets(bundle, mode, right):
    needed = {"trade"} if mode["trade_type"] == "SPOT" else {"trade", "mark", "funding"}
    if not needed <= bundle["assets"].keys() or bundle["assets"].keys() - (needed | {"index"}):
        raise ValueError("Supply every required trade, mark and funding dataset separately")
    records = {}
    for kind, ids in bundle["assets"].items():
        if not ids or len(ids) != len(set(ids)):
            raise ValueError("Each kind needs unique imported chunks")
        records[kind] = sorted([imported(value) for value in ids], key=lambda row: (row["first"], row["id"]))
        for row in records[kind]:
            descriptor = row["descriptor"]
            for key in ("venue", "symbol", "trade_type", "quote"):
                if descriptor[key] != mode[key]:
                    raise ValueError("Dataset venue, instrument and currency must match the selected reference exactly")
            if (descriptor["kind"] != kind or utc(descriptor["end_exclusive"]) > right
                    or (kind != "funding" and descriptor["interval"] != ("1hour" if mode["trade_type"] == "SPOT" else "1min"))):
                raise ValueError("Dataset kind, resolution or pre-holdout scope mismatch")
    return records


def coverage_gates(records, folds):
    streams = [rows for kind, rows in records.items() if kind in ("trade", "mark")]
    left = max(min(utc(row["first"]) for row in rows) for rows in streams)
    step = pd.Timedelta("1min" if "mark" in records else "1h")
    right = min(max(utc(row["last"]) + step for row in rows) for rows in streams)
    return {fold["id"]: {"train_price_interval_present": left < utc(fold["inner_start"]),
                         "inner_interval_bounded": left <= utc(fold["inner_start"]) and right >= utc(fold["outer_start"]),
                         "outer_interval_bounded": left <= utc(fold["outer_start"]) and right >= utc(fold["outer_end"]),
                         "aligned_source_envelope_start": left.isoformat(), "aligned_source_envelope_end": right.isoformat()}
            for fold in folds}


def design_for(profile_name, bundle):
    cfg, parent = reference(), parent_design()
    baseline = parent["design"]
    mode = cfg["profiles"][profile_name]
    right = utc(baseline["holdout"]["start"])
    records = assets(bundle, mode, right)
    for rows in records.values():
        if sum(row["rows"] for row in rows) > baseline["settings"]["budget"]["max_native_rows"]:
            raise ValueError("Joined dataset exceeds the frozen learning memory/row budget")
    if bundle.get("rules_basis") != "OBSERVED_CURRENT_AS_HYPOTHETICAL":
        raise ValueError("This version requires an explicit hypothetical current-rule scenario")
    rules = metadata(run_folder(bundle["metadata_plan"]))
    instrument = current_rules(mode["trade_type"], rules, cfg)
    if not rules.get("as_of"):
        raise ValueError("No current rule snapshot")
    if mode["trade_type"] == "FUTURES":
        calendar = bundle.get("funding_calendar", {})
        if not {"anchor", "minutes", "basis"} <= calendar.keys() or not calendar["basis"]:
            raise ValueError("Declare and cite the dated funding settlement schedule")
        for row in records["trade"]:
            descriptor = row["descriptor"]
            if descriptor["volume_unit"] == "contracts" and float(descriptor["base_per_contract"]) != instrument.base_per_contract:
                raise ValueError("Trade archive and frozen rule multipliers conflict")
    else:
        calendar = None
    settings = deepcopy(baseline["settings"])
    fee = cfg["fees"][mode["fee"]]
    settings["costs"][mode["fee"]] = {"taker_bps": fee["taker"] * 10000, "adverse_bps": fee["adverse_proxy"] * 10000}
    profile_id = mode["parent"]
    instrument_id = "kucoin_btc_usdt_spot_current_v1" if mode["trade_type"] == "SPOT" else "kucoin_xbtusdtm_current_v1"
    settings["profiles"][profile_id].update(instrument=instrument_id, primary_cost=mode["fee"])
    settings["instruments"][instrument_id] = {**instrument.__dict__, "source": f"{mode['venue']}:{mode['symbol']}",
                                              "rule_snapshot_plan": rules.get("plan")}
    trials = [{**item, "cost": mode["fee"]} for item in baseline["trials"]
              if item["profile"] == profile_id and item["role"] != "cost_diagnostic"]
    profile_design = {key: baseline[key] for key in ("feature_version", "label_version", "folds", "seed", "holdout",
                                                    "available_at_cutoff_exclusive")}
    profile_design.update(settings=settings, trials=trials, data={profile_id: records},
                          code_sha256={"phase9": code_id(), "parent": baseline["code_sha256"]})
    body = {"profile": profile_name, "design": profile_design, "reference": freeze_reference(cfg), "parent": parent["id"],
            "cost_evidence": {"id": mode["fee"], **fee, **cfg["fee_evidence"]}, "contract": instrument.__dict__,
            "rules_basis": bundle["rules_basis"], "rule_snapshot": rules["sources"], "funding_calendar": calendar,
            "minimum_spot_funds": float(rules["spot"]["minFunds"]) if mode["trade_type"] == "SPOT" else None,
            "scope": "HYPOTHETICAL_REPLAY", "coverage": coverage_gates(records, baseline["folds"])}
    # No other profile's dataset participates in this identity.
    return {"id": fingerprint(body), "body": body}


def load_rows(records, key):
    frames = []
    for row in records:
        path = OUTPUT / "imports" / row["id"] / "normalized.parquet"
        if file_hash(path) != row["normalized_sha256"]:
            raise ValueError("Normalized asset checksum mismatch")
        frames.append(pd.read_parquet(path))
    return checked_duplicates(pd.concat(frames, ignore_index=True), key).set_index(key)


def input_market(body):
    design = body["design"]
    profile_id, = design["data"]
    records = design["data"][profile_id]
    trade = load_rows(records["trade"], "open_at")
    mark = load_rows(records["mark"], "open_at") if "mark" in records else trade
    step = pd.Timedelta(seconds=design["settings"]["profiles"][profile_id]["execution_seconds"])
    rates = {}
    if "funding" in records:
        funding = load_rows(records["funding"], "settlement_at")
        calendar = body["funding_calendar"]
        interval = pd.Timedelta(minutes=int(calendar["minutes"]))
        if interval < step or interval % step:
            raise ValueError("Unsupported historical funding interval")
        first, last = trade.index[0], trade.index[-1]
        offset = (first - utc(calendar["anchor"])) % interval
        expected = pd.date_range(first if not offset else first + interval - offset, last, freq=interval)
        actual = funding.loc[first:last, "rate"]
        if not expected.equals(actual.index):
            raise ValueError("Required funding settlement evidence is missing or off-grid")
        rates = actual.to_dict()
    return profile_id, Market(trade, mark, rates, step, "HYPOTHETICAL_REPLAY")


def latest_trials(profile, current_id):
    root = OUTPUT / "campaigns"
    rows = sorted(root.glob("*/trials/*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    prior = {}
    for path in rows:
        if path.parent.parent.name == current_id:
            continue
        record = read_json(path)
        if record["profile"] == profile and record.get("fit_count"):
            prior.setdefault(record["trial"]["id"], record)
    return prior


def fit_worker(job_id):
    folder = run_folder_for_study(job_id)
    manifest = read_json(folder / "manifest.json")
    if manifest["id"] != job_id or fingerprint(manifest["body"]) != job_id:
        raise ValueError("Research manifest checksum mismatch")
    body, fits = manifest["body"], 0
    design = body["design"]
    if design["code_sha256"]["phase9"] != code_id():
        raise ValueError("Research code changed after freeze")
    profile, market = input_market(body)
    budget = design["settings"]["budget"]
    profiles = design["settings"]["profiles"]
    prior = latest_trials(body["profile"], job_id)
    batches, outcomes = {}, []
    with writer_lock(LEARNING_STATE / ".fit.lock"), threadpool_limits(limits=budget["max_threads"]):
        data = prepare(design, profile, market, body["contract"])
        for trial in design["trials"]:
            trial_key = fingerprint(trial["id"])
            path = folder / "trials" / (trial_key + ".json")
            if path.exists():
                row = read_json(path)
                outcomes.append(row)
                fits += row.get("fit_count", 0)
                continue

            def stage(phase, details=None):
                if not controls()["training"] or utc_now() >= utc(manifest["deadline"]):
                    raise RuntimeError("Research disabled or elapsed budget reached")
                if details and trial["model"] != "mean_control_v1":
                    event = {"at": utc_now().isoformat(), "trial": trial["id"], "stage": phase, **details}
                    write_json(folder / "fits" / f"{fingerprint(event)}.json", event)

            stage("EVALUATING")
            row = {"profile": body["profile"], "trial": trial, "at": utc_now().isoformat(), "status": "COMPLETED"}
            attempt_dir = folder / "attempts" / trial_key
            attempt = len(list(attempt_dir.glob("*.json"))) + 1
            if attempt > budget["max_attempts_per_trial"]:
                row.update(status="FAILED", reason="Frozen attempt ceiling")
                write_json(path, row)
                outcomes.append(row)
                continue
            write_json(attempt_dir / f"{attempt:02}.json", {"trial": trial, "at": row["at"], "attempt": attempt})
            if trial["model"] in ("cash", "passive_25pct"):
                row["metrics"] = benchmarks(design, data, trial["cost"], trial["model"])
                row["status"] = row["metrics"].get("status", row["status"])
            else:
                key = trial["entry"], trial["exit_policy"]
                if key not in batches:
                    batches[key] = label_batch(design, data, trial)
                    write_json(folder / "labels" / f"{fingerprint(key)}.json", {"records": batches[key][3], "trial": trial})
                batch = batches[key]
                row["labels"] = batch[4]
                old = prior.get(trial["id"])
                policy = profiles[profile]
                if old and old["trial"]["cost"] == trial["cost"] and (
                            (utc_now() - utc(old["at"])) < pd.Timedelta(days=policy["interval_days"]) or
                            len(set(batch[4]["label_outcome_ids"]) - set(old["labels"]["label_outcome_ids"]))
                            < policy["minimum_new_mature"]):
                    row["status"] = "WAITING_FOR_DATA"
                else:
                    predictions, folds = forecasts(design, data, trial, batch, folder / "models", stage)
                    row["folds"] = folds
                    row["fit_count"] = sum(item.get("fit_count", 0) for item in folds)
                    fits += row["fit_count"]
                    if not predictions or any(item["status"] not in ("EVALUATED", "RULES") for item in folds):
                        row["status"] = "INSUFFICIENT_EVIDENCE"
                    if predictions:
                        versions = {item["fold"]: item["model_artifacts"]["outer"]["version"]
                                    for item in folds if item.get("model_artifacts")}
                        row["metrics"], orders = account_replay(design, data, trial, batch, predictions, versions)
                        if not row["metrics"]["full_window_observed"]:
                            row["status"] = "INSUFFICIENT_EVIDENCE"
                        write_json(folder / "orders" / path.name, {"records": orders, "trial": trial})
            write_json(path, row)
            outcomes.append(row)
    forecast_windows = sorted({fold["fold"] for row in outcomes if row.get("fit_count")
                               for fold in row.get("folds", []) if fold["status"] == "EVALUATED"})
    rule_windows = sorted({name for row in outcomes if row["trial"]["model"] == "rules_v1"
                           for name in row.get("metrics", {}).get("completed_folds", {})})
    write_json(folder / "result.json", {"campaign": job_id, "scope": body["scope"], "status": "COMPLETE",
                                       "actual_fits": fits, "forecast_evaluation_windows": forecast_windows,
                                       "rule_evaluation_windows": rule_windows,
                                       "trials": dict(Counter(row["status"] for row in outcomes))})


def run_folder_for_study(job_id):
    if len(job_id) != 64 or any(char not in "0123456789abcdef" for char in job_id):
        raise ValueError("Invalid study ID")
    return OUTPUT / "campaigns" / job_id


def disk_available():
    try:
        used = sum(path.stat().st_size for path in OUTPUT.rglob("*") if path.is_file())
    except FileNotFoundError:
        # A child can atomically publish a temporary file during this scan.
        return True
    return used < reference()["acquisition"]["maximum_local_bytes"]


def reused_result(job_id):
    direct = run_folder_for_study(job_id) / "result.json"
    if direct.exists():
        return read_json(direct)
    alias = OUTPUT / "campaign-aliases" / f"{job_id}.json"
    if not alias.exists():
        return None
    record = read_json(alias)
    original = run_folder_for_study(record["original_campaign"])
    result = original / "result.json"
    erratum = original / "lineage-erratum.json"
    if (record["reconciled_design_id"] != job_id or file_hash(result) != record["result_sha256"]
            or file_hash(erratum) != record["erratum_sha256"]):
        raise ValueError("Saved campaign lineage alias failed verification")
    return {**read_json(result), "reused": True, "lineage_erratum": str(erratum)}


def run_bundle(profile, bundle_path):
    manifest = design_for(profile, read_import_json(bundle_path))
    folder = run_folder_for_study(manifest["id"])
    if previous := reused_result(manifest["id"]):
        return previous
    gates = manifest["body"]["coverage"]
    eligible = [key for key, row in gates.items() if all(row[field] for field in
                ("train_price_interval_present", "inner_interval_bounded", "outer_interval_bounded"))]
    if not eligible:
        return {"status": "BLOCKED_DATA", "actual_fits": 0, "coverage": gates,
                "reason": "None of the frozen folds has the required train, inner and outer data envelope"}
    if not controls()["training"]:
        return {"status": "TRAINING_OFF", "actual_fits": 0, "eligible_data_envelopes": eligible}
    budget = manifest["body"]["design"]["settings"]["budget"]
    with writer_lock(LEARNING_STATE / ".supervisor.lock"), writer_lock(OUTPUT / ".research.lock"):
        if learning_state.status()["mode"] != "off":
            raise ValueError("Stop the existing learner before a KuCoin campaign")
        result = folder / "result.json"
        if result.exists():
            return read_json(result)
        reason = admission(budget) if disk_available() else "Phase 9 storage ceiling reached"
        if reason:
            return {"status": "WAITING_FOR_RESOURCES", "reason": reason, "actual_fits": 0}
        if (folder / "manifest.json").exists():
            manifest = read_json(folder / "manifest.json")
        else:
            manifest["deadline"] = (utc_now() + pd.Timedelta(seconds=budget["wall_seconds"])).isoformat()
            write_json(folder / "manifest.json", manifest)
        if utc_now() >= utc(manifest["deadline"]):
            return {"status": "INCOMPLETE", "reason": "Frozen campaign deadline expired"}
        reason = None
        with (folder / "training.log").open("ab") as log:
            child = subprocess.Popen([sys.executable, "-m", "lab.phase9", "_study", manifest["id"]], cwd=ROOT,
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                     creationflags=subprocess.BELOW_NORMAL_PRIORITY_CLASS | subprocess.CREATE_NO_WINDOW)
            root = psutil.Process(child.pid)
            while child.poll() is None:
                try:
                    rss = sum(process.memory_info().rss for process in [root, *root.children(recursive=True)])
                except psutil.NoSuchProcess:
                    rss = 0
                expired = utc_now() >= utc(manifest["deadline"])
                reason = ("Disabled by operator" if not controls()["training"] else "Deadline reached" if expired
                          else "Memory ceiling reached" if rss > budget["child_rss_mib"] * MIB
                          else "Phase 9 storage ceiling reached" if not disk_available() else admission(budget))
                if reason:
                    cancel_tree(root)
                    break
                time.sleep(2)
        if result.exists():
            return read_json(result)
        record = {"campaign": manifest["id"], "status": "INCOMPLETE", "reason": reason or "Learning child failed",
                  "fit_attempts": len(list((folder / "fits").glob("*.json"))), "deadline": manifest["deadline"]}
        write_json(folder / "interruptions" / f"{fingerprint([record, utc_now().isoformat()])}.json", record)
        return record
