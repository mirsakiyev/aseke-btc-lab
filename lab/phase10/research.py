from collections import Counter
from copy import deepcopy
from pathlib import Path
import subprocess
import sys
import time

import pandas as pd
import psutil
import pyarrow.parquet as pq
from threadpoolctl import threadpool_limits

from lab.config import ROOT, fingerprint
from lab.execution import execution_open
from lab.learning.control import admission, cancel_tree
from lab.learning.design import MIB, STATE as LEARNING_STATE
from lab.learning.evaluation import benchmarks, forecasts
from lab.learning.simulator import Contract, Risk
from lab.learning.state import status as legacy_status
from lab.phase9.reference import parent_design, reference
from lab.phase10.evaluation import account_replay, label_batch
from lab.phase10.partition import Chunk, PartitionedMarket, checked_funding, partition_features
from lab.phase10.worker import OUTPUT, STATE, WAITING, controls, optional
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


JOBS = OUTPUT / "futures-jobs"
BUDGET = 256 * MIB


def code_hash():
    return fingerprint({file.name: file_hash(file) for file in sorted(Path(__file__).parent.glob("*.py"))})


def queue_path(job_id):
    if len(job_id) != 64 or any(char not in "0123456789abcdef" for char in job_id):
        raise ValueError("Invalid research job identity")
    return JOBS / job_id


def plans():
    return [read_json(path) for path in sorted(JOBS.glob("*/manifest.json"))]


def active_job(folder):
    row = optional(folder / "active.json", {})
    if row:
        try:
            process = psutil.Process(row["pid"])
            if process.create_time() == row["created"]:
                return row
        except (OSError, psutil.Error):
            pass
    return None


def work_state():
    jobs, running, waiting = plans(), None, []
    flags = controls()["training"]
    for job in jobs:
        folder = queue_path(job["id"])
        profile = job["body"]["profile"]
        if active := active_job(folder):
            running = {"id": job["id"], "profile": profile, **active}
        elif not (folder / "result.json").exists():
            failed = optional(folder / "failed.json", {})
            retry = optional(folder / "retry.json", {})
            waiting.append({"id": job["id"], "profile": profile, "retry_at": retry.get("at"),
                            "reason": failed.get("reason") or ("Training OFF" if not flags[profile]
                                                               else retry.get("reason", "Waiting for worker admission"))})
    results = [read_json(path) for path in JOBS.glob("*/result.json")]
    eligible = sorted({fold for row in jobs for fold in row["body"]["eligible_folds"]})
    attempts = len(list(JOBS.glob("*/trials/*/attempt-*.json")))
    return {"current_job": running, "last_success": max((item["at"] for item in results), default=None),
            "state": "RUNNING" if running else "WAITING_FOR_ADMISSION" if waiting else "COMPLETE" if results else WAITING,
            "eligible_folds": eligible, "jobs": len(jobs), "candidate_attempts": attempts,
            "waiting_jobs": waiting, "fit_attempts": len(list(JOBS.glob("*/fits/*.json"))),
            "actual_fits": sum(read_json(path)["fit_count"] for path in JOBS.glob("*/trials/*/result.json"))}


def checked_schedule(bundle, index):
    funding = bundle["funding"]
    path = Path(funding["path"])
    if file_hash(path) != funding["sha256"]:
        raise ValueError("Funding file checksum mismatch")
    if pq.ParquetFile(path).metadata.num_rows > 650_000:
        raise ValueError("Funding file exceeds the native row budget")
    series = pd.read_parquet(path, columns=["settlement_at", "rate"]).set_index("settlement_at").rate
    start, end = index[0], index[-1] + pd.Timedelta("1min")
    expected, prior = [], start
    for regime in funding["schedule"]:
        left, right, anchor = (utc(regime[key]) for key in ("start", "end_exclusive", "anchor"))
        minutes = regime["minutes"]
        if (left != prior or right <= left or right > end or type(minutes) is not int or minutes < 1
                or not regime.get("evidence")):
            raise ValueError("Funding regimes must form a complete ordered schedule with provenance")
        delta = pd.Timedelta(minutes=minutes)
        offset = (left - anchor) % delta
        expected.extend(pd.date_range(left if not offset else left + delta - offset, right, freq=delta, inclusive="left"))
        prior = right
    if prior != end:
        raise ValueError("Funding schedule ends before paired market coverage")
    return checked_funding(series, pd.DatetimeIndex(expected, tz="UTC"), funding["tolerance"], start, end)


def market_from(body):
    bundle = body["bundle"]
    budget = body["design"]["settings"]["budget"]
    chunks = [Chunk(Path(row["trade"]), Path(row["mark"]), row["trade_sha256"], row["mark_sha256"])
              for row in bundle["chunks"]]
    right = body["design"]["holdout"]["start"]
    # Inspect paired index/row bounds before loading the independent funding series.
    market = PartitionedMarket(chunks, {}, right, budget["max_native_rows"])
    market.set_funding(checked_schedule(bundle, market.index))
    return market


def qualify(profile, path):
    if profile not in ("swing", "day"):
        raise ValueError("Phase 10 accepts futures profiles only")
    cfg, parent = reference(), parent_design()
    base = parent["design"]
    mode = cfg["profiles"][profile]
    bundle = read_json(Path(path))
    source = bundle["source"]
    if (not all(source.get(key) for key in ("venue", "symbol", "terms", "evidence"))
            or source.get("settlement") != "USDT" or source.get("scope") != "RESEARCH_ONLY"):
        raise ValueError("Declare the actual venue, instrument, permission and USDT settlement for offline research")
    if bundle["rules_basis"] != "HYPOTHETICAL_SINGLE_TIER" or not bundle["rules_evidence"]:
        raise ValueError("Declare the source and limits of the hypothetical rule assumptions")
    instrument = Contract.load(bundle["contract"])
    if instrument.family != "linear_perp" or instrument.quote != "USDT":
        raise ValueError("Expected a separately declared linear USDT contract")
    bundle = deepcopy(bundle)
    for chunk in bundle["chunks"]:
        for basis in ("trade", "mark"):
            chunk[basis] = str((Path(path).resolve().parent / chunk[basis]).resolve())
    bundle["funding"]["path"] = str((Path(path).resolve().parent / bundle["funding"]["path"]).resolve())
    design = deepcopy(base)
    profile_id = mode["parent"]
    fee = cfg["fees"][mode["fee"]]
    design["settings"]["costs"][mode["fee"]] = {"taker_bps": fee["taker"] * 10000,
                                               "adverse_bps": fee["adverse_proxy"] * 10000}
    design["trials"] = [{**item, "cost": mode["fee"]} for item in base["trials"]
                        if item["profile"] == profile_id and item["role"] != "cost_diagnostic"]
    design.pop("data")
    body = {"profile": profile, "profile_id": profile_id, "bundle": bundle, "design": design, "parent": parent["id"],
            "scope": "HYPOTHETICAL_REPLAY", "use": "RESEARCH_ONLY", "code": code_hash(),
            "cost_evidence": {"id": mode["fee"], **fee, **cfg["fee_evidence"]}}
    market = market_from(body)
    checks = market.coverage()
    observed = market.index
    eligible = []
    for fold in design["folds"]:
        left, end = (utc(fold[key]) for key in ("inner_start", "outer_end"))
        wanted = pd.date_range(left, end, freq=market.step, inclusive="left")
        if observed[0] < left and not len(wanted.difference(observed)) and checks["missing_mark"] == 0:
            eligible.append(fold["id"])
    body.update(eligible_folds=eligible, coverage=checks)
    if not eligible:
        return {"id": fingerprint(body), "state": WAITING, "coverage": checks, "actual_fits": 0,
                "reason": "No frozen fold has complete inner/outer observations and a training prefix within budget"}
    design["folds"] = [fold for fold in design["folds"] if fold["id"] in eligible]
    chunks = [{key: chunk[key] for key in ("trade_sha256", "mark_sha256")} for chunk in bundle["chunks"]]
    input_id = fingerprint(["partition-funding-v1", profile, parent["id"], chunks, bundle["funding"]["sha256"],
                            bundle["funding"]["schedule"], bundle["funding"]["tolerance"], source,
                            bundle["contract"], body["cost_evidence"]])
    body["input_id"] = input_id
    identifier = fingerprint(body)
    folder = queue_path(identifier)
    with writer_lock(STATE / ".queue.lock"):
        existing = next((plan for plan in plans() if plan["body"].get("input_id") == input_id), None)
        if existing:
            return {"id": existing["id"], "state": "DEDUPLICATED", "eligible_folds": eligible, "coverage": checks}
        if not (folder / "manifest.json").exists():
            write_json(folder / "manifest.json", {"id": identifier, "body": body, "registered_at": utc_now().isoformat()})
    return {"id": identifier, "state": "QUEUED", "coverage": checks, "eligible_folds": eligible}


def should_run():
    for plan in plans():
        folder = queue_path(plan["id"])
        if (controls()["training"][plan["body"]["profile"]] and not (folder / "result.json").exists()
                and not (folder / "failed.json").exists()):
            if active_job(folder):
                continue
            retry = optional(folder / "retry.json", {})
            if not retry or utc_now() >= utc(retry["at"]):
                return plan["id"]
    return None


def supervise(job_id):
    folder = queue_path(job_id)
    try:
        supervise_locked(job_id, folder)
    except RuntimeError as exc:
        if not str(exc).startswith("Another writer owns"):
            raise
        write_json(folder / "retry.json", {"reason": "Shared research lock occupied",
                                           "at": (utc_now() + pd.Timedelta("10min")).isoformat()})


def supervise_locked(job_id, folder):
    plan = read_json(folder / "manifest.json")
    profile = plan["body"]["profile"]
    budget = plan["body"]["design"]["settings"]["budget"]
    with writer_lock(LEARNING_STATE / ".supervisor.lock"), writer_lock(STATE / ".research.lock"):
        flags = controls()
        if flags["paused"] or not flags["training"][profile] or (folder / "result.json").exists():
            return
        reason = "Legacy learner owns admission" if legacy_status()["mode"] != "off" else admission(budget)
        if reason:
            write_json(folder / "retry.json", {"reason": reason, "at": (utc_now() + pd.Timedelta("10min")).isoformat()})
            return
        if not (folder / "deadline.json").exists():
            write_json(folder / "deadline.json", {"at": (utc_now()+pd.Timedelta(seconds=budget["wall_seconds"])).isoformat()})
        deadline = utc(read_json(folder / "deadline.json")["at"])
        if utc_now() >= deadline:
            write_json(folder / "failed.json", {"reason": "Frozen wall budget expired"})
            return
        with (folder / "job.log").open("ab") as log:
            child = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase10", "_fit", job_id], cwd=ROOT,
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                     creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
            root = psutil.Process(child.pid)
            write_json(folder / "active.json", {"pid": root.pid, "created": root.create_time()})
            try:
                while child.poll() is None:
                    processes = [root, *root.children(recursive=True)]
                    try:
                        rss = sum(process.memory_info().rss for process in processes)
                    except psutil.NoSuchProcess:
                        rss = 0
                    used = sum(path.stat().st_size for path in JOBS.rglob("*") if path.is_file())
                    flags = controls()
                    reason = ("Training OFF" if flags["paused"] or not flags["training"][profile] else
                              "Wall budget expired" if utc_now() >= deadline else
                              "RSS ceiling reached" if rss > budget["child_rss_mib"] * MIB else
                              "Output ceiling reached" if used > BUDGET else admission(budget))
                    if reason:
                        cancel_tree(root)
                        break
                    time.sleep(2)
            finally:
                (folder / "active.json").unlink(missing_ok=True)
                if child.poll() is None:
                    cancel_tree(root)
            if not (folder / "result.json").exists():
                if reason == "Training OFF":
                    return
                write_json(folder / "failed.json", {"reason": reason or f"Fit worker exited {child.poll()}"})


def fit(job_id):
    folder = queue_path(job_id)
    manifest = read_json(folder / "manifest.json")
    body = manifest["body"]
    if fingerprint(body) != job_id or body["code"] != code_hash():
        raise ValueError("Immutable research inputs or code changed")
    profile = body["profile_id"]
    design = body["design"]
    settings = design["settings"]
    deadline = utc(read_json(folder / "deadline.json")["at"])
    with writer_lock(LEARNING_STATE / ".fit.lock"), threadpool_limits(limits=settings["budget"]["max_threads"]):
        market = market_from(body)
        policy = settings["profiles"][profile]
        features, columns = partition_features(market, policy)
        rules = settings["execution"]
        grace = pd.Timedelta(seconds=rules["processing_seconds"])
        trail = {execution_open(at+grace, market.step): (row.close, row.atr)
                 for at, row in features[features.atr.gt(0)].iterrows()}
        data = {"market": market, "profile": policy, "features": features, "columns": columns, "trail": trail,
                "rules": rules, "contract": Contract.load(body["bundle"]["contract"]),
                "risk": Risk(**settings["risk"][policy["risk"]])}
        outcomes, batches = [], {}
        for trial in design["trials"]:
            key = fingerprint(trial["id"])
            trial_dir = folder / "trials" / key
            result = trial_dir / "result.json"
            if result.exists():
                outcomes.append(read_json(result))
                continue

            def stage(_name, details=None):
                if not controls()["training"][body["profile"]] or utc_now() >= deadline:
                    raise RuntimeError("Training disabled or campaign deadline reached")
                if details and trial["model"] != "mean_control_v1":
                    write_json(folder / "fits" / f'{fingerprint([key, details, attempt])}.json',
                               {"trial": trial["id"], "at": utc_now().isoformat(), **details})

            stage("EVALUATING")
            attempt = 1 + len(list(trial_dir.glob("attempt-*.json")))
            row = {"trial": trial, "status": "COMPLETED", "folds": [], "fit_count": 0}
            if attempt > settings["budget"]["max_attempts_per_trial"]:
                row.update(status="FAILED", reason="Frozen retry ceiling")
            else:
                write_json(trial_dir / f"attempt-{attempt}.json", {"at": utc_now().isoformat(), "trial": trial})
                if trial["model"] in ("cash", "passive_25pct"):
                    row["metrics"] = benchmarks(design, data, trial["cost"], trial["model"])
                    row["status"] = row["metrics"].get("status", "COMPLETED")
                else:
                    lane = trial["entry"], trial["exit_policy"]
                    if lane not in batches:
                        batches[lane] = label_batch(design, data, trial)
                        write_json(folder / "labels" / f"{fingerprint(lane)}.json", batches[lane][3])
                    batch = batches[lane]
                    predictions, folds = forecasts(design, data, trial, batch, folder / "models", stage)
                    row.update(folds=folds, fit_count=sum(item.get("fit_count", 0) for item in folds), labels=batch[4])
                    if predictions:
                        versions = {item["fold"]: item["model_artifacts"]["outer"]["version"]
                                    for item in folds if item.get("model_artifacts")}
                        metrics, orders = account_replay(design, data, trial, batch, predictions, versions)
                        row["metrics"] = metrics
                        write_json(trial_dir / "orders.json", orders)
                    if (not row.get("metrics", {}).get("full_window_observed")
                            or any(item["status"] not in ("EVALUATED", "RULES") for item in folds)):
                        row["status"] = "INSUFFICIENT_EVIDENCE"
            write_json(result, row)
            outcomes.append(row)
    evaluated = {fold["fold"] for row in outcomes for fold in row["folds"] if fold["status"] == "EVALUATED"}
    write_json(folder / "result.json", {"at": utc_now().isoformat(), "scope": body["scope"], "use": body["use"],
               "completed_fits": sum(item["fit_count"] for item in outcomes), "evaluated": sorted(evaluated),
               "outcomes": dict(Counter(item["status"] for item in outcomes))})
