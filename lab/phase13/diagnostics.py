from collections import Counter
from contextlib import suppress
from dataclasses import fields, replace
from functools import partial
from pathlib import Path
import subprocess
import sys
import time

import numpy as np
import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.config import ROOT as PROJECT, fingerprint
from lab.learning.control import admission, cancel_tree
from lab.learning.design import MIB, STATE
from lab.learning.evaluation import candidates
from lab.learning.simulator import Cost, ExecutionAssumptions, Setup
from lab.phase10.evaluation import account_replay
from lab.phase10.trading import run_trade
from lab.phase11.archive import size_under
from lab.phase11.campaign import gates_off, load_market, start_clock
from lab.phase12.study import EquityCurve, profile_data, release_buffers, write_orders
from lab.phase13.economics import BREAKDOWN_VERSION, compare
from lab.phase13.inference import decision, fitted
from lab.phase13.registry import ROOT, checked, freeze, load_parent, orders, sealed
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


def folder(identifier):
    if len(identifier) != 64 or any(c not in "0123456789abcdef" for c in identifier):
        raise ValueError("Invalid diagnostic identity")
    return ROOT / "diagnostics" / identifier


def code_digest():
    files = [PROJECT / "lab/phase13" / f"{stem}.py" for stem in ("diagnostics", "registry", "economics", "inference")]
    files.extend(PROJECT / name for name in ("lab/phase10/evaluation.py", "lab/phase10/trading.py",
                                            "lab/phase10/partition.py", "lab/phase12/study.py"))
    return fingerprint({str(path.relative_to(PROJECT)): file_hash(path) for path in files})


def plan():
    reg = freeze()
    body = {"registry": reg["id"], "parent": reg["body"]["parent_campaign"],
            "parent_result_sha256": reg["body"]["parent_result_sha256"],
            "candidates": {key: row["id"] for key, row in reg["body"]["candidates"].items()},
            "policy": reg["body"]["policy"], "method": BREAKDOWN_VERSION,
            "chronology": "Continuous account in chronological outer folds. Fixed saved scores. "
                "Delayed IOC moves one native minute; recheck the existing session cutoff. "
                "Delay non-liquidation exits by one native minute and keep the original holding deadline.",
            "decomposition": "Actual price P&L at modeled fills. Reconstruct raw-price difference only on those same "
                "quantities and fills. Adverse attribution includes tick rounding; entry selection interactions are not a counterfactual.",
            "code": code_digest()}
    identity = fingerprint(body)
    path = folder(identity) / "manifest.json"
    with writer_lock(ROOT / ".diagnostic-registry.lock"):
        if not path.exists():
            write_json(path, {"id": identity, "body": body, "frozen_at": utc_now().isoformat()})
        write_json(ROOT / "diagnostic.json", {"id": identity})
    return sealed(path, identity)


def saved_inputs(item, ledger, design, data):
    names = [field.name for field in fields(Setup)]
    clocks = {"decision_at", "available_at", "submit_at", "expires_at", "deadline"}
    setups, groups = {}, {fold["id"]: {} for fold in design["folds"]}
    for row in ledger:
        stamp = utc(row["decision_at"])
        if row["candidate_id"] != item["trial"]["id"] or stamp in setups:
            raise ValueError("Unexpected saved Phase 12 order identity")
        setups[stamp] = Setup(**{name: utc(row[name]) if name in clocks else row[name] for name in names})
        groups[row["fold"]][stamp] = row["score_net_r"]
    strategy = item["trial"]["entry"]
    expected, _ = candidates(data["market"], data["profile"], strategy, item["strategy"], item["trial"]["exit_policy"],
                             item["exit_parameters"], data["features"], data["rules"])
    for fold in design["folds"]:
        a, b = utc(fold["outer_start"]), utc(fold["outer_end"])
        if set(groups[fold["id"]]) != {stamp for stamp in expected if a <= stamp < b}:
            raise ValueError("The saved order ledger does not cover exactly the frozen setup sequence")
    if any(expected[stamp] != setup for stamp, setup in setups.items()):
        raise ValueError("Phase 12 setup cannot be reproduced with the frozen feature path")
    return setups, {key: pd.Series(group, dtype=float).sort_index() for key, group in groups.items()}


def parity(item, ledger, data, scores):
    proof = {"candidate": item["id"], "historical_predictions_checked": 0, "maximum_absolute_delta_R": 0.,
             "structured_examples": [], "no_estimator_fits": True}
    for fold, series in scores.items():
        if not len(series):
            continue
        model = fitted(item, fold) if item["role"] == "estimator" else None
        if model:
            predictions = model[1].predict(data["features"].loc[series.index, item["feature_columns"]])
        else:
            mean = item["historical_walk_forward"][fold]["outer_mean_net_r"] if item["role"] == "mean" else 1.
            predictions = np.full(len(series), mean)
        delta = float(np.max(np.abs(predictions - series.to_numpy())))
        if delta > 1e-10 or not np.array_equal(predictions > item["threshold_net_r"], series.to_numpy() > item["threshold_net_r"]):
            raise ValueError(f"Saved Phase 12 prediction or threshold parity failed in {fold}: {delta}")
        proof["maximum_absolute_delta_R"] = max(proof["maximum_absolute_delta_R"], delta)
        proof["historical_predictions_checked"] += len(series)
        samples = {}
        for row in ledger:
            if row["fold"] == fold:
                samples.setdefault(row["score_net_r"] > item["threshold_net_r"], row)
        for positive, sample in samples.items():
            cut = utc(sample["decision_at"])
            example = decision(item, data["features"], cut, utc(sample["available_at"]),
                               {"scope": "CONSUMED_PHASE12_MODELED_AVAILABILITY", "coverage_complete": True,
                                "latest_required_source_close": cut.isoformat(), "latest_required_receipt": cut.isoformat()},
                               fold=fold, equity=sample.get("sizing_equity"), model=model)
            if example["gates"].get("threshold") != positive:
                raise ValueError(f"Structured research gate did not reproduce the saved forecast in {fold}")
            proof["structured_examples"].append(example)
    return proof


def stress_case(item, design, data, setups, scores, stress):
    configured = Cost(item["cost"]["taker"], item["cost"]["adverse_proxy"])
    omissions = 0
    if stress == "double_cost":
        cost = Cost(configured.fee*2, configured.adverse*2)
        executor = run_trade
    else:
        cost = configured
        step = data["market"].step
        session = data["profile"].get("entry_cutoff_minute_utc")
        delayed = {}
        for stamp, setup in setups.items():
            submit = setup.submit_at + step
            if session is not None and submit >= stamp.normalize() + pd.Timedelta(minutes=session):
                omissions += 1
                continue
            if submit >= setup.deadline:
                omissions += 1
                continue
            delayed[stamp] = replace(setup, submit_at=submit, expires_at=setup.expires_at+step)
        setups = delayed
        scores = {key: series.loc[series.index.isin(delayed)] for key, series in scores.items()}
        executor = partial(run_trade, assumptions=ExecutionAssumptions(delay_exit_bars=1))
    versions = {key: row["outer_artifact"]["version"] for key, row in item["historical_walk_forward"].items()
                if row["outer_artifact"]}
    metrics, ledger = account_replay(design, data, item["trial"], (setups, cost, None, [], {}), scores, versions,
                                    curve=EquityCurve(design, data["market"].step), executor=executor)
    subrecords = {fold["id"]: {"net_return": metrics["completed_folds"].get(fold["id"]),
                              "state": "COMPLETE" if fold["id"] in metrics["completed_folds"] else "UNRESOLVED_OR_INCOMPLETE",
                              "setups": sum(row["fold"] == fold["id"] for row in ledger),
                              "closed_trades": sum(row["fold"] == fold["id"] and row["status"] in ("COMPLETE", "AMBIGUOUS")
                                                   for row in ledger)}
                  for fold in design["folds"]}
    return {"metrics": metrics, "folds": subrecords, "delayed_session_or_deadline_exclusions": omissions}, ledger


def progress(path):
    runs = [read_json(p) for p in path.glob("lanes/*/stress-*.json")]
    return {"reused_saved_accounts": len(list(path.glob("lanes/*/comparison.json"))),
            "new_stress_replays": sum(row["state"] == "DONE" for row in runs),
            "reserved_replays": len(list(path.glob("lanes/*/reserved-*.json"))),
            "fold_subrecords": sum(len(row.get("folds", {})) for row in runs),
            "completed_fold_subrecords": sum(fold["state"] == "COMPLETE" for row in runs for fold in row.get("folds", {}).values()),
            "historical_estimator_fits": 0, "reused_forecast_proofs": len(list(path.glob("lanes/*/parity.json")))}


def work(identifier):
    root = folder(identifier)
    manifest = sealed(root / "manifest.json", identifier)
    if manifest["body"]["code"] != code_digest():
        raise ValueError("Diagnostic code fingerprint changed before resume")
    registry = freeze()
    if registry["id"] != manifest["body"]["registry"]:
        raise ValueError("Registry changed")
    parent, result, execution, view, resources = load_parent()
    checked(PROJECT / "runs/phase12/campaigns" / result["campaign"] / "result.json", manifest["body"]["parent_result_sha256"])
    design = parent["body"]["design"]
    budget = manifest["body"]["policy"]["budget"]
    deadline = utc(read_json(root / "deadline.json")["deadline"])
    with writer_lock(STATE / ".fit.lock"), threadpool_limits(limits=budget["max_threads"]):
        market = load_market(view, resources, design)
        release_buffers()
        for profile in ("day", "swing"):
            probe = registry["body"]["candidates"][f"{profile}/estimator"]
            data = profile_data(market, design["settings"], probe["trial"]["profile"], probe["contract"])
            for role in ("estimator", "unfiltered", "mean", "cash"):
                if utc_now() >= deadline or not gates_off():
                    raise RuntimeError("Frozen deadline or independent gate reached")
                item = registry["body"]["candidates"][f"{profile}/{role}"]
                lane = root / "lanes" / f"{profile}-{role}"
                result = read_json(checked(item["result"], item["result_sha256"]))
                ledger = list(orders(checked(item["orders"], item["orders_sha256"]))) if item["orders"] else []
                if not (lane / "comparison.json").exists():
                    write_json(lane / "comparison.json", compare(item, result, ledger, market, design))
                    print(f"Reconstructed saved {profile}/{role}", flush=True)
                if role == "cash":
                    continue
                setups, scores = saved_inputs(item, ledger, design, data)
                if not (lane / "parity.json").exists():
                    write_json(lane / "parity.json", parity(item, ledger, data, scores))
                del ledger
                for stress in manifest["body"]["policy"]["stresses"]:
                    receipt = lane / f"stress-{stress}.json"
                    ticket = lane / f"reserved-{stress}.json"
                    if receipt.exists():
                        continue
                    if ticket.exists():
                        write_json(receipt, {"state": "INTERRUPTED_RESERVATION_CONSUMED", "stress": stress})
                        continue
                    if utc_now() >= deadline or not gates_off():
                        raise RuntimeError("Frozen deadline or independent gate reached")
                    if progress(root)["reserved_replays"] >= manifest["body"]["policy"]["maximum_additional_continuous_replays"]:
                        raise ValueError("Frozen replay count reached")
                    write_json(ticket, {"at": utc_now().isoformat(), "candidate": item["id"], "stress": stress})
                    detail, evidence = stress_case(item, design, data, setups, scores, stress)
                    write_orders(lane / f"orders-{stress}.jsonl.gz", evidence)
                    write_json(receipt, {"state": "DONE", "stress": stress, **detail})
                    del detail, evidence
                    release_buffers()
                    print(f"Finished {profile}/{role} {stress}", flush=True)
            del data
            release_buffers()
    counts = progress(root)
    write_json(root / "result.json", {"diagnostic": identifier, "at": utc_now().isoformat(),
               "state": "COMPLETE" if counts["new_stress_replays"] == 12 else "PARTIAL",
               "scope": "DESCRIPTIVE_CONSUMED_DEVELOPMENT", **counts})


def supervise(manifest):
    identifier = manifest["id"]
    root = folder(identifier)
    result = root / "result.json"
    if result.exists():
        return {**read_json(result), "reused": True}
    budget = manifest["body"]["policy"]["budget"]
    with writer_lock(STATE / ".supervisor.lock"), writer_lock(ROOT / ".diagnostic-run.lock"):
        if result.exists():
            return {**read_json(result), "reused": True}
        reason = admission(budget)
        if not gates_off() or reason:
            return {"state": "WAITING_FOR_ADMISSION", "diagnostic": identifier,
                    "reason": reason or "Continuous training gates must be off", **progress(root)}
        clock = start_clock(root, budget["wall_seconds"])
        deadline = utc(clock["deadline"])
        if utc_now() >= deadline:
            return {"state": "PARTIAL", "diagnostic": identifier, "reason": "Original deadline expired", **clock, **progress(root)}
        peak = 0
        with (root / "research.log").open("ab") as log:
            child = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase13", "_diagnose", identifier],
                                     cwd=PROJECT, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                     creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
            process = psutil.Process(child.pid)
            write_json(root / "active.json", {"pid": process.pid, "created": process.create_time()})
            try:
                while child.poll() is None:
                    with suppress(psutil.NoSuchProcess):
                        peak = max(peak, sum(item.memory_info().rss for item in [process, *process.children(recursive=True)]))
                    reason = ("Child RSS" if peak > budget["child_rss_mib"] * MIB else "Output ceiling"
                              if size_under(root) >= budget["output_cap_mib"] * MIB else "Wall deadline"
                              if utc_now() >= deadline else "Continuous training gate opened" if not gates_off() else admission(budget))
                    if reason:
                        cancel_tree(process)
                        break
                    time.sleep(2)
            finally:
                (root / "active.json").unlink(missing_ok=True)
                if child.poll() is None:
                    cancel_tree(process)
        snapshot = {"at": utc_now().isoformat(), "peak_rss_bytes": peak, "output_bytes": size_under(root), **clock,
                    "stop_reason": reason if not result.exists() else None}
        write_json(root / "observations" / f"{fingerprint(snapshot)}.json", snapshot)
        return ({**read_json(result), "resources": snapshot} if result.exists() else
                {"state": "PARTIAL", "diagnostic": identifier, "reason": reason or f"Child exit {child.poll()}",
                 **snapshot, **progress(root)})
