from collections import Counter
from copy import deepcopy
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory
import gzip
import json
import os
import threading
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.execution import execution_open
from lab.learning.design import MIB
from lab.learning.simulator import Contract, Cost, Risk, Setup
from lab.phase9.reference import parent_design, reference
from lab.phase10.evaluation import account_replay, label_batch
from lab.phase10.partition import partition_features
from lab.phase10.research import market_from
from lab.phase10.trading import run_trade
from lab.phase11.plan import PRIOR, ROOT, freeze_plan
from lab.storage import read_json, utc, utc_now, write_json


def profile_data(market, settings, profile_id, instrument):
    policy = settings["profiles"][profile_id]
    features, columns = partition_features(market, policy)
    rules = settings["execution"]
    allowance = pd.Timedelta(seconds=rules["processing_seconds"])
    trail = {execution_open(at + allowance, market.step): (row.close, row.atr)
             for at, row in features[features.atr.gt(0)].iterrows()}
    return {"market": market, "profile": policy, "features": features, "columns": columns, "trail": trail,
            "rules": rules, "contract": Contract.load(instrument),
            "risk": Risk(**settings["risk"][policy["risk"]])}


def reference_design():
    parent = parent_design()
    design, cfg = deepcopy(parent["design"]), reference()
    fee = cfg["profiles"]["day"]["fee"]
    assumptions = cfg["fees"][fee]
    design["settings"]["costs"][fee] = {"taker_bps": assumptions["taker"] * 10000,
                                         "adverse_bps": assumptions["adverse_proxy"] * 10000}
    return design, cfg, fee


def jsonl_bytes(rows):
    return gzip.compress("".join(json.dumps(row, default=str, allow_nan=False, separators=(",", ":")) + "\n"
                                 for row in rows).encode("utf-8"), compresslevel=5, mtime=0)


def heartbeat():
    # Reading durable state does not require accessing another logon session.
    from lab.phase10.worker import RUNTIME
    runtime = read_json(RUNTIME)
    return {"at": utc_now().isoformat(), "heartbeat": runtime["heartbeat"], "pid": runtime["pid"],
            "launcher": runtime["launcher"], "day": runtime["profiles"]["day"]}


def run(*, overlap=False):
    suffix = "-monitor-overlap" if overlap else ""
    out = ROOT / f"benchmark-june-2022{suffix}.json"
    if out.exists():
        return read_json(out)
    plan = freeze_plan()
    sample = PRIOR / "binance-probe" / "june-2022-qualification" / "bundle.json"
    declaration = {"plan": plan["id"], "range": ["2022-06-01T00:00:00Z", "2022-07-01T00:00:00Z"],
                   "selection": "Existing earliest complete normalized Phase 10 sample. Selected by UTC coverage.",
                   "scope": "BENCHMARK_ONLY", "estimator_fits": 0, "threads": 2,
                   "tests": ["all six Day label lanes and unfiltered account replay", "Swing feature aggregation",
                             "live-input chronological funding and cross-partition exposure proxy"],
                   "fixed_price_source": str(sample)}
    declaration["purpose"] = "Concurrent scheduled decision observation" if overlap else "Capacity sizing"
    decl = ROOT / f"benchmark-declaration{suffix}.json"
    if not decl.exists():
        write_json(decl, declaration)
    proc = psutil.Process(os.getpid())
    proc.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    samples = []
    done = threading.Event()

    def sample_memory():
        while not done.wait(.02):
            samples.append(proc.memory_info().rss)

    rss = threading.Thread(target=sample_memory, daemon=True)
    rss.start()
    before = heartbeat()
    started = time.perf_counter()
    timers, sizes, counts = {}, Counter(), Counter()
    child_start = proc.memory_info().rss
    try:
        design, cfg, fee = reference_design()
        settings = design["settings"]
        bundle = read_json(sample)
        for chunk in bundle["chunks"]:
            for basis in ("trade", "mark"):
                chunk[basis] = str(sample.parent / chunk[basis])
                sizes[basis] += Path(chunk[basis]).stat().st_size
        bundle["funding"]["path"] = str(sample.parent / bundle["funding"]["path"])
        sizes["funding"] += Path(bundle["funding"]["path"]).stat().st_size
        cutoff = declaration["range"][1]
        design["available_at_cutoff_exclusive"] = cutoff
        design["folds"] = [{"id": "benchmark_june_2022", "outer_start": declaration["range"][0], "outer_end": cutoff}]
        with threadpool_limits(limits=settings["budget"]["max_threads"]):
            stage = time.perf_counter()
            market = market_from({"design": design, "bundle": bundle})
            timers["load_and_verify_schedule"] = time.perf_counter() - stage
            for profile in ("swing", "day"):
                stage = time.perf_counter()
                data = profile_data(market, settings, cfg["profiles"][profile]["parent"], bundle["contract"])
                timers[f"{profile}_features"] = time.perf_counter() - stage
                counts[f"{profile}_decision_bars"] = len(data["features"])
                if profile == "day":
                    for entry, exit_name in product(settings["strategies"], settings["exits"]):
                        trial = {"id": f"diagnostic/{entry}/{exit_name}", "entry": entry, "exit_policy": exit_name,
                                 "cost": fee, "model": "rules_v1"}
                        stage = time.perf_counter()
                        batch = label_batch(design, data, trial)
                        timers["day_six_label_passes"] = timers.get("day_six_label_passes", 0) + time.perf_counter() - stage
                        counts["day_setups"] += len(batch[0])
                        counts["day_mature"] += len(batch[2])
                        sizes["raw_label_and_orders_json"] += sum(len(json.dumps(row, default=str)) for row in batch[3])
                        sizes["gz_label_and_orders"] += len(jsonl_bytes(batch[3]))
                        predictions = {"benchmark_june_2022": pd.Series(1., index=pd.DatetimeIndex(batch[0].keys(), tz="UTC"))}
                        stage = time.perf_counter()
                        metrics, ledger = account_replay(design, data, trial, batch, predictions, {})
                        timers["day_six_account_passes"] = timers.get("day_six_account_passes", 0) + time.perf_counter() - stage
                        counts["account_closes"] += metrics["closed_trades"]
                        sizes["gz_label_and_orders"] += len(jsonl_bytes(ledger))
            # Exercise the actual funding posting kernel across a physical partition, using a
            # declared wide diagnostic setup at real observed prices. This is no candidate trial.
            stage = time.perf_counter()
            step = market.step
            at = market.index[market.starts[1]] - 8 * step
            base = float(market.trade_prices[int(market.times.searchsorted(at.value))][0])
            setup = Setup(at-step, at-step, at, at+step, at+pd.Timedelta(hours=12),
                          base, base*.08, base*.82, 1, None, None, "diagnostic", "fixed_r_v1")
            model_risk = Risk(**settings["risk"]["perp_equal_risk_v1"])
            result = run_trade(market, setup, Contract.load(bundle["contract"]), Cost.load(settings["costs"][fee]),
                               model_risk, 10000., settings["execution"], utc(cutoff), keep_marks=True)
            timers["cross_partition_funding_proxy"] = time.perf_counter() - stage
            counts["diagnostic_funding_postings"] = len(result.get("settlements", []))
            counts["diagnostic_mark_events"] = len(result.get("marks", []))
    finally:
        done.set()
        rss.join()
    elapsed = time.perf_counter() - started
    record = {"declaration": declaration, "elapsed_seconds": elapsed, "timers_seconds": timers,
              "peak_rss_bytes": max([child_start, *samples]), "initial_rss_bytes": child_start,
              "bytes": dict(sizes), "counts": dict(counts), "coverage": market.coverage(),
              "normalized_native_rows_per_second": len(market.index)/elapsed,
              "diagnostic_result": {k: result.get(k) for k in ("status", "funding", "entry_at", "exit_at", "settlements")},
              "monitor_before": before, "monitor_after": heartbeat()}
    write_json(out, record)
    return record
