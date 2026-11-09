from collections import Counter
import math
from pathlib import Path

import pandas as pd

from lab.config import fingerprint
from lab.learning.design import MIB, inventory
from lab.phase9.reference import parent_design, reference
from lab.phase10.archive import BASE, BUDGET as BATCH, OUTPUT as PRIOR, TERMS
from lab.phase10.partition import MAX_CHUNK_ROWS
from lab.storage import RUNS, file_hash, read_json, utc, write_json, writer_lock


ROOT = RUNS / "phase11"
SOURCE_FROM = "2019-09-01T00:00:00Z"  # September archive envelope, actual first data are measured.
LAUNCH = "https://www.binance.com/en/blog/all/421499824684900979"
FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
PROFILES = ("day", "swing")
PRIMARY = ("klines", "markPriceKlines", "fundingRate")
MINUTE = pd.Timedelta("1min")
AMENDMENT = "phase11-futures-resources-v1"
PACE = .35
ACQUISITION_DISK = 256 * MIB
SOURCE_GUARD_REQUESTS = 5


def archive_spec(kind, stamp, cadence):
    start = utc(stamp)
    end = start + (pd.offsets.MonthBegin() if cadence == "monthly" else pd.Timedelta("1D"))
    token = start.strftime("%Y-%m" if cadence == "monthly" else "%Y-%m-%d")
    directory = "" if kind == "fundingRate" else "/1m"
    filename = f"BTCUSDT-{'fundingRate' if kind == 'fundingRate' else '1m'}-{token}.zip"
    return {"key": f"{token}-{kind}", "kind": kind, "start": start.isoformat(), "end": end.isoformat(),
            "url": f"{BASE}/{cadence}/{kind}/BTCUSDT{directory}/{filename}"}


def sources(right):
    start = utc(SOURCE_FROM)
    last_month = right.normalize().replace(day=1)
    monthly = [archive_spec(kind, stamp, "monthly")
               for stamp in pd.date_range(start, last_month, freq="MS", inclusive="left")
               for kind in PRIMARY]
    daily = [archive_spec(kind, stamp, "daily")
             for stamp in pd.date_range(last_month, right, freq="D", inclusive="left")
             for kind in PRIMARY[:2]]
    funding = {"key": "embargo-tail-funding", "kind": "fundingREST", "start": last_month.isoformat(),
               "end": right.isoformat(), "url": FUNDING_URL,
               "params": {"symbol": "BTCUSDT", "startTime": last_month.value // 10**6,
                          "endTime": right.value // 10**6 - 1, "limit": 1000}}
    return [*monthly, *daily, funding]


def scope():
    parent = parent_design()
    design, cfg = parent["design"], reference()
    settings = design["settings"]
    right, start = utc(design["holdout"]["start"]), utc(SOURCE_FROM)
    specs = sources(right)
    previous = PRIOR / "binance-probe" / "result.json"
    probe = read_json(previous)
    by_url = {row["url"]: row for row in probe["samples"] if row["status"] == "VERIFIED_SAMPLE"}
    cached = {}
    for spec in specs:
        row = by_url.get(spec["url"])
        if not row:
            continue
        path = previous.parent / "raw" / f"{row['sha256']}.zip"
        if path.is_file() and file_hash(path) == row["sha256"]:
            cached[spec["key"]] = {"path": str(path), "sha256": row["sha256"], "rows": row["rows"],
                                   "prior_sample": row}
    by_profile = {}
    plan_folds = []
    for fold in design["folds"]:
        inner, outer, end = [utc(fold[key]) for key in ("inner_start", "outer_start", "outer_end")]
        plan_folds.append({**fold, "train_inner": [settings["evaluation"]["train_start"], inner.isoformat()],
                           "train_outer": [settings["evaluation"]["train_start"], outer.isoformat()],
                           "inner_validation": [inner.isoformat(), outer.isoformat()],
                           "outer_evaluation": [outer.isoformat(), end.isoformat()],
                           "strict_label_maturity_before": [inner.isoformat(), outer.isoformat(), end.isoformat()],
                           "shared_source_observation_prefix": [start.isoformat(), end.isoformat()],
                           "inner_outer_minutes_per_basis": int((end - inner) // MINUTE),
                           "entire_source_prefix_minutes_per_basis": int((end - start) // MINUTE)})
    source_minutes = int((right - start) // MINUTE)
    floor = int((right - utc(settings["evaluation"]["train_start"])) // MINUTE)
    outer_minutes = int((right - utc(design["folds"][0]["outer_start"])) // MINUTE)
    for name in PROFILES:
        profile = cfg["profiles"][name]["parent"]
        policy = settings["profiles"][profile]
        trials = [row for row in design["trials"] if row["profile"] == profile and row["role"] != "cost_diagnostic"]
        lanes = sorted({(row["entry"], row["exit_policy"]) for row in trials if "entry" in row})
        # Same candidate IDs and costs, with controls grouped with their label lane to bound resident objects.
        ordered = [row["id"] for lane in lanes for row in trials if (row.get("entry"), row.get("exit_policy")) == lane]
        ordered.extend(row["id"] for row in trials if "entry" not in row)
        decision = pd.Timedelta(policy["decision"])
        context = {item["frame"]: item["sma"] + 1 for item in policy["context"]}
        max_warmup = max([decision * 34, *(pd.Timedelta(frame) * n for frame, n in context.items())])
        replay_candidates = sum("entry" in row for row in trials)
        baseline = sum(row["model"] == "mean_control_v1" for row in trials)
        fits = sum(row["model"] in ("ridge_v1", "hist_v1") for row in trials) * len(plan_folds) * 2
        max_steps = policy["max_hold_minutes"] + 3
        by_profile[name] = {"profile": profile, "native_minutes_per_decision": int(decision // MINUTE),
                            "context_complete_bars_for_slope": context,
                            "planned_maximum_context_lookback_seconds": int(max_warmup.total_seconds()),
                            "feature_observation_start": start.isoformat(),
                            "pre_inner_feature_warmup_latest_start_by_fold":
                                {fold["id"]: (utc(fold["inner_start"]) - max_warmup).isoformat() for fold in plan_folds},
                            "ema_and_pivots": "Carry exact causal state on every complete derived bar since the first observation. "
                                              "Confirmed pivots may delay readiness beyond the structural minimum.",
                            "maximum_hold_minutes": policy["max_hold_minutes"],
                            "processing_seconds": settings["execution"]["processing_seconds"],
                            "exit_observation_end_exclusive": right.isoformat(), "latest_label_decision_by_fold":
                                {fold["id"]: (utc(fold["outer_end"]) - pd.Timedelta(minutes=policy["max_hold_minutes"] + 1)).isoformat()
                                 for fold in plan_folds},
                            "decision_bar_ceiling": math.ceil(source_minutes / (decision // MINUTE)),
                            "trial_order": ordered, "trial_count": len(trials), "label_passes": len(lanes),
                            "account_passes": replay_candidates, "max_estimator_fits": fits,
                            "max_baseline_mean_computations": baseline * len(plan_folds) * 2,
                            "full_equity_timeline_observation_ceiling": replay_candidates * (outer_minutes + 1),
                            "trade_kernel_bar_visits_ceiling":
                                max_steps * (len(lanes) * math.ceil(source_minutes / (decision // MINUTE))
                                             + replay_candidates * math.ceil(outer_minutes / (decision // MINUTE))),
                            "profiles_use_same_sources": True}
    sample_sizes = Counter()
    for item in cached.values():
        sample_sizes[item["prior_sample"]["kind"]] += Path(item["path"]).stat().st_size
    monthly = len([spec for spec in specs if spec["kind"] == "fundingRate"])
    zipped = sum(spec["kind"] != "fundingREST" and spec["key"] not in cached for spec in specs)
    return {"protocol": parent["id"], "reference": cfg["version"], "version": AMENDMENT, "source": {
                "venue": "Binance USD-M", "symbol": "BTCUSDT", "settlement": "USDT", "scope": "RESEARCH_ONLY",
                "terms": TERMS, "terms_cached_sha256": probe["terms_sha256"], "listing_evidence": LAUNCH},
            "original_cap": {"rows": settings["budget"]["max_native_rows"],
                             "phase8": "Parquet metadata rows per individual file before push-down",
                             "phase9": "Sum of declared imported rows per joined basis before deduplication",
                             "phase10": "Sum of admitted physical parquet trade rows separately from mark rows; each must "
                                        "fit the cap. With valid ordered nonoverlap these equal unique timestamps per basis. "
                                        "It excludes aggregate streams, replay visits, and memory. Funding has an independent file cap."},
            "windows": plan_folds, "holdout": design["holdout"], "profile_order": list(PROFILES),
            "profiles": by_profile, "parent_budget": settings["budget"], "workstation_at_plan": inventory(),
            "unique_observations": {"logical_pre_holdout_minutes_from_train_start": floor,
                                    "archive_union": [start.isoformat(), right.isoformat()],
                                    "trade_minutes_ceiling": source_minutes, "mark_minutes_ceiling": source_minutes,
                                    "combined_price_observations_ceiling": 2 * source_minutes,
                                    "funding_rows_ceiling_at_one_hour": source_minutes // 60 + 1,
                                    "required_index_rows": 0, "index_reason": "Frozen simulator consumes supplied mark. No index input.",
                                    "pre_source_listing_minutes": floor - source_minutes},
            "acquisition": {"months": monthly, "requests_per_batch": BATCH["requests"],
                            "batch_wall_seconds": BATCH["elapsed_seconds"], "batch_bytes": BATCH["total_bytes"],
                            "response_bytes": BATCH["response_bytes"], "min_spacing_seconds": PACE,
                            "max_retained_dataset_bytes": ACQUISITION_DISK,
                            "outstanding_zip_objects": zipped, "cached_zip_objects": len(cached),
                            "minimum_get_requests": 1 + zipped * 2 + 1,
                            "source_guard_request_slots": SOURCE_GUARD_REQUESTS,
                            "max_get_requests": 1 + zipped * 2 + 1 + SOURCE_GUARD_REQUESTS,
                            "cached_compressed_bytes_by_stream": dict(sample_sizes),
                            "min_batches": math.ceil((1 + zipped * 2 + 1) / BATCH["requests"])},
            "max_native_partition_rows": MAX_CHUNK_ROWS,
            "current_asset_count": len(specs), "cost_scenario": cfg["fees"][cfg["profiles"]["day"]["fee"]],
            "cached": cached, "sources": specs}


def freeze_plan():
    data = scope()
    # Resource inventory is an observation. It must not change a frozen work plan's identity.
    inventory_record = data.pop("workstation_at_plan")
    key = fingerprint(data)
    path = ROOT / "work-plans" / f"{key}.json"
    if not path.exists():
        with writer_lock(ROOT / ".work-plan.lock"):
            if not path.exists():
                write_json(path, {"id": key, "body": data, "workstation_at_first_plan": inventory_record})
    return read_json(path)
