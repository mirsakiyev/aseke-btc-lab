import math

from lab.config import fingerprint
from lab.learning.design import MIB
from lab.phase11.benchmark import run as benchmark
from lab.phase11.plan import ROOT, freeze_plan
from lab.storage import file_hash, read_json, utc_now, write_json, writer_lock


def make_amendment(plan, evidence):
    body = plan["body"]
    bounds = body["unique_observations"]
    ceiling = bounds["trade_minutes_ceiling"]
    observed = evidence["coverage"]["rows"]["trade"]
    train_ratio = ceiling / observed
    eval_ratio = body["profiles"]["day"]["full_equity_timeline_observation_ceiling"] / (
        body["profiles"]["day"]["account_passes"] * observed)
    timers, counts = evidence["timers_seconds"], evidence["counts"]
    # Swing has 1/16 as many decision slots. Give it 21x the Day maximum hold per slot.
    swing_work_ratio = (body["profiles"]["swing"]["maximum_hold_minutes"] /
                        body["profiles"]["day"]["maximum_hold_minutes"] /
                        (body["profiles"]["swing"]["native_minutes_per_decision"] /
                         body["profiles"]["day"]["native_minutes_per_decision"]))
    day_kernel_seconds = (timers["day_six_label_passes"] * train_ratio
                          + timers["day_six_account_passes"] * eval_ratio * 4)
    fixed_time = (timers["load_and_verify_schedule"] + timers["day_features"] + timers["swing_features"]) * train_ratio
    expected_time = math.ceil(fixed_time + day_kernel_seconds * (1 + swing_work_ratio) + 300)
    headroom_time = math.ceil(expected_time * 1.5)
    # Published assumptions for capacity only. Actual child RSS and deadline are enforced.
    extra_index = (ceiling - observed) * 32
    extra_equity = (body["profiles"]["day"]["full_equity_timeline_observation_ceiling"] //
                    body["profiles"]["day"]["account_passes"] - observed) * 96
    headroom_rss = math.ceil(evidence["peak_rss_bytes"] + extra_index + extra_equity + 96 * MIB)
    retained = math.ceil((evidence["bytes"]["trade"] + evidence["bytes"]["mark"] +
                          evidence["bytes"]["funding"]) * train_ratio)
    compressed_trials = math.ceil(evidence["bytes"]["gz_label_and_orders"] *
                                  (train_ratio + 4 * eval_ratio) * 2 * 1.25)
    compressed_months = sum(body["acquisition"]["cached_compressed_bytes_by_stream"].values()) / 2
    estimated_download = math.ceil(compressed_months * body["acquisition"]["months"] * 1.15)
    budget = body["parent_budget"]
    original = ROOT.parent / "phase10" / "binance-probe" / "result.json"
    return {"type": "APPEND_ONLY_RESOURCE_AMENDMENT", "profile": body["version"], "protocol": body["protocol"],
            "calculation_revision": "stream-and-pass-accounting-v2",
            "parent_blocker_sha256": file_hash(original), "work_plan": plan["id"],
            "original_cap": body["original_cap"], "old_outcome": "No full frozen fold fit the 650000 joined per-basis rows",
            "unique_observation_allowance": {"trade": ceiling, "mark": ceiling,
                                             "funding": bounds["funding_rows_ceiling_at_one_hour"], "index": 0},
            "max_native_partition_rows": body["max_native_partition_rows"],
            "max_native_price_partitions_resident": 2,
            "acquisition": body["acquisition"], "unchanged_guards": budget,
            "replay": {"profiles": body["profiles"], "max_trials": sum(p["trial_count"] for p in body["profiles"].values()),
                       "max_estimator_fits": sum(p["max_estimator_fits"] for p in body["profiles"].values()),
                       "max_baseline_mean_computations": sum(p["max_baseline_mean_computations"] for p in body["profiles"].values()),
                       "max_profile_trial_attempts": budget["max_attempts_per_trial"],
                       "funding_proxy_measure": counts["diagnostic_funding_postings"],
                       "trade_kernel_visits_all_attempts_ceiling": sum(
                           p["trade_kernel_bar_visits_ceiling"] for p in body["profiles"].values()) *
                           budget["max_attempts_per_trial"],
                       "full_equity_visits_all_attempts_ceiling": sum(
                           p["full_equity_timeline_observation_ceiling"] for p in body["profiles"].values()) *
                           budget["max_attempts_per_trial"],
                       "native_clock_filter_passes_all_attempts_ceiling": sum(
                           p["account_passes"] for p in body["profiles"].values()) * budget["max_attempts_per_trial"],
                       "native_clock_filter_rows_all_attempts_ceiling": sum(
                           p["account_passes"] for p in body["profiles"].values()) * ceiling * budget["max_attempts_per_trial"],
                       "derived_feature_scans_per_process": len(body["profiles"]),
                       "scope_note": "Mark and trade partitions are indexed once per fit process. Feature scans read trade only. "
                                     "Account passes filter the native clock before constructing each equity timeline.",
                       "deadline_policy": "Fixed once per campaign before the first fit; consumed reservations, "
                                          "completed trials, fits, and deadline are durable on resume."},
            "benchmark": {"sha256": file_hash(ROOT / "benchmark-june-2022.json"), "measurements": evidence},
            "projection": {"day_kernel_seconds": day_kernel_seconds, "swing_cost_ratio": swing_work_ratio,
                           "estimator_model_io_and_variation_seconds": 300, "fit_seconds_expected": expected_time,
                           "fit_seconds_with_50pct_headroom": headroom_time, "peak_child_rss_bytes_with_headroom": headroom_rss,
                           "rss_headroom_components": {"unique_index_bytes": extra_index, "equity_bytes": extra_equity,
                                                       "additional_runtime_bytes": 96 * MIB},
                           "estimated_compressed_download_bytes_with_headroom": estimated_download,
                           "normalized_storage_bytes_sample_scaling": retained,
                           "compressed_trial_storage_bytes_with_headroom": compressed_trials,
                           "output_headroom_factor": 1.25, "acquisition_hard_retention_bytes":
                               body["acquisition"]["max_retained_dataset_bytes"]},
            "fit_projection_within_existing_caps":
                headroom_time <= budget["wall_seconds"] and headroom_rss <= budget["child_rss_mib"] * MIB
                and compressed_trials <= budget["output_cap_mib"] * MIB}


def freeze():
    plan, evidence = freeze_plan(), benchmark()
    result = make_amendment(plan, evidence)
    key = fingerprint(result)
    path = ROOT / "resource-amendments" / f"{key}.json"
    with writer_lock(ROOT / ".amendment.lock"):
        if not path.exists():
            write_json(path, {"id": key, "body": result, "created_at": utc_now().isoformat()})
    record = read_json(path)
    if fingerprint(record["body"]) != record["id"]:
        raise ValueError("Resource amendment checksum mismatch")
    return record
