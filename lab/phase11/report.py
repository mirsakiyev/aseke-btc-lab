import psutil

from lab.learning.state import status as learning_status
from lab.phase9.reference import OUTPUT as PHASE9_OUTPUT
from lab.phase10.worker import status as worker_status
from lab.phase11.archive import acquisition_clock, folder, overview
from lab.phase11.campaign import ledger
from lab.phase11.plan import ROOT, freeze_plan
from lab.phase11.resources import freeze
from lab.storage import put_bytes, read_json


def optional(path, fallback=None):
    return read_json(path) if path.exists() else fallback


def historical():
    legacy = learning_status()
    saved = [read_json(path) for path in PHASE9_OUTPUT.glob("campaigns/*/result.json")]
    return {"phase8_campaigns": legacy["campaigns"],
            "phase8_candidate_attempts": legacy["cumulative_candidate_attempts"],
            "phase8_fit_attempts": legacy["cumulative_fit_attempts"], "phase8_mode": legacy["mode"],
            "phase9_campaigns": len(saved), "phase9_candidate_results": sum(sum(row["trials"].values()) for row in saved),
            "phase9_completed_estimator_fits": sum(row["actual_fits"] for row in saved)}


def status():
    plan = freeze_plan()
    view = worker_status()
    resource = freeze()
    archive = overview(plan)
    archive_root = folder(plan)
    clock = acquisition_clock(plan, archive_root) if archive["batches_started"] else None
    pointer = optional(ROOT / "latest-qualification.json", {})
    quality = optional(ROOT / "qualification" / f'{pointer.get("id", "")}.json')
    eligible = quality["body"]["eligible"] if quality else []
    attempts = [read_json(path) for path in (ROOT / "admission-attempts").glob("*.json")]
    reports, active = [], None
    for manifest in (ROOT / "campaigns").glob("*/manifest.json"):
        path = manifest.parent
        current = optional(path / "active.json")
        if current:
            try:
                if psutil.Process(current["pid"]).create_time() == current["created"]:
                    active = {"campaign": path.name, **current}
            except psutil.Error:
                pass
        reports.append({"id": path.name, "deadline": optional(path / "deadline.json"),
                        "result": optional(path / "result.json"), **ledger(path)})
    state = ("RUNNING" if active else "COMPLETE" if reports and
             all((row["result"] or {}).get("state") == "COMPLETE" for row in reports)
             else "PARTIAL" if reports else "READY_FOR_BOUNDED_RUN" if eligible else "WAITING_FOR_QUALIFIED_FUTURES_BUNDLE")
    return {"state": state, "execution": "MANUAL_OUTSIDE_LAB", "validation": "No validated model", "plan": plan["id"],
            "amendment": resource["id"], "resource_profile": resource["body"]["profile"],
            "qualification": pointer.get("id"), "eligible_folds": eligible, "archive": archive, "archive_clock": clock,
            "continuous": {"phase8_mode": view["research"]["legacy_mode"],
                           "phase9_manual": view["research"]["phase9_manual_gate"], "phase10": view["controls"]["training"]},
            "monitor": {"runtime": view["runtime"], "identity": view["identity"], "checkpoint": view["profiles"]["day"].get("checkpoint"),
                        "decision_counts": view["decision_counts"]}, "historical": historical(),
            "active_campaign": active, "campaigns": reports,
            "admission_attempts": attempts,
            "phase11_actual_estimator_fits": sum(row["actual_estimator_fits"] for row in reports)}


def report():
    snapshot = status()
    root = folder(freeze_plan())
    res = read_json(ROOT / "resource-amendments" / f'{snapshot["amendment"]}.json')["body"]
    qual = optional(ROOT / "qualification" / f'{snapshot["qualification"]}.json', {"body": {}})["body"]
    repair = optional(root / "repair-result.json", {})
    daily = optional(root / "daily-fallback-result.json", {})
    overlap = optional(ROOT / "benchmark-june-2022-monitor-overlap.json")
    b = res["benchmark"]["measurements"]
    projection = res["projection"]
    archive = snapshot["archive"]
    counts = archive["coverage"]
    text = ["# Phase 11 futures workload and campaign", "",
            f"State **{snapshot['state']}**. Execution **MANUAL_OUTSIDE_LAB**. Approval **No validated model**.", "",
            "## Workload and guardrails", "",
            f"The original 650,000-row cap counted admitted rows separately for trade and mark. The amended ceiling is "
            f"{res['unique_observation_allowance']['trade']:,} observations per price basis. The combined maximum is "
            f"{res['unique_observation_allowance']['trade'] + res['unique_observation_allowance']['mark']:,}. "
            "Index rows are outside the frozen simulator's inputs. The latest fold's 712,800 inner and outer minutes "
            "omit its earlier training and warmup history.", "",
            f"The profile retains {res['max_native_partition_rows']:,} rows per partition, two resident price partitions, "
            "one research job, two threads, 640 MiB child RSS, 256 MiB output and a two-hour immutable fit deadline. "
            f"The frozen catalog has {res['replay']['max_trials']} candidates/controls and at most "
            f"{res['replay']['max_estimator_fits']} estimator fits, with mean controls counted separately. "
            "Cash/benchmark controls and the fixed KuCoin fee scenario retain their earlier definitions.", "",
            f"The June 2022 benchmark covered {b['coverage']['rows']['trade']:,} native minutes in "
            f"{b['elapsed_seconds']:.2f} seconds at {b['peak_rss_bytes']/2**20:.1f} MiB peak RSS. "
            f"The full guarded estimate is {projection['fit_seconds_with_50pct_headroom']:,} seconds, "
            f"{projection['peak_child_rss_bytes_with_headroom']/2**20:.1f} MiB RSS and "
            f"{projection['compressed_trial_storage_bytes_with_headroom']/2**20:.1f} MiB of compressed trial outputs.", "",
            "## Historical coverage", "",
            f"Binance USD-M BTCUSDT, USDT-settled, retained as research only. "
            f"The archive verified {archive['objects_verified']} of {archive['objects_planned']} source objects and used "
            f"{archive['requests_used']} GETs. It reused eight Phase 10 samples. "
            f"The exact-range official price pass restored {repair.get('restored_rows',0):,} rows, "
            f"and alternate official daily packages restored {daily.get('restored_rows',0):,}.", "",
            "| Stream | Observations | First UTC | Last UTC |", "| --- | ---: | --- | --- |"]
    for name, row in counts.items():
        text.append(f"| {name} | {row['rows']:,} | {row['first'] or '-'} | {row['last'] or '-'} |")
    if overlap:
        before, after = overlap["monitor_before"]["day"], overlap["monitor_after"]["day"]
        text += ["", "## Monitoring during the benchmark", "",
                 f"The fixed-slice overlap check took {overlap['elapsed_seconds']:.2f} seconds. "
                 f"The task-managed Day checkpoint advanced from {before['checkpoint']} to {after['checkpoint']} "
                 f"with decision {after['decision']} and timing {after['timing']}."]
    text += ["", "## Frozen fold gates", "", "| Fold | Qualified | Missing inner/outer trade and mark | Funding | Reason |",
             "| --- | --- | --- | ---: | --- |"]
    for name, row in qual.get("folds", {}).items():
        gaps = row["evaluation_missing_minutes"]
        reasons = row["issues"] + row["missing_price_objects"] + row.get("missing_funding_objects", [])
        text.append(f"| {name} | {'yes' if row['eligible'] else 'no'} | {gaps.get('trade',0)} / {gaps.get('mark',0)} | "
                    f"{row.get('funding',{}).get('rows',0):,} | {'. '.join(reasons) or '-'} |")
    text += ["", f"Phase 11 completed **{snapshot['phase11_actual_estimator_fits']} estimator fits**. "
             f"Eligible windows **{', '.join(snapshot['eligible_folds']) or 'none'}**. "
             "The archive benchmark and funding proxy are diagnostic only.", "",
             "The versioned JSON files contain exact UTC intervals, trial order, counts, input hashes, benchmark measurements, "
             "and request and replay budgets. Prospective monitoring and historical inference use separate data and state.", "",
             "[Source terms](https://github.com/binance/binance-public-data/blob/master/TERMS_AND_CONDITIONS.md)", ""]
    path = ROOT / "REPORT.md"
    put_bytes(path, "\n".join(text).encode("utf-8"))
    return {"report": str(path), "state": snapshot["state"], "plan": snapshot["plan"],
            "amendment": snapshot["amendment"], "qualification": snapshot["qualification"],
            "fits": snapshot["phase11_actual_estimator_fits"]}
