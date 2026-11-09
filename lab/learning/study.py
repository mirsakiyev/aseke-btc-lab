from collections import Counter
from contextlib import closing
import json
import sqlite3

import pyarrow
from threadpoolctl import threadpool_limits

from lab.config import fingerprint
from lab.learning import state
from lab.learning.data import DataBlocked, capabilities, load_market, market_coverage
from lab.learning.design import OUTPUT, SPOT, read_manifest
from lab.learning.diagnostics import diagnostics
from lab.learning.evaluation import account_replay, benchmarks, forecasts, label_batch, prepare
from lab.learning.features import feature_registry
from lab.phase2 import store
from lab.phase7.operation import checkpoint, health
from lab.storage import file_hash, put_bytes, read_json, utc, utc_now, write_json


class Interrupted(RuntimeError):
    pass


def add(job, kind, body, trial=None):
    with state.connect(write=True) as db:
        state.event(db, job["id"], kind, body, trial)


def stage(job, trial, phase, fit=None):
    with state.connect(write=True) as db:
        status = state.current(db)
        if status["mode"] not in ("once", "continuous") or utc_now() >= utc(job["deadline"]):
            raise Interrupted("Campaign paused, stopped, or expired")
        state.save(db, status, state=phase, active_trial=trial, reason=f"{phase.title()} frozen research")
        if fit:
            state.event(db, job["id"], "FIT_STARTED" if not trial.endswith("mean_control_v1") else "MEAN_FIT_STARTED",
                        {"candidate": trial, **fit}, trial)


def observer_snapshot(path):
    temporary = path.with_suffix(".tmp")
    if not path.exists():
        try:
            temporary.unlink(missing_ok=True)
            with store.connect(readonly=True) as origin, closing(sqlite3.connect(temporary)) as destination:
                origin.backup(destination, pages=64, sleep=.05)
                if destination.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise ValueError("Read-only observer backup failed integrity check")
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    return {"path": str(path), "sha256": file_hash(path), "method": "SQLite online backup from a read-only connection",
            "learning_input": False, "scope": "Operational continuity evidence only. All labels use allowed immutable prices."}


def origin_for(job, trial, key, label_meta, profile, fitted):
    with state.connect() as db:
        matches = db.execute("""
            SELECT job,at,body FROM events WHERE trial=? AND kind='TRIAL_RESULT' AND job<>? ORDER BY seq DESC
        """, (trial["id"], job["id"])).fetchall()
    for row in matches:
        result = json.loads(row["body"])
        if result.get("dedup_key") == key and result["status"] not in ("FAILED", "WAITING_FOR_DATA"):
            return {"status": "DEDUPLICATED", "prior_job": row["job"], "reason": "Identical trial evidence already registered"}
        if fitted and result.get("fit_count", 0):
            old_ids = set(result["labels"]["label_outcome_ids"])
            new = len(set(label_meta["label_outcome_ids"]) - old_ids)
            elapsed = (utc_now() - utc(row["at"])).total_seconds()
            if elapsed < profile["interval_days"] * 86400 or new < profile["minimum_new_mature"]:
                return {"status": "WAITING_FOR_DATA", "reason": "Configured cadence and novel mature outcome floors",
                        "new_mature_outcomes": new, "prior_job": row["job"], "elapsed_seconds": elapsed,
                        "interval_days": profile["interval_days"], "minimum_new_mature": profile["minimum_new_mature"]}
            break
    return None


def run_candidate(manifest, job, folder, trial, data, batches, source):
    design = manifest["design"]
    key = fingerprint([trial, source, design["settings"], design["folds"], design["code_sha256"]])
    def phase(value, fit=None):
        stage(job, trial["id"], value, fit)

    phase("EVALUATING")
    if trial["model"] in ("cash", "passive_25pct"):
        old = origin_for(job, trial, key, None, data["profile"], False)
        if old:
            return old
        metrics = benchmarks(design, data, trial["cost"], trial["model"])
        return {"status": metrics.get("status", "COMPLETED"), "dedup_key": key, "metrics": metrics}
    batch_key = (trial["entry"], trial["exit_policy"], trial["cost"])
    if batch_key not in batches:
        batch = label_batch(design, data, trial)
        filename = f"labels-{fingerprint([trial['profile'], batch_key])[:16]}.json"
        write_json(folder / filename, {"trial_family": trial, "features": design["feature_version"],
                                       "label_version": design["label_version"], "records": batch[3]})
        batch[4]["artifact"] = {"path": filename, "sha256": file_hash(folder / filename)}
        batches[batch_key] = batch
    batch = batches[batch_key]
    label_meta = batch[4]
    fitted = trial["model"] in ("ridge_v1", "hist_v1")
    if previous := origin_for(job, trial, key, label_meta, data["profile"], fitted):
        return previous
    predictions, folds = forecasts(design, data, trial, batch, folder / "models", phase)
    if not predictions:
        return {"status": "INSUFFICIENT_EVIDENCE", "dedup_key": key, "labels": label_meta, "folds": folds,
                "fit_count": 0, "reason": "No fold cleared the frozen chronological sample floors"}
    versions = {fold["fold"]: fold["model_artifacts"]["outer"]["version"]
                for fold in folds if fold.get("model_artifacts")}
    metrics, attempts = account_replay(design, data, trial, batch, predictions, versions)
    filename = f"orders-{fingerprint(trial['id'])[:16]}.json"
    write_json(folder / filename, {"manifest": manifest["id"], "instrument": data["profile"]["instrument"], "source": source,
                                   "risk": data["profile"]["risk"], "trial": trial, "records": attempts})
    adequate = all(fold["status"] in ("EVALUATED", "RULES") for fold in folds)
    return {"status": "COMPLETED" if adequate and metrics["full_window_observed"] else "INSUFFICIENT_EVIDENCE",
            "dedup_key": key, "labels": label_meta, "folds": folds, "metrics": metrics,
            "fit_count": sum(fold.get("fit_count", 0) for fold in folds),
            "orders": {"path": filename, "sha256": file_hash(folder / filename)}}


def reports(manifest, job, folder, data_coverage, snapshot, before):
    design = manifest["design"]
    with state.connect() as db:
        results = state.trials(db, job["id"])
    verdicts = {}
    for profile in job["profiles"]:
        rows = [row for row in results.values() if row["trial"]["profile"] == profile and row["trial"]["role"] == "primary"]
        if rows and all(row["status"] == "BLOCKED_DATA" for row in rows):
            verdicts[profile] = "BLOCKED_DATA"
            continue
        filtered = [row for row in rows if row["trial"]["model"] in ("ridge_v1", "hist_v1") and row["status"] == "COMPLETED"
                    and row["metrics"]["uncertainty"]["status"] == "DESCRIPTIVE"]
        rules = {(row["trial"]["entry"], row["trial"]["exit_policy"]): row["metrics"]["return"]
                 for row in rows if row["trial"]["model"] == "rules_v1" and row["status"] == "COMPLETED"}
        comparable = [row for row in filtered if (row["trial"]["entry"], row["trial"]["exit_policy"]) in rules]
        improved = any(row["metrics"]["return"] > max(0., rules[row["trial"]["entry"], row["trial"]["exit_policy"]])
                       and row["metrics"]["uncertainty"]["mean_net_r_95_interval"][0] > 0 for row in comparable)
        verdicts[profile] = ("BETTER_ON_DEVELOPMENT" if improved else "NO_DEMONSTRATED_IMPROVEMENT" if comparable
                             else "INSUFFICIENT_EVIDENCE")
    after = checkpoint(before["event_watermark"])
    after_health = health()
    continuity = {"before": before, "after": after, "heartbeat": after_health["heartbeat_state"],
                  "worker_pid": after_health["worker"].get("pid"),
                  "journal_prefix_preserved": before["journal_prefix_sha256"] == after["journal_prefix_sha256"],
                  "experiment_preserved": before["experiment"] == after["experiment"],
                  "account_state_equal_at_checkpoints": before["accounts"] == after["accounts"]}
    write_json(folder / "report.json", {
        "job": job, "manifest": manifest["id"], "cutoff": design["available_at_cutoff_exclusive"],
        "candidate_count": sum(row["role"] == "primary" and row["profile"] in job["profiles"] for row in design["trials"]),
        "trials_by_status": dict(Counter(row["status"] for row in results.values())), "profiles": verdicts, "results": results,
        "collector_snapshot": snapshot, "collector_continuity": continuity, "holdout_used": False,
        "challengers_nominated": [], "approved_lane": "No validated model", "historical_finding": "INCONCLUSIVE",
    })
    write_json(folder / "data-capabilities.json", capabilities(design, data_coverage))
    report = ["# Controlled learning development comparison", "",
              "Previously inspected BTC development data. The final holdout remains sealed. No validated model.", "",
              f"Campaign `{job['id']}`. Data cutoff {design['available_at_cutoff_exclusive']} (exclusive).", "",
              "| Profile | Finding |", "| --- | --- |"]
    report.extend(f"| {profile} | {verdict} |" for profile, verdict in verdicts.items())
    report.extend(["", "## Spot development and controls", "",
                   "| Entry | Exit | Variant | Cost | Status | Return | Drawdown | Trades |",
                   "| --- | --- | --- | --- | --- | ---: | ---: | ---: |"])

    def percent(value):
        return f"{value:+.2%}" if value is not None else "—"

    for result in results.values():
        trial = result["trial"]
        if trial["profile"] != SPOT:
            continue
        metrics = result.get("metrics", {})
        report.append(f"| {trial.get('entry', '—')} | {trial.get('exit_policy', '—')} | {trial['model']} | "
                      f"{trial['cost']} | {result['status']} | {percent(metrics.get('return'))} | "
                      f"{percent(metrics.get('drawdown_observed_marks'))} | {metrics.get('closed_trades', '—')} |")
    report.extend(["", "Some windows have few independent trades. The descriptive moving-block intervals are "
                   "unadjusted for catalog selection. This comparison consumed previously inspected development periods. "
                   "No challenger was nominated. Synthetic leverage and execution stresses are mechanics diagnostics.", "",
                   "Use report.json for prediction errors, account risk, fees, funding, trade concentration and uncertainty. "
                   "The all-trials ledger includes blocked, incomplete and failed work. Data requirements are in data-capabilities.json."])
    put_bytes(folder / "REPORT.md", ("\n".join(report) + "\n").encode("utf-8"))


def run(job_id):
    with state.connect() as db:
        job = state.job(db, job_id)
    manifest = read_manifest(job["manifest"])
    design, folder = manifest["design"], OUTPUT / manifest["id"] / job_id
    folder.mkdir(parents=True, exist_ok=True)
    before_path = folder / "collector-before.json"
    if not before_path.exists():
        write_json(before_path, checkpoint())
    before = read_json(before_path)
    snapshot = observer_snapshot(folder / "observer-snapshot.sqlite3")
    stage(job, None, "EVALUATING")
    diagnostics_path = folder / "simulator-diagnostics.json"
    if not diagnostics_path.exists():
        values = diagnostics(design["settings"])
        write_json(diagnostics_path, values)
        add(job, "SIMULATOR_DIAGNOSTIC", {"file": str(diagnostics_path), "sha256": file_hash(diagnostics_path),
                                         "leverage_cases": list(values["stress_grid"]), "stresses": list(values["stresses"])})
        cases = {"ordinary_long": values["ordinary_long"], "ordinary_short": values["ordinary_short"],
                 **{f"leverage_{key}x": value for key, value in values["stress_grid"].items()},
                 **values["stresses"]}
        for name, result in cases.items():
            add(job, "DIAGNOSTIC_RESULT", {"name": name, "scope": "SIMULATOR_DIAGNOSTIC", "result": result})
    write_json(folder / "feature-registry.json", {"version": design["feature_version"], **feature_registry()})
    data_coverage = {}
    with threadpool_limits(limits=design["settings"]["budget"]["max_threads"]):
        for profile in job["profiles"]:
            data, batches = None, {}
            source = design["data"][profile]
            try:
                market, instrument = load_market(design, profile)
                data = prepare(design, profile, market, instrument)
                data_coverage[profile] = market_coverage(market)
            except (DataBlocked, OSError, pyarrow.ArrowInvalid) as exc:
                data_coverage[profile] = {"status": "BLOCKED_DATA", "reason": str(exc)}
            with state.connect() as db:
                previous = state.trials(db, job_id)
            for trial in (item for item in design["trials"] if item["profile"] == profile):
                trial_id = trial["id"]
                if previous.get(trial_id, {}).get("status") in state.TERMINAL_TRIALS:
                    continue
                stage(job, trial_id, "EVALUATING")
                with state.connect() as db:
                    count = db.execute("SELECT count(*) FROM events WHERE job=? AND trial=? AND kind='TRIAL_STARTED'",
                                       (job_id, trial_id)).fetchone()[0]
                if count >= design["settings"]["budget"]["max_attempts_per_trial"]:
                    result = {"status": "FAILED", "reason": "Frozen attempt ceiling"}
                else:
                    add(job, "TRIAL_STARTED", {"trial": trial, "attempt": count + 1, "manifest": manifest["id"],
                                               "data": source, "available_cutoff": design["available_at_cutoff_exclusive"]},
                        trial_id)
                    if data is None:
                        result = data_coverage[profile]
                    else:
                        try:
                            result = run_candidate(manifest, job, folder, trial, data, batches, source)
                        except Interrupted:
                            raise
                        except (ValueError, RuntimeError, ArithmeticError) as exc:
                            result = {"status": "FAILED", "reason": f"{type(exc).__name__}: {exc}"}
                add(job, "TRIAL_RESULT", {**result, "trial": trial}, trial_id)
            del data, batches
    reports(manifest, job, folder, data_coverage, snapshot, before)
    add(job, "JOB_COMPLETE", {"report": str(folder / "REPORT.md")})
