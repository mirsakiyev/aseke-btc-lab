import json
from pathlib import Path
import re

import pandas as pd

from lab.config import Profile, fingerprint
from lab.model import split_labels
from lab.phase2 import store
from lab.phase2.checkpoint import AUDITS, frozen_runs
from lab.phase2.collector import GRACE, LOG_PATH, status
from lab.phase2.shadow import nominal_open
from lab.storage import utc, utc_now

from lab.phase3.design import AUDIT_ID


def session_entry_allowed(profile: Profile, decision_close, next_open) -> bool:
    if not profile.session:
        return True
    decision, opening = utc(decision_close), utc(next_open)
    cutoff = opening.normalize() + pd.Timedelta(minutes=profile.session.cutoff_minute_utc)
    return bool(opening < cutoff and opening.normalize() == decision.normalize())


def reconcile_day(destination: Path) -> dict:
    original, manifest = frozen_runs()["day"]
    profile = Profile(**manifest["profile"])
    root = AUDITS / AUDIT_ID
    funnel = pd.read_parquet(root / "day-funnel.parquet")
    funnel = funnel.loc[funnel.window.eq("evaluation")].copy()
    labels = pd.read_parquet(original / "hypothetical-labels.parquet")
    left, right = (utc(manifest["windows"][key]) for key in ("evaluation_start", "holdout_start"))
    evaluation, stats = split_labels(labels, left, right)
    scored = funnel.loc[funnel.predicted_net_return.notna()]
    if not scored.index.is_unique or not evaluation.index.is_unique:
        raise ValueError("Nonunique Day decision identifiers")
    pd.testing.assert_series_equal(scored.predicted_net_return, labels.loc[scored.index, "predicted_net_return"])
    funnel["baseline_signal"] = True
    funnel["session_entry_allowed"] = [session_entry_allowed(profile, at, opening)
                                       for at, opening in funnel.modeled_entry.items()]
    funnel["evaluable_original_label"] = funnel.index.isin(evaluation.index)
    funnel["scored_phase2"] = funnel.predicted_net_return.notna()
    funnel["score_accepted"] = funnel.predicted_net_return.gt(manifest["model"]["entry_threshold"])
    funnel["baseline_fill"] = funnel.baseline.isin(["filled", "filled_unresolved"])
    funnel["candidate_fill"] = funnel.candidate.isin(["filled", "filled_unresolved"])
    excluded = funnel.loc[funnel.scored_phase2 & ~funnel.evaluable_original_label]
    if len(evaluation.index.difference(scored.index)) or not (
        excluded.status.eq("entry_cutoff") & ~excluded.session_entry_allowed & excluded.target.isna()
    ).all():
        raise ValueError("Unexpected mismatch between Day label and score populations")
    if (funnel.loc[~funnel.session_entry_allowed, ["baseline_fill", "candidate_fill"]]).any().any():
        raise ValueError("Recorded historical Day entry crossed its cutoff")
    first = pd.Timestamp("2026-11-08T22:30Z")
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        row = db.execute("SELECT body FROM events WHERE profile='day' AND key=?",
                         (f"decision:{first.isoformat()}",)).fetchone()
        evidence = json.loads(row[0]) if row else None
    if evidence is None:
        raise ValueError("The first Day shadow decision is absent from the live evidence store")
    first_open = evidence["modeled_fill_at"]
    assert utc(evidence["decision_recorded_at"]) < utc(first_open)
    assert evidence["intent"] is None and evidence["baseline_signal"] is False
    funnel.to_parquet(destination / "day-population-membership.parquet")
    return {
        "utc_interval_inclusive_start_exclusive_end": [left, right], "original_day_run": original.name,
        "phase2_audit": AUDIT_ID, "profile_hash": profile.identity, "model_version": manifest["model"]["model_version"],
        "source_snapshot_sha256": manifest["source_snapshot"]["sha256"],
        "scored_setups": len(scored), "evaluable_label_ids": len(evaluation), "matched_ids": len(scored.index.intersection(evaluation.index)),
        "scored_without_evaluable_label": len(excluded), "unscored_evaluable_labels": len(evaluation.index.difference(scored.index)),
        "exclusion_reasons_on_differing_ids": excluded.status.value_counts().to_dict(),
        "purged_at_evaluation_end": stats["purged_at_end"], "gap_exclusions_in_difference": int(excluded.status.eq("execution_gap").sum()),
        "baseline_fills": int(funnel.baseline_fill.sum()), "positive_scores": int(funnel.score_accepted.sum()),
        "candidate_fills": int(funnel.candidate_fill.sum()),
        "first_shadow_decision": {
            "decision_close": evidence["decision_close"], "recorded_at": evidence["decision_recorded_at"],
            "next_execution_opening": first_open, "baseline_signal": evidence["baseline_signal"],
            "session_entry_allowed": session_entry_allowed(profile, evidence["decision_close"], first_open),
            "permitted_entry_fill": None, "reason": evidence["decision_outcome"],
        },
        "session": profile.session.model_dump(), "live_effect": "Existing runner already enforces the entry gate. "
        "Phase 3 exposes independent gates from pinned policy without changing the worker or rewriting its events.",
    }


def interruptions(now: pd.Timestamp) -> list[str]:
    entries = []
    for path in [LOG_PATH.with_name(LOG_PATH.name + f".{i}") for i in range(4, 0, -1)] + [LOG_PATH]:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not re.search("Worker (starting|stopped|failed|cycle_complete)|reconnect|temporarily blocked", line):
                continue
            if len(line) > 23 and utc(line[:23].replace(",", ".")) >= now - pd.Timedelta("24h"):
                entries.append(line)
    return entries[-30:]


def health() -> dict:
    worker = status()
    now = utc_now()
    result = {
        "observed_at_utc": now, "worker_pid": worker.get("pid"), "process_alive": worker["process_alive"],
        "runner_stale": worker["runner_stale"], "state": worker["state"], "worker_started_at_utc": worker.get("started_at"),
        "current_worker_elapsed_seconds": (now - utc(worker["started_at"])).total_seconds()
        if worker["process_alive"] and worker.get("started_at") else None,
        "heartbeat": worker.get("heartbeat"), "worker_error": worker.get("last_error"), "collector_log": worker["log"],
        "recent_logged_starts_stops_and_interruptions": interruptions(now), "experimental_shadow": worker.get("shadow", {}).get("state"),
        "approved_lane": "No validated model", "database_read": "SQLite read-only transaction",
        "scope": "A 24-hour candle receipt tally and current heartbeat. Receipt counts cannot prove uninterrupted WebSocket "
                 "uptime. Current process uptime excludes older restarts. Reboot/logoff persistence unverified.",
    }
    if not store.DATABASE.exists():
        return result
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        experiment = store.get(db, "experiment")
        profiles = {key: Profile(**item[1]["profile"]) for key, item in frozen_runs().items()}
        statuses = store.feed_status(db)
        result["feeds"] = {}
        for seconds in (300, 3600):
            step = pd.Timedelta(seconds=seconds)
            end = (now - GRACE).floor(step)
            start = end - pd.Timedelta("24h")
            candle_receipts = db.execute("""
                SELECT c.open_epoch, min(r.received_at) AS receipt FROM candles c
                JOIN responses r ON c.batch=r.id
                WHERE c.seconds=? AND c.open_epoch>=? AND c.open_epoch<?
                GROUP BY c.open_epoch
            """, (seconds, store.epoch(start), store.epoch(end))).fetchall()
            lags = [(utc(row["receipt"]) - (store.from_epoch(row["open_epoch"]) + step)).total_seconds()
                    for row in candle_receipts]
            successful = db.execute(
                "SELECT received_at FROM responses WHERE seconds=? AND problem IS NULL ORDER BY id DESC LIMIT 1",
                (seconds,),
            ).fetchone()
            row = statuses[str(seconds)]
            result["feeds"][str(seconds)] = {
                **row, "last_successful_collection_utc": successful[0] if successful else None,
                "checkpoint_lag_seconds_vs_eligible_close": (end - row["scanned_through"]).total_seconds()
                if row["scanned_through"] else None, "last_complete_candle_age_seconds": (
                    now - row["last_observed_close"]).total_seconds() if row["last_observed_close"] else None,
                "window_24h_left_utc": start, "window_24h_right_utc": end,
                "expected_closed_candles_24h": 86400 // seconds, "observed_closed_candles_24h": len(lags),
                "received_within_120_seconds_of_close": sum(0 <= lag <= 120 for lag in lags),
                "missing_candles_24h": 86400 // seconds - len(lags),
            }
        if experiment:
            fields = ("version", "observation_start_utc", "profile_hashes", "model_versions")
            result["experiment_id"] = fingerprint({key: experiment[key] for key in fields})[:24]
            result["experiment"] = experiment
        result["accounts"] = {}
        for key, profile in profiles.items():
            state = store.get_account(db, key)
            if not state:
                continue
            last_event = store.events(db, key, limit=1, kind="decision")
            event = last_event[0] if last_event else None
            next_decision = utc(state["next_decision"])
            opening = nominal_open(profile, next_decision)
            result["accounts"][key] = {
                **{name: state.get(name) for name in ("cash", "position", "pending_entry", "unresolved", "realized_usd",
                                                       "fees_usd", "prospective_decision_count", "entry_fills")},
                "latest_decision": event,
                "latest_decision_latency_seconds": (utc(event["decision_recorded_at"]) - utc(event["decision_close"])).total_seconds()
                if event else None,
                "next_decision_close": next_decision, "decision_overdue_seconds": max(0, (now - next_decision).total_seconds()),
                "next_execution_opening": opening, "session_entry_allowed_at_next_opening": session_entry_allowed(
                    profile, next_decision, opening,
                ),
                "latest_decision_session_entry_allowed": session_entry_allowed(
                    profile, event["decision_close"], event["modeled_fill_at"],
                ) if event else None,
            }
    return result
