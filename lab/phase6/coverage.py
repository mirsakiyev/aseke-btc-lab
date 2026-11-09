from collections import Counter
import json

import pandas as pd

from lab.config import Profile
from lab.phase2 import store
from lab.phase2.audit import distribution
from lab.phase2.collector import GRACE, status
from lab.phase3.observation import health
from lab.phase5.observations import decision_event, profile_observations, timestamp
from lab.phase6.design import RECEIPT_HORIZON
from lab.phase6.feature_audit import checked_runs
from lab.storage import utc, utc_now

FEEDS = (300, 3600)
ARRIVALS = ("timely", "late", "catch_up", "missing", "unknown")
INPUT_BLOCKS = {"missing_latest_closed_execution_bar", "incomplete_decision_bucket",
                "baseline_warmup_or_context_gap", "missing_model_features"}


def first_receipts(db, seconds, first_close, last_close):
    step = pd.Timedelta(seconds=seconds)
    rows = db.execute("""
        SELECT c.open_epoch, c.id, r.id AS batch, r.received_at, r.start_open, r.end_open, r.problem
        FROM candles c JOIN responses r ON c.batch=r.id
        WHERE c.seconds=? AND c.open_epoch>=? AND c.open_epoch<?
        AND c.id=(SELECT min(v.id) FROM candles v WHERE v.seconds=c.seconds AND v.open_epoch=c.open_epoch)
        ORDER BY c.open_epoch
    """, (seconds, store.epoch(first_close - step), store.epoch(last_close))).fetchall()
    return {store.from_epoch(row["open_epoch"]) + step: dict(row) for row in rows}


def arrival(close, now, instrumentation_start, receipt, seconds):
    received = timestamp(receipt["received_at"]) if receipt else None
    if instrumentation_start is None or close < instrumentation_start:
        return "unknown", None
    if receipt is None or (received is not None and received > now):
        return "missing", None
    if received is None or received < close or receipt["problem"] is not None:
        return "unknown", None
    lag = (received - close).total_seconds()
    if received <= close + RECEIPT_HORIZON:
        return "timely", lag
    # A routine collection advances one candle with two overlapping candles.
    # A wider saved request proves a multi-interval scan. Narrow late requests
    # stay late because their shape alone cannot identify catch-up.
    return ("catch_up" if receipt["end_open"] - receipt["start_open"] > 3 * seconds else "late"), lag


def active_candles(db, seconds, activation, now):
    step = pd.Timedelta(seconds=seconds)
    start = activation.ceil(step)
    end = (now - RECEIPT_HORIZON).floor(step)
    closes = pd.date_range(start, end, freq=step)
    first = db.execute("""
        SELECT r.received_at FROM responses r WHERE r.seconds=? AND r.problem IS NULL
        AND EXISTS (SELECT 1 FROM candles c WHERE c.seconds=r.seconds AND c.batch=r.id)
        ORDER BY r.id LIMIT 1
    """, (seconds,)).fetchone()
    instrumented = timestamp(first["received_at"]) if first else None
    if instrumented is not None and instrumented > now:
        instrumented = None
    receipts = first_receipts(db, seconds, start, end) if len(closes) else {}
    counts = Counter()
    delays = []
    exceptions = []
    for close in closes:
        receipt = receipts.get(close)
        category, delay = arrival(close, now, instrumented, receipt, seconds)
        counts[category] += 1
        if delay is not None:
            delays.append(delay)
        if category != "timely" and close >= now - pd.Timedelta("24h"):
            receipt_at = timestamp(receipt["received_at"]) if receipt else None
            exceptions.append({"close": close, "classification": category, "first_receipt_delay_seconds": delay,
                               "first_receipt": receipt if receipt_at is None or receipt_at <= now else None})
    return {
        "receipt_instrumentation_start_utc": instrumented, "first_active_close_utc": start,
        "last_deadline_elapsed_close_utc": end if len(closes) else None, "denominator": len(closes),
        "closed_pending_deadline": len(pd.date_range(max(start, end + step), now, freq=step)),
        **{key: counts[key] for key in ARRIVALS}, "receipt_delay_seconds": distribution(pd.Series(delays, dtype=float)),
        "last_24h_exception_evidence": exceptions,
    }


def historical_candles(db, seconds, activation, now):
    step = pd.Timedelta(seconds=seconds)
    end = (now - GRACE).floor(step)
    start = end - pd.Timedelta("24h") + step
    closes = pd.date_range(start, end, freq=step)
    rows = first_receipts(db, seconds, start, end)
    receipts = [(close, timestamp(row["received_at"])) for close, row in rows.items()]
    present = [(close, receipt) for close, receipt in receipts if receipt is None or receipt <= now]
    return {"first_close": start, "last_close": end, "expected": len(closes), "present": len(present),
            "missing": len(closes) - len(present), "pre_activation_closes": int((closes < activation).sum()),
            "within_120_seconds": sum(receipt is not None and close <= receipt <= close + RECEIPT_HORIZON
                                      for close, receipt in present)}


def decision_dimensions(profile_data, entry_fills):
    timing, readiness, outcomes, reasons = Counter(), Counter(), Counter(), Counter()
    timely_and_ready = 0
    for event in profile_data["event_evidence"]:
        kind = event["classification"]
        if kind == "pending_deadline":
            continue
        timing[{"timely_before_modeled_open": "timely", "missing": "missing", "unknown_timing": "unknown"}.get(kind, "late")] += 1
        reason = event["reason"]
        reasons[reason or "no_recorded_outcome"] += 1
        readiness["complete" if event["ready_features"] else "blocked" if reason in INPUT_BLOCKS else "unknown"] += 1
        timely_and_ready += kind == "timely_before_modeled_open" and event["ready_features"]
        if event["decision_close_utc"] in entry_fills:
            outcome = "simulated_entry"
        elif event["execution_intent_evidence"] == "durable_before_open":
            outcome = "precommitted_intent_recorded"
        elif event["execution_intent_evidence"] == "unconfirmed_or_late":
            outcome = "unconfirmed_or_late_intent"
        elif not event["session_allows_entry_at_open"] and reason in ("entry_cutoff", "no_baseline_breakout_or_context_trend"):
            outcome = "session_ineligible"
        elif event["no_signal"]:
            outcome = "no_signal"
        elif reason in ("nonpositive_net_score", "deadline_infeasible", "position_occupied_or_order_pending"):
            outcome = "signal_gated"
        elif event["data_blocked"]:
            outcome = "feature_or_data_blocked"
        else:
            outcome = "unknown_or_other"
        outcomes[outcome] += 1
    return {
        "assessable": profile_data["assessable_opportunities"], "pending_deadline": profile_data["pending_before_cutoff"],
        "timing": {key: timing[key] for key in ("timely", "late", "missing", "unknown")},
        "readiness": {key: readiness[key] for key in ("complete", "blocked", "unknown")},
        "timely_and_ready": timely_and_ready,
        "outcomes": dict(outcomes), "recorded_reasons": dict(reasons),
        "late_breakdown": {key: profile_data[key] for key in ("late_causally_started_but_missed_cutoff",
                                                              "late_catchup_reconstruction", "late_input_timing_unknown")},
        "decision_lag_seconds_from_close": profile_data["decision_lag_seconds_from_close"],
        "precommitted_intents": profile_data["durably_precommitted_intents"],
        "event_evidence": profile_data["event_evidence"],
    }


def snapshot(manifest, now, *, decision_days=1):
    runs = checked_runs(manifest)
    experiment = manifest["original_experiment"]
    activation, boundary = (utc(experiment[key]) for key in ("observation_start_utc", "registered_holdout_end"))
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        current = store.get(db, "experiment")
        if any(current.get(key) != experiment[key] for key in
               ("observation_start_utc", "profile_hashes", "model_versions", "registered_holdout_end", "version")):
            raise ValueError("The active observer no longer matches the frozen cohort")
        profiles = {}
        for key, (_, run) in runs.items():
            profile = Profile(**run["profile"])
            rows = profile_observations(db, profile, activation, boundary, now - pd.Timedelta(days=decision_days), now,
                                        experiment)
            records = db.execute("""
                SELECT kind, body FROM events WHERE profile=? AND recorded_at>=? AND recorded_at<=?
                AND kind IN ('entry_fill','exit_fill')
            """, (key, activation.isoformat(), now.isoformat())).fetchall()
            fills = [(row["kind"], json.loads(row["body"])) for row in records]
            quality = Counter(body.get("observation_quality", "unknown") for _, body in fills)
            entry_fills = {timestamp(body["intent"]["decision_close"]) for kind, body in fills if kind == "entry_fill"}
            saved = next((event for event in reversed(rows["event_evidence"]) if event["decision_recorded_at"]), None)
            latest = decision_event(db, key, f"decision:{saved['decision_close_utc'].isoformat()}") if saved else None
            profiles[key] = {"schedule": rows["decision_frame"], "execution": rows["execution_frame"],
                             "from_utc": rows["active_from_utc"], **decision_dimensions(rows, entry_fills),
                             "execution_events_since_activation": len(fills),
                             "execution_quality_since_activation": dict(quality), "latest_saved_decision": latest}
        return {
            "as_of_utc": now, "original_activation_utc": activation,
            "historical_completeness": {str(s): historical_candles(db, s, activation, now) for s in FEEDS},
            "historical_price_snapshot_coverage": {
                key: {field: run["coverage"][field] for field in ("first_open", "last_close", "internal_gap_bars")}
                for key, (_, run) in runs.items()},
            "active_first_receipts": {str(s): active_candles(db, s, activation, now) for s in FEEDS},
            "decisions": profiles, "decision_lookback_hours": 24 * decision_days,
            "definitions": {
                "historical": "Closed 24h candle grid ending at the latest collector-eligible close. Includes catch-up.",
                "arrival": "Original first durable revision per candle, using its response receipt. All post-activation "
                           "closes with elapsed close+120s horizons remain in the denominator, including downtime.",
                "unknown": "Closed before receipt instrumentation or first receipt cannot establish timing.",
                "catch_up": "Late first receipt in a provably wider request than one new interval plus two overlaps.",
                "receipt_horizon": "120 seconds is descriptive. Exchange publication and exact SQLite commit times are unknown.",
                "decision": "Pinned schedule and modeled-open cutoff. An early record with receipt proof can be "
                            "assessed before its deadline. Readiness and outcome are separate from timing.",
                "execution": "A durable intent fixed before the modeled opening can subsequently have its price "
                             "reconstructed. Reconstruction carries its original flag and provides no timely fill proof.",
            },
        }


def current_coverage(manifest, *, decision_days=1):
    coverage = snapshot(manifest, utc_now(), decision_days=decision_days)
    current = health()
    current["observation_instrumentation"] = status().get("shadow", {}).get("observability")
    coverage["current_worker_health"] = current
    return coverage
