import json

import pandas as pd

from lab.config import Profile
from lab.phase2 import store
from lab.phase2.audit import distribution
from lab.phase2.checkpoint import frozen_runs
from lab.phase2.shadow import WAIT_AFTER_CLOSE, decision_grid, nominal_open
from lab.phase3.observation import health, session_entry_allowed
from lab.phase4.config import load_config
from lab.phase4.protocol import readiness
from lab.phase5.design import LABEL
from lab.storage import utc, utc_now

BLOCKS = {
    "missing_latest_closed_execution_bar", "incomplete_decision_bucket", "baseline_warmup_or_context_gap",
    "missing_model_features", "public_heartbeat_stale",
}


def scheduled(profile: Profile, start: pd.Timestamp, end: pd.Timestamp):
    grid = decision_grid(start, profile.decision)
    step = pd.Timedelta(profile.decision)
    return pd.date_range(grid if grid >= start else grid + step, end, freq=step)


def timestamp(value) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        parsed = utc(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return None if pd.isna(parsed) else parsed


def candle_receipts(db, profile: Profile, close: pd.Timestamp, record: dict, holdout_end: pd.Timestamp) -> dict | None:
    data = record.get("data") or {}
    watermark = data.get("latest_forward_revision_id")
    start = close - pd.Timedelta(profile.decision)
    if type(watermark) is not int or start < holdout_end or timestamp(data.get("last_observed_close")) != close:
        return None
    versions = db.execute("""
        SELECT c.open_epoch, c.id, r.received_at FROM candles c JOIN responses r ON c.batch = r.id
        WHERE c.seconds=? AND c.open_epoch>=? AND c.open_epoch<? AND c.id<=?
        ORDER BY c.open_epoch, c.id
    """, (profile.seconds, store.epoch(start), store.epoch(close), watermark)).fetchall()
    inputs = {}
    for row in versions:
        received = timestamp(row["received_at"])
        if received is None or received < store.from_epoch(row["open_epoch"]) + profile.step:
            return None
        inputs.setdefault(row["open_epoch"], []).append(received)
    expected = set(range(store.epoch(start), store.epoch(close), profile.seconds))
    if set(inputs) != expected:
        return None
    return {
        "first_locally_available_at": max(min(receipts) for receipts in inputs.values()),
        "selected_revisions_received_by": max(receipts[-1] for receipts in inputs.values()),
        "latest_native_bar_first_received_at": min(inputs[store.epoch(close) - profile.seconds]),
        "native_inputs": len(inputs), "as_of_revision_watermark": watermark,
    }


def classify(profile: Profile, close: pd.Timestamp, now: pd.Timestamp, record: dict | None,
             receipts: dict | None = None, proof: dict | None = None) -> dict:
    opening = nominal_open(profile, close)
    earliest = close + max(WAIT_AFTER_CLOSE, pd.Timedelta(seconds=profile.costs.processing_seconds))
    row = {
        "decision_close_utc": close, "modeled_execution_open_utc": opening,
        "worker_earliest_processing_utc": earliest,
        "session_allows_entry_at_open": session_entry_allowed(profile, close, opening),
        "decision_recorded_at": None, "decision_lag_seconds": None, "reason": None,
        "data_blocked": False, "no_signal": False, "ready_features": False, "execution_intent_evidence": "none",
    }
    if record is None:
        return {**row, "classification": "pending_deadline" if now < opening else "missing"}
    recorded, journal_at = (timestamp(record.get(key)) for key in ("decision_recorded_at", "recorded_at"))
    reason = record.get("decision_outcome")
    row.update(decision_recorded_at=recorded, reason=reason, data_blocked=reason in BLOCKS,
               no_signal=reason == "no_baseline_breakout_or_context_trend", ready_features=record.get("feature_ready") is True)
    if record.get("intent") is not None:
        ack = timestamp(proof.get("recorded_at")) if proof else None
        durable = bool(ack is not None and proof["kind"] == "durable_intent_confirmed" and recorded is not None
                       and recorded <= ack < opening)
        row["execution_intent_evidence"] = "durable_before_open" if durable else "unconfirmed_or_late"
    if (recorded is None or recorded != journal_at or timestamp(record.get("modeled_fill_at")) != opening
            or recorded < earliest or recorded > now or record.get("identity_mismatch")):
        return {**row, "classification": "unknown_timing"}
    row["decision_lag_seconds"] = (recorded - close).total_seconds()
    delay = record.get("feature_input_delay_seconds")
    began = close + pd.Timedelta(seconds=delay) if isinstance(delay, (float, int)) and delay >= 0 else None
    if receipts:
        row.update(receipts)
        row["first_local_availability_lag_seconds"] = (receipts["first_locally_available_at"] - close).total_seconds()
    ordered = bool(receipts and began is not None and earliest <= began <= recorded
                   and receipts["first_locally_available_at"] <= receipts["selected_revisions_received_by"] <= began)
    if began is not None:
        row["read_started_at"] = began
    if recorded >= opening:
        if ordered and began < opening:
            return {**row, "classification": "late_causal_computation"}
        if ((began is not None and began >= opening) or reason in ("late_catch_up_no_intent", "late_signal_exit_no_intent")):
            return {**row, "classification": "late_catchup_reconstruction"}
        return {**row, "classification": "late_unknown_input_timing"}
    if not ordered or record.get("observation_quality") not in (None, "prospective_before_modeled_open"):
        return {**row, "classification": "unknown_timing"}
    return {**row, "classification": "timely_before_modeled_open"}


def decision_event(db, key: str, event_key: str) -> dict | None:
    row = db.execute("SELECT kind, recorded_at, body FROM events WHERE profile=? AND key=?", (key, event_key)).fetchone()
    if not row:
        return None
    return {**json.loads(row["body"]), "kind": row["kind"], "recorded_at": row["recorded_at"]}


def profile_observations(db, profile, activation, boundary, lower, now, experiment) -> dict:
    active = max(activation, boundary, lower)
    events = []
    for close in scheduled(profile, active, now):
        record = decision_event(db, profile.id, f"decision:{close.isoformat()}")
        evidence, proof = None, None
        if record and (recorded := timestamp(record["recorded_at"])) is not None and recorded > now:
            record = None  # A concurrent append after the report timestamp is outside this snapshot.
        if record:
            record["identity_mismatch"] = (
                record.get("profile_hash") != experiment["profile_hashes"][profile.id]
                or record.get("model_version") != experiment["model_versions"][profile.id]
                or timestamp(record.get("observation_start_utc")) != activation
            )
            evidence = candle_receipts(db, profile, close, record, boundary)
            if record.get("intent") is not None:
                proof = decision_event(db, profile.id, f"intent-proof:{close.isoformat()}")
                if proof and (confirmed := timestamp(proof["recorded_at"])) is not None and confirmed > now:
                    proof = None
        events.append(classify(profile, close, now, record, evidence, proof))
    count = pd.Series([r["classification"] for r in events], dtype=str).value_counts()
    pending = int(count.get("pending_deadline", 0))
    timely = int(count.get("timely_before_modeled_open", 0))
    late_causal = int(count.get("late_causal_computation", 0))
    late_catchup = int(count.get("late_catchup_reconstruction", 0))
    late_unknown = int(count.get("late_unknown_input_timing", 0))
    denominator = len(events) - pending
    account = store.get_account(db, profile.id)
    return {
        "active_from_utc": active, "decision_frame": profile.decision, "execution_frame": profile.execution,
        "scheduled_closed_opportunities": len(events),
        "preactivation_or_preholdout_opportunities_excluded": sum(at < active for at in scheduled(profile, lower, now)),
        "pending_before_cutoff": pending, "assessable_opportunities": denominator,
        "timely_verified_records": timely, "timely_record_coverage_fraction": timely / denominator if denominator else None,
        "timely_feature_ready_records": sum(r["classification"] == "timely_before_modeled_open" and r["ready_features"]
                                          and not r["data_blocked"] for r in events),
        "late_total": late_causal + late_catchup + late_unknown,
        "late_causally_started_but_missed_cutoff": late_causal, "late_catchup_reconstruction": late_catchup,
        "late_input_timing_unknown": late_unknown, "missing": int(count.get("missing", 0)),
        "unknown_timing_excluding_late": int(count.get("unknown_timing", 0)),
        "data_blocked_records": sum(r["data_blocked"] for r in events),
        "no_baseline_signal_records": sum(r["no_signal"] for r in events),
        "session_ineligible_assessable_opportunities": sum(
            not r["session_allows_entry_at_open"] and r["classification"] != "pending_deadline" for r in events
        ),
        "durably_precommitted_intents": sum(r["execution_intent_evidence"] == "durable_before_open" for r in events),
        "decision_lag_seconds_from_close": distribution(pd.Series(
            [r["decision_lag_seconds"] for r in events if r["decision_lag_seconds"] is not None], dtype=float
        )),
        "first_local_availability_lag_seconds": distribution(pd.Series(
            [r["first_local_availability_lag_seconds"] for r in events if "first_local_availability_lag_seconds" in r], dtype=float
        )),
        "account_lifetime": {key: account.get(key) for key in ("entry_fills", "closed_trades",
                              "reconstructed_or_uncertain_trades", "cash", "position", "pending_entry", "unresolved")},
        "event_evidence": events,
    }


def prospective_report() -> dict:
    current = health()
    now = utc_now()
    lower = now - pd.Timedelta("24h")
    data = {
        "label": LABEL, "observed_at_utc": now, "lookback_start_utc": lower, "worker_health": current,
        "mexc": readiness(load_config()), "approved_lane": "No validated model",
        "timing_rules": {
            "primary_timely": "Decision journal, closed native bucket receipts as of its stored revision watermark, "
                              "and feature read start agree and precede the original modeled opening.",
            "pending": "Closed opportunity with no record and modeled opening still in the future. "
                       "Excluded from the assessable denominator until recorded or due.",
            "late": "Timestamp at or after the original modeled opening. Separate computations started before "
                    "the cutoff, catch-up/reconstruction, and missing input proof.",
            "blocked_and_ineligible": "Orthogonal to timing. The strategy can record a timely data block or no-signal, "
                                     "and Day still expects decisions when the session forbids entries.",
            "receipt": "First local complete-bucket availability is the maximum of its native bars' first receipts. "
                       "Selected revision receipts use the original response timestamps. Exchange publication and "
                       "exact commit times are unavailable. No candle timestamp alone proves arrival.",
            "latency_sla": "None registered. The 40-second minimum wait and 30-second processing allowance are "
                           "earliest scheduling gates. The feed health 120-second receipt tally is descriptive. "
                           "The 90-second fill observation grace is a separate rule.",
            "trade_evidence": "A pre-open decision is not a fill. Intents require a pre-open durable confirmation. "
                              "Retrospective records and unresolved or reconstructed fills are excluded from "
                              "prospective-performance claims.",
        },
    }
    if not store.DATABASE.exists():
        return {**data, "status": "no_observation_store", "profiles": {}}
    runs = frozen_runs()
    data["historical_availability"] = {
        key: {**manifest["source_snapshot"],
              "coverage": {field: manifest["coverage"][field] for field in ("first_open", "last_close", "internal_gap_bars")}}
        for key, (_, manifest) in runs.items()
    }
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        experiment = store.get(db, "experiment")
        if not experiment:
            return {**data, "status": "no_active_experiment", "profiles": {}}
        activation, boundary = (utc(experiment[key]) for key in ("observation_start_utc", "registered_holdout_end"))
        data["continuity"] = {"experiment": experiment, "experiment_id": current.get("experiment_id"),
                              "since_original_activation_seconds": (now - activation).total_seconds(),
                              "current_process_elapsed_seconds": current["current_worker_elapsed_seconds"]}
        data["profiles"] = {}
        for key, (_, manifest) in runs.items():
            profile = Profile(**manifest["profile"])
            if profile.identity != experiment["profile_hashes"][key]:
                raise ValueError("Cannot reconstruct the schedule for a changed profile identity")
            data["profiles"][key] = profile_observations(db, profile, activation, boundary, lower, now, experiment)
    return data
