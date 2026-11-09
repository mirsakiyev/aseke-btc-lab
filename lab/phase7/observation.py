import json

import pandas as pd

from lab.config import ROOT, Profile, fingerprint
from lab.data import OHLCV, feed
from lab.phase2 import store
from lab.phase2.checkpoint import frozen_prices
from lab.phase2.collector import GRACE
from lab.phase2.shadow import WAIT_AFTER_CLOSE, nominal_open
from lab.phase3.observation import session_entry_allowed
from lab.phase5.observations import candle_receipts, decision_event
from lab.phase6.feature_audit import checked_runs, swing_availability
from lab.storage import file_hash, read_json, utc


def latest_diagnostic(db, profile, run, experiment, instrumentation, now):
    effective = utc(instrumentation["effective_at_utc"])
    rows = db.execute("""
        SELECT key, recorded_at, body FROM events WHERE profile=? AND kind='decision'
        AND recorded_at>=? AND recorded_at<=? ORDER BY id DESC
    """, (profile.id, effective.isoformat(), now.isoformat())).fetchall()
    state = store.get_account(db, profile.id)
    result = {"next_decision_utc": state["next_decision"], "new_decision_count": len(rows),
              "latest_saved_decision": (store.events(db, profile.id, limit=1, kind="decision") or [None])[0]}
    if not rows:
        return {**result, "verification": "pending_next_scheduled_decision"}
    row = rows[0]
    event = json.loads(row["body"])
    close, recorded = utc(event["decision_close"]), utc(row["recorded_at"])
    modeled_open = nominal_open(profile, close)
    stamps = event.get("observability") or {}
    record_effective = utc(stamps["effective_at_utc"]) if stamps.get("effective_at_utc") else None
    model = run["model"]
    checks = {
        "versions": all(stamps.get(key) == instrumentation[key] for key in
                        ("version", "shadow_code_sha256", "execution_policy_version")),
        "pinned_identity": (event["profile_hash"] == profile.identity and event["model_version"] == model["model_version"]
                            and event["model_sha256"] == model["artifact_sha256"]
                            and event["costs"] == profile.costs.model_dump()
                            and event["feature_version"] == profile.feature_version),
        "decision_key_and_time": (row["key"] == f"decision:{close.isoformat()}"
                                  and record_effective is not None and effective <= record_effective <= recorded
                                  and recorded == utc(event["decision_recorded_at"])
                                  and close + WAIT_AFTER_CLOSE <= recorded <= now
                                  and utc(event["modeled_fill_at"]) == modeled_open),
        "experiment_boundary": event["observation_start_utc"] == experiment["observation_start_utc"]
                               and not event["holdout_performance_revealed"],
    }
    intent = event.get("intent")
    outcome = event["decision_outcome"]
    session_open = session_entry_allowed(profile, close, modeled_open)
    if intent:
        proof = decision_event(db, profile.id, f"intent-proof:{close.isoformat()}")
        checks["intent_precommitted"] = bool(proof and proof["kind"] == "durable_intent_confirmed"
                                             and recorded <= utc(proof["recorded_at"]) < modeled_open)
        if outcome == "entry_intent":
            checks["entry_gates"] = (session_open and event["baseline_signal"] and event["feature_ready"]
                                     and event["score_decimal"] > model["entry_threshold"])
    else:
        checks["intent_precommitted"] = True
    load = event.get("input_load")
    receipts = None
    if load:
        source = run["source_snapshot"]
        identity = event["data"]
        boundary = utc(experiment["registered_holdout_end"])
        receipts = candle_receipts(db, profile, close, event, boundary)
        selected = db.execute("""
            SELECT c.id, c.open_epoch, r.received_at FROM candles c JOIN responses r ON c.batch=r.id
            WHERE c.seconds=? AND c.open_epoch>=? AND c.open_epoch<?
            AND c.id=(SELECT max(v.id) FROM candles v
                      WHERE v.seconds=c.seconds AND v.open_epoch=c.open_epoch AND v.id<=?) ORDER BY c.open_epoch
        """, (profile.seconds, store.epoch(boundary), store.epoch(close),
              identity["latest_forward_revision_id"])).fetchall()
        snapshot = feed(profile.seconds) / "snapshots" / f"{source['sha256']}.parquet"
        opened, finished, base = (utc(load[key]) for key in ("started_at", "finished_at", "cached_base_loaded_at"))
        checks["source_and_revisions"] = (
            source["sha256"] == file_hash(snapshot) == identity["historical_snapshot_sha256"]
            and identity["source"] == source["source"]
            and fingerprint([record["id"] for record in selected]) == identity["forward_revisions_sha256"]
            and max((record["id"] for record in selected), default=None) == identity["latest_forward_revision_id"]
            and utc(load["first_source_open"]) == utc(run["coverage"]["first_open"]))
        checks["load_and_cutoff"] = (
            close + WAIT_AFTER_CLOSE <= opened <= finished <= recorded
            and record_effective is not None and record_effective <= base <= finished
            and utc(load["last_source_close"]) == utc(identity["last_observed_close"]) <= close
            and all(store.from_epoch(record["open_epoch"]) + profile.step <= utc(record["received_at"]) <= opened
                    for record in selected)
            and (receipts is not None or outcome in ("missing_latest_closed_execution_bar", "incomplete_decision_bucket")))
        gates = event.get("feature_gates")
        checks["feature_gate_consistency"] = (
            gates["ridge_inputs_ready"] == event["feature_ready"]
            and set(gates["finite_context_by_frame"]) == {context.frame for context in profile.context}
            and (outcome != "baseline_warmup_or_context_gap" or not gates["baseline_ready"])
            and (outcome != "no_baseline_breakout_or_context_trend"
                 or (gates["baseline_ready"] and event["baseline_signal"] is False))) if gates else (
                     outcome in ("missing_latest_closed_execution_bar", "incomplete_decision_bucket"))
    else:
        checks["missed_open_with_no_intent"] = recorded >= modeled_open and intent is None
    if recorded >= modeled_open:
        checks["late_event_kept_out_of_prospective_sample"] = (
            event["observation_quality"] == "late_catch_up" and intent is None)
    else:
        checks["preopen_journal_quality"] = event["observation_quality"] == "prospective_before_modeled_open"
    return {
        **result, "verification": ("evidence_mismatch" if not all(checks.values()) else
                                   "verified_preopen" if recorded < modeled_open else "verified_late_no_new_intent"),
        "decision": event, "journal_recorded_at": recorded, "checks": checks, "decision_bucket_receipts": receipts,
        "session_allows_entry_at_open": session_open,
    }


def warmup(db, run, experiment, now):
    profile = Profile(**run["profile"])
    source = run["source_snapshot"]
    boundary = utc(experiment["registered_holdout_end"])
    through = (now - GRACE).floor(profile.decision)
    historical = frozen_prices(source)
    forward = store.bars(db, profile.seconds, boundary, through)
    candles = pd.concat([historical.loc[historical.close_at <= min(through, boundary), [*OHLCV, "close_at"]],
                         forward[[*OHLCV, "close_at"]]])
    evidence = swing_availability(candles, profile, through)
    if evidence["consecutive_daily_bars"] >= profile.context[0].slow:
        estimate = evidence["first_contiguous_daily_close"] + pd.Timedelta(days=profile.context[0].slow - 1)
    else:
        estimate = evidence["earliest_possible_ready_daily_close_utc"]
    return {
        **evidence, "read_as_of": now, "source": source["source"], "source_snapshot_sha256": source["sha256"],
        "latest_native_close_used": candles.close_at.max(), "latest_eligible_4h_grid_cutoff": through,
        "required_daily_bars": profile.context[0].slow, "daily_bars_remaining": max(
            0, profile.context[0].slow - evidence["consecutive_daily_bars"]),
        "conditional_readiness_utc": estimate, "conditional_readiness_toronto": estimate.tz_convert("America/Toronto"),
        "scope": "Price-only input readiness. The estimate assumes complete subsequent bars and a new scheduled decision.",
    }


def observations(directory, manifest, now):
    runs = checked_runs(manifest)
    restart = read_json(directory / "worker-restart.json")
    instrumentation = restart["instrumentation"]
    if file_hash(ROOT / "lab" / "phase2" / "shadow.py") != instrumentation["shadow_code_sha256"]:
        raise ValueError("Shadow implementation changed since Phase 6")
    for name, digest in manifest["config_sha256"].items():
        if file_hash(ROOT / name) != digest:
            raise ValueError(f"Pinned configuration changed: {name}")
    experiment = manifest["original_experiment"]
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        diagnostics = {key: latest_diagnostic(db, Profile(**run["profile"]), run, experiment, instrumentation, now)
                       for key, (_, run) in runs.items()}
        return {"observability_boundary": instrumentation, "profiles": diagnostics,
                "swing_warmup": warmup(db, runs["swing"][1], experiment, now)}
