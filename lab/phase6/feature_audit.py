import numpy as np
import pandas as pd

from lab.config import Profile, fingerprint
from lab.data import OHLCV, aggregate
from lab.phase2 import store
from lab.phase2.checkpoint import frozen_prices, frozen_runs
from lab.phase5.observations import candle_receipts, decision_event
from lab.storage import file_hash, read_json, utc, utc_now


def checked_runs(manifest):
    runs = frozen_runs()
    for key, (folder, record) in runs.items():
        if (file_hash(folder / "manifest.json") != manifest["shadow_profile_manifests"][key]
                or Profile(**record["profile"]).identity != manifest["original_experiment"]["profile_hashes"][key]
                or record["model"]["model_version"] != manifest["original_experiment"]["model_versions"][key]):
            raise ValueError(f"Frozen shadow identity changed for {key}")
    return runs


def input_view(db, source, boundary, close, identity):
    watermark = identity["latest_forward_revision_id"]
    revisions = db.execute("""
        SELECT c.open_epoch, c.open, c.high, c.low, c.close, c.volume, c.id, r.received_at
        FROM candles c JOIN responses r ON c.batch=r.id
        WHERE c.seconds=? AND c.open_epoch>=? AND c.open_epoch<?
        AND c.id = (SELECT max(v.id) FROM candles v
                    WHERE v.seconds=c.seconds AND v.open_epoch=c.open_epoch AND v.id<=?)
        ORDER BY c.open_epoch
    """, (source["seconds"], store.epoch(boundary), store.epoch(close), watermark)).fetchall()
    ids = [row["id"] for row in revisions]
    if (source["sha256"] != identity["historical_snapshot_sha256"] or source["source"] != identity["source"]
            or max(ids, default=None) != watermark or fingerprint(ids) != identity["forward_revisions_sha256"]):
        raise ValueError("Original blocked Swing input identity cannot be reproduced")
    base = frozen_prices(source)
    forward = pd.DataFrame([tuple(row)[:6] for row in revisions], columns=["open_epoch", *OHLCV])
    forward.index = pd.DatetimeIndex(pd.to_datetime(forward.pop("open_epoch"), unit="s", utc=True)).as_unit("ns")
    step = pd.Timedelta(seconds=source["seconds"])
    forward["close_at"] = forward.index + step
    candles = pd.concat([base.loc[base.close_at <= boundary, [*OHLCV, "close_at"]], forward])
    if not candles.index.is_unique or not candles.index.is_monotonic_increasing or candles.close_at.max() != close:
        raise ValueError("Reconstructed input does not match the blocked source cutoff")
    return candles, len(base.loc[base.close_at <= boundary]), max(utc(row["received_at"]) for row in revisions)


def trailing_complete(bars, close, step):
    if bars.empty or close not in bars.index:
        return bars.iloc[:0]
    tail = bars.loc[:close]
    gaps = np.flatnonzero(np.diff(tail.index.as_unit("ns").asi8) != step.value)
    return tail.iloc[gaps[-1] + 1:] if len(gaps) else tail


def swing_availability(candles, profile, close):
    context, = profile.context
    daily = aggregate(candles, context.frame, profile.step).set_index("close_at")
    day = pd.Timedelta(context.frame)
    context_cutoff = close.floor(day)
    tail = trailing_complete(daily, context_cutoff, day)
    required = context.slow
    earliest = context_cutoff + max(0, required - len(tail)) * day
    latest_missing_close = tail.index[0] - day if len(tail) else context_cutoff
    gap_left = latest_missing_close - day
    latest_gap = pd.date_range(gap_left, latest_missing_close, freq=profile.step, inclusive="left").difference(candles.index)
    bars = aggregate(candles, profile.decision, profile.step).set_index("close_at")
    recent = trailing_complete(bars, close, pd.Timedelta(profile.decision))
    prior = recent.iloc[:-1]
    prev_close = recent.close.shift()
    true_range = pd.concat([recent.high - recent.low, (recent.high - prev_close).abs(),
                            (recent.low - prev_close).abs()], axis=1).max(axis=1)
    stop = (float(recent.close.iloc[-1] - profile.atr_multiple * true_range.tail(profile.atr_period).mean())
            if len(recent) >= profile.atr_period else None)
    baseline = {
        "context_1D_spread": {"required_consecutive_completed_daily_bars": required, "available": len(tail),
                              "ready": len(tail) >= required},
        "prior_high": {"required_prior_4h_bars": profile.entry_range, "available": len(prior),
                       "ready": len(prior) >= profile.entry_range},
        "prior_low": {"required_prior_4h_bars": profile.exit_range, "available": len(prior),
                      "ready": len(prior) >= profile.exit_range},
        "atr_and_positive_stop": {"required_4h_bars": profile.atr_period, "available": len(recent), "stop": stop,
                                  "ready": stop is not None and stop > 0},
    }
    ridge = {
        "momentum": {"required_prior_4h_bars": profile.momentum_period, "available": len(prior),
                     "ready": len(prior) >= profile.momentum_period},
        "relative_volume": {"required_prior_4h_bars": profile.volume_period, "available": len(prior),
                            "ready": bool(len(prior) >= profile.volume_period
                                          and prior.volume.tail(profile.volume_period).mean() > 0)},
        "atr_fraction": {"ready": stop is not None}, "breakout_distance": {"ready": baseline["prior_high"]["ready"]},
        "context_1D_spread": baseline["context_1D_spread"],
    }
    return {"decision_bucket_complete": close in bars.index, "consecutive_4h_bars": len(recent),
            "latest_daily_cutoff": context_cutoff, "consecutive_daily_bars": len(tail),
            "first_contiguous_daily_close": tail.index[0] if len(tail) else None,
            "last_valid_daily_close": daily.index.max(), "latest_incomplete_daily_open": gap_left,
            "latest_incomplete_daily_close": latest_missing_close,
            "missing_native_opens_in_latest_incomplete_daily_bucket": latest_gap.tolist(),
            "earliest_possible_ready_daily_close_utc": earliest,
            "baseline_features": baseline, "ridge_inputs": ridge}


def diagnose(manifest, phase5):
    runs = checked_runs(manifest)
    experiment = manifest["original_experiment"]
    boundary = utc(experiment["registered_holdout_end"])
    old = read_json(phase5 / "prospective-observations.json")["profiles"]["swing"]
    blocked = next(row for row in old["event_evidence"] if row["reason"] == "baseline_warmup_or_context_gap")
    close = utc(blocked["decision_close_utc"])
    run = runs["swing"][1]
    profile = Profile(**run["profile"])
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        record = decision_event(db, "swing", f"decision:{close.isoformat()}")
        if (record["feature_ready"] is not False or record["decision_outcome"] != blocked["reason"]
                or record["profile_hash"] != profile.identity or record["model_sha256"] != run["model"]["artifact_sha256"]
                or record["model_version"] != experiment["model_versions"]["swing"]):
            raise ValueError("Phase 5 blocked event or model identity mismatch")
        audit_read_started = utc_now()
        candles, base_count, selected_through = input_view(db, run["source_snapshot"], boundary, close, record["data"])
        audit_read_finished = utc_now()
        receipts = candle_receipts(db, profile, close, record, boundary)
    read_started = close + pd.Timedelta(seconds=record["feature_input_delay_seconds"])
    recorded_at = utc(record["recorded_at"])
    if not receipts or not selected_through <= read_started <= recorded_at < utc(record["modeled_fill_at"]):
        raise ValueError("Blocked event has no verified pre-open data provenance")
    readiness = swing_availability(candles, profile, close)
    missing = [key for key, feature in readiness["baseline_features"].items() if not feature["ready"]]
    if missing != ["context_1D_spread"] or not readiness["decision_bucket_complete"]:
        raise ValueError(f"Unexpected blocker in preserved Swing decision: {missing}")
    accounts = {}
    for key, (_, run) in runs.items():
        p, m = Profile(**run["profile"]), run["model"]
        accounts[key] = {
            "profile_hash": p.identity, "profile_version": p.version, "strategy_version": p.strategy_version,
            "feature_version": p.feature_version, "strategy": "Frozen baseline range breakout with trend context",
            "source": run["source_snapshot"]["source"], "model": "Frozen Ridge net-return filter",
            "model_version": m["model_version"], "model_sha256": m["artifact_sha256"],
            "training_cutoff": m["training_cutoff"], "requires_score_strictly_above": m["entry_threshold"],
            "decision_frame": p.decision, "execution_frame": p.execution, "simulated_costs": p.costs.model_dump(),
            "long_only": True, "risk": p.risk, "allocation": p.allocation,
            "session": p.session.model_dump() if p.session else None, "max_hold_minutes": p.max_minutes,
            "execution_policy": "Closed inputs, pre-open durable intent, next eligible opening, stop/time/signal exits.",
        }
    return {
        "saved_decision": record, "frozen_live_accounts": accounts, "blocking_feature": missing[0],
        "cause": "Missing source hours reset the contiguous daily SMA lookback.",
        "loaded_source": {"first_open": candles.index.min(), "last_open": candles.index.max(),
                          "last_close": candles.close_at.max(), "native_bars": len(candles), "base_bars": base_count,
                          "forward_bars": len(candles) - base_count, "original_forward_bucket_receipts": receipts,
                          "selected_forward_receipts_through": selected_through, "original_load_started_at": read_started,
                          "original_load_finished_at": None, "original_load_finished_by": recorded_at,
                          "original_base_exact_load_timestamp": None, "audit_read_started_at": audit_read_started,
                          "audit_read_finished_at": audit_read_finished},
        **readiness,
        "status_at_frozen_endpoint": "BLOCKED", "repair": "Existing loader and complete-bar resampling behaved as specified.",
        "future_condition": "All remaining daily buckets must close without native gaps. A qualifying future decision must "
                            "also load timely inputs and satisfy the other baseline and Ridge gates.",
        "scope": "Verified saved input revisions. Read native prices only for prospective readiness and computed no holdout outcomes.",
    }
