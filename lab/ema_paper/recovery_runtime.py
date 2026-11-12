"""Recover chart context while retaining the original EMA execution path and clocks."""

from contextlib import closing
from dataclasses import replace
from pathlib import Path
import shutil
import sqlite3
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.ema_paper import catalog, recovery_protocol, synthetic_protocol, synthetic_runtime, worker
from lab.phase15 import paper
from lab.storage import utc, utc_now, writer_lock
from lab.strategy_lab import synthetic_btc
from lab.strategy_lab.evaluator import proposal
from lab.strategy_lab.market_data import Purpose


class _HoldForwardDecisions:
    """Keep the predecessor's seed and protective execution reads available."""

    def __init__(self, data):
        self._data = data

    def grant(self, grant_id):
        return self._data.grant(grant_id)

    def read(self, grant_id, book_id, purpose, kind, start, end, as_of, interval=None):
        if purpose == Purpose.FORWARD_DECISION:
            raise ValueError("Audited chart gap requires the future-only recovery gate")
        return self._data.read(grant_id, book_id, purpose, kind, start, end, as_of, interval)


def _recovered_decision(db, lane, record, operation, state, fixed, book, result, minute, at, now):
    """Use the original proposal, sizing, and journal path with verified repaired features."""
    published = None
    detail = state["source_protocol"]
    signal = proposal(result.frame, at, lane["spec"], data_hash=result.snapshot_id,
                      position_open=bool(book["pending"] or book["position"] or book["problem"]))
    setup = signal.pop("setup")
    receipt = result.evidence_available_at
    if setup is not None:
        _, processing = worker._clocks(lane)
        setup = replace(setup, available_at=max(at, receipt) + processing)
        proof = {"source_mode": synthetic_protocol.SOURCE_MODE, "execution_model": record["body"]["execution_model"],
                 "execution_model_id": record["body"]["execution_model_id"], "protocol_amendment_id": record["id"],
                 "recovery_amendment_id": operation["id"], "price_source": "KRAKEN_BTC_USD_SPOT",
                 "source_id": fixed["source_id"], "source_received_at": receipt.isoformat(),
                 "publication_end_exclusive": fixed["primary_end_exclusive"], "feature_snapshot_id": result.snapshot_id}
        model = synthetic_btc.load_model(record["body"]["execution_model"], record["body"]["execution_model_id"])
        plan, reason = synthetic_btc.make_plan(lane, setup, book["equity"], now, proof, model)
        if plan:
            published = paper.publish(db, book, {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": at, "plan": plan,
                                                 "direction": "LONG" if setup.direction == 1 else "SHORT",
                                                 "strategy_version": lane["strategy_version"], "exit_policy": setup.exit_policy}, now)
        else:
            signal.update(decision="NO_TRADE", reason=reason)
        try:
            detail["counterfactual_risk"] = synthetic_btc.counterfactual_plans(lane, setup, book["equity"], now, proof)
            stress = {}
            for name in ("double_cost", "adverse_funding", "combined_adverse", "usd_usdt_minus_3pct", "usd_usdt_plus_3pct"):
                scenario = synthetic_btc.model(lane["profile"], scenario=name)
                candidate, failure = synthetic_btc.make_plan(lane, setup, book["equity"], now, proof, scenario)
                stress[name] = {"model_id": scenario.id, "sizing": candidate.get("sizing") if candidate else None,
                                "reason": failure, "counterfactual_only": True}
            detail["stress_risk"] = stress
        except (ArithmeticError, ValueError) as exc:
            detail["counterfactual_risk"] = {"state": "UNAVAILABLE", "reason": str(exc)}
    if published:
        signal["idea_id"] = published["id"]
    state.update(state="ACTIVE", reason="Timely Kraken chart qualified after context recovery",
                 last_evaluated_at=at.isoformat(), last_signal={**signal, "observed_at": now.isoformat(),
                                                               "source_mode": synthetic_protocol.SOURCE_MODE})
    detail.update(current_indicators={key: signal["values"].get(key) for key in ("ema_fast", "ema_slow", "rsi", "atr", "volume_sma")},
                  latest_close_at=at.isoformat(), last_native_receipt_at=receipt.isoformat(),
                  last_execution_receipt_at=minute.received_at.isoformat())


def _book(db, original, record, operation, scopes, data, now, deadline):
    from lab.ema_paper import chart_recovery

    lane = original["body"]
    key = lane["book_id"]
    # This remains the sole execution-minute path, and runs before any chart repair.
    predecessor_data = _HoldForwardDecisions(data) if chart_recovery.has_recorded_gap(db, key) else data
    response = synthetic_runtime._cycle_book(db, original, record, scopes, predecessor_data, now, deadline)
    state = worker._record(db, "readiness", key)
    fixed = worker._record(db, "eligibility", key)
    book = paper.load(db, lane)
    step, processing = worker._clocks(lane)
    at = now.floor(step)
    detail = state.get("source_protocol", {})
    detail["chart_recovery_protocol"] = recovery_protocol.VERSION
    detail["chart_recovery_amendment_id"] = operation["id"]
    detail["protective_execution_state"] = "UNRESOLVED" if book["problem"] else (
        "MONITORING" if book["position"] else "PENDING_ENTRY" if book["pending"] else "FLAT")
    if book["problem"] and book["cursor"]:
        detail["unresolved_execution_minute"] = book["cursor"]
    eligible = fixed and utc(fixed["first_eligible_at"]) <= at < utc(fixed["primary_end_exclusive"])
    pending_start = not fixed and response["state"] == "WAITING_FOR_DATA"
    if ((eligible or pending_start) and at > utc(record["body"]["seed_end"])
            and response["state"] in {"WAITING_FOR_DATA", "BLOCKED_DATA", "ACTIVE"}
            and time.monotonic() < deadline
            and (response["state"] != "ACTIVE" or state.get("chart_recovery")
                 or not (state.get("last_signal") or {}).get("observed_at"))):
        try:
            policy, seed = synthetic_runtime._seed(db, record, lane, scopes, data, now)
            result = chart_recovery.recover(db, data, policy, seed, decision_at=at, as_of=now, check_deadline=deadline)
            recovery_detail = {**result.detail, "state": result.state, "has_discontinuity": result.has_discontinuity}
            state["chart_recovery"] = recovery_detail
            detail["chart_recovery"] = recovery_detail
            already_evaluated = (state.get("last_evaluated_at") == at.isoformat()
                                 and (state.get("last_signal") or {}).get("observed_at"))
            if result.has_discontinuity and not already_evaluated and not book["problem"]:
                state.update(state="WAITING_FOR_DATA", reason=result.reason)
                if result.resume_at:
                    state["next_evaluation_at"] = (max(result.resume_at, at if result.state == "READY" else at + step)
                                                   + processing).isoformat()
                # A paper boundary finalized by the predecessor, or elapsed before this amendment, stays closed.
                if (eligible and result.state == "READY" and state.get("last_evaluated_at") != at.isoformat()
                        and at > utc(operation["body"]["recorded_at"])):
                    grant = scopes[Purpose.EXECUTION_EVIDENCE]
                    _, minute = synthetic_runtime._minute_bridge(data, grant, lane, now, seed.end)
                    if now >= max(at, minute.available_at, result.evidence_available_at) + processing:
                        _recovered_decision(db, lane, record, operation, state, fixed, book, result, minute, at, now)
                    else:
                        state["reason"] = "Waiting for the processing allowance after protective receipt availability"
        except (ValueError, OSError, LookupError) as exc:
            if response["state"] == "WAITING_FOR_DATA":
                state["reason"] = f"Chart recovery waiting: {exc}"
    detail.update(state=state["state"], reason=state.get("reason"), next_evaluation_at=state.get("next_evaluation_at"))
    state["source_protocol"] = detail
    worker._save_status(db, key, state)
    paper.save(db, book)
    return {name: state.get(name) for name in ("state", "reason", "source_protocol", "first_eligible_at", "next_evaluation_at")}


def cycle(*, now=None, root=None, protocol_root=None, source_root=None):
    from lab.ema_paper import chart_recovery, kraken_reference, recovery_source

    root = catalog.ROOT if root is None else Path(root)
    now = utc_now() if now is None else utc(now)
    saved = synthetic_protocol.load(root=protocol_root, now=now)
    operation = recovery_protocol.load(saved, root=protocol_root, now=now)
    grants = synthetic_protocol.grants(saved)
    source_root = kraken_reference.ROOT if source_root is None else Path(source_root)
    data = kraken_reference.market_data(grants, root=source_root)
    process, limits = psutil.Process(), catalog.LIMITS
    unit = synthetic_runtime.MIB
    if (psutil.virtual_memory().available < limits["min_available_mib"] * unit
            or shutil.disk_usage(root).free < limits["min_free_disk_mib"] * unit
            or (root / "paper.sqlite3").stat().st_size >= limits["max_ledger_mib"] * unit
            or process.memory_info().rss >= limits["worker_rss_mib"] * unit):
        return {"state": "WAITING_FOR_RESOURCES", "synthetic": True}
    started = time.monotonic()
    deadline = started + limits["cycle_seconds"]
    with writer_lock(root / ".cycle.lock"), closing(catalog.db_open(root=root)) as db, threadpool_limits(limits=1):
        db.executescript(synthetic_runtime.SCHEMA)
        chart_recovery.install_schema(db)
        results = {}
        for profile, original in catalog.registrations(db).items():
            if time.monotonic() >= deadline:
                results[profile] = {"state": "WAITING_FOR_RESOURCES"}
                continue
            record = saved["body"]["books"][profile]
            scopes = {grant.purpose: grant for grant in grants if grant.book_id == original["body"]["book_id"]}
            with db:
                results[profile] = _book(db, original, record, operation, scopes, data, now, deadline)
    repairs = None
    if time.monotonic() + 8 < deadline:
        repairs = recovery_source.repair_pending(results, grants, root=source_root, now=now)
    return {"state": "CHECKED", "synthetic": True, "revision_id": saved["id"], "recovery_amendment_id": operation["id"],
            "books": results, "recovery_source": repairs, "wall_seconds": round(time.monotonic() - started, 3),
            "rss_mib": round(process.memory_info().rss / unit, 1)}
