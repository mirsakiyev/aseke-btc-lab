"""Run one bounded paper cycle against independently commissioned KuCoin evidence."""

from contextlib import closing
from dataclasses import asdict, replace
import json
from pathlib import Path
import shutil
import sqlite3
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.config import fingerprint
from lab.data import OHLCV
from lab.ema_paper import catalog, commissioning, ledger, proxy_protocol, proxy_signal, worker
from lab.execution import execution_open
from lab.phase15 import paper
from lab.storage import utc, utc_now, writer_lock
from lab.strategy_lab.cross_venue_warmup import evaluate_transition


MINUTE = pd.Timedelta(minutes=1)
MIB = 1024**2
CHARTS_PER_CYCLE = 12
SCHEMA = """
CREATE TABLE IF NOT EXISTS ema_native_chart (
    revision TEXT NOT NULL, close_at TEXT NOT NULL, digest TEXT NOT NULL, body TEXT NOT NULL,
    PRIMARY KEY(revision,close_at)
);
CREATE TRIGGER IF NOT EXISTS ema_native_chart_update BEFORE UPDATE ON ema_native_chart
    BEGIN SELECT RAISE(ABORT,'immutable EMA native chart evidence'); END;
CREATE TRIGGER IF NOT EXISTS ema_native_chart_delete BEFORE DELETE ON ema_native_chart
    BEGIN SELECT RAISE(ABORT,'immutable EMA native chart evidence'); END;
"""


def _chart_record(reader, at, now):
    batch, source = reader.read_chart(at, now)
    values = None
    if batch.complete:
        row = batch.bars.iloc[-1]
        values = {**{key: float(row[key]) for key in OHLCV}, "close_at": at.isoformat(),
                  "available_at": row.available_at.isoformat(), "received_at": row.received_at.isoformat(),
                  "receipt_manifest_sha256": row.receipt_manifest_sha256}
    return {"values": values, "content_id": source.content_id, "source_id": source.source.id,
            "grant_id": source.grant_id, "snapshot_id": source.id}


def _chart_history(db, revision, seed, reader, at, now, lane, deadline):
    step, _ = worker._clocks(lane)
    cursor = seed.end
    proof, candles, latest = [], [], None
    for close_at, digest, payload in db.execute(
        "SELECT close_at,digest,body FROM ema_native_chart WHERE revision=? AND close_at<=? ORDER BY close_at",
        (revision, at.isoformat()),
    ):
        close = utc(close_at)
        current = json.loads(payload)
        if close != cursor + step or fingerprint([revision, close_at, current]) != digest:
            raise ValueError("The durable native chart cursor or its receipt digest failed verification")
        cursor, latest = close, current
        proof.append(digest)
        if current["values"] is not None:
            candles.append(current["values"])
    cutoff = pd.Timedelta(seconds=lane["source"]["decision_receipt_seconds"])
    for _ in range(CHARTS_PER_CYCLE):
        close = cursor + step
        if close > at or time.monotonic() >= deadline:
            break
        record = _chart_record(reader, close, now)
        if record["values"] is None and now < close + cutoff:
            break  # A lawful first receipt may still arrive for this exact chart.
        digest = fingerprint([revision, close.isoformat(), record])
        db.execute("INSERT INTO ema_native_chart VALUES (?,?,?,?)",
                   (revision, close.isoformat(), digest, json.dumps(record, sort_keys=True, allow_nan=False)))
        proof.append(digest)
        cursor, latest = close, record
        if record["values"] is not None:
            candles.append(record["values"])
    frame = None
    if candles:
        frame = pd.DataFrame(candles)
        for key in ("close_at", "available_at", "received_at"):
            frame[key] = pd.to_datetime(frame[key], utc=True)
        frame.index = pd.DatetimeIndex(frame.close_at - step, name="open_at")
    transition = evaluate_transition(seed, frame, as_of=now, forward_provider="KuCoin",
                                     forward_market=lane["market"], forward_instrument=lane["instrument"])
    return replace(transition, evidence={**transition.evidence, "chart_cursor": cursor.isoformat(),
                                         "receipt_prefix_sha256": fingerprint(proof)}), latest


def _advance(db, lane, book, fixed, reader, now, deadline):
    follow = utc(fixed["follow_end_exclusive"])
    for _ in range(lane["limits"]["minutes_per_book"]):
        if not book["cursor"] or book["permanent_block"] or time.monotonic() >= deadline:
            return
        at = utc(book["cursor"])
        if at + MINUTE > min(now, follow):
            if now >= follow:
                paper.problem(db, book, "Follow-through ended with unresolved execution evidence", at, now, permanent=True)
            return
        idea = book["position"] or book["pending"]
        proof = reader.execution_evidence(at, now, published_at=idea["published_at"])
        if proof["state"] != "READY":
            key = "funding_receipt_seconds" if proof.get("waiting_for_funding") else "price_receipt_seconds"
            if proof.get("permanent") or now > at + MINUTE + pd.Timedelta(seconds=lane["source"][key]):
                paper.problem(db, book, proof["reason"], at, now, permanent=bool(proof.get("permanent") or book["pending"]))
            return
        evidence = proof["evidence"]
        plan = idea["plan"]
        terms = plan["execution_terms"]
        if (evidence["source_id"] != fixed["source_id"] or evidence["execution_grant_id"] != fixed["execution_grant_id"]
                or asdict(proof["contract"]) != plan["contract"]
                or evidence["minimum_notional"] != terms["min_quote"] or evidence["maximum_quantity"] != terms["max_contracts"]):
            paper.problem(db, book, "Dated KuCoin execution source or terms changed", at, now, permanent=True)
            return
        if not ledger.advance(db, book, at, now, proof["trade"], proof["mark"], proof.get("funding", ())):
            return


def _readiness(state, amendment, seed, transition, latest):
    evidence = transition.evidence
    row = transition.frame.iloc[-1] if not transition.frame.empty else None
    current = None if row is None else {key: float(row[key]) if pd.notna(row[key]) else None
                                        for key in ("ema_fast", "ema_slow", "rsi", "atr")}
    native_count = int(row.contiguous_bars) if row is not None and latest and latest["values"] else 0
    source = {
        "protocol": proxy_protocol.PROTOCOL, "amendment_id": amendment["id"], "warmup_count": seed.count,
        "warmup_seed_id": seed.id, "archive_snapshot_id": seed.snapshot_id,
        "last_warmup_receipt_at": seed.available_at.isoformat(),
        "warmup_provider": "Kraken", "warmup_instrument": "BTC-USD", "forward_provider": "KuCoin",
        "forward_instrument": evidence["forward_instrument"], "source_permission": "Reviewed independent KuCoin-origin grants",
        "execution_permission": "Independent role-bound execution grant",
        "native_volume": {"complete": min(native_count, 20), "required": 20},
        "native_swing": {"complete": min(native_count, 5), "required": 5},
        "last_native_receipt_at": state.get("source_protocol", {}).get("last_native_receipt_at"),
        "transition_at": seed.end.isoformat(), "transition_warning": evidence["reason"],
        "price_basis": evidence["native_open_minus_kraken_close"], "current_indicators": current,
        "feature_mode": evidence["source_mode"], "source_mode": evidence["source_mode"],
        "readiness": {"proxy_feature_ready": evidence["proxy_feature_ready"],
                      "native_signal_ready": evidence["native_signal_ready"], "economic_execution_ready": False},
        "chart_cursor": evidence["chart_cursor"], "receipt_prefix_sha256": evidence["receipt_prefix_sha256"],
    }
    if latest and latest["values"]:
        source["last_native_receipt_at"] = latest["values"]["received_at"]
    state["source_protocol"] = source


def _decision(db, registration, amendment, seed, reader, book, state, fixed, at, now, deadline):
    lane = registration["body"]
    step, processing = worker._clocks(lane)
    transition, latest = _chart_history(db, amendment["id"], seed, reader, at, now, lane, deadline)
    _readiness(state, amendment, seed, transition, latest)
    evidence, detail = transition.evidence, state["source_protocol"]
    state.update(state="BLOCKED_DATA", reason=evidence["reason"])
    if not evidence["native_signal_ready"] or utc(evidence["chart_cursor"]) != at:
        return
    state.update(state="NATIVE_SIGNAL_READY", reason="Complete native signal windows")
    if now > at + pd.Timedelta(seconds=lane["source"]["publication_max_seconds"]):
        state.update(reason="Decision publication window elapsed")
        if fixed:
            state["last_evaluated_at"] = at.isoformat()
        return
    first = execution_open(now, step) if not fixed else utc(fixed["first_eligible_at"])
    end = first + pd.Timedelta(days=lane["horizon_days"])
    native = reader.publication_evidence(at, now, end)
    if native.decision_trade.content_id != latest["content_id"]:
        raise ValueError("Latest native decision receipts changed after the chart cursor advanced")
    available = max(native.available_at, utc(transition.frame.at[at, "available_at"])) + processing
    if now < available:
        state.update(reason="Processing allowance after actual source availability has not elapsed")
        return
    detail["readiness"]["economic_execution_ready"] = True
    detail["last_native_receipt_at"] = latest["values"]["received_at"]
    state.update(state="ECONOMIC_EXECUTION_READY", reason="Independent KuCoin execution evidence qualified")
    if not fixed:
        frozen = {"source_ready_at": now.isoformat(), "first_eligible_at": first.isoformat(),
                  "primary_end_exclusive": end.isoformat(),
                  "follow_end_exclusive": (end + pd.Timedelta(minutes=lane["follow_through_minutes"])).isoformat(),
                  "source_id": latest["source_id"], "revision_id": amendment["id"],
                  "execution_grant_id": native.trade.grant_id}
        db.execute("INSERT INTO eligibility VALUES (?,?)", (book["book_id"], paper.encode(frozen)))
        fixed.update(frozen)
    if at < first:
        state["next_evaluation_at"] = (first + processing).isoformat()
        return
    if fixed["source_id"] != latest["source_id"] or fixed["execution_grant_id"] != native.trade.grant_id:
        raise PermissionError("Current source grants differ from the frozen eligible source")
    signal = proxy_signal.inspect_decision(registration, amendment, transition, decision_at=at, as_of=now,
                                           ready_at=fixed["source_ready_at"], position_open=bool(book["pending"] or book["position"]))
    setup = signal.pop("setup")
    state.update(state="ACTIVE", reason="Qualified prospective KuCoin evaluation", last_evaluated_at=at.isoformat(),
                 last_signal={**signal, "observed_at": now.isoformat()})
    if not setup:
        return
    plan, reason = ledger.make_plan(lane, replace(setup, available_at=available), native.contract, book["equity"], now, {
        "source_mode": signal["source_mode"], "source_id": latest["source_id"],
        "source_received_at": latest["values"]["received_at"], "protocol_amendment_id": amendment["id"],
        "feature_evidence_id": transition.id, "chart_snapshot_id": native.decision_trade.id,
        "execution_trade_snapshot_id": native.trade.id, "contract_snapshot_id": native.dated_terms.id,
        "mark_snapshot_id": native.marks.id if native.marks else None,
        "funding_snapshot_id": native.settled_funding.id if native.settled_funding else None,
        "publication_end_exclusive": fixed["primary_end_exclusive"],
        "execution_terms": {"min_quote": native.minimum_notional, "max_contracts": native.maximum_quantity},
    })
    if plan:
        idea = paper.publish(db, book, {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": at, "plan": plan,
                                       "direction": "LONG" if setup.direction == 1 else "SHORT",
                                       "strategy_version": lane["strategy_version"], "exit_policy": setup.exit_policy}, now)
        if idea:
            state["last_signal"]["idea_id"] = idea["id"]
    else:
        state["last_signal"].update(decision="NO_TRADE", reason=reason)


def _run_book(db, registration, attachment, now, deadline):
    amendment, seed, reader = attachment
    lane = registration["body"]
    book_id = lane["book_id"]
    state = worker._record(db, "readiness", book_id)
    fixed = worker._record(db, "eligibility", book_id)
    book = paper.load(db, lane)
    step, processing = worker._clocks(lane)
    at = now.floor(step)
    state.update(last_checked_at=now.isoformat(), next_evaluation_at=(execution_open(at, step) + processing).isoformat())
    try:
        if fixed and fixed.get("revision_id") != amendment["id"]:
            raise PermissionError("Original ledger already has a different frozen eligibility protocol")
        if fixed:
            _advance(db, lane, book, fixed, reader, now, deadline)
        if fixed and at >= utc(fixed["primary_end_exclusive"]):
            closed = db.execute("SELECT count(*) FROM event WHERE book=? AND kind='CLOSED'", (book_id,)).fetchone()[0]
            complete = now >= utc(fixed["follow_end_exclusive"]) or not (book["pending"] or book["position"])
            assessment = "INSUFFICIENT_EVIDENCE" if closed < lane["min_complete_trades"] or book["problem"] else "REVIEW_REQUIRED"
            state.update(state="COMPLETE" if complete else "FOLLOW_THROUGH", reason="Frozen primary horizon ended",
                         next_evaluation_at=None, assessment=assessment)
        elif book["problem"]:
            state.update(state="BLOCKED_DATA", reason=book["problem"])
        elif at <= seed.end:
            state.update(state="PROXY_FEATURE_READY", reason="Waiting for the first closed native chart")
        elif state.get("last_evaluated_at") != at.isoformat() and time.monotonic() < deadline:
            _decision(db, registration, amendment, seed, reader, book, state, fixed, at, now, deadline)
    except PermissionError as exc:
        state.update(state="BLOCKED_PERMISSION", reason=str(exc))
    except (ValueError, LookupError, OSError, sqlite3.Error) as exc:
        state.update(state="BLOCKED_DATA", reason=str(exc))
    state.update(fixed)
    if "source_protocol" in state:
        state["source_protocol"].update(state=state["state"], first_eligible_at=state.get("first_eligible_at"),
                                        strategy_id=f"ema_pullback_trend_continuation/{lane['profile']}",
                                        execution_permission=state["reason"])
        ready = state["source_protocol"]["readiness"]["economic_execution_ready"]
        if ready and state["state"] in {"ACTIVE", "ECONOMIC_EXECUTION_READY"}:
            state["source_monitor"] = "HEALTHY"
        elif fixed and state["state"] in {"BLOCKED_DATA", "BLOCKED_PERMISSION"} and (
                book["problem"] or now >= at + pd.Timedelta(seconds=lane["source"]["decision_receipt_seconds"]) + processing):
            state["source_monitor"] = "DEGRADED"
    worker._save_status(db, book_id, state)
    paper.save(db, book)
    return {key: state.get(key) for key in ("state", "reason", "source_monitor", "source_protocol",
                                            "first_eligible_at", "next_evaluation_at")}


def protocol_status(*, registrations=None, runtime_books=None, root=None):
    """Combine immutable preparation with the last committed dispatcher state."""
    root = proxy_protocol.ROOT if root is None else Path(root)
    registrations = catalog.registrations() if registrations is None else registrations
    result = proxy_protocol.status(root=root, registrations=registrations)
    if not (root / commissioning.FILENAME).is_file():
        return result
    errors = {}
    try:
        sources = commissioning.attachments(registrations, utc_now(), root=root, errors=errors)
    except (OSError, ValueError, KeyError, sqlite3.Error) as exc:
        for state in result.values():
            state["source_permission"] = f"Commissioned source review failed: {type(exc).__name__}"
        return result
    if runtime_books is None:
        runtime_books = worker.status()["books"]
    for profile, state in result.items():
        if profile not in sources:
            if profile in errors:
                state["source_permission"] = errors[profile]
            continue
        live = runtime_books.get(profile, {})
        if live.get("source_protocol"):
            result[profile] = {**state, **live["source_protocol"], "reason": live.get("reason"),
                               "next_evaluation_at": live.get("next_evaluation_at")}
        else:
            state.update(state="PROXY_FEATURE_READY", source_permission="Reviewed independent KuCoin-origin grants")
    return result


def cycle(*, now=None, root=None, protocol_root=None):
    """The existing watchdog owns the cadence and Telegram delivery."""
    root = catalog.ROOT if root is None else Path(root)
    now = utc_now() if now is None else utc(now)
    registrations = catalog.registrations(root=root)
    errors = {}
    try:
        sources = commissioning.attachments(registrations, now, root=protocol_root, errors=errors)
    except (OSError, ValueError, KeyError, sqlite3.Error) as exc:
        return {"state": "BLOCKED_PERMISSION", "paper_dispatch": "BLOCKED_PERMISSION", "reason": str(exc)}
    if not sources:
        current = proxy_protocol.status(root=proxy_protocol.ROOT if protocol_root is None else Path(protocol_root),
                                        registrations=registrations)
        keys = ("book_id", "state", "feature_mode", "source_mode", "readiness")
        books = {name: {key: book[key] for key in keys} for name, book in current.items()}
        for name, reason in errors.items():
            if name in books:
                books[name].update(reason=reason, source_monitor="DEGRADED")
        return {"books": books,
                "state": "BLOCKED_PERMISSION", "paper_dispatch": "BLOCKED_PERMISSION",
                "reason": "No reviewed independent KuCoin-origin handoff is commissioned"}
    process, limits = psutil.Process(), catalog.LIMITS
    if (psutil.virtual_memory().available < limits["min_available_mib"] * MIB
            or shutil.disk_usage(root).free < limits["min_free_disk_mib"] * MIB
            or (root / "paper.sqlite3").stat().st_size >= limits["max_ledger_mib"] * MIB
            or process.memory_info().rss >= limits["worker_rss_mib"] * MIB):
        return {"state": "WAITING_FOR_RESOURCES", "paper_dispatch": "COMMISSIONED"}
    started = time.monotonic()
    deadline = started + limits["cycle_seconds"]
    with writer_lock(root / ".cycle.lock"), closing(catalog.db_open(root=root)) as db, threadpool_limits(limits=1):
        db.executescript(SCHEMA)
        results = {}
        for profile, registration in registrations.items():
            if profile not in sources:
                results[profile] = {"state": "BLOCKED_PERMISSION", "reason": errors.get(profile, "No reviewed handoff"),
                                    "source_monitor": "DEGRADED" if profile in errors else None}
                continue
            if time.monotonic() >= deadline or process.memory_info().rss >= limits["worker_rss_mib"] * MIB:
                results[profile] = {"state": "WAITING_FOR_RESOURCES"}
                continue
            with db:
                results[profile] = _run_book(db, registration, sources[profile], now, deadline)
    return {"state": "CHECKED", "paper_dispatch": "COMMISSIONED", "books": results,
            "rss_mib": round(process.memory_info().rss / MIB, 1), "wall_seconds": round(time.monotonic() - started, 3)}
