"""One bounded prospective cycle hosted by the existing health watchdog."""
from contextlib import closing
from dataclasses import asdict, is_dataclass
from pathlib import Path
import json
import shutil
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.ema_paper import catalog, ledger, source
from lab.execution import execution_open
from lab.phase15 import paper
from lab.storage import utc, utc_now, writer_lock
from lab.strategy_lab import ema_pullback
from lab.strategy_lab.evaluator import proposal


MINUTE = pd.Timedelta(minutes=1)
MIB = 1024**2


def _record(db, table, book_id):
    row = db.execute(f"SELECT body FROM {table} WHERE book=?", (book_id,)).fetchone()
    return json.loads(row[0]) if row else {}


def _save_status(db, book, state):
    db.execute("INSERT INTO readiness VALUES (?,?) ON CONFLICT(book) DO UPDATE SET body=excluded.body",
               (book, paper.encode(state)))


def _clocks(lane):
    step = pd.Timedelta(seconds=lane["execution_clock"]["decision_seconds"])
    processing = pd.Timedelta(seconds=lane["spec"]["execution"]["processing_seconds"])
    return step, processing


def _eligibility(db, lane, step, now, evidence):
    first = execution_open(now, step)
    end = first + pd.Timedelta(days=lane["horizon_days"])
    follow = end + pd.Timedelta(minutes=lane["follow_through_minutes"])
    fixed = {"source_ready_at": now.isoformat(), "first_eligible_at": first.isoformat(),
             "primary_end_exclusive": end.isoformat(), "follow_end_exclusive": follow.isoformat(),
             "source_id": evidence["source_id"]}
    db.execute("INSERT INTO eligibility VALUES (?,?)", (lane["book_id"], paper.encode(fixed)))
    return fixed


def _observation_contract(observation):
    contract = observation.get("contract")
    return asdict(contract) if is_dataclass(contract) else contract


def _advance(db, lane, book, now, fixed, source_root, deadline):
    follow = utc(fixed["follow_end_exclusive"])
    for _ in range(lane["limits"]["minutes_per_book"]):
        if not book["cursor"] or book["permanent_block"] or time.monotonic() >= deadline:
            break
        at = utc(book["cursor"])
        if at + MINUTE > min(follow, now):
            if now >= follow and not book["permanent_block"]:
                paper.problem(db, book, "Follow-through ended with incomplete execution evidence", at, now, permanent=True)
            break
        seen = source.execution(lane, at, now, root=source_root)
        if seen["state"] != "READY":
            grace = lane["source"]["funding_receipt_seconds"] if seen.get("waiting_for_funding") else lane["source"]["price_receipt_seconds"]
            if seen.get("permanent") or now > at + MINUTE + pd.Timedelta(seconds=grace):
                paper.problem(db, book, seen["reason"], at, now, permanent=bool(seen.get("permanent") or book["pending"]))
            break
        idea = book["position"] or book["pending"]
        if seen["evidence"]["source_id"] != fixed["source_id"] or _observation_contract(seen) != idea["plan"]["contract"]:
            paper.problem(db, book, "Dated execution contract differs from the committed order", at, now, permanent=True)
            break
        if not ledger.advance(db, book, at, now, seen["trade"], seen["mark"], seen.get("funding", ())):
            break
    return book


def _decision(db, lane, book, state, fixed, at, step, processing, now, source_root):
    observed = source.decision(lane, at, now, root=source_root)
    state["warmup"] = {"completed": observed.get("warmup_bars", 0), "required": observed.get("required_bars", 200)}
    state["coverage"] = observed.get("evidence", {})
    if observed["state"] != "READY":
        state.update(state=observed["state"], reason=observed["reason"])
        return fixed
    evidence = observed["evidence"]
    receipt = utc(evidence["latest_receipt_at"])
    if now < max(at, receipt) + processing:
        state.update(state="WAITING_FOR_DATA", reason="Processing allowance after actual source availability has not elapsed")
        return fixed
    if fixed and fixed["source_id"] != evidence["source_id"]:
        state.update(state="BLOCKED_PERMISSION", reason="Current source differs from the preregistered eligible source")
        return fixed
    if fixed and at < utc(fixed["first_eligible_at"]):
        state.update(state="WAITING_FOR_START", reason="Qualified source. Waiting for the frozen future decision boundary")
        return fixed
    if state.get("last_evaluated_at") == at.isoformat():
        state.update(state="ACTIVE", reason="Qualified forward evidence")
        return fixed
    spec = lane["spec"]
    native = pd.Timedelta(seconds=spec["data"]["native_seconds"])
    candles = observed["frame"]
    features = ema_pullback.features(candles.loc[candles.close_at.le(at)], spec["timeframe"], native)
    if at not in features.index or not features.at[at, "ready"]:
        state.update(state="WAITING_FOR_DATA", reason="Completed chart data does not yet produce all finite EMA indicators")
        return fixed
    if not fixed:
        fixed = _eligibility(db, lane, step, now, evidence)
        state.update(state="WAITING_FOR_START", reason="Qualified source. Waiting for the frozen future decision boundary")
        return fixed
    state.update(state="ACTIVE", reason="Qualified forward evidence")
    if now > at + pd.Timedelta(seconds=lane["source"]["publication_max_seconds"]):
        state.update(last_evaluated_at=at.isoformat(), last_signal={"decision": "NO_TRADE",
                     "reason": "Decision publication window elapsed", "decision_at": at.isoformat()})
        return fixed
    signal = proposal(features, at, spec, data_hash=evidence.get("source_id"),
                      position_open=bool(book["pending"] or book["position"] or book["problem"]))
    setup = signal.pop("setup", None)
    state.update(last_evaluated_at=at.isoformat(), last_signal={**signal, "observed_at": now.isoformat()})
    if setup is None:
        return fixed
    plan, reason = ledger.make_plan(lane, setup, observed["contract"], book["equity"], now,
                                    {**evidence, "publication_end_exclusive": fixed["primary_end_exclusive"],
                                     "execution_terms": {key: evidence[field] for key, field in
                                                         (("min_quote", "minimum_notional"), ("max_contracts", "maximum_quantity"))
                                                         if field in evidence and evidence[field] is not None}})
    if not plan:
        state["last_signal"].update(decision="NO_TRADE", reason=reason)
        return fixed
    analysis = {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": at, "plan": plan,
                "direction": "LONG" if setup.direction == 1 else "SHORT", "strategy_version": lane["strategy_version"],
                "exit_policy": setup.exit_policy}
    idea = paper.publish(db, book, analysis, now)
    if idea:
        state["last_signal"]["idea_id"] = idea["id"]
    return fixed


def _cycle_book(db, lane, now, source_root, deadline):
    book_id = lane["book_id"]
    state = _record(db, "readiness", book_id)
    fixed = _record(db, "eligibility", book_id)
    step, processing = _clocks(lane)
    book = paper.load(db, lane)
    at = (now - processing).floor(step)
    state.update(last_checked_at=now.isoformat(), next_evaluation_at=(execution_open(at, step) + processing).isoformat())
    if fixed:
        _advance(db, lane, book, now, fixed, source_root, deadline)
    if fixed and at >= utc(fixed["primary_end_exclusive"]):
        closed = db.execute("SELECT count(*) FROM event WHERE book=? AND kind='CLOSED'", (book_id,)).fetchone()[0]
        finished = now >= utc(fixed["follow_end_exclusive"])
        state.update(state="COMPLETE" if finished else "FOLLOW_THROUGH",
                     reason="Frozen primary horizon ended", next_evaluation_at=None,
                     assessment="INCONCLUSIVE" if closed < lane["min_complete_trades"] or book["problem"] or book["position"]
                     else "REVIEW_REQUIRED")
    elif book["problem"]:
        state.update(state="BLOCKED_EXECUTION_EVIDENCE", reason=book["problem"])
    elif not fixed or state.get("last_evaluated_at") != at.isoformat():
        fixed = _decision(db, lane, book, state, fixed, at, step, processing, now, source_root)
        if fixed and at < utc(fixed["first_eligible_at"]):
            state["next_evaluation_at"] = (utc(fixed["first_eligible_at"]) + processing).isoformat()
    else:
        state.update(state="ACTIVE", reason="Qualified forward evidence")
    state.update(fixed)
    _save_status(db, book_id, state)
    paper.save(db, book)
    return {key: state.get(key) for key in ("state", "reason", "warmup", "source_ready_at",
                                            "first_eligible_at", "last_evaluated_at", "next_evaluation_at")}


def cycle(*, now=None, root=None, source_root=None):
    root = catalog.ROOT if root is None else Path(root)
    source_root = root / "source" if source_root is None else Path(source_root)
    if not (root / "paper.sqlite3").is_file():
        return {"state": "UNREGISTERED"}
    limits = catalog.LIMITS
    process = psutil.Process()
    if (psutil.virtual_memory().available < limits["min_available_mib"] * MIB
            or shutil.disk_usage(root).free < limits["min_free_disk_mib"] * MIB
            or (root / "paper.sqlite3").stat().st_size >= limits["max_ledger_mib"] * MIB
            or process.memory_info().rss >= limits["worker_rss_mib"] * MIB):
        return {"state": "WAITING_FOR_RESOURCES"}
    deadline = time.monotonic() + limits["cycle_seconds"]
    now = utc_now() if now is None else utc(now)
    with writer_lock(root / ".cycle.lock"), closing(catalog.db_open(root=root)) as db, threadpool_limits(limits=1):
        result = {}
        pins = catalog.code_pins()
        for profile, record in catalog.registrations(db).items():
            lane = record["body"]
            if time.monotonic() >= deadline or process.memory_info().rss >= limits["worker_rss_mib"] * MIB:
                result[profile] = {"state": "WAITING_FOR_RESOURCES"}
                continue
            with db:
                if pins != lane["code_pins"]:
                    state = _record(db, "readiness", lane["book_id"])
                    state.update(state="BLOCKED_SOURCE", reason="Frozen prospective runtime code hash differs",
                                 last_checked_at=now.isoformat())
                    _save_status(db, lane["book_id"], state)
                    result[profile] = {"state": state["state"], "reason": state["reason"]}
                else:
                    result[profile] = _cycle_book(db, lane, now, source_root, deadline)
        return {"state": "CHECKED", "checked_at": now.isoformat(), "books": result,
                "rss_mib": round(process.memory_info().rss / MIB, 1),
                "wall_seconds": round(limits["cycle_seconds"] - (deadline - time.monotonic()), 3)}


def status(*, root=None):
    root = catalog.ROOT if root is None else Path(root)
    if not (root / "paper.sqlite3").is_file():
        return {"state": "UNREGISTERED", "books": {}}
    with closing(catalog.db_open(True, root=root)) as db:
        return {"state": "REGISTERED", "books": {
            profile: {**_record(db, "readiness", item["body"]["book_id"]),
                      "registration": item, "account": ledger.report(db, item["body"])}
            for profile, item in catalog.registrations(db).items()
        }}
