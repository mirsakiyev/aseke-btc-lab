"""Read-only registered-book status with isolated evidence failures.

The aggregate state preserves the existing dashboard convention: ACTIVE can
include a protected open account. Entry availability is separately explicit.
No missing or malformed account is replaced with an initial balance here.
"""

from contextlib import closing
from copy import deepcopy
import json
from math import isfinite
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.paper_run import capabilities, delivery, membership, operations, recovery
from lab.paper_run import protection_incidents as incidents, protection_reporting, source_amendment
from lab.phase15 import paper
from lab.storage import utc


SCHEMA = "REGISTERED_PAPER_STATUS_V1"
SEMANTICS = "ACTIVE means managed operational processing is available; it does not authorize overlapping entries or validate research results"
ERRORS = (OSError, ValueError, KeyError, TypeError, AttributeError, sqlite3.Error, OverflowError)
_LEDGER_COLUMNS = {
    "book": "id,state",
    "event": "key,book,kind,idea,source_at,recorded_at,body",
    "source_state": "id,state",
    "operational_state": "id,state",
    "recovery_work": "id,book,state",
    "paper_reference_delivery": "book,scope,state",
}


def _capture_ledger(runtime):
    # Fetch raw report inputs under one coherent read lock. Incident decoding,
    # protection projection, and external source checks can take much longer.
    with closing(runtime.connect(readonly=True)) as db:
        db.execute("BEGIN")
        tables = {name for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        return {name: db.execute(f"SELECT rowid,{columns} FROM {name}").fetchall()
                for name, columns in _LEDGER_COLUMNS.items() if name in tables}


def _load_ledger(db, snapshot):
    for name, rows in snapshot.items():
        columns = _LEDGER_COLUMNS[name]
        placeholders = ",".join("?" for _ in range(columns.count(",") + 2))
        db.execute(f"CREATE TABLE {name}(rowid INTEGER PRIMARY KEY,{columns})")
        db.executemany(f"INSERT INTO {name} VALUES ({placeholders})", rows)
    db.commit()
    db.execute("PRAGMA query_only=ON")


def _failure(code, error):
    return {"reason_code": code, "error_type": type(error).__name__, "detail": str(error)[:240]}


def _metadata(runtime, name, fallback, failures):
    try:
        value = runtime.read_status_snapshot(name, fallback)
        if value is not None and not isinstance(value, dict):
            raise ValueError("Metadata must be a JSON object")
        json.dumps(value, allow_nan=False)
        return value
    except ERRORS as error:
        problem = _failure("RUNTIME_METADATA_INSPECTION_FAILED", error)
        failures.append({"file": name, **problem})
        return {"state": "EVIDENCE_UNAVAILABLE", "reason": problem["reason_code"], "inspection_error": problem}


def _registered(db, record, amendment):
    """Retain valid registered books when one declared capability is unsupported.

    This is a reporting projection only. Registration/runtime admission still
    use membership's strict validator. Bad checksums are never admitted.
    """
    try:
        return membership.lanes(record, amendment, db=db), []
    except ERRORS as error:
        failures = [_failure("REGISTERED_MEMBERSHIP_INSPECTION_FAILED", error)]
    lanes = deepcopy(source_amendment.lanes(record, amendment))
    identities = {value["book_id"] for value in lanes.values()}
    for key, book_id, idea, raw in db.execute("SELECT key,book,idea,body FROM event WHERE kind=? ORDER BY rowid", (membership.KIND,)):
        try:
            body = json.loads(raw)
            value = body["amendment"]
            if (body.get("schema") != membership.SCHEMA or body.get("generation") != record["id"]
                    or body.get("amendment_id") != fingerprint(value)):
                raise ValueError("Membership schema, generation or checksum differs")
            slot, lane = value["slot"], value["lane"]
            request = {"generation": value.get("generation"), "slot": slot, "lane": lane,
                       "request_id": value.get("request_id"), "reason": body.get("reason")}
            if (value.get("generation") != record["id"] or book_id != lane.get("book_id")
                    or idea != record["id"] or key != fingerprint([membership.SCHEMA, record["id"], value.get("request_id")])
                    or body.get("request_sha256") != fingerprint(request)):
                raise ValueError("Membership journal identity or request checksum differs")
            if slot in lanes or lane["book_id"] in identities:
                raise ValueError("Membership replaces an existing identity")
            lanes[slot] = deepcopy(lane)
            identities.add(lane["book_id"])
        except ERRORS as error:
            failures.append({"event_id": key, **_failure("MEMBERSHIP_EVENT_INSPECTION_FAILED", error)})
    return lanes, failures


def _nominal(info):
    fields = ("cash", "equity", "starting_equity", "gross", "fees", "funding", "liquidation_cost", "unfunded_deficit")
    if not all(type(info.get(key)) in (int, float) and isfinite(info[key]) for key in fields):
        return False
    if (info.get("position") or {}).get("unresolved_costs"):
        return False
    expected = info["starting_equity"] + info["gross"] + info["funding"] - info["fees"] - info["liquidation_cost"] + info["unfunded_deficit"]
    return abs(info["cash"] - expected) <= 1e-8 and (
        bool(info.get("position")) or abs(info["equity"] - info["cash"]) <= 1e-8)


def _inspection(slot, lane, now, error):
    code = getattr(error, "code", None) or (str(error) if str(error).startswith("REGISTERED_BOOK_") else "BOOK_INSPECTION_FAILED")
    detail = _failure(str(code), error)
    return {"book_id": lane.get("book_id"), "run_id": lane.get("book_id"), "slot": slot,
            "label": lane.get("label", slot), "state": "BLOCKED", "reason": str(code),
            "inspection_error": detail, "problem": None, "historical_problem": None,
            "accounting_complete": False, "financial_reconciliation_complete": False,
            "prospective_entry_permission": False, "cash": None, "equity": None,
            "position": None, "pending": None, "exposure_state": "UNKNOWN",
            "research_state": "UNVERIFIED", "aggregate_state_semantics": SEMANTICS,
            "observed_at": now.isoformat(), "price_basis": lane.get("price_basis"),
            "entry_model": lane.get("entry_model"), "exit_model": lane.get("exit_model"),
            "freshness_seconds": 2, "qualification": lane.get("qualification"),
            "operational": {"schema": operations.SCHEMA, "generation": None, "book_id": lane.get("book_id"),
                            "source_health": {"available": False, "reason_code": str(code)},
                            "exposure": {"state": "UNKNOWN", "position_id": None, "pending_id": None},
                            "current_protection": "UNKNOWN", "nominal_financial_reconciliation": False,
                            "historical_coverage": "UNKNOWN", "research_state": "UNVERIFIED",
                            "entry_permission": {"available": False, "reason_code": str(code)},
                            "recovery": {"stage": "INSPECT_BOOK_EVIDENCE", "next_action": "Preserve the registered account and inspect the exact missing/malformed evidence; do not initialize or reset it"},
                            "restriction_started_at": None}}


def _heartbeat(heartbeat, record, now, build):
    if not isinstance(heartbeat, dict) or heartbeat.get("state") != "RUNNING" or heartbeat.get("generation") != record["id"]:
        return False, "MANAGED_RUNNER_EVIDENCE_UNAVAILABLE"
    try:
        if not pd.Timedelta(0) <= now - utc(heartbeat["at"]) < pd.Timedelta(seconds=45):
            return False, "MANAGED_RUNNER_HEARTBEAT_STALE"
    except ERRORS:
        return False, "MANAGED_RUNNER_EVIDENCE_UNAVAILABLE"
    if heartbeat.get("runner_build_id") != build:
        return False, "REVIEWED_RUNNER_BUILD_NOT_ADOPTED"
    return True, None


def _view_book(db, runtime, record, slot, original, now, stream, heartbeat, clock_ok, live, runner_reason,
               amendment, global_pause):
    lane = {**original, "slot": slot}
    contract = capabilities.contract(lane, generation=record["id"], slot=slot)
    row = db.execute("SELECT state FROM book WHERE id=?", (lane["book_id"],)).fetchone()
    if row is None:
        raise ValueError("REGISTERED_BOOK_STATE_MISSING")
    raw = json.loads(row[0])
    if not isinstance(raw, dict) or raw.get("book_id") != lane["book_id"]:
        raise ValueError("REGISTERED_BOOK_STATE_IDENTITY_MISMATCH")
    json.dumps(raw, allow_nan=False)
    info = paper.book_report(db, lane)
    incidents.restore_admission(db, info)
    if info.get("problem"):
        info["accounting_complete"] = False
    research = protection_reporting.summarize(db, lane, info)
    if info.get("problem") or (info.get("position") or {}).get("unresolved_protection"):
        research["research_state"] = "QUARANTINED"
    info.update(research)
    for field in ("closed_net", "expectancy", "expectancy_r", "closed_fees", "closed_funding",
                  "top_three_share_of_positive_gains", "average_hold_minutes"):
        info[field] = research["validated_" + field]
    try:
        health = runtime._source_health(db, lane, now, stream=stream)
        source = runtime._lane_source_state(db, lane)
    except ERRORS as error:
        health = {"available": False, "reason_code": "SOURCE_EVIDENCE_UNAVAILABLE",
                  "source_scope_id": capabilities.source_scope_id(lane),
                  "inspection_error": _failure("SOURCE_EVIDENCE_UNAVAILABLE", error)}
        source = {}
    try:
        profile_enabled = runtime._profile_entries_enabled(lane)
    except ERRORS as error:
        profile_enabled = False
        health.update(available=False, reason_code="CONFIGURATION_EVIDENCE_UNAVAILABLE",
                      inspection_error=_failure("CONFIGURATION_EVIDENCE_UNAVAILABLE", error))
    enabled = profile_enabled and not global_pause
    reference = source.get("last_observation")
    if reference is None and not health.get("inspection_error") and contract["source"]["transport"] == "WEBSOCKET_TRADE":
        reference = stream.get("last_usable_observation")
    if reference is not None and not isinstance(reference, dict):
        health.update(available=False, reason_code="SOURCE_OBSERVATION_INSPECTION_FAILED",
                      inspection_error=_failure("SOURCE_OBSERVATION_INSPECTION_FAILED",
                                                ValueError("Last observation must be a JSON object")))
        reference = None
    if reference and (reference.get("source") != lane["source"] or reference.get("symbol") != lane["symbol"]
                      or reference.get("observation_kind", "HTTP_SNAPSHOT") != lane.get("observation_kind", "HTTP_SNAPSHOT")):
        health.update(available=False, reason_code="REGISTERED_BOOK_REFERENCE_SCOPE_MISMATCH")
        reference = None
    financial = _nominal(info)
    funding = operations.funding_projection(info)
    if funding is not None:
        financial = bool(financial and funding["financial_complete"])
    operational = operations.status(db, record, lane, info, now, source_health=health,
                                    clock_ok=clock_ok, entries_enabled=enabled,
                                    financial_complete=financial, research=research)
    if global_pause and profile_enabled:
        post_pause = operations.status(db, record, lane, info, now, source_health=health,
                                       clock_ok=clock_ok, entries_enabled=True,
                                       financial_complete=financial, research=research)
        after_resume = post_pause["entry_permission"]
    else:
        after_resume = dict(operational["entry_permission"])
    if not profile_enabled and after_resume["reason_code"] == "INTENTIONAL_ENTRY_PAUSE":
        after_resume["reason_code"] = "PROFILE_ENTRY_CONTROL_DISABLED"
    backlog = next((value for value in delivery.pending_books(db) if value["book_id"] == lane["book_id"]), None)
    permission = operational["entry_permission"]
    adoption = runner_reason
    if live and contract["source"]["transport"] == "WEBSOCKET_TRADE" and amendment and (
            heartbeat.get("source_amendment_id") != amendment["id"]):
        adoption = "APPROVED_SOURCE_AMENDMENT_NOT_ADOPTED"
    if adoption:
        permission.update(available=False, reason_code=permission.get("reason_code") or adoption)
        after_resume.update(available=False, reason_code=adoption)
    if backlog:
        permission.update(available=False, reason_code="PROTECTION_DELIVERY_LOSS_REVIEW_REQUIRED" if backlog["loss_count"] else "PROTECTION_DELIVERY_PENDING")
        after_resume.update(available=False, reason_code=permission["reason_code"])
        if info.get("position"):
            operational["current_protection"] = "UNPROCESSED_REFERENCE_WORK"
    operational["source_delivery"] = backlog or {"book_id": lane["book_id"], "pending_count": 0, "loss_count": 0}
    operational["runner"] = {"available": live and not adoption, "reason_code": adoption,
                              "pid": (heartbeat or {}).get("pid"), "running_build": (heartbeat or {}).get("runner_build_id"),
                              "reviewed_build": runtime.RUNNER_BUILD}
    future = permission.get("reason_code") == "WAIT_FUTURE_ADMISSION"
    restrictions = operations.restrictions(info, lane, now, source_health=health,
                                           clock_ok=clock_ok, entries_enabled=enabled)
    # Occupancy alone keeps ACTIVE for dashboard compatibility. Explicit entry
    # permission is false; a normal NO_TRADE or pending candle is not an incident.
    normal_funding_pending = bool(funding and funding.get("state") == "FUNDING_SETTLEMENT_PENDING"
                                 and not funding.get("actionable"))
    if restrictions or (not financial and not normal_funding_pending) or backlog:
        aggregate = "BLOCKED"
    elif future or adoption or not live or not enabled or permission.get("reason_code") == "STUDY_WINDOW_CLOSED":
        aggregate = "WAITING"
    else:
        aggregate = "ACTIVE"
    reason = None if aggregate == "ACTIVE" else permission.get("reason_code") or runner_reason
    if permission.get("reason_code") == "CLOCK_UNVERIFIED":
        reason = "WORKSTATION_CLOCK_SYNC_UNVERIFIED"
    next_at = max(utc(record["body"]["start"]), now.floor(lane["timeframe"]) + pd.Timedelta(lane["timeframe"]))
    info.update(run_id=lane["book_id"], slot=slot, label=lane.get("label", slot),
                price_basis=lane["price_basis"], entry_model=lane["entry_model"], exit_model=lane["exit_model"],
                freshness_seconds=record["body"].get("freshness_seconds", 2), state=aggregate, reason=reason,
                next_evaluation=next_at.isoformat(), last_observation=reference,
                retired_book_id=lane.get("retired_book_id", lane.get("legacy_book_id")),
                ledger_unit=lane.get("ledger_unit", "NOMINAL_USDT"),
                publication_end_exclusive=lane["publication_end_exclusive"], qualification=lane.get("qualification"),
                source_effective_at=lane.get("source_effective_at"), source_amendment_id=lane.get("source_amendment_id"),
                historical_problem=info.get("problem"), prospective_entry_permission=permission["available"],
                admission_after_global_resume=after_resume,
                financial_reconciliation_complete=financial,
                protection_incidents=incidents.project(db, lane["book_id"]),
                operational=operational, capabilities=contract, source_scope_id=capabilities.source_scope_id(lane),
                exposure_state=operational["exposure"]["state"], aggregate_state_semantics=SEMANTICS,
                recovery_work=recovery.items(db, lane["book_id"]), observed_at=now.isoformat())
    if funding is not None:
        info.update(funding_state=deepcopy(funding), position_state=operational["position_state"],
                    protection_state=operational["protection_state"], entry_state=operational["entry_state"],
                    financial_state=operational["financial_state"], research_funding_state=operational["research_funding_state"])
        if not financial:
            info["accounting_complete"] = False
    json.dumps(info, default=str, allow_nan=False)
    return info


def read_status():
    from lab.paper_run import runtime

    failures = []
    try:
        record = runtime.current()
    except ERRORS as error:
        return {"schema": SCHEMA, "state": "EVIDENCE_UNAVAILABLE", "books": {},
                "reason": "CURRENT_REGISTRATION_INSPECTION_FAILED", "inspection_failures": [_failure("CURRENT_REGISTRATION_INSPECTION_FAILED", error)]}
    if not record:
        return {"schema": SCHEMA, "state": "LEGACY_RETIRED_AWAITING_RESET" if runtime.gate.retired() else "UNREGISTERED", "books": {}}
    stream = _metadata(runtime, "ema-live-status.json", {}, failures)
    heartbeat = _metadata(runtime, "last-cycle.json", None, failures)
    amendment = source_amendment.active(record)
    now = runtime.utc_now()
    clock_ok = runtime.synchronized(now)
    global_pause = (runtime.gate.ROOT / "entries-paused").exists()
    with closing(sqlite3.connect(":memory:")) as db:
        _load_ledger(db, _capture_ledger(runtime))
        watermark = db.execute("SELECT coalesce(max(rowid),0) FROM event").fetchone()[0]
        live, runner_reason = _heartbeat(heartbeat, record, now, runtime.RUNNER_BUILD)
        lanes, registry_failures = _registered(db, record, amendment)
        failures.extend(registry_failures)
        expected = {lane["book_id"] for lane in lanes.values()}
        actual = {key for key, in db.execute("SELECT id FROM book")}
        view = {}
        for slot, lane in lanes.items():
            try:
                view[slot] = _view_book(db, runtime, record, slot, lane, now, stream, heartbeat or {},
                                        clock_ok, live, runner_reason, amendment, global_pause)
            except ERRORS as error:
                view[slot] = _inspection(slot, lane, now, error)
                view[slot]["operational"]["generation"] = record["id"]
                failures.append({"slot": slot, "book_id": lane.get("book_id"), **view[slot]["inspection_error"]})
    body = record["body"]
    return {"schema": SCHEMA, "state": "CURRENT", "generation_id": record["id"],
            "cutoff": body.get("cutoff"), "start": body["start"],
            "archive": body.get("retired_generation_archive") or body.get("archive"),
            "retired_generation": body.get("retired_generation"), "books": view, "last_cycle": heartbeat,
            "ema_live_feed": stream, "observed_at": now.isoformat(), "ledger_watermark": watermark,
            "registered_book_ids": sorted(expected), "missing_account_ids": sorted(expected - actual),
            "unexpected_account_ids": sorted(actual - expected),
            "monitor_state": "PARTIAL_EVIDENCE_UNAVAILABLE" if failures or expected != actual else "OK",
            "inspection_failures": failures, "production_clock_qualified": clock_ok,
            "entries_paused": global_pause,
            "aggregate_state_semantics": SEMANTICS}
