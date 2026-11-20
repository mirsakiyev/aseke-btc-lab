"""Committed operational restrictions and name-independent book status.

Current availability is separate from historical coverage and research validity.
No function sends a notification, supplies a price, or approves recovery.
"""

from copy import deepcopy
import json
from math import isfinite

import pandas as pd

from lab.config import fingerprint
from lab.paper_run import capabilities, protection_incidents as incidents
from lab.storage import utc


SCHEMA = "PAPER_OPERATIONAL_STATE_V1"
SQL = "CREATE TABLE IF NOT EXISTS operational_state (id TEXT PRIMARY KEY, state TEXT NOT NULL)"
OPENED = "OPERATIONAL_BLOCK_OPENED"
CLEARED = "OPERATIONAL_BLOCK_CLEARED"
SOURCE = "SOURCE_OPERATIONAL_TRANSITION"
FUNDING_SCHEMA = "FUNDING_SETTLEMENT_RECONCILIATION_V1"
FUNDING_ACTIONABLE = {"FUNDING_EVIDENCE_MISSING", "FUNDING_CONFLICT", "FINANCIAL_RECONCILIATION_BLOCKED"}


def funding_projection(book):
    """Use only the authoritative writer's versioned funding classification."""
    value = (book.get("position") or {}).get("funding_state")
    if value is None:
        return None
    if not isinstance(value, dict) or value.get("schema") != FUNDING_SCHEMA:
        raise ValueError("FUNDING_STATUS_PROJECTION_INVALID")
    if not isinstance(value.get("obligations"), list) or type(value.get("financial_complete")) is not bool:
        raise ValueError("FUNDING_STATUS_PROJECTION_INVALID")
    position = book.get("position") or {}
    if value.get("book_id", book["book_id"]) != book["book_id"] or value.get("position_id", position.get("id")) != position.get("id"):
        raise ValueError("FUNDING_STATUS_PROJECTION_IDENTITY_MISMATCH")
    if len(value["obligations"]) > 512 or any(not isinstance(item, dict) or not isinstance(item.get("id"), str)
                                              or not item["id"] for item in value["obligations"]):
        raise ValueError("FUNDING_STATUS_PROJECTION_INVALID")
    if len({item["id"] for item in value["obligations"]}) != len(value["obligations"]):
        raise ValueError("FUNDING_STATUS_OBLIGATION_IDENTITY_DUPLICATED")
    return value


def init(db):
    db.execute(SQL)
    for action in ("UPDATE", "DELETE"):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS operational_journal_{action.lower()} BEFORE {action} ON event "
                   f"WHEN OLD.kind IN ('{OPENED}','{CLEARED}','{SOURCE}') "
                   "BEGIN SELECT RAISE(ABORT,'Operational journal is append-only'); END")


def _read(db, key):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='operational_state'").fetchone():
        return {}
    row = db.execute("SELECT state FROM operational_state WHERE id=?", (key,)).fetchone()
    return json.loads(row[0]) if row else {}


def _save(db, key, body):
    db.execute("INSERT OR REPLACE INTO operational_state VALUES (?,?)",
               (key, json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)))


def _scope(lane):
    try:
        return capabilities.source_scope_id(lane)
    except capabilities.UnsupportedCapability:
        return fingerprint(["UNSUPPORTED_SOURCE_SCOPE", lane.get("source"), lane.get("symbol"),
                            lane.get("observation_kind")])


def _event(db, book_id, kind, at, body, identity):
    key = fingerprint([SCHEMA, kind, identity])
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
    row = db.execute("SELECT body FROM event WHERE key=?", (key,)).fetchone()
    if row:
        if row[0] != encoded:
            raise ValueError("Operational identity reused with different evidence")
        return key
    db.execute("INSERT INTO event VALUES (?,?,?,?,?,?,?)", (
        key, book_id, kind, body.get("position_id") or body.get("episode_id") or "",
        at.isoformat(), at.isoformat(), encoded))
    return key


def source_transition(db, record, lane, at, *, available, reason=None, correlation_id=None, evidence=None):
    """One shared-source correlation, without a representative strategy book."""
    init(db)
    scope = _scope(lane)
    key = "source:" + scope
    old = _read(db, key)
    state = "AVAILABLE" if available else "UNAVAILABLE"
    reason = None if available else str(reason or "SOURCE_UNAVAILABLE")
    if old.get("state") == state and old.get("reason_code") == reason:
        return old
    sequence = old.get("sequence", 0) + 1
    value = {"schema": SCHEMA, "generation": record["id"], "source_scope_id": scope,
             "source_identity": capabilities.source_identity(lane), "state": state,
             "reason_code": reason, "observed_at": at.isoformat(), "sequence": sequence,
             "started_at": None if available else old.get("started_at") if old.get("state") == "UNAVAILABLE" else at.isoformat(),
             "correlation_id": (old.get("correlation_id") if old.get("state") == "UNAVAILABLE" else None)
             or correlation_id or fingerprint([scope, sequence, at.isoformat()]),
             "evidence": deepcopy(evidence or {})}
    value["event_id"] = _event(db, "SOURCE:" + scope, SOURCE, at, value, [record["id"], scope, sequence])
    _save(db, key, value)
    return value


def source_state(db, lane):
    return _read(db, "source:" + capabilities.source_scope_id(lane))


def _block(db, record, lane, book, at, reason, *, details=None, historical=False, since=None, restriction_key=None):
    key = "book:" + book["book_id"]
    state = _read(db, key)
    active = state.setdefault("active", {})
    active_key = restriction_key or reason
    if active_key in active:
        return active[active_key]
    sequence = state.get("sequence", 0) + 1
    scope = _scope(lane)
    episode = fingerprint([record["id"], book["book_id"], active_key, sequence])
    position = book.get("position") or {}
    value = {"schema": SCHEMA, "generation": record["id"], "book_id": book["book_id"],
             "episode_id": episode, "reason_code": reason, "reason": reason,
             "restriction_key": active_key,
             "source_scope_id": scope, "detected_at": at.isoformat(),
             "restriction_started_at": since or at.isoformat(),
             "position_id": position.get("id"), "exposure": "OPEN" if position else
             "PENDING_ENTRY" if book.get("pending") else "FLAT",
             "historical_coverage": "UNRESOLVED" if historical else "NOT_APPLICABLE",
             "requires_incident_review": historical,
             "evidence": deepcopy(details or {}), "next_action": next_action(reason)}
    value["event_id"] = _event(db, book["book_id"], OPENED, at, value, episode)
    active[active_key] = value
    state["sequence"] = sequence
    _save(db, key, state)
    return value


def next_action(reason):
    if reason == "POSITION_OPEN":
        return "Continue accepted-price protection, funding collection and legitimate exits; no overlapping entry is authorized."
    if reason == "PENDING_ENTRY":
        return "Preserve normal prospective entry validation and expiry; no overlapping intention is authorized."
    if reason in FUNDING_ACTIONABLE or reason == "FUNDING_SCHEDULE_UNAVAILABLE":
        return "Inspect the exact settlement obligation and source-correct rate/valuation receipts; use the reviewed funding reconciliation procedure. Preserve protection and all original evidence."
    if reason in ("HISTORICAL_REVIEW_REQUIRED", "TRAIL_CALCULATION_FAILED", "UNMODELED_TIER"):
        return "Inspect this book's protection report; reconcile terminal obligations and review every incident before a new prospective resume."
    if reason == "FINANCIAL_RECONCILIATION_REQUIRED":
        return "Continue protection and legitimate cost reconciliation; inspect missing financial receipts."
    if reason == "CLOCK_UNVERIFIED":
        return "Wait for the existing clock-quality requirements; do not relax the admission gate."
    if reason == "INTENTIONAL_ENTRY_PAUSE":
        return "Preserve operator pause until the supported entry-control release is authorized."
    if reason == "PERMANENT_RISK_BLOCK":
        return "Operator risk review is required; ordinary source recovery cannot remove this restriction."
    if reason.startswith("UNSUPPORTED"):
        return "Register a reviewed supported capability or implement and review the missing adapter."
    return "Inspect source permissions, qualification and fresh evidence; retry only within existing resource limits."


def inherit_source_restriction(db, record, lane, book, at):
    """Registration inherits a known outage immediately, without an exposure gap."""
    init(db)
    shared = source_state(db, lane)
    if shared.get("state") == "UNAVAILABLE":
        return _block(db, record, lane, book, at, shared["reason_code"],
                      details={"correlation_id": shared["correlation_id"], "inherited_source_outage": True},
                      since=shared.get("started_at"))
    return None


def record_fault(db, record, lane, book, at, reason, *, details=None, historical=True):
    """Give previously bare protection flags a cause, scope and supported review path."""
    init(db)
    episode = _block(db, record, lane, book, at, reason, details=details,
                     historical=historical and bool(book.get("position")))
    if not historical and not book.get("position"):
        book.setdefault("operational_faults", {})[reason] = episode["episode_id"]
    if book.get("position"):
        book["position"].setdefault("operational_faults", {})[reason] = episode["episode_id"]
        if historical:
            book.pop("prospective_admission", None)
            if incidents.active(db):
                incidents.open_incident(db, book, at, reason, diagnostics=details,
                                        lane=lane, source_scope_id=capabilities.source_scope_id(lane),
                                        correlation_id=episode["episode_id"])
            else:
                book["position"]["unresolved_protection"] = True
                from lab.phase15 import paper

                paper.event(db, book, "PROTECTION_GAP", book["position"]["id"], at, at,
                            {"reason": reason, "source_scope_id": _scope(lane),
                             "correlation_id": episode["episode_id"],
                             "last_reference": book["position"].get("mark_received_at"),
                             "historical_coverage": "UNRESOLVED", "evidence": deepcopy(details or {})})
    return episode


def clear_fault(book, reason):
    book.get("operational_faults", {}).pop(reason, None)
    if book.get("position"):
        book["position"].get("operational_faults", {}).pop(reason, None)


def note_entry_events(db, record, lane, book, after_rowid, at):
    """Journal actual technical/risk refusal without fabricating exposure loss.

    Normal signal/range/trigger/session decisions remain strategy outcomes.
    A pending technical rejection persists only for that exact intention; a
    terminal NO_FILL is an observed short restriction, cleared by capture.
    """
    normal = {"REFERENCE_HAS_NOT_REACHED_TRIGGER", "REFERENCE_PRECEDES_ELIGIBILITY",
              "REFERENCE_OUTSIDE_FROZEN_RANGE", "REFERENCE_CROSSED_STOP",
              "SESSION_ALREADY_FLAT", "ENTRIES_OFF_OR_STUDY_WINDOW_CLOSED",
              "INTENTION_PRECEDES_APPROVED_TRADE_STREAM"}
    rows = db.execute("SELECT key,kind,idea,recorded_at,body FROM event WHERE rowid>? AND book=? "
                      "AND kind IN ('ENTRY_EVIDENCE_REJECTED','NO_FILL','HYPOTHETICAL_FILL') ORDER BY rowid",
                      (after_rowid, book["book_id"])).fetchall()
    for event_id, kind, idea_id, recorded, raw in rows:
        reason = json.loads(raw).get("reason")
        if kind == "HYPOTHETICAL_FILL" or reason in normal:
            book.pop("entry_operational_restriction", None)
            continue
        if not reason:
            continue
        details = {"cause_event_id": event_id, "cause_kind": kind, "pending_id": idea_id,
                   "evidence_limitations": "Entry was refused; no hypothetical exposure was created."}
        _block(db, record, lane, book, utc(recorded), "ENTRY_GATE_" + reason, details=details)
        if kind == "ENTRY_EVIDENCE_REJECTED" and book.get("pending"):
            book["entry_operational_restriction"] = {"pending_id": idea_id, "reason_code": "ENTRY_GATE_" + reason,
                                                      "details": details}


def restrictions(book, lane, at, *, source_health, clock_ok, entries_enabled):
    result = {}
    position = book.get("position") or {}
    if book.get("permanent_block"):
        result["PERMANENT_RISK_BLOCK"] = {}
    if lane.get("qualification", {}).get("state") != "QUALIFIED":
        result["SOURCE_QUALIFICATION_UNAVAILABLE"] = {"qualification": lane.get("qualification")}
    if not source_health.get("available"):
        result[source_health.get("reason_code") or "SOURCE_UNAVAILABLE"] = source_health
    if not clock_ok:
        result["CLOCK_UNVERIFIED"] = {}
    if not entries_enabled:
        result["INTENTIONAL_ENTRY_PAUSE"] = {}
    funding = funding_projection(book)
    if funding is not None:
        for obligation in funding["obligations"]:
            if obligation.get("actionable"):
                identifier = obligation.get("id")
                if not isinstance(identifier, str) or not identifier:
                    raise ValueError("FUNDING_OBLIGATION_ID_UNAVAILABLE")
                reason = obligation.get("state")
                if reason not in FUNDING_ACTIONABLE:
                    reason = "FINANCIAL_RECONCILIATION_BLOCKED"
                result["FUNDING_OBLIGATION:" + identifier] = {
                    "reason_code": reason, "funding_obligation_id": identifier,
                    "settlement_due_at": obligation.get("settlement_due_at") or obligation.get("slot"),
                    "valuation_deadline": obligation.get("valuation_deadline"),
                    "funding_reason_code": obligation.get("reason_code"),
                    "rate_status": obligation.get("rate_status"), "valuation_status": obligation.get("valuation_status"),
                    "next_action": obligation.get("next_action"), "funding_status_schema": funding["schema"]}
        if funding.get("actionable") and not any(k.startswith("FUNDING_OBLIGATION:") for k in result):
            reason = funding.get("state") if funding.get("state") in FUNDING_ACTIONABLE else "FINANCIAL_RECONCILIATION_BLOCKED"
            result[reason] = {"reason_code": reason,
                "funding_status_schema": funding["schema"], "funding_reason_code": funding.get("reason_code")}
    elif position.get("unresolved_costs"):
        result["FINANCIAL_RECONCILIATION_REQUIRED"] = {"reason": position["unresolved_costs"]}
    amounts = ("cash", "starting_equity", "gross", "funding", "fees", "liquidation_cost", "unfunded_deficit")
    if not all(type(book.get(k)) in (int, float) and isfinite(book[k]) for k in amounts):
        result["FINANCIAL_RECONCILIATION_REQUIRED"] = {"reason": "MISSING_OR_NONFINITE_NOMINAL_AMOUNTS"}
    else:
        expected = (book["starting_equity"] + book["gross"] + book["funding"] - book["fees"]
                    - book["liquidation_cost"] + book["unfunded_deficit"])
        if abs(book["cash"] - expected) > 1e-8:
            result["FINANCIAL_RECONCILIATION_REQUIRED"] = {"reason": "NOMINAL_CASH_MISMATCH"}
    for reason in position.get("operational_faults", {}):
        result[reason] = {"position_id": position.get("id")}
    for reason in book.get("operational_faults", {}):
        result[reason] = {"current_book_fault": True}
    entry_restriction = book.get("entry_operational_restriction") or {}
    if entry_restriction.get("pending_id") == (book.get("pending") or {}).get("id") and entry_restriction.get("reason_code"):
        result[entry_restriction["reason_code"]] = entry_restriction.get("details", {})
    permit = book.get("prospective_admission") or {}
    future = bool(permit.get("effective_at") and at < utc(permit["effective_at"]))
    if book.get("problem") and not incidents.admission_allowed(book, at) and not future:
        result["HISTORICAL_REVIEW_REQUIRED"] = {"historical_problem": book["problem"]}
    return result


def capture(db, record, lane, book, at, *, source_health, clock_ok, entries_enabled):
    init(db)
    current = restrictions(book, lane, at, source_health=source_health, clock_ok=clock_ok,
                           entries_enabled=entries_enabled)
    shared = source_state(db, lane)
    for reason, details in current.items():
        _block(db, record, lane, book, at, details.get("reason_code") or reason, details=details,
               restriction_key=reason,
               since=shared.get("started_at") if reason == shared.get("reason_code") else None)
    key = "book:" + book["book_id"]
    state = _read(db, key)
    for reason, episode in list(state.get("active", {}).items()):
        if reason not in current:
            body = {**episode, "cleared_at": at.isoformat(), "observed_at": at.isoformat(),
                    "clearance": "CURRENT_CONDITION_NO_LONGER_PRESENT",
                    "historical_coverage": episode["historical_coverage"]}
            body.pop("event_id", None)
            _event(db, book["book_id"], CLEARED, at, body, episode["episode_id"])
            del state["active"][reason]
    _save(db, key, state)
    return state


def status(db, record, lane, book, at, *, source_health, clock_ok, entries_enabled,
           financial_complete=True, research=None):
    """A read-only operational contract; ACTIVE never means an overlapping entry."""
    value = capabilities.contract(lane, generation=record["id"], slot=lane.get("slot"))
    state = _read(db, "book:" + book["book_id"])
    current = restrictions(book, lane, at, source_health=source_health, clock_ok=clock_ok,
                           entries_enabled=entries_enabled)
    position, pending = book.get("position") or {}, book.get("pending") or {}
    funding = funding_projection(book)
    normal_funding_pending = bool(funding and funding.get("state") == "FUNDING_SETTLEMENT_PENDING"
                                 and not funding.get("actionable"))
    financial_complete = bool(financial_complete and (funding is None or funding["financial_complete"]))
    financial_blocked = any(k == "FINANCIAL_RECONCILIATION_REQUIRED" or k == "FINANCIAL_RECONCILIATION_BLOCKED"
                            or k.startswith("FUNDING_OBLIGATION:") for k in current)
    permit = book.get("prospective_admission") or {}
    first = next(iter(current), None)
    reason = (current[first].get("reason_code") or first) if first else None
    if not financial_complete and not normal_funding_pending:
        reason = reason or "FINANCIAL_RECONCILIATION_REQUIRED"
    if not reason and position:
        reason = "POSITION_OPEN"
    if not reason and pending:
        reason = "PENDING_ENTRY"
    if not reason and permit.get("effective_at") and at < utc(permit["effective_at"]):
        reason = "WAIT_FUTURE_ADMISSION"
    if not reason and not incidents.admission_allowed(book, at):
        reason = "HISTORICAL_REVIEW_REQUIRED"
    if not reason and not utc(record["body"]["start"]) <= at < utc(lane["publication_end_exclusive"]):
        reason = "STUDY_WINDOW_CLOSED"
    historical = incidents.summaries(db, book, record["id"])
    historical_uncertainty = bool(book.get("problem") or position.get("unresolved_protection") or historical)
    active = list(state.get("active", {}).values())
    since = min((x["restriction_started_at"] for x in active), default=None)
    stage = ("WAIT_TERMINAL_ACCOUNTING" if position or pending or not financial_complete else
             "REVIEW_INCIDENTS" if any(not i.get("outcome") for i in historical) else
             "WAIT_CURRENT_HEALTH" if current else "WAIT_FUTURE_BOUNDARY" if reason == "WAIT_FUTURE_ADMISSION"
             else "PROSPECTIVE_ADMITTED" if permit else "READY")
    return {"schema": SCHEMA, "generation": record["id"], "book_id": book["book_id"],
            "capabilities": value, "source_health": deepcopy(source_health),
            "exposure": {"state": "OPEN" if position else "PENDING_ENTRY" if pending else "FLAT",
                         "position_id": position.get("id"), "pending_id": pending.get("id")},
            "position_state": "EXIT_OBSERVED" if position.get("exit_observed") else "POSITION_OPEN" if position else "PENDING_ENTRY" if pending else "FLAT",
            "protection_state": "NOT_REQUIRED" if not position or position.get("exit_observed") else
            "PROTECTION_UNAVAILABLE" if (not source_health.get("available") or position.get("operational_faults")) else "PROTECTION_HEALTHY",
            "funding": deepcopy(funding) if funding is not None else {"state": "LEGACY_NOT_CLASSIFIED", "classified": False,
                       "reason_code": "VERSIONED_FUNDING_PROJECTION_UNAVAILABLE"},
            "financial_state": "RECONCILED" if financial_complete else "FINANCIAL_RECONCILIATION_PENDING" if normal_funding_pending and not financial_blocked
            else "FINANCIAL_RECONCILIATION_BLOCKED",
            "entry_state": "ENTRY_OCCUPIED" if position or pending else "ENTRY_ELIGIBLE" if reason is None else "ENTRY_BLOCKED",
            "research_funding_state": "RESEARCH_PROVISIONAL" if funding is not None and (not funding["financial_complete"]
            or funding.get("research_provisional") or position.get("funding_provisional")) else
            "RESEARCH_VALIDATED" if (research or {}).get("research_state") == "VALIDATED" else "RESEARCH_UNVERIFIED",
            "current_protection": "NOT_REQUIRED" if not position else
            "UNAVAILABLE" if not source_health.get("available") else
            "LIMITED_BY_OPERATIONAL_FAULT" if position.get("operational_faults") else "MONITORING",
            "nominal_financial_reconciliation": bool(financial_complete),
            "historical_coverage": "UNRESOLVED" if historical_uncertainty else "NO_KNOWN_INCIDENT",
            "historical_incidents": [{k: i.get(k) for k in ("incident_id", "outcome", "historical_coverage", "current_feed_health")}
                                     for i in historical],
            "research_state": (research or {}).get("research_state", "UNVERIFIED"),
            "entry_permission": {"available": reason is None, "reason_code": reason,
                                 "admission_event_id": permit.get("event_id"),
                                 "effective_at": permit.get("effective_at")},
            "recovery": {"stage": stage, "next_action": next_action(reason) if reason else
                         "Normal prospective evaluation under the registered strategy rules",
                         "automatic_resume": "DISABLED_PENDING_VERSIONED_POLICY_APPROVAL"},
            "restriction_started_at": since, "current_restrictions": active}
