"""Versioned, source-correct funding obligations in the authoritative writer.

Assessment is read-only. Settlement never fetches a price or approves historical
reconstruction, changes an applied rate, commits a transaction or clears a
protection incident. The public settled-history source is not immutable finality.
"""

from copy import deepcopy
import json
from math import isfinite
import re
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.learning.simulator import Position
from lab.paper_run import capabilities, execution
from lab.phase13.future import expected_funding
from lab.storage import utc


VERSION = "FUNDING_SETTLEMENT_RECONCILIATION_V1"
BASIS = "KUCOIN_SETTLED_RATE_FORWARD_MARK_VALUATION"
MAX_OBLIGATIONS = 512
TRANSITION = "FUNDING_STATUS_CHANGED"
COMPLETE = {"SETTLED", "NOT_APPLICABLE_EXCHANGE_TIMESTAMP"}


def _rates(body, slots, now):
    from lab.paper_run import funding_sources

    result = {}
    for left in range(0, len(slots), funding_sources.MAX_SLOTS):
        result.update(funding_sources.rates(body, slots[left:left+funding_sources.MAX_SLOTS], now))
    return result


def _archived(db, lane, position, slot, now):
    from lab.paper_run import funding_sources

    return funding_sources.archived_forward(db, lane, position, slot, now)


def _finite(value, *, positive=False):
    return type(value) in (float, int) and isfinite(value) and (not positive or value > 0)


def _grid(schedule, start, end):
    if not isinstance(schedule, dict):
        raise ValueError("Dated funding schedule is unavailable")
    interval = schedule.get("currentFundingRateGranularity") or schedule.get("fundingRateGranularity")
    anchor = schedule.get("nextFundingRateDateTime")
    if isinstance(interval, bool) or isinstance(anchor, bool):
        raise ValueError("Funding schedule contains invalid scalar types")
    if not interval or not anchor or int(interval) != float(interval) or int(interval) <= 0:
        raise ValueError("Dated funding schedule is unavailable")
    if (end-start) / pd.Timedelta(milliseconds=int(interval)) > MAX_OBLIGATIONS:
        raise ValueError("Funding obligation horizon exceeds the bounded review limit")
    result = expected_funding({"contract": schedule}, start, end)
    if result is None or len(result) > MAX_OBLIGATIONS:
        raise ValueError("Funding obligation horizon is unavailable or exceeds the review limit")
    return list(result)


def _schedule_identity(schedule):
    interval = int(schedule.get("currentFundingRateGranularity") or schedule["fundingRateGranularity"])
    return {"interval_ms": interval, "phase_ms": int(schedule["nextFundingRateDateTime"]) % interval}


def _blocked_context(value, position, schedule, slots, reason, detail, action):
    schedule.update(state="UNAVAILABLE", reason_code=reason, detail=str(detail))
    value.update(state="FINANCIAL_RECONCILIATION_BLOCKED", reason_code=reason,
                 financial_complete=False, actionable=True, schedule=schedule, next_action=action)
    # Retain the original frozen obligations even when current metadata cannot
    # qualify them for payment. Unknown current terms never erase known due slots.
    value["obligations"] = [{
        "id": fingerprint([VERSION, value["generation"], value["book_id"], position["id"], slot.isoformat(), value["source_scope_id"]]),
        "book_id": value["book_id"], "position_id": position["id"], "source_scope_id": value["source_scope_id"],
        "slot": slot.isoformat(), "settlement_due_at": slot.isoformat(),
        "valuation_deadline": (slot+execution.PROTECTION_GAP).isoformat(),
        "state": value["state"], "reason_code": reason, "detail": str(detail), "actionable": True,
        "financial_complete": False, "rate_status": "CURRENT_SCHEDULE_UNVERIFIED",
        "valuation_status": "NOT_ASSESSED", "valuation_reference": None, "applied_receipt": None,
        "next_action": action} for slot in slots]
    return value, slots, position


def _context(book, lane, body, now, metadata, generation):
    contract = capabilities.contract(lane, generation=generation, slot=lane.get("slot"))
    position = book.get("position")
    value = {"schema": VERSION, "generation": generation, "book_id": book["book_id"],
             "position_id": (position or {}).get("id"), "source_scope_id": capabilities.source_scope_id(lane),
             "source_identity": contract["source"], "observed_at": now.isoformat(), "obligations": [],
             "financial_complete": True, "actionable": False, "reason_code": None,
             "protection_assessment": "INDEPENDENT_ACCEPTED_PRICE_MONITORING",
             "research_provisional": bool((position or {}).get("funding_provisional")),
             "source_finality": "PUBLIC_SETTLED_HISTORY_CAN_BE_CORRECTED"}
    value["status_obligation_id"] = fingerprint([VERSION, generation, book["book_id"], value["position_id"], "SCHEDULE"])
    if contract["funding"] != "KUCOIN_SETTLED" or not position:
        value.update(state="NOT_APPLICABLE", next_action="Retain the registered financing policy and normal strategy gates")
        return value, [], None
    try:
        entry = execution.stamp(position["entry_at"])
        horizon = execution.stamp((position.get("exit_observed") or {}).get("exit_at", now))
        if horizon < entry or horizon > now:
            raise ValueError("Funding exposure horizon is inconsistent with the recorded position")
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        return _blocked_context(value, position, {}, [], "FUNDING_EXPOSURE_HORIZON_INVALID", error,
                                "Preserve exposure and inspect the original entry/exit timestamps")
    plan = position.get("plan") if isinstance(position.get("plan"), dict) else {}
    frozen = plan.get("funding_schedule")
    snapshot = metadata if isinstance(metadata, dict) else {}
    current = snapshot.get("contract")
    schedule = {"frozen": deepcopy(frozen), "current": deepcopy(current),
                "current_metadata_as_of": snapshot.get("as_of")}
    old = []
    try:
        old = _grid(frozen, entry + pd.Timedelta(nanoseconds=1), horizon + pd.Timedelta(nanoseconds=1))
        schedule.update(frozen_slots=[x.isoformat() for x in old], frozen_grid=_schedule_identity(frozen))
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        reason = "FUNDING_OBLIGATION_LIMIT_REVIEW_REQUIRED" if "bound" in str(error) else "FUNDING_SCHEDULE_UNAVAILABLE"
        return _blocked_context(value, position, schedule, old, reason, error,
                                "Obtain the original verified frozen schedule; do not invent a settlement grid")
    metadata_reason = "FUNDING_CURRENT_METADATA_UNVERIFIED"
    try:
        if not snapshot.get("as_of"):
            raise ValueError("Current dated metadata has no explicit source observation timestamp")
        metadata_reason = "FUNDING_CURRENT_METADATA_INVALID"
        observed_at = execution.stamp(snapshot["as_of"])
        if observed_at > now:
            metadata_reason = "FUNDING_CURRENT_METADATA_FUTURE"
            raise ValueError("Current dated metadata was observed after the funding decision")
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        return _blocked_context(value, position, schedule, old, metadata_reason, error,
                                "Obtain a verified current metadata timestamp; preserve every frozen due slot")
    try:
        new = _grid(current, entry + pd.Timedelta(nanoseconds=1), horizon + pd.Timedelta(nanoseconds=1))
        schedule.update(current_slots=[x.isoformat() for x in new], current_grid=_schedule_identity(current))
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        reason = "FUNDING_OBLIGATION_LIMIT_REVIEW_REQUIRED" if "bound" in str(error) else "FUNDING_SCHEDULE_UNAVAILABLE"
        return _blocked_context(value, position, schedule, old, reason, error,
                                "Obtain verified compatible dated contract metadata; do not invent a settlement grid")
    if old != new or schedule["frozen_grid"] != schedule["current_grid"]:
        schedule.update(state="CONFLICT", reason_code="FUNDING_SCHEDULE_CONFLICT")
        value.update(state="FUNDING_CONFLICT", reason_code="FUNDING_SCHEDULE_CONFLICT",
                     financial_complete=False, actionable=True, schedule=schedule,
                     next_action="Review dated schedule evidence and effective regimes explicitly; never union incompatible grids")
        return value, [], position
    schedule["state"] = "COMPATIBLE"
    value["schedule"] = schedule
    return value, old, position


def _reference(reference, lane, slot, now):
    if reference is None:
        return None, None
    try:
        if not isinstance(reference, dict) or not _finite(reference.get("price"), positive=True):
            raise ValueError("Invalid funding mark value")
        source, receipt = execution.stamp(reference["source_at"]), execution.stamp(reference["received_at"])
        if not slot <= source <= slot + execution.PROTECTION_GAP:
            raise ValueError("Funding reference is outside the original forward window")
        if not pd.Timedelta(0) <= receipt-source <= pd.Timedelta(seconds=2) or receipt > now:
            raise ValueError("Funding reference has stale, future or inconsistent receipt evidence")
        if reference.get("source", lane["source"]) != lane["source"] or reference.get("symbol", lane["symbol"]) != lane["symbol"]:
            raise ValueError("Funding reference source or instrument differs")
        if reference.get("observation_kind", "HTTP_SNAPSHOT") != "HTTP_SNAPSHOT":
            raise ValueError("Funding reference transport differs")
        if "received_mono_ns" in reference:
            reason = execution.valid_snapshot(reference, lane, receipt, reference["received_mono_ns"])
            if reason:
                raise ValueError("Original funding receipt qualification failed: " + reason)
        value = deepcopy(reference)
        value.setdefault("capture_kind", "LEGACY_POSITION_FORWARD_REFERENCE")
        value.setdefault("provenance_limit", "Legacy reference projection may omit raw-response and original decision fields")
        return value, None
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        return None, str(error)


def _applied(db, position, now, book):
    result = {}
    problem = None
    total = 0.
    model = position.get("position")
    if (not isinstance(model, dict) or type(model.get("direction")) is not int or model["direction"] not in (-1, 1)
            or not _finite(model.get("remaining"), positive=True) or not _finite(model.get("entry"), positive=True)
            or not all(_finite(model.get(field)) for field in ("wallet", "reserved", "funding", "fees"))):
        return {}, "Position financing model has invalid direction, quantity or financial amounts"
    for key, at, recorded, raw in db.execute("SELECT key,source_at,recorded_at,body FROM event "
            "WHERE book=? AND idea=? AND kind='FUNDING' ORDER BY rowid", (position["book_id"], position["id"])):
        try:
            value = json.loads(raw)
            slot = execution.stamp(value.get("settlement_at", at)).isoformat()
            if execution.stamp(at).isoformat() != slot or execution.stamp(recorded) > now:
                raise ValueError("Funding journal timestamp differs")
            if not all(_finite(value.get(field)) for field in ("rate", "valuation_reference", "cash_flow")) or value["valuation_reference"] <= 0:
                raise ValueError("Funding journal contains invalid amounts")
            if value.get("basis") != BASIS:
                raise ValueError("Funding journal basis differs from the admitted obligation model")
            if slot in result:
                raise ValueError("Duplicate funding settlement journal")
            result[slot] = {"event_id": key, "recorded_at": recorded, **value}
            total += value["cash_flow"]
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            problem = str(error)
    if abs(total-model["funding"]) > 1e-8:
        problem = "Position funding projection does not reconcile to its immutable receipts"
    book_total = 0.
    for raw, in db.execute("SELECT body FROM event WHERE book=? AND kind='FUNDING'", (book["book_id"],)):
        try:
            value = json.loads(raw)
            if not _finite(value.get("cash_flow")):
                raise ValueError("Account funding journal contains invalid cash flows")
            book_total += value["cash_flow"]
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            problem = str(error)
    if not _finite(book.get("funding")) or abs(book_total-book["funding"]) > 1e-8:
        problem = "Account funding projection does not reconcile to its immutable receipts"
    return result, problem


def _selected(item, slot, entry, horizon, now):
    selected = item.get("selected")
    if item.get("state") != "AVAILABLE":
        return None, None
    if not isinstance(selected, dict):
        return None, "INVALID_SETTLED_RATE_EVIDENCE"
    try:
        actual, receipt = execution.stamp(selected["actual_at"]), execution.stamp(selected["received_at"])
        if (abs(actual-slot) > pd.Timedelta(seconds=5) or receipt < actual or receipt > now
                or not _finite(selected.get("rate")) or not re.fullmatch("[0-9a-f]{64}", selected.get("raw_sha256", ""))):
            raise ValueError("Settled rate has invalid timestamp, value or evidence hash")
        return selected, not entry < actual <= horizon
    except (ValueError, TypeError, KeyError, OverflowError):
        return None, "INVALID_SETTLED_RATE_EVIDENCE"


def assess(db, book, lane, body, now, metadata, generation=None):
    """Inspect bounded obligations and preserved evidence, without modifying state."""
    now = execution.stamp(now)
    result, slots, record = _context(book, lane, body, now, metadata, generation)
    if not record or result.get("actionable"):
        return result
    position = {**record, "book_id": book["book_id"]}
    applied, journal_error = _applied(db, position, now, book)
    done = set(record.get("settled_funding", []))
    expected = {slot.isoformat() for slot in slots}
    if set(applied)-expected or done-expected:
        journal_error = "Applied or settled funding lies outside the admitted frozen exposure schedule"
    if not slots:
        result.update(state="FINANCIAL_RECONCILIATION_BLOCKED" if journal_error else "NO_SETTLEMENT_DUE",
                      reason_code="FUNDING_FINANCIAL_PROJECTION_MISMATCH" if journal_error else None,
                      financial_complete=not journal_error, actionable=bool(journal_error), detail=journal_error,
                      next_action="Inspect the immutable funding journal and preserved schedule" if journal_error else
                      "Continue normal prospective monitoring")
        return result
    try:
        rates = _rates(body, slots, now)
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        rates = {slot.isoformat(): {"state": "UNAVAILABLE", "reason_code": "FUNDING_SOURCE_INSPECTION_FAILED",
                                    "detail": str(error)[:300]} for slot in slots}
    entry = execution.stamp(record["entry_at"])
    horizon = execution.stamp((record.get("exit_observed") or {}).get("exit_at", now))
    for slot in slots:
        stamp = slot.isoformat()
        rate = deepcopy(rates.get(stamp) or {"state": "UNAVAILABLE", "reason_code": "FUNDING_SOURCE_RESULT_MISSING"})
        selected, excluded = _selected(rate, slot, entry, horizon, now)
        reference, reference_error = _reference(record.get("funding_references", {}).get(stamp), lane, slot, now)
        receipt = applied.get(stamp)
        deadline = slot + execution.PROTECTION_GAP
        item = {"id": fingerprint([VERSION, generation, book["book_id"], record["id"], stamp, result["source_scope_id"]]),
                "book_id": book["book_id"], "position_id": record["id"], "source_scope_id": result["source_scope_id"],
                "slot": stamp, "settlement_due_at": stamp, "valuation_deadline": deadline.isoformat(),
                "rate_status": rate.get("state"), "rate_evidence": rate, "valuation_reference": reference,
                "valuation_status": "CAPTURED_FORWARD_REFERENCE" if reference else "INVALID_REFERENCE" if reference_error else
                "WINDOW_ELAPSED" if now > deadline else "AWAITING_FORWARD_REFERENCE",
                "valuation_error": reference_error, "applied_receipt": receipt,
                "first_eligible_collection_at": rate.get("first_eligible_collection_at"),
                "actionable": False, "financial_complete": False}
        if journal_error or stamp in done and not receipt and not (selected and excluded is True):
            state, reason = "FINANCIAL_RECONCILIATION_BLOCKED", "FUNDING_FINANCIAL_PROJECTION_MISMATCH"
            item["detail"] = journal_error or "Settled projection has no matching immutable FUNDING receipt"
        elif rate.get("state") == "CONFLICT":
            state, reason = "FUNDING_CONFLICT", rate.get("reason_code") or "FUNDING_RATE_CONFLICT"
        elif excluded == "INVALID_SETTLED_RATE_EVIDENCE":
            state, reason = "FUNDING_CONFLICT", excluded
        elif receipt and selected and (receipt["rate"] != selected["rate"] or
                ((receipt.get("evidence") or receipt.get("funding_evidence") or {}).get("rate", {}).get("actual_at")
                 not in (None, selected["actual_at"]))):
            state, reason = "FUNDING_CONFLICT", "APPLIED_FUNDING_CORRECTION_REVIEW_REQUIRED"
        elif receipt and reference and receipt["valuation_reference"] != reference["price"]:
            state, reason = "FUNDING_CONFLICT", "APPLIED_FUNDING_VALUATION_CONFLICT"
        elif receipt and selected:
            state, reason = "SETTLED", None
        elif selected and excluded is True:
            state, reason = "NOT_APPLICABLE_EXCHANGE_TIMESTAMP", "SETTLEMENT_OUTSIDE_ACTUAL_EXPOSURE"
        elif reference_error:
            state, reason = "FUNDING_EVIDENCE_MISSING", "INVALID_FORWARD_VALUATION_EVIDENCE"
        elif not reference and now > deadline:
            state, reason = "FUNDING_EVIDENCE_MISSING", "FORWARD_VALUATION_WINDOW_ELAPSED"
        elif rate.get("state") in ("MISSING", "UNAVAILABLE"):
            state, reason = "FUNDING_EVIDENCE_MISSING", rate.get("reason_code") or "SETTLED_RATE_EVIDENCE_UNAVAILABLE"
        elif receipt:
            state, reason = "FUNDING_EVIDENCE_MISSING", "APPLIED_RATE_REVALIDATION_UNAVAILABLE"
        else:
            state, reason = "FUNDING_SETTLEMENT_PENDING", rate.get("reason_code") or (
                "WAITING_FOR_SETTLED_RATE" if not selected else "WAITING_FOR_FORWARD_VALUATION")
        item.update(state=state, reason_code=reason, financial_complete=state in COMPLETE,
                    actionable=state in ("FUNDING_CONFLICT", "FUNDING_EVIDENCE_MISSING", "FINANCIAL_RECONCILIATION_BLOCKED"))
        if selected and reference and state == "FUNDING_SETTLEMENT_PENDING":
            item.update(ready_to_settle=True, reason_code="READY_FOR_AUTHORITATIVE_WRITER")
        if not reference and now > deadline:
            try:
                item["preserved_forward_candidates"] = _archived(db, lane, record, slot, now)
            except (OSError, ValueError, KeyError, TypeError, sqlite3.Error):
                item["preserved_forward_candidates"] = []
                item["archive_inspection"] = "UNAVAILABLE"
        item["next_action"] = ("Retain the immutable applied receipt and continue correction monitoring" if state == "SETTLED" else
            "Preserve the source actual timestamp excluding this position's exposure" if state == "NOT_APPLICABLE_EXCHANGE_TIMESTAMP" else
            "Wait for the existing bounded collector or original forward window; do not create a price" if not item["actionable"] else
            "Preview an evidence-bound preserved-forward restoration; no current mark or candle substitution is admitted" if reason == "FORWARD_VALUATION_WINDOW_ELAPSED" else
            "Inspect source hashes, dated schedule and financial receipts; corrections require explicit review and cannot rewrite applied funding")
        if receipt:
            item["economic_timing_interval"] = {"effective_settlement_at": stamp, "accounted_at": receipt["recorded_at"],
                "liquidation_economics": "NOT_RETROACTIVELY_VERIFIED",
                "limitation": "Accepted historical protection used the collateral then known; no retroactive exit is inferred"}
            item["research_provisional"] = (utc(receipt["recorded_at"]) > deadline or bool(
                (receipt.get("evidence", {}).get("valuation") or {}).get("recovery_label")))
            result["research_provisional"] |= item["research_provisional"]
        result["obligations"].append(item)
    priority = ("FINANCIAL_RECONCILIATION_BLOCKED", "FUNDING_CONFLICT", "FUNDING_EVIDENCE_MISSING", "FUNDING_SETTLEMENT_PENDING")
    problem = next((item for state in priority for item in result["obligations"] if item["state"] == state), None)
    result.update(state=problem["state"] if problem else "SETTLED",
                  reason_code=problem["reason_code"] if problem else None,
                  financial_complete=all(item["financial_complete"] for item in result["obligations"]),
                  actionable=any(item["actionable"] for item in result["obligations"]),
                  next_action=problem["next_action"] if problem else "Continue normal protection and prospective accounting")
    return result


def _transition(db, book, previous, value, now):
    for operation in ("UPDATE", "DELETE"):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS funding_status_{operation.lower()} BEFORE {operation} ON event "
                   f"WHEN OLD.kind='{TRANSITION}' BEGIN SELECT RAISE(ABORT,'Funding status journal is append-only'); END")
    old = {item["id"]: item for item in (previous or {}).get("obligations", [])}
    history = {}
    for raw, in db.execute("SELECT body FROM event WHERE book=? AND idea=? AND kind=? ORDER BY rowid",
                           (book["book_id"], value["position_id"], TRANSITION)):
        row = json.loads(raw)
        history[row["obligation_id"]] = row
    current = value["obligations"] or [{"id": value["status_obligation_id"],
                                       "slot": None, **{k: value.get(k) for k in ("state", "reason_code", "actionable", "financial_complete")}}]
    previous = previous or {}
    for item in current:
        prior = old.get(item["id"]) or history.get(item["id"]) or (previous if item["slot"] is None else {})
        def signature(row):
            rate = row.get("rate_evidence") or {}
            return fingerprint({"state": row.get("state"), "reason": row.get("reason_code"),
                "rate_status": row.get("rate_status"), "valuation_status": row.get("valuation_status"),
                "valuation_reference": row.get("valuation_reference"),
                "candidate_hashes": sorted({c.get("raw_sha256") for c in rate.get("candidates", []) if c.get("raw_sha256")}),
                "selected": rate.get("selected"), "receipt": (row.get("applied_receipt") or {}).get("event_id")})
        revision = signature(item)
        sequence = prior.get("transition_sequence", 0)
        item["transition_sequence"] = sequence
        item["evidence_revision"] = revision
        if prior.get("evidence_revision", signature(prior)) == revision:
            continue
        item["transition_sequence"] = sequence + 1
        item["evidence_revision"] = revision
        body = {"schema": VERSION, "generation": value["generation"], "book_id": book["book_id"],
                "position_id": value["position_id"], "source_scope_id": value["source_scope_id"],
                "obligation_id": item["id"], "previous_state": prior.get("state"), "evidence_revision": revision,
                "transition_sequence": sequence + 1,
                **{k: item.get(k) for k in ("slot", "state", "reason_code", "actionable", "financial_complete",
                                          "valuation_deadline", "rate_status", "valuation_status", "next_action")}}
        # A repeated state/evidence revision is one immutable transition, even
        # when an identical observation is scanned after a process restart.
        key = fingerprint([VERSION, TRANSITION, item["id"], sequence + 1, revision])
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
        row = db.execute("SELECT body FROM event WHERE key=?", (key,)).fetchone()
        if row and row[0] != encoded:
            raise ValueError("Funding transition identity conflicts with immutable evidence")
        if not row:
            db.execute("INSERT INTO event VALUES (?,?,?,?,?,?,?)", (key, book["book_id"], TRANSITION,
                       value["position_id"], now.isoformat(), now.isoformat(), encoded))


def settle(db, book, lane, body, now, metadata, observation=None, generation=None):
    """Apply only evidenced original-model costs inside the caller's transaction."""
    if not db.in_transaction:
        raise ValueError("Funding settlement requires the authoritative writer transaction")
    now = execution.stamp(now)
    initial, slots, record = _context(book, lane, body, now, metadata, generation)
    if not record:
        return False
    previous = deepcopy(record.get("funding_state"))
    receipts = record.setdefault("funding_references", {})
    if observation and not execution.valid_snapshot(observation, lane, *execution.clock()):
        for slot in slots:
            if slot <= execution.stamp(observation["source_at"]) <= slot + execution.PROTECTION_GAP:
                candidate = {**deepcopy(observation), "captured_at": now.isoformat(),
                             "capture_kind": "TIMELY_FORWARD_SNAPSHOT", "capture_evidence_sha256": fingerprint(observation)}
                receipts.setdefault(slot.isoformat(), candidate)
    value = assess(db, book, lane, body, now, metadata, generation)
    failures = {}
    verified_done = {item["slot"] for item in value["obligations"] if item["state"] == "SETTLED" and item["applied_receipt"]}
    if verified_done:
        record["settled_funding"] = sorted(set(record.get("settled_funding", [])) | verified_done)
    for item in value["obligations"]:
        if item.get("ready_to_settle"):
            selected = item["rate_evidence"]["selected"]
            reference = item["valuation_reference"]
            evidence = {"schema": VERSION, "generation": generation, "obligation_id": item["id"],
                        "source_scope_id": value["source_scope_id"], "source_identity": value["source_identity"],
                        "rate": deepcopy(selected), "valuation": deepcopy(reference),
                        "schedule": deepcopy(value["schedule"]),
                        "economic_effect_known_at": now.isoformat(),
                        "limitation": "Late rate receipt changes current collateral; it does not prove earlier liquidation monitoring used this rate"}
            pos = Position(**record["position"])
            try:
                execution.funding(db, book, record, pos, utc(item["slot"]), selected["received_at"],
                                  selected["rate"], reference["price"], now, BASIS, evidence=evidence)
            except ValueError as error:
                failures[item["id"]] = str(error)[:400]
                continue
            record["settled_funding"] = sorted(set(record.get("settled_funding", [])) | {item["slot"]})
            if (reference.get("recovery_label") or reference.get("historical_reconciliation")
                    or now > utc(item["valuation_deadline"])):
                record["funding_provisional"] = True
    value = assess(db, book, lane, body, now, metadata, generation)
    if failures:
        for item in value["obligations"]:
            if item["id"] in failures:
                item.update(state="FINANCIAL_RECONCILIATION_BLOCKED", reason_code="FUNDING_LEDGER_PROJECTION_MISMATCH",
                            detail=failures[item["id"]], financial_complete=False, actionable=True,
                            next_action="Preserve financial state and inspect immutable funding receipts versus account projections")
        value.update(state="FINANCIAL_RECONCILIATION_BLOCKED", reason_code="FUNDING_LEDGER_PROJECTION_MISMATCH",
                     financial_complete=False, actionable=True,
                     next_action="Inspect the exact funding ledger/projection disagreement; accepted-price protection must continue")
    _transition(db, book, previous, value, now)
    if value["research_provisional"]:
        record["funding_provisional"] = True
        record["funding_timing_limitations"] = [
            {"obligation_id": item["id"], "slot": item["slot"], **item["economic_timing_interval"]}
            for item in value["obligations"] if item.get("research_provisional") and item.get("economic_timing_interval")]
    record["funding_state"] = value
    record["unresolved_costs"] = None if value["financial_complete"] else value["reason_code"] or value["state"]
    if record.get("exit_observed") and value["financial_complete"]:
        execution.close(db, book, lane, now)
    return not value["financial_complete"]
