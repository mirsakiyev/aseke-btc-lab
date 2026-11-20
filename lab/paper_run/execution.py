"""Commit references and reconcile paper balances without candle execution proxies."""

from dataclasses import asdict
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from math import isfinite
import time

import pandas as pd

from lab.learning.simulator import Contract, Cost, Position, liquidation_price, triggers
from lab.config import fingerprint
from lab.phase15 import paper
from lab.paper_run import protection_incidents as incidents
from lab.storage import utc, utc_now


AGE_NS = 2_000_000_000
CLOCK_TOLERANCE_NS = 100_000_000
PROTECTION_GAP = pd.Timedelta(seconds=45)
WS_TRADE = "WEBSOCKET_TRADE"


def clock():
    return utc_now(), time.monotonic_ns()


def stamp(value):
    value = pd.Timestamp(value)
    if pd.isna(value) or value.tzinfo is None:
        raise ValueError("Explicit exchange and local time offsets required")
    return utc(value)


def valid_snapshot(obs, lane, at, mono):
    """Validate exchange-to-commit age and wall vs monotonic progress."""
    if not obs or obs.get("status", "RECEIVED") != "RECEIVED":
        return (obs or {}).get("status", "MISSING_REFERENCE")
    try:
        if obs["source"] != lane["source"] or obs["symbol"] != lane["symbol"] or not obs["event_id"]:
            return "WRONG_REFERENCE_SOURCE"
        price = Decimal(obs["price_text"])
        if (not price.is_finite() or price <= 0 or isinstance(obs["price"], bool)
                or not isfinite(obs["price"]) or float(price) != obs["price"]):
            return "INVALID_REFERENCE_PRICE"
        source, receipt = (stamp(obs[key]) for key in ("source_at", "received_at"))
        kind = obs.get("observation_kind", "HTTP_SNAPSHOT")
        if kind != lane.get("observation_kind", "HTTP_SNAPSHOT"):
            return "UNAPPROVED_OBSERVATION_TRANSPORT"
        if kind == WS_TRADE:
            if type(obs["event_id"]) is not str or not obs["event_id"].isdigit() or int(obs["event_id"]) < 1:
                return "MALFORMED_TRADE_SEQUENCE"
            requested = stamp(obs["subscription_requested_at"])
            anchor = stamp(obs["subscription_ack_at"])
            recv_mono, anchor_mono = obs["received_mono_ns"], obs["subscription_ack_mono_ns"]
            if not requested <= anchor <= receipt <= at or not requested <= source <= receipt:
                return "FUTURE_OR_INCONSISTENT_CLOCK"
            if not all(type(value) is int for value in (anchor_mono, recv_mono, mono)):
                return "MISSING_MONOTONIC_CLOCK"
            if not anchor_mono <= recv_mono <= mono:
                return "INCONSISTENT_MONOTONIC_CLOCK"
            handling_elapsed = mono - recv_mono
            if (abs((receipt - anchor).value - (recv_mono - anchor_mono)) > CLOCK_TOLERANCE_NS
                    or abs((at - receipt).value - handling_elapsed) > CLOCK_TOLERANCE_NS):
                return "LOCAL_CLOCK_STEP"
            if max((at - source).value, (receipt - source).value + handling_elapsed) > AGE_NS:
                return "STALE_REFERENCE"
            return None
        requested = stamp(obs["requested_at"])
        sent_mono, recv_mono = (obs[key] for key in ("request_mono_ns", "received_mono_ns"))
        if not all(type(value) is int for value in (sent_mono, recv_mono, mono)):
            return "MISSING_MONOTONIC_CLOCK"
        if not requested <= receipt <= at or not source <= receipt:
            return "FUTURE_OR_INCONSISTENT_CLOCK"
        request_elapsed = recv_mono - sent_mono
        handling_elapsed = mono - recv_mono
        if min(request_elapsed, handling_elapsed) < 0 or obs["elapsed_ns"] != request_elapsed:
            return "INCONSISTENT_MONOTONIC_CLOCK"
        if (abs((receipt - requested).value - request_elapsed) > CLOCK_TOLERANCE_NS
                or abs((at - receipt).value - handling_elapsed) > CLOCK_TOLERANCE_NS):
            return "LOCAL_CLOCK_STEP"
        if max((at - source).value, (receipt - source).value + handling_elapsed,
               request_elapsed + handling_elapsed) > AGE_NS:
            return "STALE_REFERENCE"
    except (KeyError, InvalidOperation, TypeError, ValueError, OverflowError):
        return "MALFORMED_REFERENCE"
    return None


def evidence(obs, decision, committed, decision_mono, committed_mono):
    return {"price": obs["price"], "reference_price_text": obs["price_text"], "source": obs["source"],
            "symbol": obs["symbol"], "source_event_id": obs["event_id"], "exchange_source_at": obs["source_at"],
            "source_received_at": obs["received_at"], "requested_at": obs.get("requested_at"),
            "observation_kind": obs.get("observation_kind", "HTTP_SNAPSHOT"),
            "subscription_epoch": obs.get("subscription_epoch"),
            "subscription_requested_at": obs.get("subscription_requested_at"),
            "subscription_ack_at": obs.get("subscription_ack_at"),
            "paper_decision_at": decision.isoformat(), "committed_at": committed.isoformat(),
            "request_elapsed_ns": obs.get("elapsed_ns"), "raw_sha256": obs.get("raw_sha256"),
            "receipt_mono_ns": obs["received_mono_ns"], "validation_mono_ns": decision_mono,
            "commit_mono_ns": committed_mono,
            "receipt_to_validation_ns": decision_mono - obs["received_mono_ns"],
            "receipt_to_commit_ns": committed_mono - obs["received_mono_ns"],
            "validation_to_commit_ns": committed_mono - decision_mono,
            "observation_age_at_commit_ns": (committed - stamp(obs["source_at"])).value,
            "entry_adverse_rate": 0., "exit_adverse_rate": 0., "liquidity_verified": False}


def finish(db, book, now, kind, reason):
    if not book["pending"]:
        return
    paper.event(db, book, kind, book["pending"]["id"], now, now, {"reason": reason})
    book.update(pending=None, cursor=None)


def pending_ready(db, book, now):
    idea = book["pending"]
    if not idea:
        return False
    if now >= utc(idea["plan"]["entry_expiry"]):
        finish(db, book, now, "EXPIRED", idea.get("last_rejection", "NO_QUALIFYING_FORWARD_REFERENCE"))
        return False
    return (not book["position"] and incidents.admission_allowed(book, now)
            and utc(idea["plan"]["eligible_from"]) <= now)


def reject(db, book, now, reason):
    idea = book["pending"]
    if idea.get("last_rejection") != reason:
        paper.event(db, book, "ENTRY_EVIDENCE_REJECTED", idea["id"], now, now, {"reason": reason})
        idea["last_rejection"] = reason


def entry(db, book, lane, obs, terms_at_price, *, read_clock=clock):
    decision, mono = read_clock()
    decision_mono = mono
    if not pending_ready(db, book, decision):
        return
    if reason := valid_snapshot(obs, lane, decision, mono):
        return reject(db, book, decision, reason)
    idea = book["pending"]
    plan = idea["plan"]
    earliest = utc(plan["eligible_from"])
    local_start = obs["received_at"] if obs.get("observation_kind") == WS_TRADE else obs["requested_at"]
    if min(stamp(obs["source_at"]), stamp(local_start)) < earliest or stamp(obs["source_at"]) <= utc(idea["published_at"]):
        return reject(db, book, decision, "REFERENCE_PRECEDES_ELIGIBILITY")
    price, side, stop = obs["price"], plan["direction"], plan["stop"]
    if type(side) is not int or side not in (-1, 1) or (lane["long_only"] and side != 1):
        return finish(db, book, decision, "INVALIDATED", "DIRECTION_OUTSIDE_POLICY")
    if side * (price - stop) <= 0:
        return finish(db, book, decision, "INVALIDATED", "REFERENCE_CROSSED_STOP")
    band = plan["entry"]
    if band.get("trigger") is not None and side * (price - band["trigger"]) < 0:
        return reject(db, book, decision, "REFERENCE_HAS_NOT_REACHED_TRIGGER")
    if not band["minimum"] <= price <= band["maximum"]:
        return finish(db, book, decision, "INVALIDATED", "REFERENCE_OUTSIDE_FROZEN_RANGE")
    reason, terms = terms_at_price(book, lane, plan, price, decision)
    if reason:
        return finish(db, book, decision, "NO_FILL", reason)
    committed, mono = read_clock()
    if not pending_ready(db, book, committed):
        return
    if committed < decision:
        return reject(db, book, decision, "LOCAL_CLOCK_STEP")
    if reason := valid_snapshot(obs, lane, committed, mono):
        return reject(db, book, committed, f"COMMIT_{reason}")
    deadline = committed + pd.Timedelta(minutes=plan["max_hold_minutes"])
    if plan.get("session_flat"):
        deadline = min(deadline, utc(plan["session_flat"]))
    if deadline <= committed:
        return finish(db, book, committed, "NO_FILL", "SESSION_ALREADY_FLAT")

    fee, quantity, margin, notional = (terms[key] for key in ("fee", "quantity", "margin", "notional"))
    pos = Position(side, quantity, price, book["cash"] - fee, margin, fee, notional)
    entry_at = committed.isoformat()
    detail = {**terms, **evidence(obs, decision, committed, decision_mono, mono), "entry_at": entry_at, "entry_committed_at": entry_at,
              "entry_basis": lane["entry_model"], "entry_equity": book["cash"], "stop": stop,
              "deadline": deadline.isoformat(), "signal_boundary": idea["boundary"],
              "confirmed_at": idea["published_at"], "eligible_from": earliest.isoformat(),
              "source_amendment_id": lane.get("source_amendment_id")}
    provenance = incidents.dependency(book)
    if provenance:
        detail["research_provenance"] = provenance
    position = {**idea, "entry_at": entry_at, "raw_entry": price, "entry_fee": fee, "entry_cash": book["cash"],
                "stop": stop, "target": terms["target"], "deadline": deadline.isoformat(), "last_trail": None,
                "planned_risk": terms["planned_risk"], "position": asdict(pos), "last_mark": price,
                "mark_source_at": obs["source_at"], "mark_received_at": obs["received_at"],
                "mark_event_id": obs["event_id"], "entry_evidence": detail}
    if provenance:
        position["research_provenance"] = provenance
    paper.event(db, book, "HYPOTHETICAL_FILL", idea["id"], committed, committed, detail)
    book.update(position=position, pending=None, cursor=None, fees=book["fees"] + fee, turnover=book["turnover"] + notional)
    paper.mark_account(book, pos, price)


def missing_protection(db, book, at, reason, *, lane=None, source_scope_id=None, correlation_id=None):
    record = book["position"]
    start = record.get("mark_received_at", record["entry_at"])
    if at - utc(start) <= PROTECTION_GAP or record.get("exit_observed"):
        return
    if incidents.active(db):
        incidents.open_incident(db, book, at, reason, lane=lane, source_scope_id=source_scope_id,
                                correlation_id=correlation_id)
        return
    gap = record.get("protection_gap")
    if not gap or gap.get("ended_at"):
        gap = {"started_after": start, "last_checked": at.isoformat(), "reason": reason}
        paper.event(db, book, "PROTECTION_GAP", record["id"], at, at, gap)
        record["protection_gap"] = gap
    else:
        gap["last_checked"] = at.isoformat()
    record["unresolved_protection"] = True


def funding(db, book, record, pos, slot, receipt, rate, reference, now, basis, *, evidence=None):
    """Apply once by immutable receipt, even if a mutable done projection was lost.

    Receipt disagreement is a reconciliation failure. It is never authority to
    repeat or silently amend a financial flow. Callers retain protection while
    presenting that separate financial failure for review.
    """
    if rate is None or reference is None:
        return
    if (any(type(x) not in (int, float) or not isfinite(x) for x in (rate, reference)) or reference <= 0):
        raise ValueError("FUNDING_INVALID_FINANCIAL_INPUT")
    if (type(pos.direction) is not int or pos.direction not in (-1, 1)
            or any(type(x) not in (int, float) or not isfinite(x) for x in
                   (pos.remaining, pos.wallet, pos.reserved, pos.funding, book.get("funding")))
            or pos.remaining <= 0):
        raise ValueError("FUNDING_INVALID_ACCOUNT_PROJECTION")
    key = fingerprint([book["book_id"], "FUNDING", record["id"], slot.isoformat()])
    rows = db.execute("SELECT key,body FROM event WHERE book=? AND kind='FUNDING' AND idea=? AND source_at=?",
                      (book["book_id"], record["id"], slot.isoformat())).fetchall()
    if len(rows) > 1 or (rows and rows[0][0] != key):
        raise ValueError("FUNDING_DUPLICATE_RECEIPT_IDENTITY")
    import json

    book_total, position_total = 0., 0.
    for identifier, raw in db.execute("SELECT idea,body FROM event WHERE book=? AND kind='FUNDING'", (book["book_id"],)):
        amount = json.loads(raw).get("cash_flow")
        if type(amount) not in (int, float) or not isfinite(amount):
            raise ValueError("FUNDING_INVALID_IMMUTABLE_RECEIPT")
        book_total += amount
        if identifier == record["id"]:
            position_total += amount
    if abs(book_total - book["funding"]) > 1e-8 or abs(position_total - pos.funding) > 1e-8:
        raise ValueError("FUNDING_LEDGER_PROJECTION_MISMATCH")
    if rows:
        prior = json.loads(rows[0][1])
        if prior.get("rate") != rate or prior.get("valuation_reference") != reference or prior.get("basis") != basis:
            raise ValueError("FUNDING_RECEIPT_EVIDENCE_CONFLICT")
        return {"state": "ALREADY_APPLIED", "event_id": key}
    amount = -pos.direction * pos.remaining * reference * rate
    if not all(isfinite(x) for x in (amount, pos.funding + amount, pos.reserved + amount, pos.wallet + amount,
                                     book["funding"] + amount)):
        raise ValueError("FUNDING_NONFINITE_FINANCIAL_RESULT")
    event = {"rate": rate, "valuation_reference": reference, "basis": basis, "cash_flow": amount,
             "settlement_at": slot.isoformat(), "source_received_at": receipt}
    if evidence is not None:
        if not isinstance(evidence, dict):
            raise ValueError("FUNDING_EVIDENCE_MUST_BE_STRUCTURED")
        json.dumps(evidence, allow_nan=False)
        event.update(protocol="FUNDING_SETTLEMENT_RECONCILIATION_V1", evidence=deepcopy(evidence),
                     applied_at=now.isoformat(), valuation_model="ORIGINAL_FORWARD_MARK_REFERENCE")
    paper.event(db, book, "FUNDING", record["id"], slot, now, event)
    pos.fund(rate, reference)
    book["funding"] += amount
    record["position"] = asdict(pos)
    # A real financial receipt changes cash even if the next price observation
    # is temporarily unavailable. Revalue only at the already recorded mark;
    # its source/receipt timestamps are retained and no fill is manufactured.
    paper.mark_account(book, pos, record["last_mark"])
    return {"state": "APPLIED", "event_id": key}


def close(db, book, lane, now):
    """Reconcile a previously committed reference exit once cost evidence is complete."""
    record = book["position"]
    result = record["exit_observed"]
    pos = Position(**record["position"])
    contract = Contract.load(record["plan"]["contract"])
    cost = Cost(record["plan"]["cost"]["taker"], 0.)
    units, price = pos.remaining, result["price"]
    forced = result["reason"] == "liquidation"
    pos.reduce(units, price, cost, contract.liquidation_fee_rate if forced else 0., liquidated=forced)
    net = pos.wallet - record["entry_cash"]
    provisional = bool(record.get("unresolved_protection") or record.get("funding_provisional"))
    payload = {**result, "entry_at": record["entry_at"], "entry_price": pos.entry, "exit_price": price,
               "exit_kind": result["reason"], "execution_basis": lane["exit_model"], "trigger_basis": lane["price_basis"],
               "equity_before": record["entry_cash"], "equity_after": pos.wallet, "net": net,
               "net_r": net / record["planned_risk"], "planned_risk": record["planned_risk"],
               "gross_price_pnl": pos.realized, "gross_before_adverse": pos.realized, "adverse_cost_in_gross": 0.,
               "fees": pos.fees, "funding": pos.funding, "liquidation_cost": pos.liquidation_cost,
               "unfunded_deficit": pos.unfunded_deficit, "turnover": pos.turnover, "provisional": provisional,
               "holding_minutes": (utc(result["exit_at"]) - utc(record["entry_at"])).total_seconds() / 60}
    if record.get("funding_provisional"):
        payload.update(funding_provisional=True,
                       funding_reconciliation=deepcopy(record.get("funding_reconciliation")),
                       funding_timing_limitations=deepcopy(record.get("funding_timing_limitations")))
    if record.get("research_provenance"):
        payload["research_provenance"] = record["research_provenance"]
    paper.event(db, book, "CLOSED", record["id"], utc(result["exit_at"]), now, payload)
    book["gross"] += pos.realized
    book["raw_gross"] += pos.realized
    book["fees"] += pos.fees - record["entry_fee"]
    book["turnover"] += price * units
    book["liquidation_cost"] += pos.liquidation_cost
    book["unfunded_deficit"] += pos.unfunded_deficit
    paper.mark_account(book, pos, price)
    book.update(position=None, cursor=None,
                problem="UNRESOLVED_PROTECTION_INTERVAL" if record.get("unresolved_protection") else None)


def protect(db, book, lane, obs, *, unresolved_costs=False, read_clock=clock, source_context=None):
    record = book["position"]
    if not record or record.get("exit_observed"):
        return
    decision, mono = read_clock()
    decision_mono = mono
    reason = valid_snapshot(obs, lane, decision, mono)
    if reason:
        if record.get("last_protection_rejection") != reason:
            paper.event(db, book, "PROTECTION_SAMPLE_REJECTED", record["id"], decision, decision, {"reason": reason})
            record["last_protection_rejection"] = reason
        return missing_protection(db, book, decision, reason, **(source_context or {}))
    source = stamp(obs["source_at"])
    prior_source = utc(record["mark_source_at"])
    replayed_trade = (obs.get("observation_kind") == WS_TRADE and str(record.get("mark_event_id", "")).isdigit()
                      and int(obs["event_id"]) <= int(record["mark_event_id"]))
    if source <= utc(record["entry_at"]) or source < prior_source or (
            source == prior_source and obs.get("observation_kind") != WS_TRADE) or replayed_trade:
        return missing_protection(db, book, decision, "REFERENCE_REPLAY_OR_PRECEDES_ENTRY", **(source_context or {}))
    price = obs["price"]
    pos = Position(**record["position"])
    contract = Contract.load(record["plan"]["contract"])
    liq = liquidation_price(pos.entry, pos.reserved / pos.remaining, pos.direction, contract,
                            record["plan"]["cost"]["taker"]) if contract.family == "linear_perp" else None
    effective_stop = record["stop"]
    if record.get("stop_effective_at") and source < utc(record["stop_effective_at"]):
        effective_stop = record["prior_stop"]
    hits = triggers((price,) * 4, pos.direction, effective_stop, record["target"], liq, True)
    deadline = utc(record["deadline"])
    action = hits[0][0] if hits and hits[0][0] == "liquidation" else (
        "time_exit" if source >= deadline else hits[0][0] if hits else None)
    committed, mono = read_clock()
    if committed < decision or (reason := valid_snapshot(obs, lane, committed, mono)):
        return missing_protection(db, book, max(decision, committed), reason or "LOCAL_CLOCK_STEP", **(source_context or {}))
    missing_protection(db, book, committed, "ELAPSED_WITHOUT_QUALIFYING_REFERENCE", **(source_context or {}))
    if incidents.active(db):
        incidents.recovered(db, book, obs, committed, mono)
    elif gap := record.get("protection_gap"):
        if not gap.get("ended_at"):
            gap["ended_at"] = committed.isoformat()
            paper.event(db, book, "PROTECTION_RESUMED", record["id"], committed, committed, gap)
    if contract.family == "linear_perp" and pos.remaining * price > contract.tier_notional:
        record["unresolved_protection"] = True
        book.pop("prospective_admission", None)
        paper.event(db, book, "UNMODELED_TIER", record["id"], committed, committed,
                    {"observed_reference": price, "reason_code": "UNMODELED_TIER",
                     "source": lane["source"], "symbol": lane["symbol"], "detected_at": committed.isoformat(),
                     "historical_coverage": "UNRESOLVED", "requires_incident_review": True,
                     "next_action": "Review the actual instrument tier and interval; do not authorize new exposure with unsupported terms."})
        if policy := incidents.active(db):
            from lab.paper_run import operations

            operations.record_fault(db, {"id": policy["generation"]}, lane, book, committed,
                                    "UNMODELED_TIER", details={"observed_reference": price})
    record.update(last_mark=price, mark_source_at=obs["source_at"], mark_received_at=obs["received_at"], mark_event_id=obs["event_id"],
                  last_protection_rejection=None)
    if record.get("last_protection_reference", {}).get("event_id") != obs["event_id"]:
        record["last_protection_reference"] = {**obs, "protection_validated_at": committed.isoformat(),
                                                "protection_validation_mono_ns": mono}
    paper.mark_account(book, pos, price)
    if not action:
        return
    observed = {**evidence(obs, decision, committed, decision_mono, mono), "reason": action, "exit_at": committed.isoformat(),
                "exit_committed_at": committed.isoformat(), "effective_stop": effective_stop, "target": record["target"],
                "source_amendment_id": lane.get("source_amendment_id")}
    record["exit_observed"] = observed
    paper.event(db, book, "EXIT_OBSERVED", record["id"], committed, committed, observed)
    if not unresolved_costs:
        close(db, book, lane, committed)
