"""Preview-first operator actions for prospective paper protection recovery."""

from contextlib import closing
from dataclasses import asdict
import getpass
import json
from math import isfinite

import pandas as pd

from lab.config import fingerprint
from lab.learning.simulator import Position
from lab.paper_run import protection_incidents as incidents
from lab.phase15 import paper
from lab.storage import read_json, utc, utc_now, writer_lock


def _state(db, record, slot):
    from lab.paper_run import source_amendment

    lanes = source_amendment.lanes(record, source_amendment.active(record))
    if slot not in lanes:
        raise ValueError("Unknown current paper book")
    lane = lanes[slot]
    book = paper.load(db, lane)
    records = incidents.project(db, book["book_id"]) + incidents.legacy_incidents(db, book, record["id"])
    position = book.get("position") or {}
    pending = book.get("pending") or {}
    token = fingerprint([record["id"], book["book_id"], position.get("id"), pending.get("id"),
                         book["cash"], book["fees"], book["funding"], book["gross"], book["problem"],
                         position.get("stop"), position.get("target"), position.get("deadline"),
                         [(item["incident_id"], item.get("latest_event", item.get("legacy_event_key"))) for item in records]])
    return lane, book, records, token


def report(record, slot):
    from lab.paper_run import runtime

    with closing(runtime.connect(readonly=True)) as db:
        db.execute("BEGIN")
        lane, book, records, token = _state(db, record, slot)
        return {"mode": "READ_ONLY_REVIEW", "policy": incidents.POLICY, "generation": record["id"],
                "slot": slot, "book_id": lane["book_id"], "review_token": token,
                "policy_active": bool(incidents.active(db)), "incidents": records,
                "position_id": (book.get("position") or {}).get("id"),
                "pending": bool(book.get("pending")), "historical_problem": book.get("problem"),
                "nominal_cash": book["cash"], "prospective_admission": book.get("prospective_admission"),
                "note": "Unknown historical recovery timestamps remain unknown"}


def _position(db, book, identifier, *, through=None, require_static=True):
    current = book.get("position") or {}
    rows = db.execute("SELECT kind,source_at,body FROM event WHERE book=? AND idea=? ORDER BY rowid",
                      (book["book_id"], identifier)).fetchall()
    changes = [(kind, at) for kind, at, _ in rows
               if kind in ("FUNDING", "UNMODELED_TIER", "STOP_UPDATED", "TRAILING_STOP_UPDATED")]
    if require_static and any(through is None or utc(at) < utc(through) for _, at in changes):
        raise ValueError("Historical changes require a segmented position evidence review")
    if current.get("id") == identifier and not changes:
        return current
    details = {kind: json.loads(raw) for kind, _, raw in rows if kind in ("PUBLISHED", "HYPOTHETICAL_FILL")}
    if set(details) != {"PUBLISHED", "HYPOTHETICAL_FILL"}:
        raise ValueError("Frozen position evidence is incomplete")
    fill = details["HYPOTHETICAL_FILL"]
    plan = details["PUBLISHED"]["plan"]
    quantity = fill["quantity"]
    position = Position(plan["direction"], quantity, fill["price"], fill["entry_equity"] - fill["fee"],
                        fill["margin"], fill["fee"], fill["notional"])
    return {"id": identifier, "entry_at": fill["entry_at"], "entry_evidence": fill, "plan": plan,
            "stop": fill["stop"], "target": fill["target"], "deadline": fill["deadline"],
            "position": asdict(position)}


def _healthy(record, lane):
    from lab.paper_run import execution, runtime

    now, mono = execution.clock()
    if not runtime.synchronized(now) or lane["qualification"]["state"] != "QUALIFIED":
        raise ValueError("Current clock or source qualification is unavailable")
    view = runtime.status()
    if lane["source"] == "KRAKEN_BTC_USD_LAST_TRADE" and view.get("ema_live_feed", {}).get("state") != "STREAMING":
        raise ValueError("Current Kraken transport is not streaming")
    if lane["source"] == "KRAKEN_BTC_USD_LAST_TRADE":
        runtime.kraken_live.permission(now)
    elif runtime._source_permission(lane["source"], now, entry_only=True):
        raise ValueError("Current source permission is unavailable")
    heartbeat = view.get("last_cycle") or {}
    if (heartbeat.get("generation") != record["id"] or heartbeat.get("runner_build_id") != runtime.RUNNER_BUILD
            or not 0 <= (now - utc(heartbeat["at"])).total_seconds() < 45):
        raise ValueError("A fresh managed runner on the reviewed build is required")
    source = view["books"][lane["slot"]].get("last_observation") if "slot" in lane else None
    if source is None:
        source = next((item.get("last_observation") for item in view["books"].values()
                       if item["book_id"] == lane["book_id"]), None)
    if reason := execution.valid_snapshot(source, lane, now, mono):
        raise ValueError(f"Current protection reference is unavailable: {reason}")
    return {"at": now.isoformat(), "runner_pid": heartbeat["pid"], "build": runtime.RUNNER_BUILD,
            "source_event_id": source["event_id"], "source_at": source["source_at"],
            "received_at": source["received_at"], "clock_and_reference_checks": "PASSED"}


def _validate_request(request, record):
    if not isinstance(request, dict):
        raise ValueError("Protection request must be a JSON object")
    if request.get("generation") != record["id"]:
        raise ValueError("Protection request generation does not match")
    if not all(isinstance(request.get(key), str) and request[key].strip()
               for key in ("request_id", "reason", "review_token", "slot")):
        raise ValueError("Request ID, review token, slot and reason are required")


def _prior(db, request, action):
    for row in incidents._events(db):
        body = row["body"]
        if body.get("request_id") == request["request_id"]:
            if body.get("request_sha256") != fingerprint([action, request]):
                raise ValueError("Request ID was reused with different arguments")
            return {"state": "ALREADY_APPLIED", "event_id": row["key"], "record": body}
    return None


def _activate(db, record, request, book, at, operator):
    from lab.paper_run import runtime

    if incidents.active(db):
        raise ValueError("This policy is already active")
    states = [json.loads(raw) for raw, in db.execute("SELECT state FROM book")]
    if len(states) != 7 or any(item["position"] or item["pending"] for item in states):
        raise ValueError("Activation requires all seven accounts flat with no pending entry")
    for state in states:
        amounts = [state[key] for key in ("cash", "starting_equity", "gross", "fees", "funding",
                                          "liquidation_cost", "unfunded_deficit")]
        if not all(type(value) in (float, int) and isfinite(value) for value in amounts):
            raise ValueError("Activation requires finite account evidence")
        expected = state["starting_equity"] + state["gross"] + state["funding"] - state["fees"] - state["liquidation_cost"] + state["unfunded_deficit"]
        if abs(state["cash"] - expected) > 1e-8:
            raise ValueError("Activation requires reconciled nominal accounts")
    # Confirm adopted code before activating a policy on the current generation.
    view = runtime.status()
    heartbeat = view.get("last_cycle") or {}
    if (heartbeat.get("runner_build_id") != runtime.RUNNER_BUILD or heartbeat.get("generation") != record["id"]
            or not 0 <= (at - utc(heartbeat["at"])).total_seconds() < 45):
        raise ValueError("The reviewed implementation must first be adopted by the managed runner")
    return {"generation": record["id"], "policy": incidents.POLICY, "operator": operator,
            "authorized_at": at.isoformat(), "runner_build": runtime.RUNNER_BUILD}


def _recovery_bound(db, book, incident):
    if end := incident["interval"].get("ends_at"):
        return end, "FIRST_VALIDATED_RECOVERY_RECEIPT"
    # Repeating an identical rejection requires protect() to have cleared it
    # after an accepted reference in between. The later rejection is an upper
    # bound, never a fabricated first recovery timestamp.
    previous = None
    for at, raw in db.execute("SELECT recorded_at,body FROM event WHERE book=? AND idea=? "
                              "AND kind='PROTECTION_SAMPLE_REJECTED' ORDER BY rowid",
                              (book["book_id"], incident["position_id"])):
        if utc(at) <= utc(incident["detected_at"]):
            continue
        reason = json.loads(raw).get("reason")
        if reason and reason == previous:
            return at, "LEGACY_REJECTION_TRANSITIONS_PROVE_ACCEPTED_BY"
        previous = reason
    position = book.get("position") or {}
    if position.get("id") == incident["position_id"] and position.get("mark_received_at"):
        if utc(position["mark_received_at"]) > utc(incident["detected_at"]):
            return position["mark_received_at"], "CURRENT_ACCEPTED_REFERENCE_UPPER_BOUND"
    for raw, in db.execute("SELECT body FROM event WHERE book=? AND idea=? AND kind='EXIT_OBSERVED' ORDER BY rowid",
                           (book["book_id"], incident["position_id"])):
        receipt = json.loads(raw).get("source_received_at")
        if receipt and utc(receipt) > utc(incident["detected_at"]):
            return receipt, "COMMITTED_EXIT_REFERENCE_UPPER_BOUND"
    return None, "RECOVERY_BOUND_UNKNOWN"


def _adjudicate(db, record, lane, book, records, request):
    from lab.paper_run.protection_evidence import validate_evidence

    incident = next((item for item in records if item["incident_id"] == request.get("incident_id")), None)
    if not incident:
        raise ValueError("Select an incident from the current review report")
    if not isinstance(request.get("evidence"), dict):
        raise ValueError("An explicit evidence manifest is required")
    start = incident["interval"].get("starts_after")
    bound, bound_basis = _recovery_bound(db, book, incident)
    end = request.get("review_interval_end") or bound
    if bound and end and utc(end) < utc(bound):
        raise ValueError("The review interval excludes part of the evidenced incident")
    if not bound and request["evidence"]["outcome"] != "INSUFFICIENT_EVIDENCE":
        raise ValueError("Historical recovery has no trusted upper bound")
    if not start or not end or not utc(start) < utc(end) <= utc_now():
        raise ValueError("An evidence-bounded review interval is required; recovery time may remain unknown")
    position = _position(db, book, incident["position_id"], through=end,
                         require_static=request["evidence"]["outcome"] != "INSUFFICIENT_EVIDENCE")
    context = {"generation": record["id"], "book_id": book["book_id"], "position_id": incident["position_id"],
               "source": lane["source"], "symbol": lane["symbol"], "trigger_basis": lane["source"],
               "stream_id": position["plan"]["evidence"]["source_id"],
               "interval_start": start, "interval_end": end, "position": position}
    evidence = validate_evidence(request["evidence"], context)
    return {"generation": record["id"], "incident_id": incident["incident_id"],
            "recovery_bound_basis": bound_basis,
            "position_id": incident["position_id"], **evidence}


def _resume(db, record, lane, book, records, at):
    if (book["position"] or book["pending"] or book.get("permanent_block")
            or book["problem"] != "UNRESOLVED_PROTECTION_INTERVAL"):
        raise ValueError("Resume requires a flat historically blocked book without a pending entry")
    report = paper.book_report(db, lane)
    financial = [book[key] for key in ("cash", "equity", "gross", "fees", "funding", "liquidation_cost", "unfunded_deficit")]
    financial.append(report["cash_reconciliation_difference"])
    if not all(type(value) in (float, int) and isfinite(value) for value in financial):
        raise ValueError("Nominal account evidence contains invalid amounts")
    if abs(report["cash_reconciliation_difference"]) > 1e-8:
        raise ValueError("Nominal account reconciliation is incomplete")
    fills, closes = (db.execute("SELECT count(*) FROM event WHERE book=? AND kind=?", (book["book_id"], kind)).fetchone()[0]
                     for kind in ("HYPOTHETICAL_FILL", "CLOSED"))
    if fills != closes or not records or any(not item.get("outcome") for item in records):
        raise ValueError("Every exposure and protection incident requires a recorded disposition")
    if db.execute("SELECT 1 FROM event WHERE book=? AND kind='UNMODELED_TIER' LIMIT 1", (book["book_id"],)).fetchone():
        raise ValueError("Unmodeled instrument tiers require a separate supported resolution")
    if any(item.get("current_feed_health") == "UNAVAILABLE" for item in records):
        raise ValueError("An active protection obligation remains")
    health = _healthy(record, lane)
    effective = at.floor(lane["timeframe"]) + pd.Timedelta(lane["timeframe"])
    return {"generation": record["id"], "effective_at": effective.isoformat(), "current_health": health,
            "capital_dependency": {"incident_ids": sorted(item["incident_id"] for item in records),
                                   "position_ids": sorted({item["position_id"] for item in records}),
                                   "segment_id": fingerprint([record["id"], book["book_id"], effective.isoformat()])},
            "validated_results": "EXCLUDED_PENDING_SEPARATE_ANALYTICAL_RESOLUTION"}


def execute(action, request, *, apply=False, authorized=False):
    from lab.paper_run import runtime

    record = runtime.current()
    if not record:
        raise ValueError("No current paper generation")
    _validate_request(request, record)
    if apply and authorized is not True:
        raise PermissionError("Explicit operator approval is required for protection state changes")
    if action not in ("activate", "adjudicate", "resume"):
        raise ValueError("Unknown protection operation")
    if not apply:
        with closing(runtime.connect(readonly=True)) as db:
            db.execute("BEGIN")
            return _execute(db, record, action, request, False)
    with writer_lock(runtime.gate.ROOT / ".writer.lock"), closing(runtime.connect()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        return _execute(db, record, action, request, True)


def _execute(db, record, action, request, apply):
    if prior := _prior(db, request, action):
        return prior
    lane, book, records, token = _state(db, record, request["slot"])
    if token != request["review_token"]:
        raise ValueError("The reviewed account or incident state changed; obtain a new review")
    if action != "activate" and apply and not incidents.active(db):
        raise ValueError("Protection lifecycle policy is not active")
    at, operator = utc_now(), getpass.getuser()
    if action == "activate":
        details = _activate(db, record, request, book, at, operator)
        kind, position_id = "PROTECTION_POLICY_ACTIVATED", record["id"]
    elif action == "adjudicate":
        details = _adjudicate(db, record, lane, book, records, request)
        kind, position_id = "PROTECTION_INCIDENT_ADJUDICATED", details["position_id"]
    else:
        details = _resume(db, record, lane, book, records, at)
        kind, position_id = "BOOK_PROSPECTIVE_RESUME", records[-1]["position_id"]
    details.update(request_id=request["request_id"], request_sha256=fingerprint([action, request]), operation=action, operator=operator,
                   authorized_at=at.isoformat() if apply else None, reason=request["reason"])
    if not apply:
        return {"state": "PREVIEW", "operation": action, "record": details,
                "requires_explicit_operator_approval": True}
    for legacy in incidents.legacy_incidents(db, book, record["id"]):
        incidents.append(db, book, "PROTECTION_INCIDENT_IMPORTED", legacy["position_id"], at, legacy,
                         identity=legacy["incident_id"])
    key = incidents.append(db, book, kind, position_id, at, details, identity=request["request_id"])
    if action == "resume":
        # Historical problem and provisional events remain intact. Only the
        # audited admission projection permits a later prospective signal.
        book["prospective_admission"] = {"policy": incidents.POLICY, "event_id": key,
                                         "effective_at": details["effective_at"],
                                         "capital_dependency": details["capital_dependency"]}
        _healthy(record, lane)
        paper.save(db, book)
    return {"state": "APPLIED", "event_id": key, "record": details}


def command(args):
    from lab.paper_run import runtime

    if args.operation == "report":
        return report(runtime.current(), args.slot)
    request = read_json(args.request)
    return execute(args.operation, request, apply=args.apply, authorized=args.approve)
