"""Preview-first operator actions for prospective paper protection recovery."""

from contextlib import closing
from dataclasses import asdict
from copy import deepcopy
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
    from lab.paper_run import membership

    lanes = membership.lanes(record, db=db)
    if slot not in lanes:
        raise ValueError("Unknown current paper book")
    lane = lanes[slot]
    book = paper.load(db, lane)
    incidents.restore_admission(db, book)
    records = incidents.project(db, book["book_id"]) + incidents.legacy_incidents(db, book, record["id"])
    position = book.get("position") or {}
    pending = book.get("pending") or {}
    token = fingerprint([record["id"], book["book_id"], position.get("id"), pending.get("id"),
                         book["cash"], book["fees"], book["funding"], book["gross"], book["problem"],
                         position.get("stop"), position.get("target"), position.get("deadline"),
                         book.get("permanent_block"),
                         (book.get("prospective_admission") or {}).get("event_id"),
                         [(item["incident_id"], item.get("latest_event", item.get("legacy_event_key"))) for item in records]])
    return lane, book, records, token


def report(record, slot):
    from lab.paper_run import runtime, capabilities

    with closing(runtime.connect(readonly=True)) as db:
        db.execute("BEGIN")
        lane, book, records, token = _state(db, record, slot)
        capability = capabilities.contract(lane, generation=record["id"], slot=slot)
        streams = {}
        for incident in records:
            try:
                position = _position(db, book, incident["position_id"], require_static=False)
                streams[incident["incident_id"]] = position.get("plan", {}).get("evidence", {}).get("source_id")
            except ValueError:
                streams[incident["incident_id"]] = None
        stream_id = ((book.get("position") or {}).get("plan", {}).get("evidence", {}).get("source_id")
                     or next((value for value in reversed(list(streams.values())) if value), None)
                     or capability["source"]["version"])
        return {"mode": "READ_ONLY_REVIEW", "policy": incidents.POLICY, "generation": record["id"],
                "slot": slot, "book_id": lane["book_id"], "review_token": token,
                "policy_active": bool(incidents.active(db)), "incidents": records,
                "position_id": (book.get("position") or {}).get("id"),
                "pending": bool(book.get("pending")), "historical_problem": book.get("problem"),
                "nominal_cash": book["cash"], "prospective_admission": book.get("prospective_admission"),
                "capability_contract": capability,
                "evidence_source_identity": {"source": lane["source"], "symbol": lane["symbol"],
                                             "trigger_basis": lane["source"], "stream_id": stream_id,
                                             "source_identity": capability["source"]},
                "incident_historical_stream_ids": streams,
                "note": "Unknown historical recovery timestamps remain unknown"}


def _position(db, book, identifier, *, through=None, require_static=True):
    current = book.get("position") or {}
    rows = db.execute("SELECT kind,source_at,body FROM event WHERE book=? AND idea=? ORDER BY rowid",
                      (book["book_id"], identifier)).fetchall()
    changes = [(kind, at) for kind, at, _ in rows
               if kind in ("FUNDING", "UNMODELED_TIER", "STOP_UPDATED", "TRAILING_STOP_UPDATED", "TRAIL_UPDATED")]
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
            "stop": fill["stop"], "target": fill.get("target"), "deadline": fill["deadline"],
            "position": asdict(position)}


def _position_segments(db, book, identifier, left, right):
    """Reconstruct effective historical terms from economic journal time, never the latest stop."""
    left, right = utc(left), utc(right)
    position = deepcopy(_position(db, book, identifier, through=right, require_static=False))
    position.pop("last_trail", None)
    position.pop("stop_effective_at", None)
    position.pop("prior_stop", None)
    rows = db.execute("SELECT key,kind,source_at,body FROM event WHERE book=? AND idea=? "
                      "AND kind IN ('FUNDING','UNMODELED_TIER','STOP_UPDATED','TRAILING_STOP_UPDATED','TRAIL_UPDATED') "
                      "ORDER BY rowid", (book["book_id"], identifier)).fetchall()
    changes = []
    for key, kind, source_at, raw in rows:
        details = json.loads(raw)
        effective = utc(details.get("settlement_at") if kind == "FUNDING" else
                        details.get("stop_effective_at", details.get("effective_at", source_at)))
        if effective < utc(position["entry_at"]):
            raise ValueError("Historical term change precedes the position; use an explicit insufficiency review")
        if effective < right:
            changes.append((effective, key, kind, details))
    changes.sort(key=lambda row: (row[0], next(i for i, value in enumerate(rows) if value[0] == row[1])))
    segments, cursor, provenance = [], left, []
    for effective, key, kind, details in changes:
        if effective > cursor:
            segments.append({"start": cursor.isoformat(), "end": effective.isoformat(),
                             "position": deepcopy(position), "effective_event_ids": list(provenance)})
            cursor = effective
        if kind == "UNMODELED_TIER":
            raise ValueError("Unmodeled instrument terms require explicit insufficiency or a supported terms adapter")
        if kind == "FUNDING":
            amount = details.get("cash_flow")
            if isinstance(amount, bool) or not isinstance(amount, (int, float)) or not isfinite(amount):
                raise ValueError("Funding journal lacks a finite effective cash flow")
            value = position["position"]
            for field in ("funding", "wallet", "reserved"):
                value[field] = value.get(field, 0.) + amount
        else:
            stop = details.get("effective_stop", details.get("stop"))
            if isinstance(stop, bool) or not isinstance(stop, (int, float)) or not isfinite(stop) or stop <= 0:
                raise ValueError("Effective stop journal is incomplete; use an explicit insufficiency disposition")
            position["stop"] = stop
        provenance.append(key)
    if cursor < right:
        segments.append({"start": cursor.isoformat(), "end": right.isoformat(),
                         "position": deepcopy(position), "effective_event_ids": list(provenance)})
    return segments


def _healthy(record, lane, db=None):
    from lab.paper_run import execution, runtime, delivery

    if db is None:
        with closing(runtime.connect(readonly=True)) as reader:
            reader.execute("BEGIN")
            return _healthy(record, lane, reader)
    now, _ = execution.clock()
    if not runtime.synchronized(now) or lane["qualification"]["state"] != "QUALIFIED":
        raise ValueError("Current clock or source qualification is unavailable")
    if runtime._source_permission(lane["source"], now, entry_only=True):
        raise ValueError("Current source permission is unavailable")
    if any(item["book_id"] == lane["book_id"] for item in delivery.pending_books(db)):
        raise ValueError("Unprocessed source delivery or protection loss requires completion/review before resume")
    source = runtime._lane_source_state(db, lane).get("last_observation")
    is_stream = lane["source"] == "KRAKEN_BTC_USD_LAST_TRADE"
    if is_stream:
        stream = runtime.read_status_snapshot("ema-live-status.json", {})
        if stream.get("state") != "STREAMING":
            raise ValueError("Current Kraken transport is not streaming")
        runtime.kraken_live.permission(now)
        source = source or stream.get("last_usable_observation")
    heartbeat = runtime.read_status_snapshot("last-cycle.json", {})
    now, mono = execution.clock()
    if (heartbeat.get("state") != "RUNNING" or heartbeat.get("generation") != record["id"]
            or heartbeat.get("runner_build_id") != runtime.RUNNER_BUILD
            or not 0 <= (now - utc(heartbeat["at"])).total_seconds() < 45):
        raise ValueError("A fresh managed runner on the reviewed build is required")
    if is_stream:
        stream_at = stream.get("at")
        active_amendment = heartbeat.get("source_amendment_id")
        if (not stream_at or not 0 <= (now - utc(stream_at)).total_seconds() < 45
                or active_amendment != stream.get("amendment_id")):
            raise ValueError("Current Kraken transport adoption or heartbeat is unavailable")
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
    from lab.paper_run import runtime, membership

    if incidents.active(db):
        raise ValueError("This policy is already active")
    membership.validate_accounts(db, record)
    states = [json.loads(raw) for raw, in db.execute("SELECT state FROM book")]
    if any(item["position"] or item["pending"] for item in states):
        raise ValueError("Activation requires all registered accounts flat with no pending entry")
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
        return end, incident["interval"].get("end_precision", "FIRST_VALIDATED_RECOVERY_RECEIPT")
    # Repeating an identical rejection requires protect() to have cleared it
    # after an accepted reference in between. The later rejection is an upper
    # bound, never a fabricated first recovery timestamp.
    previous = None
    for at, raw in db.execute("SELECT recorded_at,body FROM event WHERE book=? AND idea=? "
                              "AND kind='PROTECTION_SAMPLE_REJECTED' ORDER BY rowid",
                              (book["book_id"], incident["position_id"])):
        if not incident.get("detected_at") or utc(at) <= utc(incident["detected_at"]):
            continue
        reason = json.loads(raw).get("reason")
        if reason and reason == previous:
            return at, "LEGACY_REJECTION_TRANSITIONS_PROVE_ACCEPTED_BY"
        previous = reason
    position = book.get("position") or {}
    if position.get("id") == incident["position_id"] and position.get("mark_received_at"):
        if incident.get("detected_at") and utc(position["mark_received_at"]) > utc(incident["detected_at"]):
            return position["mark_received_at"], "CURRENT_ACCEPTED_REFERENCE_UPPER_BOUND"
    for raw, in db.execute("SELECT body FROM event WHERE book=? AND idea=? AND kind='EXIT_OBSERVED' ORDER BY rowid",
                           (book["book_id"], incident["position_id"])):
        receipt = json.loads(raw).get("source_received_at")
        if receipt and (not incident.get("detected_at") or utc(receipt) > utc(incident["detected_at"])):
            return receipt, "COMMITTED_EXIT_REFERENCE_UPPER_BOUND"
    return None, "RECOVERY_BOUND_UNKNOWN"


def _adjudicate(db, record, lane, book, records, request):
    from lab.paper_run.protection_evidence import validate_evidence
    from lab.paper_run import capabilities

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
    insufficient = request["evidence"]["outcome"] == "INSUFFICIENT_EVIDENCE"
    if (start and end and not utc(start) < utc(end) <= utc_now()) or (not insufficient and (not start or not end)):
        raise ValueError("An evidence-bounded review interval is required; recovery time may remain unknown")
    favorable = request["evidence"]["outcome"] != "INSUFFICIENT_EVIDENCE"
    try:
        position = _position(db, book, incident["position_id"], through=end, require_static=False)
    except ValueError:
        if favorable:
            raise
        position = None
    segments = _position_segments(db, book, incident["position_id"], start, end) if favorable else []
    if segments:
        position = segments[0]["position"]
    capability = capabilities.contract(lane, generation=record["id"], slot=request["slot"])
    stream_id = ((position or {}).get("plan", {}).get("evidence", {}).get("source_id")
                 or (incident.get("source_identity") or {}).get("version"))
    if stream_id is None and lane["source"] != "KRAKEN_BTC_USD_LAST_TRADE":
        stream_id = capability["source"]["version"]
    context = {"generation": record["id"], "book_id": book["book_id"], "position_id": incident["position_id"],
               "source": lane["source"], "symbol": lane["symbol"], "trigger_basis": lane["source"],
               "stream_id": stream_id, "source_identity": capability["source"],
               "evidence_adapter": capability["adapters"]["evidence"],
               "interval_start": start, "interval_end": end, "position": position,
               "historical_model_unavailable": position is None,
               "terms_segments": segments, "segmented_terms_verified": bool(segments)}
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
    terminal_financial_checks(db, lane, book,
                              expected_position_ids={item["position_id"] for item in records if item.get("position_id")})
    if not records or any(not item.get("outcome") for item in records):
        raise ValueError("Every exposure and protection incident requires a recorded disposition")
    if db.execute("SELECT 1 FROM event WHERE book=? AND kind='UNMODELED_TIER' LIMIT 1", (book["book_id"],)).fetchone():
        raise ValueError("Unmodeled instrument tiers require a separate supported resolution")
    if any(item.get("current_feed_health") == "UNAVAILABLE" for item in records):
        raise ValueError("An active protection obligation remains")
    health = _healthy(record, lane, db)
    anchor = max(at, utc(health["at"])) if health.get("at") else at
    effective = anchor.floor(lane["timeframe"]) + pd.Timedelta(lane["timeframe"])
    if effective >= utc(lane["publication_end_exclusive"]):
        raise ValueError("The registered study has no remaining future evaluation boundary")
    return {"generation": record["id"], "effective_at": effective.isoformat(), "current_health": health,
            "capital_dependency": {"incident_ids": sorted(item["incident_id"] for item in records),
                                   "position_ids": sorted({item["position_id"] for item in records}),
                                   "segment_id": fingerprint([record["id"], book["book_id"], effective.isoformat()])},
            "validated_results": "EXCLUDED_PENDING_SEPARATE_ANALYTICAL_RESOLUTION"}


def terminal_financial_checks(db, lane, book, *, expected_position_ids=()):
    """Verify one flat book's committed economics without demanding clean research history."""
    if book.get("position") or book.get("pending"):
        raise ValueError("Terminal nominal reconciliation requires the affected book flat with no pending entry")
    if not all(type(book.get(key)) in (float, int) and isfinite(book[key]) for key in (
            "cash", "equity", "starting_equity", "fees", "funding", "gross", "liquidation_cost", "unfunded_deficit")):
        raise ValueError("Terminal nominal account contains invalid amounts")
    if abs(book["equity"] - book["cash"]) > 1e-8:
        raise ValueError("Terminal flat equity and nominal cash do not reconcile")
    rows = db.execute("SELECT rowid,kind,idea,body FROM event WHERE book=? AND kind IN "
                      "('HYPOTHETICAL_FILL','EXIT_OBSERVED','CLOSED','FUNDING') ORDER BY rowid",
                      (lane["book_id"],)).fetchall()
    grouped, funding = {}, {}
    for rowid, kind, identifier, raw in rows:
        value = json.loads(raw)
        if kind == "FUNDING":
            amount = value.get("cash_flow")
            if type(amount) not in (float, int) or not isfinite(amount):
                raise ValueError("Terminal funding journal contains invalid amounts")
            funding[identifier] = funding.get(identifier, 0.) + amount
            continue
        events = grouped.setdefault(identifier, {})
        if kind in events:
            raise ValueError("Terminal position has duplicate lifecycle evidence")
        events[kind] = (rowid, value)
    missing = set(expected_position_ids) - {identifier for identifier, events in grouped.items() if "CLOSED" in events}
    if missing:
        raise ValueError("Historical incident positions require genuine matching terminal CLOSED receipts: " + repr(sorted(missing)))
    totals = {"fees": 0., "funding": 0., "gross": 0., "liquidation_cost": 0., "unfunded_deficit": 0., "net": 0.}
    for identifier, events in grouped.items():
        if set(events) != {"HYPOTHETICAL_FILL", "EXIT_OBSERVED", "CLOSED"}:
            raise ValueError("Every terminal position requires matching fill, exit-observed and closed evidence")
        if not events["HYPOTHETICAL_FILL"][0] < events["EXIT_OBSERVED"][0] < events["CLOSED"][0]:
            raise ValueError("Terminal financial lifecycle ordering is invalid")
        closed = events["CLOSED"][1]
        for field, key in (("fees", "fees"), ("funding", "funding"), ("gross", "gross_price_pnl"),
                           ("liquidation_cost", "liquidation_cost"), ("unfunded_deficit", "unfunded_deficit"), ("net", "net")):
            amount = closed.get(key)
            if type(amount) not in (float, int) or not isfinite(amount):
                raise ValueError("Terminal closed outcome has incomplete nominal financial evidence")
            totals[field] += amount
        if abs(closed["funding"] - funding.pop(identifier, 0.)) > 1e-8:
            raise ValueError("Terminal financing does not match the committed funding journal")
        expected_net = (closed["gross_price_pnl"] + closed["funding"] - closed["fees"]
                        - closed["liquidation_cost"] + closed["unfunded_deficit"])
        if abs(expected_net - closed["net"]) > 1e-8:
            raise ValueError("Terminal position cash flows do not reconcile")
    if funding:
        raise ValueError("Funding has no matching terminal position")
    for field in ("fees", "funding", "gross", "liquidation_cost", "unfunded_deficit"):
        if abs(book[field] - totals[field]) > 1e-8:
            raise ValueError("Terminal nominal account aggregates do not reconcile")
    if abs(book["cash"] - book["starting_equity"] - totals["net"]) > 1e-8:
        raise ValueError("Terminal nominal cash does not match the closed journal")
    return {"financial_reconciliation_complete": True, "closed_positions": len(grouped), "nominal_totals": totals,
            "research_validation_is_separate": True}


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
    with writer_lock(runtime.gate.ROOT / ".writer.lock"):
        if runtime.gate.reset_pending() or (runtime.current() or {}).get("id") != record["id"]:
            raise ValueError("Protection request generation changed before commit")
        with closing(runtime.connect()) as db, db:
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
        _healthy(record, lane, db)
        if utc_now() >= utc(details["effective_at"]):
            raise ValueError("The proposed future boundary passed during the review; refresh the resume request")
        paper.save(db, book)
    return {"state": "APPLIED", "event_id": key, "record": details}


def command(args):
    from lab.paper_run import runtime

    if args.operation == "report":
        return report(runtime.current(), args.slot)
    request = read_json(args.request)
    return execute(args.operation, request, apply=args.apply, authorized=args.approve)
