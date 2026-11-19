"""Read-only paper operations scanner; durable notices use the health outbox.

This module never supervises a runner, changes paper state or calls Telegram.
The watchdog's existing check lease coordinates cursor/outbox transactions and
its existing private sender remains the only delivery owner.
"""

from contextlib import closing
from datetime import datetime, timezone
import importlib.util
import json
from math import isfinite
import re
import sqlite3

from lab.config import fingerprint
from lab.ema_paper.private_notifications import route
from lab.health.state import enqueue, get_meta, set_meta
from lab.storage import utc


VERSION = "fresh-paper-operational-health-v1"
PREFIX = "ema-private:fresh-health:"
META = "fresh_paper_operational_v1:"
MAX_EVENTS = 128
OPEN = {"PROTECTION_INCIDENT_OPENED", "PROTECTION_GAP", "UNMODELED_TIER"}
RECOVERED = {"PROTECTION_INCIDENT_RECOVERED", "PROTECTION_RESUMED"}
INVALIDATES = OPEN | {"PROTECTION_INCIDENT_IMPORTED", "PROTECTION_INCIDENT_EVIDENCE_ADDED"}
EVENTS = INVALIDATES | RECOVERED | {
    "PROTECTION_INCIDENT_ADJUDICATED", "BOOK_PROSPECTIVE_RESUME",
    "BOOK_OPERATIONAL_BLOCKED", "BOOK_OPERATIONAL_RESTORED", "OPERATIONAL_BLOCK",
    "OPERATIONAL_RESTRICTION", "OPERATIONAL_RESTORED",
    "OPERATIONAL_BLOCK_OPENED", "OPERATIONAL_BLOCK_CLEARED", "SOURCE_OPERATIONAL_TRANSITION",
}
OPERATIONAL_OPEN = {"BOOK_OPERATIONAL_BLOCKED", "OPERATIONAL_BLOCK", "OPERATIONAL_RESTRICTION",
                    "OPERATIONAL_BLOCK_OPENED"}
ERRORS = (OSError, RuntimeError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error)


def _safe(value, limit=140):
    text = re.sub(r"[\x00-\x1f\x7f]", " ", str(value))
    text = re.sub(r"[0-9]{6,}:[A-Za-z0-9_-]{20,}", "[redacted token]", text)
    text = re.sub(r"(?i)(authorization\s*[:=]\s*)(?:Bearer\s+)?\S+", r"\1[redacted]", text)
    text = re.sub(r"(?i)(token|password|secret|api[_-]?key)[\"']?\s*[:=]\s*[\"']?[^\s,\"'}]+",
                  r"\1=[redacted]", text)
    return text[:limit]


def _load(value):
    result = json.loads(value)
    if not isinstance(result, dict):
        raise ValueError("Operational state must be an object")
    return result


def _registered_lanes(record, source):
    lanes = record["body"]["lanes"]
    if importlib.util.find_spec("lab.paper_run.membership") is not None:
        from lab.paper_run import membership

        lanes = membership.lanes(record, db=source)
    if record["body"].get("protocol"):
        from lab.paper_run import source_amendment

        amended = source_amendment.lanes(record, source_amendment.active(record))
        lanes = {slot: {**lane, **amended.get(slot, {})} for slot, lane in lanes.items()}
    return lanes


def _members(record, source):
    generation = record["id"]
    lanes = _registered_lanes(record, source)
    if not isinstance(generation, str) or not generation or not isinstance(lanes, dict) or not lanes:
        raise ValueError("Registered paper membership is unavailable")
    members = {}
    for slot, lane in lanes.items():
        identifier = lane["book_id"]
        if not isinstance(slot, str) or not slot or not isinstance(identifier, str) or not identifier:
            raise ValueError("Registered paper identity is malformed")
        members[slot] = identifier
    if len(set(members.values())) != len(members):
        raise ValueError("Registered paper identities are duplicated")
    return generation, members, lanes


def _current_gates(record, lanes, now):
    """Capture actual control/clock checks, independent of an ACTIVE label."""
    from lab.paper_run import gate, runtime

    books = {}
    for lane in lanes.values():
        try:
            books[lane["book_id"]] = bool(runtime._can_enter(lane, record["body"], now)
                                          and not runtime._source_permission(lane["source"], now, entry_only=True))
        except ERRORS:
            books[lane["book_id"]] = False
    return {"clock": bool(runtime.synchronized(now)), "entries_enabled": not (gate.ROOT / "entries-paused").exists(),
            "books": books, "checked_at": now.isoformat()}


def _outbox(db, now, saved, identity, title, *, generation, slot="scanner", book=None,
            reason=None, exposure="UNKNOWN", event=None, historical=False, action=None):
    if not saved:
        return None
    identifier = PREFIX + fingerprint([generation, book, saved["route_id"], identity])
    lines = ["ASEKE BTC LAB — " + title, _safe(slot, 90),
             "Reason " + _safe(reason or "Operational transition"), "Exposure " + exposure,
             "Detected " + utc(now).isoformat()]
    if event:
        lines.append("Journal recorded " + _safe(event["at"], 40))
        if event.get("incident"):
            lines.append("Incident " + _safe(event["incident"], 64))
    if book:
        lines.append("Book " + _safe(book, 64))
    if historical:
        lines.append("Historical uncertainty/provisional exclusions remain.")
    if action:
        lines.append(_safe(action, 170))
    lines.append("Paper only · No real order")
    context = {"schema": VERSION, "generation_id": generation, "slot": slot, "book_id": book,
               "reason": _safe(reason), "exposure": exposure, "detected_at": utc(now).isoformat(),
               "private_route_id": saved["route_id"], "operational_event": event}
    enqueue(db, identifier, "health", "\n".join(lines)[:1000], utc(now).timestamp(),
            kind="operational", transition=identifier, event_json=json.dumps(context))
    return identifier


def failed(db, now, reason, *, generation="UNAVAILABLE"):
    """Make a scanner failure visible without consuming any paper cursor."""
    try:
        saved = route(db)
    except ERRORS:
        saved = None
    key = META + "scanner"
    prior = _load(get_meta(db, key, "{}"))
    code = _safe(reason, 120)
    route_id = saved["route_id"] if saved else None
    if prior.get("error") != code or prior.get("route_id") != route_id:
        prior["episode"] = prior.get("episode", 0) + 1
        _outbox(db, now, saved, ["SCANNER_FAILED", prior["episode"], code], "PAPER MONITOR CHECK FAILED",
                generation=generation, reason=code,
                action="Inspect health receipt and registered paper evidence; no recovery is inferred.")
    prior.update(error=code, route_id=route_id, attempted_at=utc(now).isoformat())
    set_meta(db, key, json.dumps(prior))
    return {"state": "CHECK_FAILED", "reason": code, "version": VERSION,
            "delivery": "PRIVATE_OUTBOX" if saved else "LOCAL_ONLY_NO_PRIVATE_ROUTE"}


def _exposure(book):
    if book.get("position"):
        return "OPEN"
    return "PENDING_ENTRY" if book.get("pending") else "FLAT"


def _blocked(book):
    position = book.get("position") or {}
    if book.get("state") == "BLOCKED" or book.get("permanent_block"):
        return str(book.get("reason") or position.get("unresolved_costs") or book.get("problem")
                   or "BOOK_BLOCKED")
    if position.get("unresolved_protection") or position.get("unresolved_costs"):
        return str(position.get("unresolved_costs") or "UNRESOLVED_PROTECTION_INTERVAL")
    # A historical problem alone must not defeat a valid prospective permit.
    if (book.get("state") != "EVIDENCE_UNAVAILABLE" and book.get("problem")
            and not book.get("prospective_entry_permission")):
        return str(book["problem"])
    return None


def _permission(db, generation, book_id, book, now, *, minimum_resume_row=0):
    if (book.get("state") != "ACTIVE" or book.get("prospective_entry_permission") is not True
            or book.get("position") or book.get("pending") or _blocked(book)):
        return False
    if (book.get("qualification") or {}).get("state") != "QUALIFIED":
        return False
    marks = ",".join("?" for _ in INVALIDATES)
    last_incident = max(minimum_resume_row, db.execute(
        f"SELECT coalesce(max(rowid),0) FROM event WHERE book=? AND kind IN ({marks})",
        (book_id, *sorted(INVALIDATES))).fetchone()[0])
    if not last_incident:
        return True
    permit = book.get("prospective_admission") or {}
    if not isinstance(permit, dict) or not permit.get("event_id"):
        return False
    row = db.execute("SELECT rowid,body FROM event WHERE key=? AND book=? AND kind='BOOK_PROSPECTIVE_RESUME'",
                     (permit["event_id"], book_id)).fetchone()
    if not row or row[0] <= last_incident:
        return False
    body = _load(row[1])
    return (body.get("generation") == generation and body.get("effective_at") == permit.get("effective_at")
            and utc(body["effective_at"]) <= now)


def _raw(db, identifier):
    row = db.execute("SELECT state FROM book WHERE id=?", (identifier,)).fetchone()
    return _load(row[0]) if row else None


def _legacy_funding_clearance(db, source, now, saved, previous, book, *, generation, slot,
                             identifier, consistent, backlog):
    """Observe a genuine legacy cost reconciliation without authorizing entry."""
    position = book.get("position") or {}
    if position.get("funding_state") is not None or not consistent or backlog:
        return None
    position_id = position.get("id")
    if not isinstance(position_id, str) or not position_id:
        return None
    pending = position.get("unresolved_costs")
    marker = previous.get("legacy_funding_wait") or {}
    if pending:
        if not (str(pending).startswith("FUNDING_") or pending == "WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION"):
            return None
        if marker.get("position_id") != position_id or marker.get("state") != "WAITING":
            latest = source.execute("SELECT coalesce(max(rowid),0) FROM event WHERE book=? AND idea=? AND kind='FUNDING'",
                                    (identifier, position_id)).fetchone()[0]
            previous["legacy_funding_wait"] = {"position_id": position_id, "state": "WAITING",
                "sequence": marker.get("sequence", 0)+1, "observed_at": now.isoformat(),
                "reason": str(pending), "after_funding_rowid": latest}
        return None
    if marker.get("position_id") != position_id or marker.get("state") != "WAITING":
        return None
    if book.get("financial_reconciliation_complete") is not True:
        return None
    required = ("cash", "starting_equity", "gross", "funding", "fees", "liquidation_cost", "unfunded_deficit")
    if not all(type(book.get(k)) in (int, float) and isfinite(book[k]) for k in required):
        return None
    expected = book["starting_equity"]+book["gross"]+book["funding"]-book["fees"]-book["liquidation_cost"]+book["unfunded_deficit"]
    if abs(book["cash"]-expected) > 1e-8:
        return None
    rows = source.execute("SELECT key,source_at,recorded_at,body FROM event WHERE book=? AND idea=? AND kind='FUNDING' "
                          "AND rowid>? ORDER BY rowid LIMIT ?", (identifier, position_id,
                          marker["after_funding_rowid"], MAX_EVENTS+1)).fetchall()
    if not rows or len(rows) > MAX_EVENTS:
        return None
    settled = set(position.get("settled_funding") or [])
    for _, source_at, recorded_at, encoded in rows:
        evidence = _load(encoded)
        settlement = evidence.get("settlement_at")
        if (settlement not in settled or utc(source_at) != utc(settlement)
                or not utc(settlement) <= utc(evidence["source_received_at"]) <= utc(recorded_at) <= now
                or not all(type(evidence.get(k)) in (int, float) and isfinite(evidence[k])
                           for k in ("rate", "valuation_reference", "cash_flow"))
                or evidence["valuation_reference"] <= 0):
            return None
    marker.update(state="CLEARED", cleared_observed_at=now.isoformat(), funding_event_ids=[r[0] for r in rows])
    previous["legacy_funding_wait"] = marker
    at = position.get("mark_received_at")
    age = (now-utc(at)).total_seconds() if at else None
    protection_current = bool(age is not None and 0 <= age <= 45 and not position.get("last_protection_rejection"))
    action = ("Position remains OPEN; entry is OCCUPIED. Accepted-price protection continues; historical exclusions remain."
              if protection_current else
              "Position remains OPEN; entry is OCCUPIED. Inspect current protection health independently; historical exclusions remain.")
    event = {"key": rows[-1][0], "kind": "FUNDING", "at": rows[-1][2], "position_id": position_id,
             "funding_event_ids": [r[0] for r in rows]}
    return _outbox(db, now, saved, ["LEGACY_FUNDING_CLEARED", position_id, marker["sequence"], *marker["funding_event_ids"]],
                   "FUNDING RESTRICTION CLEARED", generation=generation, slot=slot, book=identifier,
                   reason="New committed funding receipts and nominal cash reconciliation verified", exposure="OPEN", event=event,
                   historical=bool(book.get("historical_problem") or position.get("unresolved_protection")), action=action)


def _scope_keys(lane, book):
    operational = book.get("operational") or {}
    health = operational.get("source_health") or {} if isinstance(operational, dict) else {}
    health = health if isinstance(health, dict) else {}
    scope = book.get("source_scope_id") or lane.get("source_scope_id") or health.get("source_scope_id")
    return ("SOURCE:" + scope,) if isinstance(scope, str) and scope else ()


def _episode(body):
    return str(body.get("restriction_id") or body.get("episode_id") or body.get("correlation_id")
               or fingerprint([body.get("source_scope_id"), body.get("reason_code") or body.get("reason")]))


def _source_current(source, scope_keys, now):
    restrictions = {}
    for key in scope_keys:
        row = source.execute("SELECT key,recorded_at,body FROM event WHERE book=? "
                             "AND kind='SOURCE_OPERATIONAL_TRANSITION' ORDER BY rowid DESC LIMIT 1", (key,)).fetchone()
        if not row:
            continue
        body = _load(row[2])
        if utc(row[1]) > now or body.get("state") != "AVAILABLE":
            restrictions["SOURCE_SCOPE:" + key.removeprefix("SOURCE:")] = {
                "event_id": row[0], "reason": body.get("reason_code") or "SOURCE_OPERATIONAL_UNAVAILABLE"}
    return restrictions


def _book(db, source, now, saved, generation, slot, identifier, lane, view, *, consistent, gates):
    key = META + fingerprint([generation, identifier])
    previous = _load(get_meta(db, key, "{}"))
    supplied = view.get("books", {}).get(slot)
    scopes = _scope_keys(lane, supplied if isinstance(supplied, dict) else {})
    sources = (identifier, *scopes)
    source_marks = ",".join("?" for _ in sources)
    latest = source.execute(f"SELECT coalesce(max(rowid),0) FROM event WHERE book IN ({source_marks})", sources).fetchone()[0]
    initial = not previous
    if initial:
        previous = {"cursor": latest, "episode": 0, "entry_available": False,
                    "ever_restricted": False, "commissioned_at": now.isoformat()}
    if previous["cursor"] > latest:
        raise ValueError("Operational cursor exceeds registered journal")
    if previous.get("route_id") != (saved["route_id"] if saved else None):
        previous["notified_block"] = None
    previous["route_id"] = saved["route_id"] if saved else None
    restrictions = previous.setdefault("operational_restrictions", {})
    for key in list(restrictions):
        if key.startswith("SOURCE_SCOPE:"):
            restrictions.pop(key)
    restrictions.update(_source_current(source, scopes, now))
    inspection = None
    book = view.get("books", {}).get(slot)
    raw = _raw(source, identifier)
    if raw is None:
        inspection = "REGISTERED_BOOK_MISSING_FROM_LEDGER"
    if not isinstance(book, dict):
        inspection = inspection or "REGISTERED_BOOK_MISSING_OR_MALFORMED_STATUS"
        book = {**(raw or {}), "state": "EVIDENCE_UNAVAILABLE", "prospective_entry_permission": False}
    elif book.get("book_id") != identifier or not isinstance(book.get("state"), str):
        inspection = "REGISTERED_BOOK_STATUS_IDENTITY_INVALID"
    for field in ("position", "pending"):
        if book.get(field) is not None and not isinstance(book[field], dict):
            inspection = "REGISTERED_BOOK_STATUS_EXPOSURE_MALFORMED"
            book = {**book, field: None}
    if raw and any(book.get(k) != raw.get(k) for k in ("problem", "position", "pending")):
        consistent = False
    if inspection:
        if previous.get("inspection") != inspection:
            previous["inspection_episode"] = previous.get("inspection_episode", 0) + 1
            _outbox(db, now, saved, ["INSPECTION", previous["inspection_episode"], inspection],
                    "PAPER BOOK INSPECTION FAILED", generation=generation, slot=slot, book=identifier,
                    reason=inspection, exposure=_exposure(book), action="Inspect registered book and health receipt.")
        previous["inspection"] = inspection
    else:
        previous["inspection"] = None
    marks = ",".join("?" for _ in EVENTS)
    rows = source.execute(f"SELECT rowid,key,kind,recorded_at,body FROM event WHERE book IN ({source_marks}) AND rowid>? "
                          f"AND kind IN ({marks}) ORDER BY rowid LIMIT ?",
                          (*sources, previous["cursor"], *sorted(EVENTS), MAX_EVENTS)).fetchall()
    notices = []
    for rowid, event_id, kind, at, encoded in rows:
        if utc(at) > now:
            break
        body = _load(encoded)
        incident = body.get("incident_id") or (event_id if kind in OPEN else None)
        event = {"key": event_id, "kind": kind, "rowid": rowid, "at": at, "incident": incident}
        operational = previous.setdefault("operational_restrictions", {})
        source_transition = kind == "SOURCE_OPERATIONAL_TRANSITION"
        restricted = source_transition and (body.get("restricted") is True or body.get("active") is True
                                            or body.get("state") in {"BLOCKED", "RESTRICTED", "UNAVAILABLE"})
        cleared = kind in {"OPERATIONAL_BLOCK_CLEARED", "BOOK_OPERATIONAL_RESTORED", "OPERATIONAL_RESTORED"} or (
            source_transition and (body.get("restricted") is False or body.get("active") is False
                                   or body.get("state") in {"HEALTHY", "READY", "RECOVERED", "AVAILABLE"}))
        if kind in OPERATIONAL_OPEN or restricted:
            episode = "SOURCE_SCOPE:" + str(body.get("source_scope_id")) if source_transition else _episode(body)
            operational[episode] = {"event_id": event_id, "requires_incident_review": bool(body.get("requires_incident_review")),
                                    "reason": body.get("reason_code") or body.get("reason") or kind}
            if body.get("requires_incident_review"):
                previous["last_review_required_rowid"] = rowid
        elif cleared:
            episode = "SOURCE_SCOPE:" + str(body.get("source_scope_id")) if source_transition else _episode(body)
            operational.pop(episode, None)
        if kind in OPEN or kind in OPERATIONAL_OPEN or restricted:
            previous.update(ever_restricted=True, entry_available=False)
            notice = _outbox(db, now, saved, ["INCIDENT", incident] if kind in OPEN else ["EVENT", event_id], "PAPER PROTECTION INCIDENT" if kind in OPEN
                             else "PAPER BOOK BLOCKED", generation=generation, slot=slot, book=identifier,
                             reason=body.get("reason_code") or body.get("reason") or kind, exposure=_exposure(book), event=event,
                             historical=kind in OPEN, action="Inspect protection report; preserve current protection and history.")
            if notice:
                notices.append(notice)
        elif kind in RECOVERED:
            notice = _outbox(db, now, saved, ["RECOVERED", incident or event_id], "PROTECTION OBSERVATIONS RESUMED",
                             generation=generation, slot=slot, book=identifier, reason=body.get("reason") or kind,
                             exposure=_exposure(book), event=event, historical=True,
                             action="Source recovery alone does not restore entry permission; inspect incident review.")
            if notice:
                notices.append(notice)
        previous["cursor"] = rowid
    backlog = bool(source.execute(f"SELECT 1 FROM event WHERE book IN ({source_marks}) AND rowid>? AND kind IN ({marks}) LIMIT 1",
                                  (*sources, previous["cursor"], *sorted(EVENTS))).fetchone())
    if not backlog:
        previous["cursor"] = latest
    reason = _blocked(book) or next((r.get("reason") for k, r in previous["operational_restrictions"].items()
                                   if k.startswith("SOURCE_SCOPE:")), None)
    if reason:
        signature = fingerprint([book.get("state"), reason, bool(book.get("permanent_block"))])
        if previous.get("notified_block") != signature:
            previous["episode"] += 1
            permit = book.get("prospective_admission") or {}
            effective = permit.get("effective_at") if isinstance(permit, dict) else None
            future = bool(effective and utc(effective) > now)
            action = ("Prospective resume authorized for " + utc(effective).isoformat() + "; await that future boundary."
                      if future else "Inspect paper_run protection report and current source/financial prerequisites.")
            notice = _outbox(db, now, saved, ["BLOCK", previous["episode"], signature],
                             "PAPER BOOK BLOCKED — FUTURE ADMISSION PENDING" if future else "PAPER BOOK BLOCKED",
                             generation=generation, slot=slot, book=identifier, reason=reason,
                             exposure=_exposure(book), historical=bool(book.get("historical_problem") or
                             (book.get("position") or {}).get("unresolved_protection")),
                             action=action)
            if notice:
                notices.append(notice)
        previous.update(notified_block=signature, ever_restricted=True, entry_available=False)
    notice = _legacy_funding_clearance(db, source, now, saved, previous, book, generation=generation,
                                      slot=slot, identifier=identifier, consistent=consistent and not inspection, backlog=backlog)
    if notice:
        notices.append(notice)
    current_gates = bool(gates.get("clock") and gates.get("entries_enabled") and gates.get("books", {}).get(identifier))
    available = (not inspection and consistent and not backlog and current_gates
                 and not previous.get("operational_restrictions")
                 and _permission(source, generation, identifier, book, now,
                                 minimum_resume_row=previous.get("last_review_required_rowid", 0)))
    if available:
        if not previous.get("entry_available") and previous.get("ever_restricted"):
            previous["episode"] += 1
            notice = _outbox(db, now, saved, ["ELIGIBLE", previous["episode"],
                             (book.get("prospective_admission") or {}).get("event_id")],
                             "PROSPECTIVE ENTRY ELIGIBILITY RESTORED", generation=generation, slot=slot, book=identifier,
                             reason="Current admission/source gates pass; account flat with no pending entry",
                             exposure="FLAT", historical=bool(book.get("historical_problem")),
                             action="Future signals still require the normal strategy, study, risk and fresh-price checks.")
            if notice:
                notices.append(notice)
        previous.update(entry_available=True, notified_block=None)
    previous.update(last_scan_at=now.isoformat(), latest_rowid=latest, backlog=backlog,
                    snapshot_consistent=bool(consistent), exposure=_exposure(book), reason=reason)
    set_meta(db, key, json.dumps(previous))
    return {**previous, "book_id": identifier, "notices": notices, "initial_baseline": initial}


def collect(db, now, *, view=None, paper_db=None, registration=None, consistent=True, inspection_failure=None,
            current_gates=None):
    """Queue operational transitions under the existing watchdog check lease.

    First commissioning baselines old incident events but reports a current
    blocked book immediately. Each book has an independent bounded cursor.
    Positive permission notices require a stable snapshot and a drained journal.
    """
    now = utc(now)
    if paper_db is None:
        try:
            from lab.paper_run import runtime

            record = runtime.current() if registration is None else registration
            with closing(runtime.connect(readonly=True)) as source:
                before = source.execute("SELECT coalesce(max(rowid),0) FROM event").fetchone()[0]
                try:
                    snapshot = runtime.status() if view is None else view
                except ERRORS as exc:
                    inspection_failure = "PAPER_STATUS_INSPECTION_" + type(exc).__name__
                    failed(db, now, inspection_failure, generation=record["id"])
                    snapshot = {"generation_id": record["id"], "books": {}}
                source.execute("BEGIN")
                after = source.execute("SELECT coalesce(max(rowid),0) FROM event").fetchone()[0]
                return collect(db, now, view=snapshot, paper_db=source, registration=record,
                               consistent=consistent and before == after and view is None and not inspection_failure,
                               inspection_failure=inspection_failure, current_gates=current_gates)
        except ERRORS as exc:
            return failed(db, now, "PAPER_OPERATIONAL_SCANNER_" + type(exc).__name__)
    try:
        generation, members, lanes = _members(registration, paper_db)
        if not isinstance(view, dict) or view.get("generation_id") != generation or not isinstance(view.get("books"), dict):
            raise ValueError("Operational snapshot does not match registered generation")
        saved = route(db)
        gates = _current_gates(registration, lanes, now) if current_gates is None else current_gates
    except ERRORS as exc:
        return failed(db, now, "PAPER_OPERATIONAL_SCANNER_" + type(exc).__name__)
    if not db.in_transaction:
        db.execute("BEGIN")
    results = {}
    for slot, identifier in members.items():
        failure_key = META + "inspection_failure:" + fingerprint([generation, identifier])
        db.execute("SAVEPOINT paper_operational_book")
        try:
            results[slot] = _book(db, paper_db, now, saved, generation, slot, identifier, lanes[slot], view,
                                  consistent=consistent, gates=gates)
            failure = _load(get_meta(db, failure_key, "{}"))
            if failure.get("error"):
                failure["error"] = None
                set_meta(db, failure_key, json.dumps(failure))
        except ERRORS as exc:
            db.execute("ROLLBACK TO paper_operational_book")
            error = "BOOK_INSPECTION_" + type(exc).__name__
            failure = _load(get_meta(db, failure_key, "{}"))
            if failure.get("error") != error:
                failure["episode"] = failure.get("episode", 0) + 1
            _outbox(db, now, saved, ["INSPECTION_FAILED", failure["episode"], error], "PAPER BOOK INSPECTION FAILED",
                    generation=generation, slot=slot, book=identifier, reason=error,
                    action="Inspect this registered book/journal; other registered books remain monitored.")
            failure["error"] = error
            set_meta(db, failure_key, json.dumps(failure))
            results[slot] = {"state": "CHECK_FAILED", "reason": error, "book_id": identifier}
        except BaseException:
            db.execute("ROLLBACK TO paper_operational_book")
            raise
        finally:
            db.execute("RELEASE paper_operational_book")
    report = {"state": "CHECKED" if not inspection_failure and all(
        b.get("state") != "CHECK_FAILED" and not b.get("inspection") for b in results.values()) else "PARTIAL",
              "version": VERSION, "checked_at": now.isoformat(), "generation_id": generation,
              "snapshot_consistent": bool(consistent), "delivery": "PRIVATE_OUTBOX" if saved else "LOCAL_ONLY_NO_PRIVATE_ROUTE",
              "books": results, "current_gates": gates}
    if inspection_failure:
        report["inspection_failure"] = inspection_failure
    set_meta(db, META + "last", json.dumps(report))
    scanner = _load(get_meta(db, META + "scanner", "{}"))
    scanner.update(error=inspection_failure, attempted_at=now.isoformat(),
                   route_id=saved["route_id"] if saved else None)
    if not inspection_failure:
        scanner["last_success_at"] = now.isoformat()
    set_meta(db, META + "scanner", json.dumps(scanner))
    return report


def queue_test(db, now, request_id):
    """Queue one explicitly requested TEST using the same health category/route."""
    if not isinstance(request_id, str) or not 1 <= len(request_id) <= 128:
        raise ValueError("A bounded explicit operational-test request ID is required")
    saved = route(db)
    if not saved:
        raise PermissionError("The existing verified private route is required")
    identifier = _outbox(db, utc(now), saved, ["TEST", request_id], "OPERATIONAL NOTIFICATION TEST",
                         generation="TEST", reason="No real incident or trade occurred.", exposure="NONE",
                         action="Authorized notification commissioning test; no paper or exchange state changed.")
    created = db.execute("SELECT created FROM outbox WHERE id=?", (identifier,)).fetchone()[0]
    return {"state": "QUEUED", "outbox_id": identifier, "request_id": request_id,
            "enqueued_at": datetime.fromtimestamp(created, timezone.utc).isoformat(), "private_route_id": saved["route_id"]}


def status(db, now, *, stale_seconds=420):
    try:
        raw = get_meta(db, META + "last")
        if not raw:
            return {"state": "NOT_COMMISSIONED", "version": VERSION}
        saved = _load(raw)
        age = (utc(now) - utc(saved["checked_at"])).total_seconds()
        return {**saved, "state": saved["state"] if 0 <= age <= stale_seconds else "STALE_MONITOR",
                "age_seconds": age, "stale_after_seconds": stale_seconds}
    except ERRORS as exc:
        return {"state": "EVIDENCE_UNAVAILABLE", "version": VERSION,
                "reason": "OPERATIONAL_MONITOR_RECEIPT_" + type(exc).__name__}
