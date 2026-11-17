"""Versioned protection history projected from the immutable paper journal."""

from copy import deepcopy
import json

from lab.config import fingerprint
from lab.storage import utc


POLICY = "PROTECTION_INCIDENT_POLICY_V1"
PREFIX = "PROTECTION_INCIDENT_"


def _events(db, book_id=None):
    query = ("SELECT key,kind,idea,recorded_at,body FROM event WHERE "
             "(kind LIKE 'PROTECTION_INCIDENT_%' OR kind IN ('PROTECTION_POLICY_ACTIVATED','BOOK_PROSPECTIVE_RESUME'))")
    parameters = ()
    if book_id is not None:
        query += " AND book=?"
        parameters = (book_id,)
    rows = db.execute(query + " ORDER BY rowid", parameters).fetchall()
    result = []
    for key, kind, idea, at, raw in rows:
        body = json.loads(raw)
        result.append({"key": key, "kind": kind, "idea": idea, "recorded_at": at, "body": body})
    return result


def active(db):
    rows = db.execute("SELECT body FROM event WHERE kind='PROTECTION_POLICY_ACTIVATED' ORDER BY rowid DESC LIMIT 1").fetchone()
    if rows:
        body = json.loads(rows[0])
        if body.get("policy") == POLICY:
            return body
    return None


def append(db, book, kind, position_id, at, body, *, identity):
    for operation in ("UPDATE", "DELETE"):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS protection_journal_{operation.lower()} "
                   f"BEFORE {operation} ON event WHEN OLD.kind LIKE 'PROTECTION_INCIDENT_%' "
                   "OR OLD.kind IN ('PROTECTION_POLICY_ACTIVATED','PROTECTION_TRANSPORT_WITNESS','BOOK_PROSPECTIVE_RESUME') "
                   "BEGIN SELECT RAISE(ABORT,'Protection journal is append-only'); END")
    payload = {"policy": POLICY, "book_id": book["book_id"], **body}
    key = fingerprint([POLICY, book["book_id"], kind, identity])
    prior = db.execute("SELECT body FROM event WHERE key=?", (key,)).fetchone()
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if prior:
        if json.loads(prior[0]) != payload:
            raise ValueError("Protection request ID was reused with different evidence")
        return key
    db.execute("INSERT INTO event VALUES (?,?,?,?,?,?,?)", (
        key, book["book_id"], kind, position_id, at.isoformat(), at.isoformat(), encoded))
    return key


def project(db, book_id):
    incidents = {}
    for event in _events(db, book_id):
        body = event["body"]
        identifier = body.get("incident_id")
        if not identifier:
            continue
        if event["kind"] in (PREFIX + "OPENED", PREFIX + "IMPORTED"):
            incidents[identifier] = {**deepcopy(body), "latest_event": event["key"], "history": [event]}
        elif identifier in incidents:
            record = incidents[identifier]
            record.update(deepcopy(body))
            record["latest_event"] = event["key"]
            record["history"].append(event)
    return list(incidents.values())


def admission_allowed(book, now):
    if not book.get("problem"):
        return True
    permit = book.get("prospective_admission") or {}
    return (book["problem"] == "UNRESOLVED_PROTECTION_INTERVAL" and permit.get("policy") == POLICY
            and permit.get("event_id") and permit.get("effective_at")
            and utc(permit["effective_at"]) <= now)


def restore_admission(db, book):
    """Trust a projection only when its immutable authorization still exists."""
    permit = book.get("prospective_admission")
    if not permit:
        return
    row = db.execute("SELECT body FROM event WHERE key=? AND kind='BOOK_PROSPECTIVE_RESUME' AND book=?",
                     (permit.get("event_id"), book["book_id"])).fetchone()
    if not active(db) or not row:
        book.pop("prospective_admission", None)
        return
    body = json.loads(row[0])
    expected = {"policy": POLICY, "event_id": permit["event_id"], "effective_at": body["effective_at"],
                "capital_dependency": body["capital_dependency"]}
    if permit != expected:
        book.pop("prospective_admission", None)


def _last_reference(position):
    return deepcopy(position.get("last_protection_reference") or {
        "source_at": position.get("mark_source_at"), "received_at": position.get("mark_received_at"),
        "event_id": position.get("mark_event_id"), "price": position.get("last_mark"),
        "subscription_epoch": None, "monotonic_ns": None,
        "limitation": "Legacy position did not retain all reference fields"})


def open_incident(db, book, at, reason, *, diagnostics=None, witness_id=None):
    policy = active(db)
    position = book.get("position")
    if not policy or not position or position.get("exit_observed"):
        return None
    records = project(db, book["book_id"])
    if witness_id:
        prior = next((item for item in records if witness_id in item.get("witness_ids", [])), None)
        if prior:
            return prior["incident_id"]
    current = next((item for item in reversed(records) if item["position_id"] == position["id"]
                    and item.get("current_feed_health") == "UNAVAILABLE"), None)
    last = _last_reference(position)
    identifier = current["incident_id"] if current else fingerprint([
        POLICY, policy["generation"], book["book_id"], position["id"], witness_id or last.get("event_id"),
        None if witness_id else last.get("received_at")])
    witnesses = list(current.get("witness_ids", [])) if current else []
    if witness_id:
        witnesses.append(witness_id)
    body = {"generation": policy["generation"], "incident_id": identifier, "position_id": position["id"],
            "reason": reason, "current_feed_health": "UNAVAILABLE", "historical_coverage": "UNRESOLVED",
            "exit_impact": "PENDING_REVIEW", "witness_ids": witnesses,
            "evidence": deepcopy(diagnostics or (current or {}).get("evidence") or {})}
    entry = position.get("entry_evidence") or {}
    body.update(source=entry.get("source", position["plan"].get("trigger_basis")),
                symbol=entry.get("symbol"), observation_kind=entry.get("observation_kind"),
                detection_monotonic_ns=(diagnostics or {}).get("monotonic_ns"),
                current_reference_rejection=reason)
    if diagnostics and diagnostics.get("kind") in ("DISCONNECTED", "QUEUE_DROPPED", "WITNESS_OVERFLOW"):
        body["transport_fence_at"] = diagnostics["at"]
    elif reason == "KRAKEN_STREAM_DISCONTINUITY":
        body["transport_fence_at"] = at.isoformat()
    if not current:
        detected = (diagnostics or {}).get("at", at.isoformat())
        if utc(detected) > at:
            raise ValueError("Future-dated protection incident")
        body.update(last_accepted_reference=last, detected_at=detected, first_accepted_reference=None,
                    frozen_position=deepcopy(position),
                    interval={"starts_after": last.get("received_at"), "ends_at": None,
                              "start_precision": "LAST_ACCEPTED_RECEIPT", "end_precision": "UNKNOWN"})
    kind = PREFIX + ("EVIDENCE_ADDED" if current else "OPENED")
    append(db, book, kind, position["id"], at, body,
           identity=witness_id or [identifier, kind, reason, fingerprint(body)])
    position["unresolved_protection"] = True
    position["protection_gap"] = {"incident_id": identifier, "started_after": last.get("received_at"),
                                  "last_checked": at.isoformat(), "reason": reason}
    book.pop("prospective_admission", None)
    return identifier


def recovered(db, book, obs, at, mono):
    if not active(db):
        return
    position = book["position"]
    reference = deepcopy(obs)
    reference.update(protection_committed_at=at.isoformat(), protection_commit_mono_ns=mono,
                     clock_and_reference_checks="PASSED_AT_PROTECTION_COMMIT")
    resolved = set()
    for incident in project(db, book["book_id"]):
        if incident["position_id"] != position["id"] or incident.get("current_feed_health") != "UNAVAILABLE":
            continue
        fence = incident.get("transport_fence_at")
        if fence and utc(obs["received_at"]) <= utc(fence):
            continue
        transport = incident.get("evidence") or {}
        old_epoch = transport.get("subscription_epoch")
        if transport.get("kind") == "DISCONNECTED" and old_epoch is not None:
            if obs.get("subscription_epoch", old_epoch) <= old_epoch:
                continue
        body = {"generation": incident["generation"], "incident_id": incident["incident_id"],
                "position_id": position["id"], "current_feed_health": "RECOVERED",
                "first_accepted_reference": reference,
                "interval": {**incident["interval"], "ends_at": obs["received_at"],
                             "end_precision": "FIRST_VALIDATED_RECOVERY_RECEIPT"}}
        append(db, book, PREFIX + "RECOVERED", position["id"], at, body,
               identity=incident["incident_id"])
        resolved.add(incident["incident_id"])
    gap = position.get("protection_gap")
    if gap and gap.get("incident_id") in resolved and not gap.get("ended_at"):
        gap["ended_at"] = at.isoformat()
    position["last_protection_reference"] = reference


def dependency(book):
    permit = book.get("prospective_admission") or {}
    return deepcopy(permit.get("capital_dependency") or {})


def legacy_incidents(db, book, generation):
    """Read legacy evidence without inventing missing recovery observations."""
    known = {item.get("legacy_event_key") for item in project(db, book["book_id"])}
    records = []
    for key, position_id, at, raw in db.execute(
            "SELECT key,idea,recorded_at,body FROM event WHERE book=? AND kind='PROTECTION_GAP' ORDER BY rowid",
            (book["book_id"],)):
        if key in known:
            continue
        detail = json.loads(raw)
        records.append({"policy": POLICY, "generation": generation, "book_id": book["book_id"],
                        "position_id": position_id, "incident_id": fingerprint([POLICY, key]),
                        "legacy_event_key": key, "detected_at": at, "reason": detail.get("reason"),
                        "last_accepted_reference": {"received_at": detail.get("last_reference", detail.get("started_after"))},
                        "first_accepted_reference": None, "current_feed_health": "UNKNOWN_HISTORICAL",
                        "historical_coverage": "UNRESOLVED", "exit_impact": "PENDING_REVIEW",
                        "interval": {"starts_after": detail.get("last_reference", detail.get("started_after")),
                                     "ends_at": detail.get("ended_at"), "end_precision": "UNKNOWN"},
                        "legacy_evidence": detail})
    return records
