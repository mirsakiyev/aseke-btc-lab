"""Versioned protection history projected from the immutable paper journal."""

from copy import deepcopy
import json

from lab.config import fingerprint
from lab.storage import utc


POLICY = "PROTECTION_INCIDENT_POLICY_V1"
PREFIX = "PROTECTION_INCIDENT_"
FEED_EVENTS = "('PROTECTION_INCIDENT_OPENED','PROTECTION_INCIDENT_EVIDENCE_ADDED','PROTECTION_INCIDENT_RECOVERED')"


def event_rows(db, book_id=None):
    """Copy journal rows during a read snapshot without projecting history under its lock."""
    query = ("SELECT key,kind,idea,recorded_at,body FROM event WHERE "
             "(kind LIKE 'PROTECTION_INCIDENT_%' OR kind IN ('PROTECTION_POLICY_ACTIVATED','BOOK_PROSPECTIVE_RESUME'))")
    parameters = ()
    if book_id is not None:
        query += " AND book=?"
        parameters = (book_id,)
    return db.execute(query + " ORDER BY rowid", parameters).fetchall()


def _events(db, book_id=None):
    return [{"key": key, "kind": kind, "idea": idea, "recorded_at": at, "body": json.loads(raw)}
            for key, kind, idea, at, raw in event_rows(db, book_id)]


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


def project_rows(rows):
    incidents = {}
    for key, kind, idea, at, raw in rows:
        body = json.loads(raw)
        identifier = body.get("incident_id")
        if not identifier:
            continue
        event = {"key": key, "kind": kind, "idea": idea, "recorded_at": at, "body": body}
        if kind in (PREFIX + "OPENED", PREFIX + "IMPORTED"):
            incidents[identifier] = {**deepcopy(body), "latest_event": key, "history": [event]}
        elif identifier in incidents:
            record = incidents[identifier]
            record.update(deepcopy(body))
            record["latest_event"] = key
            record["history"].append(event)
    return list(incidents.values())


def project(db, book_id):
    return project_rows(event_rows(db, book_id))


def _journal_summaries(db, book_id):
    """Project only the facts needed for recovery and admission, excluding evidence bodies."""
    fields = ("incident_id", "position_id", "legacy_event_key", "current_feed_health", "historical_coverage", "outcome")
    paths = ",".join(f"'$.{field}'" for field in fields)
    rows = db.execute(f"SELECT kind,idea,json_extract(body,{paths}) FROM event WHERE book=? AND kind IN "
                      "('PROTECTION_INCIDENT_OPENED','PROTECTION_INCIDENT_IMPORTED',"
                      "'PROTECTION_INCIDENT_RECOVERED','PROTECTION_INCIDENT_ADJUDICATED') ORDER BY rowid",
                      (book_id,))
    records = {}
    for kind, position_id, raw in rows:
        item = {field: value for field, value in zip(fields, json.loads(raw)) if value is not None}
        identifier = item.get("incident_id")
        if kind in (PREFIX + "OPENED", PREFIX + "IMPORTED") and identifier:
            item.setdefault("position_id", position_id)
            records[identifier] = item
        elif identifier in records:
            records[identifier].update(item)
    return list(records.values())


def summaries(db, book, generation):
    records = _journal_summaries(db, book["book_id"])
    return records + legacy_incidents(db, book, generation, records=records)


def admission_allowed(book, now):
    if book.get("permanent_block"):
        return False
    if not book.get("problem"):
        return True
    permit = book.get("prospective_admission") or {}
    return (book["problem"] == "UNRESOLVED_PROTECTION_INTERVAL" and permit.get("policy") == POLICY
            and permit.get("event_id") and permit.get("effective_at")
            and utc(permit["effective_at"]) <= now)


def restore_admission(db, book):
    """Trust a projection only when its immutable authorization still exists."""
    # A historical cause can survive in the journal after a legacy projection
    # lost its problem field. Derive a conservative in-memory restriction before
    # considering a permit; never infer clean history from that missing field.
    if not book.get("problem") and ((book.get("position") or {}).get("unresolved_protection")
            or summaries(db, book, None)):
        book["problem"] = "UNRESOLVED_PROTECTION_INTERVAL"
    permit = book.get("prospective_admission")
    if not permit:
        return
    row = db.execute("SELECT rowid,body FROM event WHERE key=? AND kind='BOOK_PROSPECTIVE_RESUME' AND book=?",
                     (permit.get("event_id"), book["book_id"])).fetchone()
    if not active(db) or not row:
        book.pop("prospective_admission", None)
        return
    rowid, encoded = row
    body = json.loads(encoded)
    expected = {"policy": POLICY, "event_id": permit["event_id"], "effective_at": body["effective_at"],
                "capital_dependency": body["capital_dependency"]}
    if permit != expected:
        book.pop("prospective_admission", None)
        return
    # A saved projection cannot authorize a later historical protection cause,
    # including legacy paths which did not clear the projection themselves.
    causes = db.execute("SELECT kind,idea,body FROM event WHERE book=? AND rowid>? "
                        "AND kind IN ('PROTECTION_GAP','UNMODELED_TIER',"
                        "'PROTECTION_INCIDENT_OPENED','PROTECTION_INCIDENT_IMPORTED',"
                        "'PROTECTION_INCIDENT_ADJUDICATED','OPERATIONAL_BLOCK_OPENED','CLOSED') ORDER BY rowid",
                        (book["book_id"], rowid)).fetchall()
    reviewed_positions = set(body.get("capital_dependency", {}).get("position_ids", ()))
    for kind, position_id, raw in causes:
        details = json.loads(raw)
        if kind == "CLOSED":
            if not details.get("provisional") or position_id in reviewed_positions:
                continue
        if kind != "OPERATIONAL_BLOCK_OPENED" or details.get("requires_incident_review") or (
                details.get("historical_coverage") == "UNRESOLVED"):
            book.pop("prospective_admission", None)
            return
    reviewed = set(body.get("capital_dependency", {}).get("incident_ids", ()))
    if any(item["incident_id"] not in reviewed for item in _journal_summaries(db, book["book_id"])):
        book.pop("prospective_admission", None)


def _last_reference(position):
    return deepcopy(position.get("last_protection_reference") or {
        "source_at": position.get("mark_source_at"), "received_at": position.get("mark_received_at"),
        "event_id": position.get("mark_event_id"), "price": position.get("last_mark"),
        "subscription_epoch": None, "monotonic_ns": None,
        "limitation": "Legacy position did not retain all reference fields"})


def _witness_incident(db, book_id, witness_id):
    keys = [fingerprint([POLICY, book_id, PREFIX + kind, witness_id]) for kind in ("OPENED", "EVIDENCE_ADDED")]
    row = db.execute("SELECT json_extract(body,'$.incident_id') FROM event WHERE key IN (?,?) ORDER BY rowid LIMIT 1",
                     keys).fetchone()
    return row[0] if row else None


def _projected_active_incidents(db, book_id, position_id):
    return [item for item in project(db, book_id) if item["position_id"] == position_id
            and item.get("current_feed_health") == "UNAVAILABLE"]


def _active_incidents(db, book_id, position_id):
    """An OPENED episode stays current until RECOVERED; subsequent witnesses extend it."""
    identity = (book_id, position_id)
    if db.execute("SELECT 1 FROM event WHERE book=? AND idea=? AND kind='PROTECTION_INCIDENT_IMPORTED' "
                  "AND json_extract(body,'$.current_feed_health')='UNAVAILABLE' LIMIT 1", identity).fetchone():
        return _projected_active_incidents(db, book_id, position_id)
    head = db.execute(f"SELECT rowid,kind FROM event WHERE book=? AND idea=? AND kind IN {FEED_EVENTS} "
                      "ORDER BY rowid DESC LIMIT 1", identity).fetchone()
    if not head or head[1] == PREFIX + "RECOVERED":
        return []
    head_id, kind = head
    body, = db.execute("SELECT body FROM event WHERE rowid=?", (head_id,)).fetchone()
    current = json.loads(body)
    if kind == PREFIX + "EVIDENCE_ADDED":
        opening = db.execute(f"SELECT rowid,body FROM event WHERE book=? AND idea=? AND kind IN {FEED_EVENTS} "
                             "AND kind='PROTECTION_INCIDENT_OPENED' AND rowid<? ORDER BY rowid DESC LIMIT 1",
                             (*identity, head_id)).fetchone()
        if not opening:
            return _projected_active_incidents(db, book_id, position_id)
        opening_id, body = opening
        opened = json.loads(body)
        if opened.get("incident_id") != current.get("incident_id"):
            return _projected_active_incidents(db, book_id, position_id)
        if "transport_fence_at" not in current:
            fence = db.execute(f"SELECT json_extract(body,'$.transport_fence_at') FROM event "
                               f"WHERE book=? AND idea=? AND kind IN {FEED_EVENTS} AND rowid BETWEEN ? AND ? "
                               "AND json_type(body,'$.transport_fence_at') IS NOT NULL ORDER BY rowid DESC LIMIT 1",
                               (*identity, opening_id, head_id)).fetchone()
            if fence:
                current["transport_fence_at"] = fence[0]
        current = {**opened, **current}
    return [current] if current.get("current_feed_health") == "UNAVAILABLE" else []


def open_incident(db, book, at, reason, *, diagnostics=None, witness_id=None, lane=None,
                  source_scope_id=None, correlation_id=None):
    policy = active(db)
    position = book.get("position")
    if not policy or not position or position.get("exit_observed"):
        return None
    if witness_id:
        prior = _witness_incident(db, book["book_id"], witness_id)
        if prior:
            return prior
    active_incidents = _active_incidents(db, book["book_id"], position["id"])
    current = active_incidents[-1] if active_incidents else None
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
    if lane is not None:
        from lab.paper_run import capabilities

        contract = capabilities.contract(lane, generation=policy["generation"])
        body.update(source_identity=contract["source"],
                    source_scope_id=source_scope_id or capabilities.source_scope_id(lane),
                    capability_contract=contract)
    elif source_scope_id:
        body["source_scope_id"] = source_scope_id
    if correlation_id:
        body["source_correlation_id"] = correlation_id
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
    for incident in _active_incidents(db, book["book_id"], position["id"]):
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


def legacy_incidents(db, book, generation, *, records=None):
    """Read legacy evidence without inventing missing recovery observations."""
    projected = _journal_summaries(db, book["book_id"]) if records is None else records
    known = {item.get("legacy_event_key") for item in projected}
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
    covered = {item["position_id"] for item in projected} | {
        item["position_id"] for item in records}
    unknown = []
    current = book.get("position") or {}
    if current.get("unresolved_protection") and current["id"] not in covered:
        unknown.append((current["id"], None, None))
    for key, position_id, at, raw in db.execute(
            "SELECT key,idea,recorded_at,body FROM event WHERE book=? AND kind='CLOSED' ORDER BY rowid",
            (book["book_id"],)):
        value = json.loads(raw)
        if value.get("provisional") and position_id not in covered:
            unknown.append((position_id, value.get("source_received_at"), at))
    for position_id, end, terminal_at in unknown:
        if position_id in covered:
            continue
        covered.add(position_id)
        records.append({"policy": POLICY, "generation": generation, "book_id": book["book_id"],
                        "position_id": position_id,
                        "incident_id": fingerprint([POLICY, generation, book["book_id"], position_id, "UNATTRIBUTED_FLAG"]),
                        "reason": "UNATTRIBUTED_HISTORICAL_PROTECTION_FLAG", "detected_at": None,
                        "known_terminal_recorded_at": terminal_at, "last_accepted_reference": None,
                        "first_accepted_reference": None, "current_feed_health": "UNKNOWN_HISTORICAL",
                        "historical_coverage": "UNRESOLVED", "exit_impact": "PENDING_REVIEW",
                        "interval": {"starts_after": None, "ends_at": end,
                                     "start_precision": "UNKNOWN", "end_precision": "COMMITTED_EXIT_REFERENCE" if end else "UNKNOWN"},
                        "legacy_evidence": {"limitation": "A historical flag/provisional close has no attributable incident record; original detection and missing prices are unknown"},
                        "next_action": "Review explicit insufficiency without inventing the original interval"})
    return records
