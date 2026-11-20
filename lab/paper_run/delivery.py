"""Bounded per-book observation work, separate from a source ingestion watermark.

All mutations participate in the caller's existing writer transaction. This
module never commits, sends notifications, executes financial transitions or
treats stored catch-up evidence as a fresh executable paper reference.
"""

from copy import deepcopy
import json

from lab.config import fingerprint
from lab.paper_run import capabilities
from lab.storage import utc, utc_now


SCHEMA = "PAPER_REFERENCE_DELIVERY_V1"
MAX_PENDING = 16
MAX_LOSSES = 16
TABLE = "paper_reference_delivery"


def ensure(db):
    db.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} (book TEXT NOT NULL, scope TEXT NOT NULL, state TEXT NOT NULL, "
               "PRIMARY KEY(book,scope))")


def _transaction(db):
    if not db.in_transaction:
        raise ValueError("DELIVERY_REQUIRES_WRITER_TRANSACTION")
    ensure(db)


def _load(db, book_id, scope):
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()
    row = db.execute(f"SELECT state FROM {TABLE} WHERE book=? AND scope=?", (book_id, scope)).fetchone() if exists else None
    value = json.loads(row[0]) if row else {"schema": SCHEMA, "book_id": book_id, "source_scope_id": scope,
                                         "pending": [], "losses": [], "checkpoint": None, "last_reserved": None}
    if (value.get("schema") != SCHEMA or value.get("book_id") != book_id or value.get("source_scope_id") != scope
            or not isinstance(value.get("pending"), list) or not isinstance(value.get("losses"), list)
            or len(value["pending"]) > MAX_PENDING or len(value["losses"]) > MAX_LOSSES):
        raise ValueError("MALFORMED_DELIVERY_CHECKPOINT")
    return value


def _save(db, value):
    db.execute(f"INSERT INTO {TABLE} VALUES(?,?,?) ON CONFLICT(book,scope) DO UPDATE SET state=excluded.state",
               (value["book_id"], value["source_scope_id"], json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)))


def state(db, book_id, scope):
    return deepcopy(_load(db, book_id, scope))


def pending(db, book_id, scope=None):
    if scope is not None:
        return deepcopy(_load(db, book_id, scope)["pending"])
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()
    if not exists:
        return []
    values = []
    for scope, in db.execute(f"SELECT scope FROM {TABLE} WHERE book=? ORDER BY scope", (book_id,)):
        values.extend(_load(db, book_id, scope)["pending"])
    return deepcopy(sorted(values, key=lambda item: (item["reserved_at"], item["id"])))


def pending_books(db):
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()
    if not exists:
        return []
    result = []
    for book_id, scope in db.execute(f"SELECT book,scope FROM {TABLE} ORDER BY book,scope"):
        value = _load(db, book_id, scope)
        if value["pending"] or value["losses"]:
            result.append({"book_id": book_id, "source_scope_id": scope,
                           "pending_count": len(value["pending"]), "loss_count": len(value["losses"])})
    return result


def losses(db, book_id, scope):
    return deepcopy(_load(db, book_id, scope)["losses"])


def _stamp(value):
    # utc() alone accepts naive timestamps. Delivery metadata must preserve an
    # actual exchange/local receipt ordering, not invent a timezone.
    import pandas as pd
    result = pd.Timestamp(value)
    if pd.isna(result) or result.tzinfo is None:
        raise ValueError("DELIVERY_TIMESTAMP_REQUIRES_OFFSET")
    return utc(result)


def _point(obs):
    if not isinstance(obs, dict) or not isinstance(obs.get("event_id"), str) or not obs["event_id"]:
        raise ValueError("DELIVERY_REQUIRES_IDENTIFIED_OBSERVATION")
    source, receipt = (_stamp(obs[key]) for key in ("source_at", "received_at"))
    if receipt < source:
        raise ValueError("DELIVERY_RECEIPT_PRECEDES_SOURCE")
    return {"event_id": obs["event_id"], "source_at": source.isoformat(),
            "received_at": receipt.isoformat(), "observation_kind": obs.get("observation_kind", "HTTP_SNAPSHOT")}


def _order(point, prior):
    if not prior:
        return 1
    if point["event_id"] == prior["event_id"]:
        return 0
    if (point.get("observation_kind") == "WEBSOCKET_TRADE"
            and prior.get("observation_kind") == "WEBSOCKET_TRADE"):
        if not point["event_id"].isdigit() or not prior["event_id"].isdigit():
            raise ValueError("DELIVERY_INVALID_TRADE_SEQUENCE")
        if int(point["event_id"]) <= int(prior["event_id"]) or _stamp(point["source_at"]) < _stamp(prior["source_at"]):
            return -1
        return 1
    return 1 if _stamp(point["source_at"]) > _stamp(prior["source_at"]) else -1


def _frozen(position):
    if not position:
        return None
    fields = ("id", "entry_at", "stop", "target", "deadline", "position", "last_mark", "mark_source_at",
              "mark_received_at", "mark_event_id", "last_protection_reference", "unresolved_protection",
              "stop_effective_at", "prior_stop", "last_trail", "entry_evidence")
    value = {key: deepcopy(position[key]) for key in fields if key in position}
    plan = position.get("plan") or {}
    value["plan"] = {key: deepcopy(plan[key]) for key in (
        "contract", "cost", "trigger_basis", "model_financing_rate", "model_financing_interval_minutes", "funding_schedule") if key in plan}
    return value


def _loss(value, work, at, reason):
    target = work["position_id"] or work["pending_id"]
    existing = next((item for item in value["losses"] if item["target_kind"] == work["target_kind"]
                     and item["target_id"] == target and item["reason"] == reason), None)
    if existing is None:
        if len(value["losses"]) >= MAX_LOSSES:
            raise RuntimeError("DELIVERY_LOSS_CAPACITY_REQUIRES_REVIEW")
        existing = {"id": fingerprint([SCHEMA, value["book_id"], value["source_scope_id"], reason, work["id"]]),
                    "book_id": value["book_id"], "source_scope_id": value["source_scope_id"],
                    "target_kind": work["target_kind"], "target_id": target,
                    "position_id": work["position_id"], "pending_id": work["pending_id"],
                    "requires_review": work["protection_required"], "frozen_position": work.get("frozen_position"),
                    "first_event_id": work["event_id"], "first_source_at": work["source_at"],
                    "first_received_at": work["received_at"], "detected_at": at.isoformat(),
                    "reason": reason, "lost_observations": 0,
                    "limitation": "Stored observations were not committed to this consumer; historical protection coverage is unresolved"}
        value["losses"].append(existing)
    existing.update(last_event_id=work["event_id"], last_source_at=work["source_at"],
                    last_received_at=work["received_at"], last_detected_at=at.isoformat(),
                    lost_observations=existing["lost_observations"] + 1)
    return existing


def reserve(db, lane, book, obs, *, at=None, rejection=None):
    """Durably reserve this point for exactly the current exposure or pending idea."""
    _transaction(db)
    if book.get("book_id") != lane.get("book_id"):
        raise ValueError("DELIVERY_BOOK_IDENTITY_MISMATCH")
    position, intention = book.get("position"), book.get("pending")
    if not position and not intention:
        return {"state": "NO_EXPOSURE_OR_PENDING_ENTRY", "book_id": book["book_id"]}
    if not isinstance(obs, dict):
        raise ValueError("DELIVERY_REQUIRES_IDENTIFIED_OBSERVATION")
    if rejection not in (None, "STALE_REFERENCE"):
        raise ValueError("DELIVERY_UNKNOWN_INGESTION_REJECTION")
    scope = capabilities.source_scope_id(lane)
    contract = capabilities.contract(lane)
    expected = contract["source"]
    if obs.get("source") != expected["id"] or obs.get("symbol") != expected["instrument"] or obs.get("observation_kind", "HTTP_SNAPSHOT") != expected["transport"]:
        raise ValueError("DELIVERY_REFERENCE_SOURCE_MISMATCH")
    point = _point(obs)
    point["observation_sha256"] = fingerprint(obs)
    at = utc_now() if at is None else _stamp(at)
    if _stamp(point["received_at"]) > at:
        raise ValueError("DELIVERY_FUTURE_RECEIPT")
    value = _load(db, book["book_id"], scope)
    order = _order(point, value["last_reserved"])
    if order == 0:
        if value["last_reserved"].get("observation_sha256") != point["observation_sha256"]:
            raise ValueError("DELIVERY_SOURCE_EVENT_EVIDENCE_CHANGED")
        return {"state": "ALREADY_RESERVED", "book_id": book["book_id"], "source_scope_id": scope,
                "event_id": point["event_id"]}
    if order < 0:
        return {"state": "REPLAY_OR_REORDERED", "book_id": book["book_id"], "source_scope_id": scope,
                "event_id": point["event_id"]}
    target_kind = "POSITION" if position else "PENDING_ENTRY"
    position_id, pending_id = (position or {}).get("id"), (intention or {}).get("id") if not position else None
    target = position_id or pending_id
    if not isinstance(target, str) or not target:
        raise ValueError("DELIVERY_TARGET_IDENTITY_MISSING")
    work = {"id": fingerprint([SCHEMA, book["book_id"], scope, point["event_id"], target_kind, target]),
            "book_id": book["book_id"], "source_scope_id": scope, **point,
            "target_kind": target_kind, "position_id": position_id, "pending_id": pending_id,
            "protection_required": bool(position and not position.get("exit_observed")),
            "reserved_at": at.isoformat(), "observation": deepcopy(obs), "frozen_position": _frozen(position)}
    if rejection:
        work["ingestion_rejection"] = rejection
    if len(value["pending"]) == MAX_PENDING:
        _loss(value, value["pending"].pop(0), at, "DELIVERY_QUEUE_OVERFLOW")
    value["pending"].append(work)
    value["last_reserved"] = point
    _save(db, value)
    return {"state": "RESERVED", **deepcopy(work)}


def acknowledge(db, book_id, scope, event_id, at, result):
    """Commit acknowledgment atomically with the book's actual financial outcome."""
    _transaction(db)
    value = _load(db, book_id, scope)
    prior = value.get("checkpoint") or {}
    if prior.get("event_id") == event_id:
        if prior.get("result") != result:
            raise ValueError("DELIVERY_ACKNOWLEDGMENT_CHANGED_RESULT")
        return {"state": "ALREADY_ACKNOWLEDGED", "checkpoint": deepcopy(prior)}
    if not value["pending"]:
        raise ValueError("DELIVERY_WORK_NOT_RESERVED")
    work = value["pending"][0]
    if work["event_id"] != event_id:
        raise ValueError("DELIVERY_OUT_OF_ORDER_ACKNOWLEDGMENT")
    at = _stamp(at)
    if at < _stamp(work["reserved_at"]):
        raise ValueError("DELIVERY_ACKNOWLEDGMENT_PRECEDES_RESERVATION")
    status = result.get("state") if isinstance(result, dict) else result
    if status not in ("APPLIED", "PROCESSED", "NO_ECONOMIC_TRANSITION", "EXPIRED", "STALE", "TARGET_CHANGED", "REJECTED"):
        raise ValueError("DELIVERY_ACKNOWLEDGMENT_REQUIRES_EXPLICIT_RESULT")
    if status in ("EXPIRED", "STALE", "TARGET_CHANGED", "REJECTED"):
        _loss(value, work, at, "DELIVERY_" + status)
    value["pending"].pop(0)
    value["checkpoint"] = {key: deepcopy(work[key]) for key in (
        "event_id", "source_at", "received_at", "target_kind", "position_id", "pending_id")}
    value["checkpoint"].update(acknowledged_at=at.isoformat(), result=deepcopy(result))
    _save(db, value)
    return {"state": "ACKNOWLEDGED", "checkpoint": deepcopy(value["checkpoint"])}


def _contains(value, needle):
    if value == needle:
        return True
    if isinstance(value, dict):
        return any(_contains(item, needle) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains(item, needle) for item in value)
    return False


def acknowledge_loss(db, book_id, scope, loss_id, at, evidence):
    """Remove a loss only after its required durable review cause is preserved."""
    _transaction(db)
    value = _load(db, book_id, scope)
    loss = next((item for item in value["losses"] if item["id"] == loss_id), None)
    previous = value.get("loss_checkpoint") or {}
    if loss is None:
        if previous.get("loss_id") == loss_id:
            return {"state": "ALREADY_RESOLVED", "checkpoint": deepcopy(previous)}
        raise ValueError("DELIVERY_LOSS_NOT_FOUND")
    if not isinstance(evidence, dict) or not evidence:
        raise ValueError("DELIVERY_LOSS_REQUIRES_RESOLUTION_EVIDENCE")
    recorded = None
    if loss["requires_review"]:
        for key, kind, idea, raw in db.execute("SELECT key,kind,idea,body FROM event WHERE book=? ORDER BY rowid DESC", (book_id,)):
            body = json.loads(raw)
            selected = evidence.get("event_id") == key or evidence.get("incident_id") == body.get("incident_id")
            if (selected and idea == loss["position_id"] and
                    (kind.startswith("PROTECTION_INCIDENT_") or kind == "PROTECTION_GAP") and _contains(body, loss_id)):
                recorded = {"event_id": key, "incident_id": body.get("incident_id")}
                break
        if recorded is None:
            raise ValueError("DELIVERY_LOSS_REVIEW_EVIDENCE_MISSING")
    elif not evidence.get("reason"):
        raise ValueError("NONEXPOSURE_DELIVERY_LOSS_REQUIRES_EXPLICIT_REASON")
    value["losses"] = [item for item in value["losses"] if item["id"] != loss_id]
    value["loss_checkpoint"] = {"loss_id": loss_id, "resolved_at": _stamp(at).isoformat(),
                                "recorded_evidence": recorded, "resolution": deepcopy(evidence),
                                "first_event_id": loss["first_event_id"], "last_event_id": loss["last_event_id"],
                                "target_id": loss["target_id"], "lost_observations": loss["lost_observations"]}
    _save(db, value)
    return {"state": "RESOLVED", "checkpoint": deepcopy(value["loss_checkpoint"])}
