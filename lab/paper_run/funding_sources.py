"""Read-only, source-correct funding receipts and preserved forward references.

No acquisition, table creation, financial application or newest-revision policy
is provided here. Native history and the forward mark have different roles.
"""

from contextlib import closing
from hashlib import sha256
import json
from math import isfinite
from pathlib import Path
import re
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.phase9.data import settled_rates
from lab.phase9.public import funding, ROUTES
from lab.phase13 import observation
from lab.phase14 import outcomes
from lab.phase15 import feeds
from lab.paper_run import capabilities, execution


SCHEMA = "PAPER_FUNDING_SOURCE_EVIDENCE_V1"
MAX_SLOTS = 64
MAX_REVISIONS = 64
MAX_QUERIES = 32
MAX_ARCHIVED_FORWARD = 64
MAX_VALIDATED_RAW_BYTES = 8 * 1024 * 1024
STAMP_TOLERANCE = pd.Timedelta(seconds=5)
RAW_HASH_LIMITATION = "RECORDED_HASH_ONLY_RAW_BYTES_NOT_RETAINED"
SOURCE = {"id": "KUCOIN_XBTUSDTM_SETTLED_FUNDING_RATE_HISTORY", "venue": "KuCoin", "instrument": "XBTUSDTM",
          "product": "LINEAR_PERP", "basis": "SETTLED_FUNDING_RATE", "transport": "HTTP_HISTORY",
          "endpoint": ROUTES["funding"], "version": "kucoin-ua-v2-funding-history-v1"}
ERRORS = (OSError, ValueError, KeyError, TypeError, OverflowError, sqlite3.Error)


class FundingEvidenceError(ValueError):
    def __init__(self, code, detail=""):
        self.code = code
        super().__init__(code + (": " + str(detail) if detail else ""))


def _require(condition, code):
    if not condition:
        raise FundingEvidenceError(code)


def _stamp(value):
    try:
        return execution.stamp(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise FundingEvidenceError("FUNDING_TIMESTAMP_INVALID") from error


def _digest(raw):
    return sha256(raw).hexdigest()


def _hash(value):
    _require(isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None, "FUNDING_RAW_HASH_INVALID")
    return value


def _number(value):
    _require(type(value) in (float, int) and isfinite(value), "FUNDING_RATE_INVALID")
    return float(value)


def _bound_sources(body):
    amendment = feeds.futures_source(body)
    instrument = amendment["body"]["instrument"]
    _require(amendment["body"].get("registration_version") == "phase14-feature-context-amendment-v1",
             "FUNDING_SOURCE_AUTHORITY_VERSION_UNSUPPORTED")
    _require(instrument.get("venue") == "KuCoin" and instrument.get("symbol") == "XBTUSDTM"
             and instrument.get("trade_type") == "FUTURES" and instrument.get("settlement") == "USDT",
             "FUNDING_SOURCE_INSTRUMENT_MISMATCH")
    stores = outcomes.stores(amendment)
    _require(stores and len(stores) <= 2 and all(store["id"] in {body["source_protocol"], amendment["id"]} for store in stores),
             "FUNDING_SOURCE_STORE_AUTHORITY_MISMATCH")
    grace = amendment["body"].get("rules", {}).get("funding_receipt_grace_seconds")
    _require(type(grace) is int and 0 <= grace <= 86400, "FUNDING_PUBLICATION_GRACE_UNDECLARED")
    scoped = [{**store, "_funding_closeout": {"allowed": store["id"] == amendment["id"],
                "amendment_start": amendment["body"]["start"], "publication_grace_seconds": grace}}
              for store in stores]
    return scoped, grace


def _policy(store):
    policy = store["body"]["acquisition"]
    step, poll, grace = (policy.get(key) for key in ("step_minutes", "funding_poll_minutes", "close_grace_seconds"))
    _require(all(type(value) is int for value in (step, poll, grace)) and 1 <= step <= 60 and 60 % step == 0
             and 1 <= poll <= 60 and 60 % poll == 0 and poll % step == 0 and 0 <= grace <= 300,
             "FUNDING_COLLECTION_CADENCE_UNSUPPORTED")
    return policy


def _collection_after(slot, policy, *, inclusive=False):
    step = pd.Timedelta(minutes=policy["step_minutes"])
    point = slot.ceil(step)
    if point < slot or (point == slot and not inclusive):
        point += step
    for _ in range(61):
        if point.minute % policy["funding_poll_minutes"] == 0:
            return point + pd.Timedelta(seconds=policy["close_grace_seconds"])
        point += step
    raise FundingEvidenceError("FUNDING_COLLECTION_CADENCE_UNSUPPORTED")


def _collection_times(slot, store, publication_grace):
    policy = _policy(store)
    start, end = (_stamp(store["body"][key]) for key in ("start", "end_exclusive"))
    context = store.get("_funding_closeout", {})
    closeout = None
    if context.get("allowed") and max(_stamp(context["amendment_start"]), end-pd.Timedelta(hours=policy["funding_overlap_hours"])) <= slot < end:
        closeout = end+pd.Timedelta(seconds=publication_grace)
    normal = start <= slot < end
    if not normal and closeout is None:
        return None
    first = min(_collection_after(slot, policy), end+pd.Timedelta(seconds=policy["close_grace_seconds"])) if normal else closeout
    confirmation = _collection_after(slot+pd.Timedelta(seconds=publication_grace), policy, inclusive=True)
    if not normal or confirmation > end+pd.Timedelta(seconds=policy["close_grace_seconds"]):
        confirmation = closeout or max(first, end+pd.Timedelta(seconds=policy["close_grace_seconds"]))
    return first, max(first, confirmation)


def _spec(value, store, boundary):
    _require(isinstance(value, dict), "FUNDING_REQUEST_SPEC_INVALID")
    left, right = _stamp(value["start"]), _stamp(value["end_exclusive"])
    policy = _policy(store)
    context = store.get("_funding_closeout", {})
    horizon = _stamp(store["body"]["end_exclusive"])
    if value.get("key") == "amended-funding-closeout":
        _require(context.get("allowed") and boundary == horizon.isoformat()+"/funding-closeout"
                 and right == horizon and left == max(_stamp(context["amendment_start"]), horizon-pd.Timedelta(hours=policy["funding_overlap_hours"])),
                 "FUNDING_CLOSEOUT_REQUEST_UNAUTHORIZED")
    else:
        _require(_stamp(store["body"]["start"]) <= left, "FUNDING_REQUEST_RANGE_UNAUTHORIZED")
    _require(left < right <= horizon and right-left <= pd.Timedelta(hours=policy["funding_overlap_hours"]),
             "FUNDING_REQUEST_RANGE_UNAUTHORIZED")
    key = value.get("key")
    _require(isinstance(key, str) and 1 <= len(key) <= 128 and value == funding(key, left, right),
             "FUNDING_REQUEST_SOURCE_MISMATCH")
    return left, right


def _raw(store, receipt, spec, cache):
    digest = _hash(receipt.get("raw_sha256"))
    root = observation.directory(store).resolve()
    key = str(root), digest, fingerprint(spec)
    if key not in cache:
        raw_key = "RAW", str(root), digest
        if raw_key not in cache:
            path = root / "raw" / (digest + ".json")
            _require(path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(root)
                     and path.stat().st_size <= _policy(store)["max_response_bytes"], "FUNDING_RAW_BYTES_UNAVAILABLE")
            _require(cache.get("validated_bytes", 0)+path.stat().st_size <= MAX_VALIDATED_RAW_BYTES,
                     "FUNDING_RAW_VALIDATION_BUDGET_REQUIRES_REVIEW")
            raw = path.read_bytes()
            _require(_digest(raw) == digest, "FUNDING_RAW_HASH_MISMATCH")
            payload = json.loads(raw)
            _require(isinstance(payload, dict) and payload.get("code") == "200000" and isinstance(payload.get("data"), dict),
                     "FUNDING_NATIVE_RESPONSE_INVALID")
            rows = payload["data"].get("list")
            _require(isinstance(rows, list) and all(isinstance(row, dict) and not isinstance(row.get("fundingRate"), bool) for row in rows),
                     "FUNDING_NATIVE_RESPONSE_INVALID")
            cache[raw_key] = payload["data"], str(path)
            cache["validated_bytes"] = cache.get("validated_bytes", 0)+len(raw)
        data, path = cache[raw_key]
        try:
            frame = settled_rates(data, spec)
        except ValueError as error:
            if "Conflicting duplicate" in str(error):
                raise FundingEvidenceError("FUNDING_NATIVE_RESPONSE_CONFLICT") from error
            raise FundingEvidenceError("FUNDING_NATIVE_RESPONSE_INVALID") from error
        cache[key] = frame, path
    frame, path = cache[key]
    if "rows" in receipt:
        _require(type(receipt["rows"]) is int and receipt["rows"] == len(frame), "FUNDING_RECEIPT_ROW_COUNT_MISMATCH")
    return cache[key]


def _query(row, store, slot, now, cache):
    identifier, boundary, encoded, reserved, receipt_text = row
    spec = json.loads(encoded)
    left, right = _spec(spec, store, boundary)
    reserved_at = _stamp(reserved)
    _require(right <= reserved_at <= now, "FUNDING_REQUEST_TIME_INCONSISTENT")
    if spec["key"] == "amended-funding-closeout":
        _require(reserved_at >= right+pd.Timedelta(seconds=store["_funding_closeout"]["publication_grace_seconds"]),
                 "FUNDING_CLOSEOUT_BEFORE_REGISTERED_GRACE")
    receipt = json.loads(receipt_text) if receipt_text else {}
    _require(isinstance(receipt, dict), "FUNDING_REQUEST_RECEIPT_INVALID")
    at = _stamp(receipt["received_at"]) if receipt.get("received_at") else None
    _require(at is None or reserved_at <= at <= now, "FUNDING_RECEIPT_FUTURE_OR_INCONSISTENT")
    status = receipt.get("status", "RECEIPT_PENDING")
    value = {"store_id": store["id"], "request_id": identifier, "boundary": boundary,
             "start": left.isoformat(), "end_exclusive": right.isoformat(), "reserved_at": reserved_at.isoformat(),
             "received_at": at.isoformat() if at is not None else None, "status": status,
             "covers_settlement": left <= slot < right, "raw_sha256": receipt.get("raw_sha256"),
             "request_sha256": fingerprint(spec), "receipt_sha256": fingerprint(receipt)}
    if status == "RECEIVED":
        _require(at is not None and receipt.get("http_status") == 200, "FUNDING_SUCCESS_RECEIPT_INVALID")
        frame, path = _raw(store, receipt, spec, cache)
        value.update(raw_path=path, raw_bytes_verified=True, parsed_rows=len(frame))
    return value, spec, receipt


def _candidate(db, store, row, slot, now, kind, cache):
    # Original item and corrections retain their own native and receipt labels.
    actual, close, received, digest, encoded = row
    at, receipt_at = _stamp(actual), _stamp(received)
    _require(abs(at-slot) <= STAMP_TOLERANCE and _stamp(close) == at and at <= receipt_at <= now,
             "FUNDING_SETTLEMENT_TIME_INCONSISTENT")
    body = json.loads(encoded)
    _require(isinstance(body, dict) and _stamp(body["settlement_at"]) == at, "FUNDING_ITEM_SOURCE_TIMESTAMP_MISMATCH")
    rate = _number(body["rate"])
    digest = _hash(digest)
    matches = db.execute("SELECT id,boundary,spec,reserved,receipt FROM request WHERE basis='funding' "
                         "AND json_extract(receipt,'$.raw_sha256')=? AND json_extract(receipt,'$.received_at')=? "
                         "ORDER BY id LIMIT 3", (digest, received)).fetchall()
    _require(len(matches) == 1, "FUNDING_ITEM_RECEIPT_PROVENANCE_UNVERIFIED")
    query, spec, receipt = _query(matches[0], store, slot, now, cache)
    _require(query["status"] == "RECEIVED" and _stamp(spec["start"]) <= at < _stamp(spec["end_exclusive"]),
             "FUNDING_ITEM_REQUEST_DOES_NOT_COVER_SETTLEMENT")
    frame, path = _raw(store, receipt, spec, cache)
    native = frame.loc[frame.settlement_at == at]
    _require(len(native) == 1 and float(native.iloc[0].rate) == rate, "FUNDING_ITEM_DIFFERS_FROM_NATIVE_RAW")
    return {"rate": rate, "actual_at": at.isoformat(), "received_at": receipt_at.isoformat(),
            "raw_sha256": digest, "store_id": store["id"], "revision_kind": kind,
            "evidence": {"schema": SCHEMA, "source_identity": dict(SOURCE), "source_scope_id": fingerprint(SOURCE),
                         "request_id": query["request_id"], "request_sha256": fingerprint(spec),
                         "receipt_sha256": fingerprint(receipt), "raw_path": path, "raw_bytes_verified": True,
                         "stored_body_sha256": _digest(encoded.encode("utf-8"))}}


def _error(error, store_id=None, kind=None):
    return {"reason_code": error.code if isinstance(error, FundingEvidenceError) else "FUNDING_SOURCE_EVIDENCE_UNAVAILABLE",
            "detail": str(error), "store_id": store_id, "revision_kind": kind}


def _result(slot):
    return {"schema": SCHEMA, "settlement_at": slot.isoformat(), "source_identity": dict(SOURCE),
            "state": "UNAVAILABLE", "candidates": [], "selected": None, "queries": [], "rejected_evidence": [],
            "first_eligible_collection_at": None, "collection_deadline_at": None,
            "reason_code": None, "next_action": None, "query_history_truncated": False}


def rates(body, slots, now):
    """Inspect admitted receipts; never query a venue or silently choose a revision."""
    now = _stamp(now)
    slots = sorted(set(_stamp(value) for value in slots))
    _require(len(slots) <= MAX_SLOTS and all(value == value.floor("min") for value in slots), "FUNDING_SLOT_SET_INVALID")
    if not slots:
        return {}
    result = {slot.isoformat(): _result(slot) for slot in slots}
    try:
        stores, publication_grace = _bound_sources(body)
        for store in stores:
            _policy(store)
    except ERRORS as error:
        for value in result.values():
            value.update(reason_code="FUNDING_SOURCE_AUTHORITY_UNAVAILABLE", next_action="REVIEW_REGISTERED_SOURCE_AUTHORITY",
                         rejected_evidence=[_error(error)])
        return result
    cache = {}
    for slot in slots:
        value = result[slot.isoformat()]
        times = [candidate_times for store in stores
                 if (candidate_times := _collection_times(slot, store, publication_grace)) is not None]
        if not times:
            value.update(reason_code="FUNDING_SETTLEMENT_OUTSIDE_REGISTERED_COLLECTION_RANGE",
                         next_action="REVIEW_REGISTERED_SOURCE_HORIZON_WITHOUT_EXTENDING_OR_FABRICATING_EVIDENCE")
            continue
        first, deadline = min(item[0] for item in times), min(item[1] for item in times)
        value.update(first_eligible_collection_at=first.isoformat(), collection_deadline_at=deadline.isoformat(),
                     native_publication_grace_seconds=publication_grace)
        for store in stores:
            try:
                path = observation.directory(store) / "observations.sqlite3"
                with closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True, timeout=3)) as db:
                    db.execute("BEGIN")
                    left, right = (slot-STAMP_TOLERANCE).isoformat(), (slot+STAMP_TOLERANCE).isoformat()
                    for kind, query in (("ORIGINAL_ITEM", "SELECT source_at,source_close,received_at,raw_sha,body FROM item WHERE basis='funding' AND source_at>=? AND source_at<=? ORDER BY received_at LIMIT ?"),
                                        ("CORRECTION", "SELECT source_at,source_at,received_at,raw_sha,body FROM correction WHERE basis='funding' AND source_at>=? AND source_at<=? ORDER BY received_at LIMIT ?")):
                        rows = db.execute(query, (left, right, MAX_REVISIONS+1)).fetchall()
                        _require(len(rows)+len(value["candidates"]) <= MAX_REVISIONS, "FUNDING_REVISION_CAPACITY_REQUIRES_REVIEW")
                        for row in rows:
                            try:
                                value["candidates"].append(_candidate(db, store, row, slot, now, kind, cache))
                            except ERRORS as error:
                                value["rejected_evidence"].append(_error(error, store["id"], kind))
                    rows = db.execute("SELECT id,boundary,spec,reserved,receipt FROM request WHERE basis='funding' "
                                      "AND json_extract(spec,'$.start')<=? AND json_extract(spec,'$.end_exclusive')>=? "
                                      "ORDER BY id DESC LIMIT ?", (slot.isoformat(), slot.isoformat(), MAX_QUERIES+1)).fetchall()
                    value["query_history_truncated"] |= len(rows) > MAX_QUERIES
                    for row in rows[:MAX_QUERIES]:
                        try:
                            value["queries"].append(_query(row, store, slot, now, cache)[0])
                        except ERRORS as error:
                            value["rejected_evidence"].append(_error(error, store["id"], "REQUEST"))
            except ERRORS as error:
                value["rejected_evidence"].append(_error(error, store["id"]))
        value["candidates"].sort(key=lambda candidate: (candidate["received_at"], candidate["store_id"], candidate["revision_kind"]))
        value["queries"].sort(key=lambda query: (query["reserved_at"], query["store_id"], query["request_id"]), reverse=True)
        economics = {(candidate["rate"], candidate["actual_at"]) for candidate in value["candidates"]}
        errors = value["rejected_evidence"]
        if len(economics) > 1 or any(error["reason_code"] == "FUNDING_NATIVE_RESPONSE_CONFLICT" for error in errors):
            value.update(state="CONFLICT", reason_code="FUNDING_NATIVE_REVISIONS_CONFLICT", next_action="REVIEW_ALL_NATIVE_REVISIONS_WITHOUT_OVERWRITING_PRIOR_FINANCING")
        elif errors:
            value.update(state="UNAVAILABLE", reason_code=errors[0]["reason_code"], next_action="PRESERVE_AND_REVIEW_INVALID_OR_INACCESSIBLE_SOURCE_EVIDENCE")
        elif economics:
            # Equivalent observations have one economic value; earliest receipt
            # is canonical provenance, never a newest-wins correction policy.
            value.update(state="AVAILABLE", reason_code="NATIVE_SETTLED_RATE_VERIFIED", selected=value["candidates"][0],
                         next_action="AUTHORITATIVE_WRITER_MAY_RECONCILE_WITH_REQUIRED_FORWARD_VALUATION")
        else:
            covering = [query for query in value["queries"] if query["covers_settlement"]]
            latest = covering[0] if covering else None
            if latest and latest["status"] not in ("RECEIVED", "RECEIPT_PENDING"):
                value.update(state="UNAVAILABLE", reason_code="FUNDING_COVERING_QUERY_FAILED", next_action="WAIT_FOR_EXISTING_AUTHORIZED_COLLECTOR_RETRY_OR_REVIEW_SOURCE_RESTRICTION")
            elif latest and latest["status"] == "RECEIPT_PENDING" and now-_stamp(latest["reserved_at"]) > pd.Timedelta(seconds=90):
                value.update(state="UNAVAILABLE", reason_code="FUNDING_QUERY_RECEIPT_MISSING", next_action="REVIEW_COLLECTOR_OWNERSHIP_AND_UNFINISHED_REQUEST_WITHOUT_DUPLICATE_ACQUISITION")
            elif now <= deadline:
                reason = "FUNDING_FIRST_COVERING_COLLECTION_NOT_DUE" if now < first else "FUNDING_SETTLEMENT_PUBLICATION_PENDING"
                value.update(state="PENDING", reason_code=reason, next_action="WAIT_FOR_EXISTING_SCHEDULED_FUNDING_COLLECTION")
            elif latest and latest["status"] == "RECEIVED" and _stamp(latest["received_at"]) >= slot+pd.Timedelta(seconds=publication_grace):
                value.update(state="MISSING", reason_code="NATIVE_SETTLED_RATE_MISSING_AFTER_GRACE", next_action="REVIEW_CONFIRMED_MISSING_NATIVE_SETTLEMENT_AND_EXISTING_AUTHORIZED_EVIDENCE")
            else:
                value.update(state="UNAVAILABLE", reason_code="FUNDING_FINALITY_QUERY_UNCONFIRMED", next_action="REVIEW_MISSING_CONFIRMING_COLLECTION_WITHOUT_INVENTING_A_RATE")
    return result


def archived_forward(db, lane, position, slot, now):
    """Return original accepted HTTP mark receipts, never native candles or current marks.

    This is a read-only review candidate. Historical admission remains separate
    from fresh entry-price admission and never backdates a financial event.
    """
    slot, now = _stamp(slot), _stamp(now)
    _require(slot == slot.floor("min"), "ARCHIVED_FORWARD_SETTLEMENT_SLOT_INVALID")
    try:
        identity = capabilities.source_identity(lane)
    except capabilities.UnsupportedCapability as error:
        raise FundingEvidenceError("ARCHIVED_FORWARD_SOURCE_UNSUPPORTED", str(error)) from error
    _require(identity["id"] == capabilities.MARK and identity["instrument"] == "XBTUSDTM"
             and identity["product"] == "LINEAR_PERP" and identity["transport"] == "HTTP_SNAPSHOT",
             "ARCHIVED_FORWARD_SOURCE_UNSUPPORTED")
    _require(isinstance(position.get("id"), str) and position["id"] and _stamp(position["entry_at"]) < slot <= now,
             "ARCHIVED_FORWARD_POSITION_INTERVAL_INVALID")
    if position.get("exit_observed"):
        _require(slot <= _stamp(position["exit_observed"]["exit_at"]) <= now, "ARCHIVED_FORWARD_SETTLEMENT_AFTER_EXIT")
    rows = db.execute("SELECT source,at,event_id,body FROM observation WHERE source=? AND at>=? AND at<=? "
                      "ORDER BY at,event_id LIMIT ?", (lane["source"], slot.isoformat(),
                       (slot+execution.PROTECTION_GAP).isoformat(), MAX_ARCHIVED_FORWARD+1)).fetchall()
    _require(len(rows) <= MAX_ARCHIVED_FORWARD, "ARCHIVED_FORWARD_CAPACITY_REQUIRES_REVIEW")
    candidates = []
    for source, at, identifier, encoded in rows:
        point = json.loads(encoded)
        _require(isinstance(point, dict) and point.get("status") == "RECEIVED" and source == point.get("source") == lane["source"]
                 and at == point.get("source_at") and identifier == point.get("event_id") == point.get("source_at"),
                 "ARCHIVED_FORWARD_OBSERVATION_IDENTITY_MISMATCH")
        received, actual = _stamp(point["received_at"]), _stamp(point["source_at"])
        _require(slot <= actual <= slot+execution.PROTECTION_GAP and received <= now,
                 "ARCHIVED_FORWARD_RECEIPT_FUTURE_OR_OUTSIDE_WINDOW")
        _hash(point.get("raw_sha256"))
        failure = execution.valid_snapshot(point, lane, received, point.get("received_mono_ns"))
        _require(failure is None, "ARCHIVED_FORWARD_ORIGINAL_RECEIPT_INVALID: " + str(failure))
        candidates.append({"schema": SCHEMA, "recovery_label": "PRESERVED_ACCEPTED_FORWARD_EVIDENCE",
                           "position_id": position["id"], "settlement_at": slot.isoformat(),
                           "source_at": point["source_at"], "received_at": point["received_at"],
                           "observation_event_id": identifier,
                           "price": point["price"], "price_text": point["price_text"], "raw_sha256": point["raw_sha256"],
                           "source_identity": identity, "source_scope_id": capabilities.source_scope_id(lane),
                           "observation_identity": {"source": source, "at": at, "event_id": identifier},
                           "observation": point, "observation_sha256": fingerprint(point),
                           "observation_body_sha256": _digest(encoded.encode("utf-8")),
                           "raw_hash_status": RAW_HASH_LIMITATION, "historical_reconstruction": False,
                           "validation_basis": "ORIGINAL_ACCEPTED_TABLE_AND_ORIGINAL_RECEIPT_TIME_REVALIDATION",
                           "limitations": ["Original HTTP raw bytes are not retained by this adapter",
                                           "The accepted table records original admission; its exact decision-time clock bound is not independently reconstructed",
                                           "Historical evidence is not a fresh entry reference and grants no financial write permission"]})
    return candidates
