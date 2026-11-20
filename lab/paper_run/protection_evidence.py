"""Read-only evidence review for prospective protection-incident adjudication."""

from contextlib import closing
from hashlib import sha256
import json
from math import isclose, isfinite
from pathlib import Path
import re
import sqlite3

import pandas as pd

from lab.learning.simulator import Contract, liquidation_price


SCHEMA = "PROTECTION_EVIDENCE_REVIEW_V1"
CHART_SCHEMA = "ASEKE_EMA_PROTECTION_CHART_AUDIT_V1"
SEGMENTED_SCHEMA = "ASEKE_PROTECTION_SEGMENTED_CHART_AUDIT_V1"
SOURCE = "KRAKEN_BTC_USD_LAST_TRADE"
OUTCOMES = {
    "COVERAGE_VERIFIED", "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED",
    "EXIT_IMPACT_POSSIBLE", "INSUFFICIENT_EVIDENCE",
}
MAX_ARTIFACT_BYTES = 16 * 1024**2
MINUTE = pd.Timedelta(minutes=1)


def _stamp(value):
    value = pd.Timestamp(value)
    if pd.isna(value) or value.tzinfo is None:
        raise ValueError("Evidence timestamps require an explicit UTC offset")
    return value.tz_convert("UTC")


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
        raise ValueError("Evidence requires finite numeric prices and quantities")
    return float(value)


def _model(position):
    plan, value = position["plan"], position["position"]
    contract = Contract.load(plan["contract"])
    quantity, margin, entry = (_number(value[key]) for key in ("remaining", "reserved", "entry"))
    side = value["direction"]
    if type(side) is not int or side not in (-1, 1) or min(quantity, margin, entry) <= 0:
        raise ValueError("Invalid frozen position model")
    fee = _number(plan["cost"]["taker"])
    if not 0 <= fee < 1:
        raise ValueError("Invalid frozen reference fee")
    return {
        "entry": entry, "side": side, "entry_at": _stamp(position["entry_at"]).isoformat(),
        "stop": _number(position["stop"]),
        "target": _number(position["target"]) if position.get("target") is not None else None,
        "deadline": _stamp(position["deadline"]).isoformat(), "quantity_remaining": quantity,
        "reserved_margin": margin, "taker_fee": fee, "contract": plan["contract"],
        "liquidation_recomputed": (liquidation_price(entry, margin / quantity, side, contract, fee)
                                   if contract.family == "linear_perp" else None),
    }


def _frozen_model(document, context, left, right):
    position = context["position"]
    if position["id"] != context["position_id"]:
        raise ValueError("Evidence context has the wrong position")
    model = _model(position)
    if _stamp(model["entry_at"]) > left:
        raise ValueError("Reviewed interval begins before the position existed")
    if not context.get("segmented_terms_verified") and (position.get("stop_effective_at") or position.get("prior_stop") or position.get("last_trail")
            or position.get("settled_funding") or position.get("unresolved_costs")
            or position["position"].get("funding", 0) or position.get("unresolved_tier")):
        raise ValueError("Changing stops, financing or terms require a segmented evidence review")
    recorded = document["model"]
    for key, expected in model.items():
        actual = recorded.get(key)
        if isinstance(expected, (int, float)):
            valid = isclose(_number(actual), expected, rel_tol=1e-12, abs_tol=1e-8)
        elif key in ("deadline", "entry_at"):
            valid = _stamp(actual) == _stamp(expected)
        else:
            valid = actual == expected
        if not valid:
            raise ValueError(f"Evidence differs from frozen position model: {key}")
    return model


def _raw_payloads(document):
    records = {}
    for record in document["raw_receipts"]:
        raw, digest = record["raw_utf8"].encode("utf-8"), record["sha256"]
        if digest in records or sha256(raw).hexdigest() != digest:
            raise ValueError("Duplicate or damaged Kraken raw evidence")
        body = json.loads(raw)
        if body.get("error") != [] or set(body["result"]) != {"XXBTZUSD", "last"}:
            raise ValueError("Evidence has the wrong Kraken market or an endpoint error")
        records[digest] = (raw, body["result"]["XXBTZUSD"])
    return records


def _database(context, document):
    if context.get("source_database"):
        approved = Path(context["source_database"]).resolve()
    else:
        from lab.ema_paper import kraken_reference
        from lab.kraken_paper import source

        approved = (kraken_reference.ROOT / "source" / source.FILE).resolve()
    if Path(document["source_database"]).resolve() != approved or not approved.is_file():
        raise ValueError("Evidence source database differs from the approved source store")
    return sqlite3.connect(approved.as_uri() + "?mode=ro", uri=True, timeout=3)


def _chart_review(document, context, left, right):
    model = _frozen_model(document, context, left, right)
    raw = _raw_payloads(document)
    scope = document["scope"]
    start, end = _stamp(scope["left_inclusive"]), _stamp(scope["right_exclusive"])
    if (start != start.floor("1min") or end != end.floor("1min") or not start < end
            or not start <= left < right <= end):
        raise ValueError("Candle scope does not cover the entire reviewed interval")
    expected = set(pd.date_range(start, end, freq="1min", inclusive="left"))
    rows = document["candles"]
    if (scope["missing_minutes"] or len(rows) != len(expected)
            or any(scope[key] != len(expected) for key in ("expected_minutes", "observed_minutes", "unique_minutes"))):
        raise ValueError("Candle evidence has missing, duplicate or inconsistent coverage")
    seen, bounds = set(), []
    with closing(_database(context, document)) as db:
        db.execute("BEGIN")
        for candle in rows:
            opened = _stamp(candle["opened_at"])
            if opened not in expected or opened in seen or candle["interval_minutes"] != 1:
                raise ValueError("Candle evidence has duplicate or unexpected intervals")
            seen.add(opened)
            requested, received, available = (_stamp(candle[key]) for key in (
                "request_started_at", "received_at", "available_at"))
            if not opened + MINUTE <= requested <= received <= available <= _stamp(document["recorded_at"]):
                raise ValueError("Candle was incomplete or its receipt ordering is invalid")
            if candle["stream_id"] != context["stream_id"]:
                raise ValueError("Candle evidence has a different authorized source stream")
            digest = candle["raw_sha256"]
            payload, values = raw[digest]
            # Kraken's final returned OHLC row is the ongoing candle.
            matches = [row for row in values[:-1] if _stamp(pd.Timestamp(row[0], unit="s", tz="UTC")) == opened]
            if len(matches) != 1:
                raise ValueError("No unique final candle in the referenced Kraken payload")
            native = matches[0]
            actual = dict(zip(("open", "high", "low", "close", "volume"), map(float, (*native[1:5], native[6]))))
            if actual != candle["values"]:
                raise ValueError("Candle values differ from their immutable Kraken payload")
            numbers = {key: _number(actual[key]) for key in actual}
            if (min(numbers[key] for key in ("open", "high", "low", "close")) <= 0 or numbers["volume"] < 0
                    or numbers["high"] < max(numbers[key] for key in ("open", "low", "close"))
                    or numbers["low"] > min(numbers[key] for key in ("open", "high", "close"))):
                raise ValueError("Invalid candle range")
            receipt = db.execute("SELECT stream,minutes,started,received,available,raw_sha FROM receipt WHERE id=?",
                                 (candle["receipt_id"],)).fetchone()
            expected_receipt = (context["stream_id"], 1, requested.value, received.value, available.value, digest)
            stored_raw = db.execute("SELECT body FROM raw WHERE sha=?", (digest,)).fetchone()
            observed = db.execute("SELECT values_json FROM observation WHERE stream=? AND minutes=1 AND opened=?",
                                  (context["stream_id"], opened.value)).fetchall()
            if (receipt != expected_receipt or stored_raw is None or bytes(stored_raw[0]) != payload
                    or len(observed) != 1 or json.loads(observed[0][0]) != actual):
                raise ValueError("Candle receipt, source history or raw provenance does not reconcile")
            if opened < right and opened + MINUTE > left:
                bounds.append((numbers["low"], numbers["high"]))
    low, high = min(row[0] for row in bounds), max(row[1] for row in bounds)
    stop, target, liquidation = model["stop"], model["target"], model["liquidation_recomputed"]
    crossed = (high >= stop or target is not None and low <= target or liquidation is not None and high >= liquidation
               if model["side"] == -1 else
               low <= stop or target is not None and high >= target or liquidation is not None and low <= liquidation)
    time_exit_possible = _stamp(model["deadline"]) < right
    if model["quantity_remaining"] * high > model["contract"]["tier_notional"]:
        raise ValueError("Candle evidence exceeds the frozen instrument tier")
    return {"candle_count": len(rows), "minimum_low": low, "maximum_high": high,
            "threshold_crossing_possible": crossed or time_exit_possible, "time_exit_possible": time_exit_possible,
            "frozen_model": model,
            "native_receipts_reconciled": True, "full_trade_stream_coverage_verified": False}


def _segmented_review(document, context, left, right):
    """Reconcile every effective interval against journal-derived terms.

    A minute straddling a stop change is conservatively tested against both
    applicable models. This may require a possible-impact disposition; it
    cannot turn coarse candle evidence into favorable sub-minute proof.
    """
    expected = context.get("terms_segments") or []
    supplied = document.get("segments") or []
    if not expected or len(supplied) != len(expected):
        raise ValueError("Segmented evidence must cover every journal-derived effective interval")
    if _stamp(expected[0]["start"]) != left or _stamp(expected[-1]["end"]) != right:
        raise ValueError("Effective term segments do not cover the entire reviewed interval")
    checks, previous = [], left
    for actual, frozen in zip(supplied, expected):
        start, end = _stamp(frozen["start"]), _stamp(frozen["end"])
        if (start != previous or not start < end or _stamp(actual["interval_start"]) != start
                or _stamp(actual["interval_end"]) != end
                or actual.get("effective_event_ids", []) != frozen.get("effective_event_ids", [])):
            raise ValueError("Evidence effective intervals or event provenance differ from the journal")
        inner = {**context, "position": frozen["position"], "segmented_terms_verified": True}
        chart = {**document, **actual, "schema": CHART_SCHEMA}
        result = _chart_review(chart, inner, start, end)
        checks.append({"start": start.isoformat(), "end": end.isoformat(),
                       "effective_event_ids": frozen.get("effective_event_ids", []), **result})
        previous = end
    return {"effective_segments": checks,
            "threshold_crossing_possible": any(item["threshold_crossing_possible"] for item in checks),
            "time_exit_possible": any(item["time_exit_possible"] for item in checks),
            "native_receipts_reconciled": True, "full_trade_stream_coverage_verified": False}


def validate_evidence(manifest, context):
    """Validate a claimed outcome against preserved evidence without changing any state.

    The context is supplied by the incident journal, never from the evidence file.
    This review supports native Kraken candle audits and explicit insufficiency notes.
    """
    try:
        outcome = manifest["outcome"]
        if outcome not in OUTCOMES:
            raise ValueError("Unsupported incident adjudication outcome")
        if outcome == "COVERAGE_VERIFIED":
            raise ValueError("This evidence adapter cannot establish complete historical trade coverage")
        provenance = manifest["provenance"]
        if any(not isinstance(provenance.get(key), str) or not provenance[key].strip() or len(provenance[key]) > 500
               for key in ("origin", "operator", "acquired_at")):
            raise ValueError("Explicit acquisition provenance and operator identity are required")
        reviewed_at = _stamp(context.get("reviewed_at", pd.Timestamp.now(tz="UTC")))
        if _stamp(provenance["acquired_at"]) > reviewed_at:
            raise ValueError("Acquisition provenance is future dated")
        for key in ("source", "symbol", "trigger_basis"):
            expected = context[key]
            if not isinstance(expected, str) or not expected or provenance.get(key) != expected:
                raise ValueError("Evidence does not match the approved instrument and trigger basis")
        if provenance.get("stream_id") != context["stream_id"]:
            raise ValueError("Evidence provenance has a different authorized source stream")
        identity = context.get("source_identity")
        if identity and (context["source"] != SOURCE or provenance.get("source_identity") is not None):
            if provenance.get("source_identity") != identity:
                raise ValueError("Evidence provenance has a different complete source identity")
        supplied_path = Path(manifest["path"])
        path = supplied_path.resolve()
        if supplied_path.is_symlink() or path.stat().st_size > MAX_ARTIFACT_BYTES:
            raise ValueError("Evidence artifact path or size is invalid")
        raw = path.read_bytes()
        digest = manifest["sha256"]
        if not re.fullmatch(r"[0-9a-f]{64}", digest) or sha256(raw).hexdigest() != digest:
            raise ValueError("Evidence artifact hash does not match its reviewed bytes")
        document = json.loads(raw)
        for key in ("generation", "book_id", "position_id"):
            if document[key] != context[key]:
                raise ValueError(f"Evidence has a different {key}")
        left = _stamp(context["interval_start"]) if context.get("interval_start") else None
        right = _stamp(context["interval_end"]) if context.get("interval_end") else None
        if left is not None and right is not None and left >= right:
            raise ValueError("An explicit nonempty reviewed interval is required")
        if left is not None and left > reviewed_at:
            raise ValueError("The historical interval is future dated")
        if (not _stamp(document["recorded_at"]) <= reviewed_at
                or right is not None and _stamp(document["recorded_at"]) < right):
            raise ValueError("Evidence recording time is outside the interval and review bounds")
        if (left is None or right is None) and outcome != "INSUFFICIENT_EVIDENCE":
            raise ValueError("Unknown historical intervals permit only explicit insufficiency")
        checks = {}
        adapter = context.get("evidence_adapter", "kraken_native_trade_bounds_v1" if context["source"] == SOURCE else
                              "explicit_insufficiency_v1")
        if document.get("schema") in (CHART_SCHEMA, SEGMENTED_SCHEMA):
            if left is None or right is None:
                raise ValueError("A candle review cannot invent an unknown historical interval")
            if (adapter != "kraken_native_trade_bounds_v1" or context["source"] != SOURCE
                    or context["symbol"] != "BTC/USD" or context["trigger_basis"] != SOURCE):
                raise ValueError("No approved favorable evidence adapter matches this source and trigger basis")
        if document.get("schema") == SEGMENTED_SCHEMA:
            checks = _segmented_review(document, context, left, right)
        elif document.get("schema") == CHART_SCHEMA:
            if len(context.get("terms_segments", [])) > 1:
                raise ValueError("Changing historical terms require segmented evidence for every effective interval")
            checks = _chart_review(document, context, left, right)
        elif outcome != "INSUFFICIENT_EVIDENCE":
            raise ValueError("No approved evidence adapter supports this outcome")
        if outcome == "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED" and checks["threshold_crossing_possible"]:
            raise ValueError("The candle range touches or crosses an exit threshold")
        if outcome == "EXIT_IMPACT_POSSIBLE" and not checks["threshold_crossing_possible"]:
            raise ValueError("The reviewed evidence does not support a possible threshold crossing")
        return {
            "schema": SCHEMA, "outcome": outcome, "historical_coverage": "UNRESOLVED",
            "exit_impact": {"NO_EXIT_THRESHOLD_CROSSING_SUPPORTED": "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED",
                            "EXIT_IMPACT_POSSIBLE": "EXIT_IMPACT_POSSIBLE"}.get(outcome, "UNRESOLVED"),
            "evidence": [{"path": str(path), "sha256": digest,
                          "provenance": {key: provenance[key] for key in (
                              "origin", "operator", "acquired_at", "source", "symbol", "trigger_basis", "stream_id")},
                          "source_identity": identity, "adapter": adapter}],
            "reviewed_interval": {"start": left.isoformat() if left is not None else None,
                                  "end": right.isoformat() if right is not None else None},
            "historical_model_available": not context.get("historical_model_unavailable", False),
            "checks": checks,
        }
    except (OSError, KeyError, IndexError, AttributeError, TypeError, OverflowError, sqlite3.Error) as error:
        raise ValueError(f"Protection evidence is incomplete or inaccessible: {type(error).__name__}") from error


def review_evidence(manifest, context):
    """Produce an offline decision report. Approval and state mutation are separate."""
    try:
        return {"valid": True, **validate_evidence(manifest, context)}
    except ValueError as error:
        return {"schema": SCHEMA, "valid": False, "reason": str(error), "historical_coverage": "UNRESOLVED"}
