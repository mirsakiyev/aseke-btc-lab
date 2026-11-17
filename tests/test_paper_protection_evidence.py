from copy import deepcopy
from hashlib import sha256
import json
import sqlite3

import pandas as pd
import pytest

from lab.paper_run import protection_evidence as evidence


BASE = pd.Timestamp("2026-11-17T16:00:00Z")
CONTRACT = {
    "family": "linear_perp", "quote": "USDT", "base_per_contract": 1., "quantity_step": .000001,
    "price_tick": .01, "min_contracts": .000001, "tier_notional": 1e12, "max_leverage": 5,
    "maintenance_rate": .01, "liquidation_fee_rate": .005,
}


@pytest.fixture
def chart(tmp_path):
    database = tmp_path / "source.sqlite3"
    position = {
        "id": "position", "entry_at": BASE.isoformat(), "deadline": (BASE + pd.Timedelta(hours=8)).isoformat(),
        "stop": 110., "target": 90., "plan": {"contract": CONTRACT, "cost": {"taker": .001}},
        "position": {"entry": 100., "remaining": 1., "reserved": 33., "direction": -1, "funding": 0.},
    }
    document = {
        "schema": evidence.CHART_SCHEMA, "generation": "generation", "book_id": "book", "position_id": "position",
        "recorded_at": (BASE + pd.Timedelta(minutes=10)).isoformat(), "source_database": str(database),
        "scope": {"left_inclusive": BASE.isoformat(), "right_exclusive": (BASE + pd.Timedelta(minutes=3)).isoformat(),
                  "expected_minutes": 3, "observed_minutes": 3, "unique_minutes": 3, "missing_minutes": []},
        "model": {"entry": 100., "side": -1, "entry_at": BASE.isoformat(), "stop": 110., "target": 90.,
                  "deadline": position["deadline"], "quantity_remaining": 1., "reserved_margin": 33.,
                  "taker_fee": .001, "contract": CONTRACT, "liquidation_recomputed": 133. / 1.016},
        "candles": [], "raw_receipts": [],
    }
    context = {
        "generation": "generation", "book_id": "book", "position_id": "position",
        "source": evidence.SOURCE, "symbol": "BTC/USD", "trigger_basis": evidence.SOURCE, "stream_id": "stream",
        "interval_start": (BASE + pd.Timedelta(seconds=30)).isoformat(),
        "interval_end": (BASE + pd.Timedelta(minutes=2, seconds=30)).isoformat(),
        "position": position, "source_database": str(database), "reviewed_at": BASE + pd.Timedelta(minutes=20),
    }
    manifest = {
        "outcome": "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED", "path": str(tmp_path / "audit.json"),
        "provenance": {"operator": "test-operator", "origin": "PRESERVED_NATIVE_KRAKEN_OHLC",
                       "acquired_at": document["recorded_at"], "source": evidence.SOURCE, "symbol": "BTC/USD",
                       "trigger_basis": evidence.SOURCE, "stream_id": "stream"},
    }
    for minute in range(3):
        opened = BASE + pd.Timedelta(minutes=minute)
        document["candles"].append({
            "opened_at": opened.isoformat(), "interval_minutes": 1, "stream_id": "stream",
            "request_started_at": (opened + pd.Timedelta(seconds=61)).isoformat(),
            "received_at": (opened + pd.Timedelta(seconds=61.1)).isoformat(),
            "available_at": (opened + pd.Timedelta(seconds=61.2)).isoformat(),
            "receipt_id": f"receipt-{minute}", "values": {"open": 100., "high": 101., "low": 99., "close": 100., "volume": 2.},
        })
    return manifest, context, document


def save_chart(manifest, context, document):
    """Create independently matching native receipts for each test's candle payload."""
    db = sqlite3.connect(context["source_database"])
    db.executescript("""
        DROP TABLE IF EXISTS receipt;
        DROP TABLE IF EXISTS observation;
        DROP TABLE IF EXISTS raw;
        CREATE TABLE receipt(id TEXT, stream TEXT, minutes INTEGER, started INTEGER,
                             received INTEGER, available INTEGER, raw_sha TEXT);
        CREATE TABLE observation(stream TEXT, minutes INTEGER, opened INTEGER, values_json TEXT);
        CREATE TABLE raw(sha TEXT, body BLOB);
    """)
    document["raw_receipts"] = []
    for candle in document["candles"]:
        opened = pd.Timestamp(candle["opened_at"])
        values = candle["values"]
        row = [int(opened.timestamp()), *[str(values[k]) for k in ("open", "high", "low", "close")],
               "100.0", str(values["volume"]), 5]
        ongoing = [int(opened.timestamp()) + 60, "100.0", "100.0", "100.0", "100.0", "100.0", "1.0", 1]
        raw = json.dumps({"error": [], "result": {"XXBTZUSD": [row, ongoing], "last": int(opened.timestamp())}}).encode()
        digest = sha256(raw).hexdigest()
        candle["raw_sha256"] = digest
        document["raw_receipts"].append({"sha256": digest, "raw_utf8": raw.decode()})
        db.execute("INSERT INTO receipt VALUES (?,?,?,?,?,?,?)", (
            candle["receipt_id"], "stream", 1,
            *[pd.Timestamp(candle[key]).value for key in ("request_started_at", "received_at", "available_at")], digest))
        db.execute("INSERT INTO raw VALUES (?,?)", (digest, raw))
        db.execute("INSERT INTO observation VALUES (?,?,?,?)", ("stream", 1, opened.value, json.dumps(values)))
    db.commit()
    db.close()
    return save_document(manifest, document)


def save_document(manifest, document):
    from pathlib import Path

    raw = json.dumps(document).encode()
    Path(manifest["path"]).write_bytes(raw)
    manifest["sha256"] = sha256(raw).hexdigest()
    return manifest


def test_complete_native_bounds_support_no_crossing_without_claiming_stream_coverage(chart):
    manifest, context, document = chart
    save_chart(*chart)
    with open(context["source_database"], "rb") as stream:
        original = sha256(stream.read()).hexdigest()
    first = evidence.validate_evidence(manifest, context)
    assert first == evidence.validate_evidence(manifest, context)
    assert first["historical_coverage"] == "UNRESOLVED"
    assert first["exit_impact"] == "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED"
    assert first["checks"]["native_receipts_reconciled"]
    assert not first["checks"]["full_trade_stream_coverage_verified"]
    with open(context["source_database"], "rb") as stream:
        assert sha256(stream.read()).hexdigest() == original


@pytest.mark.parametrize("field,value", [("high", 110.), ("low", 90.)])
def test_threshold_touch_requires_possible_exit_impact(chart, field, value):
    manifest, context, document = chart
    document["candles"][1]["values"][field] = value
    save_chart(*chart)
    with pytest.raises(ValueError, match="touches or crosses"):
        evidence.validate_evidence(manifest, context)
    manifest["outcome"] = "EXIT_IMPACT_POSSIBLE"
    assert evidence.validate_evidence(manifest, context)["exit_impact"] == "EXIT_IMPACT_POSSIBLE"


def test_long_thresholds_use_the_long_liquidation_model(chart):
    manifest, context, document = chart
    context["position"].update(stop=90., target=110.)
    context["position"]["position"]["direction"] = 1
    document["model"].update(side=1, stop=90., target=110., liquidation_recomputed=67. / .984)
    save_chart(*chart)
    result = evidence.validate_evidence(manifest, context)
    assert result["checks"]["frozen_model"]["side"] == 1
    assert not result["checks"]["threshold_crossing_possible"]


@pytest.mark.parametrize("change", ["missing", "duplicate", "wrong_pair", "raw_hash", "incomplete", "source_store"])
def test_corrupt_incomplete_or_inapplicable_chart_proof_fails_closed(chart, change):
    manifest, context, document = chart
    if change == "missing":
        document["candles"].pop()
    elif change == "duplicate":
        document["candles"][1] = deepcopy(document["candles"][0])
    elif change == "incomplete":
        document["candles"][0]["request_started_at"] = BASE.isoformat()
    save_chart(*chart)
    if change == "wrong_pair":
        document["raw_receipts"][0]["raw_utf8"] = document["raw_receipts"][0]["raw_utf8"].replace("XXBTZUSD", "XXBTZEUR")
        document["raw_receipts"][0]["sha256"] = sha256(document["raw_receipts"][0]["raw_utf8"].encode()).hexdigest()
    elif change == "raw_hash":
        document["raw_receipts"][0]["raw_utf8"] += " "
    elif change == "source_store":
        with sqlite3.connect(context["source_database"]) as db:
            db.execute("UPDATE raw SET body=?", (b"different",))
    save_document(manifest, document)
    assert not evidence.review_evidence(manifest, context)["valid"]


@pytest.mark.parametrize("change", ["generation", "basis", "stop", "financing", "deadline", "future", "artifact_hash"])
def test_frozen_context_provenance_and_model_must_reconcile(chart, change):
    manifest, context, document = chart
    save_chart(*chart)
    if change == "generation":
        context["generation"] = "other"
    elif change == "basis":
        manifest["provenance"]["trigger_basis"] = "FUTURES_MARK"
    elif change == "stop":
        context["position"]["stop"] = 109.
    elif change == "financing":
        context["position"]["position"]["funding"] = -.1
    elif change == "deadline":
        context["position"]["deadline"] = context["interval_start"]
    elif change == "future":
        context["reviewed_at"] = BASE
    else:
        manifest["sha256"] = "0" * 64
    assert not evidence.review_evidence(manifest, context)["valid"]


def test_candles_cannot_prove_complete_trade_coverage(chart):
    manifest, context, _ = chart
    save_chart(*chart)
    manifest["outcome"] = "COVERAGE_VERIFIED"
    with pytest.raises(ValueError, match="cannot establish complete"):
        evidence.validate_evidence(manifest, context)


def test_time_deadline_inside_the_gap_prevents_no_exit_adjudication(chart):
    manifest, context, document = chart
    deadline = (BASE + pd.Timedelta(minutes=1)).isoformat()
    context["position"]["deadline"] = document["model"]["deadline"] = deadline
    save_chart(*chart)
    with pytest.raises(ValueError, match="touches or crosses"):
        evidence.validate_evidence(manifest, context)
    manifest["outcome"] = "EXIT_IMPACT_POSSIBLE"
    assert evidence.validate_evidence(manifest, context)["checks"]["time_exit_possible"]


def test_insufficient_evidence_is_explicit_and_preserves_uncertainty(chart):
    manifest, context, document = chart
    manifest["outcome"] = "INSUFFICIENT_EVIDENCE"
    document = {key: document[key] for key in ("generation", "book_id", "position_id", "recorded_at")}
    document["schema"] = "OPERATOR_EVIDENCE_NOTE_V1"
    document["reason"] = "First accepted recovery observation was not retained"
    save_document(manifest, document)
    result = evidence.validate_evidence(manifest, context)
    assert result["historical_coverage"] == result["exit_impact"] == "UNRESOLVED"
