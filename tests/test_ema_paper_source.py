from hashlib import sha256
import json
import sqlite3

import pandas as pd
import pytest

from lab.ema_paper import source
from lab.phase9.public import funding, kline, request


MIN = pd.Timedelta(minutes=1)
AT = pd.Timestamp("2026-11-11T00:00Z")


def lane(profile="day"):
    spot = profile == "position"
    return {"profile": profile, "book_id": "a" * 64 if spot else "b" * 64,
            "market": "SPOT" if spot else "FUTURES", "instrument": "BTC-USDT" if spot else "XBTUSDTM",
            "spec": {"timeframe": "1D" if spot else "15min"}, "registered_at": AT.isoformat(),
            "source": {"decision_receipt_seconds": 300}}


def descriptor(book):
    return {"scope": source.SCOPE, "book_id": book["book_id"], "venue": "KuCoin",
            "market": book["market"], "instrument": book["instrument"], "operator": "Fixture",
            "rights_reference": "Synthetic proof-only test", "producer": "fixture",
            "independent_forward_receipts": True, "protected_evidence_excluded": True}


def put(book, grant, req, data, received, root, *, observed=None):
    raw = json.dumps({"code": "200000", "data": data}, separators=(",", ":"))
    return source.ingest(book, {"source_id": grant["source_id"], "producer": "fixture",
                        "request": req, "http_status": 200, "raw_response": raw,
                        "raw_sha256": sha256(raw.encode()).hexdigest(), "received_at": received.isoformat()},
                        observed or received, root=root)


def price(book, grant, begin, end, received, root, *, basis="TRADE", step="1min", observed=None, price=100):
    req = kline("fixture", book["instrument"], book["market"], basis, step, begin, end)
    rows = [[str(int(at.timestamp())), str(price), str(price+1), str(price-1), str(price)]
            + (["2", "200"] if basis == "TRADE" else [])
            for at in pd.date_range(begin, end, freq="1h" if step == "1hour" else "1min", inclusive="left")]
    return put(book, grant, req, {"tradeType": book["market"], "symbol": book["instrument"], "list": rows},
               received, root, observed=observed)


def metadata(book, grant, observed, root, anchor):
    if book["market"] == "SPOT":
        record = {"symbol": book["instrument"], "baseCurrency": "BTC", "quoteCurrency": "USDT",
                  "feeCategory": "classA", "tradingStatus": "TradingEnabled", "baseOrderStep": ".00001",
                  "tickSize": ".1", "minBaseOrderSize": ".00001", "maxQuoteOrderSize": "10000000",
                  "minFunds": "1", "maxBaseOrderSize": "100"}
    else:
        record = {"symbol": book["instrument"], "minOrderSize": 1}
    put(book, grant, request("fixture", "instrument", tradeType=book["market"], symbol=book["instrument"]),
        {"tradeType": book["market"], "list": [record]}, observed, root)
    if book["market"] == "FUTURES":
        put(book, grant, request("fixture", "contract"), {
            "symbol": book["instrument"], "baseCurrency": "XBT", "quoteCurrency": "USDT", "settleCurrency": "USDT",
            "isInverse": False, "isQuanto": False, "expireDate": None, "status": "Open",
            "multiplier": ".001", "lotSize": 1, "tickSize": ".1", "maxLeverage": 100,
            "marketMaxOrderQty": 100000, "currentFundingRateGranularity": 28800000,
            "nextFundingRateDateTime": int(anchor.timestamp()*1000)}, observed, root)
        put(book, grant, request("fixture", "isolated_tiers"), [{"symbol": book["instrument"], "level": 1,
            "maxRiskLimit": 250000, "maxLeverage": 100, "maintainMargin": .005}], observed, root)


def test_source_reads_are_fail_closed_and_grants_are_frozen(tmp_path):
    book = lane()
    root = tmp_path / "scope"
    at = AT + 15*MIN
    assert source.decision(book, at, at, root=root)["state"] == "BLOCKED_PERMISSION"
    assert source.decision(book, AT, AT, root=root)["state"] == "BLOCKED_PERMISSION"
    assert source.execution(book, at, at+MIN, root=root)["state"] == "BLOCKED_PERMISSION"
    assert not root.exists()
    with pytest.raises(ValueError, match="Explicit independent"):
        source.authorize(book, {**descriptor(book), "protected_evidence_excluded": False}, AT, root=root)
    grant = source.authorize(book, descriptor(book), AT, root=root)
    assert source.authorize(book, descriptor(book), at, root=root) == grant
    with pytest.raises(ValueError, match="immutable"):
        source.authorize(book, {**descriptor(book), "producer": "other"}, at, root=root)
    with source.database(root, write=True) as db, pytest.raises(sqlite3.IntegrityError, match="immutable"):
        db.execute("DELETE FROM grant_scope")
    with pytest.raises(ValueError, match="crosses"):
        price(book, grant, AT-MIN, AT, at, root)


def test_day_warmup_counts_contiguous_causal_trade_and_requires_mark(tmp_path, monkeypatch):
    monkeypatch.setattr(source, "WARMUP", 2)
    book = lane()
    root = tmp_path / "source"
    grant = source.authorize(book, descriptor(book), AT, root=root)
    metadata(book, grant, AT, root, AT + pd.Timedelta(hours=8))
    at = AT + 30*MIN
    first = price(book, grant, AT, AT+15*MIN, AT+16*MIN, root)
    assert first["new_observations"] == 15
    assert price(book, grant, AT, AT+15*MIN, AT+16*MIN, root)["state"] == "ALREADY_RECORDED"
    price(book, grant, AT+15*MIN, at, at+MIN, root)
    blocked = source.decision(book, at, at+MIN, root=root)
    assert blocked["state"] == "BLOCKED_EXECUTION_EVIDENCE"
    assert blocked["warmup_bars"] == 2
    price(book, grant, AT+15*MIN, at, at+MIN, root, basis="MARK_PRICE")
    ready = source.decision(book, at, at+MIN, root=root)
    assert ready["state"] == "READY" and ready["warmup_bars"] == 2
    assert ready["frame"].volume.iloc[-1] == .002 and ready["contract"].base_per_contract == .001
    assert ready["evidence"]["latest_provider_received_at"] == ready["evidence"]["latest_handoff_at"] == (at+MIN).isoformat()

    # Earlier event time cannot silently promote a bar handed to the study too late.
    later = at + 15*MIN
    price(book, grant, at, later, later+MIN, root, observed=later+6*MIN)
    late = source.decision(book, later, later+8*MIN, root=root)
    assert late["state"] == "WAITING_FOR_DATA" and late["warmup_bars"] == 0


def test_futures_requires_actual_settlements_and_preserves_first_receipts(tmp_path):
    book = lane()
    grant = source.authorize(book, descriptor(book), AT, root=tmp_path)
    at = AT + pd.Timedelta(hours=8)
    metadata(book, grant, at-30*MIN, tmp_path, at)
    price(book, grant, at, at+MIN, at+2*MIN, tmp_path)
    assert source.execution(book, at, at+2*MIN, root=tmp_path)["reason"].endswith("mark minute")
    price(book, grant, at, at+MIN, at+2*MIN, tmp_path, basis="MARK_PRICE", price=101)
    waiting = source.execution(book, at, at+2*MIN, root=tmp_path)
    assert waiting["state"] == "BLOCKED_EXECUTION_EVIDENCE" and "settlement" in waiting["reason"]
    assert waiting["waiting_for_funding"] is True
    put(book, grant, funding("settled", at, at+MIN), {"symbol": book["instrument"],
        "list": [{"ts": int(at.timestamp()*1000), "fundingRate": "0.0001"}]}, at+2*MIN, tmp_path)
    ready = source.execution(book, at, at+2*MIN, root=tmp_path)
    assert ready["state"] == "READY"
    assert (ready["trade"]["open"], ready["mark"]["open"], ready["funding"][0]["rate"]) == (100, 101, .0001)
    price(book, grant, at, at+MIN, at+3*MIN, tmp_path, price=102)
    conflict = source.execution(book, at, at+3*MIN, root=tmp_path)
    assert conflict["state"] == "BLOCKED_EXECUTION_EVIDENCE" and conflict["permanent"] is True


def test_spot_only_uses_real_spot_trade_minutes_and_causal_contract(tmp_path):
    book = lane("position")
    grant = source.authorize(book, descriptor(book), AT, root=tmp_path)
    at = AT + 60*MIN
    price(book, grant, at, at+MIN, at+2*MIN, tmp_path)
    metadata(book, grant, at+MIN, tmp_path, at)
    blocked = source.execution(book, at, at+2*MIN, root=tmp_path)
    assert blocked["state"] == "BLOCKED_EXECUTION_EVIDENCE" and "dated public instrument" in blocked["reason"]
    later = at + 10*MIN
    price(book, grant, later, later+MIN, later+2*MIN, tmp_path)
    ready = source.execution(book, later, later+2*MIN, root=tmp_path)
    assert ready["state"] == "READY" and ready["trade"] == ready["mark"] and ready["funding"] == []
    assert ready["contract"].family == "spot" and ready["evidence"]["minimum_notional"] == 1.
