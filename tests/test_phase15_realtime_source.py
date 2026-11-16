from contextlib import closing
from hashlib import sha256
import json
import sqlite3

import httpx
import pandas as pd
import pytest

from lab.phase9 import public
from lab.phase15 import realtime
from lab.storage import utc


ENDPOINT = "https://api-futures.kucoin.com/api/v1/mark-price/XBTUSDTM/current"
SOURCE_AT = utc("2026-11-16T06:16:00Z")
REQUESTED_AT = SOURCE_AT - pd.Timedelta(milliseconds=100)
RECEIVED_AT = SOURCE_AT + pd.Timedelta(milliseconds=800)
DATA = {"symbol": "XBTUSDTM", "granularity": 1000, "timePoint": SOURCE_AT.value // 10**6,
        "value": 64279.87, "indexPrice": 64282.3}


@pytest.fixture
def source_store(tmp_path, monkeypatch):
    monkeypatch.setattr(public, "utc_now", lambda: RECEIVED_AT)
    monkeypatch.setitem(realtime.SOURCE_POLICY, "min_free_bytes", 0)
    with closing(sqlite3.connect(tmp_path / "source.sqlite3")) as db:
        realtime.init_observations(db)
        yield db, tmp_path


def collect(store, handler, at=REQUESTED_AT):
    return realtime.observe(*store, clock=lambda: at, transport=httpx.MockTransport(handler))


def test_current_mark_route_preserves_raw_body_and_both_clocks(source_store):
    db, root = source_store
    raw = json.dumps({"code": "200000", "data": DATA}).encode()
    calls = []

    def get(request):
        calls.append((request.method, str(request.url)))
        assert "authorization" not in request.headers
        assert "kc-api-key" not in request.headers
        return httpx.Response(200, content=raw)

    observation = collect(source_store, get)
    assert calls == [("GET", ENDPOINT)]
    assert observation["status"] == "RECEIVED"
    assert observation["mark"] == DATA["value"]
    assert observation["source_at"] == SOURCE_AT.isoformat()
    assert observation["received_at"] == RECEIVED_AT.isoformat()
    assert observation["requested_at"] == REQUESTED_AT.isoformat()
    digest = sha256(raw).hexdigest()
    assert observation["raw_sha256"] == digest
    assert (root / "raw" / f"{digest}.json").read_bytes() == raw
    stored = db.execute("SELECT observation FROM mark_request WHERE id=?", (observation["request_id"],)).fetchone()[0]
    assert json.loads(stored) == observation


@pytest.mark.parametrize("data", [
    {**DATA, "symbol": "XBTUSDM"},
    {**DATA, "granularity": 60000},
    {**DATA, "timePoint": DATA["timePoint"] // 1000},
    {**DATA, "value": True},
    {**DATA, "value": "NaN"},
    {key: value for key, value in DATA.items() if key != "timePoint"},
])
def test_malformed_mark_cannot_supply_price(source_store, data):
    observation = collect(source_store, lambda request: httpx.Response(200, json={"code": "200000", "data": data}))
    assert observation["status"] == "INVALID_MARK_RESPONSE"
    assert "mark" not in observation
    assert "source_at" not in observation
    assert observation["received_at"] == RECEIVED_AT.isoformat()
    assert observation["raw_sha256"]


def test_retry_after_blocks_requests_until_provider_boundary(source_store, monkeypatch):
    db, _ = source_store
    first = collect(source_store, lambda request: httpx.Response(429, headers={"Retry-After": "90"},
                                                               json={"code": "429000"}))
    retry_at = RECEIVED_AT + pd.Timedelta(seconds=90)
    assert first["status"] == "RATE_LIMITED"
    assert first["resume_after"] == retry_at.isoformat()

    def unexpected(request):
        pytest.fail("A 429 response must suppress requests until Retry-After")

    blocked = collect(source_store, unexpected, retry_at - pd.Timedelta(milliseconds=1))
    assert blocked["status"] == "BACKOFF"
    assert db.execute("SELECT count(*) FROM mark_request").fetchone()[0] == 1

    monkeypatch.setattr(public, "utc_now", lambda: retry_at + pd.Timedelta(milliseconds=100))
    later = collect(source_store, lambda request: httpx.Response(200, json={"code": "200000", "data": DATA}), retry_at)
    assert later["status"] == "RECEIVED"
    assert later["request_id"] == 2


def test_access_denial_is_durable_and_sticky(source_store):
    _, root = source_store
    first = collect(source_store, lambda request: httpx.Response(403, content=b"denied"))
    assert first["status"] == "ACCESS_RESTRICTED"

    def unexpected(request):
        pytest.fail("A stored access denial must suppress future requests")

    with closing(sqlite3.connect(root / "source.sqlite3")) as reopened:
        blocked = collect((reopened, root), unexpected, REQUESTED_AT + pd.Timedelta(days=1))
        assert blocked["status"] == "ACCESS_RESTRICTED"
        assert reopened.execute("SELECT count(*) FROM mark_request").fetchone()[0] == 1


def test_request_spacing_hourly_and_lifetime_limits(source_store, monkeypatch):
    db, _ = source_store
    monkeypatch.setitem(realtime.SOURCE_POLICY, "min_request_spacing_seconds", 15)
    monkeypatch.setitem(realtime.SOURCE_POLICY, "requests_per_hour", 1)
    monkeypatch.setitem(realtime.SOURCE_POLICY, "total_requests", 2)

    def respond(request):
        return httpx.Response(200, json={"code": "200000", "data": DATA})

    def unexpected(request):
        pytest.fail("A request guard must run before opening the HTTP request")

    assert collect(source_store, respond)["request_id"] == 1
    assert collect(source_store, unexpected, REQUESTED_AT + pd.Timedelta(seconds=14))["status"] == "REQUEST_SPACING"
    assert collect(source_store, unexpected, REQUESTED_AT + pd.Timedelta(seconds=16))["status"] == "REQUEST_BUDGET"
    assert collect(source_store, respond, REQUESTED_AT + pd.Timedelta(minutes=61))["request_id"] == 2
    assert collect(source_store, unexpected, REQUESTED_AT + pd.Timedelta(hours=3))["status"] == "REQUEST_BUDGET"
    assert db.execute("SELECT count(*) FROM mark_request").fetchone()[0] == 2


def test_interrupted_requests_still_use_the_ideas_budget(source_store):
    db, root = source_store

    def interrupt(request):
        raise KeyboardInterrupt

    for offset in (0, 20):
        with pytest.raises(KeyboardInterrupt):
            realtime.observe(db, root, idea_ids=("idea",), clock=lambda: REQUESTED_AT + pd.Timedelta(seconds=offset),
                             transport=httpx.MockTransport(interrupt))
    with closing(sqlite3.connect(root / "source.sqlite3")) as reopened:
        assert not realtime.request_capacity(reopened, "idea")
        assert realtime.request_capacity(reopened, "other")
        blocked = realtime.observe(reopened, root, idea_ids=("idea",), clock=lambda: REQUESTED_AT + pd.Timedelta(minutes=1),
                                   transport=httpx.MockTransport(interrupt))
        assert blocked["status"] == "REQUEST_BUDGET"


def test_redirect_cannot_reach_another_route(source_store):
    def redirect(request):
        assert str(request.url) == ENDPOINT
        return httpx.Response(302, headers={"Location": "https://example.test/different-mark"})

    observation = collect(source_store, redirect)
    assert observation["status"] == "HTTP_ERROR"
    assert observation["http_status"] == 302


def test_oversized_response_cannot_supply_mark_or_raw_evidence(source_store):
    _, root = source_store
    oversized = b"x" * (realtime.SOURCE_POLICY["max_response_bytes"] + 1)
    observation = collect(source_store, lambda request: httpx.Response(200, content=oversized))
    assert observation["status"] == "RESPONSE_LIMIT"
    assert "mark" not in observation
    assert "raw_sha256" not in observation
    assert not list((root / "raw").glob("*.json"))
