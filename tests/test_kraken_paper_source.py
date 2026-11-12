from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import httpx
import pandas as pd
import pytest

import lab.kraken_paper as kraken_paper
from lab.kraken_paper import source
from lab.strategy_lab.market_data import ProvenanceClass, Purpose, SourceGrant, SourceIdentity


SEAM = pd.Timestamp("2026-12-01T00:00:00Z")
MINUTE = pd.Timedelta(minutes=1)
TRADE = source.TRADE


class Clock:
    def __init__(self, at):
        self.at = at

    def __call__(self):
        return self.at

    def pause(self, seconds):
        self.at += pd.Timedelta(seconds=seconds)


def _scope():
    identity = SourceIdentity("Kraken", "kraken-source-fixture", "BTC-USD", "SPOT", "fixture use only",
                              sha256(b"fixture").hexdigest(), ProvenanceClass.INDEPENDENT)
    approved = SEAM - pd.Timedelta(days=3)
    grants = []
    for book, minutes in (("day", 15), ("swing", 240), ("position", 1440)):
        step = pd.Timedelta(minutes=minutes)
        ranges = {
            Purpose.WARMUP: (SEAM - 720 * step, SEAM),
            Purpose.FORWARD_DECISION: (SEAM, SEAM + pd.Timedelta(days=100)),
            Purpose.EXECUTION_EVIDENCE: (SEAM, SEAM + pd.Timedelta(days=131)),
        }
        for purpose, (start, end) in ranges.items():
            grants.append(SourceGrant(identity, book, purpose, frozenset({TRADE}), start, end, approved, approved,
                                      "fixture grant"))
    return source.CollectionScope(tuple(grants), {"day": 15, "swing": 240, "position": 1440})


def _handler(clock, calls, *, failed=False, gap=None, correction=None, pair_tick="0.1"):
    def response(request):
        calls.append((clock.at, request))
        if failed:
            return httpx.Response(429, headers={"Retry-After": "180"})
        assert request.method == "GET"
        assert "Authorization" not in request.headers and "API-Key" not in request.headers
        assert request.url.params["pair"] == "XBTUSD"
        if request.url.path == "/0/public/AssetPairs":
            assert request.url.params["country_code"] == "CA"
            clock.pause(.1)
            return httpx.Response(200, json={"error": [], "result": {"XXBTZUSD": {
                "altname": "XBTUSD", "wsname": "XBT/USD", "base": "XXBT", "quote": "ZUSD", "status": "online",
                "tick_size": pair_tick, "lot_decimals": 8, "ordermin": "0.00006", "costmin": "0.5",
            }}})
        assert str(request.url).startswith("https://api.kraken.com/0/public/OHLC?")
        step = pd.Timedelta(minutes=int(request.url.params["interval"]))
        since = pd.Timestamp(int(request.url.params["since"]), unit="s", tz="UTC")
        cutoff = clock.at.floor(step)
        opens = pd.date_range(since, cutoff, freq=step)
        rows = []
        for opened in opens:
            if opened == gap:
                continue
            close = "102" if opened == correction else "101"
            rows.append([int(opened.timestamp()), "100", "103", "99", close, "100", "1", 5])
        clock.pause(.1)
        return httpx.Response(200, json={"error": [], "result": {"XXBTZUSD": rows, "last": rows[-1][0]}})
    return response


def _collect(scope, root, clock, calls, *, terms=None, **response):
    with httpx.Client(transport=httpx.MockTransport(_handler(clock, calls, **response))) as client:
        return source._collect(scope, root, client, clock, clock.pause, instrument_terms=terms)


def _grant(scope, book, purpose):
    return next(g for g in scope.grants if g.book_id == book and g.purpose == purpose)


def _read(scope, root, book, purpose, left, right, clock, step):
    grant = _grant(scope, book, purpose)
    return source.market_data(scope.grants, root=root).read(
        grant.id, book, purpose, TRADE, left, right, clock.at, step)


def test_warmup_uses_one_source_and_excludes_open_rows_across_three_books(tmp_path):
    scope = _scope()
    clock = Clock(SEAM + pd.Timedelta(seconds=5))
    calls = []
    result = _collect(scope, tmp_path, clock, calls)
    assert set(result) == {"15", "240", "1440"}
    assert [item["complete_rows"] for item in result.values()] == [720] * 3
    assert all(right[0] - left[0] >= source.MIN_REQUEST_SPACING for left, right in zip(calls, calls[1:]))
    assert len(calls) == 3 and not _collect(scope, tmp_path, clock, calls)
    for book, minutes in scope.chart_intervals.items():
        step = pd.Timedelta(minutes=minutes)
        warmup = _read(scope, tmp_path, book, Purpose.WARMUP, SEAM - 720 * step, SEAM, clock, step)
        assert warmup.complete and len(warmup.observations) == 720
        assert warmup.observations[-1].end == SEAM
        assert all(row.received_at > SEAM and row.available_at >= row.received_at
                   for row in warmup.observations)
        with pytest.raises(ValueError, match="as-of clock"):
            _read(scope, tmp_path, book, Purpose.FORWARD_DECISION, SEAM, SEAM + step, clock, step)
    assert len(source.status(root=tmp_path)["checkpoints"]) == 3


def test_prefetch_before_future_seam_only_collects_closed_feature_inputs(tmp_path):
    scope = _scope()
    clock = Clock(SEAM - pd.Timedelta(minutes=2))
    calls = []
    provisional = _collect(scope, tmp_path, clock, calls)
    assert set(provisional) == {"15", "240", "1440"}
    assert [item["complete_rows"] for item in provisional.values()] == [719] * 3
    clock.at = SEAM + pd.Timedelta(seconds=5)
    final = _collect(scope, tmp_path, clock, calls)
    assert [item["new_rows"] for item in final.values()] == [1] * 3
    for book, minutes in scope.chart_intervals.items():
        step = pd.Timedelta(minutes=minutes)
        warmup = _read(scope, tmp_path, book, Purpose.WARMUP, SEAM - 720 * step, SEAM, clock, step)
        assert warmup.complete and len(warmup.observations) == 720
    assert "1" not in provisional and "1" not in final


def test_forward_minute_receipts_share_data_without_crossing_grants_and_restart(tmp_path):
    scope = _scope()
    clock = Clock(SEAM + MINUTE + pd.Timedelta(seconds=5))
    calls = []
    result = _collect(scope, tmp_path, clock, calls)
    assert result["1"]["new_rows"] == 1
    receipts = [_read(scope, tmp_path, book, Purpose.EXECUTION_EVIDENCE, SEAM, SEAM + MINUTE,
                      clock, MINUTE) for book in scope.chart_intervals]
    assert len({snapshot.content_id for snapshot in receipts}) == 1
    assert len({snapshot.grant_id for snapshot in receipts}) == 3
    assert _collect(scope, tmp_path, clock, calls) == {}
    assert len(calls) == 4


def test_gap_and_later_correction_preserve_as_of_and_fail_closed(tmp_path):
    scope = _scope()
    clock = Clock(SEAM + MINUTE + pd.Timedelta(seconds=5))
    calls = []
    _collect(scope, tmp_path, clock, calls)
    original_clock = Clock(clock.at)
    clock.at = SEAM + 2 * MINUTE + pd.Timedelta(seconds=5)
    result = _collect(scope, tmp_path, clock, calls, gap=SEAM + MINUTE, correction=SEAM)
    assert result["1"]["state"] == "GAP" and result["1"]["missing_rows"] == 1
    original = _read(scope, tmp_path, "day", Purpose.EXECUTION_EVIDENCE, SEAM, SEAM + MINUTE,
                     original_clock, MINUTE)
    assert original.complete and original.observations[0].values["close"] == 101
    with pytest.raises(ValueError, match="Conflicting observations"):
        _read(scope, tmp_path, "day", Purpose.EXECUTION_EVIDENCE, SEAM, SEAM + 2 * MINUTE, clock, MINUTE)


def test_same_boundary_gap_can_recover_while_chart_receipt_is_timely(tmp_path):
    scope = _scope()
    clock = Clock(SEAM + pd.Timedelta(seconds=5))
    calls = []
    step = pd.Timedelta(minutes=15)
    first = _collect(scope, tmp_path, clock, calls, gap=SEAM - step)
    assert first["15"]["state"] == "GAP"
    clock.at = SEAM + pd.Timedelta(seconds=65)
    second = _collect(scope, tmp_path, clock, calls)
    assert second["15"]["new_rows"] == 1
    snapshot = _read(scope, tmp_path, "day", Purpose.WARMUP, SEAM - 720 * step, SEAM, clock, step)
    assert snapshot.complete and snapshot.observations[-1].received_at > SEAM + MINUTE


def test_persisted_public_terms_are_causal_fresh_and_frozen_for_collection(tmp_path):
    scope = _scope()
    clock = Clock(SEAM + pd.Timedelta(seconds=5))
    initial = clock.at
    calls = []
    terms = {"price_tick": .1, "quantity_step": 1e-8, "min_quantity": .00006, "min_quote": .5}
    results = _collect(scope, tmp_path, clock, calls, terms=terms)
    assert results["instrument"]["state"] == "READY"
    assert len(calls) == 4 and calls[0][1].url.path == "/0/public/AssetPairs"
    assert all(right[0] - left[0] >= source.MIN_REQUEST_SPACING for left, right in zip(calls, calls[1:]))
    assert source.instrument_evidence(scope.grants, as_of=initial, root=tmp_path)["state"] == "WAITING"
    current = source.instrument_evidence(scope.grants, as_of=clock.at, root=tmp_path)
    assert current == results["instrument"]
    assert source.instrument_evidence(scope.grants, as_of=clock.at + pd.Timedelta(days=1),
                                      root=tmp_path)["state"] == "STALE"
    clock.pause(23 * 3600 + 1)
    changed = _collect(scope, tmp_path, clock, calls, terms=terms, pair_tick="0.2")
    assert set(changed) == {"instrument"} and changed["instrument"]["state"] == "MISMATCH"
    assert changed["instrument"]["terms"]["price_tick"] == .2
    assert len(calls) == 5


def test_retry_after_is_persisted_and_no_live_access_without_verified_catalog(tmp_path, monkeypatch):
    scope = _scope()
    clock = Clock(SEAM + MINUTE + pd.Timedelta(seconds=5))
    calls = []
    backoff = _collect(scope, tmp_path, clock, calls, failed=True)
    assert backoff["1"]["reason"] == "HTTP 429"
    assert pd.Timestamp(backoff["1"]["retry_at"]) - clock.at == pd.Timedelta(seconds=180)
    clock.pause(61)
    assert _collect(scope, tmp_path, clock, calls)["1"]["state"] == "BACKOFF"
    assert len(calls) == 1
    clock.pause(120)
    assert _collect(scope, tmp_path, clock, calls)["1"]["state"] == "CAPTURED"
    assert source.status(root=tmp_path)["request_clocks"][0]["retry_at"] is None

    closed = []
    fake_db = SimpleNamespace(close=lambda: closed.append(True))
    catalog = SimpleNamespace(
        db_open=lambda **kwargs: fake_db,
        approved_authorization=lambda *args, **kwargs: None,
        registrations=lambda *args: {},
        grants=lambda *args: (),
    )
    monkeypatch.setattr(kraken_paper, "catalog", catalog, raising=False)

    def no_client(*args, **kwargs):
        pytest.fail("A live HTTP client must never be constructed without consent")
    monkeypatch.setattr(source.httpx, "Client", no_client)
    with pytest.raises(PermissionError, match="validated consent"):
        source.collect_registered(root=tmp_path / "not-created")
    assert closed == [True] and not (tmp_path / "not-created").exists()


def test_mismatched_or_unreviewed_scope_is_rejected():
    scope = _scope()
    with pytest.raises(PermissionError, match="separate warmup"):
        source.CollectionScope(scope.grants[:-1], scope.chart_intervals)
    other = replace(scope.source, provider="Coinbase Advanced Trade")
    with pytest.raises(PermissionError, match="approved BTC/USD Spot source"):
        source.CollectionScope(tuple(replace(g, source=other) for g in scope.grants), scope.chart_intervals)


def test_collection_stops_before_approval_and_when_last_grant_ends(tmp_path):
    scope = _scope()
    clock = Clock(min(g.approved_at for g in scope.grants) - MINUTE)
    terms = {"price_tick": .1, "quantity_step": 1e-8, "min_quantity": .00006, "min_quote": .5}
    calls = []
    assert _collect(scope, tmp_path, clock, calls, terms=terms) == {}
    clock.at = max(g.end for g in scope.grants)
    assert _collect(scope, tmp_path, clock, calls, terms=terms) == {}
    assert not calls and not (tmp_path / source.FILE).exists()
