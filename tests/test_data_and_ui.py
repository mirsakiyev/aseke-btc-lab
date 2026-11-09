from dataclasses import asdict

import httpx
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from lab import data, research
from lab.config import ROOT, load_profiles
from lab.execution import Setup
from lab.storage import file_hash, utc, writer_lock, write_json


def candle(timestamp, close=100):
    return [int(utc(timestamp).timestamp()), 98, 102, 100, close, 8]


def test_request_bounds_duplicate_ingestion_and_snapshot_reuse(tmp_path, monkeypatch):
    monkeypatch.setattr(data.time, "sleep", lambda _: None)
    requested = []

    def endpoint(request):
        requested.append(request)
        return httpx.Response(200, json=[
            candle("2025-01-01T00:15Z"), candle("2025-01-01T00:10Z"),
            candle("2025-01-01T00:00Z"), candle("2025-01-01T00:00Z"),
            candle("2024-12-31T23:55Z"),
        ])

    with httpx.Client(transport=httpx.MockTransport(endpoint)) as client:
        args = (300, "2025-01-01", "2025-01-01T00:15Z", 1, tmp_path, client)
        first, retry = data.backfill(*args), data.backfill(*args)
    assert first["downloaded"] == len(requested) == 1 and retry["downloaded"] == 0
    assert first["snapshot"]["sha256"] == retry["snapshot"]["sha256"]
    assert first["snapshot"]["duplicates_within_responses"] == 1
    frame, record = data.snapshot(300, tmp_path)
    assert len(frame) == 2
    assert frame.iloc[0].volume == 8 and frame.iloc[0].open == 100
    assert frame.iloc[0].source_id == data.SOURCE
    coverage = data.coverage(frame, 300, utc("2025-01-01"), utc("2025-01-01T00:15Z"))
    assert coverage["internal_gap_bars"] == coverage["missing_target_bars"] == 1
    assert record["requests"] == 1
    assert all((right - left) / pd.Timedelta(minutes=5) < 300 for left, right in data.requests(
        utc("2025-01-01"), utc("2025-01-03"), 300,
    ))


def test_conflicting_prices_and_duplicate_writer_are_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(data.time, "sleep", lambda _: None)
    price = [100]

    def endpoint(request):
        return httpx.Response(200, json=[candle("2025-01-01", price[0])])

    with httpx.Client(transport=httpx.MockTransport(endpoint)) as client:
        with writer_lock(tmp_path / ".writer.lock"):
            with pytest.raises(RuntimeError, match="Another writer"):
                data.backfill(300, "2025-01-01", "2025-01-01T00:05Z", 1, tmp_path, client)
        first = data.backfill(300, "2025-01-01", "2025-01-01T00:05Z", 1, tmp_path, client)
        price[0] = 101
        with pytest.raises(ValueError, match="Conflicting candles across requests"):
            data.backfill(300, "2025-01-01", "2025-01-01T00:10Z", 1, tmp_path, client)
    assert data.snapshot(300, tmp_path)[1]["sha256"] == first["snapshot"]["sha256"]
    assert (data.feed(300, tmp_path) / "conflict.json").exists()


def test_empty_response_and_later_prices_can_share_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(data.time, "sleep", lambda _: None)
    responses = iter([[], [candle("2025-01-02")]])
    with httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=next(responses))
    )) as client:
        empty = data.backfill(300, "2025-01-01", "2025-01-01T00:05Z", 1, tmp_path, client)
        assert empty["snapshot"]["rows"] == 0
        populated = data.backfill(300, "2025-01-02", "2025-01-02T00:05Z", 1, tmp_path, client)
    assert populated["snapshot"]["rows"] == 1
    assert data.snapshot(300, tmp_path)[0].iloc[0].source_id == data.SOURCE


def test_run_idempotence_for_missing_data(tmp_path, monkeypatch):
    profile = load_profiles()["day"]
    calls = []

    def no_data(seconds, root):
        calls.append(seconds)
        return pd.DataFrame(), {}

    monkeypatch.setattr(research, "snapshot", no_data)
    first = research.run(profile, True, tmp_path / "data", tmp_path / "runs")
    stamp = file_hash(first / "manifest.json")
    assert research.run(profile, True, tmp_path / "data", tmp_path / "runs") == first
    assert file_hash(first / "manifest.json") == stamp
    assert len(list(first.parent.glob("*/manifest.json"))) == 1
    assert calls == [300, 300]


def test_dashboard_mode_switch_preserves_originating_position(tmp_path, monkeypatch):
    profiles = load_profiles()
    profile = profiles["position_spot"]
    at = utc("2025-05-01T01:00Z")
    position = Setup(at, at, at, 100, 95, at + pd.Timedelta("30D"), entry_raw=100,
                     reason="window_boundary", observed_through=at + pd.Timedelta("1h"))
    position_file = tmp_path / "existing-position.json"
    write_json(position_file, {"profile": profile.id, "strategy_version": profile.strategy_version,
                               "exit_policy": asdict(position)})
    starting_hash = file_hash(position_file)
    manifest = {"readiness": "BASELINE_ONLY", "economic_evidence": "NO_EDGE_DEMONSTRATED",
                "run_id": "fixture", "model": None, "profile_hash": profile.identity,
                "source_snapshot": {}, "periods": {"evaluation": {"open_position": asdict(position)}}}
    monkeypatch.setattr(data, "snapshot", lambda _: (pd.DataFrame(), {}))
    monkeypatch.setattr(research, "latest_run",
                        lambda key: (tmp_path, manifest) if key == profile.id else None)
    app = AppTest.from_file(str(ROOT / "dashboard.py")).run()
    assert not app.exception and app.header[0].value == "Spot / Position"
    app.radio(key="profile").set_value("day").run()
    assert not app.exception and app.header[0].value == "Day"
    app.radio(key="profile").set_value("position_spot").run()
    assert not app.exception and app.header[0].value == "Spot / Position"
    assert any(button.disabled for button in app.button)
    assert file_hash(position_file) == starting_hash
