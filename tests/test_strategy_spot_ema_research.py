from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace
import json

import pandas as pd
import pytest

from lab.learning.simulator import Contract, Cost, Risk, size_order
from lab.strategy_lab import ema_pullback
from lab.strategy_lab.feature_warmup import (
    DecisionFeatures, WarmupPolicy, decision_features, ema_indicators, freeze_seed, record_eligibility,
)
from lab.strategy_lab.market_data import MarketData, ObservationKind, ProvenanceClass, Purpose, SourceGrant, SourceIdentity
from lab.strategy_lab.registry import digest
from lab.strategy_lab.spot_ema_research import proposals, signal_diagnostic
from lab.strategy_lab.spot_sources import SpotCandleProvider, SpotCandleReceipt, SpotCandleRequest, Venue
from lab.strategy_lab.technical_features import FeatureIdentity, IncrementalFeatures, build_candles


@pytest.mark.parametrize(("venue", "display"), (("kraken", "Kraken"), ("coinbase", "Coinbase Advanced Trade")))
def test_review_drafts_are_stable_separate_and_inactive(venue, display):
    drafts = proposals(venue)
    assert drafts == proposals(venue)
    assert set(drafts) == {"day", "swing", "position"}
    assert len({record["draft_id"] for record in drafts.values()}) == 3
    assert {record["draft_id"] for record in drafts.values()}.isdisjoint(
        {record["draft_id"] for record in proposals("coinbase" if venue == "kraken" else "kraken").values()})
    for profile, record in drafts.items():
        body = record["body"]
        assert record["draft_id"] == digest(body)
        assert body["status"] == "BLOCKED_PERMISSION"
        assert body["registration_id"] is body["book_id"] is body["approval"] is None
        assert body["activation_enabled"] is False
        assert body["venue"] == display and body["instrument"] == "BTC-USD"
        assert body["market"] == "SPOT" and body["quote"] == "USD"
        assert body["executable_sides"] == ["long"] and body["short_handling"] == "SIGNAL_ONLY"
        assert body["capital"]["hypothetical_endowment"] == 10_000
        assert body["capital"]["endowment_currency"] == "USDT"
        assert body["capital"]["initial_usd"] is None
        assert body["risk"]["leverage"] == 1 and body["execution"]["funding"] == "NOT_APPLICABLE"
        assert body["recipe"]["entry_parameters"] == ema_pullback.ENTRY_PARAMETERS
        assert body["recipe"]["exit_parameters"] == ema_pullback.EXIT_PARAMETERS
        assert body["execution"]["feature_native_seconds"] == body["execution"]["decision_seconds"]
        assert body["research"]["warmup_bars"] == 200
        assert body["research"]["primary_days"] == 90
        assert body["research"]["min_complete_trades"] == 25
        assert body["research"]["first_eligible_at"] is body["research"]["source_id"] is None
        assert body["session"]["cadence"] == "24/7"
        assert body["session"]["max_hold_minutes"] == {"day": 480, "swing": 10080, "position": 43200}[profile]


def _features(current, previous):
    close = pd.Timestamp("2027-02-01T04:00:00Z")
    return DecisionFeatures(close, close + pd.Timedelta(seconds=10), "fixture-hash",
                            tuple(current.items()), tuple(previous.items()))


def test_short_diagnostic_has_no_setup_and_spot_sizing_cannot_open_short():
    features = _features(
        {"close": 90, "ema_fast": 95, "ema_slow": 102, "volume": 200, "volume_sma": 100, "rsi": 45},
        {"ema_fast": 96, "low": 90, "high": 101, "close": 97},
    )
    assert signal_diagnostic(features) == {"side": "SHORT", "state": "SIGNAL_ONLY",
                                           "decision_at": features.decision_at.isoformat(),
                                           "input_snapshot_id": "fixture-hash"}
    spot = Contract("spot", "USD", 1, .0001, .1, .0001, 10_000, 1, 0, 0)
    risk = Risk(10_000, .0025, 1, 1, 1, 0)
    cost = Cost(.008, .0005)
    assert size_order(SimpleNamespace(direction=-1), spot, cost, risk, 10_000, {}) == (None, "spot_long_cash_only")
    long = SimpleNamespace(direction=1, reference=100, stop=95, atr=3)
    sized, reason = size_order(long, spot, cost, risk, 10_000, {"entry_limit_atr": .25})
    assert reason is None and sized.projected_liquidation is None
    assert size_order(long, spot, cost, replace(risk, leverage=2), 10_000, {})[0] is None


def test_long_signal_is_a_candidate_and_absent_signal_creates_no_action():
    features = _features(
        {"close": 110, "ema_fast": 105, "ema_slow": 100, "volume": 200, "volume_sma": 100, "rsi": 55},
        {"ema_fast": 104, "low": 102, "high": 110, "close": 106},
    )
    assert signal_diagnostic(features)["state"] == "LONG_CANDIDATE"
    changed = tuple((key, 90 if key == "volume" else value) for key, value in features.current)
    assert signal_diagnostic(replace(features, current=changed))["state"] == "NO_SIGNAL"
    with pytest.raises(TypeError, match="eligible forward"):
        signal_diagnostic(dict(features.current))


def _receipt(venue, chart, start, end, *, offset=0):
    width = int(chart.total_seconds() / 60)
    opens = pd.date_range(start, end, freq=chart, inclusive="left")
    values = [str(100 + (offset + i) / 10) for i in range(len(opens))]
    if venue == Venue.KRAKEN:
        rows = [[t.value // 10**9, p, str(float(p) + 3), str(float(p) - 2),
                 str(float(p) + 1), p, "2", 8] for t, p in zip(opens, values)]
        rows.append([end.value // 10**9, "100", "110", "95", "107", "100", "1", 8])
        payload = {"error": [], "result": {"XXBTZUSD": rows, "last": end.value // 10**9}}
    else:
        payload = {"candles": [{"start": str(t.value // 10**9), "open": p, "high": str(float(p) + 3),
                                "low": str(float(p) - 2), "close": str(float(p) + 1), "volume": "2"}
                               for t, p in zip(reversed(opens), reversed(values))]}
    request = SpotCandleRequest(venue, width, start, end)
    return SpotCandleReceipt(request, end + pd.Timedelta(seconds=2), end + pd.Timedelta(seconds=3),
                             end + pd.Timedelta(seconds=5), 200, json.dumps(payload).encode())


@pytest.mark.parametrize("venue", list(Venue))
@pytest.mark.parametrize(("profile", "interval"), (("day", "15min"), ("swing", "4h"), ("position", "1D")))
def test_spot_receipts_feed_shared_warmup_and_incremental_indicators(tmp_path, venue, profile, interval):
    trade = ObservationKind.TRADE_OHLCV
    seam, step = pd.Timestamp("2027-02-01T00:00:00Z"), pd.Timedelta(interval)
    start, registered = seam - 220 * step, seam - pd.Timedelta(minutes=5)
    close = seam + 2 * step
    source = SourceIdentity(venue.value, "synthetic-spot", "BTC-USD", "SPOT", "fixture only",
                            sha256(b"fixture only").hexdigest(), ProvenanceClass.INDEPENDENT)
    seed_grant = SourceGrant(source, profile, Purpose.WARMUP, frozenset({trade}), start, seam,
                             registered, registered, "fixture")
    forward = SourceGrant(source, profile, Purpose.FORWARD_DECISION, frozenset({trade}), seam,
                          seam + pd.Timedelta(days=93), registered, registered, "fixture")
    other = replace(seed_grant, book_id="second-strategy")
    receipts = [_receipt(venue, step, start, seam),
                _receipt(venue, step, seam, seam + step, offset=220),
                _receipt(venue, step, seam + step, close, offset=221)]
    provider = SpotCandleProvider(source, receipts)
    data = MarketData([seed_grant, forward, other], {source.id: provider})
    seed_clock = seam + pd.Timedelta(seconds=5)
    archive = data.read(seed_grant.id, profile, Purpose.WARMUP, trade, start, seam, seed_clock, step)
    second = data.read(other.id, "second-strategy", Purpose.WARMUP, trade, start, seam, seed_clock, step)
    assert archive.content_id == second.content_id and archive.id != second.id
    policy = WarmupPolicy(f"{profile}-fixture", profile, registered, seed_grant.id, forward.id,
                          archive.content_id, start, seam, step, step, step, ema_indicators(), 200)
    seed = freeze_seed(data, policy, as_of=seed_clock)
    assert seed.bars == 220 and seed.source_available_at == seed_clock
    assert seed.last_provider_receipt_at == receipts[0].received_at
    bridge_clock = seam + step + pd.Timedelta(seconds=35)
    eligibility = record_eligibility(tmp_path, data, policy, seed, as_of=bridge_clock)
    assert eligibility.first_eligible_at == close
    assert record_eligibility(tmp_path, data, policy, seed, as_of=close) == eligibility
    with pytest.raises(ValueError, match="outside"):
        decision_features(data, policy, seed, eligibility, decision_at=seam + step, as_of=bridge_clock)
    now = close + pd.Timedelta(seconds=35)
    actual = decision_features(data, policy, seed, eligibility, decision_at=close, as_of=now)
    assert actual.evidence_available_at == receipts[-1].available_at
    new = data.read(forward.id, profile, Purpose.FORWARD_DECISION, trade, seam, close, now, step)
    cursor = IncrementalFeatures(FeatureIdentity(source.id, archive.content_id, "BTC-USD", "SPOT", step),
                                 dict(ema_indicators()))
    for snapshot in (archive, new):
        bars = build_candles(snapshot.frame(), timeframe=step, native_step=step, as_of=snapshot.as_of,
                             start=snapshot.start, end=snapshot.end).bars
        values = cursor.update(bars, as_of=snapshot.as_of, expected_tip=cursor.tip)
    assert dict(values.iloc[-1]) == pytest.approx(
        {name: dict(actual.current)[name] for name, _ in ema_indicators()}, rel=1e-13, abs=1e-12)
