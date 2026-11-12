from dataclasses import replace
from hashlib import sha256

import pandas as pd
import pytest

from lab.strategy_lab.feature_warmup import (
    WarmupPolicy, decision_features, ema_indicators, freeze_seed, load_checkpoint, qualify_bridge,
    record_eligibility,
)
from lab.strategy_lab.market_data import (
    MarketData, Observation, ObservationKind, ProvenanceClass, Purpose, SourceGrant, SourceIdentity,
)
from lab.strategy_lab.technical_features import FeatureCache


TRADE, MARK = ObservationKind.TRADE_OHLCV, ObservationKind.MARK_OHLCV
SEAM = pd.Timestamp("2027-01-01T00:00:00Z")
RAW = b"synthetic receipt, no production source"
SHA = sha256(RAW).hexdigest()


class FixtureProvider:
    def __init__(self, rows):
        self.rows = rows

    def read(self, kind, start, end, as_of, interval):
        return (row for row in self.rows if row.kind == kind and start <= row.start < end)

    def raw(self, digest):
        assert digest == SHA
        return RAW


def _experiment(book="day", *, chart="15min", native="1min", spot=False, count=200):
    chart, native = pd.Timedelta(chart), pd.Timedelta(native)
    seed_native = native if spot else chart
    start = SEAM - count * chart
    registered = SEAM - pd.Timedelta(minutes=3)
    market, instrument = ("SPOT", "BTC-USDT") if spot else ("FUTURES", "XBTUSDTM")
    historical, live = (
        SourceIdentity("Synthetic venue", namespace, instrument, market, "fixture rights", SHA,
                       ProvenanceClass.INDEPENDENT)
        for namespace in ("archived", "forward")
    )
    kinds = frozenset({TRADE} if spot else {TRADE, MARK})
    warm_grant = SourceGrant(historical, book, Purpose.WARMUP, frozenset({TRADE}),
                            start, SEAM, registered, registered, "synthetic feature approval")
    next_grant = SourceGrant(live, book, Purpose.FORWARD_DECISION, kinds,
                            SEAM, SEAM + pd.Timedelta(days=95), registered, registered, "synthetic forward approval")

    def rows(source, kind, left, right, step, archived=False):
        for i, opening in enumerate(pd.date_range(left, right, freq=step, inclusive="left")):
            close = opening + step
            received = SEAM + pd.Timedelta(seconds=10) if archived else close + pd.Timedelta(seconds=10)
            price = 100. + i / 100
            value = {"open": price, "high": price + 2, "low": price - 1, "close": price + 1}
            if kind == TRADE:
                value["volume"] = 1 + i / 1000
            yield Observation(source.id, kind, opening, close, received, received, SHA, value)

    archive = FixtureProvider(list(rows(historical, TRADE, start, SEAM, seed_native, True)))
    forward = FixtureProvider([row for kind in kinds
                               for row in rows(live, kind, SEAM, SEAM + 3 * chart, native)])
    grants = [warm_grant, next_grant]
    providers = {historical.id: archive, live.id: forward}
    data = MarketData(grants, providers)
    snapshot = data.read(warm_grant.id, book, Purpose.WARMUP, TRADE, start, SEAM,
                         SEAM + pd.Timedelta(seconds=11), seed_native)
    policy = WarmupPolicy(
        registration_id=f"{book}-proposal", book_id=book, registered_at=registered,
        seed_grant_id=warm_grant.id, forward_grant_id=next_grant.id, seed_content_id=snapshot.content_id,
        seed_start=start, seed_end=SEAM, seed_native_step=seed_native, timeframe=chart, native_step=native,
        indicators=ema_indicators(), minimum_bars=200, require_mark=not spot,
        native_grace=pd.Timedelta(minutes=62 if spot else 17),
    )
    return data, policy, archive, forward, grants, providers


@pytest.mark.parametrize(("book", "chart", "native", "spot"), (
    ("day", "15min", "1min", False), ("swing", "4h", "1min", False), ("position", "1D", "1h", True),
))
def test_ema_feature_seed_then_strictly_future_decision(book, chart, native, spot, tmp_path):
    data, policy, _, _, _, _ = _experiment(book, chart=chart, native=native, spot=spot)
    cache = FeatureCache()
    seed = freeze_seed(data, policy, as_of=SEAM + pd.Timedelta(seconds=11), cache=cache)
    assert seed.bars == 200
    assert seed.end == SEAM and seed.source_available_at == SEAM + pd.Timedelta(seconds=10)
    assert set(dict(seed.values)) == set(dict(ema_indicators()))
    ready = SEAM + policy.timeframe + pd.Timedelta(seconds=40)
    fixed = record_eligibility(tmp_path, data, policy, seed, as_of=ready, cache=cache)
    first = SEAM + 2 * policy.timeframe
    assert fixed.first_eligible_at == first
    assert fixed.primary_end_exclusive == first + pd.Timedelta(days=90)
    assert len(fixed.bridge_snapshot_ids) == (1 if spot else 2)
    assert load_checkpoint(tmp_path, seed.id) == seed
    assert load_checkpoint(tmp_path, fixed.id) == fixed
    assert record_eligibility(tmp_path, data, policy, seed, as_of=first) == fixed
    assert len(list(tmp_path.glob("eligibility-*.txt"))) == 1

    for too_early in (seed.end, seed.end + policy.timeframe):
        with pytest.raises(ValueError, match="outside"):
            decision_features(data, policy, seed, fixed, decision_at=too_early,
                              as_of=too_early + pd.Timedelta(seconds=40))
    observed = first + pd.Timedelta(seconds=40)
    values = decision_features(data, policy, seed, fixed, decision_at=first, as_of=observed, cache=cache)
    assert values == decision_features(data, policy, seed, fixed, decision_at=first, as_of=observed)
    assert dict(values.current)["ema_slow"] != dict(seed.values)["ema_slow"]
    assert values.decision_at == first
    assert len(values.current) == len(values.previous) == 12
    with pytest.raises(ValueError, match="outside"):
        decision_features(data, policy, seed, fixed, decision_at=first, as_of=first + pd.Timedelta(minutes=7))


def test_seed_needs_200_contiguous_authorized_and_available_candles():
    data, policy, archive, _, _, _ = _experiment(count=199)
    with pytest.raises(ValueError, match="200 completed"):
        freeze_seed(data, policy, as_of=SEAM + pd.Timedelta(seconds=11))
    data, policy, archive, _, _, _ = _experiment()
    with pytest.raises(ValueError, match="contiguous"):
        freeze_seed(data, policy, as_of=SEAM)
    with pytest.raises(ValueError, match="content hash"):
        freeze_seed(data, replace(policy, seed_content_id="1" * 64), as_of=SEAM + pd.Timedelta(seconds=11))
    archive.rows.pop(100)
    with pytest.raises(ValueError, match="contiguous"):
        freeze_seed(data, policy, as_of=SEAM + pd.Timedelta(seconds=11))


def test_bridge_rejects_missing_mark_late_handoff_and_processing_window():
    data, policy, _, forward, _, _ = _experiment()
    seed = freeze_seed(data, policy, as_of=SEAM + pd.Timedelta(seconds=11))
    close = SEAM + policy.timeframe
    with pytest.raises(ValueError, match="Processing"):
        qualify_bridge(data, policy, seed, as_of=close + pd.Timedelta(seconds=39))
    original = forward.rows[:]
    forward.rows = [row for row in original if not (row.kind == MARK and row.end == close)]
    with pytest.raises(ValueError, match="missing a native"):
        qualify_bridge(data, policy, seed, as_of=close + pd.Timedelta(seconds=40))
    forward.rows = [replace(row, available_at=close + pd.Timedelta(seconds=121))
                    if row.end == close else row for row in original]
    with pytest.raises(ValueError, match="missed"):
        qualify_bridge(data, policy, seed, as_of=close + pd.Timedelta(seconds=180))


def test_foreign_grant_and_changed_checkpoint_cannot_issue_eligibility(tmp_path):
    data, policy, _, _, grants, providers = _experiment()
    now = SEAM + pd.Timedelta(seconds=11)
    second_grant = replace(grants[1], book_id="another book")
    combined = MarketData([*grants, second_grant], providers)
    with pytest.raises(ValueError, match="registered experiment"):
        freeze_seed(combined, replace(policy, forward_grant_id=second_grant.id), as_of=now)
    seed = freeze_seed(data, policy, as_of=now)
    ready = SEAM + policy.timeframe + pd.Timedelta(seconds=40)
    with pytest.raises(ValueError, match="does not match"):
        qualify_bridge(data, policy, replace(seed, bars=400), as_of=ready)
    fixed = record_eligibility(tmp_path, data, policy, seed, as_of=ready)
    checkpoint = tmp_path / f"{fixed.id}.json"
    checkpoint.write_text('{"version":"feature-warmup-v1"}', encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        record_eligibility(tmp_path, data, policy, seed, as_of=ready)


def test_one_timely_bridge_does_not_replace_a_complete_forward_grant():
    _, policy, _, _, grants, providers = _experiment()
    shortened = replace(grants[1], end=SEAM + pd.Timedelta(days=90))
    data = MarketData([grants[0], shortened], providers)
    policy = replace(policy, forward_grant_id=shortened.id)
    seed = freeze_seed(data, policy, as_of=SEAM + pd.Timedelta(seconds=11))
    ready = SEAM + policy.timeframe + pd.Timedelta(seconds=40)
    with pytest.raises(ValueError, match="entire prospective horizon"):
        qualify_bridge(data, policy, seed, as_of=ready)
    with pytest.raises(ValueError, match="observation window elapsed"):
        qualify_bridge(data, policy, seed, as_of=ready + pd.Timedelta(minutes=7))
