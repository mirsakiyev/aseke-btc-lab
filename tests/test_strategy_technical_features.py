"""Synthetic tests of shared chart and indicator calculations."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from lab.strategy_lab import ema_pullback, technical_features as technical


START = pd.Timestamp("2026-01-01", tz="UTC")
EMA_PLAN = {
    "ema_fast": technical.IndicatorSpec("ema", 50),
    "ema_slow": technical.IndicatorSpec("ema", 200),
    "rsi": technical.IndicatorSpec("rsi", 14),
    "atr": technical.IndicatorSpec("atr", 14),
    "volume_sma": technical.IndicatorSpec("sma", 20),
    "swing_high": technical.IndicatorSpec("rolling_high", 5),
    "swing_low": technical.IndicatorSpec("rolling_low", 5),
}


def native_candles(count, native_step):
    step = pd.Timedelta(native_step)
    index = pd.date_range(START, periods=count, freq=step, name="open_at")
    opened = 100 + np.arange(count) * .001 + np.sin(np.arange(count) / 30)
    return pd.DataFrame({"open": opened, "high": opened + 2, "low": opened - 1,
                         "close": opened + .5, "volume": 1. + np.arange(count) % 11,
                         "close_at": index + step, "available_at": index + step + pd.Timedelta("2s")},
                        index=index)


def build(native, interval, step, periods, as_of=None):
    end = START + periods * pd.Timedelta(interval)
    cutoff = end + pd.Timedelta("2s") if as_of is None else as_of
    return technical.build_candles(native, timeframe=interval, native_step=step,
                                   start=START, end=end, as_of=cutoff)


@pytest.mark.parametrize(("interval", "step", "size"), [("15min", "1min", 15),
                                                         ("4h", "1min", 240),
                                                         ("1D", "1h", 24)])
def test_chart_aggregation_uses_each_closed_constituent_and_its_actual_availability(interval, step, size):
    frame = native_candles(2 * size, step)
    first_end = START + pd.Timedelta(interval)
    delayed = first_end + pd.Timedelta("10s")
    frame.loc[frame.index[size - 2], "available_at"] = delayed
    unseen = build(frame, interval, step, 2, as_of=first_end + pd.Timedelta("2s"))
    assert unseen.expected_bars == unseen.missing_bars == 2
    assert not unseen.complete
    batch = build(frame, interval, step, 2)
    assert batch.complete
    first = batch.bars.iloc[0]
    samples = frame.iloc[:size]
    assert first.open == samples.open.iloc[0]
    assert first.high == samples.high.max()
    assert first.low == samples.low.min()
    assert first.close == samples.close.iloc[-1]
    assert first.volume == samples.volume.sum()
    assert first.close_at == first_end
    assert first.available_at == delayed
    assert batch.bars.index.tolist() == [START, first_end]


def test_invalid_missing_and_duplicate_constituents_do_not_generate_synthetic_bars():
    step = pd.Timedelta("1min")
    frame = native_candles(75, step)
    frame = frame.drop(frame.index[3])
    frame.loc[START + 16 * step, "volume"] = -1
    frame.loc[START + 31 * step, "available_at"] = START + 31 * step
    frame = pd.concat([frame, frame.loc[[START + 50 * step]]])
    batch = build(frame, "15min", step, 5)
    assert batch.missing_bars == 4
    assert batch.bars.index.tolist() == [START + pd.Timedelta("1h")]
    with pytest.raises(ValueError, match="row budget"):
        technical.build_candles(frame, timeframe="15min", native_step=step, start=START,
                                end=START + pd.Timedelta("2h"), as_of=batch.as_of, max_native_rows=100)
    with pytest.raises(ValueError, match="timezone-aware"):
        build(frame, "15min", step, 5, as_of=pd.Timestamp("2026-01-02"))


@pytest.mark.parametrize(("interval", "step", "size"), [("15min", "1min", 15),
                                                         ("4h", "1min", 240),
                                                         ("1D", "1h", 24)])
def test_reviewed_indicators_match_registered_ema_features_on_each_timeframe(interval, step, size):
    count = 207
    frame = native_candles(count * size, step)
    batch = build(frame, interval, step, count)
    expected = ema_pullback.features(frame, interval, step)
    observed = technical.compute_indicators(batch.bars, timeframe=interval, indicators=EMA_PLAN)
    observed.index = batch.bars.close_at
    pd.testing.assert_frame_equal(observed, expected[list(EMA_PLAN)], check_freq=False)
    assert expected.ready.iloc[199]
    assert not expected.ready.iloc[198]


def test_gaps_reset_indicator_state_and_appending_cannot_change_the_past():
    step = pd.Timedelta("1h")
    frame = native_candles(30, step).drop(START + 22 * step)
    batch = build(frame, step, step, 30)
    full = technical.compute_indicators(batch.bars, timeframe=step, indicators=EMA_PLAN)
    prefix = technical.compute_indicators(batch.bars.iloc[:18], timeframe=step, indicators=EMA_PLAN)
    pd.testing.assert_frame_equal(full.loc[prefix.index], prefix)
    reopened = START + 23 * step
    assert full.loc[reopened, "ema_slow"] == frame.loc[reopened, "close"]
    assert pd.isna(full.loc[reopened, "atr"])
    assert pd.isna(full.loc[reopened, "swing_low"])


def test_exact_cache_hits_isolate_receipts_values_parameters_and_authorized_identity(monkeypatch):
    frame = native_candles(120, "1min")
    bars = build(frame, "15min", "1min", 8).bars
    identity = technical.FeatureIdentity("source", "snapshot", "XBTUSDTM", "FUTURES", "15min", "grant-a")
    plan = {"signal": technical.IndicatorSpec("ema", 5)}
    expected = technical.compute_indicators(bars, timeframe="15min", indicators=plan)
    cache = technical.FeatureCache(max_entries=16)
    compute = technical._compute
    observed = []

    def count(*args):
        observed.append(1)
        return compute(*args)

    monkeypatch.setattr(technical, "_compute", count)
    output = cache.compute(bars, identity=identity, indicators=plan)
    pd.testing.assert_frame_equal(output, expected)
    output.iloc[0, 0] = -1
    pd.testing.assert_frame_equal(cache.compute(bars.copy(), identity=identity, indicators=plan), expected)
    reused = cache.compute(bars, identity=identity, indicators={"macd_fast": plan["signal"]})
    pd.testing.assert_frame_equal(reused, expected.rename(columns={"signal": "macd_fast"}))
    assert len(observed) == 1

    receipt_changed = bars.copy()
    receipt_changed.loc[bars.index[0], "available_at"] += pd.Timedelta("1s")
    pd.testing.assert_frame_equal(cache.compute(receipt_changed, identity=identity, indicators=plan), expected)
    values_changed = bars.copy()
    values_changed.loc[bars.index[0], "close"] += .25
    assert not cache.compute(values_changed, identity=identity, indicators=plan).equals(expected)
    for field, value in (("snapshot_id", "new"), ("source_id", "elsewhere"), ("instrument", "BTC-USDT"),
                         ("market", "SPOT"), ("authorization_id", "grant-b")):
        pd.testing.assert_frame_equal(
            cache.compute(bars, identity=replace(identity, **{field: value}), indicators=plan), expected)
    altered_plan = {"signal": technical.IndicatorSpec("ema", 5, "open")}
    assert not cache.compute(bars, identity=identity, indicators=altered_plan).equals(expected)
    altered_plan = {"signal": technical.IndicatorSpec("ema", 6)}
    assert not cache.compute(bars, identity=identity, indicators=altered_plan).equals(expected)
    other_bars = build(frame, "1h", "1min", 2).bars
    other_id = replace(identity, timeframe="1h")
    result = cache.compute(other_bars, identity=other_id, indicators=plan)
    pd.testing.assert_frame_equal(result, technical.compute_indicators(other_bars, timeframe="1h", indicators=plan))
    assert len(observed) == 12


def test_cache_budget_and_indicator_plan_are_bounded(monkeypatch):
    bars = build(native_candles(30, "1min"), "15min", "1min", 2).bars
    identity = technical.FeatureIdentity("source", "snapshot", "XBTUSDTM", "FUTURES", "15min")
    cache = technical.FeatureCache(max_entries=1)
    plan = {"signal": technical.IndicatorSpec("ema", 2)}
    compute = technical._compute
    calls = []

    def count(*args):
        calls.append(1)
        return compute(*args)

    monkeypatch.setattr(technical, "_compute", count)
    for item in (identity, replace(identity, snapshot_id="other"), identity):
        cache.compute(bars, identity=item, indicators=plan)
    assert len(calls) == 3
    with pytest.raises(ValueError, match="input series"):
        technical.IndicatorSpec("atr", 14, "close")
    with pytest.raises(ValueError, match="length"):
        technical.IndicatorSpec("ema", 10**6)
    with pytest.raises(ValueError, match="budget"):
        technical.FeatureCache(max_entries=1000)


def test_cache_preserves_index_frequency_metadata():
    bars = native_candles(3, "1h")
    no_frequency = bars.copy()
    no_frequency.index = pd.DatetimeIndex(list(bars.index), name=bars.index.name)
    identity = technical.FeatureIdentity("source", "snapshot", "BTC-USDT", "SPOT", "1h")
    cache = technical.FeatureCache()
    plan = {"ema": technical.IndicatorSpec("ema", 2)}
    cache.compute(bars, identity=identity, indicators=plan)
    pd.testing.assert_frame_equal(
        cache.compute(no_frequency, identity=identity, indicators=plan),
        technical.compute_indicators(no_frequency, timeframe="1h", indicators=plan),
    )


@pytest.mark.parametrize(("interval", "native_step", "size"), [
    ("15min", "1min", 15), ("4h", "1min", 240), ("1D", "1h", 24),
])
def test_incremental_updates_match_complete_batch_and_replay_tip(interval, native_step, size):
    bars = build(native_candles(221 * size, native_step), interval, native_step, 221).bars
    identity = technical.FeatureIdentity("authorized-source", "approved-seed", "BTC-USD", "SPOT", interval)
    plan = {**EMA_PLAN, "alias": EMA_PLAN["ema_fast"], "price_sma": technical.IndicatorSpec("sma", 9, "close")}
    cursor = technical.IncrementalFeatures(identity, plan)
    outputs = []
    for left, right in ((0, 7), (7, 200), (200, 201), (201, 221)):
        block = bars.iloc[left:right]
        outputs.append(cursor.update(block, as_of=block.available_at.max(), expected_tip=cursor.tip))
    expected = technical.compute_indicators(bars, timeframe=interval, indicators=plan)
    pd.testing.assert_frame_equal(pd.concat(outputs), expected, rtol=1e-13, atol=1e-12)
    replay = technical.IncrementalFeatures(identity, plan)
    replay.update(bars, as_of=bars.available_at.max(), expected_tip=replay.tip)
    assert replay.tip == cursor.tip
    assert technical.IncrementalFeatures(replace(identity, snapshot_id="other"), plan).tip != cursor.tip


def test_incremental_updates_refuse_stale_corrections_incomplete_and_future_receipts():
    bars = build(native_candles(30, "1h"), "1h", "1h", 30).bars
    identity = technical.FeatureIdentity("authorized-source", "approved-seed", "BTC-USD", "SPOT", "1h")
    cursor = technical.IncrementalFeatures(identity, EMA_PLAN)
    start_tip = cursor.tip
    seed = bars.iloc[:20]
    cursor.update(seed, as_of=seed.available_at.max(), expected_tip=start_tip)
    verified = cursor.tip
    suffix = bars.iloc[20:]
    cutoff = suffix.available_at.max()
    with pytest.raises(ValueError, match="Stale"):
        cursor.update(suffix, as_of=cutoff, expected_tip=start_tip)
    with pytest.raises(ValueError, match="causally unavailable"):
        cursor.update(suffix, as_of=cutoff - pd.Timedelta("1ns"), expected_tip=verified)
    with pytest.raises(ValueError, match="missing chart"):
        cursor.update(suffix.iloc[1:], as_of=cutoff, expected_tip=verified)
    broken = suffix.copy()
    broken.loc[broken.index[0], "close_at"] -= pd.Timedelta("1s")
    with pytest.raises(ValueError, match="incomplete"):
        cursor.update(broken, as_of=cutoff, expected_tip=verified)
    corrected = bars.copy()
    corrected.loc[corrected.index[19], "close"] += .5
    with pytest.raises(ValueError, match="overlaps"):
        cursor.update(corrected.iloc[19:], as_of=cutoff, expected_tip=verified)
    assert cursor.tip == verified
    observed = cursor.update(suffix, as_of=cutoff, expected_tip=verified)
    pd.testing.assert_frame_equal(
        observed, technical.compute_indicators(bars, timeframe="1h", indicators=EMA_PLAN).iloc[20:],
        rtol=1e-13, atol=1e-12,
    )
    rebuilt = technical.IncrementalFeatures(replace(identity, snapshot_id="reviewed-correction"), EMA_PLAN)
    rebuilt.update(corrected, as_of=cutoff, expected_tip=rebuilt.tip)
    assert rebuilt.tip != cursor.tip


def test_incremental_gap_restart_flat_rsi_and_window_budget():
    bars = build(native_candles(40, "1h"), "1h", "1h", 40).bars
    bars.iloc[:, :4] = (100., 102., 98., 101.)
    bars = bars.drop(bars.index[20])
    identity = technical.FeatureIdentity("source", "seed", "BTC-USD", "SPOT", "1h")
    cursor = technical.IncrementalFeatures(identity, EMA_PLAN, require_contiguous=False)
    first = cursor.update(bars.iloc[:20], as_of=bars.available_at.iloc[19], expected_tip=cursor.tip)
    second = cursor.update(bars.iloc[20:], as_of=bars.available_at.max(), expected_tip=cursor.tip)
    observed = pd.concat([first, second])
    assert observed.rsi.isna().all()
    assert observed.atr.iloc[20:33].isna().all()
    assert observed.ema_slow.iloc[20] == 101.
    pd.testing.assert_frame_equal(
        observed, technical.compute_indicators(bars, timeframe="1h", indicators=EMA_PLAN),
        rtol=1e-13, atol=1e-12,
    )
    with pytest.raises(ValueError, match="state budget"):
        technical.IncrementalFeatures(identity, {"sma": technical.IndicatorSpec("sma", 5000)})
