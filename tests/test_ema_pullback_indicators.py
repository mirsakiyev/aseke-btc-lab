import numpy as np
import pandas as pd
import pytest

from lab.strategy_lab.ema_pullback import WARMUP_BARS, features, gates, sides


def native_candles(close):
    at = pd.date_range("2024-01-01", periods=len(close), freq="1h", tz="UTC")
    return pd.DataFrame({"open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 10.,
                         "close_at": at + pd.Timedelta("1h")}, index=at)


def pullbacks(direction):
    candles = native_candles(200 + direction * np.arange(216) * .5)
    signals = [205, 210, 213]
    for pos in signals:
        field = "low" if direction == 1 else "high"
        candles.loc[candles.index[pos - 1], field] = candles.close.iloc[pos - 1] - direction * 20
        candles.loc[candles.index[pos], "volume"] = 40.
    return candles, signals


@pytest.mark.parametrize(("direction", "label"), [(1, "long"), (-1, "short")])
def test_pine_entry_accepts_repeated_touches_and_both_directions(direction, label):
    candles, positions = pullbacks(direction)
    output = features(candles, "1h", "1h")
    result = sides(output)
    assert result.iloc[positions].tolist() == [direction] * len(positions)
    for position in positions:
        assert all(gates(output.iloc[position])[label].values())
    assert not output.ready.iloc[:WARMUP_BARS - 1].any()
    assert output.ready.iloc[WARMUP_BARS - 1]
    assert (result != 0).sum() == 3


@pytest.mark.parametrize(("direction", "label", "threshold"), [(1, "long", 40.), (-1, "short", 60.)])
def test_pine_strict_thresholds_and_prior_touch(direction, label, threshold):
    candles, positions = pullbacks(direction)
    row = features(candles, "1h", "1h").iloc[positions[0]].copy()
    boundary = {
        "close_confirmation": {"close": row.ema_fast},
        "regime": {"ema_slow": row.ema_fast},
        "volume": {"volume": row.volume_sma},
        "rsi": {"rsi": threshold + direction * .0000000003},
    }
    for name, fields in boundary.items():
        failing = row.copy()
        for key, value in fields.items():
            failing[key] = value
        assert not gates(failing)[label][name]
        assert sides(failing.to_frame().T).iloc[0] == 0
    below = row.copy()
    below["volume"] = row.volume_sma - 1
    assert not gates(below)[label]["volume"]
    invalid = row.copy()
    invalid["volume"] = np.inf
    assert not gates(invalid)[label]["ready"]
    assert sides(invalid.to_frame().T).iloc[0] == 0
    # The entire previous bar can be on the opposite side of its fast EMA.
    row["prior_close"] = row.prior_ema_fast - direction * 2
    row["prior_high"] = row.prior_close + 1
    row["prior_low"] = row.prior_close - 1
    assert all(gates(row)[label].values())


def test_wilder_rsi_atr_and_recursive_ema_hand_calculation():
    close = 100. + np.cumsum(np.tile([2., -1., 3., -4., .5], 45))
    candles = native_candles(close)
    output = features(candles, "1h", "1h")
    returns = np.diff(close)
    gains, losses = np.maximum(returns, 0), np.maximum(-returns, 0)
    tr = np.r_[2., np.maximum(2., np.abs(returns) + 1.)]
    atr_seed = sum(tr[:14]) / 14
    gain = sum(gains[:14]) / 14
    loss = sum(losses[:14]) / 14
    assert output.atr.first_valid_index() == output.index[13]
    assert output.atr.iloc[13:15].to_numpy() == pytest.approx([atr_seed, (13 * atr_seed + tr[14]) / 14])
    assert output.rsi.first_valid_index() == output.index[14]
    assert output.rsi.iloc[14] == pytest.approx(100 - 100 / (1 + gain / loss))
    assert output.rsi.iloc[15] == pytest.approx(100 - 100 / (1 + (13 * gain + gains[14]) / (13 * loss + losses[14])))
    for name, length in [("ema_fast", 50), ("ema_slow", 200)]:
        alpha = 2 / (length + 1)
        assert output[name].iloc[:2].to_numpy() == pytest.approx([close[0], (1 - alpha) * close[0] + alpha * close[1]])


def test_confirmation_candle_counts_in_extremes_and_volume_average():
    candles, positions = pullbacks(1)
    signal = positions[0]
    candles.loc[candles.index[signal], ["low", "high"]] = [120., 400.]
    row = features(candles, "1h", "1h").iloc[signal]
    assert (row.swing_low, row.swing_high) == (120., 400.)
    assert row.volume_sma == pytest.approx((19 * 10 + 40) / 20)
    assert row.prior_low == candles.low.iloc[signal - 1]


def test_invalid_or_incomplete_native_candles_cannot_complete_a_chart_bar():
    candles = native_candles(100. + np.arange(12))
    corrupt = candles.copy()
    corrupt.loc[corrupt.index[2], "volume"] = np.nan
    corrupt.loc[corrupt.index[9], "volume"] = -1
    output = features(corrupt, "4h", "1h")
    assert output.index.tolist() == [candles.close_at.iloc[7]]
    assert output.contiguous_bars.tolist() == [1]
    assert not output.ready.iloc[0]
    assert features(candles.iloc[:3], "4h", "1h").ready.empty
    assert features(candles.drop(columns="volume"), "4h", "1h").ready.empty
    duplicates = pd.concat([candles, candles.iloc[[1]]]).sort_index()
    assert features(duplicates, "4h", "1h").index.tolist() == candles.close_at.iloc[[7, 11]].tolist()
    corrupt = candles.copy()
    corrupt.loc[corrupt.index[2], "close_at"] += pd.Timedelta(seconds=1)
    assert features(corrupt, "4h", "1h").index.tolist() == candles.close_at.iloc[[7, 11]].tolist()


def test_indicators_reset_after_invalid_bar_and_never_read_future_inputs():
    candles, _ = pullbacks(1)
    prefix = features(candles.iloc[:211], "1h", "1h")
    pd.testing.assert_frame_equal(prefix, features(candles, "1h", "1h").loc[prefix.index])
    corrupt = candles.copy()
    corrupt.loc[corrupt.index[204], "volume"] = np.inf
    output = features(corrupt, "1h", "1h")
    assert corrupt.close_at.iloc[204] not in output.index
    restart = output.loc[corrupt.close_at.iloc[205]]
    assert restart.contiguous_bars == 1
    assert restart.ema_fast == restart.ema_slow == restart.close
    assert not restart.ready
    assert sides(output.loc[corrupt.close_at.iloc[205]:]).eq(0).all()


def test_flat_start_is_undefined_rsi_and_no_trade():
    output = features(native_candles(np.full(201, 100.)), "1h", "1h")
    assert output.rsi.isna().all()
    assert output.ema_fast.eq(output.ema_slow).all()
    assert sides(output).eq(0).all()
