"""Indicators and entry gates for the frozen Pine v6 EMA pullback recipe."""
import numpy as np
import pandas as pd

from lab.config import ORIGIN
from lab.data import OHLCV, aggregate
from lab.features import contiguous_groups


FAMILY = "ema_pullback_trend_continuation"
EXIT = "signal_atr_bracket_v1"
ENTRY_PARAMETERS = {
    "ema_fast": 50,
    "ema_slow": 200,
    "rsi_length": 14,
    "rsi_long": 40,
    "rsi_short": 60,
    "volume_length": 20,
    "atr_length": 14,
}
EXIT_PARAMETERS = {"target_atr": 2.5, "stop_atr": 0.5, "swing_bars": 5}
WARMUP_BARS = ENTRY_PARAMETERS["ema_slow"]

COMPARISONS = ["close", "ema_fast", "ema_slow", "prior_ema_fast", "prior_low", "prior_high",
               "prior_close", "volume", "volume_sma", "rsi"]


def _empty_features():
    columns = dict.fromkeys([*OHLCV, *COMPARISONS, "atr", "swing_low", "swing_high", "realized_vol"], float)
    columns.update(contiguous_bars=int, ready=bool, uptrend=bool, downtrend=bool)
    return pd.DataFrame({name: pd.Series(dtype=kind) for name, kind in columns.items()},
                        index=pd.DatetimeIndex([], name="close_at", tz="UTC"))


def _rma(values, length):
    """Wilder smoothing seeded by the first complete arithmetic average."""
    observed = values.dropna()
    if len(observed) < length:
        return pd.Series(np.nan, index=values.index, dtype=float)
    seeded = observed.iloc[length - 1:].copy()
    seeded.iloc[0] = observed.iloc[:length].mean()
    return seeded.ewm(alpha=1 / length, adjust=False).mean().reindex(values.index)


def _segment_features(bars):
    close = bars.close
    previous = close.shift()
    change = close - previous
    true_range = pd.concat([bars.high - bars.low, (bars.high - previous).abs(),
                            (bars.low - previous).abs()], axis=1).max(axis=1)
    gain = _rma(change.clip(lower=0), ENTRY_PARAMETERS["rsi_length"])
    loss = _rma(-change.clip(upper=0), ENTRY_PARAMETERS["rsi_length"])
    # Positive gains and zero losses produce 100, and the reverse produces 0.
    # Both averages zero is undefined in the documented RSI formula.
    rsi = (100 - 100 / (1 + gain / loss)).where(gain.gt(0) | loss.gt(0))
    fast = close.ewm(span=ENTRY_PARAMETERS["ema_fast"], adjust=False).mean()
    slow = close.ewm(span=ENTRY_PARAMETERS["ema_slow"], adjust=False).mean()
    frame = bars.copy()
    frame["ema_fast"], frame["ema_slow"] = fast, slow
    frame["prior_ema_fast"], frame["prior_close"] = fast.shift(), previous
    frame["prior_low"], frame["prior_high"] = bars.low.shift(), bars.high.shift()
    frame["rsi"] = rsi
    frame["atr"] = _rma(true_range, ENTRY_PARAMETERS["atr_length"])
    frame["volume_sma"] = bars.volume.rolling(ENTRY_PARAMETERS["volume_length"]).mean()
    frame["swing_low"] = bars.low.rolling(EXIT_PARAMETERS["swing_bars"]).min()
    frame["swing_high"] = bars.high.rolling(EXIT_PARAMETERS["swing_bars"]).max()
    frame["realized_vol"] = np.log(close).diff().rolling(20).std(ddof=0)
    frame["contiguous_bars"] = np.arange(1, len(frame) + 1)
    required = [*COMPARISONS, "atr", "swing_low", "swing_high"]
    frame["ready"] = frame.contiguous_bars.ge(WARMUP_BARS) & np.isfinite(frame[required]).all(axis=1)
    frame["uptrend"] = fast.round(9).gt(slow.round(9))
    frame["downtrend"] = fast.round(9).lt(slow.round(9))
    return frame


def features(candles, decision, native_step):
    """Aggregate only complete valid UTC buckets and restart after each gap."""
    if candles.empty or not set(OHLCV).issubset(candles.columns):
        return _empty_features()
    if not isinstance(candles.index, pd.DatetimeIndex) or str(candles.index.tz) != "UTC":
        raise ValueError("EMA pullback requires a UTC index of native candle opens")
    native_step = pd.Timedelta(native_step)
    native = candles[OHLCV].apply(pd.to_numeric, errors="coerce").astype(float)
    valid = (np.isfinite(native).all(axis=1) & native.volume.ge(0)
             & native[OHLCV[:-1]].gt(0).all(axis=1)
             & native.high.ge(native[["open", "close"]].max(axis=1))
             & native.low.le(native[["open", "close"]].min(axis=1)))
    valid &= ((native.index - ORIGIN) % native_step == pd.Timedelta(0)) & ~native.index.duplicated(keep=False)
    if "close_at" in candles:
        valid &= candles.close_at.eq(native.index + native_step)
    bars = aggregate(native.loc[valid], decision, native_step).set_index("close_at")
    # Even a sum of individually finite volumes can overflow.
    bars = bars.loc[np.isfinite(bars.volume)]
    if bars.empty:
        return _empty_features()
    groups = contiguous_groups(bars.index, pd.Timedelta(decision))
    return pd.concat([_segment_features(segment) for _, segment in bars.groupby(groups, sort=False)])


def _gate_values(data):
    # Pine v6 rounds floating-point operands to nine fractional digits for comparisons.
    values = data[COMPARISONS].astype(float).round(9)
    ready = np.equal(data.ready, True) & np.isfinite(values).all(axis=values.ndim - 1)
    volume = values.volume > values.volume_sma
    return {
        "long": {"ready": ready, "regime": values.ema_fast > values.ema_slow,
                 "prior_touch": (values.prior_low <= values.prior_ema_fast)
                 | (values.prior_close < values.prior_ema_fast),
                 "close_confirmation": values.close > values.ema_fast,
                 "volume": volume, "rsi": values.rsi > ENTRY_PARAMETERS["rsi_long"]},
        "short": {"ready": ready, "regime": values.ema_fast < values.ema_slow,
                  "prior_touch": (values.prior_high >= values.prior_ema_fast)
                  | (values.prior_close > values.prior_ema_fast),
                  "close_confirmation": values.close < values.ema_fast,
                  "volume": volume, "rsi": values.rsi < ENTRY_PARAMETERS["rsi_short"]},
    }


def gates(row):
    """Explain each market-data gate without applying position or profile rules."""
    return {side: {name: bool(result) if pd.notna(result) else False for name, result in checks.items()}
            for side, checks in _gate_values(row).items()}


def gate_frame(frame):
    if frame.empty:
        return pd.DataFrame(index=frame.index)
    return pd.DataFrame({f"{side}.{name}": value for side, checks in _gate_values(frame).items()
                         for name, value in checks.items()})


def sides(frame):
    """Return the eligible source side as +1, -1 or 0 for each decision."""
    if frame.empty:
        return pd.Series(index=frame.index, dtype=np.int8)
    checks = _gate_values(frame)
    long = pd.DataFrame(checks["long"]).fillna(False).all(axis=1)
    short = pd.DataFrame(checks["short"]).fillna(False).all(axis=1)
    return pd.Series(np.select([long, short], [1, -1], default=0), index=frame.index, dtype=np.int8)
