import numpy as np
import pandas as pd

from lab.data import aggregate
from lab.features import contiguous_groups


REGISTRY = {
    "return_1": "Close / prior close - 1, 1 prior decision bar.",
    "return_5": "Close / close five bars ago - 1, 5 prior decision bars.",
    "range_high_distance": "Close / highest high of 20 prior decision bars - 1.",
    "range_low_distance": "Close / lowest low of 20 prior decision bars - 1.",
    "ema_distance": "Close / EMA(26) - 1.",
    "ema_slope": "EMA(26) / previous EMA(26) - 1.",
    "atr_fraction": "Mean 14-bar true range / current close.",
    "realized_vol": "20-bar standard deviation of closed log returns, ddof=0.",
    "relative_volume": "Current volume / mean volume of 20 prior closed bars - 1.",
    "rsi": "14-bar arithmetic RSI scaled from 0 to 1. Flat window = 0.5.",
    "macd": "(EMA(12) - EMA(26)) / close.",
    "macd_hist": "(MACD price spread - its EMA(9)) / close.",
    "swing_high_distance": "Distance from last confirmed 2-left/2-right high, divided by ATR(14).",
    "swing_low_distance": "Distance from last confirmed 2-left/2-right low, divided by ATR(14).",
    "fib_618_distance": "Close minus the 61.8% level between most recent valid confirmed low and high, divided by ATR.",
    "context_distance_i": "Latest completed higher-frame close / configured SMA - 1.",
    "context_slope_i": "Latest completed higher-frame SMA / its previous completed SMA - 1.",
}


def feature_registry():
    return {"source": "Canonical closed trade OHLCV, aggregated only from complete native buckets.",
            "availability": "Decision-bar close plus frozen processing allowance. Historical receipt times are unknown. "
                            "Rolling state resets on gaps, and pivots first appear at the second following completed bar.",
            "features": REGISTRY, "missing": "No imputation across gaps, missing context, or unconfirmed anchors."}


def confirmed_pivot(values, high, radius=2):
    window = values.rolling(2 * radius + 1)
    candidate = values.shift(radius)
    extreme = window.max() if high else window.min()
    return candidate.where(candidate == extreme).ffill()


def segment_features(bar):
    close = bar.close
    previous = close.shift()
    true_range = pd.concat([bar.high - bar.low, (bar.high - previous).abs(), (bar.low - previous).abs()], axis=1)
    atr = true_range.max(axis=1).rolling(14).mean()
    ema = close.ewm(span=26, adjust=False, min_periods=26).mean()
    macd = close.ewm(span=12, adjust=False, min_periods=12).mean() - ema
    gain = close.diff().clip(lower=0).rolling(14).mean()
    loss = -close.diff().clip(upper=0).rolling(14).mean()
    prior_high = bar.high.shift().rolling(20).max()
    prior_low = bar.low.shift().rolling(20).min()
    high, low = confirmed_pivot(bar.high, True), confirmed_pivot(bar.low, False)
    fib = (low + (high - low) * .618).where(high > low)
    return pd.DataFrame({
        "close": close, "atr": atr, "ema": ema, "prior_close": previous, "prior_ema": ema.shift(), "prior_atr": atr.shift(),
        "prior_high": prior_high, "prior_low": prior_low, "pivot_high": high, "pivot_low": low,
        "return_1": close / previous - 1, "return_5": close / close.shift(5) - 1,
        "range_high_distance": close / prior_high - 1, "range_low_distance": close / prior_low - 1,
        "ema_distance": close / ema - 1, "ema_slope": ema / ema.shift() - 1, "atr_fraction": atr / close,
        "realized_vol": np.log(close).diff().rolling(20).std(ddof=0),
        "relative_volume": bar.volume / bar.volume.shift().rolling(20).mean().replace(0, np.nan) - 1,
        "rsi": (gain / (gain + loss)).where((gain + loss) != 0, .5),
        "macd": macd / close, "macd_hist": (macd - macd.ewm(span=9, adjust=False, min_periods=9).mean()) / close,
        "swing_high_distance": (close - high) / atr, "swing_low_distance": (close - low) / atr,
        "fib_618_distance": (close - fib) / atr,
    }, index=bar.index)


def build_features(candles, profile, native_step):
    bars = aggregate(candles, profile["decision"], native_step).set_index("close_at")
    if bars.empty:
        raise ValueError("No complete decision buckets")
    groups = contiguous_groups(bars.index, pd.Timedelta(profile["decision"]))
    features = pd.concat([segment_features(segment) for _, segment in bars.groupby(groups)])
    model_columns = [name for name in REGISTRY if not name.startswith("context_")]
    contexts = []
    for index, item in enumerate(profile["context"]):
        frame = aggregate(candles, item["frame"], native_step).set_index("close_at")
        step = pd.Timedelta(item["frame"])
        groups = contiguous_groups(frame.index, step)
        average = frame.close.groupby(groups).transform(lambda x: x.rolling(item["sma"]).mean())
        previous = average.groupby(groups).shift()
        distance, slope = f"context_distance_{index}", f"context_slope_{index}"
        features[distance] = (frame.close / average - 1).reindex(features.index, method="ffill",
                                                                 tolerance=step - pd.Timedelta("1ns"))
        features[slope] = (average / previous - 1).reindex(features.index, method="ffill",
                                                           tolerance=step - pd.Timedelta("1ns"))
        model_columns.extend((distance, slope))
        contexts.append(distance)
    features["uptrend"] = (features[contexts] > 0).all(axis=1)
    features["downtrend"] = (features[contexts] < 0).all(axis=1)
    features["context_abs"] = features[contexts].abs().max(axis=1, skipna=False)
    features["ready"] = np.isfinite(features[model_columns]).all(axis=1) & features.atr.gt(0)
    return features, model_columns


def directions(features, strategy, parameters, long_only):
    f = features
    if strategy == "breakout_v1":
        if parameters["prior_range"] != 20:
            raise ValueError("Breakout version fixes the prior range at 20")
        long = f.uptrend & f.close.gt(f.prior_high)
        short = f.downtrend & f.close.lt(f.prior_low)
    elif strategy == "pullback_v1":
        minimum = parameters["prior_retracement_atr"]
        long = f.uptrend & (f.prior_close <= f.prior_ema - minimum * f.prior_atr) & f.return_1.gt(0)
        short = f.downtrend & (f.prior_close >= f.prior_ema + minimum * f.prior_atr) & f.return_1.lt(0)
    elif strategy == "range_reversion_v1":
        regime = f.context_abs.le(parameters["context_abs_max"])
        deviation = parameters["deviation_atr"] * f.atr
        long = regime & (f.close < f.ema - deviation) & f.rsi.lt(.4)
        short = regime & (f.close > f.ema + deviation) & f.rsi.gt(.6)
    else:
        raise ValueError(f"Unknown strategy {strategy}")
    return pd.Series(np.select([f.ready & long, f.ready & short & (not long_only)], [1, -1], default=0), index=f.index)
