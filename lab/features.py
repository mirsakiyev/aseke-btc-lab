import numpy as np
import pandas as pd

from lab.config import Profile
from lab.data import aggregate


def contiguous_groups(index: pd.DatetimeIndex, step: pd.Timedelta) -> np.ndarray:
    return (np.r_[0, np.cumsum(np.diff(index.as_unit("ns").asi8) != step.value)]
            if len(index) else np.array([], dtype=int))


def feature_columns(profile: Profile) -> list[str]:
    columns = ["momentum", "atr_fraction", "breakout_distance", "relative_volume"]
    columns.extend(f"context_{item.frame}_spread" for item in profile.context)
    if profile.session:
        columns.append("session_remaining_hours")
    return columns


def decisions(candles: pd.DataFrame, profile: Profile) -> pd.DataFrame:
    bars = aggregate(candles, profile.decision, profile.step).set_index("close_at")
    groups = contiguous_groups(bars.index, pd.Timedelta(profile.decision))
    by_segment = bars.groupby(groups, sort=False)
    prior_close = by_segment.close.shift()
    true_range = pd.concat([
        bars.high - bars.low, (bars.high - prior_close).abs(), (bars.low - prior_close).abs(),
    ], axis=1).max(axis=1)
    bars["atr"] = true_range.groupby(groups).transform(lambda x: x.rolling(profile.atr_period).mean())
    prior_high = by_segment.high.transform(lambda x: x.shift().rolling(profile.entry_range).max())
    prior_low = by_segment.low.transform(lambda x: x.shift().rolling(profile.exit_range).min())
    bars["momentum"] = bars.close / by_segment.close.shift(profile.momentum_period) - 1
    bars["atr_fraction"] = bars.atr / bars.close
    bars["breakout_distance"] = bars.close / prior_high - 1
    volume_base = by_segment.volume.transform(lambda x: x.shift().rolling(profile.volume_period).mean())
    bars["relative_volume"] = bars.volume / volume_base.where(volume_base > 0) - 1
    bars["stop"] = bars.close - profile.atr_multiple * bars.atr
    context_ok = pd.Series(True, index=bars.index)
    context_ready = context_ok.copy()
    for context in profile.context:
        frame = aggregate(candles, context.frame, profile.step).set_index("close_at")
        duration = pd.Timedelta(context.frame)
        groups = contiguous_groups(frame.index, duration)
        close = frame.close.groupby(groups, sort=False)
        if context.average == "sma":
            reference = close.transform(lambda x: x.rolling(context.slow).mean())
        else:
            reference = close.transform(
                lambda x: x.ewm(span=context.slow, adjust=False, min_periods=context.slow).mean()
            )
        comparison = frame.close
        if context.fast is not None:
            comparison = close.transform(
                lambda x: x.ewm(span=context.fast, adjust=False, min_periods=context.fast).mean()
            )
        spread = comparison / reference - 1
        aligned = spread.reindex(bars.index, method="ffill", tolerance=duration - pd.Timedelta("1ns"))
        bars[f"context_{context.frame}_spread"] = aligned
        context_ok &= aligned > 0
        context_ready &= np.isfinite(aligned)
    if profile.session:
        flat_at = bars.index.normalize() + pd.Timedelta(minutes=profile.session.flat_minute_utc)
        bars["session_remaining_hours"] = (flat_at - bars.index) / pd.Timedelta("1h")
    bars["baseline_ready"] = context_ready & prior_high.notna() & prior_low.notna() & bars.stop.gt(0)
    bars["ready"] = bars.baseline_ready & np.isfinite(bars[feature_columns(profile)]).all(axis=1)
    bars["enter"] = bars.baseline_ready & context_ok & (bars.close > prior_high)
    bars["exit"] = bars.close < prior_low
    return bars
