from dataclasses import dataclass

import numpy as np
import pandas as pd

from lab.config import Execution, Profile


def execution_open(available: pd.Timestamp, step: pd.Timedelta, delay_bars: int = 0) -> pd.Timestamp:
    return available.floor(step) + (1 + delay_bars) * step


@dataclass(frozen=True)
class Setup:
    signal_at: pd.Timestamp
    available_at: pd.Timestamp
    entry_at: pd.Timestamp
    decision_close: float
    stop: float
    deadline: pd.Timestamp
    entry_raw: float | None = None
    exit_raw: float | None = None
    exit_at: pd.Timestamp | None = None
    exit_bar_open: pd.Timestamp | None = None
    reason: str = ""
    observed_through: pd.Timestamp | None = None


def known_exit(entry: pd.Timestamp, profile: Profile, costs: Execution) -> tuple:
    deadline = entry + pd.Timedelta(minutes=profile.max_minutes)
    reason = "max_hold"
    if profile.session:
        flat = entry.normalize() + pd.Timedelta(minutes=profile.session.flat_minute_utc)
        if flat <= deadline:
            deadline, reason = flat, "session"
    target_open = deadline.floor(profile.step)
    release = max(entry, target_open - (1 + costs.delay_bars) * profile.step)
    return deadline, execution_open(release, profile.step, costs.delay_bars), reason


def plan_setups(candles: pd.DataFrame, signals: pd.DataFrame, profile: Profile,
                costs: Execution, boundary: pd.Timestamp) -> list[Setup]:
    """Resolve each qualifying setup independently using the same execution conventions."""
    step = profile.step
    processing = pd.Timedelta(seconds=costs.processing_seconds)
    candles = candles.loc[(candles.index < boundary) & (candles.close_at <= boundary)]
    signals = signals.loc[signals.index < boundary]
    index = candles.index
    opens, lows = candles.open.to_numpy(), candles.low.to_numpy()
    gap_starts = np.flatnonzero(np.diff(index.as_unit("ns").asi8) != step.value) + 1
    exit_signals = signals.index[signals.exit]
    scheduled_exits = pd.DatetimeIndex([
        execution_open(ts + processing, step, costs.delay_bars) for ts in exit_signals
    ])
    setups = []
    for signal_at, signal in signals[signals.enter].iterrows():
        available = signal_at + processing
        entry = execution_open(available, step, costs.delay_bars)
        deadline, time_exit, reason = known_exit(entry, profile, costs)
        fields = dict(signal_at=signal_at, available_at=available, entry_at=entry,
                      decision_close=float(signal.close), stop=float(signal.stop), deadline=deadline)
        if profile.session:
            cutoff = entry.normalize() + pd.Timedelta(minutes=profile.session.cutoff_minute_utc)
            if entry >= cutoff or entry.normalize() != signal_at.normalize():
                setups.append(Setup(**fields, reason="entry_cutoff"))
                continue
        if time_exit > deadline:
            setups.append(Setup(**fields, reason="deadline_infeasible"))
            continue
        if entry >= boundary:
            setups.append(Setup(**fields, reason="entry_outside_window"))
            continue
        start = index.searchsorted(entry)
        if start == len(index) or index[start] != entry:
            setups.append(Setup(**fields, reason="missing_entry_bar"))
            continue
        next_gap = gap_starts.searchsorted(start, side="right")
        segment_end = gap_starts[next_gap] if next_gap < len(gap_starts) else len(index)
        first_missing = index[segment_end - 1] + step
        if len(scheduled_exits):
            sell = exit_signals.searchsorted(entry)
            if sell < len(scheduled_exits) and scheduled_exits[sell] < time_exit:
                time_exit, reason = scheduled_exits[sell], "signal"
        due = index.searchsorted(time_exit)
        stop_end = min(due, segment_end)
        stop_hits = np.flatnonzero(lows[start:stop_end] <= signal.stop)
        fields["entry_raw"] = float(opens[start])
        if len(stop_hits):
            hit = start + stop_hits[0]
            bar_open = index[hit]
            gap_through = opens[hit] <= signal.stop
            setups.append(Setup(**fields, exit_raw=float(min(signal.stop, opens[hit])),
                                exit_at=bar_open if gap_through else bar_open + step,
                                exit_bar_open=bar_open, reason="stop_open" if gap_through else "stop_intrabar"))
        elif due < segment_end:
            stop_open = opens[due] <= signal.stop
            setups.append(Setup(**fields, exit_raw=float(opens[due]), exit_at=index[due],
                                exit_bar_open=index[due], reason="stop_open" if stop_open else reason))
        else:
            unresolved = "execution_gap" if first_missing < boundary else "window_boundary"
            setups.append(Setup(**fields, reason=unresolved, observed_through=first_missing))
    return setups


def adverse(raw: float, costs: Execution, buy: bool) -> float:
    return raw * (1 + costs.proxy if buy else 1 - costs.proxy)


def trade_return(setup: Setup, costs: Execution) -> float:
    entry = adverse(setup.entry_raw, costs, buy=True)
    exit_price = adverse(setup.exit_raw, costs, buy=False)
    return (exit_price * (1 - costs.fee) - entry * (1 + costs.fee)) / entry
