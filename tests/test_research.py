from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from lab.backtest import simulate, size_order
from lab.broker import Fill, replay
from lab.config import Execution, Profile, load_profiles
from lab.data import aggregate
from lab.execution import execution_open, plan_setups
from lab.features import decisions, feature_columns
from lab.model import check_model, fit, label_set, model_identity, split_labels, windows
from lab.research import clip
from lab.storage import utc


@pytest.fixture
def profiles():
    return load_profiles()


def candles(start, periods, step, price=100.0):
    index = pd.date_range(start, periods=periods, freq=step, tz="UTC", name="open_at")
    return pd.DataFrame({"open": price, "high": price + 1, "low": price - 1, "close": price,
                         "volume": 1.0, "close_at": index + step}, index=index)


def signals(*times, stop=95.0):
    return pd.DataFrame({"enter": True, "exit": False, "close": 100.0, "stop": stop},
                        index=pd.DatetimeIndex([utc(ts) for ts in times]))


def test_profile_isolation_and_validation(profiles):
    pos, swing, day = profiles.values()
    assert {pos.decision, swing.decision, day.decision} == {"1D", "4h", "15min"}
    assert all(profile.market_type == "spot" and profile.starting_cash == 10000 for profile in profiles.values())
    assert len({profile.identity for profile in profiles.values()}) == 3
    assert "session_remaining_hours" in feature_columns(day)
    altered = day.model_dump()
    altered["execution"] = "1h"
    with pytest.raises(ValueError, match="Decision bars"):
        Profile(**altered)
    changed = Profile(**{**pos.model_dump(), "label": "Alternate display name"})
    assert changed.identity != pos.identity


def test_incompatible_model_and_research_only_override(profiles):
    pos, swing, _ = profiles.values()
    info = model_identity(pos)
    check_model(info, pos, pos.costs)
    with pytest.raises(ValueError, match="profile_id"):
        check_model(info, swing, swing.costs)
    alternate = Execution(fee_bps=80, proxy_bps=10, delay_bars=1)
    with pytest.raises(ValueError, match="research-only"):
        check_model(info, pos, alternate)
    check_model(info, pos, alternate, sensitivity=True)
    with pytest.raises(ValueError, match="research-only"):
        check_model(info, pos, Execution(processing_seconds=90), sensitivity=True)


def test_causal_context_alignment_and_complete_buckets(profiles):
    base = profiles["day"]
    profile = Profile(**{**base.model_dump(), "context": [{"frame": "1h", "average": "sma", "slow": 2}]})
    bars = candles("2025-01-06", 110, profile.step)
    bars.loc[bars.index >= utc("2025-01-06T07:00Z"), "close"] = 110
    computed = decisions(bars, profile)
    earlier = decisions(clip(bars, utc("2025-01-06T07:45Z")), profile)
    pd.testing.assert_frame_equal(computed.loc[earlier.index], earlier)
    column = "context_1h_spread"
    assert computed.loc[utc("2025-01-06T07:45Z"), column] == 0
    assert computed.loc[utc("2025-01-06T08:00Z"), column] == pytest.approx(110 / 105 - 1)
    missing = bars.drop(utc("2025-01-06T07:05Z"))
    assert utc("2025-01-06T07:00Z") not in aggregate(missing, "1h", profile.step).index
    assert np.isnan(decisions(missing, profile).loc[utc("2025-01-06T08:00Z"), column])
    week = aggregate(candles("2025-01-06", 168, pd.Timedelta("1h")), "7D", pd.Timedelta("1h"))
    assert week.index[0].weekday() == 0 and week.close_at.iloc[0] == utc("2025-01-13")


def test_purging_and_scaler_use_only_resolved_training_examples(profiles, tmp_path):
    profile = profiles["swing"]
    freeze = windows(tmp_path, persist=True)
    assert windows(tmp_path) == freeze
    end = utc(freeze.validation_start)
    index = pd.DatetimeIndex([end - pd.Timedelta(days=n) for n in (3, 2, 1, 0)])
    features = pd.DataFrame({column: [1, 3, 999, 999] for column in feature_columns(profile)}, index=index)
    features["entry_at"] = index + pd.Timedelta("1h")
    features["resolved_at"] = [end - pd.Timedelta("36h"), end - pd.Timedelta("1h"), end, end + pd.Timedelta("2h")]
    features["target"] = [0.01, -0.02, 0.9, 0.9]
    features["status"] = "signal"
    training, counts = split_labels(features, None, end)
    assert counts["purged_at_end"] == 1
    assert len(training) == 2
    assert fit(training, profile)["standardscaler"].mean_[0] == 2


def test_holdout_prices_never_resolve_an_open_trade(profiles):
    p = profiles["day"]
    bars = candles("2025-11-07T23:45Z", 8, p.step)
    boundary = utc("2025-11-08")
    p = Profile(**{**p.model_dump(), "session": {"cutoff_minute_utc": 1430, "flat_minute_utc": 1435}})
    # Independent window boundary, before the strategy's exit. No low from the boundary bar may be used.
    end = boundary - pd.Timedelta("10min")
    bars.loc[bars.index >= end, ["open", "low", "close"]] = 1
    result = plan_setups(bars, signals("2025-11-07T23:40Z"), p, p.costs, end)[0]
    assert result.reason == "window_boundary"
    assert result.exit_raw is None and result.observed_through == end


def test_fractional_engine_cash_and_same_bar_stop(profiles):
    p = profiles["day"]
    bars = candles("2025-01-01", 10, p.step)
    entry = utc("2025-01-01T00:05Z")
    result = replay(bars, [Fill(entry, 0.25, 100), Fill(entry, -0.25, 90)], 10000, 0.005)
    # Buy $25.00 plus $0.125. Sell $22.50 less $0.1125.
    assert result.cash.iloc[-1] == pytest.approx(9997.2625)
    assert result.units.iloc[-1] == 0
    bars.loc[entry, "low"] = 94
    boundary = bars.close_at.iloc[-1]
    setups = plan_setups(bars, signals("2025-01-01"), p, p.costs, boundary)
    setup = setups[0]
    assert setup.reason == "stop_intrabar" and setup.exit_at == entry + p.step
    units, planned = size_order(setup, p, p.costs, 10000)
    assert planned == pytest.approx(25)
    assert 4 < units < 5
    ledger = simulate(bars, setups, p, p.costs, bars.index[0], boundary)
    assert ledger.metrics["ending_equity_usd"] == pytest.approx(9975)
    assert ledger.trades.exit_price.iloc[0] == pytest.approx(95 * 0.9995)


def test_gap_open_stop_and_fixed_quantity(profiles):
    p = profiles["day"]
    bars = candles("2025-01-01", 15, p.step)
    entry = utc("2025-01-01T00:05Z")
    exit_at = utc("2025-01-01T00:10Z")
    bars.loc[entry, ["open", "high", "close"]] = 110
    bars.loc[exit_at, ["open", "low", "close"]] = 90
    end = bars.close_at.iloc[-1]
    setups = plan_setups(bars, signals("2025-01-01"), p, p.costs, end)
    result = simulate(bars, setups, p, p.costs, bars.index[0], end)
    trade = result.trades.iloc[0]
    assert trade.reason == "stop_open" and trade.exit_at == exit_at
    assert trade.exit_price == pytest.approx(89.955)
    assert trade.realized_loss_overrun_usd > trade.fill_risk_overrun_usd > 0
    assert trade.units_btc == pytest.approx(size_order(setups[0], p, p.costs, 10000)[0])


def test_missing_bar_stops_segment_and_retains_open_position(profiles):
    p = profiles["day"]
    bars = candles("2025-01-01", 20, p.step).drop(utc("2025-01-01T00:15Z"))
    end = bars.close_at.iloc[-1]
    setups = plan_setups(bars, signals("2025-01-01", "2025-01-01T01:00Z"), p, p.costs, end)
    result = simulate(bars, setups, p, p.costs, bars.index[0], end)
    assert result.metrics["status"] == "execution_gap"
    assert not result.metrics["valid_through_last_mark"]
    assert result.metrics["trade_count"] == 0
    assert result.metrics["entries"] == 1 and result.metrics["open_position"]["reason"] == "execution_gap"
    assert result.equity.index[-1] == utc("2025-01-01T00:15Z")


def test_day_session_cutoff_delay_and_dst(profiles):
    p = profiles["day"]
    bars = candles("2025-11-02T21:45Z", 36, p.step)
    end = bars.close_at.iloc[-1]
    basic = signals("2025-11-02T21:45Z", "2025-11-02T21:50Z", "2025-11-02T21:55Z", "2025-11-02T23:55Z")
    setups = plan_setups(bars, basic, p, p.costs, end)
    assert setups[0].exit_at == utc("2025-11-02T23:55Z") and setups[0].reason == "session"
    assert setups[2].reason == setups[3].reason == "entry_cutoff"
    delayed = plan_setups(bars, basic, p, Execution(delay_bars=1), end)
    assert delayed[1].reason == "entry_cutoff" and delayed[0].exit_at == setups[0].exit_at
    assert execution_open(utc("2025-01-01T16:00:30Z"), pd.Timedelta("1h")) == utc("2025-01-01T17:00Z")
    assert execution_open(utc("2025-01-01T16:00:30Z"), p.step) == utc("2025-01-01T16:05Z")
    toronto = ZoneInfo("America/Toronto")
    before, after = [utc(ts).tz_convert(toronto) for ts in ("2025-11-02T05:30Z", "2025-11-02T06:30Z")]
    assert before.hour == after.hour == 1 and before.utcoffset() != after.utcoffset()


def test_hypothetical_signals_while_holding_are_separate(profiles):
    p = profiles["day"]
    bars = candles("2025-01-01T21:00Z", 40, p.step)
    s = signals("2025-01-01T21:00Z", "2025-01-01T21:15Z")
    for col in feature_columns(p):
        s[col] = 0.0
    boundary = bars.close_at.iloc[-1]
    setups = plan_setups(bars, s, p, p.costs, boundary)
    labels = label_set(setups, s, p)
    account = simulate(bars, setups, p, p.costs, bars.index[0], boundary)
    assert len(labels) == 2 and labels.target.notna().all()
    assert labels.resolved_at.eq(utc("2025-01-01T23:55Z")).all()
    assert account.metrics["trade_count"] == 1
    assert split_labels(labels, None, boundary)[1]["overlapping_outcome_pairs"] == 1


def test_day_missing_deadline_bar_never_claims_flat(profiles):
    p = profiles["day"]
    deadline = utc("2025-01-01T23:55Z")
    bars = candles("2025-01-01T21:45Z", 32, p.step).drop(deadline)
    end = bars.close_at.iloc[-1]
    setups = plan_setups(bars, signals("2025-01-01T21:45Z"), p, p.costs, end)
    result = simulate(bars, setups, p, p.costs, bars.index[0], end)
    assert result.metrics["status"] == "execution_gap"
    assert result.metrics["trade_count"] == 0
    assert result.metrics["open_position"]["deadline_status"] == "breached"
    assert result.equity.units.iloc[-1] > 0
