from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from lab.learning.data import Market
from lab.learning.simulator import Contract, Cost, Risk, run_trade as spot_trade
from lab.phase10.trading import run_trade as futures_trade
from lab.strategy_lab.ema_execution import BracketSetup, run_bracket


START = pd.Timestamp("2024-06-01T00:00:00Z")
MINUTE = pd.Timedelta(minutes=1)
RULES = {"entry_limit_atr": .5, "funding_reserve_bps": 5}
SPOT = Contract("spot", "USDT", 1., .001, .01, .001, 250_000, 1, 0., 0.)
FUTURES = Contract("linear_perp", "USDT", .001, 1., .1, 1., 250_000, 100, .004, .003)
RISK = Risk(10_000, .0025, .10, 1, 5, 0.)


def fixture_market(trade_rows, mark_rows=None, *, funding=None, start=START):
    index = pd.date_range(start, freq=MINUTE, periods=len(trade_rows))
    columns = ("open", "high", "low", "close")
    trade = pd.DataFrame(trade_rows, index=index, columns=columns).assign(volume=1., close_at=index + MINUTE)
    mark = trade if mark_rows is None else pd.DataFrame(mark_rows, index=index, columns=columns)
    market = Market(trade, mark, funding or {}, MINUTE, "CONTROLLED_FIXTURE")
    market.intrabar_funding = {}
    return market


def setup(source=False, direction=1):
    decision = START if source else START - MINUTE
    available = decision if source else decision + pd.Timedelta(seconds=30)
    return BracketSetup(
        decision_at=decision, available_at=available, submit_at=START, expires_at=START + MINUTE,
        deadline=None if source else START + 3 * MINUTE,
        reference=100., atr=2., stop=95.12 if direction == 1 else 104.88,
        direction=direction, target_r=None, trail_atr=None,
        family="ema_pullback_trend_continuation", exit_policy="signal_atr_bracket_v1",
        target_price=105.13 if direction == 1 else 94.87, source_allocation=source,
    )


def replay(market, order, *, executor=spot_trade, contract=SPOT, cost=Cost(.0005, 0.), cutoff=START + 5 * MINUTE):
    return run_bracket(executor, market, order, contract, cost, RISK, 10_000, RULES, cutoff, keep_marks=True)


def test_aseke_open_gap_retains_signal_target_and_risk_model():
    market = fixture_market([(100.8, 105.5, 99., 102.)])
    result = replay(market, setup())
    assert result["status"] == "COMPLETE"
    assert result["entry_at"] == START and result["entry_fill_price"] == 100.8
    assert result["target_price"] == 105.13
    assert result["fills"][0]["trigger_mark"] == result["signal_target_price"] == 105.13
    assert result["risk_budget"] == 25. and result["risk_budget_applies"]
    assert result["target_r"] is None


def test_source_market_entry_uses_equity_allocation_and_fee():
    # 103 is beyond the profile's 101 signal-based IOC limit.
    market = fixture_market([(103., 106., 102., 105.)])
    result = replay(market, setup(source=True))
    assert result["status"] == "COMPLETE"
    assert result["entry_at"] == START and result["entry_fill_price"] == 103.
    assert result["target_price"] == 105.13 and result["deadline"] is None
    assert result["filled_contracts"] == 9.708
    assert result["filled_contracts"] * result["entry_fill_price"] <= 1_000
    assert result["fees"] == pytest.approx((103. + 105.13) * 9.708 * .0005)
    assert result["risk_budget"] is None and not result["risk_budget_applies"]
    assert replay(market, setup())["status"] == "NO_FILL"


def test_source_open_exposure_is_unresolved_at_cutoff_without_future_fill():
    market = fixture_market([(100., 101., 99., 100.), (106., 110., 106., 109.)])
    result = replay(market, setup(source=True), cutoff=START + MINUTE)
    assert result["status"] == "INCOMPLETE"
    assert result["deadline"] is None and result["remaining_base"] == 10.
    assert result["fills"] == [] and "net" not in result
    assert max(stamp for stamp, _, _ in result["marks"]) == START + MINUTE


def test_dual_hit_is_ambiguous_with_conservative_stop():
    market = fixture_market([(100., 106., 94., 101.)])
    result = replay(market, setup(source=True))
    assert result["status"] == "AMBIGUOUS"
    assert result["reason"] == "stop"
    assert result["fills"][0]["trigger_mark"] == 95.12


@pytest.mark.parametrize("open_price", [95.12, 94., 105.13, 106.])
def test_entry_gap_at_or_beyond_bracket_is_rejected(open_price):
    market = fixture_market([(open_price, open_price + 1, open_price - 1, open_price)])
    result = replay(market, setup(source=True))
    assert result["status"] == "REJECTED"
    assert result["reason"] == "entry_gap_or_rounded_price_outside_frozen_bracket"
    assert "entry_at" not in result


def test_entry_adverse_rounding_cannot_cross_target():
    market = fixture_market([(105.12, 106., 104., 105.)])
    result = replay(market, setup(source=True), cost=Cost(.0005, .0001))
    assert result["status"] == "REJECTED" and "entry_at" not in result


def test_missing_open_is_not_replaced_by_next_observed_bar():
    market = fixture_market([(100., 106., 99., 101.)], start=START + MINUTE)
    result = replay(market, setup(source=True))
    assert result["status"] == "INCOMPLETE" and result["reason"] == "missing_bracket_entry_open"


def test_entry_cannot_precede_decision_or_availability():
    market = fixture_market([(100., 106., 99., 101.)])
    invalid = replace(setup(source=True), decision_at=START + MINUTE)
    result = replay(market, invalid)
    assert result["status"] == "REJECTED" and result["reason"] == "invalid_bracket_entry_timing"


def test_futures_short_target_uses_frozen_mark_level_and_trade_exit():
    market = fixture_market(
        [(100., 101., 99., 100.), (100., 102., 98., 100.)],
        [(100., 101., 99., 100.), (100., 101., 94., 96.)],
        funding={START + MINUTE: .001},
    )
    result = replay(market, setup(source=True, direction=-1), executor=futures_trade, contract=FUTURES)
    assert result["status"] == "COMPLETE" and result["reason"] == "target"
    assert result["signal_target_price"] == 94.87 and result["target_price"] == 94.8
    assert result["fills"][0]["trigger_mark"] == 94.8 and result["fills"][0]["raw_trade_price"] == 102.
    assert result["funding"] > 0 and result["settlements"][0]["settlement_at"] == START + MINUTE
    assert result["net"] == pytest.approx(result["gross_realized"] + result["funding"] - result["fees"])


def test_futures_missing_or_outside_mark_open_does_not_enter():
    no_mark = fixture_market([(100., 101., 99., 100.)], [(np.nan,) * 4])
    rejected = replay(no_mark, setup(source=True), executor=futures_trade, contract=FUTURES)
    assert rejected["status"] == "INCOMPLETE" and rejected["reason"] == "missing_or_invalid_bracket_entry_mark"
    gap_mark = fixture_market([(100., 101., 99., 100.)], [(95., 100., 94., 96.)])
    rejected = replay(gap_mark, setup(source=True), executor=futures_trade, contract=FUTURES)
    assert rejected["status"] == "REJECTED" and "entry_at" not in rejected


def test_contract_and_spot_direction_constraints_remain_enforced():
    market = fixture_market([(100., 101., 99., 100.)])
    short_spot = replay(market, setup(source=True, direction=-1))
    assert short_spot["status"] == "REJECTED" and short_spot["reason"] == "spot_long_cash_only"
    tier = replay(market, setup(source=True), contract=replace(SPOT, tier_notional=950.))
    assert tier["status"] == "REJECTED" and tier["reason"] == "source_allocation_exceeds_contract_or_collateral"
