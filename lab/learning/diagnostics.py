from dataclasses import replace

import pandas as pd

from lab.learning.data import Market
from lab.learning.simulator import Contract, Cost, ExecutionAssumptions, Risk, Setup, run_trade


def fixture(settings):
    start = pd.Timestamp("2024-02-01T00:00Z")
    step = pd.Timedelta(minutes=1)
    grid = pd.date_range(start, periods=13, freq=step)
    # No actual venue, trade, or account data is used.
    trade = pd.DataFrame({"open": 100., "high": 100.5, "low": 99.5, "close": 100., "volume": 20.,
                          "close_at": grid + step}, index=grid)
    trade.loc[grid[7], ["open", "high", "low", "close"]] = [100, 105, 99.8, 104]
    trade.loc[grid[8]:, ["open", "high", "low", "close"]] = [104, 104.5, 103.5, 104]
    mark = trade.copy()
    funding = {grid[4]: .0004, grid[10]: -.0001}
    setup = Setup(grid[0], grid[0] + pd.Timedelta(seconds=30), grid[1], grid[2], grid[9], 100., 1.,
                  98., 1, 1.5, None, "synthetic_path", "fixed_r_v1")
    contract = Contract.load(settings["instruments"]["linear_usdt_perp_v1"])
    risk = Risk(**settings["risk"]["perp_equal_risk_v1"])
    cost = Cost.load(settings["costs"]["perp_published_api_v1"])
    return trade, mark, funding, setup, contract, risk, cost


def diagnostics(settings):
    trade, mark, funding, setup, contract, risk, cost = fixture(settings)
    rules, step = settings["execution"], pd.Timedelta(minutes=1)
    cutoff = trade.index[-1] + 2 * step
    grid = trade.index

    def run(prices=trade, fair=mark, settlements=funding, plan=setup, policy=risk, costs=cost,
            assumptions=ExecutionAssumptions(), mode="ordinary"):
        market = Market(prices, fair, settlements, step, "SIMULATOR_DIAGNOSTIC")
        return run_trade(market, plan, contract, costs, policy, policy.starting_equity, rules, cutoff,
                         assumptions=assumptions, evaluation_mode=mode)

    base = run()
    if base["status"] != "COMPLETE" or base["funding"] >= 0 or base["initial_margin"] <= 0:
        raise ValueError("Futures end-to-end accounting failed")
    if abs(base["net"] - (base["gross_realized"] + base["funding"] - base["fees"])) > 1e-8:
        raise ValueError("Isolated margin was incorrectly treated as an expense")
    adverse = mark.copy()
    adverse.loc[grid[5], ["open", "high", "low", "close"]] = [95, 100, 90, 95]
    gapped_trade = trade.copy()
    gapped_trade.loc[grid[5], ["open", "high", "low", "close"]] = [96, 97, 95, 96]
    delay = replace(setup, submit_at=setup.submit_at + step, expires_at=setup.expires_at + step)
    stresses = {
        "double_cost": run(costs=Cost(cost.fee * 2, cost.adverse * 2)),
        "delayed_entry_exit": run(plan=delay, assumptions=ExecutionAssumptions(delay_exit_bars=1)),
        "gap": run(prices=gapped_trade, fair=adverse),
        "adverse_funding": run(settlements={grid[4]: .02}),
        "fair_last_divergence": run(fair=adverse),
        "partial_rejected": {"partial": run(assumptions=ExecutionAssumptions(entry_fraction=.5, exit_fraction=.5)),
                             "rejected": run(assumptions=ExecutionAssumptions(entry_fraction=0))},
        "outage": run(prices=trade.drop(index=grid[4])),
    }
    leverage = {str(value): run(policy=replace(risk, leverage=value), mode="stress")
                for value in rules["leverage_stress"]}
    short_trade = 200 - trade[["open", "close"]]
    short_trade["high"] = 200 - trade.low
    short_trade["low"] = 200 - trade.high
    short_trade[["volume", "close_at"]] = trade[["volume", "close_at"]]
    short = run(prices=short_trade, fair=short_trade,
                plan=replace(setup, direction=-1, stop=102.))
    if short["status"] != "COMPLETE" or short["funding"] <= 0:
        raise ValueError("Short funding sign or synthetic path failed")
    return {"scope": "SIMULATOR_DIAGNOSTIC", "instrument": settings["instruments"]["linear_usdt_perp_v1"],
            "ordinary_long": base, "ordinary_short": short, "stress_grid": leverage, "stresses": stresses,
            "unsupported": ["cross margin", "inverse contracts", "hedge mode", "automatic margin additions",
                            "venue-tier transitions", "partial liquidation", "full ADL", "actual IOC liquidity",
                            "exact event order inferred from OHLC"],
            "ranking": "Leverage/stress results provide no alpha ranking or promotion eligibility."}
