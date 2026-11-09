from dataclasses import dataclass

import numpy as np
import pandas as pd

from lab.broker import Fill, replay
from lab.config import Execution, Profile
from lab.execution import Setup, adverse


@dataclass
class Simulation:
    metrics: dict
    equity: pd.DataFrame
    trades: pd.DataFrame
    monthly: pd.DataFrame


def size_order(setup: Setup, profile: Profile, costs: Execution, equity: float) -> tuple[float, float]:
    modeled_entry = adverse(setup.decision_close, costs, buy=True)
    modeled_stop = adverse(setup.stop, costs, buy=False)
    per_unit_risk = modeled_entry * (1 + costs.fee) - modeled_stop * (1 - costs.fee)
    units = min(equity * profile.risk / per_unit_risk, equity * profile.allocation / modeled_entry,
                equity / (modeled_entry * (1 + costs.fee)))
    return units, units * per_unit_risk


def drawdown(equity: pd.Series, starting_cash: float) -> pd.Series:
    return equity / equity.cummax().clip(lower=starting_cash) - 1


def benchmark(candles: pd.DataFrame, initial: float, costs: Execution,
              allocation: float) -> tuple[np.ndarray, dict]:
    entry = adverse(float(candles.open.iloc[0]), costs, buy=True)
    units = initial * allocation / (entry * (1 + costs.fee))
    notional = units * entry
    fee = notional * costs.fee
    cash = initial - notional - fee
    values = candles.close.to_numpy() * units
    equity = cash + values
    return equity, {
        "net_return": float(equity[-1] / initial - 1),
        "mean_btc_weight": float(np.mean(values / equity)),
        "time_exposure_observed": float(allocation > 0),
        "fees_usd": fee, "turnover": float(notional / np.mean(equity)),
        "max_drawdown": float(drawdown(pd.Series(equity), initial).min()),
    }


def simulate(candles: pd.DataFrame, setups: list[Setup], profile: Profile, costs: Execution,
             start: pd.Timestamp, end: pd.Timestamp, predictions: pd.Series | None = None,
             model_version: str | None = None) -> Simulation:
    candles = candles.loc[(candles.index >= start) & (candles.close_at <= end)]
    if candles.empty:
        raise ValueError("No execution bars in evaluation window")
    cash = profile.starting_cash
    fills = []
    journal = []
    pending_through = None
    halt_at = None
    rejected_cash = 0
    missing_entry = 0
    for setup in setups:
        if setup.signal_at < start or setup.entry_at >= end:
            continue
        if pending_through is not None and setup.available_at <= pending_through:
            continue
        if predictions is not None and not predictions.get(setup.signal_at, np.nan) > 0:
            continue
        if setup.entry_raw is None:
            if setup.reason == "missing_entry_bar":
                missing_entry += 1
                pending_through = setup.entry_at
            continue
        units, planned_loss = size_order(setup, profile, costs, cash)
        entry = adverse(setup.entry_raw, costs, buy=True)
        buy_notional = units * entry
        buy_fee = buy_notional * costs.fee
        if buy_notional + buy_fee > cash:
            rejected_cash += 1
            pending_through = setup.entry_at
            continue
        exit_price = adverse(setup.exit_raw, costs, buy=False) if setup.exit_raw is not None else None
        sell_notional = exit_price * units if exit_price is not None else 0
        sell_fee = sell_notional * costs.fee
        net = sell_notional - sell_fee - buy_notional - buy_fee if exit_price is not None else None
        modeled_stop = adverse(setup.stop, costs, buy=False)
        fill_stop_risk = buy_notional + buy_fee - units * modeled_stop * (1 - costs.fee)
        fills.append(Fill(setup.entry_at, units, entry))
        if exit_price is not None:
            fills.append(Fill(setup.exit_bar_open, -units, exit_price))
        last_observed = setup.exit_at if exit_price is not None else setup.observed_through
        journal.append({
            "profile": profile.id, "profile_version": profile.version, "profile_hash": profile.identity,
            "strategy_version": profile.strategy_version, "model_version": model_version,
            "signal_at": setup.signal_at, "available_at": setup.available_at, "entry_at": setup.entry_at,
            "exit_at": setup.exit_at, "exit_bar_open": setup.exit_bar_open, "deadline": setup.deadline,
            "max_minutes": profile.max_minutes, "session_flat_utc": (
                profile.session.flat_minute_utc if profile.session else None
            ),
            "exit_range": profile.exit_range, "fee_bps": costs.fee_bps, "proxy_bps": costs.proxy_bps,
            "delay_bars": costs.delay_bars, "processing_seconds": costs.processing_seconds,
            "reason": setup.reason, "stop": setup.stop, "entry_price": entry, "exit_price": exit_price,
            "units_btc": units, "entry_notional": buy_notional, "exit_notional": sell_notional,
            "fees_usd": buy_fee + sell_fee, "net_usd": net, "planned_risk_usd": planned_loss,
            "risk_budget_usd": cash * profile.risk, "fill_to_stop_risk_usd": fill_stop_risk,
            "fill_risk_overrun_usd": max(0, fill_stop_risk - cash * profile.risk),
            "realized_loss_overrun_usd": max(0, -net - cash * profile.risk) if net is not None else None,
            "allocation_overrun_usd": max(0, buy_notional - cash * profile.allocation),
            "holding_minutes": (last_observed - setup.entry_at) / pd.Timedelta("1min"),
            "deadline_status": (
                "met" if exit_price is not None and last_observed <= setup.deadline else
                "breached" if last_observed >= setup.deadline else "unresolved"
            ),
        })
        cash += sell_notional - sell_fee - buy_notional - buy_fee
        if exit_price is None:
            halt_at = last_observed
            break
        pending_through = setup.exit_at

    if halt_at is not None:
        candles = candles.loc[candles.index < halt_at]
    equity = replay(candles, fills, profile.starting_cash, costs.fee)
    if not np.isclose(equity.cash.iloc[-1], cash, rtol=1e-10, atol=1e-8):
        raise ValueError("VectorBT cash does not reconcile with the order planner")
    equity["drawdown"] = drawdown(equity.equity, profile.starting_cash)
    equity["btc_weight"] = equity.units.to_numpy() * candles.close.to_numpy() / equity.equity
    benchmarks = {}
    for name, weight in (("cash_benchmark", 0), ("full_btc", 1), ("passive_25pct", 0.25)):
        equity[name], benchmarks[name] = benchmark(candles, profile.starting_cash, costs, weight)
    trades = pd.DataFrame(journal)
    closed = trades[trades.exit_at.notna()] if len(trades) else trades
    profits = closed.net_usd if len(closed) else pd.Series(dtype=float)
    open_trade = journal[-1] if halt_at is not None else None
    average = lambda values: float(values.mean()) if len(values) else None
    months = equity.index.strftime("%Y-%m").rename("month_utc")
    monthly_marks = equity[["equity", *benchmarks]].groupby(months).last()
    monthly = monthly_marks.div(monthly_marks.shift().fillna(profile.starting_cash)).sub(1)
    monthly["marked_through_utc"] = equity.index.to_series().groupby(months).last()
    metrics = {
        "evaluation_start": candles.index[0], "evaluation_end": equity.index[-1],
        "requested_end": end, "starting_cash_usd": profile.starting_cash,
        "ending_equity_usd": float(equity.equity.iloc[-1]),
        "net_return": float(equity.equity.iloc[-1] / profile.starting_cash - 1),
        "max_drawdown": float(equity.drawdown.min()),
        "mean_btc_weight": float(equity.btc_weight.mean()),
        "time_exposure_observed": sum(item["holding_minutes"] for item in journal)
        / (len(candles) * profile.step.total_seconds() / 60),
        "trade_count": len(closed), "entries": len(trades), "rejected_for_cash": rejected_cash,
        "missing_entry_bars": missing_entry,
        "fees_usd": sum(item["fees_usd"] for item in journal),
        "turnover": float(sum(item["entry_notional"] + item["exit_notional"] for item in journal)
                          / equity.equity.mean()),
        "net_expectancy_usd": average(profits),
        "net_expectancy_on_entry_notional": average(
            profits / closed.entry_notional if len(closed) else profits
        ),
        "average_win_usd": average(profits[profits > 0]), "average_loss_usd": average(profits[profits < 0]),
        "mean_closed_holding_minutes": average(closed.holding_minutes) if len(closed) else None,
        "open_position": open_trade,
        "unrealized_usd": (float(open_trade["units_btc"] * candles.close.iloc[-1]
                                 - open_trade["entry_notional"] - open_trade["fees_usd"])
                           if open_trade is not None else 0),
        "valid_through_last_mark": not (open_trade and open_trade["reason"] == "execution_gap"),
        "status": open_trade["reason"] if open_trade else "complete",
        "benchmarks": benchmarks,
    }
    return Simulation(metrics, equity, trades, monthly)
