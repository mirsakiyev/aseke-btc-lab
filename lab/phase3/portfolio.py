from itertools import combinations
from pathlib import Path

import pandas as pd

from lab.backtest import drawdown, simulate
from lab.config import Execution, Profile
from lab.execution import plan_setups
from lab.phase2.audit import distribution
from lab.phase3.design import METHODS
from lab.storage import utc

BENCHMARKS = ("cash_benchmark", "full_btc", "passive_25pct")


def execution_events(trades: pd.DataFrame, costs: Execution) -> pd.DataFrame:
    columns = ["signal_at", "at", "side", "notional", "fee", "drag", "raw_price", "adverse_price"]
    rows = []
    for trade in trades.itertuples():
        raw_buy = trade.entry_price / (1 + costs.proxy)
        rows.append((trade.signal_at, trade.entry_at, "buy", trade.entry_notional, trade.entry_notional * costs.fee,
                     (trade.entry_price - raw_buy) * trade.units_btc, raw_buy, trade.entry_price))
        if pd.notna(trade.exit_at):
            raw_sell = trade.exit_price / (1 - costs.proxy)
            rows.append((trade.signal_at, trade.exit_bar_open, "sell", trade.exit_notional,
                         trade.exit_notional * costs.fee, (raw_sell - trade.exit_price) * trade.units_btc,
                         raw_sell, trade.exit_price))
    events = pd.DataFrame(rows, columns=columns)
    events["at"] = pd.to_datetime(events["at"], utc=True)
    return events


def fixed_ledger_fee(gross: float, drag: float, notional: float) -> dict:
    budget = gross - drag
    state = "no_traded_notional" if notional <= 0 else ("no_feasible_nonnegative_fee" if budget < 0 else "estimated")
    return {
        "status": state, "net_before_fees_usd": budget, "traded_adverse_notional_usd": notional,
        "break_even_fee_bps_per_side": 10000 * budget / notional if state == "estimated" else None,
        "method": "Fixed traded units, fills and adverse price proxy. Mark open BTC at observed closes.",
    }


def period_metrics(sim, events, start, end, initial, step) -> dict | None:
    equity = sim.equity.loc[(sim.equity.index > start) & (sim.equity.index <= end)]
    if equity.empty:
        return None
    actual_end = equity.index[-1]
    earlier = sim.equity.loc[sim.equity.index <= start]
    starting = earlier.iloc[-1] if len(earlier) else pd.Series(
        {**{name: initial for name in ("equity", *BENCHMARKS)}, "units": 0}
    )
    trades = sim.trades
    fills = events.loc[(events["at"] >= start) & (events["at"] < actual_end)]
    cash_change = float(equity.equity.iloc[-1] - starting.equity)
    fees, drag, notional = (float(fills[name].sum()) for name in ("fee", "drag", "notional"))
    gross = cash_change + fees + drag
    if len(trades):
        opened = trades.loc[(trades.entry_at >= start) & (trades.entry_at < actual_end)]
        closed = trades.loc[trades.exit_at.notna() & (trades.exit_bar_open >= start) & (trades.exit_bar_open < actual_end)]
        intervals = trades.loc[(trades.entry_at < actual_end) & (trades.exit_at.isna() | (trades.exit_at > start))]
        held_minutes = sum((min(utc(t.exit_at), actual_end) - max(t.entry_at, start)).total_seconds() / 60
                           if pd.notna(t.exit_at) else (actual_end - max(t.entry_at, start)).total_seconds() / 60
                           for t in intervals.itertuples())
        remaining = trades.loc[(trades.entry_at < actual_end)
                              & (trades.exit_at.isna() | (trades.exit_bar_open >= actual_end))]
    else:
        opened = closed = remaining = trades
        held_minutes = 0
    open_usd = 0.0
    if len(remaining):
        if len(remaining) != 1 or equity.units.iloc[-1] <= 0:
            raise ValueError("Fold mark does not reconcile with the carried position")
        lot = remaining.iloc[0]
        open_usd = float(equity.equity.iloc[-1] - equity.cash.iloc[-1] - lot.entry_notional * (1 + lot.fee_bps / 10000))
    return {
        "start_mark": earlier.index[-1] if len(earlier) else start, "end_mark": actual_end,
        "window_complete": actual_end == end, "observed_hourly_marks": len(equity),
        "starting_equity_usd": float(starting.equity), "ending_equity_usd": float(equity.equity.iloc[-1]),
        "net_pnl_usd": cash_change, "net_return": float(equity.equity.iloc[-1] / starting.equity - 1),
        "gross_marked_same_trades_usd": gross, "fees_usd": fees, "adverse_execution_drag_usd": drag,
        "drawdown_on_observed_marks": float(drawdown(equity.equity, float(starting.equity)).min()),
        "mean_btc_weight": float(equity.btc_weight.mean()),
        "observed_time_exposure": float(held_minutes / (len(equity) * step.total_seconds() / 60)),
        "turnover_notional_usd": notional, "turnover_ratio_mean_equity": float(notional / equity.equity.mean()),
        "positions_carried_in": int(starting.units > 0), "positions_opened": len(opened), "positions_closed": len(closed),
        "positions_carried_out": len(remaining), "unrealized_pnl_at_end_usd": open_usd,
        "closed_holding_minutes": distribution(closed.holding_minutes) if len(closed) else {"count": 0},
        "benchmark_net_returns": {name: float(equity[name].iloc[-1] / starting[name] - 1) for name in BENCHMARKS},
        "fixed_ledger_fee": fixed_ledger_fee(gross, drag, notional),
    }


def annotate_trades(sim, predictions, method):
    if sim.trades.empty:
        return
    journal = sim.trades
    fold_field = predictions.loc[journal.signal_at, "fold"].to_numpy()
    journal["signal_fold"] = fold_field
    if method != "baseline":
        journal["model_version"] = predictions.loc[journal.signal_at, f"{method}_model_id"].to_numpy()
    if sim.metrics["open_position"]:
        sim.metrics["open_position"]["signal_fold"] = fold_field[-1]
        sim.metrics["open_position"]["model_version"] = journal.model_version.iloc[-1]


def overlap(simulations, start, end):
    identities = {}
    for name, sim in simulations.items():
        trade = sim.trades
        identities[name] = set(trade.loc[(trade.entry_at >= start) & (trade.entry_at < end), "signal_at"]) if len(trade) else set()
    return {
        f"{a}_vs_{b}": {"shared_signal_identities": len(identities[a] & identities[b]),
                       "only_left": sorted(identities[a] - identities[b]), "only_right": sorted(identities[b] - identities[a])}
        for a, b in combinations(METHODS, 2)
    }


def run_scenario(candles, signals, predictions, folds, profile: Profile, costs: Execution,
                 destination: Path, scenario: str) -> tuple[dict, dict]:
    start = utc(next(fold["evaluation_start_utc"] for fold in folds if fold["fit_state"] == "fitted"))
    end = utc(folds[-1]["evaluation_end_exclusive_utc"])
    setup_by_time = {item.signal_at: item for item in plan_setups(candles, signals, profile, costs, end)}
    eligible = predictions.loc[predictions.fit_eligible & (predictions.index >= start)]
    setups = [setup_by_time[ts] for ts in eligible.index]
    destination.mkdir()
    result, simulations, cash_events = {}, {}, {}
    for method in METHODS:
        scores = None if method == "baseline" else eligible[f"{method}_score"]
        account = simulate(candles, setups, profile, costs, start, end, scores)
        annotate_trades(account, predictions, method)
        events = execution_events(account.trades, costs)
        if abs(float(events.fee.sum()) - account.metrics["fees_usd"]) > 1e-7:
            raise ValueError("Execution-side fees do not reconcile with the original engine")
        account.trades.to_parquet(destination / f"{method}-trades.parquet", index=False)
        account.equity.to_parquet(destination / f"{method}-equity.parquet")
        events.to_parquet(destination / f"{method}-execution-events.parquet", index=False)
        cash_events[method], simulations[method] = events, account
    matched_end = min(account.equity.index[-1] for account in simulations.values())
    for method, sim in simulations.items():
        events = cash_events[method]
        matched = period_metrics(sim, events, start, matched_end, profile.starting_cash, profile.step)
        if matched:
            matched["window_complete"] = matched_end == end
        result[method] = {
            "observed_engine_status": sim.metrics["status"], "account_last_valid_mark": sim.equity.index[-1],
            "account_open_or_unresolved_position": sim.metrics["open_position"], "matched": matched,
            "benchmark_exposure_and_total_costs_at_account_end": sim.metrics["benchmarks"],
            "full_period_drawdown_available": bool(matched_end == end and sim.metrics["valid_through_last_mark"]),
        }
    periods = {}
    for fold in folds:
        left = utc(fold["evaluation_start_utc"])
        right = utc(fold["evaluation_end_exclusive_utc"])
        if left < start or left >= matched_end:
            periods[fold["id"]] = {"account_state": "no_comparable_account_marks"}
            continue
        matched_right = min(right, matched_end)
        metrics = {name: period_metrics(simulations[name], cash_events[name], left, matched_right,
                                       profile.starting_cash, profile.step) for name in METHODS}
        for row in metrics.values():
            if row:
                row["window_complete"] = matched_right == right
        periods[fold["id"]] = {"account_state": "complete" if matched_right == right else "truncated",
                               "methods": metrics, "trade_overlap": overlap(simulations, left, matched_right)}
    funnel = predictions[["fold", "fit_eligible", "ridge_accepted", "mean_accepted"]].copy()
    funnel["opening"] = [setup_by_time[ts].entry_at for ts in funnel.index]
    funnel["execution_setup_status"] = [setup_by_time[ts].reason for ts in funnel.index]
    for method, sim in simulations.items():
        actual_ids = sim.trades.signal_at if len(sim.trades) else []
        funnel[f"{method}_filled"] = funnel.index.isin(actual_ids)
    funnel.to_parquet(destination / "execution-comparison.parquet")
    return {
        "scenario": scenario, "costs": costs.model_dump(), "training_costs": profile.costs.model_dump(),
        "research_sensitivity": scenario != "reference", "approved_configuration": False,
        "account_start": start, "requested_end": end, "matched_end": matched_end,
        "valid_full_period": matched_end == end, "methods": result, "folds": periods,
        "matched_trade_overlap": overlap(simulations, start, matched_end),
    }, simulations
