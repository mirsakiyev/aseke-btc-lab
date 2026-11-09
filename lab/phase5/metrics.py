import numpy as np
import pandas as pd

from lab.backtest import drawdown
from lab.config import Execution
from lab.execution import adverse
from lab.phase2.audit import distribution
from lab.phase3.portfolio import BENCHMARKS, execution_events, period_metrics
from lab.phase5.design import ANNUAL_DAYS
from lab.storage import utc


def calendar_years(start: pd.Timestamp, end: pd.Timestamp):
    while start < end:
        right = min(end, utc(f"{start.year + 1}-01-01"))
        yield str(start.year), start, right
        start = right


def geometric(net_return: float, start: pd.Timestamp, end: pd.Timestamp, full: bool) -> float | None:
    elapsed_days = (end - start) / pd.Timedelta("1d")
    return float((1 + net_return) ** (ANNUAL_DAYS / elapsed_days) - 1) if full and elapsed_days > 0 else None


def benchmark_metrics(sim, costs: Execution, sizing: Execution, opening_price: float, initial: float,
                      account_start: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp, annual: bool) -> dict:
    sample = sim.equity.loc[(sim.equity.index > start) & (sim.equity.index <= end)]
    prior = sim.equity.loc[sim.equity.index <= start]
    entry_fill = adverse(opening_price, costs, buy=True)
    out = {}
    for name, allocation in zip(BENCHMARKS, (0, 1, .25)):
        entry_notional = initial * allocation / (1 + sizing.fee)
        units = entry_notional / entry_fill
        hold_cash = initial - entry_notional * (1 + costs.fee)
        beginning = float(prior[name].iloc[-1]) if len(prior) else initial
        ending = float(sample[name].iloc[-1])
        fee = entry_notional * costs.fee if start <= account_start < end else 0.
        drag = units * (entry_fill - opening_price) if start <= account_start < end else 0.
        weights = (sample[name] - hold_cash) / sample[name]
        net = ending - beginning
        returns = ending / beginning - 1
        out[name] = {
            "start_mark": prior.index[-1] if len(prior) else start, "end_mark": sample.index[-1],
            "start_usd": beginning, "end_usd": ending, "net_pnl_usd": net, "net_return": returns,
            "geometric_annualized_return": geometric(returns, start, end, annual),
            "gross_usd": net + fee + drag, "fees_usd": fee, "adverse_execution_usd": drag,
            "observed_drawdown": float(drawdown(sample[name], beginning).min()),
            "mean_btc_weight": float(weights.mean()), "max_btc_weight": float(weights.max()),
            "observed_time_exposure": float(allocation > 0), "btc_units": units,
            "entry_notional_usd": entry_notional if start <= account_start < end else 0.,
        }
    return out


def measure(sim, events, costs, sizing, opening_price, initial, step, account_start, start, end, *, annual=False):
    row = period_metrics(sim, events, start, end, initial, step)
    if row is None:
        return None
    marks = sim.equity.loc[(sim.equity.index > start) & (sim.equity.index <= end)]
    buys = events.loc[events.side.eq("buy") & (events["at"] >= start) & (events["at"] < row["end_mark"])]
    row.update(
        requested_start_utc=start, requested_end_utc=end,
        elapsed_utc_days=(row["end_mark"] - start) / pd.Timedelta("1d"),
        peak_btc_weight_on_hourly_marks=float(marks.btc_weight.max()),
        entry_notional_usd=distribution(buys.notional),
        annualized_net_return=geometric(row["net_return"], start, row["end_mark"], annual and row["window_complete"]),
        benchmarks=benchmark_metrics(sim, costs, sizing, opening_price, initial, account_start, start,
                                     row["end_mark"], annual and row["window_complete"]),
    )
    return row


def concentration(sim, end: pd.Timestamp, start: pd.Timestamp) -> dict:
    trades = sim.trades
    closed = trades.loc[trades.exit_at.notna() & (trades.entry_at >= start) & (trades.exit_bar_open < end)] if len(trades) else trades
    net = closed.net_usd if len(closed) else pd.Series(dtype=float)
    winners, losses = net.loc[net > 0], -net.loc[net < 0]
    total = float(net.sum())
    win_total = float(winners.sum())
    return {
        "closed_trades": len(net), "realized_closed_net_usd": total,
        "trade_net_usd": distribution(net), "win_magnitude_usd": distribution(winners),
        "loss_magnitude_usd": distribution(losses),
        "top_winners": {str(n): {
            "count": min(n, len(winners)), "contribution_usd": float(winners.nlargest(n).sum()),
            "share_of_all_positive_trade_pnl": float(winners.nlargest(n).sum() / win_total) if win_total else None,
            "closed_net_minus_winners_arithmetic_only_usd": total - float(winners.nlargest(n).sum()),
        } for n in (1, 3, 5)},
        "method": "Closed-trade USD only. Subtracting winners holds all other quantities fixed and is not a replay. "
                  "No independence, power, or win-probability inference.",
    }


def comparison(accounts, manifest, opening_price, profile):
    start, end = (utc(manifest[key]) for key in ("requested_start_utc", "requested_end_utc"))
    common = min(end, *(sim.equity.index[-1] for sim in accounts.values()))
    full = common == end
    if not all(sim.equity.index[0] == start + profile.step for sim in accounts.values()):
        raise ValueError("Unexpected first observed mark or benchmark alignment")
    folds = []
    for window in manifest["frozen_windows"]:
        left, right = (utc(window[key]) for key in ("evaluation_start_utc", "evaluation_end_exclusive_utc"))
        state = "insufficient_original_coverage" if not window["entry_eligible"] else (
            "unavailable_after_common_mark" if left >= common else ("partial_valid" if common < right else "complete")
        )
        folds.append({"id": window["id"], "left": left, "right": right, "state": state, "methods": {}})
    years = [{"calendar_year_utc": key, "left": left, "right": right, "methods": {}}
             for key, left, right in calendar_years(start, common)]
    methods = {}
    expected = pd.date_range(start + profile.step, common, freq=profile.step)
    for key, sim in accounts.items():
        costs = Execution(**manifest["costs"][key])
        sizing = Execution(**manifest["costs"]["reference"]) if key == "fixed_ledger" else costs
        events = execution_events(sim.trades, costs)
        observed = sim.equity.index[(sim.equity.index > start) & (sim.equity.index <= common)]
        missing = expected.difference(observed)

        def measure_span(left, right, annual=False):
            return measure(sim, events, costs, sizing, opening_price, profile.starting_cash, profile.step,
                           start, left, right, annual=annual)

        aggregate = measure_span(start, common, annual=full)
        for period in [*folds, *years]:
            if period.get("state") not in ("insufficient_original_coverage", "unavailable_after_common_mark"):
                period["methods"][key] = measure_span(period["left"], min(period["right"], common))
        methods[key] = {
            "execution": costs.model_dump(), "aggregate": aggregate, "engine_end_mark": sim.equity.index[-1],
            "engine_status": sim.metrics["status"], "open_or_unresolved_at_engine_end": sim.metrics.get("open_position"),
            "missing_hourly_mark_count": len(missing), "missing_hourly_marks": list(missing),
            "closed_trade_concentration": concentration(sim, common, start),
        }
        months = [r["methods"][key]["net_pnl_usd"] for r in years]
        np.testing.assert_allclose(sum(months), aggregate["net_pnl_usd"], atol=1e-8)
    return {
        "requested_start_utc": start, "requested_end_utc": end, "matched_end_utc": common,
        "elapsed_utc_days": (common - start) / pd.Timedelta("1d"), "full_frozen_continuous_span": full,
        "requested_utc_days": (end - start) / pd.Timedelta("1d"), "expected_hourly_marks": len(expected),
        "mark_frequency": "Hourly closes. Gaps remain unfilled and intrabar extremes are outside the drawdown measurement.",
        "methods": methods, "evaluation_windows": folds, "calendar_years": years,
    }
