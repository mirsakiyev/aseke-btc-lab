from collections import Counter
from types import SimpleNamespace

import numpy as np
import pandas as pd

from lab.config import Execution, Profile
from lab.execution import execution_open, known_exit
from lab.features import decisions
from lab.phase3.design import development_prices
from lab.phase5.sizing import audit_ledger
from lab.storage import read_json, utc

EPSILON = 1e-8


def loss_bridge(row, costs):
    qty = row["final_quantity_btc"]
    reference, stop = row["observable_signal_close_usd"], row["raw_protective_stop_usd"]
    buy, sell = row["entry_candle_raw_open_usd"], row["exit_raw_usd"]
    planned_buy, planned_sell = row["estimated_adverse_entry_usd_per_btc"], row["estimated_adverse_stop_usd_per_btc"]
    planned_entry_fee, planned_exit_fee = planned_buy * qty * costs.fee, planned_sell * qty * costs.fee
    budget = row["planned_risk_budget_usd"]
    bridge = {
        "sizing_slack_usd": row["planned_cost_inclusive_risk_usd"] - budget,
        "entry_price_movement_usd": (buy - reference) * qty,
        "stop_to_exit_price_movement_usd": (stop - sell) * qty,
        "fee_delta_usd": row["entry_fee_usd"] + row["exit_fee_usd"] - planned_entry_fee - planned_exit_fee,
        "adverse_delta_usd": ((buy + sell) - (reference + stop)) * costs.proxy * qty,
    }
    excess = -row["net_usd"] - budget
    np.testing.assert_allclose(sum(bridge.values()), excess, atol=EPSILON, rtol=0)
    overrun = max(0., excess)
    cause = "within_budget"
    if overrun > EPSILON:
        entry = bridge["entry_price_movement_usd"] > EPSILON
        stop_gap = row["exit_reason"] == "stop_open" and bridge["stop_to_exit_price_movement_usd"] > EPSILON
        cause = {(True, True): "adverse_entry_and_stop_gap", (True, False): "adverse_entry_only",
                 (False, True): "stop_gap_only", (False, False): "other_requires_review"}[entry, stop_gap]
    return {**bridge, "planned_entry_fee_usd": planned_entry_fee, "planned_exit_fee_usd": planned_exit_fee,
            "modeled_entry_adverse_usd": (row["entry_after_proxy_usd"] - buy) * qty,
            "modeled_exit_adverse_usd": (sell - row["exit_after_proxy_usd"]) * qty,
            "overrun_usd": overrun, "realized_loss_in_budgets": max(0., -row["net_usd"]) / budget, "cause": cause}


def exit_proof(trade, candles, scheduled_signals, profile, costs):
    entry, exit_bar = trade.entry_at, trade.exit_bar_open
    deadline, scheduled, reason = known_exit(entry, profile, costs)
    earliest_signal = scheduled_signals.searchsorted(entry)
    if earliest_signal < len(scheduled_signals) and scheduled_signals[earliest_signal] < scheduled:
        scheduled, reason = scheduled_signals[earliest_signal], "signal"
    bars = candles.loc[entry:exit_bar]
    if not bars.index.equals(pd.date_range(entry, exit_bar, freq=profile.step)):
        raise ValueError(f"Unobserved bar in closed trade at {entry}")
    if not (bars.low.iloc[:-1] > trade.stop).all() or exit_bar > scheduled:
        raise ValueError(f"Earlier exit was overlooked at {entry}")
    bar = bars.iloc[-1]
    if bar.open <= trade.stop:
        expected, at, interval = "stop_open", exit_bar, [exit_bar, exit_bar]
    elif exit_bar == scheduled:
        expected, at, interval = reason, exit_bar, None
    elif bar.low <= trade.stop:
        expected, at, interval = "stop_intrabar", bar.close_at, [exit_bar, bar.close_at]
    else:
        raise ValueError(f"Exit has no observed trigger at {exit_bar}")
    already_beyond = bool(bars.open.iloc[0] <= trade.stop)
    if ((trade.reason, trade.exit_at, trade.deadline) != (expected, at, deadline)
            or (already_beyond and (expected != "stop_open" or at != entry))):
        raise ValueError(f"Frozen stop or exit precedence mismatch at {entry}")
    return {"exit_bar_open": exit_bar, "precommitted_deadline": deadline, "earliest_scheduled_exit": scheduled,
            "scheduled_exit_reason": reason, "stop_trigger_utc_bounds": interval,
            "intrabar_order_and_exact_trigger_unknown": expected == "stop_intrabar",
            "entry_open_at_or_below_stop": already_beyond, "earlier_complete_bars_checked": len(bars) - 1,
            "entry_bar_ohlcv": bars.iloc[0].to_dict(), "exit_bar_ohlcv": bar.to_dict()}


def reconcile(manifest, plan, phase4, phase5):
    profile = Profile(**manifest["frozen_profile"])
    costs = Execution(**manifest["costs"])
    candles = development_prices(plan)
    signals = decisions(candles, profile)
    trades = pd.read_parquet(phase4 / "execution_replay-trades.parquet")
    equity = pd.read_parquet(phase4 / "execution_replay-equity.parquet")
    if (len(trades) != 68 or trades.exit_at.isna().any() or trades.exit_at.max() > utc(manifest["historical_evaluation_end_utc"])
            or trades.signal_at.min() < utc(manifest["historical_evaluation_start_utc"])):
        raise ValueError("Unexpected lower-fee replay ledger bounds")
    audited, prior = audit_ledger(SimpleNamespace(trades=trades, equity=equity), candles, signals, costs, profile)
    processing = pd.Timedelta(seconds=costs.processing_seconds)
    sell_opens = pd.DatetimeIndex([execution_open(close + processing, profile.step, costs.delay_bars)
                                  for close in signals.index[signals.exit]])
    compact, details, losers, winners = [], [], [], []
    for trade in trades.itertuples():
        planned = audited[trade.signal_at]
        available = trade.signal_at + processing
        intended = execution_open(available, profile.step, costs.delay_bars)
        if (not signals.loc[trade.signal_at, "enter"] or trade.available_at != available
                or trade.entry_at != intended or trade.profile_hash != profile.identity
                or trade.model_version is not None
                or (trade.fee_bps, trade.proxy_bps, trade.processing_seconds, trade.delay_bars)
                != (costs.fee_bps, costs.proxy_bps, costs.processing_seconds, costs.delay_bars)):
            raise ValueError(f"Ledger identity or causal sizing mismatch at {trade.signal_at}")
        proof = exit_proof(trade, candles, sell_opens, profile, costs)
        bridge = loss_bridge(planned, costs)
        np.testing.assert_allclose(bridge["overrun_usd"], trade.realized_loss_overrun_usd, atol=EPSILON)
        trigger = proof["stop_trigger_utc_bounds"] or (None, None)
        keys = ("signal_at", "information_cutoff", "hypothetical_entry_at", "hypothetical_exit_at",
                "decision_equity_usd", "observable_signal_close_usd", "raw_protective_stop_usd",
                "estimated_adverse_entry_usd_per_btc", "estimated_adverse_stop_usd_per_btc",
                "planned_risk_budget_usd", "planned_cost_inclusive_risk_usd", "planned_cost_inclusive_loss_per_btc_usd",
                "uncapped_risk_quantity_btc", "cash_cap_quantity_btc", "exposure_cap_quantity_btc",
                "maximum_cash_outlay_usd", "planned_entry_notional_cap_usd", "rounding",
                "final_quantity_btc", "binding_constraint", "entry_candle_raw_open_usd", "exit_raw_usd",
                "entry_after_proxy_usd", "exit_after_proxy_usd", "entry_fee_usd", "exit_fee_usd", "net_usd", "exit_reason")
        row = {"profile_hash": profile.identity, "strategy_version": profile.strategy_version,
               "profile_version": profile.version, "fee_bps": costs.fee_bps, "proxy_bps": costs.proxy_bps,
               "intended_entry_utc": intended, "stop_trigger_from_utc": trigger[0], "stop_trigger_through_utc": trigger[1],
               "exit_bar_open": trade.exit_bar_open, "ledger_row": trade.Index,
               "model": "unfiltered", "raw_source_snapshot_sha256": manifest["price_snapshot"]["sha256"],
               "confirmed_implementation_error": False, **{key: planned[key] for key in keys}, **bridge}
        compact.append(row)
        detail = {**planned, **row, "exit_proof": proof,
                  "decision_source_cutoff_utc": trade.signal_at, "modeled_availability_utc": available,
                  "costs": costs.model_dump(),
                  "explanation": ("Negative bridge components offset losses. The raw entry movement is measured from "
                                  "the observable decision close. Stop-to-exit movement uses the frozen raw stop "
                                  "as reference. Stop intrabar is modeled at the stop and resolved at the bar close.")}
        if bridge["cause"] != "within_budget":
            details.append(detail)
        elif trade.net_usd < 0:
            losers.append(detail)
        elif trade.net_usd > 0:
            winners.append(detail)
    table = pd.DataFrame(compact)
    excess = table.loc[table.overrun_usd > EPSILON].copy()
    old = read_json(phase5 / "sizing.json")["audits"]["lower_fee"]["overruns"]["realized_risk_overrun_usd"]
    if len(excess) != old["count"]:
        raise ValueError("Phase 5 overrun count changed")
    np.testing.assert_allclose(excess.overrun_usd.max(), old["largest_usd"], atol=EPSILON)
    if (excess.cause == "other_requires_review").any():
        raise ValueError("An overrun has no identified price-movement cause")
    for detail, selection in ((losers[0], "within_budget_loser"), (winners[0], "winner")):
        details.append({**detail, "control_selection": selection})
    results = read_json(phase5 / "results.json")
    methods = results["methods"]
    summary = {
        "trades": len(table), "losers": int(table.net_usd.lt(0).sum()), "overruns": len(excess),
        "total_excess_usd": excess.overrun_usd.sum(), "median_overrun_usd": excess.overrun_usd.median(),
        "worst_trade": details[excess.overrun_usd.argmax()], "cause_counts": dict(Counter(excess.cause)),
        "overrun_exit_reasons": dict(Counter(excess.exit_reason)), "all_exit_reasons": dict(Counter(table.exit_reason)),
        "entry_at_or_below_stop_count": int((table.entry_candle_raw_open_usd <= table.raw_protective_stop_usd).sum()),
        "binding_constraints": prior["binding_constraints"], "fill_to_stop_overruns": prior["overruns"]["fill_risk_overrun_usd"],
        "net_pnl_usd": table.net_usd.sum(), "top_five_pnl_usd": table.net_usd.nlargest(5).sum(),
        "historical_window_days": results["elapsed_utc_days"],
        "historical_lower_fee": methods["lower_fee"]["aggregate"],
        "historical_stresses_net_return": {name: methods[name]["aggregate"]["net_return"]
                                           for name in ("proxy_10bp", "extra_hour")},
        "historical_finding": "INCONCLUSIVE", "corrective_replays_run": 0,
        "defect_status": "The saved arithmetic and all 68 stop/exit paths match the frozen conventions.",
        "uncertainty": "Hourly OHLC bounds intrabar stop hits. Exact trigger time, path and executable prices are unknown.",
    }
    return table, details, summary
