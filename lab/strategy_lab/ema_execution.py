"""Bind signal-priced EMA brackets to the existing sandbox trade executors."""
from dataclasses import asdict, dataclass, replace
from math import isfinite

import numpy as np
import pandas as pd

from lab.learning.simulator import ExecutionAssumptions, Setup, entry_fill, fill_price, quantize, size_order


SOURCE_NOTIONAL_FRACTION = .10


@dataclass(frozen=True)
class BracketSetup(Setup):
    deadline: pd.Timestamp | None
    target_price: float
    source_allocation: bool


def inside_bracket(price, stop, target, side):
    return side * (price - stop) > 0 and side * (target - price) > 0


def run_bracket(executor, market, setup, contract, cost, risk, equity, rules, cutoff, **kwargs):
    """Preserve frozen levels while delegating fills, marks, funding and P&L."""
    if not isinstance(setup, BracketSetup):
        return executor(market, setup, contract, cost, risk, equity, rules, cutoff, **kwargs)

    assumptions = kwargs.get("assumptions", ExecutionAssumptions())
    evaluation_mode = kwargs.get("evaluation_mode", "ordinary")
    record = {
        **asdict(setup), "scope": market.scope, "sizing_equity": equity, "selected_leverage": risk.leverage,
        "evaluation_mode": evaluation_mode, "fill_assumptions": asdict(assumptions),
        "signal_stop_price": setup.stop, "signal_target_price": setup.target_price,
        "allocation_basis": "10_percent_equity_notional" if setup.source_allocation else "profile_risk_and_notional_cap",
        "risk_budget_applies": not setup.source_allocation,
    }
    side = setup.direction
    if (side not in (-1, 1) or setup.target_r is not None or setup.trail_atr is not None
            or (setup.deadline is None) != setup.source_allocation
            or not all(isfinite(value) and value > 0 for value in (setup.reference, setup.atr, setup.stop,
                                                                 setup.target_price, equity))
            or not inside_bracket(setup.reference, setup.stop, setup.target_price, side)):
        return {**record, "status": "REJECTED", "reason": "invalid_signal_bracket"}
    if (setup.available_at < setup.decision_at or setup.submit_at < setup.available_at
            or (not setup.source_allocation and setup.submit_at == setup.available_at)
            or setup.expires_at <= setup.submit_at):
        return {**record, "status": "REJECTED", "reason": "invalid_bracket_entry_timing"}
    if setup.submit_at >= cutoff or (setup.deadline is not None and setup.deadline >= cutoff):
        return {**record, "status": "INCOMPLETE", "reason": "fixed_deadline_reaches_boundary"}
    index = int(market.times.searchsorted(setup.submit_at.value))
    if index == len(market.times) or market.times[index] != setup.submit_at.value:
        return {**record, "status": "INCOMPLETE", "reason": "missing_bracket_entry_open"}
    raw_open = float(market.trade_prices[index][0])
    mark = np.asarray(market.mark_prices[index])
    if not isfinite(raw_open) or raw_open <= 0:
        return {**record, "status": "INCOMPLETE", "reason": "invalid_bracket_entry_open"}
    if not np.isfinite(mark).all() or (mark <= 0).any():
        return {**record, "status": "INCOMPLETE", "reason": "missing_or_invalid_bracket_entry_mark"}

    stop = quantize(setup.stop, contract.price_tick, side < 0)
    target = quantize(setup.target_price, contract.price_tick, side > 0)
    entry = fill_price(raw_open, side, contract, cost)
    if stop <= 0 or target <= 0 or entry <= 0 or not (
        inside_bracket(raw_open, setup.stop, setup.target_price, side)
        and inside_bracket(entry, stop, target, side)
        and inside_bracket(mark[0], stop, target, side)
    ):
        return {**record, "status": "REJECTED", "reason": "entry_gap_or_rounded_price_outside_frozen_bracket"}

    simulated, sizing, settings = setup, risk, rules
    if setup.source_allocation:
        # NaT disables the legacy time-stop comparisons while the real cutoff
        # still bounds replay. The already-known modeled fill provides a market
        # order proxy through the legacy IOC interface without a signal price cap.
        simulated = replace(setup, deadline=pd.NaT, reference=entry)
        # Only the notional and instrument constraints size the source order.
        sizing = replace(risk, risk_fraction=float("inf"), notional_cap=SOURCE_NOTIONAL_FRACTION)
        settings = {**rules, "entry_limit_atr": 0.}
    order, reason = size_order(simulated, contract, cost, sizing, equity, settings, evaluation_mode=evaluation_mode)
    if order is None:
        return {**record, "status": "REJECTED", "reason": reason}
    if setup.source_allocation and order.quote_cap < equity * SOURCE_NOTIONAL_FRACTION:
        return {**record, "status": "REJECTED", "reason": "source_allocation_exceeds_contract_or_collateral"}
    contracts, modeled_entry = entry_fill(order, raw_open, side, contract, cost, assumptions.entry_fraction)
    if not contracts:
        return {**record, "status": "NO_FILL", "reason": "ioc_limit_or_capacity", "filled_contracts": 0.}

    # The legacy engine encodes its target as fill-relative R. Choose the
    # interior of the tick's rounding interval to retain the frozen price.
    inset = min(contract.price_tick / 2, abs(target - modeled_entry) / 2)
    raw_target = target - side * inset
    target_r = side * (raw_target - modeled_entry) / abs(modeled_entry - stop)
    translated = modeled_entry + side * target_r * abs(modeled_entry - stop)
    if target_r <= 0 or quantize(translated, contract.price_tick, side > 0) != target:
        return {**record, "status": "REJECTED", "reason": "target_tick_cannot_be_encoded"}
    simulated = replace(simulated, target_r=target_r)
    result = executor(market, simulated, contract, cost, sizing, equity, settings, cutoff, **kwargs)
    if result.get("entry_at") is not None and result["target_price"] != target:
        raise ValueError("Trade executor did not preserve the signal-priced target")
    result.update(reference=setup.reference, deadline=setup.deadline, target_r=None,
                  signal_stop_price=setup.stop, signal_target_price=setup.target_price,
                  allocation_basis=record["allocation_basis"], risk_budget_applies=not setup.source_allocation)
    if setup.source_allocation:
        # The original position allocation is not a stop-risk budget. R still
        # uses the simulator's estimated loss at the frozen protective stop.
        result["risk_budget"] = None
        if "realized_loss_overrun" in result:
            result["realized_loss_overrun"] = 0.
    return result
