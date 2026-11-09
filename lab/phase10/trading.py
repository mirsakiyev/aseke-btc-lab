"""The Phase 8 execution policy on lazy partitions with explicit settlement events."""
from dataclasses import asdict

import numpy as np

from lab.learning.simulator import (
    ExecutionAssumptions, Position, entry_fill, fill_price, liquidation_price, quantize, size_order, triggers,
)


def run_trade(market, setup, contract, cost, risk, equity, rules, cutoff, *,
              assumptions=ExecutionAssumptions(), trail_updates=None, evaluation_mode="ordinary", keep_marks=False):
    if (not 0 <= assumptions.entry_fraction <= 1 or not 0 <= assumptions.exit_fraction <= 1
            or assumptions.delay_exit_bars < 0):
        raise ValueError("Invalid execution assumptions")
    record = {**asdict(setup), "scope": market.scope, "sizing_equity": equity, "selected_leverage": risk.leverage,
              "evaluation_mode": evaluation_mode, "fill_assumptions": asdict(assumptions)}
    order, reason = size_order(setup, contract, cost, risk, equity, rules, evaluation_mode=evaluation_mode)
    if order is None:
        return {**record, "status": "REJECTED", "reason": reason}
    record.update(asdict(order))
    if setup.deadline >= cutoff or setup.submit_at >= cutoff:
        return {**record, "status": "INCOMPLETE", "reason": "fixed_deadline_reaches_boundary"}
    entry_i = int(market.times.searchsorted(setup.submit_at.value))
    if entry_i == len(market.times) or market.times[entry_i] != setup.submit_at.value:
        return {**record, "status": "INCOMPLETE", "reason": "missing_ioc_bar"}
    trade = market.trade_prices[entry_i]
    if not np.isfinite(market.mark_prices[entry_i]).all():
        return {**record, "status": "INCOMPLETE", "reason": "missing_entry_mark"}
    contracts, entry_price = entry_fill(order, trade[0], setup.direction, contract, cost, assumptions.entry_fraction)
    if not contracts:
        return {**record, "status": "NO_FILL", "reason": "ioc_limit_or_capacity", "filled_contracts": 0.}
    qty = contracts * contract.base_per_contract
    notional = qty * entry_price
    fee = notional * cost.fee
    margin = notional / risk.leverage
    position = Position(setup.direction, qty, entry_price, equity - fee, margin, fee, notional)
    stop = quantize(setup.stop, contract.price_tick, setup.direction < 0)
    target = (quantize(entry_price + setup.direction * setup.target_r * abs(entry_price - stop),
                       contract.price_tick, setup.direction > 0) if setup.target_r else None)
    record.update(entry_at=setup.submit_at, entry_fill_price=entry_price, filled_contracts=contracts,
                  signed_contracts=contracts * setup.direction, signed_base_quantity=qty * setup.direction,
                  ioc_cancelled_contracts=max(0., order.contracts - contracts), initial_margin=margin,
                  free_after_entry=position.free, account_effective_leverage=notional / equity, target_price=target)
    is_perp = contract.family == "linear_perp"
    pending, ambiguous, fills, marks, settlements = None, False, [], [], []
    reason = "no_resolving_bar"
    for i in range(entry_i, len(market.times)):
        at = market.index[i]
        if at >= cutoff:
            break
        if market.times[i] != setup.submit_at.value + (i - entry_i) * market.step.value:
            reason = "trade_feed_gap_while_exposed"
            break
        trade, mark = market.trade_prices[i], market.mark_prices[i]
        if not np.isfinite(mark).all():
            reason = "mark_feed_gap_while_exposed"
            break
        if i != entry_i and at in market.funding:
            rate = market.funding[at]
            position.fund(rate, mark[0])
            settlements.append({"settlement_at": at, "rate": rate, "mark_proxy": mark[0],
                                "precedence": "AT_OPEN_BEFORE_EXITS"})
        if is_perp and position.remaining * mark[1] > contract.tier_notional:
            reason = "unsupported_margin_tier_transition"
            break
        liquidation = (liquidation_price(entry_price, position.reserved / position.remaining, setup.direction,
                                         contract, cost.fee) if is_perp else None)
        if not fills and i == entry_i:
            record["entry_liquidation_mark"] = liquidation
        if setup.trail_atr and trail_updates and at in trail_updates and pending is None:
            trail = trail_updates[at]
            proposed = quantize(trail[0] - setup.direction * setup.trail_atr * trail[1],
                                contract.price_tick, setup.direction < 0)
            stop = max(stop, proposed) if setup.direction > 0 else min(stop, proposed)
        opened = triggers(mark, setup.direction, stop, target, liquidation, True)
        opening_liquidation = opened and opened[0][0] == "liquidation"
        intrabar = False
        executable_at = at
        if opening_liquidation:
            action, level = opened[0]
        elif pending and at >= pending[1]:
            action, level = pending[0], None
        elif pending:
            hits = triggers(mark, setup.direction, stop, None, liquidation, False)
            if hits and hits[0][0] == "liquidation":
                action, level, intrabar = "liquidation", liquidation, True
            else:
                action, level = None, None
        elif at >= setup.deadline:
            action, level = "time_exit", None
        elif opened:
            action, level = opened[0]
        else:
            hits = triggers(mark, setup.direction, stop, target, liquidation, False)
            ambiguous |= len(hits) > 1
            action, level = hits[0] if hits else (None, None)
            intrabar = bool(hits)
        if intrabar:
            executable_at = at + market.step
        if action and action != "liquidation" and pending is None and assumptions.delay_exit_bars:
            pending = (action, at + assumptions.delay_exit_bars * market.step)
            action = None
        # The frozen replay resolves candle-open actions at the open and
        # intrabar exits at the close. Between them use the known mark open
        # as the funding-price proxy, retaining the exact settlement instant.
        if not action or intrabar:
            for settled_at, rate in market.intrabar_funding.get(at, ()):
                if settled_at >= cutoff:
                    break
                position.fund(rate, mark[0])
                settlements.append({"settlement_at": settled_at, "rate": rate, "mark_proxy": mark[0],
                                    "precedence": "AFTER_OPEN_BEFORE_CLOSE_RESOLUTION"})
                if is_perp:
                    revised = liquidation_price(entry_price, position.reserved / position.remaining,
                                                setup.direction, contract, cost.fee)
                    if triggers(mark, setup.direction, stop, None, revised, False)[:1] == [("liquidation", revised)]:
                        ambiguous |= action is not None and action != "liquidation"
                        action, level, intrabar = "liquidation", revised, True
                        executable_at = at + market.step
        if action:
            if executable_at >= cutoff:
                reason = "exit_resolution_reaches_boundary"
                break
            if intrabar and is_perp:
                raw = trade[2] if setup.direction > 0 else trade[1]
            elif intrabar:
                raw = level
            else:
                raw = trade[0]
            price = fill_price(raw, -setup.direction, contract, cost)
            forced = action == "liquidation"
            ratio = 1. if forced or fills else assumptions.exit_fraction
            units = position.remaining / contract.base_per_contract
            amount = position.remaining if ratio == 1 else quantize(units * ratio, contract.quantity_step) * contract.base_per_contract
            if amount > 0:
                position.reduce(amount, price, cost, contract.liquidation_fee_rate if forced else 0., liquidated=forced)
                fills.append({"resolved_at": executable_at, "reason": action, "trigger_mark": level,
                              "raw_trade_price": raw, "filled_price": price, "base": amount,
                              "reduce_only": True, "stop_target_siblings_cancelled": True})
            if position.remaining <= contract.base_per_contract * contract.quantity_step * 1e-6:
                if keep_marks:
                    marks.append((executable_at, position.wallet, False))
                return {**record, "status": "AMBIGUOUS" if ambiguous else "COMPLETE", "reason": action,
                        "mature_at": executable_at, "exit_at": executable_at, "fills": fills, "marks": marks,
                        "fees": position.fees, "funding": position.funding, "settlements": settlements, "gross_realized": position.realized,
                        "liquidation_cost": position.liquidation_cost, "unfunded_deficit": position.unfunded_deficit,
                        "turnover": position.turnover, "net": position.wallet - equity, "ending_equity": position.wallet,
                        "net_r": (position.wallet - equity) / order.planned_risk,
                        "realized_loss_overrun": max(0., equity - position.wallet - order.risk_budget)}
            pending = (action, at + market.step)
        if keep_marks:
            marks.append((at + market.step, position.wallet + position.unrealized(mark[3]), True))
    return {**record, "status": "INCOMPLETE", "reason": reason, "fills": fills, "marks": marks,
            "remaining_base": position.remaining, "free_collateral": position.free, "reserved_collateral": position.reserved,
            "fees": position.fees, "funding": position.funding, "settlements": settlements, "turnover": position.turnover,
            "liquidation_cost": position.liquidation_cost}

