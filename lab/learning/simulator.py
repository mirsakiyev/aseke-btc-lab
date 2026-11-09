from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from lab.execution import execution_open


@dataclass(frozen=True)
class Contract:
    family: str
    quote: str
    base_per_contract: float
    quantity_step: float
    price_tick: float
    min_contracts: float
    tier_notional: float
    max_leverage: float
    maintenance_rate: float
    liquidation_fee_rate: float

    @classmethod
    def load(cls, config):
        spec = cls(**{name: config[name] for name in cls.__dataclass_fields__})
        if (spec.family not in ("spot", "linear_perp") or
                (spec.family == "linear_perp" and spec.quote != "USDT") or
                min(spec.base_per_contract, spec.quantity_step, spec.price_tick, spec.min_contracts, spec.tier_notional) <= 0
                or not 1 <= spec.max_leverage <= 100
                or not 0 <= spec.maintenance_rate + spec.liquidation_fee_rate < 1):
            raise ValueError("Unsupported contract or isolated margin tier")
        return spec


@dataclass(frozen=True)
class Cost:
    fee: float
    adverse: float

    @classmethod
    def load(cls, settings):
        return cls(settings["taker_bps"] / 10000, settings["adverse_bps"] / 10000)


@dataclass(frozen=True)
class Risk:
    starting_equity: float
    risk_fraction: float
    notional_cap: float
    leverage: float
    ordinary_cap: float
    liquidation_buffer_atr: float


@dataclass(frozen=True)
class Setup:
    decision_at: pd.Timestamp
    available_at: pd.Timestamp
    submit_at: pd.Timestamp
    expires_at: pd.Timestamp
    deadline: pd.Timestamp
    reference: float
    atr: float
    stop: float
    direction: int
    target_r: float | None
    trail_atr: float | None
    family: str
    exit_policy: str


@dataclass(frozen=True)
class Order:
    contracts: float
    limit_price: float
    quote_cap: float
    risk_budget: float
    planned_risk: float
    leverage: float
    projected_liquidation: float | None
    projected_buffer: float | None


@dataclass(frozen=True)
class ExecutionAssumptions:
    entry_fraction: float = 1
    exit_fraction: float = 1
    delay_exit_bars: int = 0
    version: str = "ioc-open-proxy-capped-quote-v1"


def quantize(value, unit, upward=False):
    scale = Decimal(str(unit))
    steps = (Decimal(str(value)) / scale).to_integral_value(rounding=ROUND_CEILING if upward else ROUND_FLOOR)
    return float(steps * scale)


def fill_price(raw, side, contract, cost):
    return quantize(raw * (1 + side * cost.adverse), contract.price_tick, side > 0)


def liquidation_price(entry, isolated_per_base, direction, contract, fee=0.0):
    rates = contract.maintenance_rate + contract.liquidation_fee_rate + fee
    return max(0., (entry - direction * isolated_per_base) / (1 - direction * rates))


def make_setup(close_at, row, direction, family, exit_name, exit_params, profile, rules, step, delay_bars=0):
    available = close_at + pd.Timedelta(seconds=rules["processing_seconds"])
    submit = execution_open(available, step, delay_bars)
    deadline = submit + pd.Timedelta(minutes=profile["max_hold_minutes"])
    if "entry_cutoff_minute_utc" in profile:
        day = close_at.normalize()
        if submit >= day + pd.Timedelta(minutes=profile["entry_cutoff_minute_utc"]):
            return None
        deadline = min(deadline, day + pd.Timedelta(minutes=profile["flat_minute_utc"]))
    close, atr = float(row.close), float(row.atr)
    stop = close - direction * exit_params["initial_atr"] * atr
    structure = float(row.pivot_low if direction == 1 else row.pivot_high)
    if atr <= direction * (close - structure) < exit_params["initial_atr"] * atr:
        stop = structure
    return Setup(close_at, available, submit, submit + step, deadline, close, atr, stop, direction,
                 exit_params.get("target_r"), exit_params.get("trailing_atr"), family, exit_name)


def size_order(setup, contract, cost, risk, equity, rules, *, evaluation_mode="ordinary"):
    direction, leverage = setup.direction, risk.leverage
    if not 1 <= leverage <= min(100, contract.max_leverage) or (
        evaluation_mode != "stress" and leverage > risk.ordinary_cap
    ):
        return None, "selected_leverage_outside_policy"
    if contract.family == "spot" and (direction != 1 or leverage != 1):
        return None, "spot_long_cash_only"
    if min(equity, setup.stop) <= 0 or direction * (setup.reference - setup.stop) <= 0:
        return None, "invalid_invalidation_or_equity"
    distance = rules["entry_limit_atr"] * setup.atr
    limit = quantize(setup.reference + direction * distance, contract.price_tick, direction < 0)
    ceiling = quantize(setup.reference + distance, contract.price_tick, True)
    if limit <= 0:
        return None, "nonpositive_entry_bound"
    stop = quantize(setup.stop, contract.price_tick, direction < 0)
    stop_fill = fill_price(stop, -direction, contract, cost)
    funding_reserve = ceiling * rules["funding_reserve_bps"] / 10000 if contract.family == "linear_perp" else 0.
    per_base_loss = direction * (limit - stop_fill) + cost.fee * (ceiling + stop_fill) + funding_reserve
    if per_base_loss <= 0:
        return None, "nonpositive_modeled_risk"
    liq, buffer = None, None
    if contract.family == "linear_perp":
        liq = liquidation_price(limit, limit / leverage - funding_reserve, direction, contract, cost.fee)
        buffer = direction * (stop - liq)
        if buffer < risk.liquidation_buffer_atr * setup.atr:
            return None, "stop_to_liquidation_buffer"
    budget = equity * risk.risk_fraction
    cap = min(equity * risk.notional_cap, contract.tier_notional,
              equity / (1 / leverage + cost.fee + funding_reserve / ceiling))
    max_base = min(budget / per_base_loss, cap / ceiling)
    contracts = quantize(max_base / contract.base_per_contract, contract.quantity_step)
    if contracts < contract.min_contracts:
        return None, "minimum_lot_or_collateral"
    return Order(contracts, limit, cap, budget, contracts * contract.base_per_contract * per_base_loss,
                 leverage, liq, buffer), None


def entry_fill(order, raw_open, direction, contract, cost, fraction):
    price = fill_price(raw_open, direction, contract, cost)
    if direction * (price - order.limit_price) > 1e-10:
        return 0., price
    # Matching quantity is bounded by precommitted quote capacity before a fill exists.
    # Any unfilled IOC remainder expires. OHLC provides no evidence of actual capacity.
    quantity = min(order.contracts * fraction, order.quote_cap / (price * contract.base_per_contract))
    contracts = quantize(quantity, contract.quantity_step)
    return (contracts if contracts >= contract.min_contracts else 0.), price


@dataclass
class Position:
    direction: int
    remaining: float
    entry: float
    wallet: float
    reserved: float
    fees: float
    turnover: float
    funding: float = 0.
    realized: float = 0.
    liquidation_cost: float = 0.
    unfunded_deficit: float = 0.

    @property
    def free(self):
        return self.wallet - self.reserved

    def unrealized(self, mark):
        return self.direction * self.remaining * (mark - self.entry)

    def fund(self, rate, mark):
        amount = -self.direction * self.remaining * mark * rate
        self.funding += amount
        self.reserved += amount
        self.wallet += amount

    def reduce(self, amount, price, cost, liq_fee=0., *, liquidated=False):
        amount = min(amount, self.remaining)
        fee = price * amount * cost.fee
        liquidation = price * amount * liq_fee
        pnl = self.direction * amount * (price - self.entry)
        release = self.reserved * (amount / self.remaining)
        deficit = max(0., -(release + pnl - fee - liquidation)) if liquidated else 0.
        self.wallet += pnl - fee - liquidation + deficit
        self.reserved -= release
        self.remaining = max(0., self.remaining - amount)
        self.turnover += price * amount
        self.fees += fee
        self.realized += pnl
        self.liquidation_cost += liquidation
        self.unfunded_deficit += deficit


def triggers(mark, direction, stop, target, liquidation, at_open):
    opened, high, low, _ = mark
    adverse = opened if at_open else low if direction > 0 else high
    favorable = opened if at_open else high if direction > 0 else low
    hits = []
    if liquidation is not None and direction * (adverse - liquidation) <= 0:
        hits.append(("liquidation", liquidation))
    if direction * (adverse - stop) <= 0:
        hits.append(("stop", stop))
    if target is not None and direction * (favorable - target) >= 0:
        hits.append(("target", target))
    return hits


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
    pending, ambiguous, fills, marks = None, False, [], []
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
            position.fund(market.funding[at], mark[0])
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
                        "fees": position.fees, "funding": position.funding, "gross_realized": position.realized,
                        "liquidation_cost": position.liquidation_cost, "unfunded_deficit": position.unfunded_deficit,
                        "turnover": position.turnover, "net": position.wallet - equity, "ending_equity": position.wallet,
                        "net_r": (position.wallet - equity) / order.planned_risk,
                        "realized_loss_overrun": max(0., equity - position.wallet - order.risk_budget)}
            pending = (action, at + market.step)
        if keep_marks:
            marks.append((at + market.step, position.wallet + position.unrealized(mark[3]), True))
    return {**record, "status": "INCOMPLETE", "reason": reason, "fills": fills, "marks": marks,
            "remaining_base": position.remaining, "free_collateral": position.free, "reserved_collateral": position.reserved,
            "fees": position.fees, "funding": position.funding, "turnover": position.turnover,
            "liquidation_cost": position.liquidation_cost}
