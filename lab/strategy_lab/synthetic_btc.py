"""Kraken BTC/USD reference pricing for hypothetical BTC paper positions."""

from dataclasses import asdict, dataclass
from math import ceil, isfinite

import pandas as pd

from lab.config import fingerprint
from lab.execution import execution_open
from lab.learning.simulator import Contract, Cost, Order, Position, entry_fill, fill_price, liquidation_price, quantize, triggers
from lab.phase15 import paper
from lab.phase15.catalog import QUARTER, STEP
from lab.strategy_lab.ema_execution import BracketSetup, inside_bracket


VERSION = "SYNTHETIC_BTC_LINEAR_PERPETUAL_V1"
SPOT_VERSION = "SYNTHETIC_BTC_CASH_SPOT_V1"
PRICE_SOURCE = "KRAKEN_BTC_USD_SPOT"
EXECUTION_MODEL = "SYNTHETIC_LEVERAGED_BTC"
REFERENCE_PROXY = "KRAKEN_BTC_USD_REFERENCE_PROXY"
NOT_AVAILABLE = "NOT_AVAILABLE"
RISK_FRACTION = .0025
MINIMUM_PERPETUAL_LIQUIDATION_BUFFER_ATR = .5
LEVERAGES = frozenset((1, 2, 3, 5))
PRIMARY_LEVERAGE = {"day": 3, "swing": 3, "position": 1}
STRESS_SCENARIOS = ("double_cost", "adverse_funding", "combined_adverse", "usd_usdt_minus_3pct", "usd_usdt_plus_3pct")


def _utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Synthetic execution requires a timestamp with an explicit UTC offset")
    return stamp.tz_convert("UTC")


@dataclass(frozen=True, slots=True)
class SyntheticModel:
    profile: str
    leverage: int
    scenario: str
    perpetual: bool
    taker_fee: float
    adverse_execution: float
    funding_cost_rate: float
    funding_interval_minutes: int
    maintenance_rate: float
    liquidation_fee: float
    usd_to_nominal_usdt: float
    quantity_step_btc: float = .000001
    quote_tick: float = .01

    def __post_init__(self):
        if (self.profile not in PRIMARY_LEVERAGE or type(self.leverage) is not int or self.leverage not in LEVERAGES
                or self.scenario not in {"primary", "counterfactual", *STRESS_SCENARIOS}
                or type(self.perpetual) is not bool or not self.perpetual and self.leverage != 1
                or self.profile == "position" and self.scenario == "primary" and self.perpetual):
            raise ValueError("Unsupported synthetic profile, leverage or scenario")
        rates = (self.taker_fee, self.adverse_execution, self.funding_cost_rate, self.maintenance_rate, self.liquidation_fee)
        if (any(not isfinite(rate) or not 0 <= rate < 1 for rate in rates)
                or sum((self.maintenance_rate, self.liquidation_fee, self.taker_fee, self.adverse_execution)) >= 1
                or not self.perpetual and any(rates[2:])
                or type(self.funding_interval_minutes) is not int or self.funding_interval_minutes <= 0
                or 1440 % self.funding_interval_minutes
                or any(not isfinite(value) or value <= 0 for value in
                       (self.usd_to_nominal_usdt, self.quantity_step_btc, self.quote_tick))):
            raise ValueError("Invalid synthetic cost, margin, funding clock or currency assumptions")

    @property
    def version(self):
        return VERSION if self.perpetual else SPOT_VERSION

    @property
    def cost(self):
        return Cost(self.taker_fee, self.adverse_execution)

    @property
    def contract(self):
        # The shared simulator's quantity field is denominated in BTC here.
        # Its finite ceiling is a numerical bound; this model has no venue tiers.
        return Contract("linear_perp" if self.perpetual else "spot", "USDT", 1., self.quantity_step_btc,
                        self.quote_tick, self.quantity_step_btc, 1e12, max(LEVERAGES) if self.perpetual else 1,
                        self.maintenance_rate, self.liquidation_fee)

    def body(self):
        return {"version": self.version, "execution_model": EXECUTION_MODEL, "price_source": PRICE_SOURCE,
                "actual_futures_mark": NOT_AVAILABLE, "actual_funding": NOT_AVAILABLE,
                "price_unit": "USD", "ledger_unit": "NOMINAL_USDT", "quantity_unit": "BTC",
                "margin_model": "isolated" if self.perpetual else "unleveraged cash",
                "funding_model": "both directions pay modeled financing at UTC boundaries before opening exits"
                if self.perpetual else "no financing for unleveraged cash",
                "currency_model": "frozen nominal USD to USDT simulation ratio",
                "intraminute_order": ["liquidation", "stop", "target"],
                "entry_or_exit_cost": "one adverse price adjustment and one taker fee per fill",
                "liquidation_trigger": "reference-price isolated equity versus maintenance and forced-close cost reserve",
                "minimum_perpetual_liquidation_buffer_atr": MINIMUM_PERPETUAL_LIQUIDATION_BUFFER_ATR,
                "liquidation_fill": "open on opening gap; adverse reference extreme for intraminute liquidation",
                "protective_fill": "open on opening gap; fixed threshold for intraminute stop or target",
                "risk_fraction": RISK_FRACTION, "paper_only": True, **asdict(self)}

    @property
    def id(self):
        return fingerprint(self.body())


def model(profile, *, leverage=None, scenario="primary", funding_cost_rate=None, funding_interval_minutes=480):
    """Freeze one profile's primary assumptions or an isolated sensitivity case."""
    if profile not in PRIMARY_LEVERAGE:
        raise ValueError("Unknown synthetic EMA profile")
    leverage = PRIMARY_LEVERAGE[profile] if leverage is None else leverage
    perpetual = profile != "position" or leverage != 1
    double_cost = scenario in {"double_cost", "combined_adverse"}
    adverse_funding = scenario in {"adverse_funding", "combined_adverse"}
    rate = (.0003 if adverse_funding else .0001) if perpetual else 0.
    conversion = {"usd_usdt_minus_3pct": .97, "usd_usdt_plus_3pct": 1.03}.get(scenario, 1.)
    return SyntheticModel(profile, leverage, scenario, perpetual, .002 if double_cost else .001,
                          .001 if double_cost else .0005, rate if funding_cost_rate is None else funding_cost_rate,
                          funding_interval_minutes, .01 if perpetual else 0., .005 if perpetual else 0., conversion)


def load_model(body, identifier):
    """Verify the exact frozen model used by the published plan."""
    if not isinstance(body, dict) or not SyntheticModel.__dataclass_fields__.keys() <= body.keys():
        raise ValueError("Synthetic model body is incomplete")
    result = SyntheticModel(**{name: body[name] for name in SyntheticModel.__dataclass_fields__})
    if result.body() != body or result.id != identifier:
        raise ValueError("Synthetic model identity or assumptions differ from the frozen plan")
    return result


def _liquidation(entry, collateral_per_btc, side, policy):
    if not policy.perpetual:
        return None
    return liquidation_price(entry, collateral_per_btc, side, policy.contract, policy.taker_fee + policy.adverse_execution)


def margin_state(quantity_btc, entry, reference, side, wallet, policy, *, funding=0.):
    """Report normalized BTC margin and price P&L in the nominal ledger unit."""
    if (type(side) is not int or side not in (-1, 1) or not policy.perpetual and side != 1
            or any(not isfinite(value) or value <= 0 for value in (quantity_btc, entry, reference))
            or not isfinite(wallet) or not isfinite(funding)):
        raise ValueError("Invalid synthetic position economics")
    notional = quantity_btc * entry
    initial = notional / policy.leverage
    used = max(0., initial + funding)
    current_notional = quantity_btc * reference
    unrealized = side * quantity_btc * (reference - entry)
    liquidation = _liquidation(entry, (initial + funding) / quantity_btc, side, policy)
    maintenance = current_notional * policy.maintenance_rate
    forced_reserve = current_notional * (policy.liquidation_fee + policy.taker_fee + policy.adverse_execution)
    return {"quantity_btc": quantity_btc, "entry_notional": notional, "notional": current_notional,
            "initial_margin": initial, "used_margin": used, "free_collateral": wallet - used,
            "maintenance_margin": maintenance,
            "modeled_forced_close_reserve": forced_reserve if policy.perpetual else 0.,
            "maintenance_buffer": initial + funding + unrealized - maintenance - forced_reserve
            if policy.perpetual else None,
            "unrealized_pnl": unrealized, "marked_equity": wallet + unrealized,
            "liquidation_price": liquidation,
            "liquidation_buffer": side * (reference - liquidation) if liquidation is not None else None}


def currency_basis_sensitivity(quantity_btc, direction, entry_usd, exit_usd, *, entry_ratio=1., exit_ratio=1.):
    """Compare gross P&L for explicitly chosen entry and exit conversion ratios."""
    if (direction not in (-1, 1) or any(not isfinite(value) or value <= 0
                                        for value in (quantity_btc, entry_usd, exit_usd, entry_ratio, exit_ratio))):
        raise ValueError("Currency sensitivity requires positive USD prices and nominal ratios")
    baseline = direction * quantity_btc * (exit_usd - entry_usd)
    stressed = direction * quantity_btc * (exit_usd * exit_ratio - entry_usd * entry_ratio)
    return {"baseline_gross": baseline, "stressed_gross": stressed, "change": stressed - baseline,
            "entry_ratio": entry_ratio, "exit_ratio": exit_ratio, "modeled_only": True}


def _sized_order(lane, side, reference, atr, stop, target, equity, policy):
    rules = lane["risk"]
    contract, cost = policy.contract, policy.cost
    gap = lane["spec"]["execution"]["entry_limit_atr"] * atr
    limit = quantize(reference + side * gap, contract.price_tick, side < 0)
    if limit <= 0 or side * (limit - stop) <= 0:
        return None, None, "INVALID_BOUNDED_ENTRY"
    lower, upper = sorted((stop, target))
    if side == 1:
        upper = min(upper, limit)
    else:
        lower = max(lower, limit)
    if lower >= upper:
        return None, None, "EMPTY_ENTRY_RANGE"

    worst_entry = upper if side == 1 else lower
    stop_fill = fill_price(stop, -side, contract, cost)
    if stop_fill <= 0:
        return None, None, "NONPOSITIVE_MODELED_STOP_FILL"
    holds = lane["spec"]["session"]["max_hold_minutes"]
    settlements = ceil(holds / policy.funding_interval_minutes) if policy.perpetual else 0
    funding_per_btc = max(upper, stop_fill, target) * settlements * policy.funding_cost_rate
    loss_per_btc = side * (worst_entry - stop_fill) + cost.fee * (upper + stop_fill) + funding_per_btc
    liquidation = _liquidation(worst_entry, worst_entry / policy.leverage - funding_per_btc, side, policy)
    buffer = side * (stop - liquidation) if liquidation is not None else None
    minimum_buffer = max(rules["liquidation_buffer_atr"], MINIMUM_PERPETUAL_LIQUIDATION_BUFFER_ATR) * atr if policy.perpetual else 0.
    if policy.perpetual and (worst_entry / policy.leverage <= funding_per_btc or buffer <= 0 or buffer < minimum_buffer):
        return None, None, "STOP_TO_LIQUIDATION_BUFFER"
    if loss_per_btc <= 0:
        return None, None, "NONPOSITIVE_MODELED_RISK"
    budget = equity * RISK_FRACTION
    cash_per_btc = upper / policy.leverage + cost.fee * (upper + stop_fill) + funding_per_btc
    leverage_capacity = equity * policy.leverage
    max_notional = min(leverage_capacity, equity * rules["notional_cap"], contract.tier_notional,
                       equity / cash_per_btc * upper)
    quantity = quantize(min(budget / loss_per_btc, max_notional / upper), contract.quantity_step)
    if quantity < contract.min_contracts:
        return None, None, "MINIMUM_BTC_QUANTITY_OR_COLLATERAL"
    order = Order(quantity, limit, max_notional, budget, quantity * loss_per_btc, policy.leverage, liquidation, buffer)
    sizing = {"base": quantity, "quantity_btc": quantity, "planned_risk": order.planned_risk,
              "risk_budget": budget, "leverage": policy.leverage, "risk_per_btc": loss_per_btc,
              "maximum_position_notional": max_notional, "gross_leverage_capacity": leverage_capacity,
              "maximum_initial_margin": quantity * upper / policy.leverage,
              "maximum_financing_reserve": quantity * funding_per_btc, "funding_periods_reserved": settlements,
              "minimum_liquidation_buffer": minimum_buffer, "liquidation_buffer": buffer,
              "projected_liquidation": liquidation, "entry_lower": lower, "entry_upper": upper}
    return order, sizing, None


def make_plan(lane, setup, equity, now, evidence, policy):
    """Create the fixed EMA bracket using only a causally available signal."""
    if (not isinstance(setup, BracketSetup) or setup.source_allocation or setup.target_r is not None
            or setup.trail_atr is not None or lane["profile"] != policy.profile):
        return None, "PAPER_PROFILE_BRACKET_REQUIRED"
    if lane["risk"]["risk_fraction"] != RISK_FRACTION:
        raise ValueError("Synthetic study must preserve the 0.25% frozen stop-risk budget")
    side = setup.direction
    label = {1: "long", -1: "short"}.get(side)
    if label not in lane["spec"]["sides"] or not policy.perpetual and side != 1:
        return None, "INCOMPATIBLE_PROFILE_SIDE"
    now = _utc(now)
    step = pd.Timedelta(seconds=lane["execution_clock"]["entry_seconds"])
    entry_at = execution_open(setup.available_at, step)
    if not setup.decision_at <= setup.available_at <= now < entry_at:
        return None, "MISSED_OR_UNAVAILABLE_ENTRY_BOUNDARY"
    if evidence.get("publication_end_exclusive") and entry_at >= _utc(evidence["publication_end_exclusive"]):
        return None, "ENTRY_OUTSIDE_PRIMARY_HORIZON"
    ratio, tick = policy.usd_to_nominal_usdt, policy.quote_tick
    reference, raw_stop, raw_target, atr = (value * ratio for value in (setup.reference, setup.stop, setup.target_price, setup.atr))
    if (not all(isfinite(value) and value > 0 for value in (reference, raw_stop, raw_target, atr, equity))
            or not inside_bracket(reference, raw_stop, raw_target, side)):
        return None, "INVALID_SIGNAL_BRACKET"
    stop, target = quantize(raw_stop, tick, side < 0), quantize(raw_target, tick, side > 0)
    if min(stop, target) <= 0 or not inside_bracket(reference, stop, target, side):
        return None, "INVALID_ROUNDED_SIGNAL_BRACKET"
    order, sizing, reason = _sized_order(lane, side, reference, atr, stop, target, equity, policy)
    if order is None:
        return None, reason
    costs = {"taker": policy.taker_fee, "adverse_proxy": policy.adverse_execution, "basis": "SYNTHETIC_ASSUMPTION"}
    origin = {**evidence, "price_source": PRICE_SOURCE, "execution_model": EXECUTION_MODEL,
              "actual_futures_mark": NOT_AVAILABLE, "actual_funding": NOT_AVAILABLE, "synthetic_model_id": policy.id}
    return {
        "direction": side, "eligible_from": entry_at, "entry_expiry": entry_at + STEP,
        "entry": {"minimum": sizing["entry_lower"], "maximum": sizing["entry_upper"], "reference": reference,
                  "trigger": reference},
        "entry_basis": "KRAKEN_REFERENCE_MINUTE_OPEN_WITH_MODELED_ADVERSE", "stop": stop, "target": target,
        "signal_stop": raw_stop, "signal_target": raw_target, "target_r": None, "trailing_atr": None,
        "target_basis": "CONFIRMATION_CLOSE", "signal_at": setup.decision_at, "signal_available_at": setup.available_at,
        "trigger_basis": REFERENCE_PROXY, "exit_execution_basis": REFERENCE_PROXY, "session_flat": None,
        "max_hold_minutes": lane["spec"]["session"]["max_hold_minutes"], "contract": asdict(policy.contract),
        "cost": costs, "order": asdict(order), "sizing": sizing, "evidence": origin,
        "synthetic_model": policy.body(), "synthetic_model_id": policy.id,
        "price_receipt_seconds": lane["source"]["price_receipt_seconds"],
        "decision_receipt_seconds": lane["source"]["decision_receipt_seconds"],
    }, None


def counterfactual_plans(lane, setup, equity, now, evidence):
    """Compare sizing and liquidation on the same signal without touching any ledger."""
    profile = lane["profile"]
    comparisons = {}
    for leverage in ((3, 5) if profile != "position" else (1, 3, 5)):
        candidate = model(profile, leverage=leverage, scenario="counterfactual")
        plan, reason = make_plan(lane, setup, equity, now, evidence, candidate)
        comparisons[str(leverage)] = {"model_id": candidate.id, "leverage": leverage,
                                      "sizing": plan["sizing"] if plan else None, "reason": reason,
                                      "counterfactual_only": True}
    return comparisons


def counterfactual_margins(profile, *, quantity_btc, entry, reference, direction, wallet, funding=0.):
    """Compare margin and P&L at the same observed reference, entry and BTC size."""
    leverages = (1, 3, 5) if profile == "position" else (3, 5)
    return {str(leverage): {"leverage": leverage, "counterfactual_only": True,
                           **margin_state(quantity_btc, entry, reference, direction, wallet,
                                          model(profile, leverage=leverage, scenario="counterfactual"),
                                          funding=funding if profile != "position" or leverage > 1 else 0.)}
            for leverage in leverages}


@dataclass(frozen=True, slots=True)
class MinuteExit:
    kind: str
    raw_price: float
    at_open: bool
    competing_triggers: tuple[str, ...]


def minute_exit(prices, *, direction, stop, target, liquidation, at_deadline=False):
    """Resolve gaps at the open and competing intraminute hits conservatively."""
    at_open = triggers(prices, direction, stop, target, liquidation, True)
    if at_open or at_deadline:
        kind = at_open[0][0] if at_open else "time_exit"
        if at_deadline and kind != "liquidation":
            kind = "time_exit"
        return MinuteExit(kind, prices[0], True, tuple(hit[0] for hit in at_open))
    hits = triggers(prices, direction, stop, target, liquidation, False)
    if not hits:
        return None
    kind, level = hits[0]
    if kind == "liquidation":
        level = prices[2] if direction == 1 else prices[1]
    return MinuteExit(kind, level, False, tuple(hit[0] for hit in hits))


def _tags(policy, plan):
    return {"price_source": PRICE_SOURCE, "execution_model": EXECUTION_MODEL, "synthetic_model_id": policy.id,
            "execution_model_version": policy.version, "leverage": policy.leverage,
            "actual_funding": NOT_AVAILABLE, "actual_futures_mark": NOT_AVAILABLE,
            "source_mode": plan["evidence"].get("source_mode", "KRAKEN_NATIVE")}


def _open(db, book, plan, policy, bar, at, now):
    idea = book["pending"]
    if at != _utc(plan["eligible_from"]) or at <= _utc(idea["published_at"]) or at >= _utc(plan["entry_expiry"]):
        raise ValueError("Synthetic paper entry must follow publication at its committed opening minute")
    stop, target, side = plan["stop"], plan["target"], plan["direction"]
    order, contract = Order(**plan["order"]), policy.contract
    if order.leverage != policy.leverage:
        raise ValueError("Published position sizing differs from its frozen synthetic leverage")
    tags = _tags(policy, plan)
    quantity, price = entry_fill(order, bar["open"], side, contract, policy.cost, 1.)
    if not (inside_bracket(bar["open"], plan["signal_stop"], plan["signal_target"], side)
            and inside_bracket(price, stop, target, side)):
        reason = "ENTRY_GAP_OUTSIDE_BRACKET"
    elif not quantity or not plan["entry"]["minimum"] <= price <= plan["entry"]["maximum"]:
        reason = "ENTRY_OUTSIDE_FROZEN_RANGE_OR_CAPACITY"
    else:
        reason = None
    notional, receipt = quantity * price, bar["received_at"]
    fee = notional * policy.taker_fee
    margin = notional / policy.leverage
    if reason is None and fee + margin > book["cash"]:
        reason = "ENTRY_COLLATERAL_LIMIT"
    if reason is not None:
        paper.expire(db, book, at, now, reason, "INVALIDATED", source_received_at=receipt, reason_code=reason, **tags)
        return False
    pos = Position(side, quantity, price, book["cash"] - fee, margin, fee, notional)
    deadline = at + pd.Timedelta(minutes=plan["max_hold_minutes"])
    record = {**idea, "entry_at": at.isoformat(), "raw_entry": bar["open"], "entry_fee": fee,
              "entry_cash": book["cash"], "stop": stop, "target": target, "deadline": deadline.isoformat(),
              "last_trail": None, "planned_risk": order.planned_risk * quantity / order.contracts,
              "position": asdict(pos), "last_mark": bar["open"], "mae_quote": 0., "mfe_quote": 0.}
    economics = margin_state(quantity, price, bar["open"], side, pos.wallet, policy)
    paper.event(db, book, "ENTRY_OBSERVED", idea["id"], at, now,
                {**tags, "basis": REFERENCE_PROXY, "trade": bar, "trigger_bar": bar})
    paper.event(db, book, "HYPOTHETICAL_FILL", idea["id"], at, now,
                {**tags, **economics, "entry_at": at.isoformat(), "quantity": quantity, "price": price,
                 "fee": fee, "margin": margin, "planned_risk": record["planned_risk"],
                 "entry_basis": plan["entry_basis"], "stop": stop, "target": target,
                 "entry_equity": record["entry_cash"], "deadline": deadline.isoformat(),
                 "source_received_at": receipt, "source_handoff_at": bar["observed_at"],
                 "source_at": at.isoformat(), "delayed_confirmation": now - at >= QUARTER})
    book["position"], book["pending"] = record, None
    book["fees"] += fee
    book["turnover"] += notional
    paper.mark_account(book, pos, bar["open"])
    return True


def _fund(db, book, record, pos, policy, bar, at, now):
    period = pd.Timedelta(minutes=policy.funding_interval_minutes)
    if not policy.perpetual or not policy.funding_cost_rate or at.value % period.value:
        return
    # The simulator's signed funding rate convention makes both directions pay
    # this disclosed financing debit. No external settlement row is represented.
    paper.apply_funding(db, book, record, pos, {"settlement_at": at.isoformat(),
                        "received_at": bar["received_at"], "reference_received_at": bar["received_at"],
                        "rate": pos.direction * policy.funding_cost_rate, "modeled_rate": policy.funding_cost_rate,
                        "modeled_interval_minutes": policy.funding_interval_minutes, "observed_settlement": False,
                        "funding_basis": "SYNTHETIC_FINANCING_DEBIT", **_tags(policy, record["plan"])}, bar["open"], now)


def advance(db, book, at, now, reference_bar):
    """Reconcile one fully received reference minute in the original paper ledger."""
    at, now = _utc(at), _utc(now)
    record = book["position"] or book["pending"]
    if book["permanent_block"] or not book["cursor"] or _utc(book["cursor"]) != at or not record or not reference_bar:
        return False
    if (not all(_utc(reference_bar[name]) == wanted for name, wanted in (("open_at", at), ("close_at", at + STEP)))
            or not paper.complete_bar(reference_bar, at)):
        return False
    received = _utc(reference_bar["received_at"])
    handed = _utc(reference_bar.get("observed_at", received))
    if not at + STEP <= received <= handed <= now:
        return False
    plan = record["plan"]
    policy = load_model(plan["synthetic_model"], plan["synthetic_model_id"])
    if policy.scenario != "primary":
        raise PermissionError("Counterfactual synthetic assumptions cannot advance the primary ledger")
    if (plan["contract"] != asdict(policy.contract) or plan["trigger_basis"] != REFERENCE_PROXY
            or plan["cost"]["taker"] != policy.taker_fee or plan["cost"]["adverse_proxy"] != policy.adverse_execution):
        raise ValueError("Published plan differs from its frozen synthetic execution assumptions")
    if book["pending"]:
        entry_cutoff = (at + STEP).ceil(QUARTER) + pd.Timedelta(seconds=plan["decision_receipt_seconds"])
        if handed > entry_cutoff:
            paper.expire(db, book, at, now, "Entry minute exceeded the frozen receipt allowance",
                         source_received_at=received.isoformat(), source_handoff_at=handed.isoformat(),
                         reason_code="ENTRY_RECEIPT_TOO_LATE", **_tags(policy, plan))
            return True
    if handed > at + STEP + pd.Timedelta(seconds=plan["price_receipt_seconds"]):
        paper.problem(db, book, "Reference minute exceeded the frozen execution receipt allowance", at, now, permanent=True)
        return False
    prices = {name: float(reference_bar[name]) * policy.usd_to_nominal_usdt for name in paper.PRICE}
    bar = {**reference_bar, **prices, **_tags(policy, plan), "received_at": received.isoformat(),
           "observed_at": handed.isoformat(), "raw_kraken_prices_usd": {name: reference_bar[name] for name in paper.PRICE}}
    if book["problem"]:
        paper.event(db, book, "EVIDENCE_RECOVERED", record["id"], at, now, {"previous": book["problem"], **_tags(policy, plan)})
        book["problem"] = None
    if book["pending"] and not _open(db, book, plan, policy, bar, at, now):
        return True

    record = book["position"]
    pos = Position(**record["position"])
    _fund(db, book, record, pos, policy, bar, at, now)
    liquidation = _liquidation(pos.entry, pos.reserved / pos.remaining, pos.direction, policy)
    decision = minute_exit(tuple(prices.values()), direction=pos.direction, stop=record["stop"], target=record["target"],
                           liquidation=liquidation, at_deadline=at >= _utc(record["deadline"]))
    observed = (bar["low"], bar["high"]) if decision is None else (bar["open"],)
    swings = [pos.unrealized(mark) for mark in observed]
    record["mae_quote"], record["mfe_quote"] = min(record["mae_quote"], *swings), max(record["mfe_quote"], *swings)
    if decision is None:
        record.update(position=asdict(pos), last_mark=bar["close"], mark_source_at=bar["close_at"],
                      mark_received_at=bar["received_at"],
                      synthetic_margin=margin_state(pos.remaining, pos.entry, bar["close"], pos.direction, pos.wallet,
                                                    policy, funding=pos.funding))
        book["occupied_complete_minutes"] = book.get("occupied_complete_minutes", 0) + 1
        paper.mark_account(book, pos, bar["close"])
    else:
        exit_at = at if decision.at_open else at + STEP
        trigger = {**bar, "competing_triggers": list(decision.competing_triggers), "liquidation_price": liquidation,
                   "simulated_funding": pos.funding, "modeled_exit_price": decision.raw_price}
        paper.close_at(db, book, record, pos, decision.kind, decision.raw_price, exit_at, now, bar, trigger)
        paper.event(db, book, "EXCURSION", record["id"], at + STEP, now,
                    {"mae_quote": record["mae_quote"], "mfe_quote": record["mfe_quote"], "exit_minute_open_only": True})
    if decision is not None or at == _utc(record["entry_at"]) or (at + STEP).minute == 0:
        paper.event(db, book, "EQUITY_MARK", record["id"], at + STEP, now,
                    {"cash": book["cash"], "equity": book["equity"], "drawdown": book["drawdown"],
                     "position_open": decision is None, **_tags(policy, plan)})
    book["last_source_close"], book["last_source_received_at"] = (at + STEP).isoformat(), received.isoformat()
    book["cursor"] = (at + STEP).isoformat() if decision is None else None
    return True
