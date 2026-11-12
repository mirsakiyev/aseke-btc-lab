"""Prospective EMA entry adapter and reporting for an isolated paper ledger."""
from dataclasses import asdict
import json
from math import isfinite

import pandas as pd

from lab.execution import execution_open
from lab.learning.simulator import Contract, Cost, Order, Position, Risk, entry_fill, quantize, size_order
from lab.phase15 import paper
from lab.phase15.catalog import QUARTER, STEP
from lab.storage import utc
from lab.strategy_lab.ema_execution import BracketSetup, inside_bracket


def make_plan(lane, setup, contract: Contract, equity, now, evidence):
    """Reserve a bounded one-open IOC from the already available signal."""
    if (not isinstance(setup, BracketSetup) or setup.source_allocation
            or setup.target_r is not None or setup.trail_atr is not None):
        return None, "PAPER_PROFILE_BRACKET_REQUIRED"
    spec = lane["spec"]
    side = setup.direction
    expected_family = "spot" if spec["market"] == "SPOT" else "linear_perp"
    label = {1: "long", -1: "short"}.get(side)
    if contract.family != expected_family or contract.quote != "USDT" or label not in spec["sides"]:
        return None, "INCOMPATIBLE_MARKET_OR_SIDE"
    now = utc(now)
    clock = lane["execution_clock"]
    seconds = clock["entry_seconds"] if isinstance(clock, dict) else clock
    entry_at = execution_open(setup.available_at, pd.Timedelta(seconds=seconds))
    if not setup.decision_at <= setup.available_at <= now < entry_at:
        return None, "MISSED_OR_UNAVAILABLE_ENTRY_BOUNDARY"
    if evidence.get("publication_end_exclusive") and entry_at >= utc(evidence["publication_end_exclusive"]):
        return None, "ENTRY_OUTSIDE_PRIMARY_HORIZON"

    if not all(isfinite(value) and value > 0 for value in (setup.reference, setup.stop, setup.target_price, setup.atr, equity)):
        return None, "INVALID_SIGNAL_BRACKET"
    if not inside_bracket(setup.reference, setup.stop, setup.target_price, side):
        return None, "INVALID_SIGNAL_BRACKET"
    stop = quantize(setup.stop, contract.price_tick, side < 0)
    target = quantize(setup.target_price, contract.price_tick, side > 0)
    if stop <= 0 or target <= 0 or not inside_bracket(setup.reference, stop, target, side):
        return None, "INVALID_ROUNDED_SIGNAL_BRACKET"

    cost_values = spec["cost"]
    cost = Cost(cost_values["taker"], cost_values["adverse_proxy"])
    order, reason = size_order(setup, contract, cost, Risk(**lane["risk"]), equity, spec["execution"])
    if order is None:
        return None, reason
    minimum, maximum = sorted((stop, target))
    if side == 1:
        maximum = min(maximum, order.limit_price)
    else:
        minimum = max(minimum, order.limit_price)
    if minimum >= maximum:
        return None, "EMPTY_ENTRY_RANGE"

    profile_rules = spec["session"]
    return {
        "direction": side, "eligible_from": entry_at, "entry_expiry": entry_at + STEP,
        "entry": {"minimum": minimum, "maximum": maximum, "reference": setup.reference, "trigger": setup.reference},
        "entry_basis": "TRADE_MINUTE_OPEN_WITH_ADVERSE", "stop": stop, "target": target,
        "signal_stop": setup.stop, "signal_target": setup.target_price, "target_r": None, "trailing_atr": None,
        "target_basis": "CONFIRMATION_CLOSE", "signal_at": setup.decision_at, "signal_available_at": setup.available_at,
        "trigger_basis": "MARK" if expected_family == "linear_perp" else "TRADE",
        "exit_execution_basis": "TRADE", "session_flat": None, "max_hold_minutes": profile_rules["max_hold_minutes"],
        "contract": asdict(contract), "cost": cost_values, "order": asdict(order),
        "sizing": {"contracts": order.contracts, "planned_risk": order.planned_risk, "risk_budget": order.risk_budget,
                   "base": order.contracts * contract.base_per_contract, "leverage": order.leverage},
        "execution_terms": evidence.get("execution_terms", {}), "evidence": evidence,
    }, None


def open_at(db, book, trade, mark, at, now):
    idea = book["pending"]
    plan = idea["plan"]
    if at != utc(plan["eligible_from"]) or at <= utc(idea["published_at"]) or at >= utc(plan["entry_expiry"]):
        raise ValueError("EMA paper entry must use the committed opening instant after publication")
    receipt = max(utc(bar["received_at"]) for bar in (trade, mark))
    handoff = max(utc(bar.get("observed_at", bar["received_at"])) for bar in (trade, mark))
    if max(receipt, handoff) > (at + STEP).ceil(QUARTER) + pd.Timedelta(minutes=2):
        paper.expire(db, book, at, now, "Entry minute arrived outside its registered receipt allowance",
                     source_received_at=receipt.isoformat(), source_handoff_at=handoff.isoformat(),
                     reason_code="ENTRY_RECEIPT_TOO_LATE")
        return False
    side = plan["direction"]
    stop, target = plan["stop"], plan["target"]
    contract = Contract.load(plan["contract"])
    cost = Cost(plan["cost"]["taker"], plan["cost"]["adverse_proxy"])
    order = Order(**plan["order"])
    contracts, price = entry_fill(order, trade["open"], side, contract, cost, 1.)
    if not all((inside_bracket(trade["open"], plan["signal_stop"], plan["signal_target"], side),
                inside_bracket(mark["open"], stop, target, side),
                inside_bracket(price, stop, target, side))):
        reason = "ENTRY_GAP_OUTSIDE_BRACKET"
    elif not contracts or not plan["entry"]["minimum"] <= price <= plan["entry"]["maximum"]:
        reason = "ENTRY_OUTSIDE_FROZEN_RANGE_OR_CAPACITY"
    else:
        reason = None
    quantity = contracts * contract.base_per_contract
    notional = quantity * price
    terms = plan.get("execution_terms", {})
    if reason is None and (notional < terms.get("min_quote", 0.) or contracts > terms.get("max_contracts", float("inf"))):
        reason = "ENTRY_OUTSIDE_INSTRUMENT_SIZE_LIMITS"
    if reason:
        paper.expire(db, book, at, now, reason, "INVALIDATED", source_received_at=receipt.isoformat(), reason_code=reason)
        return False

    fee, margin = notional * cost.fee, notional / order.leverage
    if fee + margin > book["cash"]:
        paper.expire(db, book, at, now, "Insufficient free paper collateral", "INVALIDATED",
                     source_received_at=receipt.isoformat(), reason_code="ENTRY_COLLATERAL_LIMIT")
        return False
    pos = Position(side, quantity, price, book["cash"] - fee, margin, fee, notional)
    deadline = at + pd.Timedelta(minutes=plan["max_hold_minutes"])
    record = {
        **idea, "entry_at": at.isoformat(), "raw_entry": trade["open"], "entry_fee": fee,
        "entry_cash": book["cash"], "stop": stop, "target": target, "deadline": deadline.isoformat(), "last_trail": None,
        "planned_risk": order.planned_risk * contracts / order.contracts, "position": asdict(pos), "last_mark": mark["open"],
        "mae_quote": 0., "mfe_quote": 0.,
    }
    paper.event(db, book, "ENTRY_OBSERVED", idea["id"], at, now,
                {"basis": "TRADE_MINUTE_OPEN", "trade": trade, "trigger_bar": mark})
    paper.event(db, book, "HYPOTHETICAL_FILL", idea["id"], at, now, {
        "entry_at": at.isoformat(), "quantity": quantity, "price": price, "fee": fee, "margin": margin,
        "planned_risk": record["planned_risk"], "entry_basis": plan["entry_basis"],
        "contracts": contracts if contract.family == "linear_perp" else None,
        "stop": stop, "target": target, "entry_equity": record["entry_cash"], "deadline": deadline.isoformat(),
        "source_received_at": receipt.isoformat(), "source_handoff_at": handoff.isoformat(), "source_at": at.isoformat(),
        "delayed_confirmation": now - at >= QUARTER,
    })
    book["position"], book["pending"] = record, None
    book["fees"] += fee
    book["turnover"] += notional
    paper.mark_account(book, pos, mark["open"])
    return True


def advance(db, book, at, now, trade, mark, funding=()):
    """Reconcile one minute at a committed cursor using first-received source evidence."""
    at, now = utc(at), utc(now)
    if book["permanent_block"] or not book["cursor"] or utc(book["cursor"]) != at:
        return False
    record = book["position"] or book["pending"]
    if not record:
        return False
    if record["plan"]["trigger_basis"] == "TRADE":
        mark = trade
    if not all(paper.complete_bar(bar, at) and utc(bar["received_at"]) <=
               utc(bar.get("observed_at", bar["received_at"])) <= now for bar in (trade, mark)):
        return False
    funding = tuple(funding)
    if any(not at <= utc(row["settlement_at"]) < at + STEP or not utc(row["settlement_at"]) <= utc(row["received_at"])
           or not isfinite(row["rate"]) for row in funding):
        paper.problem(db, book, "Funding timestamp, availability, or rate is invalid", at, now, permanent=True)
        return False
    if any(utc(row["received_at"]) > now for row in funding):
        return False
    if book["pending"]:
        if book["problem"]:
            paper.event(db, book, "EVIDENCE_RECOVERED", book["pending"]["id"], at, now, {"previous": book["problem"]})
            book["problem"] = None
        if not open_at(db, book, trade, mark, at, now):
            return True

    record = book["position"]
    if not record:
        return False
    advanced = paper.minute(db, book, at, now, trade, mark, funding)
    if not advanced:
        return False

    # A complete occupied minute bounds both excursions. On an exit minute,
    # only the opening mark is attributable without a finer path.
    held = book["position"] is not None
    pos = Position(**record["position"])
    observed = (mark["low"], mark["high"]) if held else (mark["open"],)
    swings = [pos.direction * pos.remaining * (value - pos.entry) for value in observed]
    record["mae_quote"] = min(record["mae_quote"], *swings)
    record["mfe_quote"] = max(record["mfe_quote"], *swings)
    if held:
        book["occupied_complete_minutes"] = book.get("occupied_complete_minutes", 0) + 1
    else:
        paper.event(db, book, "EXCURSION", record["id"], at + STEP, now,
                    {"mae_quote": record["mae_quote"], "mfe_quote": record["mfe_quote"], "exit_minute_open_only": True})
    if not held or at == utc(record["entry_at"]) or (at + STEP).minute == 0:
        paper.event(db, book, "EQUITY_MARK", record["id"], at + STEP, now,
                    {"cash": book["cash"], "equity": book["equity"], "drawdown": book["drawdown"],
                     "position_open": held})
    return True


def report(db, lane):
    result = paper.book_report(db, lane)
    closed = [json.loads(row[0]) for row in db.execute(
        "SELECT body FROM event WHERE book=? AND kind='CLOSED' ORDER BY source_at,rowid", (lane["book_id"],))]
    wins = [row["net"] for row in closed if row["net"] > 0]
    losses = [row["net"] for row in closed if row["net"] < 0]
    excursions = [json.loads(row[0]) for row in db.execute(
        "SELECT body FROM event WHERE book=? AND kind='EXCURSION'", (lane["book_id"],))]
    marks = [{"source_at": stamp, **json.loads(body)} for stamp, body in db.execute(
        "SELECT source_at,body FROM event WHERE book=? AND kind='EQUITY_MARK' ORDER BY source_at,rowid", (lane["book_id"],))]
    return {
        **result, "closed_count": len(closed), "wins": len(wins), "losses": len(losses),
        "win_rate": len(wins) / len(closed) if closed else None,
        "average_winner": sum(wins) / len(wins) if wins else None,
        "average_loser": sum(losses) / len(losses) if losses else None,
        "profit_factor": sum(wins) / -sum(losses) if losses else None,
        "net_return": (result["equity"] / result["starting_equity"] - 1) if closed or result["position"] else None,
        "maximum_adverse_excursion": min((item["mae_quote"] for item in excursions), default=None),
        "maximum_favorable_excursion": max((item["mfe_quote"] for item in excursions), default=None),
        "occupied_complete_minutes": result.get("occupied_complete_minutes", 0), "equity_curve": marks,
        "primary_sample_floor": 25,
    }
