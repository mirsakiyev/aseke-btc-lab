from collections import Counter
from dataclasses import asdict
from decimal import Decimal
import json
from math import isfinite

import pandas as pd

from lab.config import fingerprint
from lab.learning.simulator import Contract, Cost, Order, Position, entry_fill, fill_price, liquidation_price, quantize, triggers
from lab.phase15.catalog import QUARTER, STEP, encode
from lab.storage import utc


PRICE = ("open", "high", "low", "close")


def fixed_target(entry, stop, target_r, side, tick):
    if not target_r:
        return None
    entry, stop, target_r = (Decimal(str(value)) for value in (entry, stop, target_r))
    return quantize(entry + side * target_r * abs(entry - stop), tick, side > 0)


def initial(lane):
    equity = lane["risk"]["starting_equity"]
    return {"book_id": lane["book_id"], "starting_equity": equity, "cash": equity, "equity": equity,
            "peak_equity": equity, "drawdown": 0., "cursor": None, "pending": None, "position": None,
            "problem": None, "permanent_block": False, "funding": 0., "fees": 0., "liquidation_cost": 0.,
            "turnover": 0., "gross": 0., "raw_gross": 0., "unfunded_deficit": 0., "last_source_close": None}


def load(db, lane):
    result = db.execute("SELECT state FROM book WHERE id=?", (lane["book_id"],)).fetchone()
    return json.loads(result[0]) if result else initial(lane)


def save(db, book):
    db.execute("INSERT INTO book VALUES (?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state",
               (book["book_id"], encode(book)))


def event(db, book, kind, idea, at, now, details):
    stamp = utc(at).isoformat()
    identity = [book["book_id"], kind, idea, stamp]
    if kind == "USER_REPORTED_EXECUTION":
        identity.append(details)
    key = fingerprint(identity)
    db.execute("INSERT OR IGNORE INTO event VALUES (?,?,?,?,?,?,?)", (key, book["book_id"], kind, idea,
               stamp, now.isoformat(), encode(details)))


def publish(db, book, analysis, now):
    if analysis["state"] != "ELIGIBLE_CONDITIONAL_IDEA" or not analysis.get("plan"):
        return None
    boundary = utc(analysis["source_cutoff"])
    identifier = fingerprint([book["book_id"], boundary.isoformat()])
    if db.execute("SELECT 1 FROM event WHERE book=? AND idea=? AND kind IN ('PUBLISHED','REJECTED')",
                  (book["book_id"], identifier)).fetchone():
        return None
    if book["pending"] or book["position"] or book["problem"]:
        event(db, book, "REJECTED", identifier, boundary, now, {"reason": "Book occupied or unresolved"})
        return None
    plan = analysis["plan"]
    if not boundary <= now < utc(plan["eligible_from"]) < utc(plan["entry_expiry"]):
        raise ValueError("Invalid prospective idea eligibility times")
    idea = json.loads(encode({"id": identifier, "published_at": now.isoformat(), "boundary": boundary.isoformat(),
                              "direction": analysis["direction"], "strategy_version": analysis["strategy_version"],
                              "exit_policy": analysis["exit_policy"], "model_version": None, "plan": plan}))
    event(db, book, "PUBLISHED", identifier, now, now, idea)
    book["pending"], book["cursor"] = idea, utc(plan["eligible_from"]).isoformat()
    return idea


def problem(db, book, reason, at, now, *, permanent=False):
    idea = book["position"] or book["pending"]
    if book["problem"] != reason:
        event(db, book, "UNRESOLVED", idea["id"], at, now, {"reason": reason, "permanent": permanent})
    book["problem"], book["permanent_block"] = reason, permanent


def expire(db, book, at, now, reason, kind="EXPIRED"):
    event(db, book, kind, book["pending"]["id"], at, now, {"reason": reason})
    book["pending"], book["cursor"] = None, None


def complete_bar(bar, at):
    return (bar and utc(bar["open_at"]) == at and utc(bar["close_at"]) == at+STEP <= utc(bar["received_at"])
            and all(isfinite(bar[key]) and bar[key] > 0 for key in PRICE)
            and bar["high"] >= max(bar[key] for key in ("open", "low", "close"))
            and bar["low"] <= min(bar[key] for key in ("open", "high", "close")))


def mark_account(book, pos, price):
    book["cash"] = pos.wallet
    book["equity"] = pos.wallet + pos.unrealized(price)
    book["peak_equity"] = max(book["peak_equity"], book["equity"])
    book["drawdown"] = min(book["drawdown"], book["equity"]/book["peak_equity"]-1)


def open_at(db, book, trade, mark, at, now):
    idea = book["pending"]
    plan = idea["plan"]
    if at >= utc(plan["entry_expiry"]):
        expire(db, book, at, now, "Entry window elapsed")
        return
    if at < utc(plan["eligible_from"]) or at <= utc(idea["published_at"]):
        raise ValueError("A modeled entry must follow actual publication")
    if max(utc(bar["received_at"]) for bar in (trade,mark)) > (at+STEP).ceil(QUARTER)+pd.Timedelta(minutes=2):
        problem(db, book, "Entry minute arrived outside its declared receipt allowance", at, now, permanent=True)
        return
    side = plan["direction"]
    if side*(mark["open"]-plan["stop"]) <= 0:
        expire(db, book, at, now, "Stop invalidation at the trigger-basis open", "INVALIDATED")
        return
    if side*(trade["open"]-plan["entry"]["trigger"]) < 0:
        # The whole bar can invalidate a still-unfilled idea without guessing an entry from its extremes.
        if side*((mark["low"] if side > 0 else mark["high"])-plan["stop"]) <= 0:
            expire(db, book, at+STEP, now, "Stop invalidation before any eligible open", "INVALIDATED")
        return
    contract = Contract.load(plan["contract"])
    cost = Cost(plan["cost"]["taker"], plan["cost"]["adverse_proxy"])
    order = Order(**plan["order"])
    contracts, price = entry_fill(order, trade["open"], side, contract, cost, 1.)
    if not contracts or not plan["entry"]["minimum"] <= price <= plan["entry"]["maximum"]:
        expire(db, book, at, now, "Adverse-adjusted entry outside the frozen range or capacity", "INVALIDATED")
        return
    quantity = contracts*contract.base_per_contract
    notional = quantity*price
    fee = notional*cost.fee
    margin = notional/order.leverage
    pos = Position(side, quantity, price, book["cash"]-fee, margin, fee, notional)
    deadline = at+pd.Timedelta(minutes=plan["max_hold_minutes"])
    if plan["session_flat"]:
        deadline = min(deadline, utc(plan["session_flat"]))
    target = fixed_target(price, plan["stop"], plan["target_r"], side, contract.price_tick)
    record = {**idea, "entry_at": at.isoformat(), "raw_entry": trade["open"], "entry_fee": fee, "entry_cash": book["cash"],
              "stop": plan["stop"], "target": target, "deadline": deadline.isoformat(), "last_trail": None,
              "planned_risk": order.planned_risk*contracts/order.contracts, "position": asdict(pos), "last_mark": mark["open"]}
    event(db, book, "ENTRY_OBSERVED", idea["id"], at, now, {"basis": "TRADE_MINUTE_OPEN", "trade": trade, "trigger_bar": mark})
    event(db, book, "HYPOTHETICAL_FILL", idea["id"], at, now, {"quantity": quantity, "price": price, "fee": fee,
          "margin": margin, "planned_risk": record["planned_risk"], "entry_basis": "TRADE_MINUTE_OPEN_WITH_ADVERSE"})
    book["position"], book["pending"] = record, None
    book["fees"] += fee
    book["turnover"] += notional
    mark_account(book, pos, mark["open"])


def apply_funding(db, book, record, pos, settlement, mark_open, now):
    at = utc(settlement["settlement_at"])
    if at <= utc(record["entry_at"]):
        return
    key = fingerprint([book["book_id"], "FUNDING", record["id"], at.isoformat()])
    if db.execute("SELECT 1 FROM event WHERE key=?", (key,)).fetchone():
        return
    old = pos.funding
    pos.fund(settlement["rate"], mark_open)
    amount = pos.funding-old
    book["funding"] += amount
    event(db, book, "FUNDING", record["id"], at, now, {**settlement, "mark_open_proxy": mark_open, "cash_flow": amount})


def close_at(db, book, record, pos, action, raw, at, now, trade, mark):
    plan = record["plan"]
    contract = Contract.load(plan["contract"])
    cost = Cost(plan["cost"]["taker"], plan["cost"]["adverse_proxy"])
    price = fill_price(raw, -pos.direction, contract, cost)
    units = pos.remaining
    forced = action == "liquidation"
    pos.reduce(units, price, cost, contract.liquidation_fee_rate if forced else 0., liquidated=forced)
    raw_gross = pos.direction*units*(raw-record["raw_entry"])
    net = pos.wallet-record["entry_cash"]
    settled = {"reason": action, "entry_at": record["entry_at"], "exit_at": at.isoformat(), "exit_price": price, "raw_exit": raw,
               "gross_price_pnl": pos.realized, "gross_before_adverse": raw_gross, "adverse_cost_in_gross": raw_gross-pos.realized,
               "fees": pos.fees, "funding": pos.funding, "liquidation_cost": pos.liquidation_cost,
               "unfunded_deficit": pos.unfunded_deficit, "net": net, "net_r": net/record["planned_risk"],
               "planned_risk": record["planned_risk"], "realized_loss": max(0., -net),
               "loss_over_plan": max(0., -net-record["planned_risk"]), "turnover": pos.turnover,
               "holding_minutes": (at-utc(record["entry_at"])).total_seconds()/60,
               "trigger_basis": plan["trigger_basis"], "execution_basis": plan["exit_execution_basis"],
               "effective_stop": record["stop"], "trade": trade, "trigger_bar": mark}
    event(db, book, "CLOSED", record["id"], at, now, settled)
    book["gross"] += pos.realized
    book["raw_gross"] += raw_gross
    book["fees"] += pos.fees-record["entry_fee"]
    book["turnover"] += price*units
    book["liquidation_cost"] += pos.liquidation_cost
    book["unfunded_deficit"] += pos.unfunded_deficit
    mark_account(book, pos, price)
    book["position"], book["cursor"] = None, None


def minute(db, book, at, now, trade, mark, funding=(), trail=None):
    if book["permanent_block"] or not book["cursor"] or utc(book["cursor"]) != at:
        return False
    if not all(complete_bar(bar, at) and utc(bar["received_at"]) <= now for bar in (trade,mark)):
        return False
    if any(not at <= utc(row["settlement_at"]) < at+STEP or not utc(row["settlement_at"]) <= utc(row["received_at"]) <= now
           or not isfinite(row["rate"]) for row in funding):
        problem(db, book, "Funding timestamp, availability, or rate is invalid", at, now, permanent=True)
        return False
    if book["problem"]:
        event(db, book, "EVIDENCE_RECOVERED", (book["position"] or book["pending"])["id"], at, now, {"previous": book["problem"]})
        book["problem"] = None
    if book["pending"]:
        open_at(db, book, trade, mark, at, now)
    if book["permanent_block"]:
        return False
    if record := book["position"]:
        pos = Position(**record["position"])
        plan, side = record["plan"], pos.direction
        contract = Contract.load(plan["contract"])
        cost = Cost(plan["cost"]["taker"], plan["cost"]["adverse_proxy"])
        for settled in funding:
            if utc(settled["settlement_at"]) == at:
                apply_funding(db, book, record, pos, settled, mark["open"], now)
        if trail and plan["trailing_atr"] and utc(trail["available_at"]) <= at and utc(record["entry_at"]) < utc(trail["close_at"]):
            last = record["last_trail"]
            if not last or utc(last) < utc(trail["close_at"]):
                proposal = quantize(trail["close"]-side*plan["trailing_atr"]*trail["atr"], contract.price_tick, side < 0)
                record["stop"] = max(record["stop"], proposal) if side > 0 else min(record["stop"], proposal)
                record["last_trail"] = trail["close_at"]
                event(db, book, "TRAIL_UPDATED", record["id"], at, now, {**trail, "effective_stop": record["stop"]})
        record["position"] = asdict(pos)
        if contract.family == "linear_perp" and pos.remaining*mark["high"] > contract.tier_notional:
            mark_account(book, pos, record["last_mark"])
            problem(db, book, "Unmodeled maintenance tier crossed", at, now, permanent=True)
            return False
        liq = liquidation_price(pos.entry, pos.reserved/pos.remaining, side, contract, cost.fee) if contract.family == "linear_perp" else None
        prices = tuple(mark[key] for key in PRICE)
        opening = triggers(prices, side, record["stop"], record["target"], liq, True)
        if opening or at >= utc(record["deadline"]):
            action = "liquidation" if opening and opening[0][0] == "liquidation" else (
                "time_exit" if at >= utc(record["deadline"]) else opening[0][0])
            close_at(db, book, record, pos, action, trade["open"], at, now, trade, mark)
        else:
            hits = triggers(prices, side, record["stop"], record["target"], liq, False)
            off_grid = [settled for settled in funding if utc(settled["settlement_at"]) > at]
            if len(hits) > 1 or (hits and off_grid):
                mark_account(book, pos, record["last_mark"])
                problem(db, book, "Intraminute ordering of exit or funding events is ambiguous", at, now, permanent=True)
                return False
            for settled in off_grid:
                apply_funding(db, book, record, pos, settled, mark["open"], now)
            if off_grid and contract.family == "linear_perp":
                liq = liquidation_price(pos.entry, pos.reserved/pos.remaining, side, contract, cost.fee)
                if triggers(prices, side, record["stop"], None, liq, False)[:1] == [("liquidation", liq)]:
                    record["position"] = asdict(pos)
                    mark_account(book, pos, record["last_mark"])
                    problem(db, book, "Funding-adjusted liquidation ordering is ambiguous", at, now, permanent=True)
                    return False
            if hits:
                action, level = hits[0]
                raw = (trade["low"] if side > 0 else trade["high"]) if contract.family == "linear_perp" else level
                close_at(db, book, record, pos, action, raw, at+STEP, now, trade, mark)
            else:
                record.update(position=asdict(pos), last_mark=mark["close"])
                mark_account(book, pos, mark["close"])
    book["last_source_close"] = (at+STEP).isoformat()
    book["cursor"] = (at+STEP).isoformat() if book["pending"] or book["position"] else None
    return True


def book_report(db, lane):
    book = load(db, lane)
    rows = [{"type": kind, "idea_id": idea, "source_at": source, "recorded_at": received, **json.loads(body)}
            for kind, idea, source, received, body in db.execute("SELECT kind,idea,source_at,recorded_at,body FROM event "
                                                                "WHERE book=? ORDER BY recorded_at,rowid", (lane["book_id"],))]
    counts = Counter(row["type"] for row in rows)
    closed = [row for row in rows if row["type"] == "CLOSED"]
    gains = sorted((body["net"] for body in closed if body["net"] > 0), reverse=True)
    reasons = Counter(row["reason"] for row in rows if row["type"] in {"INVALIDATED", "EXPIRED", "REJECTED", "SETUP_REJECTED", "UNRESOLVED"})
    record = book["position"]
    position = Position(**record["position"]) if record else None
    cash_change = book["cash"]-book["starting_equity"]
    attributed_cash_change = book["gross"]+book["funding"]-book["fees"]-book["liquidation_cost"]+book["unfunded_deficit"]
    return {**book, "book_status": "UNRESOLVED" if book["problem"] else "PAPER_POSITION_OPEN" if position else
            "AWAITING_ENTRY" if book["pending"] else "FLAT", "counts": dict(counts), "reasons": dict(reasons),
            "closed_net": sum(item["net"] for item in closed), "expectancy": sum(item["net"] for item in closed)/len(closed) if closed else None,
            "accounting_complete": not bool(book["problem"]), "adverse_cost_in_realized_gross": book["raw_gross"]-book["gross"],
            "cash_change": cash_change, "cash_reconciliation_difference": cash_change-attributed_cash_change,
            "closed_fees": sum(item["fees"] for item in closed), "closed_funding": sum(item["funding"] for item in closed),
            "expectancy_r": sum(item["net_r"] for item in closed)/len(closed) if closed else None,
            "top_three_share_of_positive_gains": sum(gains[:3])/sum(gains) if gains else None,
            "average_hold_minutes": sum(item["holding_minutes"] for item in closed)/len(closed) if closed else None,
            "available_cash": position.free if position else book["cash"], "reserved_margin": position.reserved if position else 0.,
            "reserved_risk": record["planned_risk"] if record else 0., "exposure": position.remaining*record["last_mark"] if position else 0.,
            "unrealized": position.unrealized(record["last_mark"]) if position else 0.,
            "last_ideas": [row for row in rows if row["type"] == "PUBLISHED"][-5:][::-1],
            "last_events": rows[-12:][::-1], "recent_closed": closed[-5:][::-1]}


def user_execution(db, book, idea_id, details, now):
    found = db.execute("SELECT body FROM event WHERE book=? AND idea=? AND kind='PUBLISHED'", (book["book_id"], idea_id)).fetchone()
    if not found:
        raise ValueError("Select a published Phase 15 idea")
    idea = json.loads(found[0])
    at = utc(details["executed_at"])
    units = {"base"} if idea["plan"]["contract"]["family"] == "spot" else {"base", "contracts"}
    price, quantity = float(details["price"]), float(details["quantity"])
    if not utc(idea["published_at"]) <= at <= now or details["unit"] not in units or not all(isfinite(x) and x > 0 for x in (price,quantity)):
        raise ValueError("Invalid execution time, quantity, price, or units")
    event(db, book, "USER_REPORTED_EXECUTION", idea_id, at, now,
          {"price": price, "quantity": quantity, "unit": details["unit"], "executed_at": at.isoformat()})
