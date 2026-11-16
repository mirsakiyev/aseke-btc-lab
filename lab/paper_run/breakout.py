"""Reuse the frozen Breakout signals, instrument rules and ATR trail."""

from dataclasses import asdict
from math import isfinite

import pandas as pd

from lab.learning.simulator import Contract, Order, Risk, quantize
from lab.phase9.ideas import current_rules
from lab.phase13.future import expected_funding
from lab.phase13.registry import checked
from lab.phase14.outcomes import market_evidence
from lab.phase15 import catalog, paper, realtime, worker
from lab.phase15.feeds import futures_source, swing_trails
from lab.paper_run import execution, sources
from lab.storage import utc


LABELS = {"day/fixed": "Breakout Day Fixed", "swing/fixed": "Breakout Swing Fixed",
          "swing/trail": "Breakout Swing ATR Trail", "position/fixed": "Breakout Position Fixed"}


def lanes(frozen):
    result = {}
    for key, label in LABELS.items():
        lane = frozen["body"]["lanes"][key]
        spot = key.startswith("position/")
        model = "REALTIME_SPOT_TICKER_REFERENCE_PAPER" if spot else realtime.MODE
        result[f"breakout/{key}"] = {**lane, "family": "BREAKOUT", "label": label, "legacy_book_id": lane["book_id"],
            "legacy_registration": frozen["id"], "source": sources.SPOT if spot else sources.MARK,
            "symbol": lane["mode"]["symbol"], "long_only": spot,
            "price_basis": "KUCOIN_SPOT_LAST_PRICE_AT_TICKER_SNAPSHOT" if spot else "KUCOIN_XBTUSDTM_MARK",
            "entry_model": model, "exit_model": model, "timeframe": lane["profile_rules"]["decision"]}
    return result


def confirmation_rows(record, selected, books, now):
    body = record["body"]
    return worker.opportunity_rows({**record, "body": {**body, "lanes": selected}}, books, now)


def plan(analysis, lane, metadata):
    original = analysis["plan"]
    plan = {**original, "atr": float(analysis["structure"]["atr"]),
            "cost": {**original["cost"], "adverse_proxy": 0.},
            "historical_cap_uses_original_sizing_assumptions": True,
            "entry": {**original["entry"], "paper_basis": lane["entry_model"]},
            "trigger_basis": lane["price_basis"], "exit_execution_basis": lane["exit_model"],
            "cancel_if": ["Expiry", "Reference crosses stop", "Reference outside frozen entry range", "Missing source evidence"]}
    if lane["source"] == sources.MARK:
        contract = metadata.get("contract", {})
        observed = {key: contract.get(key) for key in ("currentFundingRateGranularity", "fundingRateGranularity",
                                                        "nextFundingRateDateTime")}
        if expected_funding({"contract": observed}, now := utc(analysis["as_of"]), now + pd.Timedelta(minutes=1)) is None:
            return None
        plan["funding_schedule"] = observed
    return plan


def entry_terms(book, lane, plan, metadata, config, price, now):
    if lane["source"] == sources.MARK:
        schedule = plan["funding_schedule"]
        if expected_funding({"contract": schedule}, now, now + pd.Timedelta(minutes=1)) is None:
            return "DATED_FUNDING_SCHEDULE_UNAVAILABLE", None
        return realtime._entry_terms(book, lane, plan, metadata, config, price, now)
    try:
        age = now - execution.stamp(metadata["as_of"])
        if not pd.Timedelta(0) <= age <= pd.Timedelta(hours=config["ideas"]["maximum_metadata_age_hours"]):
            return "STALE_INSTRUMENT_RULES", None
        contract = Contract.load(plan["contract"])
        if contract != current_rules("SPOT", metadata, config):
            return "INSTRUMENT_RULES_CHANGED", None
        limit, minimum = (float(metadata["spot"][key]) for key in ("maxBaseOrderSize", "minFunds"))
        if not all(isfinite(x) and x > 0 for x in (limit, minimum)):
            return "INVALID_INSTRUMENT_LIMITS", None
    except (KeyError, TypeError, ValueError):
        return "MISSING_INSTRUMENT_RULES", None
    order, risk = Order(**plan["order"]), Risk(**lane["risk"])
    if plan["direction"] != 1 or order.leverage != 1 or risk.leverage != 1:
        return "SPOT_REQUIRES_LONG_CASH", None
    rate, stop = plan["cost"]["taker"], plan["stop"]
    loss = price - stop + rate * (price + stop)
    cash = book["cash"]
    budget = min(order.risk_budget, cash * risk.risk_fraction)
    cap = min(order.quote_cap, cash * risk.notional_cap, contract.tier_notional)
    base = quantize(min(order.contracts, limit, budget / loss, cap / price, cash / (price * (1 + rate))),
                    contract.quantity_step)
    value = base * price
    if base < contract.min_contracts or value < minimum:
        return "INSUFFICIENT_SPOT_SIZE_OR_FUNDS", None
    return None, {"contracts": base, "quantity": base, "notional": value, "fee": value * rate, "margin": value,
                  "reference_taker_fee_rate": rate, "reference_fee_basis": plan["cost"].get("basis"),
                  "planned_risk": base * loss, "risk_budget": budget, "target_basis": "ACTUAL_ENTRY_AND_INITIAL_STOP",
                  "target": paper.fixed_target(price, stop, plan["target_r"], 1, contract.price_tick)}


def trail(db, book, lane, now):
    record, step = book["position"], pd.Timedelta(hours=4)
    if lane["exit"] != "atr_trail_v1" or record.get("exit_observed"):
        return
    check = now.floor(catalog.STEP).isoformat()
    if record.get("trail_check") == check:
        return
    record["trail_check"] = check
    end = now.floor(step)
    start = max(utc(record["entry_at"]), end - step)
    if end <= start or (record["last_trail"] and utc(record["last_trail"]) >= end):
        return
    rows = swing_trails(lane, start, end, now)
    proposals = [row for row in rows.values() if utc(record["entry_at"]) < utc(row["close_at"])
                 and utc(row["available_at"]) <= now and (not record["last_trail"] or utc(record["last_trail"]) < utc(row["close_at"]))]
    for row in sorted(proposals, key=lambda item: item["close_at"]):
        side = record["plan"]["direction"]
        tick = record["plan"]["contract"]["price_tick"]
        proposal = quantize(row["close"] - side * record["plan"]["trailing_atr"] * row["atr"], tick, side < 0)
        record["prior_stop"] = record["stop"]
        record["stop"] = max(record["stop"], proposal) if side > 0 else min(record["stop"], proposal)
        record["last_trail"] = row["close_at"]
        record["stop_effective_at"] = now.isoformat()
        paper.event(db, book, "TRAIL_UPDATED", record["id"], now, now, {**row, "effective_stop": record["stop"]})


def settled(body, start, end, now):
    amendment = futures_source(body)
    result = {}
    for actual, _, receipt, path, row in market_evidence(amendment, start - pd.Timedelta(seconds=5),
                                                        end + pd.Timedelta(seconds=6), basis="funding"):
        slot = actual.round("min")
        if receipt > now or abs(actual - slot) > pd.Timedelta(seconds=5) or not isfinite(row["rate"]):
            continue
        checked(path, path.stem)
        if slot in result and result[slot]["rate"] != row["rate"]:
            result[slot] = {"rate": None, "conflict": True}
        else:
            result[slot] = {"rate": row["rate"], "actual_at": actual.isoformat(), "received_at": receipt.isoformat(),
                            "sha256": path.stem}
    return result
