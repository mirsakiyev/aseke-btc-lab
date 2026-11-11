from dataclasses import asdict

import numpy as np
import pandas as pd

from lab.data import OHLCV
from lab.learning.features import build_features, directions
from lab.learning.simulator import Cost, Risk, fill_price, make_setup, quantize, size_order
from lab.phase9.ideas import current_rules, frame_digest, fresh, timedelta_age
from lab.phase9.public import INTERVALS
from lab.phase10.worker import OUTPUT, cache_read, decision_key
from lab.phase15.catalog import STEP
from lab.phase15.paper import fixed_target
from lab.storage import read_json, utc


def current_inputs(lane, config, now):
    name = lane["profile"]
    step = pd.Timedelta(lane["profile_rules"]["decision"])
    cutoff = (now-pd.Timedelta(seconds=config["ideas"]["completed_candle_grace_seconds"])).floor(step)
    path = OUTPUT / "decisions" / name / f"{decision_key(name, cutoff, config['ideas']['version'])}.json"
    prior = read_json(path).get("idea") if path.exists() else None
    frame = cache_read(name)
    if not frame.empty:
        native = pd.Timedelta(INTERVALS[lane["mode"]["warmup_interval"]])
        frame = frame.loc[(frame.open_at >= cutoff-native*lane["mode"]["warmup_bars"]) & (frame.received_at <= now)]
    return cutoff, frame, prior.get("quote_observed") if prior else None, prior.get("mark_observed") if prior else None


def price_summary(order, setup, contract, cost):
    side, bound = setup.direction, order.limit_price
    stop = quantize(setup.stop, contract.price_tick, side < 0)
    base = order.contracts * contract.base_per_contract
    target = fixed_target(bound, stop, setup.target_r, side, contract.price_tick)

    def stop_loss(model):
        executable = fill_price(stop, -side, contract, model)
        return base*(side*(bound-executable)+model.fee*(bound+executable))

    return {"stop": stop, "target": target, "base_quantity": base, "notional": base*bound,
            "margin": base*bound/order.leverage, "initial_stop_loss": stop_loss(cost),
            "double_cost_stop_loss": stop_loss(Cost(cost.fee*2, cost.adverse*2))}


def analyze(lane, config, frame, cutoff, now, quote, mark, metadata, equity, *, holdout_end):
    profile, mode = lane["profile_rules"], lane["mode"]
    policy = config["ideas"]
    decision_step = pd.Timedelta(profile["decision"])
    grace = pd.Timedelta(seconds=policy["completed_candle_grace_seconds"])
    spot = mode["trade_type"] == "SPOT"
    next_close = cutoff + decision_step
    session = {"entry_cutoff": (cutoff.normalize()+pd.Timedelta(minutes=profile["entry_cutoff_minute_utc"]))
               if "entry_cutoff_minute_utc" in profile else None,
               "flat": (cutoff.normalize()+pd.Timedelta(minutes=profile["flat_minute_utc"]))
               if "flat_minute_utc" in profile else None}
    row = {"profile": lane["profile"], "instrument": mode["symbol"], "market": mode["trade_type"],
           "timeframe": profile["decision"], "strategy": lane["strategy"], "exit_policy": lane["exit"],
           "strategy_version": lane["strategy_version"], "model_version": None, "classification": "EXPERIMENTAL",
           "evidence_basis": "RULES_ONLY", "as_of": now, "source_cutoff": cutoff, "latest_completed": None,
           "next_decision_at": next_close+grace, "direction": None, "session": session,
           "gates": dict.fromkeys(("completed candle", "feature readiness", "higher-timeframe direction", "closed range breakout",
                 "fresh quote and required price bases", "entry session", "instrument rules", "entry expiry", "risk and sizing",
                 "executable entry range", "invalidation level"), "PENDING"), "chart": [],
           "quote_age_seconds": (now-utc(quote["at"])).total_seconds() if quote else None,
           "mark_age_seconds": (now-utc(mark["at"])).total_seconds() if mark else None}
    gates = row["gates"]

    def result(state, category, reason):
        return {**row, "state": state, "decision": "CONDITIONAL" if state == "ELIGIBLE_CONDITIONAL_IDEA" else "NO_TRADE",
                "category": category, "reason": reason}

    if cutoff < utc(holdout_end):
        gates["completed candle"] = "FAILED"
        return result("WATCHING", "DATA_UNAVAILABLE", "Only prospective decision cutoffs are eligible")
    usable = frame.loc[(frame.close_at <= cutoff) & (frame.received_at <= now)].sort_values("open_at") if not frame.empty else frame
    if usable.empty or usable.close_at.iloc[-1] != cutoff:
        gates["completed candle"] = "PENDING"
        row["latest_completed"] = None if usable.empty else usable.close_at.iloc[-1]
        return result("WATCHING", "DATA_UNAVAILABLE", "Latest expected completed KuCoin trade candle is missing")
    row["latest_completed"] = cutoff
    row["candle_age_seconds"] = (now-cutoff).total_seconds()
    row["latest_candle_receipt"] = usable.received_at.iloc[-1]
    row["chart"] = usable[["close_at", *OHLCV[:4]]].tail(45).to_dict("records")
    gates["completed candle"] = "PASSED"
    try:
        features, _ = build_features(usable.set_index("open_at"), profile, pd.Timedelta(INTERVALS[mode["warmup_interval"]]))
    except ValueError:
        gates["feature readiness"] = "PENDING"
        return result("WATCHING", "DATA_UNAVAILABLE", "Completed decision and higher-timeframe buckets are unavailable")
    if cutoff not in features.index:
        gates["feature readiness"] = "PENDING"
        return result("WATCHING", "DATA_UNAVAILABLE", "Completed decision bucket is unavailable")
    candle = features.loc[cutoff]
    structure = {key: float(candle[key]) if np.isfinite(candle[key]) else None for key in
                 ("close", "prior_high", "prior_low", "atr", "pivot_high", "pivot_low")}
    structure["context"] = {spec["frame"]: {"sma_bars": spec["sma"], "close_over_sma_minus_one":
        float(candle[f"context_distance_{i}"]) if np.isfinite(candle[f"context_distance_{i}"]) else None}
        for i, spec in enumerate(profile["context"])}
    row["structure"] = structure
    if not candle.ready:
        gates["feature readiness"] = "PENDING"
        return result("WATCHING", "DATA_UNAVAILABLE", "The frozen breakout feature readiness or higher-timeframe history is incomplete")
    gates["feature readiness"] = "PASSED"
    trend = 1 if candle.uptrend else -1 if candle.downtrend and not profile["long_only"] else 0
    side = int(directions(features.loc[[cutoff]], lane["strategy"], lane["entry_parameters"], profile["long_only"]).iloc[0])
    gates["higher-timeframe direction"] = "PASSED" if trend else "FAILED"
    gates["closed range breakout"] = "PASSED" if side else "PENDING"
    skew = policy["maximum_quote_clock_skew_seconds"]
    quotes_ready = fresh(quote, now, policy["maximum_quote_age_seconds"], skew) and (
        spot or fresh(mark, now, policy["maximum_quote_age_seconds"], skew))
    gates["fresh quote and required price bases"] = "PASSED" if quotes_ready else "PENDING"
    if trend:
        level = structure["prior_high" if trend > 0 else "prior_low"]
        row.update(direction="LONG" if trend > 0 else "SHORT", confirmation_level=level,
                   distance_to_confirmation=max(0., trend*(level-structure["close"])),
                   confirmation="Completed trade candle close strictly above the prior 20-bar high" if trend > 0 else
                                "Completed trade candle close strictly below the prior 20-bar low",
                   watch_invalidation="Higher-timeframe alignment fails or the next completed candle changes the prior range")
    if session["entry_cutoff"] is not None and now >= session["entry_cutoff"]:
        gates["entry session"] = "FAILED"
        return result("WATCHING", "SESSION_EXCLUDED", "The Day UTC entry session has closed")
    gates["entry session"] = "PASSED" if session["entry_cutoff"] is not None else "NOT_APPLICABLE"
    if not side:
        reason = "No completed breakout in the eligible trend direction" if trend else "Higher-timeframe direction is mixed or disallowed"
        return result("AWAITING_CONFIRMATION" if trend else "WATCHING", "NO_QUALIFYING_SETUP", reason)
    if not quotes_ready:
        return result("WATCHING", "DATA_UNAVAILABLE", "The confirmed breakout still needs a fresh bid/ask and required trigger-basis price")
    if not metadata.get("as_of") or not timedelta_age(metadata["as_of"], now, policy["maximum_metadata_age_hours"]):
        gates["instrument rules"] = "PENDING"
        return result("WATCHING", "DATA_UNAVAILABLE", "Current contract or spot instrument rules are missing or stale")
    try:
        contract = current_rules(mode["trade_type"], metadata, config)
    except (KeyError, StopIteration, ValueError) as error:
        gates["instrument rules"] = "FAILED"
        return result("WATCHING", "DATA_UNAVAILABLE", f"Current instrument rules unavailable ({error})")
    gates["instrument rules"] = "PASSED"
    setup = make_setup(cutoff, candle, side, lane["strategy"], lane["exit"], lane["exit_parameters"], profile,
                       lane["execution"], pd.Timedelta(seconds=profile["execution_seconds"]))
    if setup is None:
        gates["entry session"] = "FAILED"
        return result("WATCHING", "SESSION_EXCLUDED", "The frozen Day submission clock is outside the entry session")
    expiry = min(next_close, now+pd.Timedelta(minutes=mode["idea_ttl_minutes"]))
    if session["entry_cutoff"] is not None:
        expiry = min(expiry, session["entry_cutoff"])
    eligible_from = now.floor(STEP)+STEP
    if eligible_from >= expiry:
        gates["entry expiry"] = "FAILED"
        return result("EXPIRED", "ENTRY_EXPIRED", "No complete post-publication entry minute remains")
    gates["entry expiry"] = "PASSED"
    cost = Cost(lane["cost"]["taker"], lane["cost"]["adverse_proxy"])
    order, reason = size_order(setup, contract, cost, Risk(**lane["risk"]), equity, lane["execution"])
    if not order:
        gates["risk and sizing"] = "FAILED"
        return result("WATCHING", "RISK_REJECTED", reason)
    sizing = price_summary(order, setup, contract, cost)
    if (spot and (sizing["notional"] < float(metadata["spot"]["minFunds"]) or order.contracts > float(metadata["spot"]["maxBaseOrderSize"]))) or (
            not spot and order.contracts > float(metadata["contract"]["marketMaxOrderQty"])):
        gates["risk and sizing"] = "FAILED"
        return result("WATCHING", "RISK_REJECTED", "Quantity or notional is outside current public instrument limits")
    gates["risk and sizing"] = "PASSED"
    trigger = row["confirmation_level"]
    distance = lane["execution"]["entry_limit_atr"]*setup.atr
    lower, upper = ((max(trigger, setup.reference-distance), order.limit_price) if side == 1 else
                    (order.limit_price, min(trigger, setup.reference+distance)))
    quote_value = quote["ask" if side > 0 else "bid"]
    if lower > upper or side*(quote_value-order.limit_price) > 0:
        gates["executable entry range"] = "FAILED"
        return result("WATCHING", "EXECUTION_INFEASIBLE", "Current executable quote is beyond the acceptable entry range")
    if side*((quote["last"] if spot else mark["mark"])-sizing["stop"]) <= 0:
        gates["invalidation level"] = "FAILED"
        return result("INVALIDATED", "STOP_INVALIDATED", "The current trigger-basis price crossed the proposed stop")
    gates["executable entry range"] = gates["invalidation level"] = "PASSED"
    pivot = structure["pivot_low" if side > 0 else "pivot_high"]
    stop_multiple = lane["exit_parameters"]["initial_atr"]
    uses_pivot = pivot is not None and setup.atr <= side*(setup.reference-pivot) < stop_multiple*setup.atr
    row["plan"] = {"eligible_from": eligible_from, "entry_expiry": expiry, "entry": {"minimum": lower, "maximum": upper,
        "trigger": trigger, "quote_basis": "ASK" if side == 1 else "BID", "paper_basis": "TRADE_MINUTE_OPEN_WITH_ADVERSE"},
        "stop": sizing["stop"], "initial_stop_rule": "confirmed pivot" if uses_pivot else f"{stop_multiple:g} × ATR from the confirmed close",
        "trigger_basis": "TRADE" if spot else "MARK", "exit_execution_basis": "TRADE",
        "fixed_target": sizing["target"], "target_r": setup.target_r, "trailing_atr": setup.trail_atr,
        "max_hold_minutes": profile["max_hold_minutes"], "session_flat": session["flat"],
        "direction": side, "order": asdict(order), "sizing": {**sizing, "equity": equity, "effective_exposure": sizing["notional"]/equity},
        "contract": asdict(contract), "metadata_as_of": metadata["as_of"], "cost": lane["cost"],
        "quote_receipt": quote, "mark_receipt": mark, "source_digest": frame_digest(usable),
        "candle_raw_sha256": sorted(set(usable.raw_sha256)) if "raw_sha256" in usable else [],
        "cancel_if": ["Entry expiry", "Stop invalidation", "Adverse-adjusted open outside the range after the trigger", "Unresolved market evidence"]}
    return result("ELIGIBLE_CONDITIONAL_IDEA", "CLOSED_BREAKOUT", "Completed breakout and higher-timeframe direction agree")
