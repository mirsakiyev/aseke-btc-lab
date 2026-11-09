from dataclasses import asdict
from math import isfinite

import numpy as np
import pandas as pd

from lab.config import fingerprint
from lab.data import OHLCV
from lab.learning.features import build_features, directions
from lab.learning.simulator import Contract, Cost, Risk, fill_price, make_setup, quantize, size_order
from lab.phase9.data import checked_duplicates, inspect_batch, metadata
from lab.phase9.public import INTERVALS, latest, run_folder
from lab.phase9.reference import (APPROVAL, MODE, NAMES, OUTPUT, controls, freeze_reference, parent_design,
                                  read_frozen_reference, reference)
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


RECORDS = OUTPUT / "ideas"
FINAL = {"EXPIRED", "INVALIDATED", "ENTRY_OUTSIDE_RANGE", "SUPERSEDED"}
EVENT_CLASSES = {**dict.fromkeys(FINAL | {"ENTRY_CONDITION_OBSERVED"}, "MARKET_OBSERVATION"),
                 "SIMULATED_FILL": "SIMULATED_EXECUTION", "SIMULATED_OUTCOME": "SIMULATED_EXECUTION",
                 "USER_REPORTED_EXECUTION": "USER_REPORTED_EXECUTION"}


def current_rules(market, rules, config):
    if market == "SPOT":
        row = rules["spot"]
        if (row["feeCategory"] != "classA" or row["tradingStatus"] != "TradingEnabled"
                or row["baseCurrency"] != "BTC" or row["quoteCurrency"] != "USDT"):
            raise ValueError("BTC-USDT Class A spot trading rules are unavailable")
        return Contract("spot", "USDT", 1, float(row["baseOrderStep"]), float(row["tickSize"]),
                        float(row["minBaseOrderSize"]), float(row["maxQuoteOrderSize"]), 1, 0, 0)
    row, tier = rules["contract"], next(item for item in rules["isolated"] if int(item["level"]) == 1)
    if row["status"] != "Open":
        raise ValueError("The perpetual is not in Open status")
    return Contract("linear_perp", "USDT", float(row["multiplier"]), float(row["lotSize"]), float(row["tickSize"]),
                    float(rules.get("perp", {}).get("minOrderSize") or row["lotSize"]),
                    float(tier["maxRiskLimit"]), min(100, float(tier["maxLeverage"]), float(row["maxLeverage"])),
                    float(tier["maintainMargin"]), config["ideas"]["modeled_liquidation_fee"])


def fresh(quote, now, limit, skew=0):
    if not quote:
        return False
    market_time, received = utc(quote["at"]), utc(quote["received_at"])
    # Reject future market timestamps and records arriving after this decision.
    return (max(market_time, received) <= now and (market_time - received).total_seconds() <= skew
            and (now - min(market_time, received)).total_seconds() <= limit)


def frame_digest(frame):
    rows = [[row.open_at.isoformat(), *[float(getattr(row, key)) for key in OHLCV]] for row in frame.itertuples()]
    return fingerprint(rows)


def estimate(order, setup, contract, cost):
    direction = setup.direction
    entry = order.limit_price
    stop = quantize(setup.stop, contract.price_tick, direction < 0)
    target = quantize(entry + direction * setup.target_r * abs(entry - stop), contract.price_tick, direction > 0)
    quantity = order.contracts * contract.base_per_contract
    # The range bound is already a fill-price cap. Apply the proxy to each exit only.
    exit_stop = fill_price(stop, -direction, contract, cost)
    exit_target = fill_price(target, -direction, contract, cost)
    loss = (direction * (entry - exit_stop) + cost.fee * (entry + exit_stop)) * quantity
    reward = (direction * (exit_target - entry) - cost.fee * (entry + exit_target)) * quantity
    return {"stop": stop, "target": target, "net_loss_scenario": loss, "net_reward_scenario": reward,
            "net_reward_risk": reward / loss if loss > 0 else None, "base_quantity": quantity,
            "estimated_notional": quantity * entry, "estimated_initial_margin": quantity * entry / order.leverage}


def compute(profile_name, frame, quote, mark, metadata_record, config, parent, now, *, evidence=None):
    mode = config["profiles"][profile_name]
    settings = parent["design"]["settings"]
    profile = settings["profiles"][mode["parent"]]
    policy = config["ideas"]
    step = pd.Timedelta(INTERVALS[mode["warmup_interval"]])
    decision_step = pd.Timedelta(profile["decision"])
    grace = pd.Timedelta(seconds=policy["completed_candle_grace_seconds"])
    cutoff = (now - grace).floor(decision_step)
    next_close = cutoff + decision_step
    fee = config["fees"][mode["fee"]]
    base = {"profile": profile_name, "venue": mode["venue"], "instrument": mode["symbol"], "market": mode["trade_type"],
            "quote_currency": mode["quote"], "decision_at": now.isoformat(), "source_cutoff": cutoff.isoformat(),
            "classification": "EXPERIMENTAL", "validation": APPROVAL, "evidence_basis": "RULES_ONLY",
            "strategy": policy["family"], "exit_policy": policy["exit"], "model_version": None,
            "strategy_version": policy["version"], "feature_version": parent["design"]["feature_version"],
            "parent_manifest": parent["id"], "execution_mode": MODE,
            "cost": {"id": mode["fee"], **fee, **config["fee_evidence"]},
            "data_provenance": evidence or {}, "quote_observed": quote, "mark_observed": mark, "direction": "NO_TRADE"}

    def outcome(status, reason, **details):
        return {**base, "status": status, "reason": reason, **details}

    if cutoff < utc(parent["design"]["holdout"]["end_exclusive"]):
        return outcome("DATA_NOT_READY", "Decision cutoff must be genuinely prospective after the sealed period")
    if frame.empty:
        return outcome("DATA_NOT_READY", "No complete current venue candles available")
    usable = frame[frame.close_at <= cutoff].copy().sort_values("open_at")
    if usable.empty or usable.close_at.iloc[-1] != cutoff:
        return outcome("DATA_NOT_READY", "Latest expected completed venue candle is missing",
                       last_source_close=usable.close_at.iloc[-1].isoformat() if len(usable) else None)
    base.update(source_first_open=usable.open_at.iloc[0].isoformat(), source_last_close=usable.close_at.iloc[-1].isoformat(),
                candle_age_seconds=(now - cutoff).total_seconds(), input_rows=len(usable), input_digest=frame_digest(usable))
    features, _ = build_features(usable.set_index("open_at"), profile, step)
    if cutoff not in features.index:
        return outcome("DATA_NOT_READY", "Completed decision bucket is unavailable")
    row = features.loc[cutoff]
    structure = {key: float(row[key]) if np.isfinite(row[key]) else None for key in
                 ("close", "prior_high", "prior_low", "atr", "return_1", "ema_distance", "rsi", "context_abs")}
    structure["context"] = {item["frame"]: (float(row[f"context_distance_{index}"])
                                            if np.isfinite(row[f"context_distance_{index}"]) else None)
                            for index, item in enumerate(profile["context"])}
    structure.update(uptrend=bool(row.uptrend), downtrend=bool(row.downtrend), feature_ready=bool(row.ready))
    base["structure"] = structure
    if not row.ready:
        return outcome("DATA_NOT_READY", "The pinned breakout features or completed higher-timeframe context need warmup")
    skew = policy["maximum_quote_clock_skew_seconds"]
    if not fresh(quote, now, policy["maximum_quote_age_seconds"], skew):
        return outcome("DATA_NOT_READY", "Fresh timestamped venue quote unavailable")
    if mode["trade_type"] == "FUTURES" and not fresh(mark, now, policy["maximum_quote_age_seconds"], skew):
        return outcome("DATA_NOT_READY", "Fresh timestamped mark price unavailable")
    side = int(directions(features.loc[[cutoff]], policy["family"], settings["strategies"][policy["family"]],
                          profile["long_only"]).iloc[0])
    if not side:
        return outcome("NO_TRADE", "No closed-range breakout aligned with the configured trend context")
    if not metadata_record.get("as_of") or (
        not timedelta_age(metadata_record["as_of"], now, policy["maximum_metadata_age_hours"])
    ):
        return outcome("DATA_NOT_READY", "Current contract or spot instrument metadata expired")
    try:
        contract = current_rules(mode["trade_type"], metadata_record, config)
    except (KeyError, StopIteration, ValueError) as exc:
        return outcome("DATA_NOT_READY", f"Current sizing rules unavailable ({exc})")
    cost = Cost(fee["taker"], fee["adverse_proxy"])
    risk = Risk(**settings["risk"][profile["risk"]])
    setup = make_setup(cutoff, row, side, policy["family"], policy["exit"], settings["exits"][policy["exit"]],
                       profile, settings["execution"], pd.Timedelta(seconds=profile["execution_seconds"]))
    if setup is None:
        return outcome("NO_TRADE", "The pinned Day session excludes this decision")
    expiry = min(next_close, now + pd.Timedelta(minutes=mode["idea_ttl_minutes"]))
    if "entry_cutoff_minute_utc" in profile:
        expiry = min(expiry, cutoff.normalize() + pd.Timedelta(minutes=profile["entry_cutoff_minute_utc"]))
    if expiry <= now:
        return outcome("NO_TRADE", "The entry window has expired")
    order, reason = size_order(setup, contract, cost, risk, risk.starting_equity, settings["execution"])
    if not order:
        return outcome("NO_TRADE", reason)
    sizing = estimate(order, setup, contract, cost)
    if mode["trade_type"] == "SPOT":
        metadata_row = metadata_record["spot"]
        if (sizing["estimated_notional"] < float(metadata_row["minFunds"])
                or order.contracts > float(metadata_row["maxBaseOrderSize"])):
            return outcome("NO_TRADE", "Paper sizing is outside the current spot order constraints")
    elif order.contracts > float(metadata_record["contract"]["marketMaxOrderQty"]):
        return outcome("NO_TRADE", "Paper sizing exceeds the current contract order limit")
    trigger = float(row.prior_high if side == 1 else row.prior_low)
    distance = settings["execution"]["entry_limit_atr"] * setup.atr
    lower, upper = ((max(trigger, setup.reference - distance), order.limit_price) if side == 1
                    else (order.limit_price, min(trigger, setup.reference + distance)))
    quote_value = quote["ask" if side == 1 else "bid"]
    invalidation = mark["mark"] if mode["trade_type"] == "FUTURES" else quote["last"]
    if lower > upper or side * (quote_value - order.limit_price) > 0:
        return outcome("NO_TRADE", "Current executable quote is beyond the allowable entry deviation")
    if side * (invalidation - sizing["stop"]) <= 0:
        return outcome("NO_TRADE", "Current trigger-basis price has invalidated the proposed stop")
    return outcome("CONDITIONAL", "Closed breakout and trend gates agree", direction="LONG" if side == 1 else "SHORT",
                   expires_at=expiry.isoformat(),
                   entry={"trigger": trigger, "minimum": lower, "maximum": upper,
                          "price_basis": "ASK" if side == 1 else "BID", "max_deviation_from_reference": distance},
                   stop={"price": sizing["stop"], "basis": "MARK" if mode["trade_type"] == "FUTURES" else "LAST"},
                   take_profit=[{"price": sizing["target"], "allocation_fraction": 1.}],
                   time_exit={"maximum_hold_minutes": profile["max_hold_minutes"],
                              "session_flat_minute_utc": profile.get("flat_minute_utc"), "trailing": None},
                   paper={"equity": risk.starting_equity, "risk_version": profile["risk"], **asdict(order), **sizing,
                          "effective_account_leverage": sizing["estimated_notional"] / risk.starting_equity,
                          "funding": "Future settlements unknown. Net reward/risk includes trading costs only."
                          if mode["trade_type"] == "FUTURES" else "Spot funding inapplicable"},
                   contract_rules={"observed": metadata_record["as_of"], "plan": metadata_record.get("plan"),
                                   "simulation_scope": "Hypothetical single tier and liquidation-cost assumption"},
                   cancel_if=["Expiry", "Stop invalidation", "Entry quote beyond the allowed range", "Stale market inputs"])


def timedelta_age(value, now, hours):
    return pd.Timedelta(0) <= now - utc(value) <= pd.Timedelta(hours=hours)


def _records(profile):
    folder = RECORDS / profile
    return sorted((read_json(path) for path in folder.glob("*.json")), key=lambda row: row["decision_at"])


def publish(idea, ref_id):
    identity = {key: idea.get(key) for key in ("profile", "source_cutoff", "input_digest", "status", "reason",
                                               "direction", "entry", "stop", "contract_rules")}
    digest = fingerprint([ref_id, identity])
    with writer_lock(OUTPUT / ".ideas.lock"):
        folder = RECORDS / idea["profile"]
        path = folder / f"{digest}.json"
        if path.exists():
            return read_json(path)
        history = _records(idea["profile"])
        previous = next((item for item in reversed(history) if item["source_cutoff"] == idea["source_cutoff"]
                         or (item["status"] == "CONDITIONAL" and utc(item["expires_at"]) > utc(idea["decision_at"]))), None)
        row = {**idea, "idea_id": digest, "reference_id": ref_id, "published_at": idea["decision_at"],
               "supersedes_id": previous["idea_id"] if previous else None}
        write_json(path, row)
        return row


def decide(names=NAMES, folder=None):
    cfg, parent, flags = reference(), parent_design(), controls()
    ref_id = freeze_reference(cfg)
    rules = metadata()
    folder = folder or latest("current")
    source_profiles = {}
    if folder:
        plan = read_json(folder / "plan.json")["design"]
        if plan["scope"] != "PROSPECTIVE_FEATURE_WARMUP_ONLY":
            raise ValueError("Ideas require a current acquisition plan")
        source_profiles = read_frozen_reference(plan["reference_id"])["reference"]["profiles"]
        if pinned_rules := plan.get("metadata_plan"):
            rules = metadata(run_folder(pinned_rules))
    history, frames, quotes = inspect_batch(folder, rules) if folder else ([], {}, {})
    extra = latest("quotes")
    quote_source = {key: folder.name for key in quotes}
    if extra:
        _, _, latest_quotes = inspect_batch(extra, rules)
        for key, value in latest_quotes.items():
            if key not in quotes or utc(value["received_at"]) > utc(quotes[key]["received_at"]):
                quotes[key] = value
                quote_source[key] = extra.name
    now = utc_now()
    results = []
    for name in names:
        if not flags["ideas"][name]:
            results.append({"profile": name, "status": "OFF", "reason": "Trade ideas disabled for this profile"})
            continue
        if source_profiles and any(source_profiles[name][field] != cfg["profiles"][name][field]
                                   for field in ("venue", "trade_type", "symbol", "quote", "warmup_interval")):
            raise ValueError("Current candles were acquired for a different instrument or resolution")
        records = [frame for key, frame in frames.items() if key.startswith(f"{name}-")]
        frame = checked_duplicates(pd.concat(records, ignore_index=True), "open_at") if records else pd.DataFrame()
        spot = name == "position"
        idea = compute(name, frame, quotes.get("spot-quote" if spot else "perp-quote"), quotes.get("perp-mark"),
                       rules, cfg, parent, now, evidence={"acquisition_plan": folder.name if folder else None,
                                                          "quote_plans": quote_source,
                                                          "responses": [row for row in history if row["key"].startswith(name + "-")]})
        results.append(publish(idea, ref_id))
    return results


def observation_state(idea, quote, mark, now, freshness, skew=0, previously_triggered=False):
    if idea["status"] != "CONDITIONAL":
        return None
    if now >= utc(idea["expires_at"]):
        return "EXPIRED"
    if not fresh(quote, now, freshness, skew) or min(utc(quote["at"]), utc(quote["received_at"])) <= utc(idea["published_at"]):
        return None
    if idea["stop"]["basis"] == "MARK" and (
        not fresh(mark, now, freshness, skew) or min(utc(mark["at"]), utc(mark["received_at"])) <= utc(idea["published_at"])
    ):
        return None
    side = 1 if idea["direction"] == "LONG" else -1
    trigger_value = mark["mark"] if idea["stop"]["basis"] == "MARK" else quote["last"]
    if side * (trigger_value - idea["stop"]["price"]) <= 0:
        return "INVALIDATED"
    entry = idea["entry"]
    value = quote[entry["price_basis"].lower()]
    if not entry["minimum"] <= value <= entry["maximum"]:
        return "ENTRY_OUTSIDE_RANGE" if previously_triggered or side * (value - entry["trigger"]) >= 0 else None
    if side * (value - entry["trigger"]) < 0:
        return None
    return "ENTRY_CONDITION_OBSERVED"


def events(idea_id):
    return sorted((read_json(path) for path in (OUTPUT / "idea-events" / idea_id).glob("*.json")),
                  key=lambda row: row["recorded_at"])


def append_event(idea, kind, evidence, now):
    if kind not in EVENT_CLASSES:
        raise ValueError("Unknown idea event type")
    key = fingerprint([idea["idea_id"], kind, evidence])
    path = OUTPUT / "idea-events" / idea["idea_id"] / f"{key}.json"
    with writer_lock(OUTPUT / ".ideas.lock"):
        if EVENT_CLASSES[kind] == "MARKET_OBSERVATION":
            final = next((event for event in events(idea["idea_id"]) if event["type"] in FINAL), None)
            if final:
                return final
        if not path.exists():
            write_json(path, {"id": key, "idea_id": idea["idea_id"], "type": kind, "recorded_at": now.isoformat(),
                              "classification": EVENT_CLASSES[kind], "evidence": evidence})
    return read_json(path)


def observe(folder=None):
    if not controls()["monitor"]:
        raise ValueError("Phase 9 market monitoring is off")
    folder = folder or latest("quotes") or latest("current")
    _, _, quotes = inspect_batch(folder, metadata()) if folder else ([], {}, {})
    cfg = reference()["ideas"]
    now, limit = utc_now(), cfg["maximum_quote_age_seconds"]
    results = []
    for profile in NAMES:
        history = _records(profile)
        if not history:
            continue
        idea = history[-1]
        prior = {event["type"] for event in events(idea["idea_id"])}
        if prior & FINAL:
            continue
        quote = quotes.get("spot-quote" if profile == "position" else "perp-quote")
        mark = quotes.get("perp-mark")
        state = observation_state(idea, quote, mark, now, limit, cfg["maximum_quote_clock_skew_seconds"],
                                  previously_triggered="ENTRY_CONDITION_OBSERVED" in prior)
        if state and state not in prior:
            results.append(append_event(idea, state, {"quote": quote, "mark": mark, "plan": folder.name if folder else None}, now))
    return results


def user_execution(idea_id, details):
    now = utc_now()
    idea = next((row for profile in NAMES for row in _records(profile) if row["idea_id"] == idea_id), None)
    if not idea or idea["status"] != "CONDITIONAL":
        raise ValueError("Select a published conditional idea")
    executed = utc(details["executed_at"])
    units = ("base",) if idea.get("market") == "SPOT" else ("base", "contracts")
    if not (utc(idea["published_at"]) <= executed <= now) or details["unit"] not in units:
        raise ValueError("Invalid actual execution timestamp or units")
    price, size = float(details["price"]), float(details["quantity"])
    if not all(isfinite(value) and value > 0 for value in (price, size)):
        raise ValueError("Invalid user-reported execution size or price")
    return append_event(idea, "USER_REPORTED_EXECUTION", {"executed_at": executed.isoformat(), "price": price,
                        "quantity": size, "unit": details["unit"], "source": "USER_REPORTED"}, now)


def recent(profile):
    rows = _records(profile)
    if not rows:
        return []
    now = utc_now()
    cfg = reference()["ideas"]
    results = []
    superseded = {row["supersedes_id"] for row in rows}
    for row in reversed(rows[-8:]):
        trail = events(row["idea_id"])
        observed = next((event["type"] for event in reversed(trail) if event["type"] in FINAL | {"ENTRY_CONDITION_OBSERVED"}), None)
        market_state = observed or row["status"]
        if row["status"] == "CONDITIONAL":
            if row["idea_id"] in superseded:
                market_state = "SUPERSEDED"
            elif observed not in FINAL and now >= utc(row["expires_at"]):
                market_state = "EXPIRED"
            elif observed not in FINAL:
                last_quote = next((event["evidence"].get("quote") for event in reversed(trail)
                                   if event["type"] == "ENTRY_CONDITION_OBSERVED"), row.get("quote_observed"))
                if not fresh(last_quote, now, cfg["maximum_quote_age_seconds"], cfg["maximum_quote_clock_skew_seconds"]):
                    market_state = "STALE_QUOTE"
        results.append({**row, "market_state": market_state, "events": trail,
                        "simulation_events": [event for event in trail if EVENT_CLASSES[event["type"]] == "SIMULATED_EXECUTION"],
                        "user_reported_events": [event for event in trail if event["type"] == "USER_REPORTED_EXECUTION"]})
    return results
