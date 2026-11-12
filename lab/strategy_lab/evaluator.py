"""Causal signal generation backed by the lab's existing rule implementations."""
from dataclasses import asdict
from math import isfinite

import numpy as np
import pandas as pd

from lab.config import ORIGIN, ROOT as PROJECT
from lab.execution import execution_open
from lab.features import contiguous_groups
from lab.learning.features import build_features, directions
from lab.learning.simulator import Cost, Risk, make_setup
from lab.storage import file_hash, utc
from lab.strategy_lab import ema_pullback
from lab.strategy_lab.ema_execution import BracketSetup
from lab.strategy_lab.registry import digest, event, get
from lab.strategy_lab.specification import (
    EMA_INDICATORS, ENTRIES, EXITS, INDICATORS, POLICY, SOURCE_POLICY, ema_rule_description, recipe_description, validate,
)


def number(value, minimum, maximum):
    return type(value) in (int, float) and isfinite(value) and minimum <= value <= maximum


def ema_code_hashes():
    paths = ("lab/strategy_lab/ema_pullback.py", "lab/strategy_lab/ema_execution.py")
    return {path: file_hash(PROJECT / path) for path in paths}


def compile_spec(spec):
    if spec.get("origin") == "PHASE16_INITIAL_HISTORICAL" or spec.get("recipe") is None:
        raise ValueError("No sandbox rules-only evaluator is bound to this version")
    if issues := validate(spec):
        raise ValueError(f"Specification requires review: {issues}")
    recipe = spec["recipe"]
    if not isinstance(recipe, dict) or not isinstance(recipe.get("entry"), str) or not isinstance(recipe.get("exit"), str):
        raise ValueError("Supply an implemented entry and exit recipe")
    entry, exit_name = recipe["entry"], recipe["exit"]
    is_ema = entry == ema_pullback.FAMILY
    rules = spec["execution"]
    source_benchmark = is_ema and rules.get("version") == SOURCE_POLICY
    if entry not in ENTRIES or exit_name not in EXITS or (not source_benchmark and rules.get("version") != POLICY):
        raise ValueError("Unsupported recipe or research execution policy")
    if spec["family_id"] != entry:
        raise ValueError("Use the built-in entry family ID for its profile, parameter and exit variants")
    ep, xp = recipe["entry_parameters"], recipe["exit_parameters"]
    if is_ema:
        if (exit_name != ema_pullback.EXIT or ep != ema_pullback.ENTRY_PARAMETERS or xp != ema_pullback.EXIT_PARAMETERS
                or spec["profile"] not in ("position", "swing") or spec["indicators"] != EMA_INDICATORS or spec["context"]):
            raise ValueError("EMA v1 requires the frozen same-chart indicators, bracket defaults and a Position or Swing profile")
        if spec.get("evaluator", {}).get("code_sha256") != ema_code_hashes():
            raise ValueError("EMA v1 evaluator code differs from its frozen version")
    else:
        entry_keys = {ENTRIES[entry][0]} | ({"context_abs_max"} if entry == "range_reversion_v1" else set())
        exit_keys = {EXITS[exit_name][0], "initial_atr"}
        if exit_name == ema_pullback.EXIT or not isinstance(ep, dict) or not isinstance(xp, dict) or (
            set(ep) != entry_keys or set(xp) != exit_keys or not all(number(value, 1e-8, 100) for value in (*ep.values(), *xp.values()))
            or (entry == "breakout_v1" and ep["prior_range"] != 20)
        ):
            raise ValueError("Supply exact supported entry and exit parameters")
    if not all(number(rules.get(key), lo, hi) for key, lo, hi in
               (("processing_seconds", 0 if source_benchmark else 1, 3600),
                ("entry_limit_atr", 0, 10), ("funding_reserve_bps", 0, 500))):
        raise ValueError("Invalid processing, entry or funding reserve settings")
    if any(not number(spec["cost"].get(key), 0, .025) for key in ("taker", "adverse_proxy")):
        raise ValueError("Supply explicit per-side taker and adverse rates")
    risk_values = spec["risk"]
    bounds = {"starting_equity": (1, 1e9), "risk_fraction": (1e-8, .05), "notional_cap": (1e-8, 5),
              "leverage": (1, 5), "ordinary_cap": (1, 5), "liquidation_buffer_atr": (0, 100)}
    if set(risk_values) != set(bounds) or not all(number(risk_values[k], *bounds[k]) for k in bounds):
        raise ValueError("Supply all bounded sizing parameters")
    if risk_values["leverage"] > risk_values["ordinary_cap"] or (
        spec["market"] == "SPOT" and (risk_values["leverage"] != 1 or risk_values["notional_cap"] > 1)
    ):
        raise ValueError("Leverage and notional exceed the account policy")
    if source_benchmark and (spec["profile"] != "swing" or spec["sides"] != ["long", "short"]
        or risk_values["notional_cap"] != .10 or risk_values["leverage"] != 1 or spec["cost"]["taker"] != .0005
        or spec["cost"]["adverse_proxy"] != 0 or any(rules[key] != 0 for key in
                                                   ("processing_seconds", "entry_limit_atr", "funding_reserve_bps"))
    ):
        raise ValueError("The source benchmark fixes next-chart-open timing, 10% notional and 0.05% commission")
    step, decision = pd.Timedelta(seconds=spec["data"]["native_seconds"]), pd.Timedelta(spec["timeframe"])
    context = spec["context"]
    if not is_ema and (not context or len(context) > 4 or len({pd.Timedelta(item["frame"]) for item in context}) != len(context) or any(
        pd.Timedelta(item["frame"]) <= decision or pd.Timedelta(item["frame"]) % step or item["sma"] > 500
        for item in context
    )):
        raise ValueError("Supply one to four distinct, aligned higher timeframes and SMA windows")
    if not source_benchmark and (spec["session"]["max_hold_minutes"] > 43200 or any(
        pd.Timedelta(minutes=value) % step for key, value in spec["session"].items() if key != "timezone"
    )):
        raise ValueError("Hold and session boundaries must align to the execution grid with a 30-day maximum hold")
    if not is_ema and set(spec["indicators"]) - INDICATORS.keys():
        raise ValueError("At least one required indicator has no evaluator implementation")
    if "candle_vwap" in spec["indicators"] and decision >= pd.Timedelta("1D"):
        raise ValueError("The candle VWAP proxy is supported on intraday decision bars")
    if spec.get("origin") != "PHASE15_REGISTERED_PAPER":
        expected = ema_rule_description(spec["sides"]) if is_ema else recipe_description(entry, exit_name, xp)
        if any(spec[key] != value for key, value in zip(("entry_rules", "exit_rules", "invalidation_rules"), expected)):
            raise ValueError("Reviewed rule text must match the implemented recipe")
        if not is_ema and any(params != {"definition": INDICATORS[key]} and (
            key != "sma" or params != {"definition": INDICATORS[key], "windows": context}
        ) for key, params in spec["indicators"].items()):
            raise ValueError("Indicator settings differ from the implemented calculation")
    profile = {**spec["session"], "decision": spec["timeframe"], "context": context,
               "long_only": "short" not in spec["sides"]}
    return profile, rules, recipe, Risk(**risk_values), Cost(spec["cost"]["taker"], spec["cost"]["adverse_proxy"])


def causal_bars(candles, spec, *, as_of=None):
    step = pd.Timedelta(spec["timeframe"])
    allowance = pd.Timedelta(seconds=spec["execution"]["processing_seconds"])
    # A missed decision bucket stays missing. Later receipts cannot revise prior signals.
    bucket_close = ORIGIN + ((candles.close_at - ORIGIN - pd.Timedelta("1ns")) // step + 1) * step
    usable = candles.received_at.le(bucket_close + allowance)
    if as_of is not None:
        usable &= candles.close_at.le(as_of) & candles.received_at.le(as_of)
    return candles.loc[usable]


def feature_frame(candles, spec, step, *, as_of=None):
    profile, _, recipe, *_ = compile_spec(spec)
    usable = causal_bars(candles, spec, as_of=as_of)
    if recipe["entry"] == ema_pullback.FAMILY:
        return ema_pullback.features(usable, profile["decision"], step)
    frame = build_features(usable, profile, step)[0]
    if "bollinger" in spec["indicators"]:
        groups = contiguous_groups(frame.index, pd.Timedelta(spec["timeframe"]))
        by_segment = frame.close.groupby(groups)
        middle = by_segment.transform(lambda values: values.rolling(20).mean())
        width = by_segment.transform(lambda values: values.rolling(20).std(ddof=0)) * 2
        frame["bollinger_mid"], frame["bollinger_upper"], frame["bollinger_lower"] = middle, middle + width, middle - width
        frame["ready"] &= middle.notna()
    if "candle_vwap" in spec["indicators"]:
        day = usable.index.normalize()
        sequence = usable.groupby(day).cumcount()
        continuous = (usable.index - day) == sequence * step
        typical = (usable.high + usable.low + usable.close) / 3
        total = usable.volume.groupby(day).cumsum()
        vwap = ((typical * usable.volume).groupby(day).cumsum() / total.replace(0, np.nan)).where(continuous)
        frame["candle_vwap"] = pd.Series(vwap.to_numpy(), index=pd.DatetimeIndex(usable.close_at)).reindex(frame.index)
        frame["ready"] &= frame.candle_vwap.notna()
    return frame


def entry_sides(features, profile, recipe, allowed_sides):
    if recipe["entry"] != ema_pullback.FAMILY:
        return directions(features, recipe["entry"], recipe["entry_parameters"], profile["long_only"])
    side = ema_pullback.sides(features)
    eligible = [direction for label, direction in (("long", 1), ("short", -1)) if label in allowed_sides]
    return side.where(side.isin(eligible), 0)


def ema_proposal(row, at, spec, profile, rules, recipe, step, position_open):
    sides = ema_pullback.gates(row)
    signal_side = next((sign for side, sign in (("long", 1), ("short", -1))
                        if all(sides[side].values())), 0)
    eligible = (signal_side == 1 and "long" in spec["sides"]) or (signal_side == -1 and "short" in spec["sides"])
    gates = {"completed_decision": True, "warmup": bool(row.ready), "account_flat": not position_open,
             "entry_recipe": bool(signal_side), "eligible_side": eligible, "session": None, "bracket_valid": None,
             **sides}
    if not eligible:
        return None, gates
    source = rules["version"] == SOURCE_POLICY
    available = at + pd.Timedelta(seconds=rules["processing_seconds"])
    # Adjacent chart bars share a timestamp boundary. The source order is
    # created on the old bar's close and uses only the new bar's open.
    submit = at if source else execution_open(available, step)
    deadline = None if source else submit + pd.Timedelta(minutes=profile["max_hold_minutes"])
    if "entry_cutoff_minute_utc" in profile:
        day = at.normalize()
        if submit >= day + pd.Timedelta(minutes=profile["entry_cutoff_minute_utc"]):
            gates["session"] = False
            return None, gates
        deadline = min(deadline, day + pd.Timedelta(minutes=profile["flat_minute_utc"]))
    gates["session"] = True
    atr, close = float(row.atr), float(row.close)
    xp = recipe["exit_parameters"]
    extreme = float(row.swing_low if signal_side == 1 else row.swing_high)
    stop = extreme - signal_side * xp["stop_atr"] * atr
    target = close + signal_side * xp["target_atr"] * atr
    valid = (all(isfinite(value) and value > 0 for value in (atr, close, stop, target))
             and signal_side * (close - stop) > 0 and signal_side * (target - close) > 0)
    gates["bracket_valid"] = valid
    if position_open or not valid:
        return None, gates
    return BracketSetup(at, available, submit, submit + step, deadline, close, atr, stop, signal_side,
                        None, None, recipe["entry"], recipe["exit"], target_price=target, source_allocation=source), gates


def proposal(features, at, spec, *, data_hash, compiled=None, position_open=False):
    profile, rules, recipe, *_ = compiled or compile_spec(spec)
    step = pd.Timedelta(seconds=spec["data"]["native_seconds"])
    if at not in features.index:
        return {"decision": "NO_TRADE", "gates": {"completed_decision": False}, "input_hash": data_hash,
                "decision_at": at.isoformat(), "values": {}}
    row = features.loc[at]
    values = {key: (float(value) if np.isfinite(value) else None) for key, value in row.items()
              if isinstance(value, (int, float, np.number)) and not isinstance(value, (bool, np.bool_))}
    if recipe["entry"] == ema_pullback.FAMILY:
        setup, gates = ema_proposal(row, at, spec, profile, rules, recipe, step, position_open)
    else:
        side = int(directions(features.loc[[at]], recipe["entry"], recipe["entry_parameters"],
                              profile["long_only"]).iloc[0])
        allowed = (side == 1 and "long" in spec["sides"]) or (side == -1 and "short" in spec["sides"])
        setup = make_setup(at, row, side, recipe["entry"], recipe["exit"], recipe["exit_parameters"],
                           profile, rules, step) if allowed and not position_open else None
        gates = {"completed_decision": True, "warmup": bool(row.ready), "uptrend": bool(row.uptrend),
                 "downtrend": bool(row.downtrend), "entry_recipe": bool(side),
                 "eligible_side": allowed, "session": None if not allowed else setup is not None}
    return {"decision": "PROPOSAL" if setup else "NO_TRADE", "setup": setup, "gates": gates, "values": values,
            "input_hash": data_hash, "decision_at": at.isoformat(),
            "available_at": (at + pd.Timedelta(seconds=rules["processing_seconds"])).isoformat(),
            "context_closes": {item["frame"]: (ORIGIN + (at - ORIGIN) // pd.Timedelta(item["frame"]) *
                                               pd.Timedelta(item["frame"])).isoformat() for item in profile["context"]}}


def evaluate(candles, spec, at, *, as_of, data_hash, position_open=False):
    at, as_of = utc(at), utc(as_of)
    allowance = pd.Timedelta(seconds=spec["execution"]["processing_seconds"])
    if as_of < at + allowance:
        return {"decision": "NO_TRADE", "gates": {"decision_available": False},
                "decision_at": at.isoformat(), "input_hash": data_hash, "values": {}}
    step = pd.Timedelta(seconds=spec["data"]["native_seconds"])
    available = causal_bars(candles, spec, as_of=min(as_of, at + allowance))
    available = available.loc[available.close_at.le(at)]
    try:
        features = feature_frame(available, spec, step)
    except ValueError as exc:
        if str(exc) != "No complete decision buckets":
            raise
        features = pd.DataFrame()
    result = proposal(features, at, spec, data_hash=data_hash, position_open=position_open)
    if result.get("setup"):
        result["setup"] = asdict(result["setup"])
    return result


def code_hash():
    paths = ("lab/data.py", "lab/execution.py", "lab/features.py", "lab/learning/data.py", "lab/learning/features.py",
             "lab/learning/simulator.py", "lab/learning/evaluation.py", "lab/phase10/trading.py",
             "lab/phase10/evaluation.py", "lab/phase10/partition.py", "lab/phase12/study.py", "lab/phase16/evaluate.py")
    files = [PROJECT / path for path in paths]
    files.extend(path for path in sorted((PROJECT / "lab" / "strategy_lab").glob("*.py"))
                 if path.stem in {"specification", "evaluator", "datasets", "replay", "comparison", "jobs", "testing",
                                  "ema_pullback", "ema_execution"})
    return digest({path.relative_to(PROJECT).as_posix(): file_hash(path) for path in files})


def test_version(db, version):
    record = get(db, version)
    if record["state"] not in {"SPECIFIED", "IMPLEMENTED", "UNIT_TESTED", "HISTORICAL_RESEARCH", "INCONCLUSIVE", "SHORTLISTED"}:
        raise ValueError("Review the strategy specification before testing")
    spec = record["spec"]
    profile, _, _, _, _ = compile_spec(spec)
    if record["state"] == "SPECIFIED":
        if not isinstance(spec.get("implementation_version"), str) or not spec["implementation_version"].strip():
            raise ValueError("Pin an implementation version in the specification")
        event(db, version, "IMPLEMENTED", "Reviewed recipe and settings bind to the built-in evaluator")
    # Exercise a ready rule row and a deliberately unready version of the same row.
    from lab.strategy_lab.testing import signal_contract
    checks = signal_contract(spec, profile)
    state = "UNIT_TESTED" if record["state"] in {"SPECIFIED", "IMPLEMENTED", "UNIT_TESTED"} else record["state"]
    suite = "tests/test_ema_pullback_strategy.py" if spec["family_id"] == ema_pullback.FAMILY else "tests/test_strategy_lab.py"
    evidence = {"code": code_hash(), "checks": checks, "suite": suite}
    event(db, version, state, "Built-in deterministic signal contract passed", reference={"unit_test": evidence})
    return get(db, version)


def tested(record, code):
    return any((item["reference"] or {}).get("unit_test", {}).get("code") == code for item in record["history"])
