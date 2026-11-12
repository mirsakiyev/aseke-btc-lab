from collections import Counter
from copy import copy
from dataclasses import asdict, replace
from functools import partial
from base64 import b64encode
import gzip
from hashlib import sha256
import json
from math import ceil

import numpy as np
import pandas as pd

from lab.execution import execution_open
from lab.learning.data import Market
from lab.learning.simulator import Cost, ExecutionAssumptions, run_trade as spot_trade
from lab.phase10.evaluation import account_replay
from lab.phase10.trading import run_trade as futures_trade
from lab.phase12.study import EquityCurve
from lab.phase16.evaluate import account_economics
from lab.strategy_lab import ema_pullback
from lab.strategy_lab.datasets import timestamp
from lab.strategy_lab.ema_execution import run_bracket
from lab.strategy_lab.evaluator import compile_spec, entry_sides, feature_frame, proposal
from lab.strategy_lab.registry import digest
from lab.strategy_lab.specification import SOURCE_POLICY


BASE_CASES = ("base", "double_cost", "delay", "adverse_price", "missing_data", "high_volatility")
FUTURES_CASES = ("mark_divergence", "adverse_funding")
MIN_TRADES = 25
MAX_SETUPS = 2500
ASSUMPTIONS = {
    "engine": "Phase 10 account replay; existing Spot and futures trade executors",
    "fills": "First strictly later native open, one-bar IOC proxy, adverse rounding and taker fees. "
             "Futures protection triggers on mark and exits use trade prices.",
    "boundary": "Each partition starts flat. Suppress entries whose original deadline plus one bar can reach the boundary.",
    "double_cost": "Double the taker fee and adverse rate on both sides.",
    "delay": "Shift entry and eligible non-liquidation exits by one native bar. Keep the original deadline and UTC flat cap.",
    "adverse_price": "Add 10 basis points of adverse price adjustment on each side.",
    "missing_data": "Remove the middle native candle from both bases. Missing evidence invalidates full-period economics.",
    "high_volatility": "Allow validation entries only when closed realized volatility exceeds the development median.",
    "mark_divergence": "Displace the entire mark path upward by 30 basis points.",
    "adverse_funding": "Debit the absolute published rate times two at each observed settlement for each position.",
    "funding": "Use evidenced settlements. Off-grid payments use containing mark open under the historical ordering convention.",
}


def assumptions_for(spec):
    if spec["execution"].get("version") != SOURCE_POLICY:
        return ASSUMPTIONS
    return {
        **ASSUMPTIONS,
        "fills": "Source benchmark enters at the next chart open using 10% equity notional and 0.05% per-side commission. "
                 "The native execution path still uses dated instrument terms, funding and futures mark triggers. "
                 "Phase 15 rules-only control retains its own sizing and entry timing.",
        "boundary": "Start flat. An open bracket at the exclusive boundary remains unresolved.",
    }


def stages(futures):
    return ("development/base", "validation/baseline", *(
        f"validation/{case}" for case in BASE_CASES + (FUTURES_CASES if futures else ())
    ))


def prepare(market, spec, cache, key):
    if key not in cache:
        try:
            cache[key] = feature_frame(market.trade, spec, market.step)
        except ValueError as exc:
            if str(exc) != "No complete decision buckets":
                raise
            cache[key] = pd.DataFrame()
    return cache[key]


def restrict(frame, left, right):
    return frame.loc[(frame.index >= left) & (frame.index < right)]


def proposals(features, spec, compiled, market, left, right, case, dataset_hash, vol_gate):
    if features.empty:
        return {}, [], 0, 0
    profile, _, recipe, *_ = compiled
    frame = restrict(features, left, right)
    sides = entry_sides(frame, profile, recipe, spec["sides"])
    if vol_gate is not None:
        sides = sides.where(frame.realized_vol.gt(vol_gate), 0)
    eligible = sides[sides != 0]
    if len(eligible) > MAX_SETUPS:
        raise ValueError("Decision count exceeds per-case budget")
    setups, examples = {}, []
    for at in eligible.index:
        signal = proposal(features, at, spec, data_hash=dataset_hash, compiled=compiled)
        setup = signal["setup"]
        if len(examples) < 5:
            examples.append({**signal, "setup": asdict(setup) if setup else None})
        if not setup or setup.submit_at >= right or (setup.deadline is not None and setup.deadline + market.step >= right):
            continue
        if case == "delay":
            submit = setup.submit_at + market.step
            cutoff = profile.get("entry_cutoff_minute_utc")
            if submit >= right or (setup.deadline is not None and submit >= setup.deadline - market.step) or (
                cutoff is not None and submit >= at.normalize() + pd.Timedelta(minutes=cutoff)
            ):
                continue
            # The existing executor delays scheduled exits too. Schedule the
            # time exit one bar earlier so its actual fill keeps the deadline.
            deadline = setup.deadline - market.step if setup.deadline is not None else None
            setup = replace(setup, submit_at=submit, expires_at=setup.expires_at + market.step, deadline=deadline)
        setups[at] = setup
    return setups, examples, int(frame.ready.sum()), len(eligible)


def stress_market(market, case, left, right):
    if case == "missing_data":
        midpoint = left + ((right - left) // market.step // 2) * market.step
        changed = Market(market.trade.drop(midpoint, errors="ignore"), market.mark.drop(midpoint, errors="ignore"),
                         market.funding, market.step, market.scope)
        changed.intrabar_funding = market.intrabar_funding
        return changed
    if case == "mark_divergence":
        changed = copy(market)
        changed.mark_prices = market.mark_prices * 1.003
        return changed
    return market


def funding_executor(market):
    by_side = {}
    for direction in (-1, 1):
        changed = copy(market)
        changed.funding = {at: direction * abs(rate) * 2 for at, rate in market.funding.items()}
        changed.intrabar_funding = {at: [(settled, direction * abs(rate) * 2) for settled, rate in events]
                                   for at, events in market.intrabar_funding.items()}
        by_side[direction] = changed

    def execute(_market, setup, *args, **kwargs):
        return futures_trade(by_side[setup.direction], setup, *args, **kwargs)

    return execute


def ratios(equity, valid):
    if not valid or equity.iloc[0] <= 0 or (equity <= 0).any():
        return {"sharpe": None, "sortino": None, "risk_ratio_days": 0}
    daily = equity.resample("1D").last()
    # Exclude the possibly partial opening and closing days.
    returns = daily.iloc[1:-1].pct_change().dropna()
    sigma = returns.std()
    downside = np.sqrt(np.mean(np.minimum(returns, 0)**2)) if len(returns) else 0.
    sufficient = len(returns) >= 30 and int((returns != 0).sum()) >= 20
    return {"sharpe": float(returns.mean() / sigma * np.sqrt(365.25)) if sufficient and sigma > 0 else None,
            "sortino": float(returns.mean() / downside * np.sqrt(365.25)) if sufficient and downside > 0 else None,
            "risk_ratio_days": len(returns)}


def weekly_uncertainty(trades, max_hold_minutes):
    """Bound the fallback blocks by the strategy's exposure horizon."""
    width = max(3, 2 * ceil(max_hold_minutes / (7 * 1440)))
    minimum_weeks = max(12, 4 * width)
    insufficient = {"status": "INSUFFICIENT_EVIDENCE", "trade_count": len(trades), "minimum_trade_weeks": minimum_weeks}
    if len(trades) < MIN_TRADES:
        return insufficient
    table = pd.DataFrame({"week": [row["exit_at"].tz_localize(None).to_period("W-SUN") for row in trades],
                          "net_r": [row["net_r"] for row in trades]}).groupby("week").net_r.agg(["sum", "count"])
    if len(table) < minimum_weeks:
        return {**insufficient, "weeks_with_trades": len(table)}
    clock = pd.period_range(table.index[0], table.index[-1], freq="W-SUN")
    values = table.reindex(clock, fill_value=0).to_numpy()
    starts = np.random.default_rng(2718).integers(0, len(values) - width + 1, size=(1000, ceil(len(values) / width)))
    blocks = (starts[:, :, None] + np.arange(width)).reshape(1000, -1)[:, :len(values)]
    totals = values[blocks].sum(axis=1)
    active = totals[totals[:, 1] > 0]
    interval = np.quantile(active[:, 0] / active[:, 1], [.025, .975])
    return {"status": "DESCRIPTIVE", "trade_count": len(trades), "weeks_with_trades": len(table),
            "replicates": len(active), "method": f"{width}-week moving-block bootstrap",
            "mean_net_r_95_interval": [float(value) for value in interval], "selection_adjusted": False}


def ema_decision_audit(features, profile, recipe, spec, left, right, ledger, vol_gate):
    clock = pd.date_range(left, right, freq=profile["decision"], inclusive="left")
    names = [*ema_pullback.COMPARISONS, "atr", "swing_low", "swing_high", "realized_vol", "contiguous_bars"]
    values = features.reindex(index=clock, columns=names)
    checks = ema_pullback.gate_frame(features).reindex(clock)
    path = {row["decision_at"]: f"{row['status']}/{row['reason']}" for row in ledger}
    side = entry_sides(features, profile, recipe, spec["sides"]).reindex(clock)
    audit = pd.concat([values, checks], axis=1)
    audit["completed_decision"] = clock.isin(features.index)
    audit["profile_signal"] = side
    audit["volatility_gate"] = values.realized_vol.gt(vol_gate) if vol_gate is not None else None
    audit["account_replay"] = [path.get(at) for at in clock]
    nullable = audit.astype(object).where(audit.notna(), None)
    payload = json.dumps(nullable.values.tolist(), separators=(",", ":"), allow_nan=False).encode("utf-8")
    return {"start": left.isoformat(), "step": profile["decision"], "rows": len(clock),
            "columns": list(audit.columns), "format": "base64+gzip+json-array", "sha256": sha256(payload).hexdigest(),
            "data": b64encode(gzip.compress(payload, mtime=0)).decode("ascii")}


def run_case(market, contract, spec, stage, intervals, dataset_hash, cache):
    window, case = stage.split("/")
    left, right = map(timestamp, intervals[window])
    compiled = compile_spec(spec)
    profile, rules, recipe, risk, cost = compiled
    is_futures = spec["market"] == "FUTURES"
    changed = stress_market(market, case, left, right)
    key = (digest(spec), case == "missing_data")
    features = prepare(changed, spec, cache, key)
    vol_gate = None
    if case == "high_volatility" and not features.empty:
        development = restrict(features, *map(timestamp, intervals["development"]))
        finite = development.loc[development.ready, "realized_vol"].dropna()
        vol_gate = float(finite.median()) if len(finite) else None
    setups, examples, ready, signals = proposals(features, spec, compiled, changed, left, right,
                                                case, dataset_hash, vol_gate)
    if case == "high_volatility" and vol_gate is None:
        setups, examples = {}, []
    if case == "double_cost":
        cost = Cost(cost.fee * 2, cost.adverse * 2)
    elif case == "adverse_price":
        cost = replace(cost, adverse=cost.adverse + .001)
    executor = futures_trade if is_futures else spot_trade
    if case == "adverse_funding":
        executor = funding_executor(changed)
    if case == "delay":
        executor = partial(executor, assumptions=ExecutionAssumptions(delay_exit_bars=1))
    if recipe["entry"] == ema_pullback.FAMILY:
        executor = partial(run_bracket, executor)
    allowance = pd.Timedelta(seconds=rules["processing_seconds"])
    trail = {} if features.empty else {
        execution_open(at + allowance, changed.step): (row.close, row.atr)
        for at, row in restrict(features, left, right).loc[lambda rows: rows.atr.gt(0)].iterrows()
    }
    design = {"seed": 2718, "feature_version": "closed-trade-context", "label_version": "historical-hypothetical",
              "folds": [{"id": window, "outer_start": left, "outer_end": right}],
              "settings": {"evaluation": {"threshold_net_r": 0., "minimum_trades_for_uncertainty": MIN_TRADES,
                                          "monthly_block_bootstrap_samples": 1000}}}
    trial = {"id": spec["strategy_id"], "model": "rules_v1", "cost": case}
    data = {"market": changed, "contract": contract, "risk": risk, "rules": rules, "trail": trail}
    curve = EquityCurve(design, changed.step)
    metrics, ledger = account_replay(design, data, trial, (setups, cost, None, [], {}),
                                     {window: pd.Series(1., index=list(setups), dtype=float)}, {},
                                     curve=curve, executor=executor)
    metrics = account_economics(changed, metrics, ledger)
    expected = int((right - left) // changed.step)
    trade_rows, mark_rows = len(restrict(changed.trade, left, right)), len(restrict(changed.mark, left, right))
    completed = [row for row in ledger if row["status"] == "COMPLETE"]
    net = [row["net"] for row in completed]
    gross_profit, gross_loss = sum(v for v in net if v > 0), -sum(v for v in net if v < 0)
    decision_step = pd.Timedelta(spec["timeframe"])
    expected_ready = int((right - left) // decision_step) if (right - left) % decision_step == pd.Timedelta(0) else None
    complete = (metrics["accounting_complete"] and trade_rows == mark_rows == expected and ready == expected_ready
                and not metrics["ambiguous_closed_trades"] and not metrics["execution_states"].get("INCOMPLETE")
                and (case != "high_volatility" or vol_gate is not None))
    equity = curve.equity(changed, right if metrics["account_state"] == "FLAT" else curve.latest)
    metrics.update(
        accounting_complete=bool(complete), win_rate=sum(v > 0 for v in net) / len(net) if net else None,
        profit_factor=gross_profit / gross_loss if gross_loss else None, cash_reference_return=0.,
        **ratios(equity, complete),
    )
    if complete and profile["max_hold_minutes"] is not None and metrics["uncertainty"]["status"] == "INSUFFICIENT_EVIDENCE":
        metrics["uncertainty"] = weekly_uncertainty(completed, spec["session"]["max_hold_minutes"])
    if not complete:
        for key in ("return", "net_expectancy", "mean_net_r", "win_rate", "profit_factor", "sharpe", "sortino"):
            metrics[key] = None
        metrics.update(completed_folds={}, worst_completed_fold_return=None)
    points = np.unique(np.linspace(0, len(equity) - 1, min(241, len(equity)), dtype=int))
    result = {"case": case, "window": window, "start": left.isoformat(), "end_exclusive": right.isoformat(),
            "status": "DESCRIPTIVE" if complete and len(completed) >= MIN_TRADES else "INCONCLUSIVE",
            "metrics": metrics, "coverage": {"trade": trade_rows, "mark": mark_rows, "expected": expected,
                "feature_ready_decisions": ready, "expected_decisions": expected_ready},
            "signals": signals, "eligible_setups": len(setups), "cost": asdict(cost),
            "volatility_threshold": vol_gate, "proposal_examples": examples,
            "execution_reasons": dict(Counter(row["reason"] for row in ledger)),
            "ledger_hash": digest(json.loads(json.dumps(ledger, default=str, allow_nan=False))),
            "equity": [[equity.index[i].isoformat(), float(equity.iloc[i])] for i in points]}
    if recipe["entry"] == ema_pullback.FAMILY:
        result["decision_audit"] = ema_decision_audit(features, profile, recipe, spec, left, right, ledger, vol_gate)
    return result
