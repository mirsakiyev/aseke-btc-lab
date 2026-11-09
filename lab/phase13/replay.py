"""Offline replay of the fixed protocol after its observation horizon."""
from dataclasses import replace
import gzip
import json
from pathlib import Path

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from lab.config import fingerprint
from lab.data import OHLCV
from lab.execution import execution_open
from lab.learning.data import Market, validate_prices
from lab.learning.features import build_features, directions
from lab.learning.simulator import Contract, Cost, Risk, make_setup
from lab.phase10.evaluation import account_replay
from lab.phase10.trading import run_trade
from lab.phase12.study import EquityCurve
from lab.phase13.future import prepare_evaluation
from lab.phase13.inference import fitted, inferred_score
from lab.phase13.protocol import active
from lab.phase13.registry import CAMPAIGN, ROOT, checked, freeze, sealed
from lab.storage import file_hash, read_json, utc, write_json, writer_lock


STEP = pd.Timedelta(minutes=1)


def records(asset):
    with gzip.open(checked(asset["file"], asset["sha256"]), "rt", encoding="utf-8") as source:
        for line in source:
            yield json.loads(line)


def prices(asset, basis, start, end):
    columns = OHLCV if basis == "trade" else OHLCV[:4]
    count = int((end - start) // STEP)
    if asset["rows"] != count or not 0 < count <= 90 * 1440:
        raise ValueError("Incomplete or oversized price snapshot")
    values, receipts = np.empty((count, len(columns))), np.empty(count, dtype=np.int64)
    rows = 0
    for row in records(asset):
        if rows >= count:
            raise ValueError("Price snapshot exceeds the registered interval")
        stamp = start + rows * STEP
        close, receipt = utc(row["source_close"]), utc(row["received_at"])
        if utc(row["source_at"]) != stamp or close != stamp + STEP or receipt < close:
            raise ValueError("Price provenance is outside the registered interval or out of sequence")
        values[rows] = [row["observation"][key] for key in columns]
        receipts[rows] = receipt.value
        rows += 1
    if rows != count:
        raise ValueError("Price snapshot row count changed")
    index = pd.date_range(start, end, freq=STEP, inclusive="left")
    frame = pd.DataFrame(values, index=index, columns=list(columns))
    if basis == "mark":
        frame["volume"] = 0.
    frame["close_at"] = index + STEP
    validate_prices(frame, STEP)
    return frame, receipts


def native_market(snapshot):
    assets = snapshot["assets"]
    start, end = map(utc, snapshot["interval"])
    trade, receipts = prices(assets["trade"], "trade", start, end)
    mark, _ = prices(assets["mark"], "mark", start, end)
    market = Market(trade, mark, {}, STEP, "HYPOTHETICAL_REPLAY")
    market.intrabar_funding = {}
    settled = set()
    for row in records(assets["funding"]):
        at, rate = utc(row["source_at"]), float(row["observation"]["rate"])
        if not start <= at < end or at in settled or utc(row["received_at"]) < at or not np.isfinite(rate):
            raise ValueError("Invalid settled funding evidence")
        settled.add(at)
        minute = at.floor(STEP)
        if minute == at:
            market.funding[at] = rate
        else:
            market.intrabar_funding.setdefault(minute, []).append((at, rate))
    if len(settled) != snapshot["funding_slots"]:
        raise ValueError("Registered funding coverage changed")
    return market, receipts


def receipt_setups(item, market, receipts):
    profile, rules = item["profile_rules"], item["execution_rules"]
    features, columns = build_features(market.trade, profile, market.step)
    if columns != item["feature_columns"]:
        raise ValueError("Frozen replay feature schema changed")
    entry, exit_name = item["trial"]["entry"], item["trial"]["exit_policy"]
    sides = directions(features, entry, item["strategy"], profile["long_only"])
    all_required_receipts = np.maximum.accumulate(receipts)
    counts = {"feature_ready": int(features.ready.sum()), "setup_signals": int(sides.ne(0).sum()),
              "unavailable_or_late": 0, "outside_session_or_horizon": 0}
    setups = {}
    for at, side in sides[sides.ne(0)].items():
        pos = market.times.searchsorted(at.value) - 1
        latest = pd.Timestamp(all_required_receipts[pos], tz="UTC")
        if latest > at + pd.Timedelta(seconds=120):
            counts["unavailable_or_late"] += 1
            continue
        plan = make_setup(at, features.loc[at], int(side), entry, exit_name, item["exit_parameters"], profile, rules, STEP)
        available = max(at, latest) + pd.Timedelta(seconds=rules["processing_seconds"])
        submit = execution_open(available, STEP)
        cutoff = at.normalize() + pd.Timedelta(minutes=profile["entry_cutoff_minute_utc"])
        if plan is None or submit >= min(cutoff, market.trade.close_at.iloc[-1], plan.deadline):
            counts["outside_session_or_horizon"] += 1
            continue
        deadline = min(plan.deadline, submit + pd.Timedelta(minutes=profile["max_hold_minutes"]))
        setups[at] = replace(plan, available_at=available, submit_at=submit, expires_at=submit+STEP, deadline=deadline)
    return features, setups, counts


def horizon_trade(market, setup, contract, cost, risk, equity, rules, cutoff, **kwargs):
    if setup.deadline < cutoff:
        return run_trade(market, setup, contract, cost, risk, equity, rules, cutoff, **kwargs)
    # This sentinel lies after the last eligible open. The simulator can observe an
    # earlier exit, but cannot turn the study boundary into a synthetic time exit.
    bounded = replace(setup, deadline=cutoff - pd.Timedelta(nanoseconds=1))
    row = run_trade(market, bounded, contract, cost, risk, equity, rules, cutoff, **kwargs)
    row["deadline"] = setup.deadline
    if row.get("remaining_base"):
        row["reason"] = "PRIMARY_HORIZON_EXPOSURE_UNRESOLVED"
    return row


def day_comparison(registry, protocol, market, receipts, seed):
    candidates = registry["body"]["candidates"]
    item = candidates["day/estimator"]
    features, setups, readiness = receipt_setups(item, market, receipts)
    rules = protocol["body"]["rules"]
    start, end = (protocol["body"][key] for key in ("start", "end_exclusive"))
    risk = Risk(**item["risk"])
    contract = Contract.load(protocol["body"]["instrument"]["contract"])
    cost = Cost(item["cost"]["taker"], item["cost"]["adverse_proxy"])
    data = {"market": market, "contract": contract, "risk": risk, "trail": {}, "rules": item["execution_rules"]}
    evaluation = {"threshold_net_r": item["threshold_net_r"],
                  "minimum_trades_for_uncertainty": rules["sample_floors"]["uncertainty_closed_trades"],
                  "monthly_block_bootstrap_samples": 500}
    fold = "registered_primary"
    design = {"settings": {"evaluation": evaluation}, "seed": seed,
              "folds": [{"id": fold, "outer_start": start, "outer_end": end}],
              "feature_version": "frozen_phase12", "label_version": "phase13_received_future_data"}
    labels = {}
    for at, setup in setups.items():
        row = horizon_trade(market, setup, contract, cost, risk, data["rules"]["label_equity"], data["rules"], utc(end))
        if row["status"] == "COMPLETE" and row["mature_at"] < utc(end):
            labels[at] = row["net_r"]
    output = {}
    for role in ("estimator", "unfiltered", "mean", "cash"):
        candidate = candidates[f"day/{role}"]
        model = fitted(candidate) if role == "estimator" else None
        scores = {at: inferred_score(candidate, features.loc[[at]], model=model)[0] for at in setups} if role != "cash" else {}
        versions = {fold: model[0]["version"]} if model else {}
        metrics, orders = account_replay(design, data, candidate["trial"], (setups, cost, None, [], {}),
                                         {fold: pd.Series(scores, dtype=float)}, versions,
                                         curve=EquityCurve(design, STEP), executor=horizon_trade)
        forecast = None
        if role in ("estimator", "mean"):
            forecast = {"matched_mature_labels": len(labels), "state": "INSUFFICIENT_EVIDENCE"}
            if len(labels) >= rules["sample_floors"]["outer_forecast_mature"]:
                error = np.array([scores[at] - value for at, value in labels.items()])
                forecast.update(state="DESCRIPTIVE_OFFLINE_REPLAY", mae_R=float(abs(error).mean()),
                                rmse_R=float(np.sqrt((error**2).mean())))
        output[role] = {"candidate": candidate["id"], "forecast": forecast, "account": metrics, "orders": orders}
    return {"readiness": readiness, "lanes": output}


def evaluate():
    snapshot = prepare_evaluation()
    if snapshot["state"] != "FROZEN_OFFLINE_SNAPSHOT":
        return snapshot
    protocol, registry = active(), freeze()
    if protocol["body"]["registry"] != registry["id"]:
        raise ValueError("Registered comparison does not match the frozen candidate registry")
    package = sealed(Path(snapshot["path"]), snapshot["snapshot"])["body"]
    if package["protocol"] != protocol["id"] or package["registry"] != registry["id"]:
        raise ValueError("Observation snapshot does not belong to this registered study")
    parent = checked(CAMPAIGN / "manifest.json", registry["body"]["parent_manifest_sha256"])
    seed = sealed(parent, registry["body"]["parent_campaign"])["body"]["design"]["seed"]
    code = [Path(__file__), *(Path(__file__).with_name(name) for name in ("inference.py", "future.py")),
            *(Path(__file__).parent.parent / name for name in ("phase10/evaluation.py", "phase10/trading.py",
              "learning/simulator.py", "learning/features.py"))]
    identifier = fingerprint({"protocol": protocol["id"], "snapshot": snapshot["snapshot"],
                              "code": {p.name: file_hash(p) for p in code}})
    result = ROOT / "future-evaluations" / identifier / "result.json"
    if result.exists():
        return {**read_json(result), "reused": True}
    from lab.phase13.diagnostics import STATE, gates_off
    with writer_lock(STATE / ".supervisor.lock"), writer_lock(STATE / ".fit.lock"), threadpool_limits(limits=2):
        if result.exists():
            return {**read_json(result), "reused": True}
        if not gates_off():
            return {"state": "WAITING_FOR_CONTINUOUS_TRAINING_OFF", "future_evaluations": 0}
        market, receipts = native_market(package)
        day = day_comparison(registry, protocol, market, receipts, seed)
        record = {"id": identifier, "state": "COMPLETE_RESEARCH_REPLAY", "protocol": protocol["id"],
                  "snapshot": snapshot["snapshot"], "scope": "REGISTERED_RECEIPT_OFFLINE_REPLAY",
                  "approval": "No validated model", "historical_estimator_fits": 0, "future_evaluations": 1,
                  "day": day, "swing": {"state": "INSUFFICIENT_EVIDENCE",
                  "reason": "The 201-day feature context exceeds the registered 90-day observation interval"}}
        write_json(result, record)
        write_json(ROOT / "future-result.json", {"id": identifier, "path": str(result), "sha256": file_hash(result)})
        return record
