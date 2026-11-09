"""Frozen labels and account replay using the partition-aware execution function."""
from collections import Counter
from dataclasses import asdict

import numpy as np
import pandas as pd

from lab.learning.evaluation import candidates, monthly_uncertainty, outcome_identity
from lab.learning.simulator import Cost
from lab.phase10.trading import run_trade
from lab.storage import utc


def label_batch(design, data, trial):
    settings = design["settings"]
    entry, exit_name = trial["entry"], trial["exit_policy"]
    cost = Cost.load(settings["costs"][trial["cost"]])
    setups, skipped = candidates(data["market"], data["profile"], entry, settings["strategies"][entry], exit_name,
                                 settings["exits"][exit_name], data["features"], data["rules"])
    left, right = utc(settings["evaluation"]["train_start"]), utc(design["available_at_cutoff_exclusive"])
    setups = {at: setup for at, setup in setups.items() if left <= at < right}
    label_equity = data["rules"]["label_equity"]
    labels = [run_trade(data["market"], setup, data["contract"], cost, data["risk"], label_equity,
                        data["rules"], right, trail_updates=data["trail"])
              for setup in setups.values()]
    complete = [record for record in labels if record["status"] == "COMPLETE"]
    training = pd.DataFrame.from_records(
        [(record["decision_at"], record["mature_at"], record["net_r"]) for record in complete],
        columns=["decision_at", "mature_at", "net_r"],
    )
    for name in ("decision_at", "mature_at"):
        training[name] = pd.to_datetime(training[name], utc=True)
    training = training.set_index("decision_at")
    outcome_ids = [outcome_identity(record) for record in complete]
    records = [*labels, *[row for row in skipped if left <= row["decision_at"] < right]]
    metadata = {"observed_label_states": dict(Counter(row["status"] for row in records)),
                "non_complete_reasons": dict(Counter(row["reason"] for row in records if row["status"] != "COMPLETE")),
                "complete_mature_outcomes": len(complete), "label_outcome_ids": outcome_ids,
                "reference_equity": label_equity, "selected_leverage": data["risk"].leverage,
                "features": data["columns"], "fixed_label_capital": True}
    return setups, cost, training, records, metadata


def account_replay(design, data, trial, batch, predictions, fold_versions, *, curve=None, executor=run_trade):
    setups, cost, _, _, _ = batch
    policy = design["settings"]["evaluation"]
    starting = cash = data["risk"].starting_equity
    market = data["market"]
    first = utc(design["folds"][0]["outer_start"])
    end = utc(design["folds"][-1]["outer_end"])
    available, ledger, marks, exposed, filter_counts = first, [], {first: cash}, set(), Counter()
    if curve is not None:
        curve.post(first, cash, False)
    unresolved = None
    for fold in design["folds"]:
        right = utc(fold["outer_end"])
        forecast = predictions.get(fold["id"])
        if forecast is None:
            continue
        for at, score in forecast.items():
            setup = setups[at]
            version = {"fold": fold["id"], "score_net_r": float(score), "model": trial["model"],
                       "model_version": fold_versions.get(fold["id"], trial["model"]), "cost": trial["cost"],
                       "candidate_id": trial["id"], "feature_version": design["feature_version"],
                       "label_version": design["label_version"]}
            skip = ("score_at_or_below_zero" if score <= policy["threshold_net_r"] else
                    "account_occupied_at_sizing" if setup.available_at <= available else None)
            if skip:
                filter_counts[skip] += 1
                ledger.append({**asdict(setup), **version, "status": "SKIPPED", "reason": skip,
                               "selected_leverage": data["risk"].leverage, "order": None})
            else:
                result = executor(market, setup, data["contract"], cost, data["risk"], cash, data["rules"], right,
                                  trail_updates=data["trail"], keep_marks=True)
                for mark_at, equity, holding in result.pop("marks", []):
                    if curve is not None:
                        curve.post(mark_at, equity, holding)
                    else:
                        marks[mark_at] = equity
                        if holding:
                            exposed.add(mark_at)
                ledger.append({**result, **version})
                if result["status"] in ("COMPLETE", "AMBIGUOUS"):
                    cash, available = result["ending_equity"], result["exit_at"]
                    if curve is not None:
                        curve.post(available, cash, False)
                    else:
                        marks[available] = cash
                elif result.get("remaining_base"):
                    unresolved = result["reason"]
                    break
        if unresolved:
            break
    if curve is not None:
        equity = curve.equity(market, curve.latest if unresolved else end)
        exposure_count = curve.exposure_count
    else:
        final_mark = max(marks) if unresolved else end
        observed = market.trade.close_at.loc[(market.trade.close_at >= first) & (market.trade.close_at <= final_mark)]
        index = pd.DatetimeIndex(observed).union(pd.DatetimeIndex(marks.keys())).sort_values()
        equity = pd.Series(marks, dtype=float).reindex(index).ffill()
        exposure_count = len(exposed)
    closed = [row for row in ledger if row["status"] in ("COMPLETE", "AMBIGUOUS")]
    total_net = sum(row["net"] for row in closed)
    top = sum(sorted((row["net"] for row in closed if row["net"] > 0), reverse=True)[:5])
    folds = {}
    for fold in design["folds"]:
        left, right = utc(fold["outer_start"]), utc(fold["outer_end"])
        segment = equity.loc[left:right]
        if len(segment) and segment.index[-1] >= right and fold["id"] in predictions:
            folds[fold["id"]] = float(segment.iloc[-1] / segment.iloc[0] - 1)
    metrics = {
        "full_window_observed": not unresolved and len(predictions) == len(design["folds"]),
        "account_state": "UNRESOLVED_EXPOSURE" if unresolved else "FLAT",
        "unresolved_reason": unresolved, "starting_equity": starting, "ending_observed_equity": float(equity.iloc[-1]),
        "return": float(equity.iloc[-1] / starting - 1) if not unresolved else None,
        "drawdown_observed_marks": float((equity / equity.cummax() - 1).min()), "observed_equity_marks": len(equity),
        "exposure_fraction_of_observed_marks": exposure_count / len(equity),
        "closed_trades": len(closed), "total_realized_net": total_net,
        "net_expectancy": total_net / len(closed) if closed else None,
        "mean_net_r": float(np.mean([row["net_r"] for row in closed])) if closed else None,
        "fees": sum(row.get("fees", 0.) for row in ledger), "funding": sum(row.get("funding", 0.) for row in ledger),
        "liquidation_cost": sum(row.get("liquidation_cost", 0.) for row in ledger),
        "turnover": sum(row.get("turnover", 0.) for row in ledger), "top_five_winners": top,
        "top_five_share_of_net_if_positive": top / total_net if total_net > 0 else None,
        "worst_completed_fold_return": min(folds.values(), default=None), "completed_folds": folds,
        "by_direction": {key: {"trades": sum(row["direction"] == sign for row in closed),
                               "net": sum(row["net"] for row in closed if row["direction"] == sign)}
                         for sign, key in ((1, "long"), (-1, "short"))},
        "execution_states": dict(Counter(row["status"] for row in ledger)), "filter_counts": dict(filter_counts),
        "uncertainty": monthly_uncertainty(closed, design["seed"], policy["minimum_trades_for_uncertainty"],
                                           policy["monthly_block_bootstrap_samples"]),
    }
    return metrics, ledger



