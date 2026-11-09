from collections import Counter
from dataclasses import asdict
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from lab.config import fingerprint
from lab.execution import execution_open
from lab.learning.features import build_features, directions
from lab.learning.simulator import Contract, Cost, Risk, make_setup, run_trade
from lab.storage import file_hash, utc


def candidates(market, profile, strategy, parameters, exit_name, exit_policy, features, rules):
    signal = directions(features, strategy, parameters, profile["long_only"])
    setups, skipped = {}, []
    for at, side in signal[signal != 0].items():
        setup = make_setup(at, features.loc[at], int(side), strategy, exit_name, exit_policy, profile, rules, market.step)
        if setup is None:
            skipped.append({"decision_at": at, "direction": int(side), "status": "REJECTED", "reason": "session_ineligible"})
        else:
            setups[at] = setup
    return setups, skipped


def prepare(design, profile_id, market, instrument):
    settings = design["settings"]
    profile = settings["profiles"][profile_id]
    features, columns = build_features(market.trade, profile, market.step)
    rules = settings["execution"]
    allowance = pd.Timedelta(seconds=rules["processing_seconds"])
    trail = {execution_open(at + allowance, market.step): (row.close, row.atr)
             for at, row in features[features.atr.gt(0)].iterrows()}
    return {"market": market, "profile": profile, "features": features, "columns": columns, "trail": trail,
            "rules": rules, "contract": Contract.load(instrument), "risk": Risk(**settings["risk"][profile["risk"]])}


def outcome_identity(record):
    return fingerprint([utc(record[key]).isoformat() for key in ("decision_at", "mature_at")])


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


def matured(labels, start, cutoff):
    # A known exit at the split itself is unavailable to its next fitting window.
    return labels[(labels.index >= start) & (labels.index < cutoff) & (labels.mature_at < cutoff)]


def fit_model(kind, params, seed, data, labels):
    train_x = data["features"].loc[labels.index, data["columns"]]
    train_y = labels.net_r
    if kind == "mean":
        return float(train_y.mean())
    if kind == "ridge":
        model = make_pipeline(StandardScaler(), Ridge(alpha=params["alpha"]))
    elif kind == "hist":
        options = {key: value for key, value in params.items() if key not in ("type", "minimum_train")}
        if options["early_stopping"]:
            raise ValueError("Internal randomized validation is forbidden")
        model = HistGradientBoostingRegressor(random_state=seed, **options)
    else:
        raise ValueError(f"Unknown model {kind}")
    return model.fit(train_x, train_y)


def scores(model, data, index):
    if not len(index):
        return pd.Series(dtype=float, index=index)
    values = model if isinstance(model, float) else model.predict(data["features"].loc[index, data["columns"]])
    return pd.Series(values, index=index, dtype=float)


def errors(prediction, labels):
    if not len(labels):
        return {"count": 0, "mae_net_r": None, "rmse_net_r": None}
    residual = prediction.loc[labels.index] - labels.net_r
    return {"count": len(residual), "mae_net_r": float(residual.abs().mean()),
            "rmse_net_r": float(np.sqrt((residual ** 2).mean()))}


def forecasts(design, data, trial, batch, model_directory, stage):
    setups, _, labels, _, _ = batch
    settings = design["settings"]
    policy = settings["evaluation"]
    params = settings["models"].get(trial["model"], {"type": "mean", "minimum_train": policy["minimum_inner"]})
    kind = params["type"]
    start = utc(policy["train_start"])
    evidence, predictions = [], {}
    setup_index = pd.DatetimeIndex(setups.keys(), tz="UTC")
    for fold in design["folds"]:
        inner, left, right = (utc(fold[key]) for key in ("inner_start", "outer_start", "outer_end"))
        evaluation = setup_index[(setup_index >= left) & (setup_index < right)]
        if kind == "unfiltered":
            predictions[fold["id"]] = pd.Series(1., index=evaluation)
            evidence.append({"fold": fold["id"], "status": "RULES", "planned_setups": len(evaluation)})
            continue
        inner_train, outer_train = matured(labels, start, inner), matured(labels, start, left)
        validation, outer = matured(labels, inner, left), matured(labels, left, right)
        counts = {"inner_train": len(inner_train), "inner_validation": len(validation),
                  "outer_train": len(outer_train), "outer_mature": len(outer)}
        record = {"fold": fold["id"], "sample_counts": counts,
                  "purged_inner_train": int(((labels.index < inner) & (labels.mature_at >= inner)).sum()),
                  "purged_outer_train": int(((labels.index < left) & (labels.mature_at >= left)).sum())}
        required = params["minimum_train"]
        if len(inner_train) < required or len(outer_train) < required or len(validation) < policy["minimum_inner"]:
            evidence.append({**record, "status": "INSUFFICIENT_EVIDENCE", "reason": "Chronological train/inner sample floor"})
            continue
        stage("TRAINING", {"fold": fold["id"], "partition": "inner", "rows": len(inner_train)})
        inner_model = fit_model(kind, params, design["seed"], data, inner_train)
        stage("TRAINING", {"fold": fold["id"], "partition": "outer", "rows": len(outer_train)})
        outer_model = fit_model(kind, params, design["seed"], data, outer_train)
        stage("EVALUATING")
        candidate_scores = scores(outer_model, data, evaluation)
        predictions[fold["id"]] = candidate_scores
        outer_score = scores(outer_model, data, outer.index)
        artifacts = {}
        if kind != "mean":
            for fit_name, model, fit_labels in (("inner", inner_model, inner_train), ("outer", outer_model, outer_train)):
                version = fingerprint([trial, fold, fit_name, [(str(at), float(row.net_r), str(row.mature_at))
                                                              for at, row in fit_labels.iterrows()],
                                       design["seed"], design["feature_version"], design["label_version"],
                                       design["code_sha256"], design["data"][trial["profile"]]])[:24]
                destination = model_directory / f"{version}.joblib"
                model_directory.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_suffix(".tmp")
                joblib.dump({"estimator": model, "version": version, "fold": fold, "fit": fit_name, "trial": trial,
                             "features": data["columns"]}, temporary, compress=3)
                temporary.replace(destination)
                artifacts[fit_name] = {"version": version, "sha256": file_hash(destination), "path": str(destination)}
        evidence.append({**record, "status": "EVALUATED" if len(outer) >= policy["minimum_outer"] else "THIN_OUTER",
                         "inner_error": errors(scores(inner_model, data, validation.index), validation),
                         "outer_error": errors(outer_score, outer), "model_artifacts": artifacts,
                         "fit_count": 2 if kind != "mean" else 0})
    return predictions, evidence


def monthly_uncertainty(closed, seed, minimum, repetitions):
    if len(closed) < minimum:
        return {"status": "INSUFFICIENT_EVIDENCE", "trade_count": len(closed)}
    monthly = pd.DataFrame({"month": [item["exit_at"].tz_localize(None).to_period("M") for item in closed],
                            "net_r": [item["net_r"] for item in closed]}).groupby("month").net_r.agg(["sum", "count"])
    if len(monthly) < 8:
        return {"status": "INSUFFICIENT_EVIDENCE", "trade_count": len(closed), "months_with_trades": len(monthly)}
    values = monthly.reindex(pd.period_range(monthly.index[0], monthly.index[-1], freq="M"), fill_value=0).to_numpy()
    width = 3
    generator = np.random.default_rng(seed)
    starts = generator.integers(0, len(values) - width + 1, size=(repetitions, int(np.ceil(len(values) / width))))
    indices = (starts[:, :, None] + np.arange(width)).reshape(repetitions, -1)[:, :len(values)]
    sums = values[indices].sum(axis=1)
    ratios = sums[sums[:, 1] > 0, 0] / sums[sums[:, 1] > 0, 1]
    lower, upper = np.quantile(ratios, [.025, .975])
    return {"status": "DESCRIPTIVE", "trade_count": len(closed), "months_with_trades": len(monthly),
            "replicates": len(ratios), "method": "Three-consecutive-calendar-month moving-block bootstrap",
            "mean_net_r_95_interval": [float(lower), float(upper)], "selection_adjusted": False}


def account_replay(design, data, trial, batch, predictions, fold_versions):
    setups, cost, _, _, _ = batch
    policy = design["settings"]["evaluation"]
    starting = cash = data["risk"].starting_equity
    market = data["market"]
    first = utc(design["folds"][0]["outer_start"])
    end = utc(design["folds"][-1]["outer_end"])
    available, ledger, marks, exposed, filter_counts = first, [], {first: cash}, set(), Counter()
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
                result = run_trade(market, setup, data["contract"], cost, data["risk"], cash, data["rules"], right,
                                   trail_updates=data["trail"], keep_marks=True)
                for mark_at, equity, holding in result.pop("marks", []):
                    marks[mark_at] = equity
                    if holding:
                        exposed.add(mark_at)
                ledger.append({**result, **version})
                if result["status"] in ("COMPLETE", "AMBIGUOUS"):
                    cash, available = result["ending_equity"], result["exit_at"]
                    marks[available] = cash
                elif result.get("remaining_base"):
                    unresolved = result["reason"]
                    break
        if unresolved:
            break
    final_mark = max(marks) if unresolved else end
    observed = market.trade.close_at.loc[(market.trade.close_at >= first) & (market.trade.close_at <= final_mark)]
    index = pd.DatetimeIndex(observed).union(pd.DatetimeIndex(marks.keys())).sort_values()
    equity = pd.Series(marks, dtype=float).reindex(index).ffill()
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
        "exposure_fraction_of_observed_marks": len(exposed) / len(equity),
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


def benchmarks(design, data, cost_name, kind):
    left, right = (utc(design["folds"][0]["outer_start"]), utc(design["folds"][-1]["outer_end"]))
    frame = data["market"].trade
    if kind == "cash":
        return {"return": 0., "drawdown_observed_marks": 0., "closed_trades": 0}
    if data["contract"].family != "spot":
        return {"status": "NOT_APPLICABLE", "reason": "No unlevered cash-asset control on this perpetual account"}
    if left not in frame.index:
        return {"status": "INSUFFICIENT_EVIDENCE", "reason": "Missing scheduled passive start opening"}
    start_price = float(frame.loc[left, "open"])
    prices = frame.loc[(frame.index >= left) & (frame.close_at <= right), "close"]
    cost = Cost.load(design["settings"]["costs"][cost_name])
    equity = .75 + .25 * prices * (1 - cost.adverse) * (1 - cost.fee) / (
        start_price * (1 + cost.adverse) * (1 + cost.fee)
    )
    values = pd.Series([1., *equity.tolist()])
    return {"return": float(values.iloc[-1] - 1), "drawdown_observed_marks": float((values / values.cummax() - 1).min()),
            "weight_at_start": .25, "observed_equity_marks": len(values),
            "basis": "Fee-inclusive passive 25% BTC with modeled exit costs marked."}
