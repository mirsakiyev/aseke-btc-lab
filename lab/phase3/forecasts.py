from hashlib import sha256
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from lab.config import Profile, fingerprint
from lab.execution import plan_setups
from lab.features import decisions
from lab.model import fit, label_set, split_labels
from lab.phase2.audit import distribution
from lab.research import clip
from lab.storage import file_hash, utc, write_json


def error(target: pd.Series, prediction: pd.Series) -> dict | None:
    if target.empty:
        return None
    residual = prediction - target
    return {"count": len(target), "mae": float(abs(residual).mean()),
            "rmse": float(np.sqrt((residual ** 2).mean())), "bias_prediction_minus_target": float(residual.mean()),
            "prediction_mean": float(prediction.mean()), "realized_target_mean": float(target.mean())}


def quality(rows: pd.DataFrame, reporting_floor: int) -> dict:
    sample = rows.loc[rows.target_evaluable & rows.ridge_score.notna() & rows.mean_score.notna()]
    dependence = {}
    if len(sample):
        _, intervals = split_labels(sample, None, sample.resolved_at.max() + pd.Timedelta("1ns"))
        dependence = {key: intervals[key] for key in (
            "overlapping_outcome_pairs", "entries_overlapping_earlier_outcomes",
        )}
    reports = {
        "paired_label_count": len(sample),
        "sample_status": "descriptive" if len(sample) >= reporting_floor else "sparse_or_unfitted",
        "error_same_mature_labels": {name: error(sample.target, sample[f"{name}_score"]) for name in ("ridge", "mean")},
        "scores": {}, "dependence": dependence,
    }
    for name in ("ridge", "mean"):
        scored = rows.loc[rows[f"{name}_score"].notna()]
        scores = scored[f"{name}_score"]
        accepted = scored[f"{name}_accepted"]
        groups = {}
        for label, flag in (("passed", True), ("rejected", False)):
            segment = sample.loc[sample[f"{name}_accepted"].eq(flag)]
            groups[label] = {
                "sparse": len(segment) < reporting_floor,
                "prediction": distribution(segment[f"{name}_score"]), "realized_target": distribution(segment.target),
            }
        reports["scores"][name] = {
            **distribution(scores), "std": float(scores.std(ddof=0)) if len(scores) else None,
            "accepted": int(accepted.sum()), "acceptance_rate": float(accepted.mean()) if len(accepted) else None,
            "mature_by_acceptance": groups,
        }
    return reports


def build_forecasts(candles: pd.DataFrame, profile: Profile, plan: dict, directory: Path):
    models = directory / "fold-models"
    models.mkdir()
    holdout = utc(plan["windows"]["holdout_start"])
    signals = decisions(candles, profile)
    reference = plan_setups(candles, signals, profile, profile.costs, holdout)
    labels = label_set(reference, signals, profile)
    cols = plan["feature_columns"]
    threshold = plan["entry_threshold_decimal_strictly_greater_than"]
    if threshold != 0:
        raise ValueError("The existing simulation gate expects the frozen zero threshold")
    row_sets, folds = [], []
    for frozen in plan["folds"]:
        start = utc(frozen["training_cutoff_utc"])
        end = utc(frozen["evaluation_end_exclusive_utc"])
        before = signals.loc[signals.index < start]
        warm = before.index[before.ready]
        usable_months = int(warm.strftime("%Y-%m").nunique())
        elapsed_ok = bool(len(warm) and warm[0] + pd.DateOffset(months=plan["initial_training_months_min"]) <= start)
        training, stats = split_labels(labels, None, start)
        training = training.dropna(subset=cols)
        span = (training.index[-1] - training.index[0]).total_seconds() / 86400 if len(training) else 0
        _, eval_stats = split_labels(labels, start, end)
        feature_period = signals.loc[(signals.index >= start) & (signals.index < end)]
        setup_period = labels.loc[(labels.index >= start) & (labels.index < end)].copy()
        rows = setup_period.loc[:, ["entry_at", "resolved_at", "status", "target"]].copy()
        rows["fold"] = frozen["id"]
        rows["target_evaluable"] = rows.target.notna() & (rows.resolved_at < end) & setup_period[cols].notna().all(axis=1)
        rows["target_exclusion"] = np.select(
            [rows.target_evaluable, rows.target.notna() & (rows.resolved_at >= end), rows.target.notna()],
            ["none", "outcome_crossed_fold_boundary", "missing_features"], default=rows.status,
        )
        rows.loc[~rows.target_evaluable, "target"] = np.nan
        rows[["ridge_score", "mean_score"]] = np.nan
        rows["ridge_model_id"] = None
        rows["mean_model_id"] = None
        fit_issues = []
        if not elapsed_ok or usable_months < plan["initial_training_months_min"]:
            fit_issues.append("less_than_24_usable_training_calendar_months")
        if len(training) < profile.min_train:
            fit_issues.append("too_few_mature_training_labels")
        if span < profile.min_train_days:
            fit_issues.append("training_label_span_too_short")
        summary = {
            **frozen, "fit_state": "insufficient" if fit_issues else "fitted", "insufficient_reasons": fit_issues,
            "warm_calendar_months": usable_months, "first_warm_close": warm.min() if len(warm) else None,
            "warm_training_decision_bars": len(warm),
            "training": {**stats, "fit_count": len(training), "fit_span_days": span,
                         "latest_resolved_at": training.resolved_at.max() if len(training) else None,
                         "target": distribution(training.target)},
            "evaluation": {**eval_stats, "completed_decision_bars": len(feature_period),
                           "warm_decision_bars": int(feature_period.ready.sum()), "target_evaluable": int(rows.target_evaluable.sum()),
                           "target": distribution(rows.target), "target_exclusions": rows.target_exclusion.value_counts().to_dict()},
            "fold_models": None,
        }
        if not fit_issues:
            # Independently replan from the prefix to prove no fit membership or outcome needs later prices.
            prefix = clip(candles, start)
            prefix_features = decisions(prefix, profile)
            pd.testing.assert_frame_equal(prefix_features.tail(4), signals.loc[prefix_features.index].tail(4))
            prefix_labels = label_set(plan_setups(prefix, prefix_features, profile, profile.costs, start),
                                      prefix_features, profile)
            prefix_train, _ = split_labels(prefix_labels, None, start)
            pd.testing.assert_frame_equal(training, prefix_train.dropna(subset=cols))
            sample_hash = sha256(pd.util.hash_pandas_object(training, index=True).to_numpy().tobytes()).hexdigest()
            identity = {"design_sha256": plan["design_sha256"], "data_sha256": plan["source_snapshot"]["sha256"],
                        "profile_hash": profile.identity, "training_cutoff": start.isoformat(),
                        "training_sample_sha256": sample_hash, "training_execution": plan["cost_scenarios"]["reference"],
                        "feature_version": profile.feature_version, "target_version": profile.target_version,
                        "strategy_version": profile.strategy_version, "threshold": threshold, "approved": False}
            model = fit(training, profile)
            mean = float(training.target.mean())
            ridge_id = fingerprint({**identity, "method": "ridge", "settings": plan["ridge"]})[:20]
            mean_id = fingerprint({**identity, "method": "mean"})[:20]
            artifact = models / f"{frozen['id']}-{ridge_id}.joblib"
            joblib.dump(model, artifact, compress=3)
            eligible = setup_period[cols].dropna()
            if len(eligible):
                rows.loc[eligible.index, "ridge_score"] = model.predict(eligible)
                rows.loc[eligible.index, "mean_score"] = mean
            rows["ridge_model_id"], rows["mean_model_id"] = ridge_id, mean_id
            training[["entry_at", "resolved_at", "target", *cols]].to_parquet(
                models / f"{frozen['id']}-training-labels.parquet"
            )
            record = {**identity, "ridge_id": ridge_id, "mean_id": mean_id, "mean_decimal": mean,
                      "artifact": artifact.name, "artifact_sha256": file_hash(artifact),
                      "fit_count": len(training), "fit_data_prefix_verified": True}
            write_json(models / f"{frozen['id']}.json", record)
            summary["fold_models"] = record
        rows["fit_eligible"] = not fit_issues
        for name in ("ridge", "mean"):
            rows[f"{name}_accepted"] = rows[f"{name}_score"].gt(threshold)
        summary["prediction_quality"] = quality(rows, profile.min_eval)
        row_sets.append(rows)
        folds.append(summary)
    result = pd.concat(row_sets).sort_index()
    result.to_parquet(directory / "reference-predictions.parquet")
    return signals, result, folds
