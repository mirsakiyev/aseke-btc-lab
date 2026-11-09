from hashlib import sha256
from itertools import product
from pathlib import Path
import sys

import pandas as pd
from threadpoolctl import threadpool_limits

from lab import __version__
from lab.backtest import Simulation, simulate
from lab.config import ROOT, Execution, Profile, fingerprint, load_profiles
from lab.data import coverage, snapshot
from lab.execution import plan_setups
from lab.features import decisions, feature_columns
from lab.model import (
    Readiness, check_model, dependency_versions, fit, label_set, load_model, prediction_error,
    register, split_labels, windows,
)
from lab.storage import DATA, RUNS, file_hash, read_json, utc, utc_now, write_json, writer_lock


def code_hash() -> str:
    paths = sorted((ROOT / "lab").glob("*.py"))
    paths += [ROOT / name for name in ("profiles.yaml", "research.yaml", "requirements.lock.txt")]
    return fingerprint({path.name: file_hash(path) for path in paths if path.is_file()})


def clip(candles: pd.DataFrame, boundary: pd.Timestamp) -> pd.DataFrame:
    return candles.loc[(candles.index < boundary) & (candles.close_at <= boundary)]


def scenarios(reference: Execution) -> list[Execution]:
    alternatives = {
        (fee, proxy, reference.delay_bars) for fee, proxy in product((25, 50, 80), (5, 10))
    }
    alternatives.discard((reference.fee_bps, reference.proxy_bps, reference.delay_bars))
    alternatives.add((reference.fee_bps, reference.proxy_bps, reference.delay_bars + 1))
    return [Execution(fee_bps=fee, proxy_bps=proxy, delay_bars=delay,
                      processing_seconds=reference.processing_seconds)
            for fee, proxy, delay in sorted(alternatives)]


def save_simulation(directory: Path, name: str, result: Simulation) -> dict:
    result.equity.to_parquet(directory / f"{name}-equity.parquet")
    result.trades.to_parquet(directory / f"{name}-trades.parquet", index=False)
    result.monthly.to_csv(directory / f"{name}-monthly.csv")
    return result.metrics


def predict_signals(model, signals: pd.DataFrame, profile: Profile) -> pd.Series | None:
    if model is None:
        return None
    features = signals.loc[signals.enter & signals.ready, feature_columns(profile)]
    if features.empty:
        return pd.Series(dtype=float, index=features.index, name="predicted_net_return")
    with threadpool_limits(limits=1):
        scores = model.predict(features)
    return pd.Series(scores, index=features.index, name="predicted_net_return")


def latest_run(profile_id: str, train: bool | None = None, root: Path = RUNS) -> tuple[Path, dict] | None:
    choices = ("train", "baseline") if train is None else ("train" if train else "baseline",)
    for mode in choices:
        pointer = root / profile_id / f"latest-{mode}.json"
        if pointer.exists():
            directory = root / profile_id / read_json(pointer)["run_id"]
            return directory, read_json(directory / "manifest.json")
    return None


def run(profile: Profile, train: bool, data_root: Path = DATA, runs_root: Path = RUNS) -> Path:
    with writer_lock(runs_root / ".writer.lock"):
        rules = windows(data_root, persist=True)
        candles, source = snapshot(profile.seconds, data_root)
        holdout = utc(rules.holdout_start)
        request = {
            "profile": profile.model_dump(mode="json"), "profile_hash": profile.identity,
            "lab_version": __version__, "python": sys.version, "code_sha256": code_hash(),
            "dependencies": dependency_versions(), "windows": rules.model_dump(),
            "source_snapshot": source, "requested_fit": train,
            "holdout_used": False,
        }
        run_id = fingerprint(request)[:20]
        directory = runs_root / profile.id / run_id
        manifest_path = directory / "manifest.json"
        if not manifest_path.exists():
            directory.mkdir(parents=True, exist_ok=True)
            summary = {
                **request, "run_id": run_id, "coverage": coverage(candles, profile.seconds, end=utc(rules.data_cutoff)),
                "economic_evidence": "NO_EDGE_DEMONSTRATED", "paper_approved": False,
                "readiness": Readiness.DATA_MISSING, "model": None, "periods": {},
                "sensitivity": [], "availability": "Decision close plus processing allowance",
            }
            if not candles.empty:
                development = clip(candles, holdout)
                study(development, profile, train, directory, summary, source, rules)
            summary["created_at"] = utc_now()
            summary["files"] = {
                path.name: file_hash(path) for path in sorted(directory.iterdir()) if path.is_file()
            }
            write_json(manifest_path, summary)
        mode = "train" if train else "baseline"
        write_json(runs_root / profile.id / f"latest-{mode}.json", {"run_id": run_id})
        return directory


def study(development: pd.DataFrame, profile: Profile, train: bool, directory: Path,
          summary: dict, source: dict, rules) -> None:
    summary["readiness"] = Readiness.INSUFFICIENT_DATA
    if development.empty:
        summary["blocker"] = "No candles before the locked holdout"
        return
    holdout, validate_at, evaluate_at = (
        utc(rules.holdout_start), utc(rules.validation_start), utc(rules.evaluation_start)
    )
    signals = decisions(development, profile)
    ready = signals.index[signals.baseline_ready]
    feature_ready = signals.index[signals.ready]
    summary["warmup"] = {
        "decision_bars": len(signals), "baseline_ready_bars": len(ready),
        "first_baseline_ready_close": ready.min() if len(ready) else None,
        "model_feature_ready_bars": len(feature_ready),
        "first_model_feature_ready_close": feature_ready.min() if len(feature_ready) else None,
    }
    if not len(ready):
        summary["blocker"] = "No completed decision bars have the required baseline and context warmups"
        return
    summary["readiness"] = Readiness.BASELINE_ONLY
    labels = label_set(plan_setups(development, signals, profile, profile.costs, holdout), signals, profile)
    training, summary["training_samples"] = split_labels(labels, None, validate_at)
    training = training.dropna(subset=feature_columns(profile))
    summary["training_samples"]["with_all_features"] = len(training)
    fit_days = (training.index.max() - training.index.min()).total_seconds() / 86400 if len(training) else 0
    summary["training_samples"]["fit_span_days"] = fit_days
    train_ready = len(training) >= profile.min_train and fit_days >= profile.min_train_days
    if train and train_ready:
        fitted = fit(training, profile)
        sample_bytes = pd.util.hash_pandas_object(training, index=True).to_numpy().tobytes()
        summary["model"] = register(
            fitted, profile, source, validate_at, sha256(sample_bytes).hexdigest(), summary["run_id"],
        )
        model = load_model(summary["model"], profile, profile.costs)
        summary["readiness"] = Readiness.TRAINED_UNVALIDATED
    else:
        model = None
        if train:
            summary["blocker"] = (f"Training requires {profile.min_train} resolved labels spanning "
                                  f"{profile.min_train_days} days")
    predictions = predict_signals(model, signals, profile)
    if predictions is not None:
        labels = labels.join(predictions)
    labels.to_parquet(directory / "hypothetical-labels.parquet")
    for name, start, end in (("validation", validate_at, evaluate_at), ("evaluation", evaluate_at, holdout)):
        period = {"start": start, "end": end, "minimum_error_samples": profile.min_eval}
        summary["periods"][name] = period
        sample, period["samples"] = split_labels(labels, start, end)
        sample = sample.dropna(subset=feature_columns(profile))
        period["samples"]["with_all_features"] = len(sample)
        period["prediction_error"] = prediction_error(model, sample, profile) if model is not None else None
        period["sample_floor_met"] = len(sample) >= profile.min_eval
        window_data = clip(development, end)
        if ready.min() >= end or not len(window_data.loc[window_data.index >= start]):
            period["status"] = "INSUFFICIENT_DATA"
            continue
        setups = plan_setups(window_data, signals.loc[signals.index < end], profile, profile.costs, end)
        period["baseline"] = save_simulation(directory, f"{name}-baseline", simulate(
            window_data, setups, profile, profile.costs, start, end,
        ))
        period["status"] = "DEVELOPMENT_ONLY"
        period["candidate"] = None
        if model is not None:
            period["candidate"] = save_simulation(directory, f"{name}-candidate", simulate(
                window_data, setups, profile, profile.costs, start, end, predictions, summary["model"]["model_version"],
            ))
        if name == "evaluation":
            for alternate in scenarios(profile.costs):
                alternate_setups = plan_setups(window_data, signals, profile, alternate, end)
                row = {
                    "evaluation_execution": alternate.model_dump(),
                    "evaluation_execution_hash": fingerprint(alternate.model_dump()),
                    "research_only": True, "promotion_eligible": False,
                    "training_model_version": summary["model"]["model_version"] if model is not None else None,
                    "baseline": simulate(window_data, alternate_setups, profile, alternate, start, end).metrics,
                }
                if model is not None:
                    check_model(summary["model"], profile, alternate, sensitivity=True)
                    row["candidate"] = simulate(window_data, alternate_setups, profile, alternate, start, end,
                                                predictions, summary["model"]["model_version"]).metrics
                summary["sensitivity"].append(row)


def compare_latest(root: Path = RUNS) -> dict:
    runs = {key: latest_run(key, root=root) for key in load_profiles()}
    result = {}
    for model in ("baseline", "candidate"):
        curves = {}
        starts = set()
        for key, item in runs.items():
            if item:
                directory, manifest = item
                metrics = manifest["periods"].get("evaluation", {}).get(model)
                if metrics:
                    starts.add(metrics["evaluation_start"])
                    curve = pd.read_parquet(directory / f"evaluation-{model}-equity.parquet")
                    curves[key] = curve.equity / metrics["starting_cash_usd"] - 1
        if len(curves) < 2 or len(starts) != 1:
            result[model] = {"status": "Fewer than two comparable accounts or differing effective start dates"}
            continue
        shared = pd.concat(curves, axis=1, join="inner").sort_index()
        if shared.empty:
            result[model] = {"status": "No common candle-close marks"}
            continue
        result[model] = {"start": next(iter(starts)), "end": shared.index[-1],
                         "net_return_by_profile": shared.iloc[-1].to_dict()}
    return result
