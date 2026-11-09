from enum import StrEnum
from importlib.metadata import version
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from pydantic import model_validator
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits
import yaml

from lab.config import ROOT, Execution, Frozen, Profile, fingerprint
from lab.execution import Setup, trade_return
from lab.features import feature_columns
from lab.storage import DATA, MODELS, file_hash, read_json, utc, write_json


class Readiness(StrEnum):
    DATA_MISSING = "DATA_MISSING"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    BASELINE_ONLY = "BASELINE_ONLY"
    TRAINED_UNVALIDATED = "TRAINED_UNVALIDATED"
    VALIDATED_FOR_PAPER = "VALIDATED_FOR_PAPER"
    STALE_MODEL = "STALE_MODEL"


class Windows(Frozen):
    data_cutoff: str
    holdout_start: str
    validation_start: str
    evaluation_start: str

    @model_validator(mode="after")
    def check_dates(self):
        cutoff = utc(self.data_cutoff)
        if not utc(self.validation_start) < utc(self.evaluation_start) < utc(self.holdout_start):
            raise ValueError("Windows must be chronological")
        if utc(self.holdout_start) != cutoff - pd.DateOffset(years=1):
            raise ValueError("Reserve exactly twelve calendar months for the holdout")
        dates = self.model_dump().values()
        if any(utc(date) != utc(date).normalize() for date in dates):
            raise ValueError("Research window boundaries must be at UTC midnight")
        return self


def windows(root: Path = DATA, persist: bool = False) -> Windows:
    rules = Windows(**yaml.safe_load((ROOT / "research.yaml").read_text(encoding="utf-8")))
    path = root / "holdout.lock.json"
    if path.exists() and read_json(path) != rules.model_dump():
        raise ValueError("research.yaml differs from the existing locked research windows")
    if persist and not path.exists():
        write_json(path, rules.model_dump())
    return rules


def label_set(setups: list[Setup], signals: pd.DataFrame, profile: Profile) -> pd.DataFrame:
    columns = feature_columns(profile)
    features = signals.loc[:, columns]
    rows = [{
        "signal_at": setup.signal_at, "available_at": setup.available_at, "entry_at": setup.entry_at,
        "resolved_at": setup.exit_at, "deadline": setup.deadline, "status": setup.reason,
        "entry_raw": setup.entry_raw, "exit_raw": setup.exit_raw,
        "target": trade_return(setup, profile.costs) if setup.exit_raw is not None else None,
    } for setup in setups]
    keys = ["signal_at", "available_at", "entry_at", "resolved_at", "deadline", "status",
            "entry_raw", "exit_raw", "target"]
    labels = pd.DataFrame(rows, columns=keys)
    for name in ("signal_at", "available_at", "entry_at", "resolved_at", "deadline"):
        labels[name] = pd.to_datetime(labels[name], utc=True)
    labels["target"] = labels.target.astype(float)
    return labels.set_index("signal_at").join(features, how="left")


def split_labels(labels: pd.DataFrame, start: pd.Timestamp | None,
                 end: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    assigned = labels[labels.index < end]
    if start is not None:
        assigned = assigned[assigned.index >= start]
    resolved = assigned[assigned.target.notna()]
    kept = resolved[resolved.resolved_at < end]
    # For each entry, count earlier positions whose outcome was not yet resolved at entry.
    intervals = kept.sort_values("entry_at")
    ends = np.sort(pd.DatetimeIndex(intervals.resolved_at).as_unit("ns").asi8)
    entries = pd.DatetimeIndex(intervals.entry_at).as_unit("ns").asi8
    overlapping = np.arange(len(entries)) - np.searchsorted(ends, entries, side="left")
    return kept, {
        "signals": len(assigned), "usable": len(kept), "purged_at_end": len(resolved) - len(kept),
        "unresolved_or_ineligible": len(assigned) - len(resolved),
        "status_counts": {key: int(n) for key, n in assigned.status.value_counts().items()},
        "first_signal": kept.index.min() if len(kept) else None,
        "last_signal": kept.index.max() if len(kept) else None,
        "signal_span_days": (kept.index.max() - kept.index.min()).total_seconds() / 86400 if len(kept) else 0,
        "overlapping_outcome_pairs": int(overlapping.sum()),
        "entries_overlapping_earlier_outcomes": int((overlapping > 0).sum()),
    }


def fit(train: pd.DataFrame, profile: Profile) -> Pipeline:
    pipeline = make_pipeline(StandardScaler(), Ridge(alpha=profile.ridge_alpha, solver="cholesky"))
    with threadpool_limits(limits=1):
        return pipeline.fit(train[feature_columns(profile)], train.target)


def prediction_error(model: Pipeline, sample: pd.DataFrame, profile: Profile) -> dict | None:
    if sample.empty:
        return None
    with threadpool_limits(limits=1):
        errors = model.predict(sample[feature_columns(profile)]) - sample.target
    return {"samples": len(sample), "mae": float(abs(errors).mean()),
            "rmse": float(np.sqrt((errors ** 2).mean())), "bias": float(errors.mean())}


def dependency_versions() -> dict:
    return {name: version(name) for name in (
        "numpy", "pandas", "pyarrow", "duckdb", "httpx", "scikit-learn", "scipy", "vectorbt",
        "numba", "plotly", "streamlit", "pydantic", "PyYAML", "joblib", "tzdata",
    )}


def logic_hash() -> str:
    names = ("config", "data", "features", "execution", "backtest", "broker", "model")
    return fingerprint({name: file_hash(ROOT / "lab" / f"{name}.py") for name in names})


def model_identity(profile: Profile) -> dict:
    return {
        "venue": profile.venue, "instrument": profile.instrument, "market_type": profile.market_type,
        "profile_id": profile.id, "profile_version": profile.version, "profile_hash": profile.identity,
        "decision_timeframe": profile.decision, "feature_columns": feature_columns(profile),
        "feature_version": profile.feature_version, "target_version": profile.target_version,
        "strategy_version": profile.strategy_version,
        "training_execution_hash": fingerprint(profile.costs.model_dump()),
        "training_execution": profile.costs.model_dump(), "logic_hash": logic_hash(),
        "dependencies": dependency_versions(), "entry_threshold": 0,
    }


def check_model(metadata: dict, profile: Profile, costs: Execution, *, sensitivity: bool = False) -> None:
    for key, value in model_identity(profile).items():
        if metadata.get(key) != value:
            raise ValueError(f"Incompatible model metadata: {key}")
    if costs != profile.costs and (
        not sensitivity or costs.processing_seconds != profile.costs.processing_seconds
    ):
        raise ValueError("Alternate costs or delay require a research-only sensitivity run")


def register(model: Pipeline, profile: Profile, snapshot_record: dict, cutoff: pd.Timestamp,
             sample_hash: str, report_id: str, root: Path = MODELS) -> dict:
    metadata = {
        **model_identity(profile), "data_sha256": snapshot_record["sha256"],
        "checkpoint_hash": snapshot_record["checkpoint_hash"], "training_cutoff": cutoff.isoformat(),
        "training_sample_sha256": sample_hash, "registry_status": Readiness.TRAINED_UNVALIDATED,
        "development_report_id": report_id,
    }
    version_id = fingerprint(metadata)[:24]
    directory = root / profile.id / version_id
    artifact = directory / "ridge.joblib"
    metadata_path = directory / "model.json"
    if metadata_path.exists():
        existing = read_json(metadata_path)
        if existing["artifact_sha256"] != file_hash(artifact):
            raise ValueError("Existing model artifact failed hash verification")
        return existing
    directory.mkdir(parents=True, exist_ok=True)
    temporary = artifact.with_suffix(".tmp")
    joblib.dump(model, temporary, compress=3)
    temporary.replace(artifact)
    metadata.update({"model_version": version_id, "artifact_sha256": file_hash(artifact)})
    write_json(metadata_path, metadata)
    return metadata


def load_model(metadata: dict, profile: Profile, costs: Execution, *, sensitivity: bool = False,
               root: Path = MODELS) -> Pipeline:
    check_model(metadata, profile, costs, sensitivity=sensitivity)
    version_id = metadata["model_version"]
    if len(version_id) != 24 or any(char not in "0123456789abcdef" for char in version_id):
        raise ValueError("Invalid model identifier")
    artifact = root / profile.id / version_id / "ridge.joblib"
    if file_hash(artifact) != metadata["artifact_sha256"]:
        raise ValueError("Model artifact failed hash verification")
    return joblib.load(artifact)  # Only trusted local registry contents.
