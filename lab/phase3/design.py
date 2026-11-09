from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from lab.config import Profile, fingerprint
from lab.data import feed
from lab.features import feature_columns
from lab.model import dependency_versions, logic_hash, windows
from lab.phase2.checkpoint import AUDITS, frozen_runs, preserve
from lab.storage import RUNS, file_hash, read_json, utc, utc_now, write_json

OUTPUT = RUNS / "phase3"
VERSION = "swing-continuous-1"
METHODS = ("baseline", "ridge", "mean")
AUDIT_ID = "20261108T222605Z"


def calendar(first_ready: pd.Timestamp, holdout_start: pd.Timestamp) -> list[dict]:
    minimum = first_ready + pd.DateOffset(months=24)
    anchor = utc(f"{minimum.year}-01-01")
    while anchor < minimum:
        anchor += pd.DateOffset(months=6)
    folds = []
    while anchor < holdout_start and len(folds) < 12:
        expected = anchor + pd.DateOffset(months=6)
        right = min(expected, holdout_start)
        folds.append({"id": f"f{len(folds) + 1:02d}", "training_cutoff_utc": anchor.isoformat(),
                      "evaluation_start_utc": anchor.isoformat(), "evaluation_end_exclusive_utc": right.isoformat(),
                      "partial": right < expected})
        anchor = right
    return folds


def freeze(root: Path = OUTPUT) -> Path:
    originals = preserve()
    folder, run = frozen_runs()["swing"]
    profile = Profile(**run["profile"])
    reference = profile.costs.model_dump()
    higher = {**reference, "fee_bps": 80, "proxy_bps": 10}
    delayed = {**reference, "delay_bars": reference["delay_bars"] + 1}
    observed_stress = [row["evaluation_execution"] for row in run["sensitivity"]]
    if higher not in observed_stress or delayed not in observed_stress:
        raise ValueError("Study stress must come from the original approved research grid")
    if windows().model_dump() != run["windows"] or logic_hash() != run["model"]["logic_hash"]:
        raise ValueError("Registered holdout or compatible Phase 1 execution code changed")
    first_ready = utc(run["warmup"]["first_model_feature_ready_close"])
    holdout = utc(run["windows"]["holdout_start"])
    folds = calendar(first_ready, holdout)
    if not folds:
        raise ValueError("Insufficient date coverage for a frozen schedule")
    audit = AUDITS / AUDIT_ID
    plan = {
        "version": VERSION, "profile": profile.model_dump(mode="json"), "profile_hash": profile.identity,
        "source_snapshot": run["source_snapshot"], "source_profile_checkpoint": folder.name,
        "source_profile_manifest_sha256": file_hash(folder / "manifest.json"),
        "phase1_checkpoint_source_sha256": originals["source_archive_sha256"],
        "phase2_audit": AUDIT_ID, "phase2_reconciliation_sha256": file_hash(audit / "data-audit.json"),
        "phase1_logic_sha256": logic_hash(), "dependencies": dependency_versions(),
        "windows": run["windows"], "folds": folds, "max_folds": 12, "initial_training_months_min": 24,
        "first_warm_feature_close_utc": first_ready.isoformat(),
        "schedule_rule": "First January/July boundary at least 24 calendar months after first usable warmup. "
                         "Consecutive six-month blocks, with the last truncated at the holdout.",
        "first_fit_coverage": "Require 24 calendar months after first warm feature close and 24 month-buckets "
                              "with at least one warmed-up decision before the first cutoff.",
        "methods": list(METHODS), "feature_columns": feature_columns(profile),
        "ridge": {"alpha": profile.ridge_alpha, "solver": "cholesky", "preprocessor": "StandardScaler, training only"},
        "mean": "Arithmetic mean of the exact training labels used by Ridge in that fold",
        "entry_threshold_decimal_strictly_greater_than": run["model"]["entry_threshold"],
        "fold_eligibility": {"training_labels_min": profile.min_train, "training_signal_span_days_min": profile.min_train_days,
                             "prediction_reporting_floor": profile.min_eval},
        "cost_scenarios": {"reference": reference, "higher_friction": higher, "extra_bar_delay": delayed},
        "price_and_window_semantics": "Decision close belongs to [left,right). Use bars with open < the "
                                     "holdout boundary; the last bar may close at that boundary. "
                                     "Training outcome must resolve strictly before fit cutoff, evaluation "
                                     "outcome strictly before fold right. Open-time fills are known at that "
                                     "open; an intrabar stop resolves at its completed candle close.",
        "portfolio_rules": "One continuous independent account per method and cost scenario. A signal keeps its "
                           "originating fold even if its order or exit crosses a boundary. Insufficient fit "
                           "folds block new entries for all three methods. A missing bar during exposure "
                           "stops the affected account. Matched results end at the earliest valid mark.",
        "benchmarks": "Zero-yield cash, buy-and-hold BTC and 25% passive BTC. Enter at the first evaluation "
                      "opening, account for the scenario's adverse price and fee, then hold quantity.",
        "sensitivity_rule": "Use unchanged reference-trained artifacts and the same threshold. Recompute "
                            "execution and sizing under the two frozen counterfactuals.",
        "scope": "Exploratory development. The original May–November 2025 study is prior evidence only; "
                 "do not combine its overlapping returns with this series. Holdout performance sealed.",
        "holdout_used_for_training_or_evaluation": False, "approved": False,
    }
    stamp = utc_now()
    directory = root / f"{stamp.strftime('%Y%m%dT%H%M%SZ')}-{fingerprint(plan)[:10]}"
    directory.mkdir(parents=True)
    write_json(directory / "frozen-study.json", {
        **plan, "frozen_at_utc": stamp.isoformat(), "design_sha256": fingerprint(plan),
    })
    return directory


def load_plan(directory: Path) -> dict:
    plan = read_json(directory / "frozen-study.json")
    hashed = {key: value for key, value in plan.items() if key not in ("frozen_at_utc", "design_sha256")}
    if fingerprint(hashed) != plan["design_sha256"] or plan["version"] != VERSION:
        raise ValueError("Frozen study manifest is incompatible or altered")
    if plan["phase1_logic_sha256"] != logic_hash() or plan["dependencies"] != dependency_versions():
        raise ValueError("The frozen executable environment changed")
    if plan["windows"] != windows().model_dump():
        raise ValueError("The registered holdout moved")
    return plan


def development_prices(plan: dict) -> pd.DataFrame:
    record = plan["source_snapshot"]
    source = feed(record["seconds"]) / "snapshots" / f"{record['sha256']}.parquet"
    if file_hash(source) != record["sha256"]:
        raise ValueError("Published immutable snapshot failed verification")
    end = utc(plan["windows"]["holdout_start"])
    # Only pre-holdout rows are returned to the study, even though the published file spans more dates.
    table = pq.read_table(source, filters=[("open_at", "<", end.to_pydatetime()),
                                           ("close_at", "<=", end.to_pydatetime())])
    candles = table.to_pandas()
    candles["open_at"] = candles.open_at.dt.as_unit("ns")
    candles["close_at"] = candles.close_at.dt.as_unit("ns")
    candles = candles.set_index("open_at").sort_index()
    if candles.empty or not candles.index.is_unique or not candles.index.max() < end:
        raise ValueError("No valid pre-holdout canonical price history")
    return candles
