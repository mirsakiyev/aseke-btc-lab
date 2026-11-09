from dataclasses import dataclass

import numpy as np
import pandas as pd

from lab.phase10.partition import partition_features
from lab.phase13.registry import checked, freeze
from lab.phase14.context import (DAILY, FOUR, MINUTE, QUARTER, SCOPE, combine, decode, pointer,
                                 resample, save, window)
from lab.storage import utc, utc_now


@dataclass(frozen=True)
class FeatureBars:
    frames: dict

    def bars(self, resolutions):
        return {resolution: self.frames[pd.Timedelta(resolution)].set_index("close_at") for resolution in resolutions}


def inputs(seed, minutes, as_of):
    body = seed["body"]
    if body["scope"] != SCOPE or utc(body["admitted_at"]) > as_of:
        raise ValueError("Feature-only seed was not admitted at this as-of time")
    cutoff = utc(body["cutoff"])
    quarter, daily = (decode(body["bars"][key]) for key in ("quarter", "daily"))
    for frame, step, end in ((quarter, QUARTER, cutoff), (daily, DAILY, cutoff.floor(DAILY))):
        window(frame, frame.index[0], end, step)
        if (frame.received_at > as_of).any():
            raise ValueError("Feature context contains an observation from after the as-of time")
    if not minutes.empty:
        if minutes.index[0] < cutoff:
            raise ValueError("New feature observations cannot replace an admitted seed row")
        minutes = minutes.loc[(minutes.close_at <= as_of) & (minutes.received_at <= as_of)]
        quarter = combine([quarter, resample(minutes, QUARTER, MINUTE)])
    return {QUARTER: quarter, pd.Timedelta("1h"): resample(quarter, pd.Timedelta("1h"), QUARTER),
            FOUR: resample(quarter, FOUR, QUARTER), DAILY: combine([daily, resample(quarter, DAILY, QUARTER).loc[
                lambda frame: frame.index >= cutoff.floor(DAILY)]])}


def seeded_features(item, frames, admitted_at):
    profile = item["profile_rules"]
    features, columns = partition_features(FeatureBars(frames), profile)
    if columns != item["feature_columns"]:
        raise ValueError("Admitted context does not match the pinned model schema")
    basis = frames[pd.Timedelta(profile["decision"])].set_index("close_at")
    # EMA state and confirmed pivots depend on prior bars in the decision segment.
    last = basis.received_at.cummax().reindex(features.index)
    for spec in profile["context"]:
        step = pd.Timedelta(spec["frame"])
        context = frames[step].set_index("close_at")
        available = context.received_at.cummax().reindex(features.index, method="ffill", tolerance=step-pd.Timedelta("1ns"))
        last = last.where(available.isna() | last.ge(available), available)
    latest_receipt = last.clip(lower=admitted_at)
    return features, latest_receipt


def readiness(seed=None, now=None, minutes=None):
    seed = seed or pointer("seed")
    if not seed:
        return {"state": "WAITING_FOR_FEATURE_SEED"}
    registry = freeze()
    if seed["body"]["registry"] != registry["id"]:
        raise ValueError("Seed and frozen candidate registry differ")
    now = now if now is not None else utc_now()
    admitted = utc(seed["body"]["admitted_at"])
    frames = inputs(seed, minutes if minutes is not None else combine([]), now)
    result = {}
    for profile in ("day", "swing"):
        item = registry["body"]["candidates"][f"{profile}/estimator"]
        receipt = item["fixed_artifact"]
        checked(receipt["path"], receipt["sha256"])
        features, available = seeded_features(item, frames, admitted)
        last = features.iloc[-1]
        ready = bool(last.ready) and available.iloc[-1] <= now
        model_values = last[item["feature_columns"]]
        result[profile] = {"state": "READY" if ready else "MISSING_OR_UNINITIALIZED_FEATURES",
                           "cutoff": features.index[-1].isoformat(), "available_as_of": available.iloc[-1].isoformat(),
                           "closed_decision_bars": len(features), "required_schema": item["feature_columns"],
                           "nonfinite_columns": model_values.index[~np.isfinite(model_values.to_numpy(dtype=float))].tolist(),
                           "artifact": receipt["version"], "sha256": receipt["sha256"],
                           "model_scores": 0, "setups_evaluated": 0}
    state = "BOTH_FEATURES_READY" if all(x["state"] == "READY" for x in result.values()) else "FEATURES_NOT_READY"
    return {"state": state, "seed": seed["id"], "profiles": result}


def freeze_readiness(seed):
    if existing := pointer("readiness"):
        if existing["body"]["seed"] != seed["id"]:
            raise ValueError("Readiness is already frozen for another seed")
        return existing
    report = readiness(seed)
    return save("readiness", {**report, "checked_at": utc_now().isoformat()})
