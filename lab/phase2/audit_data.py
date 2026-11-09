from pathlib import Path

import httpx
import numpy as np
import pandas as pd

from lab.data import OHLCV, SOURCE, aggregate, fetch, normalize
from lab.storage import immutable_bytes, utc_now

TOLERANCE = {"open": 0.01, "high": 0.01, "low": 0.01, "close": 0.01, "volume": 1e-6}


def gaps(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, seconds: int) -> list[dict]:
    step = pd.Timedelta(seconds=seconds)
    expected = pd.date_range(start, end, freq=step, inclusive="left")
    missing = expected.difference(frame.index)
    if not len(missing):
        return []
    starts = np.r_[0, np.flatnonzero(np.diff(missing.asi8) != step.value) + 1]
    ends = np.r_[starts[1:], len(missing)]
    cluster, last = 0, start - pd.Timedelta("2D")
    result = []
    for first, stop in zip(starts, ends):
        left, right = missing[first], missing[stop - 1] + step
        if left - last > pd.Timedelta("24h"):
            cluster += 1
        result.append({"start": left, "end": right, "bars": int(stop - first),
                       "duration_minutes": int((right - left) / pd.Timedelta("1min")), "cluster": cluster})
        last = right
    return result


def integrity(frame: pd.DataFrame, seconds: int) -> dict:
    step = pd.Timedelta(seconds=seconds)
    checks = {
        "duplicate_open_times": int(frame.index.duplicated().sum()),
        "off_grid_opens": int((frame.index.floor(step) != frame.index).sum()),
        "incorrect_closes": int((frame.close_at != frame.index + step).sum()),
        "close_after_receipt": int((frame.close_at > frame.ingested_at).sum()),
        "nonfinite_rows": int((~np.isfinite(frame[OHLCV]).all(axis=1)).sum()),
        "invalid_price_or_volume": int(((frame[OHLCV[:4]] <= 0).any(axis=1) | (frame.volume < 0)).sum()),
        "invalid_ohlc": int(((frame.low > frame[["open", "high", "close"]].min(axis=1)) |
                             (frame.high < frame[["open", "close"]].max(axis=1))).sum()),
        "unexpected_provenance": int(((frame.source_id != SOURCE) | (frame.resolution_seconds != seconds)).sum()),
        "zero_volume_bars": int(frame.volume.eq(0).sum()),
    }
    assert frame.index.tz is not None and frame.index.is_monotonic_increasing
    assert not any(value for name, value in checks.items() if name != "zero_volume_bars")
    return checks


def reconciliation(hourly: pd.DataFrame, fine: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    derived = aggregate(fine, "1h", pd.Timedelta("5min"))
    common = hourly.index.intersection(derived.index)
    native = hourly.loc[common, OHLCV]
    built = derived.loc[common, OHLCV]
    absolute = (native - built).abs()
    relative = absolute.div(native.abs().where(native != 0))
    exceeds = absolute.gt(pd.Series(TOLERANCE))
    ohlc = exceeds[OHLCV[:4]].any(axis=1)
    volume_only = exceeds.volume & ~ohlc
    details = pd.concat({"hourly": native, "five_minute_aggregate": built,
                         "absolute_difference": absolute, "relative_to_native": relative}, axis=1)
    details.columns = ["_".join(key) for key in details.columns]
    details["class"] = np.where(ohlc, "ohlc", np.where(volume_only, "volume_only", "within_tolerance"))
    details = details.loc[ohlc | volume_only]
    magnitudes = {}
    for name in OHLCV:
        selection = exceeds[name]
        rel = relative.loc[selection, name].dropna()
        magnitudes[name] = {
            "affected_hours": int(selection.sum()),
            "max_absolute": float(absolute[name].max()) if len(common) else None,
            "median_absolute_on_affected": float(absolute.loc[selection, name].median()) if selection.any() else None,
            "max_relative_fraction": float(rel.max()) if len(rel) else None,
            "median_relative_fraction_on_affected": float(rel.median()) if len(rel) else None,
            "relative_undefined_on_affected": int(relative.loc[selection, name].isna().sum()),
        }
    return {
        "overlapping_complete_hours": len(common), "ohlc_hours": int(ohlc.sum()),
        "volume_only_hours": int(volume_only.sum()), "different_hours": len(details),
        "tolerances_absolute": TOLERANCE, "magnitudes": magnitudes,
        "relative_denominator": "Absolute native hourly value",
        "representative_hours": [str(ts) for ts in details.loc[ohlc.reindex(details.index)].index[:3]]
        + [str(ts) for ts in details.loc[volume_only.reindex(details.index)]
           .nlargest(3, "absolute_difference_volume").index],
    }, details


def retries(frames: dict[int, pd.DataFrame], missing: dict[int, list[dict]], differences: pd.DataFrame,
            directory: Path) -> list[dict]:
    """Re-observe a small fixed set. Publish evidence and proposals outside both canonical stores."""
    probes = set()
    for seconds, intervals in missing.items():
        step = pd.Timedelta(seconds=seconds)
        for item in sorted(intervals, key=lambda gap: -gap["bars"])[:2]:
            probes.add((seconds, item["start"] - step, item["end"] + step))
    anomaly = differences[differences["class"].eq("ohlc")].head(4)
    volume = differences[differences["class"].eq("volume_only")].nlargest(1, "absolute_difference_volume")
    hours = list(anomaly.index.union(volume.index))
    groups = []
    for hour in hours:
        if groups and hour == groups[-1][1]:
            groups[-1][1] += pd.Timedelta("1h")
        else:
            groups.append([hour, hour + pd.Timedelta("1h")])
    for left, right in groups:
        for seconds in frames:
            probes.add((seconds, left, right))
    reports, proposals = [], []
    with httpx.Client(timeout=12) as client:
        for seconds, start, end in sorted(probes):
            step = pd.Timedelta(seconds=seconds)
            if (end - start) // step >= 300:
                raise ValueError("Audit probe exceeded the request budget")
            row = {"seconds": seconds, "start": start, "end": end}
            try:
                response = fetch(client, start, end, seconds)
                received = utc_now()
                raw = immutable_bytes(directory / "raw", response.content, "json")
                row.update(raw_sha256=raw, received_at=received, request_url=str(response.request.url))
                fresh = normalize(response.json(), start, end, seconds, received, raw).set_index("open_at")
                original = frames[seconds].loc[start:end - step]
                common = original.index.intersection(fresh.index)
                changed = common[(original.loc[common, OHLCV] != fresh.loc[common, OHLCV]).any(axis=1)]
                new = fresh.index.difference(original.index)
                row.update(observed=len(fresh), newly_observed=len(new), changed=len(changed),
                           original_missing_still_missing=int((end - start) // step - len(
                               original.index.union(fresh.index))),
                           originally_observed_absent_on_retry=len(original.index.difference(fresh.index)))
                if len(new.union(changed)):
                    proposed = fresh.loc[new.union(changed)].copy()
                    proposed["proposal"] = np.where(proposed.index.isin(new), "recovered", "revision")
                    proposals.append(proposed)
            except (httpx.HTTPError, ValueError) as exc:
                row["error"] = str(exc)
            reports.append(row)
    if proposals:
        pd.concat(proposals).reset_index().to_parquet(directory / "proposed-revisions.parquet", index=False)
    return reports
