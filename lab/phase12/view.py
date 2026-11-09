from pathlib import Path
from uuid import uuid4

import pandas as pd

from lab.config import fingerprint
from lab.phase11.archive import folder, overview
from lab.phase11.plan import MINUTE, ROOT as PHASE11, freeze_plan
from lab.phase11.qualify import matching_funding, read_values, records, rows_by_period
from lab.phase11.resources import freeze as freeze_resources
from lab.storage import RUNS, file_hash, read_json, utc, utc_now, write_json, writer_lock


ROOT = RUNS / "phase12"
POLICY = "phase12-first-complete-utc-day-after-final-mark-gap-v1"


def gaps(index, start, end):
    if str(index.tz) != "UTC" or not index.is_monotonic_increasing or not index.is_unique:
        raise ValueError("Unexpected unordered, repeated, or non-UTC price timestamps")
    expected = pd.date_range(start, end, freq=MINUTE, inclusive="left")
    if len(index.difference(expected)):
        raise ValueError("Out-of-partition or off-grid source price timestamp")
    return expected.difference(index)


def ranges(missing):
    result = []
    for at in missing:
        if result and utc(result[-1]["last"]) + MINUTE == at:
            result[-1]["last"] = at.isoformat()
            result[-1]["rows"] += 1
        else:
            result.append({"first": at.isoformat(), "last": at.isoformat(), "rows": 1})
    return result


def cutoff(mark_gaps):
    if not mark_gaps:
        raise ValueError("The amended view requires a documented mark gap")
    return max(utc(item["last"]) for item in mark_gaps).normalize() + pd.Timedelta("1D")


def original_record():
    # Read the original failure through its append-only admission identity.
    attempts = sorted((PHASE11 / "admission-attempts").glob("*.json"))
    for attempt_path in attempts:
        attempt = read_json(attempt_path)
        old_path = PHASE11 / "qualification" / f'{attempt["qualification"]}.json'
        original = read_json(old_path)
        if (attempt["actual_estimator_fits"] == 0 and original["id"] == attempt["qualification"]
                and fingerprint(original["body"]) == original["id"] and not original["body"]["eligible"]
                and original["body"]["folds"]["y2025"]["coverage_trade_rows"] -
                original["body"]["folds"]["y2025"]["coverage_mark_rows"] == 53):
            return original, {"qualification": original["id"], "qualification_sha256": file_hash(old_path),
                              "admission_sha256": file_hash(attempt_path), "admission": str(attempt_path)}
    raise ValueError("Original Phase 11 rejection and gap evidence unavailable")


def slice_boundary(asset, start, end, basis):
    transform = {"op": "select_existing_open_times", "start_inclusive": start.isoformat(), "end_exclusive": end.isoformat(),
                 "source": asset["file"], "source_normalized_sha256": asset["normalized_sha256"]}
    dest = ROOT / "view-partitions" / f'{basis}-{fingerprint(transform)}.parquet'
    receipt = dest.with_suffix(".json")
    frame = read_values(asset, None)
    selected = frame.loc[(frame.open_at >= start) & (frame.open_at < end)]
    if selected.empty:
        raise ValueError("The cutoff intersects no cached minute rows")
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not receipt.exists():
        tmp = dest.with_name(f".{uuid4().hex}.tmp")
        try:
            selected.to_parquet(tmp, index=False, compression="zstd", compression_level=6)
            tmp.replace(dest)
            write_json(receipt, {**transform, "sha256": file_hash(dest), "rows": len(selected)})
        finally:
            tmp.unlink(missing_ok=True)
    proof = read_json(receipt)
    if (any(proof[key] != value for key, value in transform.items()) or file_hash(dest) != proof["sha256"]
            or proof["rows"] != len(selected) or not pd.read_parquet(dest).equals(selected.reset_index(drop=True))):
        raise ValueError("Boundary view changed or does not match the source slice")
    return str(dest), proof["sha256"], proof


def freeze():
    plan, resource = freeze_plan(), freeze_resources()
    original, lineage = original_record()
    policy = {"version": POLICY, "rule": "First complete UTC day after the final documented mark gap",
              "selection_inputs": "Cached price presence and original funding settlement evidence only"}
    if original["body"]["plan"] != plan["id"] or original["body"]["amendment"] != resource["id"]:
        raise ValueError("Phase 11 scope and resource identities changed")
    assets = records(plan)
    for key, prior in original["body"]["inputs"].items():
        current = assets.get(key)
        if (not current or current["raw_sha256"] != prior["raw_sha256"]
                or current.get("repairs", []) != prior["repaired_with"]):
            raise ValueError(f"Phase 11 source lineage changed for {key}")
    right = utc(plan["body"]["holdout"]["start"])
    earliest = utc(original["body"]["first_common_archive_period"])
    coverage, missing = [], {"trade": [], "mark": []}
    for (a, b), keys in rows_by_period(plan).items():
        left, end = utc(a), utc(b)
        if left < earliest or end > right:
            continue
        item = {"start": a, "end": b}
        for kind, basis in (("klines", "trade"), ("markPriceKlines", "mark")):
            asset = assets.get(keys[kind])
            if not asset:
                raise ValueError(f"Missing cached {basis} partition for {a}")
            index = pd.DatetimeIndex(read_values(asset, ["open_at"]).open_at)
            missing[basis].extend(ranges(gaps(index, left, end)))
            item[basis] = asset
        coverage.append(item)
    left = cutoff(missing["mark"])
    if left >= right:
        raise ValueError("No admissible complete day remains before the holdout")
    blocking = {basis: [span for span in values if utc(span["last"]) >= left] for basis, values in missing.items()}
    if any(blocking.values()):
        write_json(ROOT / "blocked-view.json", {"policy": policy, "start": left.isoformat(), "missing_in_view": blocking})
        raise ValueError("Cached price gaps remain in the required amended interval")
    funding = [row for row in assets.values() if row["spec"]["kind"].startswith("funding")]
    funding.sort(key=lambda row: row["spec"]["start"])
    all_funding = pd.concat([read_values(row, ["settlement_at", "rate", "interval_minutes"]) for row in funding], ignore_index=True)
    admitted_funding, settlement_evidence = matching_funding(all_funding, left, right)
    actual_rows = int((right - left) // MINUTE)
    ceiling = resource["body"]["unique_observation_allowance"]
    if actual_rows > min(ceiling["trade"], ceiling["mark"]) or len(admitted_funding) > ceiling["funding"]:
        raise ValueError("Amended view exceeds the original Phase 11 resource allowance")
    archive = overview(plan)
    clock = folder(plan) / "acquisition-clock.json"
    proof = {"plan": plan["id"], "requests_used": archive["requests_used"],
             "requests_remaining": archive["requests_remaining"], "clock_sha256": file_hash(clock)}
    before = original["body"]["folds"]["y2025"]
    chunks, boundary_parts = [], []
    with writer_lock(ROOT / ".view.lock"):
        for item in coverage:
            start, end = utc(item["start"]), utc(item["end"])
            if end <= left:
                continue
            row = {}
            for basis in ("trade", "mark"):
                asset = item[basis]
                if start < left:
                    path, digest, transform = slice_boundary(asset, left, end, basis)
                    boundary_parts.append({"basis": basis, **transform})
                else:
                    path, digest = asset["file"], asset["normalized_sha256"]
                row.update({basis: path, f"{basis}_sha256": digest,
                            f"{basis}_source_sha256": asset["raw_sha256"]})
            chunks.append(row)
        fold_evidence = {}
        for row in plan["body"]["windows"]:
            start, outer, end = (utc(row[key]) for key in ("inner_start", "outer_start", "outer_end"))
            fold_evidence[row["id"]] = {"eligible": left < start < outer < end <= right,
                "admitted_history_start": left.isoformat(), "inner_start": start.isoformat(),
                "outer_start": outer.isoformat(), "outer_end": end.isoformat(),
                "inner_trade_and_mark_minutes_each": int((outer-start)//MINUTE),
                "outer_trade_and_mark_minutes_each": int((end-outer)//MINUTE),
                "required_funding_observed": int(admitted_funding.settlement_at.lt(end).sum()),
                "missing_trade_or_mark_minutes": 0}
        body = {"coverage_policy": {**policy, "cutoff": left.isoformat(), "documented_mark_gaps": missing["mark"]},
                "original": lineage, "plan": plan["id"], "amendment": resource["id"], "source": original["body"]["source"],
                "inputs": original["body"]["inputs"], "acquisition": proof, "chunks": chunks, "boundary_slices": boundary_parts,
                "source_start_inclusive": left.isoformat(), "source_end_exclusive": right.isoformat(),
                "holdout": plan["body"]["holdout"], "rows": {"trade": actual_rows, "mark": actual_rows},
                "funding": settlement_evidence,
                "funding_chunks": [{key: row[key] for key in ("spec", "file", "normalized_sha256", "raw_sha256")}
                                   for row in funding if utc(row["spec"]["end"]) > left],
                "excluded": {"interval": [earliest.isoformat(), left.isoformat()],
                             "trade": before["coverage_trade_rows"] - actual_rows,
                             "mark": before["coverage_mark_rows"] - actual_rows,
                             "funding": len(all_funding) - len(admitted_funding),
                             "regimes": "January through mid-December 2020 are absent from amended inputs, "
                                        "including the March 2020 sell-off and the ensuing recovery and autumn rally."},
                "folds": fold_evidence, "eligible": [key for key, row in fold_evidence.items() if row["eligible"]],
                "scope": "HYPOTHETICAL_REPLAY", "use": "RESEARCH_ONLY"}
        identity = fingerprint(body)
        dest = ROOT / "views" / f"{identity}.json"
        if not dest.exists():
            write_json(dest, {"id": identity, "body": body, "preregistered_at": utc_now().isoformat()})
        write_json(ROOT / "view.json", {"id": identity})
    return read_json(dest)
