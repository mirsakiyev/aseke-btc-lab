from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from lab.config import fingerprint
from lab.phase11.archive import folder, manifest_path
from lab.phase11.plan import MINUTE, ROOT, freeze_plan
from lab.phase11.resources import freeze as freeze_resources
from lab.storage import file_hash, read_json, utc, utc_now, write_json


FUNDING_OFFSET_LIMIT = pd.Timedelta("5s")


def records(plan):
    root = folder(plan)
    result = {}
    for spec in plan["body"]["sources"]:
        path = manifest_path(root, spec)
        if path.exists():
            item = read_json(path)
            if item["status"] == "VERIFIED":
                overlay = root / "overlays" / f'{spec["key"]}.json'
                if overlay.exists():
                    repaired = read_json(overlay)
                    if repaired["original_normalized_sha256"] != item["normalized_sha256"]:
                        raise ValueError("Repair lineage disagrees with archive observation")
                    item = repaired
                result[spec["key"]] = item
    return result


def read_values(row, fields):
    path = Path(row["file"])
    if file_hash(path) != row["normalized_sha256"]:
        raise ValueError(f"Source snapshot changed: {path.name}")
    return pd.read_parquet(path, columns=fields)


def rows_by_period(plan):
    price = {}
    for spec in plan["body"]["sources"]:
        if spec["kind"] not in ("klines", "markPriceKlines"):
            continue
        price.setdefault((spec["start"], spec["end"]), {})[spec["kind"]] = spec["key"]
    return price


def matching_funding(frame, start, end):
    """Verify the published dated cadence, while retaining every original event timestamp."""
    frame = frame[(frame.settlement_at >= start) & (frame.settlement_at < end)].copy()
    if frame.empty or not frame.settlement_at.is_monotonic_increasing or not frame.settlement_at.is_unique:
        raise ValueError("Settlements missing, unordered, or duplicated")
    clock = pd.DatetimeIndex(frame.settlement_at)
    # This clock is used only to test the schedule. Replay receives clock, never nominal.
    nominal = clock.round("min")
    offsets = abs(clock - nominal)
    if (offsets > FUNDING_OFFSET_LIMIT).any() or not nominal.is_unique:
        raise ValueError("Settlement does not match one nominal slot within the declared 5-second tolerance")
    intervals = frame.interval_minutes.astype(int).replace(0, np.nan).ffill()
    if intervals.isna().any() or (intervals <= 0).any() or (intervals > 24*60).any():
        raise ValueError("Published funding interval unavailable")
    intervals = intervals.astype(int).to_numpy()
    steps = (nominal[1:] - nominal[:-1]).as_unit("ns").asi8 // MINUTE.value
    # The field describes the schedule in force at the event. On a change either adjacent
    # published interval is admissible if the observed spacing matches exactly.
    bad = np.flatnonzero((steps != intervals[:-1]) & (steps != intervals[1:]))
    if len(bad):
        pairs = [(clock[i].isoformat(), clock[i+1].isoformat(), int(steps[i])) for i in bad[:8]]
        raise ValueError(f"{len(bad)} funding spacing gaps; first (settlement, next, minutes): {pairs}")
    if nominal[0] - pd.Timedelta(minutes=int(intervals[0])) >= start:
        raise ValueError("First expected funding slot missing")
    if nominal[-1] + pd.Timedelta(minutes=int(intervals[-1])) < end:
        raise ValueError("Last expected funding slot missing")
    return frame, {"rows": len(frame), "first": clock[0].isoformat(), "last": clock[-1].isoformat(),
                   "zero_rates": int(frame.rate.eq(0).sum()), "off_grid_events": int((clock != nominal).sum()),
                   "maximum_nominal_offset_ms": float(offsets.max().total_seconds() * 1000),
                   "published_intervals_minutes": sorted(set(map(int, intervals))),
                   "cutoff_api_uses_adjacent_published_schedule": bool(frame.interval_minutes.eq(0).any()),
                   "ordering": "Original UTC timestamp. Grid settlements before opening exits for carried exposure; "
                               "off-grid after opening actions and before modeled close resolution, priced at observed mark open."}


def price_coverage(pair):
    trade = pd.DatetimeIndex(read_values(pair["klines"], ["open_at"]).open_at)
    mark = pd.DatetimeIndex(read_values(pair["markPriceKlines"], ["open_at"]).open_at)
    return trade, mark


def qualify(plan=None, amendment=None):
    plan = plan or freeze_plan()
    amendment = amendment or freeze_resources()
    guard = amendment["body"]
    if guard["work_plan"] != plan["id"] or fingerprint(guard) != amendment["id"]:
        raise ValueError("Resource amendment is not linked to this frozen plan")
    found = records(plan)
    prices = rows_by_period(plan)
    first_funding = min((utc(row["spec"]["start"]) for row in found.values() if row["spec"]["kind"] == "fundingRate"),
                        default=None)
    first_pair = next((utc(period[0]) for period, keys in prices.items()
                      if first_funding is not None and utc(period[0]) >= first_funding and
                      all(key in found for key in keys.values()) and
                      found.get(f"{utc(period[0]):%Y-%m}-fundingRate")), None)
    paired, price_checks, previous = {}, {}, None
    if first_pair is not None:
        for (a, b), keys in prices.items():
            left, right = utc(a), utc(b)
            if left < first_pair:
                continue
            expected = int((right-left)//MINUTE)
            result = {"expected": expected, "missing_trade": expected, "missing_mark": expected,
                      "missing_trade_on_mark_grid": None, "missing_mark_on_trade_grid": None}
            if all(key in found for key in keys.values()):
                pair = {basis: found[key] for basis, key in keys.items()}
                trade, mark = price_coverage(pair)
                result.update(missing_trade=expected-len(trade), missing_mark=expected-len(mark),
                              missing_trade_on_mark_grid=len(mark.difference(trade)),
                              missing_mark_on_trade_grid=len(trade.difference(mark)),
                              missing_first_trade=next((at.isoformat() for at in pd.date_range(left, right, freq=MINUTE,
                                  inclusive="left").difference(trade)), None) if len(trade) != expected else None,
                              missing_first_mark=next((at.isoformat() for at in pd.date_range(left, right, freq=MINUTE,
                                  inclusive="left").difference(mark)), None) if len(mark) != expected else None,
                              first_trade=trade[0].isoformat(), first_mark=mark[0].isoformat(),
                              last_trade=trade[-1].isoformat(), last_mark=mark[-1].isoformat(),
                              trade_rows=len(trade), mark_rows=len(mark))
                if previous and trade[0] <= previous:
                    raise ValueError("Unexpected overlap between official archive partitions")
                if len(mark.difference(trade)):
                    result["unpaired_mark_evidence"] = "Phase 10 pairing requires mark rows to use the trade grid"
                previous = trade[-1]
                paired[(a, b)] = pair
            price_checks[(a, b)] = result
    fund = [row for row in found.values() if row["spec"]["kind"].startswith("funding")]
    fund.sort(key=lambda row: row["spec"]["start"])
    funding_frames = {row["spec"]["key"]: read_values(row, ["settlement_at", "rate", "interval_minutes"]) for row in fund}
    scopes, eligible, views = {}, [], {}
    for fold in plan["body"]["windows"]:
        inner, end = utc(fold["inner_start"]), utc(fold["outer_end"])
        missing_files, errors, evaluation_gaps = [], [], Counter()
        relevant = [(a, b) for a, b in prices if first_pair is not None and utc(a) >= first_pair and utc(b) <= end]
        for period in relevant:
            a, b = period
            summary = price_checks[period]
            if period not in paired:
                missing_files.extend(key for key in prices[period].values() if key not in found)
            if utc(a) >= inner:
                evaluation_gaps["trade"] += summary["missing_trade"]
                evaluation_gaps["mark"] += summary["missing_mark"]
            if summary["missing_mark_on_trade_grid"]:
                errors.append(f"{a}: {summary['missing_mark_on_trade_grid']} trade observations lack paired mark")
            if summary["missing_trade_on_mark_grid"]:
                errors.append(f"{a}: {summary['missing_trade_on_mark_grid']} mark observations lack paired trade")
        checks = {"eligible": False, "source_prefix_start": first_pair.isoformat() if first_pair is not None else None,
                  "evaluation_missing_minutes": dict(evaluation_gaps), "missing_price_objects": missing_files,
                  "issues": errors}
        views[fold["id"]] = checks
        if not relevant or first_pair >= inner or utc(relevant[-1][1]) != end:
            errors.append("Frozen training/inner/outer envelope not present")
            continue
        fund_specs = [item for item in plan["body"]["sources"] if item["kind"].startswith("funding")
                      and utc(item["start"]) >= first_pair and utc(item["end"]) <= end]
        funding_missing = [item["key"] for item in fund_specs if item["key"] not in funding_frames]
        if funding_missing:
            checks["missing_funding_objects"] = funding_missing
        if missing_files or funding_missing:
            continue
        first_trade = utc(price_checks[relevant[0]]["first_trade"])
        try:
            frame, observation = matching_funding(pd.concat([funding_frames[item["key"]] for item in fund_specs],
                                                          ignore_index=True), first_trade, end)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        checks.update(funding=observation, first_actual_trade=first_trade.isoformat(),
                      coverage_trade_rows=sum(price_checks[period]["trade_rows"] for period in relevant),
                      coverage_mark_rows=sum(price_checks[period]["mark_rows"] for period in relevant),
                      known_training_prefix_gaps=[{"start": a, **price_checks[(a,b)]} for a,b in relevant
                                                  if utc(a) < inner and (price_checks[(a,b)]["missing_trade"] or
                                                                         price_checks[(a,b)]["missing_mark"])])
        if (checks["coverage_trade_rows"] > guard["unique_observation_allowance"]["trade"]
                or checks["coverage_mark_rows"] > guard["unique_observation_allowance"]["mark"]
                or len(frame) > guard["unique_observation_allowance"]["funding"]):
            raise ValueError("Qualified asset exceeds its frozen amended stream allowance")
        if any(evaluation_gaps.values()) or errors:
            continue
        checks["eligible"] = True
        eligible.append(fold["id"])
        scopes[fold["id"]] = (relevant, frame)
    raw_hashes = {key: {"raw_sha256": value["raw_sha256"], "repaired_with": value.get("repairs", [])}
                  for key, value in found.items()}
    body = {"plan": plan["id"], "amendment": amendment["id"], "inputs": raw_hashes, "folds": views, "eligible": eligible,
            "state": "PRICE_AND_FUNDING_QUALIFIED" if eligible else "WAITING_FOR_QUALIFIED_FUTURES_BUNDLE",
            "first_common_archive_period": first_pair.isoformat() if first_pair is not None else None,
            "scope": "HYPOTHETICAL_REPLAY", "use": "RESEARCH_ONLY", "source": plan["body"]["source"],
            "pre_first_paired_input": "Frozen training start retained. Any preceding period without the paired streams "
                                      "has no fabricated observations or labels.",
            "price_periods": {a: {"end_exclusive": b, **item} for (a,b), item in price_checks.items()}}
    if eligible:
        relevant, settlements = scopes[eligible[-1]]
        chunks = []
        for period in relevant:
            pair = paired[period]
            chunks.append({key: pair[basis][field] for basis, name in (("klines", "trade"), ("markPriceKlines", "mark"))
                           for key, field in ((name, "file"), (f"{name}_sha256", "normalized_sha256"))})
        body.update(chunks=chunks, source_end_exclusive=relevant[-1][1],
                    funding_chunks=[{key: item[key] for key in ("spec", "file", "normalized_sha256", "raw_sha256")}
                                    for item in fund if utc(item["spec"]["end"]) <= utc(relevant[-1][1])
                                    and utc(item["spec"]["start"]) >= first_pair],
                    funding_start=settlements.settlement_at.iloc[0].isoformat())
    identifier = fingerprint(body)
    path = ROOT / "qualification" / f"{identifier}.json"
    if not path.exists():
        write_json(path, {"id": identifier, "body": body, "checked_at": utc_now().isoformat()})
    write_json(ROOT / "latest-qualification.json", {"id": identifier})
    return read_json(path)
