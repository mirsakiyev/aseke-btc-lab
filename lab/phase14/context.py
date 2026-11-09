from contextlib import closing
import json
from pathlib import Path
import shutil
import time

import httpx
import numpy as np
import pandas as pd

from lab.config import ORIGIN, fingerprint
from lab.data import OHLCV, aggregate
from lab.phase9.data import candles
from lab.phase9.public import OUTPUT as PHASE9, kline, load_payload, send
from lab.phase10.worker import OUTPUT as PHASE10
from lab.phase13 import observation, protocol
from lab.phase13.registry import checked, freeze, sealed
from lab.storage import RUNS, file_hash, read_json, utc, utc_now, write_json, writer_lock


ROOT = RUNS / "phase14"
SCOPE = "FEATURE_CONTEXT_ONLY"
MINUTE, QUARTER, FOUR, DAILY = map(pd.Timedelta, ("1min", "15min", "4h", "1D"))
BUDGET = {"http_requests": 64, "wall_seconds": 1800, "attempts_per_page": 2, "spacing_seconds": .5,
          "request_timeout_seconds": 5, "max_response_bytes": 2 * 1024**2,
          "max_retained_bytes": 32 * 1024**2, "min_free_disk_bytes": 2 * 1024**3}
FIELDS = [*OHLCV, "close_at", "received_at", "evidence"]


def intervals(start, end, step):
    return pd.date_range(start, end, freq=step, inclusive="left")


def source_spec(spec):
    params = spec["params"]
    if (spec["route"] != "kline" or params.get("symbol") != "XBTUSDTM" or params.get("tradeType") != "FUTURES"
            or params.get("klineType") != "TRADE" or params.get("interval") not in {"1min", "15min", "4hour", "1day"}):
        raise ValueError("Feature exception is restricted to public KuCoin XBTUSDTM futures trade candles")
    expected = kline(spec["key"], "XBTUSDTM", "FUTURES", "TRADE", params["interval"],
                     utc(spec["start"]), utc(spec["end_exclusive"]))
    if spec != expected:
        raise ValueError("Feature context source or exact request changed")
    return params["interval"]


def parse(folder, spec, receipt, multiplier, as_of):
    source_spec(spec)
    stamp = utc(receipt["received_at"])
    if receipt["status"] != "RECEIVED" or stamp > as_of:
        raise ValueError("Feature response unavailable at the admission cutoff")
    frame = candles(load_payload(folder, receipt), spec, stamp, multiplier).set_index("open_at")
    if frame.index.has_duplicates:
        raise ValueError("Feature source contains duplicate candle timestamps")
    raw = folder / "raw" / (receipt["raw_sha256"] + ".json")
    return frame.assign(received_at=stamp, evidence=str(raw))[FIELDS]


def cache_sources():
    for plan in sorted((PHASE9 / "acquisition").glob("*/plan.json")):
        saved = read_json(plan)
        design = saved["design"]
        if design["kind"] != "current" or design["scope"] != "PROSPECTIVE_FEATURE_WARMUP_ONLY":
            continue
        if fingerprint(design) != saved["id"] or saved["id"] != plan.parent.name:
            raise ValueError("Existing public cache plan checksum changed")
        for path in sorted((plan.parent / "attempts").glob("*.json")):
            evidence = read_json(path)
            if evidence.get("status") == "RECEIVED" and evidence.get("request") in design["requests"]:
                yield plan.parent, evidence, path
    for path in sorted((PHASE10 / "cycles").glob("*.json")):
        record = read_json(path)
        if record.get("profile") not in ("day", "swing"):
            continue
        if fingerprint(record) != path.stem:
            raise ValueError("Existing monitor cycle checksum changed")
        for evidence in record["source"]:
            if evidence["status"] == "RECEIVED":
                yield PHASE10, evidence, path


def combine(pieces):
    if not pieces:
        return pd.DataFrame(columns=FIELDS, index=pd.DatetimeIndex([], tz="UTC"))
    # The monitor retains the first observation. Later responses are provenance, not revisions.
    joined = pd.concat(pieces).sort_values("received_at", kind="stable")
    return joined[~joined.index.duplicated(keep="first")].sort_index()


def resample(frame, target, step):
    if frame.empty:
        return combine([])
    if frame.index.has_duplicates or not frame.index.is_monotonic_increasing or (frame.index.as_unit("ns").asi8 % step.value).any():
        raise ValueError("Feature input is unordered, duplicated, or off its UTC grid")
    bars = aggregate(frame, target, step)
    grouped = frame.resample(target, origin=ORIGIN, label="left", closed="left")
    bars["received_at"] = grouped.received_at.max().reindex(bars.index)
    bars["evidence"] = grouped.evidence.agg(lambda values: sorted({path for value in values
                                           for path in (value if isinstance(value, list) else [value])})).reindex(bars.index)
    return bars[FIELDS]


def original_minutes(start, end):
    original = protocol.active()
    with closing(observation.db_open(original, readonly=True)) as db:
        rows = db.execute("SELECT source_at,source_close,received_at,raw_sha,body FROM item WHERE basis='trade' "
                          "AND source_at>=? AND source_at<? ORDER BY source_at", (start.isoformat(), end.isoformat()))
        values = []
        for at, close, receipt, digest, raw in rows:
            path = observation.directory(original) / "raw" / (digest + ".json")
            values.append({**{k: json.loads(raw)[k] for k in OHLCV}, "open_at": utc(at), "close_at": utc(close),
                           "received_at": utc(receipt), "evidence": str(path)})
    return pd.DataFrame(values).set_index("open_at") if values else combine([])


def page(interval, left, right, key):
    return kline(key, "XBTUSDTM", "FUTURES", "TRADE", interval, left, right)


def missing_pages(expected, present, step, interval):
    missing = expected.difference(present)
    requests = []
    for stamp in missing:
        if requests and stamp == requests[-1][1] and stamp - requests[-1][0] < 200 * step:
            requests[-1][1] += step
        else:
            requests.append([stamp, stamp + step])
    return [page(interval, left, right, f"missing-{interval}-{i}") for i, (left, right) in enumerate(requests)]


def pointer(name):
    path = ROOT / f"{name}.json"
    if path.exists():
        identity = read_json(path)["id"]
        return sealed(ROOT / name / (identity + ".json"), identity)
    return None


def save(name, body):
    body = json.loads(json.dumps(body, default=str, allow_nan=False))
    identity = fingerprint(body)
    path = ROOT / name / (identity + ".json")
    if not path.exists():
        write_json(path, {"id": identity, "body": body})
    write_json(ROOT / f"{name}.json", {"id": identity})
    return sealed(path, identity)


def freeze_plan(now=None):
    with writer_lock(ROOT / ".plan.lock"):
        if existing := pointer("plan"):
            return existing
        original, registry = protocol.active(), freeze()
        now = now if now is not None else utc_now()
        close = (now - pd.Timedelta(minutes=2)).floor(QUARTER)
        fine = original_minutes(close - 4*QUARTER, close)
        fine = fine.loc[fine.received_at <= now]
        complete = resample(fine, QUARTER, MINUTE)
        if complete.empty:
            raise ValueError("Wait for a completed Phase 13 trade quarter-hour for real aggregation parity")
        end = complete.close_at.iloc[-1]
        day_start = end.floor(FOUR) - 51*FOUR
        daily_end = end.floor(DAILY)
        daily_start = daily_end - 201*DAILY
        multiplier = original["body"]["instrument"]["contract"]["base_per_contract"]
        pieces, refs = {"15min": [], "4hour": []}, []
        for root, receipt, provenance in cache_sources():
            spec = receipt["request"]
            try:
                interval = source_spec(spec)
            except (ValueError, KeyError):
                continue
            if interval not in pieces or utc(spec["end_exclusive"]) <= (day_start if interval == "15min" else daily_start):
                continue
            if utc(spec["start"]) >= end or utc(receipt["received_at"]) > now:
                continue
            frame = parse(root, spec, receipt, multiplier, now)
            pieces[interval].append(frame)
            refs.append({"folder": str(root), "receipt": receipt, "provenance": str(provenance),
                         "provenance_sha256": file_hash(provenance)})
        quarter, four = (combine(pieces[interval]) for interval in ("15min", "4hour"))
        daily = resample(four, DAILY, FOUR)
        parity_open = complete.index[-1]
        daily_sample = daily_end - 3*DAILY
        requests = missing_pages(intervals(day_start, end, QUARTER), quarter.index.union(complete.index), QUARTER, "15min")
        requests += missing_pages(intervals(daily_start, daily_end, DAILY), daily.index, DAILY, "1day")
        if parity_open not in quarter.index:
            requests.append(page("15min", parity_open, parity_open+QUARTER, "native-quarter-parity"))
        requests.append(page("1day", daily_sample, daily_end, "native-daily-parity"))
        if len(requests)*BUDGET["attempts_per_page"] > BUDGET["http_requests"]:
            raise ValueError(f"Exact warm-up needs {len(requests)} pages before retries, above the frozen allowance")
        fine = fine.loc[(fine.index >= parity_open) & (fine.index < parity_open + QUARTER)]
        fine_rows = fine.reset_index(names="open_at").to_dict("records")
        for raw in fine.evidence.unique():
            checked(raw, Path(raw).stem)
        return save("plan", {"scope": SCOPE, "policy": "phase14-public-kucoin-trade-context-v1",
                    "original_protocol": original["id"], "registry": registry["id"], "frozen_at": now.isoformat(),
                    "deadline": (now + pd.Timedelta(seconds=BUDGET["wall_seconds"])).isoformat(), "budget": BUDGET,
                    "symbol": "XBTUSDTM", "market": "FUTURES", "basis": "TRADE", "base_per_contract": multiplier,
                    "quarter_range": [day_start.isoformat(), end.isoformat()], "daily_range": [daily_start.isoformat(), daily_end.isoformat()],
                    "expected_quarter": len(intervals(day_start, end, QUARTER)), "expected_daily": 201,
                    "cache_rows": {"quarter": int(quarter.index.isin(intervals(day_start, end, QUARTER)).sum()),
                                   "daily_from_four_hour": int(daily.index.isin(intervals(daily_start, daily_end, DAILY)).sum())},
                    "cache_sources": refs, "fine_parity_rows": fine_rows, "requests": requests,
                    "sealed_dates": registry["body"]["holdout"], "allowed_consumers": ["phase14.feature_seed", "phase14.seeded_features"],
                    "forbidden": ["fit", "training_labels", "evaluation_labels", "parameter_selection", "ranking", "historical_replay", "historical_PnL"]})


def acquire(plan, transport=None):
    body = plan["body"]
    budget = body["budget"]
    root = ROOT / "acquisition" / plan["id"]
    root.mkdir(parents=True, exist_ok=True)
    receipts = []
    with writer_lock(root / ".lock"), httpx.Client(timeout=budget["request_timeout_seconds"], transport=transport) as client:
        for spec in body["requests"]:
            source_spec(spec)
            for n in range(1, budget["attempts_per_page"]+1):
                path = root / f"{spec['key']}-{n}.json"
                if path.exists():
                    evidence = read_json(path)
                else:
                    if (utc_now() + pd.Timedelta(seconds=budget["request_timeout_seconds"]+budget["spacing_seconds"]) >= utc(body["deadline"])
                            or len(list(root.glob("*.json"))) >= budget["http_requests"]
                            or sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) + budget["max_response_bytes"] > budget["max_retained_bytes"]
                            or shutil.disk_usage(root).free < budget["min_free_disk_bytes"]):
                        raise ValueError("Frozen context acquisition resource limit reached")
                    write_json(path, {"status": "RESERVED", "request": spec, "reserved_at": utc_now().isoformat()})
                    time.sleep(budget["spacing_seconds"])
                    evidence = {**send(client, spec, budget["max_response_bytes"], root / "raw"), "request": spec}
                    write_json(path, evidence)
                if evidence["status"] == "RECEIVED":
                    receipts.append((root, evidence))
                    break
                if evidence["status"] == "ACCESS_RESTRICTED" or (evidence.get("resume_after") and utc_now() < utc(evidence["resume_after"])):
                    raise ValueError(f"Context request paused: {evidence['status']} {evidence.get('resume_after', '')}")
            else:
                raise ValueError(f"Frozen attempts consumed for {spec['key']}")
    return receipts


def decode(rows):
    if not rows:
        return combine([])
    frame = pd.DataFrame(rows)
    for field in ("open_at", "close_at", "received_at"):
        frame[field] = pd.to_datetime(frame[field], utc=True, format="mixed")
    return frame.set_index("open_at")[FIELDS]


def encode(frame):
    return frame.reset_index(names="open_at").to_dict("records")


def window(frame, start, end, step):
    selected = frame.loc[(frame.index >= start) & (frame.index < end)]
    if not selected.index.equals(intervals(start, end, step)) or not selected.close_at.eq(selected.index + step).all():
        raise ValueError(f"Incomplete {step} context for [{start}, {end})")
    if (selected.received_at < selected.close_at).any():
        raise ValueError("Feature receipt preceded a candle close")
    return selected


def parity(reference, derived, label):
    paired = reference.index.intersection(derived.index)
    if not len(paired):
        raise ValueError(f"No complete bars for {label} parity")
    delta = (reference.loc[paired, OHLCV] - derived.loc[paired, OHLCV]).abs()
    if not np.allclose(reference.loc[paired, OHLCV], derived.loc[paired, OHLCV], rtol=1e-12, atol=1e-8):
        raise ValueError(f"KuCoin aggregation mismatch for {label}: {delta.max().to_dict()}")
    return {"bars": len(paired), "first_open": paired[0].isoformat(), "last_open": paired[-1].isoformat(),
            "maximum_absolute_difference": delta.max().to_dict()}


def admit():
    plan = freeze_plan()
    with writer_lock(ROOT / ".seed.lock"):
        if existing := pointer("seed"):
            return existing
        responses = acquire(plan)
        now = utc_now()
        body = plan["body"]
        streams = {key: [] for key in ("15min", "4hour", "1day")}
        for item in body["cache_sources"]:
            checked(item["provenance"], item["provenance_sha256"])
            receipt = item["receipt"]
            spec = receipt["request"]
            streams[source_spec(spec)].append(parse(Path(item["folder"]), spec, receipt, body["base_per_contract"], now))
        for folder, receipt in responses:
            spec = receipt["request"]
            streams[source_spec(spec)].append(parse(folder, spec, receipt, body["base_per_contract"], now))
        quarter, four, native_daily = (combine(streams[key]) for key in ("15min", "4hour", "1day"))
        finer = resample(decode(body["fine_parity_rows"]), QUARTER, MINUTE)
        derived_daily = resample(four, DAILY, FOUR)
        proofs = {"15m_from_1m": parity(quarter, finer, "one-minute to quarter-hour"),
                  "4h_from_15m": parity(four, resample(quarter, FOUR, QUARTER), "quarter-hour to four-hour"),
                  "1d_from_4h": parity(native_daily, derived_daily, "four-hour to daily")}
        a, b = map(utc, body["quarter_range"])
        c, d = map(utc, body["daily_range"])
        quarter = window(combine([quarter, finer]), a, b, QUARTER)
        daily = window(combine([derived_daily, native_daily]), c, d, DAILY)
        holdout = body["sealed_dates"]
        left, right = utc(holdout["start"]), utc(holdout["end_exclusive"])
        overlap = {"15min": int(((quarter.index >= left) & (quarter.index < right)).sum()),
                   "1day": int(((daily.index >= left) & (daily.index < right)).sum())}
        return save("seed", {"scope": SCOPE, "plan": plan["id"], "registry": body["registry"],
                    "original_protocol": body["original_protocol"], "admitted_at": now.isoformat(),
                    "cutoff": b.isoformat(), "bars": {"quarter": encode(quarter), "daily": encode(daily)},
                    "parity": proofs, "calendar_overlapping_bars": overlap, "protected_holdout_artifacts_accessed": False,
                    "warmup_scored": False, "warmup_labels": 0, "warmup_account_replays": 0,
                    "new_http_requests": len(list((ROOT / "acquisition" / plan["id"]).glob("*.json"))),
                    "http_receipts": [receipt for _, receipt in responses]})
