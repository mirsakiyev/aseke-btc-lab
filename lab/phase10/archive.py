from pathlib import Path
import csv
from hashlib import sha256
from io import BytesIO, TextIOWrapper
from itertools import chain
import math
import time
from zipfile import ZipFile

import httpx
import pandas as pd

from lab.phase9.reference import parent_design
from lab.storage import RUNS, immutable_bytes, read_json, utc, utc_now, write_json, writer_lock


OUTPUT = RUNS / "phase10"
TERMS = "https://raw.githubusercontent.com/binance/binance-public-data/master/TERMS_AND_CONDITIONS.md"
BASE = "https://data.binance.vision/data/futures/um"
BUDGET = {"requests": 19, "elapsed_seconds": 140, "total_bytes": 32 * 1024**2, "response_bytes": 8 * 1024**2}
FIELDS = {
    "klines": ("open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume",
               "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"),
    "markPriceKlines": ("open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume",
                       "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"),
    "fundingRate": ("calc_time", "funding_interval_hours", "last_funding_rate"),
}


def calendar():
    design = parent_design()["design"]
    settings = design["settings"]
    step = pd.Timedelta("1min")
    folds = []
    for fold in design["folds"]:
        inner, outer, end = (utc(fold[key]) for key in ("inner_start", "outer_start", "outer_end"))
        folds.append({**fold, "training_eligible_from": settings["evaluation"]["train_start"],
                      "minimum_minutes_per_price_basis_before_training_prefix": (end - inner) // step,
                      "inner_train_labels_mature_strictly_before": inner.isoformat(),
                      "outer_train_labels_mature_strictly_before": outer.isoformat(),
                      "outer_labels_mature_strictly_before": end.isoformat()})
    return {"parent": parent_design()["id"], "holdout": design["holdout"], "folds": folds,
            "max_joined_rows_per_basis": settings["budget"]["max_native_rows"],
            "per_profile": {
                profile: {"max_holding_minutes": value["max_hold_minutes"], "decision": value["decision"],
                          "context": value["context"], "processing_seconds": settings["execution"]["processing_seconds"],
                          "context_minimum_completed_bars_for_value_and_slope":
                              {item["frame"]: item["sma"] + 1 for item in value["context"]},
                          "train_label_floors": {name: model["minimum_train"] for name, model in settings["models"].items()
                                                 if "minimum_train" in model},
                          "minimum_inner": settings["evaluation"]["minimum_inner"],
                          "minimum_outer": settings["evaluation"]["minimum_outer"]}
                for profile, value in settings["profiles"].items() if "perp" in profile}}


def inspect_archive(blob, kind, start, end):
    fields = FIELDS[kind]
    funding = kind == "fundingRate"
    with ZipFile(BytesIO(blob)) as archive:
        members = archive.infolist()
        if len(members) != 1 or not members[0].filename.endswith(".csv") or members[0].file_size > 16 * 1024**2:
            raise ValueError("Unexpected archive members or expanded size")
        with TextIOWrapper(archive.open(members[0]), encoding="utf-8-sig") as handle:
            rows = csv.reader(handle)
            first = next(rows)
            header = first if first[0].isalpha() or "_" in first[0] else None
            if header and (header[0] != fields[0] or len(header) != len(fields)
                           or (funding and tuple(header) != fields)
                           or (not funding and tuple(header[:7]) != fields[:7])):
                raise ValueError(f"Unexpected header {header}")
            values = list(rows if header else chain((first,), rows))
    dates = []
    intervals, zero = set(), 0
    for row in values:
        if len(row) != len(fields):
            raise ValueError("Unexpected CSV width")
        ms = int(row[0])
        stamp = pd.Timestamp(ms, unit="ms", tz="UTC")
        if not start <= stamp < end or (dates and stamp <= dates[-1]):
            raise ValueError("Out-of-window, repeated or unordered timestamps")
        dates.append(stamp)
        if funding:
            interval, rate = int(row[1]), float(row[2])
            if interval <= 0 or not math.isfinite(rate):
                raise ValueError("Invalid funding interval or rate")
            intervals.add(interval)
            zero += rate == 0.
        else:
            opened, high, low, closed, volume = map(float, row[1:6])
            if (ms % 60000 or int(row[6]) != ms + 59999 or
                not all(map(math.isfinite, (opened, high, low, closed, volume))) or
                low <= 0 or low > min(opened, closed) or high < max(opened, closed) or volume < 0):
                raise ValueError("Invalid UTC minute OHLCV")
    result = {"rows": len(dates), "first": dates[0].isoformat(), "last": dates[-1].isoformat(),
              "timestamp_unit": "ms", "header": header, "columns": fields}
    if funding:
        result.update(observed_interval_hours=sorted(intervals), recorded_zero_rates=zero,
                      off_minute_open=sum(at != at.floor("min") for at in dates),
                      observed_spacing_seconds=sorted({int((b - a).total_seconds()) for a, b in zip(dates, dates[1:])}))
    else:
        result.update(expected=int((end-start)/pd.Timedelta("1min")),
                      missing=int((end-start)/pd.Timedelta("1min"))-len(dates))
    return result


def probe():
    directory = OUTPUT / "binance-probe"
    evidence = directory / "result.json"
    with writer_lock(directory / ".probe.lock"):
        if evidence.exists():
            return read_json(evidence)
        periods = [("monthly", "2022-06", "2022-06-01", "2022-07-01", tuple(FIELDS)),
                   ("monthly", "2024-07", "2024-07-01", "2024-08-01", tuple(FIELDS)),
                   ("daily", "2025-11-07", "2025-11-07", "2025-11-08", ("klines", "markPriceKlines"))]
        specs = []
        for cadence, name, start, end, kinds in periods:
            for kind in kinds:
                nested = "" if kind == "fundingRate" else "/1m"
                filename = f"BTCUSDT-{kind if kind == 'fundingRate' else '1m'}-{name}.zip"
                specs.append((kind, f"{BASE}/{cadence}/{kind}/BTCUSDT{nested}/{filename}", start, end))
        requirements = calendar()
        write_json(directory / "plan.json", {"at": utc_now().isoformat(), "terms_url": TERMS, "budget": BUDGET,
                    "planned": specs, "requirements": requirements,
                    "license": "RESEARCH_ONLY", "venue": "Binance USD-M", "symbol": "BTCUSDT", "settlement": "USDT"})
        attempts, started, used = [], time.monotonic(), 0

        def fetch(client, url):
            nonlocal used
            if len(attempts) >= BUDGET["requests"] or time.monotonic() - started >= BUDGET["elapsed_seconds"]:
                raise ValueError("Frozen acquisition budget reached")
            attempt = {"url": url, "at": utc_now().isoformat()}
            attempts.append(attempt)
            try:
                with client.stream("GET", url) as response:
                    attempt["status"] = response.status_code
                    if response.status_code != 200:
                        return None
                    content = bytearray()
                    for block in response.iter_bytes():
                        used += len(block)
                        content.extend(block)
                        if (used > BUDGET["total_bytes"] or len(content) > BUDGET["response_bytes"]
                                or time.monotonic() - started >= BUDGET["elapsed_seconds"]):
                            raise ValueError("Frozen acquisition response, elapsed or byte budget reached")
                attempt["sha256"] = sha256(content).hexdigest()
                attempt["bytes"] = len(content)
                return bytes(content)
            except httpx.HTTPError as exc:
                attempt["error"] = str(exc)
                return None
            finally:
                write_json(directory / "attempts.json", attempts)

        results = []
        with httpx.Client(timeout=8, follow_redirects=False) as client:
            terms = fetch(client, TERMS)
            terms_hash = immutable_bytes(directory / "raw", terms, "md") if terms else None
            if terms and b"personal non-production research" in terms and b"models" in terms:
                for kind, url, start, end in specs:
                    checksum = fetch(client, url + ".CHECKSUM")
                    row = {"kind": kind, "url": url, "status": "CHECKSUM_UNAVAILABLE"}
                    results.append(row)
                    if attempts[-1].get("status") in (401, 403, 429, 451):
                        break
                    if checksum is None:
                        continue
                    published = checksum.decode("ascii").split()
                    if len(published) != 2 or published[1] != url.rsplit("/", 1)[1] or len(published[0]) != 64:
                        row["status"] = "INVALID_CHECKSUM"
                        continue
                    immutable_bytes(directory / "raw", checksum, "checksum")
                    content = fetch(client, url)
                    if content is None:
                        row["status"] = "ZIP_UNAVAILABLE"
                        if attempts[-1].get("status") in (401, 403, 429, 451):
                            break
                        continue
                    if sha256(content).hexdigest() != published[0]:
                        row["status"] = "CHECKSUM_MISMATCH"
                        continue
                    row["sha256"] = immutable_bytes(directory / "raw", content, "zip")
                    try:
                        row.update(inspect_archive(content, kind, utc(start), utc(end)), status="VERIFIED_SAMPLE")
                    except (ValueError, OSError) as exc:
                        row.update(status="SCHEMA_ERROR", error=str(exc))
        result = {"state": "WAITING_FOR_QUALIFIED_FUTURES_BUNDLE", "scope": "RESEARCH_ONLY", "venue": "Binance USD-M",
                  "symbol": "BTCUSDT", "settlement": "USDT", "terms": TERMS, "terms_sha256": terms_hash,
                  "requests": len(attempts), "bytes_received": used, "elapsed_seconds": round(time.monotonic()-started, 2),
                  "samples": results, "eligible_folds": [], "actual_fits": 0,
                  "reason": "Partial samples only. Each full frozen fold also exceeds the joined price-basis workload cap "
                            "before training warm-up. Historical contract and complete funding-schedule evidence are unverified.",
                  "requirements": requirements}
        write_json(evidence, result)
        return result
