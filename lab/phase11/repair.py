from collections import defaultdict
from hashlib import sha256
from pathlib import Path
import json
from uuid import uuid4

import httpx
import numpy as np
import pandas as pd

from lab.config import fingerprint
from lab.learning.design import inventory
from lab.phase11.archive import BatchLimit, Download, Restricted, folder, overview
from lab.phase11.plan import MINUTE, freeze_plan
from lab.phase11.qualify import read_values, records
from lab.storage import file_hash, put_bytes, read_json, utc, utc_now, write_json, writer_lock


MARK = "https://fapi.binance.com/fapi/v1/markPriceKlines"
TRADE = "https://fapi.binance.com/fapi/v1/klines"
EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"


def repair_specs(plan, observed):
    specs = []
    for spec in plan["body"]["sources"]:
        key, kind = spec["key"], spec["kind"]
        item = observed.get(key)
        if not item or kind not in ("markPriceKlines", "klines") or not item["missing_in_object_envelope"]:
            continue
        have = pd.DatetimeIndex(read_values(item, ["open_at"]).open_at)
        expected = pd.date_range(spec["start"], spec["end"], freq="1min", inclusive="left")
        missing = expected.difference(have)
        cursor = 0
        while cursor < len(missing):
            left = missing[cursor]
            end = missing.searchsorted(left + 1500 * MINUTE)
            relevant = missing[cursor:end]
            spec_id = fingerprint([key, list(relevant.astype(str))])
            specs.append({"id": spec_id, "asset": key, "kind": kind, "count": len(relevant),
                          "start": relevant[0].isoformat(), "last": relevant[-1].isoformat(),
                          "url": MARK if kind == "markPriceKlines" else TRADE,
                          "params": {"symbol": "BTCUSDT", "interval": "1m", "startTime": relevant[0].value // 10**6,
                                     "endTime": relevant[-1].value // 10**6 + 59_999, "limit": 1500}})
            cursor = end
    return specs


def parsed_price(payload, spec, original):
    rows = json.loads(payload)
    if not isinstance(rows, list) or len(rows) > 1500 or any(len(row) != 12 for row in rows):
        raise ValueError("Unexpected historical candle response schema")
    values = pd.DataFrame([row[:7] for row in rows], columns=("start", "open", "high", "low", "close", "volume", "end"))
    for key in values:
        values[key] = pd.to_numeric(values[key])
    if ((values.start % 60000).any() or not values.end.eq(values.start+59999).all() or
            not values.start.is_monotonic_increasing or not values.start.is_unique or not np.isfinite(values).all().all()):
        raise ValueError("REST candle response is not on the verified minute grid")
    values["open_at"] = pd.to_datetime(values.start, unit="ms", utc=True)
    if len(values) and (values.open_at.iloc[0] < utc(spec["start"]) or values.open_at.iloc[-1] > utc(spec["last"])
                        or (values.low <= 0).any() or (values.low > values[["open", "close"]].min(axis=1)).any()
                        or (values.high < values[["open", "close"]].max(axis=1)).any() or (values.volume < 0).any()):
        raise ValueError("REST returned out-of-plan or invalid price observations")
    return values.loc[~values.open_at.isin(original.open_at), list(original.columns)]


def normalize_overlay(root, original, additions, proofs, cap):
    merged = pd.concat([original, *additions], ignore_index=True).sort_values("open_at")
    if not merged.open_at.is_unique:
        raise ValueError("Repair attempted to overwrite or duplicate an observed archive row")
    key = proofs[0]["asset"]
    pointer = root / "overlays" / f"{key}.json"
    original_record = read_json(root / "streams" / f"{key}.json")
    target = root / "normalized" / f'{key}-repair-{fingerprint(proofs)[:12]}.parquet'
    temporary = target.with_name(f".{uuid4().hex}.tmp")
    try:
        merged.to_parquet(temporary, index=False, compression="zstd", compression_level=6)
        if cap.bytes_left() < 0:
            raise Restricted("Repair exceeds the dataset storage envelope")
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    repaired = len(merged) - original_record["rows"]
    result = {**original_record, "file": str(target), "normalized_sha256": file_hash(target),
              "normalized_bytes": target.stat().st_size, "rows": len(merged),
              "first": merged.open_at.iloc[0].isoformat(), "last": merged.open_at.iloc[-1].isoformat(),
              "missing_in_object_envelope": original_record["missing_in_object_envelope"] - repaired,
              "original_normalized_sha256": original_record["normalized_sha256"], "repairs": proofs}
    write_json(pointer, result)


def repair(plan=None):
    plan = plan or freeze_plan()
    root = folder(plan)
    result_path = root / "repair-result.json"
    if result_path.exists():
        return read_json(result_path)
    cap = plan["body"]["acquisition"]
    if inventory()["ram_available_mib"] < plan["body"]["parent_budget"]["min_available_ram_mib"]:
        return {"state": "WAITING_FOR_RESOURCES", "reason": "Available RAM below collector reserve"}
    with writer_lock(root / ".acquire.lock"):
        if result_path.exists():
            return read_json(result_path)
        prior = overview(plan)
        if prior["objects_verified"] + sum(row["status"] == "UNAVAILABLE" for row in prior["problems"]) < prior["objects_planned"]:
            raise ValueError("Finish the frozen source-object pass before planning exact gap repair")
        observed = records(plan)
        specifications = root / "repair-plan.json"
        if not specifications.exists():
            specs = repair_specs(plan, observed)
            write_json(specifications, {"plan": plan["id"], "metadata_url": EXCHANGE_INFO, "specs": specs,
                                        "requests_available_before_plan": prior["requests_remaining"], "at": utc_now().isoformat()})
        specs = read_json(specifications)["specs"]
        batch = prior["batches_started"] + 1
        batch_path = root / "batches" / f"{batch:03}.json"
        fetch = Download(plan, root, batch)
        start = utc_now()
        result, overlays, responses = {}, defaultdict(list), defaultdict(list)
        final, attempts = "COMPLETED_BOUNDED_REPAIR", []
        with httpx.Client(timeout=8, follow_redirects=False) as client:
            try:
                if not (root / "contract-current.json").exists():
                    row, data = fetch.get(client, EXCHANGE_INFO)
                    if data:
                        wire = json.loads(data)
                        btc = next(info for info in wire["symbols"] if info["symbol"] == "BTCUSDT")
                        if (btc["contractType"] != "PERPETUAL" or btc["baseAsset"] != "BTC" or
                                btc["quoteAsset"] != "USDT" or btc["marginAsset"] != "USDT"):
                            raise ValueError("Official BTCUSDT metadata disagrees with planned contract")
                        write_json(root / "contract-current.json", {"scope": "CURRENT_PUBLIC_METADATA", "receipt": row,
                                   "symbol": btc, "contract_representation": "BTC quantity, USDT linear settlement"})
                    else:
                        write_json(root / "contract-current.json", {"scope": "METADATA_UNAVAILABLE", "receipt": row})
                        if row["status"] in (401, 403, 418, 429, 451):
                            raise Restricted(f"Official API returned {row['status']}")
                for spec in specs:
                    prior_response = root / "repairs" / f'{spec["id"]}.json'
                    if prior_response.exists():
                        item = read_json(prior_response)
                        if item.get("status") == 200 and item.get("sha256"):
                            content = (root / "repair-raw" / (item["sha256"] + ".json")).read_bytes()
                            if sha256(content).hexdigest() != item["sha256"]:
                                raise ValueError("Repair receipt hash mismatch")
                        else:
                            content = None
                        row = item
                    else:
                        row, content = fetch.get(client, spec["url"], spec["params"])
                        if content:
                            put_bytes(root / "repair-raw" / (row["sha256"] + ".json"), content)
                        write_json(prior_response, row)
                    attempt = {**spec, "response": row, "restored_rows": 0}
                    attempts.append(attempt)
                    if content is not None:
                        original = read_values(observed[spec["asset"]], None)
                        additions = parsed_price(content, spec, original)
                        attempt["restored_rows"] = len(additions)
                        if len(additions):
                            overlays[spec["asset"]].append(additions)
                            responses[spec["asset"]].append({"asset": spec["asset"], "sha256": row["sha256"],
                                                            "from": spec["start"], "through": spec["last"],
                                                            "rows": len(additions)})
                    if row["status"] in (401, 403, 418, 429, 451):
                        raise Restricted(f"Official API returned {row['status']}")
            except (BatchLimit, Restricted, ValueError) as exc:
                final = str(exc)
        for key, additions in overlays.items():
            normalize_overlay(root, read_values(observed[key], None), additions, responses[key], fetch)
        write_json(batch_path, {"start": start.isoformat(), "end": utc_now().isoformat(),
                                "requests": fetch.count, "received_bytes": fetch.used, "status": final})
        result = {"state": final, "planned_gap_requests": len(specs), "planned_missing_rows": sum(row["count"] for row in specs),
                  "requests_available_at_start": prior["requests_remaining"], "gap_attempts": attempts,
                  "restored_rows": sum(row["restored_rows"] for row in attempts),
                  "unfinished_requests": len(specs) - len(attempts), "source": "Official Binance USD-M public market data",
                  "contract_evidence": str(root / "contract-current.json")}
        write_json(result_path, result)
        return result


def daily_fallback(plan=None):
    """Check the alternate official packaging once for the still-missing training marks."""
    from lab.phase11.archive import normalize
    from lab.phase11.plan import archive_spec

    plan = plan or freeze_plan()
    root = folder(plan)
    result_path = root / "daily-fallback-result.json"
    with writer_lock(root / ".acquire.lock"):
        if result_path.exists():
            return read_json(result_path)
        guard = overview(plan)
        prior = read_json(root / "repair-result.json")
        remaining = [row for row in prior["gap_attempts"] if row["restored_rows"] == 0 and row["response"]["status"] == 200]
        targets = {}
        for row in remaining:
            for at in pd.date_range(utc(row["start"]).normalize(), utc(row["last"]).normalize(), freq="D"):
                item = archive_spec(row["kind"], at, "daily")
                targets[item["key"]] = (row["asset"], item)
        if len(targets) * 2 > guard["requests_remaining"]:
            return {"state": "WAITING_FOR_RESOURCES", "needed_requests": len(targets)*2,
                    "requests_remaining": guard["requests_remaining"]}
        write_json(root / "daily-fallback-plan.json", {"plan": plan["id"], "objects": list(targets.values()),
                   "reason": "Check exact missing dates through alternate official packaging before declaring data gaps"})
        batch = guard["batches_started"] + 1
        started = utc_now()
        fetch = Download(plan, root, batch)
        result = []
        observed = records(plan)
        with httpx.Client(timeout=8, follow_redirects=False) as client:
            for key, (asset, item) in targets.items():
                row, checksum = fetch.get(client, item["url"] + ".CHECKSUM")
                attempt = {"asset": asset, "day": item["start"], "checksum": row, "restored_rows": 0}
                result.append(attempt)
                if row["status"] in (401, 403, 418, 429, 451):
                    break
                if checksum is None:
                    continue
                published = checksum.decode("ascii").split()
                if len(published) != 2 or published[1] != item["url"].rsplit("/", 1)[-1]:
                    raise ValueError(f"Unexpected alternate archive checksum: {key}")
                receipt, payload = fetch.get(client, item["url"])
                attempt["archive_receipt"] = receipt
                if receipt["status"] in (401, 403, 418, 429, 451):
                    break
                if payload is None:
                    continue
                if receipt["sha256"] != published[0]:
                    raise ValueError(f"Published alternate archive checksum mismatch: {key}")
                frame, _ = normalize(payload, item)
                source = read_values(observed[asset], None)
                additions = frame.loc[~frame.open_at.isin(source.open_at)]
                attempt["restored_rows"] = len(additions)
                if len(additions):
                    proof = {"asset": asset, "sha256": published[0], "url": item["url"],
                             "from": item["start"], "through": item["end"], "rows": len(additions)}
                    normalize_overlay(root, source, [additions], [*observed[asset].get("repairs", []), proof], fetch)
        write_json(root / "batches" / f"{batch:03}.json", {"start": started.isoformat(), "end": utc_now().isoformat(),
                   "requests": fetch.count, "received_bytes": fetch.used, "status": "EXACT_DAILY_FALLBACK"})
        report = {"state": "COMPLETE", "attempts": result, "restored_rows": sum(row["restored_rows"] for row in result)}
        write_json(result_path, report)
        return report
