from collections import Counter
import shutil
import time

import httpx
import pandas as pd

from lab.config import fingerprint
from lab.phase9.data import candles, settled_rates
from lab.phase9.public import attempts, funding, kline, latest, load_payload, request, send
from lab.phase10.worker import cache_read
from lab.phase13.registry import sealed
from lab.phase16.plan import freeze_initial
from lab.storage import RUNS, read_json, utc, utc_now, write_json, writer_lock


ROOT = RUNS / "phase17"
DOCS = {
    "kline": "https://www.kucoin.com/docs-new/v2/rest/ua/get-klines",
    "funding": "https://www.kucoin.com/docs-new/v2/rest/ua/get-history-funding-rate",
    "instrument": "https://www.kucoin.com/docs-new/v2/rest/ua/instrument",
    "tiers": "https://www.kucoin.com/docs-new/v2/rest/ua/get-position-tiers",
    "terms": "https://www.kucoin.com/legal/terms-of-use",
}
LIMITS = {"maximum_gets": 24, "max_response_bytes": 512 * 1024, "min_free_bytes": 2 * 1024**3,
          "timeout_seconds": 5, "spacing_seconds": .75}


def freeze():
    root = ROOT / "discovery"
    with writer_lock(root / ".plan.lock"):
        if (root / "plan.json").exists():
            record = read_json(root / "plan.json")
            return sealed(root / "plan.json", record["id"])
        initial = freeze_initial()
        previous = {}
        for name in ("probe", "extended", "metadata", "history", "current"):
            if path := latest(name):
                previous[name] = {"plan": path.name, "requests": dict(Counter(row["status"] for row in attempts(path)))}
        caches = {}
        for name in ("day", "swing", "position"):
            frame = cache_read(name)
            caches[name] = {"rows": len(frame), "first": frame.open_at.min().isoformat() if len(frame) else None,
                            "last": frame.close_at.max().isoformat() if len(frame) else None,
                            "scope": "Existing monitoring context only"}
        specs = []
        for name, at in (("spot-old", "2022-06-30"), ("spot-preseal", "2025-11-07"), ("spot-postseal", "2026-11-08")):
            start = utc(at)
            specs.append(kline(name, "BTC-USDT", "SPOT", "TRADE", "1hour", start, start + pd.Timedelta(days=1)))
        for name, at in (("before-2025", "2024-12-31T23:30Z"), ("early-2025", "2025-01-02"),
                         ("preseal", "2025-11-07"), ("postseal", "2026-11-08")):
            start = utc(at)
            for basis in ("TRADE", "MARK_PRICE"):
                specs.append(kline(f"{name}-{basis}", "XBTUSDTM", "FUTURES", basis, "1min", start, start + pd.Timedelta(minutes=30)))
            if name != "before-2025":
                specs.append(funding(f"{name}-funding", start, start + pd.Timedelta(days=1)))
        specs.extend([
            kline("pre-2025-daily-context", "XBTUSDTM", "FUTURES", "TRADE", "1day", utc("2024-05-01"), utc("2024-11-01")),
            request("spot-rules-now", "instrument", tradeType="SPOT", symbol="BTC-USDT"),
            request("futures-rules-now", "instrument", tradeType="FUTURES", symbol="XBTUSDTM"),
            request("contract-now", "contract"), request("isolated-tiers-now", "isolated_tiers"),
        ])
        body = {"version": "phase17-discovery-v1", "scope": "PUBLIC_CAPABILITY_PROBE_ONLY", "frozen_at": utc_now().isoformat(),
                "docs": DOCS, "limits": LIMITS, "requests": specs, "protected_holdout": initial["body"]["holdout"],
                "previous_plans": previous, "existing_caches": caches, "phase16_campaign": initial["id"]}
        record = {"id": fingerprint(body), "body": body}
        write_json(root / "plan.json", record)
        return record


def discover(transport=None):
    plan = freeze()
    root = ROOT / "discovery"
    limits = plan["body"]["limits"]
    with writer_lock(root / ".get.lock"):
        previous = attempts(root)
        used = {row["request"]["key"] for row in previous}
        stop = next((row for row in reversed(previous) if row["status"] in {"RATE_LIMITED", "ACCESS_RESTRICTED"}), None)
        if not stop or (stop["status"] == "RATE_LIMITED" and utc_now() >= utc(stop["resume_after"])):
            with httpx.Client(timeout=limits["timeout_seconds"], follow_redirects=False, transport=transport) as client:
                for spec in plan["body"]["requests"]:
                    if len(previous) >= limits["maximum_gets"] or shutil.disk_usage(root).free < limits["min_free_bytes"]:
                        break
                    if spec["key"] in used:
                        continue
                    path = root / "attempts" / f"{len(previous)+1:03}.json"
                    reserved = {"request": spec, "reserved_at": utc_now().isoformat(), "status": "RESERVED"}
                    write_json(path, reserved)
                    time.sleep(limits["spacing_seconds"])
                    row = {**reserved, **send(client, spec, limits["max_response_bytes"], root / "raw")}
                    if row["status"] == "RECEIVED":
                        try:
                            payload = load_payload(root, row)
                            if spec["route"] in {"funding", "kline"}:
                                frame = (settled_rates(payload, spec) if spec["route"] == "funding" else
                                         candles(payload, spec, utc(row["received_at"]), .001))
                                stamp = "settlement_at" if spec["route"] == "funding" else "open_at"
                                row.update(rows=len(frame), first=frame[stamp].min().isoformat() if len(frame) else None,
                                           last=frame[stamp].max().isoformat() if len(frame) else None)
                            else:
                                row.update(rows=len(payload) if isinstance(payload, list) else len(payload.get("list", [payload])),
                                           dated_historical_rules=False)
                        except (KeyError, TypeError, ValueError) as exc:
                            row.update(status="INVALID_RESPONSE", reason=str(exc))
                    write_json(path, row)
                    previous.append(row)
                    if row["status"] in {"RATE_LIMITED", "ACCESS_RESTRICTED"}:
                        break
        summary = {"plan": plan["id"], "attempted": len(previous), "maximum": limits["maximum_gets"],
                   "results": [{k: row[k] for k in ("status", "http_status", "rows", "first", "last", "received_at", "raw_sha256")
                                if k in row} | {"key": row["request"]["key"]} for row in previous]}
        write_json(root / "result.json", summary)
        return summary


def acquisition_decision():
    from lab.phase9.reference import OUTPUT
    from lab.phase17.registry import save

    root = ROOT / "discovery"
    result = read_json(root / "result.json")
    by_key = {row["key"]: row for row in result["results"]}
    start, end = utc("2025-01-01"), utc(freeze_initial()["body"]["holdout"]["start"])
    minutes = int((end-start)//pd.Timedelta(minutes=1))
    raw = {}
    for basis in ("TRADE", "MARK_PRICE"):
        row = by_key.get(f"early-2025-{basis}", {})
        if row.get("rows"):
            size = (root / "raw" / (row["raw_sha256"] + ".json")).stat().st_size
            raw[basis] = round(minutes*size/row["rows"])
    spot = []
    for path in sorted((OUTPUT / "imports").glob("*/manifest.json")):
        item = read_json(path)
        fields = item["descriptor"]
        if fields["trade_type"] == "SPOT" and fields["kind"] == "trade":
            spot.append({"id": item["id"], "start": fields["start"], "end_exclusive": fields["end_exclusive"],
                         "permission": fields.get("permission_evidence"), "consumed_by": ["Phase 9", "Phase 16"]})
    body = {"version": "phase17-native-history-acquisition-v1", "scope": "DISCOVERY_ONLY", "discovery": result["plan"],
            "actual_gets": result["attempted"], "maximum_gets": result["maximum"], "extra_bulk_gets": 0,
            "bulk_plan": [], "decision": "NO_QUALIFIED_USEFUL_BULK_RANGE", "spot_existing": spot,
            "spot_reason": "Pre-holdout Spot history is already cached and consumed. The unprotected post-holdout interval is too short for 147 days of context and chronological sample floors.",
            "futures_estimate": {"start": start.isoformat(), "end_exclusive": end.isoformat(), "rows_per_price_basis": minutes,
                                 "price_gets_at_200_per_page": 2*((minutes+199)//200),
                                 "funding_gets_at_seven_day_pages": ((end-start).days+6)//7,
                                 "retained_raw_price_bytes_approx": raw, "compact_clock_bytes": minutes*8,
                                 "two_partition_price_array_bytes": 2*50000*9*8, "rss_ceiling_mib": 640,
                                 "possible_context": "Day with 51 four-hour closes and Swing with 201 daily closes if fully paired and qualified. Trade counts are unknown."},
            "futures_reason": "Probe pages are sparse capability evidence. No dated compatible contract and isolated-margin evidence across that range or complete settled-funding schedule was admitted. Thousands of requests cannot repair that provenance or create an untouched sample.",
            "probe_before_documented_boundary": "The sampled December 31, 2024 minute window responded. Bulk availability earlier than the documented 2025 boundary remains unproven.",
            "future": "Collect only under the registered managed budgets. A useful separate history acquisition would require date-specific rules, permitted provenance and a new frozen page ledger."}
    return save("acquisition", body)
