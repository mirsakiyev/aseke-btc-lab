from datetime import timedelta
from email.utils import parsedate_to_datetime
import json
import time

import httpx
import pandas as pd

from lab.config import fingerprint
from lab.phase9.reference import OUTPUT, controls, freeze_reference, parent_design, reference
from lab.storage import file_hash, immutable_bytes, read_json, utc, utc_now, write_json, writer_lock


UA = "https://api.kucoin.com/api/ua/v2/market"
CLASSIC = "https://api-futures.kucoin.com/api/v1"
ROUTES = {
    "instrument": f"{UA}/instrument",
    "kline": f"{UA}/kline",
    "funding": f"{UA}/funding-rate-history",
    "ticker": f"{UA}/ticker",
    "contract": f"{CLASSIC}/contracts/XBTUSDTM",
    "isolated_tiers": f"{CLASSIC}/contracts/risk-limit/XBTUSDTM",
    "mark_now": f"{CLASSIC}/mark-price/XBTUSDTM/current",
}
INTERVALS = {"1min": "1min", "15min": "15min", "1hour": "1h", "4hour": "4h", "1day": "1D"}


def request(key, route, **params):
    return {"key": key, "route": route, "url": ROUTES[route], "params": params}


def kline(key, symbol, trade_type, basis, interval, start, end):
    return {**request(key, "kline", symbol=symbol, tradeType=trade_type, klineType=basis, interval=interval,
                      startAt=int(start.timestamp()), endAt=int(end.timestamp()) - 1),
            "start": start.isoformat(), "end_exclusive": end.isoformat()}


def funding(key, start, end):
    return {**request(key, "funding", symbol="XBTUSDTM", startAt=int(start.timestamp() * 1000),
                      endAt=int(end.timestamp() * 1000) - 1), "start": start.isoformat(), "end_exclusive": end.isoformat()}


def plan_requests(kind, config, now, holdout, historical=None):
    if kind == "metadata":
        return [request("spot", "instrument", tradeType="SPOT", symbol="BTC-USDT"),
                request("perp", "instrument", tradeType="FUTURES", symbol="XBTUSDTM"),
                request("contract", "contract"), request("isolated", "isolated_tiers")]
    if kind == "probe":
        rows = [kline("spot-before-inner", "BTC-USDT", "SPOT", "TRADE", "1hour",
                      utc("2022-06-30T20:00Z"), utc("2022-07-01T00:00Z"))]
        for label, start in (("pre-limit", utc("2024-12-31T23:45Z")), ("early-2025", utc("2025-01-02T00:00Z")),
                             ("pre-holdout", utc(holdout["start"]) - pd.Timedelta("1D"))):
            for basis in ("TRADE", "MARK_PRICE", "INDEX_PRICE"):
                rows.append(kline(f"{label}-{basis}", "XBTUSDTM", "FUTURES", basis, "1min",
                                  start, start + pd.Timedelta("15min")))
        for label, start in (("funding-early", utc("2025-01-02T00:00Z")),
                             ("funding-late", utc(holdout["start"]) - pd.Timedelta("2D"))):
            rows.append(funding(label, start, start + pd.Timedelta("1D")))
        return rows
    if kind == "extended":
        return [kline(f"{label}-{basis}", "XBTUSDTM", "FUTURES", basis, "1min", utc(at),
                      utc(at) + pd.Timedelta("15min"))
                for label, at in (("train-2022", "2022-06-30T12:00Z"), ("inner-2024", "2024-06-30T12:00Z"),
                                  ("before-2025", "2024-12-30T12:00Z"))
                for basis in ("TRADE", "MARK_PRICE", "INDEX_PRICE")]
    quotes = [request("spot-quote", "ticker", tradeType="SPOT", symbol="BTC-USDT"),
              request("perp-quote", "ticker", tradeType="FUTURES", symbol="XBTUSDTM"),
              request("perp-mark", "mark_now")]
    if kind == "quotes":
        return quotes
    if kind == "history":
        if not historical:
            raise ValueError("Supply profile, basis and explicit development boundaries")
        mode = config["profiles"][historical["profile"]]
        left, right = utc(historical["start"]), utc(historical["end"])
        if not left < right <= utc(holdout["start"]):
            raise ValueError("Historical acquisition must end by the sealed holdout boundary")
        is_spot, basis = mode["trade_type"] == "SPOT", historical["basis"]
        if is_spot and basis != "trade":
            raise ValueError("Spot history has trade candles only")
        if basis == "funding":
            width = pd.Timedelta(days=7)
            builder = funding
        else:
            interval = "1hour" if is_spot else "1min"
            step = pd.Timedelta(INTERVALS[interval])
            page = config["acquisition"]["spot_page_bars" if is_spot else "futures_page_bars"]
            width = step * page
            if left.floor(step) != left or right.floor(step) != right:
                raise ValueError("Historical candle boundaries must match the native grid")
            if not is_spot and left < utc(config["acquisition"]["futures_intraday_history_from"]):
                raise ValueError("Bulk futures collection starts within the published range. Import older documented exports.")
            provider_basis = {"trade": "TRADE", "mark": "MARK_PRICE", "index": "INDEX_PRICE"}[basis]

            def builder(key, start, end):
                return kline(key, mode["symbol"], mode["trade_type"], provider_basis, interval, start, end)

        count = -(-(right - left) // width)
        if count > config["acquisition"]["history_max_requests"]:
            raise ValueError("Historical request exceeds the frozen page cap. Select a smaller interval.")
        return [builder(f"history-{index:03}", start, min(start + width, right))
                for index, start in enumerate(pd.date_range(left, right, freq=width, inclusive="left"))]
    if kind != "current" or now < utc(holdout["end_exclusive"]):
        raise ValueError("Current warmup requires a genuinely new post-holdout decision")
    rows = []
    for name, profile in config["profiles"].items():
        interval, market = profile["warmup_interval"], profile["trade_type"]
        step = pd.Timedelta(INTERVALS[interval])
        grace = pd.Timedelta(seconds=config["ideas"]["completed_candle_grace_seconds"])
        end = (now - grace).floor(step)
        start = end - profile["warmup_bars"] * step
        page = config["acquisition"]["spot_page_bars" if market == "SPOT" else "futures_page_bars"]
        for number in range(1, (profile["warmup_bars"] + page - 1) // page + 1):
            right = min(start + page * step, end)
            rows.append(kline(f"{name}-{number:02}", profile["symbol"], market, "TRADE", interval, start, right))
            start = right
    return rows + quotes


def retry_boundary(value, now):
    if not value:
        return now + timedelta(minutes=1)
    if value.isdecimal():
        return now + timedelta(seconds=int(value))
    try:
        return max(now, utc(parsedate_to_datetime(value)))
    except (TypeError, ValueError, OverflowError):
        return now + timedelta(minutes=1)


def send(client, spec, limit, raw_folder):
    if spec["url"] != ROUTES[spec["route"]]:
        raise ValueError("Saved request is outside the public GET allowlist")
    try:
        with client.stream("GET", spec["url"], params=spec["params"]) as response:
            status = response.status_code
            throttle = response.headers.get("Retry-After")
            content = bytearray()
            for block in response.iter_bytes():
                content.extend(block)
                if len(content) > limit:
                    return {"status": "RESPONSE_LIMIT", "http_status": status, "received_at": utc_now().isoformat()}
        received = utc_now()
        digest = immutable_bytes(raw_folder, bytes(content), "json")
        evidence = {"http_status": status, "raw_sha256": digest, "received_at": received.isoformat()}
        if status == 429:
            return {**evidence, "status": "RATE_LIMITED", "resume_after": retry_boundary(throttle, received).isoformat()}
        if status in (401, 403, 451):
            return {**evidence, "status": "ACCESS_RESTRICTED"}
        if status != 200:
            return {**evidence, "status": "HTTP_ERROR"}
        try:
            payload = json.loads(content)
        except (UnicodeError, ValueError):
            return {**evidence, "status": "INVALID_RESPONSE"}
        if not isinstance(payload, dict) or payload.get("code") != "200000" or "data" not in payload:
            code = payload.get("code") if isinstance(payload, dict) else None
            return {**evidence, "status": "API_ERROR", "api_code": code}
        return {**evidence, "status": "RECEIVED"}
    except httpx.TransportError as exc:
        return {"status": "TRANSPORT_ERROR", "error": type(exc).__name__, "received_at": utc_now().isoformat()}


def run_folder(plan_id):
    if len(plan_id) != 64 or any(char not in "0123456789abcdef" for char in plan_id):
        raise ValueError("Invalid acquisition plan ID")
    return OUTPUT / "acquisition" / plan_id


def attempts(folder):
    return [read_json(path) for path in sorted((folder / "attempts").glob("*.json"))]


def load_payload(folder, evidence):
    path = folder / "raw" / f"{evidence['raw_sha256']}.json"
    if file_hash(path) != evidence["raw_sha256"]:
        raise ValueError("Raw response checksum mismatch")
    return read_json(path)["data"]


def collect(kind=None, *, resume=None, transport=None, historical=None):
    if not controls()["monitor"]:
        raise ValueError("Phase 9 market monitoring is off. Use monitor on before collecting.")
    config = reference()
    with writer_lock(OUTPUT / ".public.lock"):
        if resume:
            folder = run_folder(resume)
            plan = read_json(folder / "plan.json")
            if plan["id"] != resume or fingerprint(plan["design"]) != resume:
                raise ValueError("Acquisition plan checksum mismatch")
            design = plan["design"]
            kind = design["kind"]
        else:
            now, parent = utc_now(), parent_design()
            observed_rules = latest("metadata") if kind != "metadata" else None
            design = {"kind": kind, "frozen_at": now.isoformat(), "reference_id": freeze_reference(config),
                      "holdout": parent["design"]["holdout"], "budget": config["acquisition"],
                      "scope": ("PROSPECTIVE_FEATURE_WARMUP_ONLY" if kind == "current" else
                                "PRE_HOLDOUT_RESEARCH" if kind == "history" else "PUBLIC_CAPABILITY_PROBE"),
                      "historical": historical,
                      "metadata_plan": observed_rules.name if observed_rules else None,
                      "requests": plan_requests(kind, config, now, parent["design"]["holdout"], historical)}
            plan = {"id": fingerprint(design), "design": design}
            folder = run_folder(plan["id"])
            write_json(folder / "plan.json", plan)
        budget = design["budget"]
        maximum = budget[f"{kind}_max_requests"]
        previous = attempts(folder)
        if len(design["requests"]) > maximum:
            raise ValueError("Plan exceeds its frozen request budget")
        accepted = {row["request"]["key"] for row in previous if row["status"] == "RECEIVED"}
        terminal = {row["request"]["key"] for row in previous if row["status"] in ("HTTP_ERROR", "API_ERROR")}
        limited = next((row for row in reversed(previous) if row["status"] == "RATE_LIMITED"), None)
        access_denied = any(row["status"] == "ACCESS_RESTRICTED" for row in previous)
        if access_denied or (limited and utc_now() < utc(limited["resume_after"])):
            return folder
        with httpx.Client(timeout=budget["timeout_seconds"], follow_redirects=False, transport=transport) as client:
            for spec in design["requests"]:
                if len(previous) >= maximum or not controls()["monitor"]:
                    break
                if spec["key"] in accepted | terminal:
                    continue
                used = sum(path.stat().st_size for path in OUTPUT.rglob("*") if path.is_file())
                if used + budget["maximum_response_bytes"] > budget["maximum_local_bytes"]:
                    raise ValueError("Phase 9 public evidence disk budget reached")
                time.sleep(budget["minimum_spacing_seconds"])
                row = {"request": spec, **send(client, spec, budget["maximum_response_bytes"], folder / "raw")}
                previous.append(row)
                write_json(folder / "attempts" / f"{len(previous):03}.json", row)
                if row["status"] in ("RATE_LIMITED", "ACCESS_RESTRICTED"):
                    break
        write_json(OUTPUT / f"latest-{kind}.json", {"plan": plan["id"]})
        return folder


def latest(kind):
    pointer = OUTPUT / f"latest-{kind}.json"
    return run_folder(read_json(pointer)["plan"]) if pointer.exists() else None
