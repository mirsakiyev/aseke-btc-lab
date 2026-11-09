from pathlib import Path
import json

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from lab.config import fingerprint
from lab.data import OHLCV
from lab.phase9.public import INTERVALS, attempts, latest, load_payload, run_folder
from lab.phase9.reference import OUTPUT, parent_design, read_frozen_reference, reference
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


PRICES = OHLCV[:4]
META_FIELDS = ("symbol", "multiplier", "baseCurrency", "quoteCurrency", "settleCurrency", "lotSize", "tickSize",
               "maxOrderQty", "marketMaxOrderQty", "maxLeverage", "initialMargin", "maintainMargin", "minRiskLimit",
               "maxRiskLimit", "riskStep", "isInverse", "isQuanto", "markMethod", "indexSymbol", "expireDate",
               "fundingRateGranularity", "currentFundingRateGranularity", "effectiveFundingRateCycleStartTime",
               "nextFundingRateDateTime", "fundingRateCap", "fundingRateFloor", "status")


def read_import_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def matching(payload, symbol, trade_type):
    if payload.get("tradeType") != trade_type:
        raise ValueError("Unexpected KuCoin market type")
    records = [row for row in payload["list"] if row.get("symbol") == symbol]
    if len(records) != 1:
        raise ValueError(f"Expected one exact {trade_type} instrument {symbol}")
    return records[0]


def timestamps(values, unit):
    if unit == "iso_utc":
        values = list(values)
        if any(pd.isna(value) or pd.Timestamp(value).tzinfo is None for value in values):
            raise ValueError("Import timestamps must have explicit offsets")
        return pd.to_datetime(values, utc=True, format="mixed").as_unit("ns")
    scale = {"s": (1_000_000_000, 10_000_000_000), "ms": (1_000_000_000_000, 10_000_000_000_000),
             "ns": (1_000_000_000_000_000_000, 9_000_000_000_000_000_000)}
    lower, upper = scale[unit]
    parsed = []
    for value in values:
        if isinstance(value, (bool, np.bool_)) or not str(value).isdigit():
            raise ValueError(f"Expected integer timestamp in {unit}")
        value = int(value)
        if not lower <= value < upper:
            raise ValueError(f"Timestamp out of documented {unit} units")
        parsed.append(value)
    return pd.to_datetime(parsed, unit=unit, utc=True).as_unit("ns")


def validate_candles(frame, step, trade):
    frame = frame.copy()
    price_fields = OHLCV if trade else PRICES
    for key in price_fields:
        frame[key] = pd.to_numeric(frame[key], errors="raise").astype(float)
    if (not np.isfinite(frame[price_fields]).all().all() or (frame[PRICES] <= 0).any().any()
            or (trade and (frame.volume < 0).any())
            or (frame.high < frame[["open", "low", "close"]].max(axis=1)).any()
            or (frame.low > frame[["open", "high", "close"]].min(axis=1)).any()
            or (frame.open_at.dt.as_unit("ns").astype("int64") % step.value != 0).any()):
        raise ValueError("Invalid, misaligned or nonfinite candle")
    frame["close_at"] = frame.open_at + step
    return checked_duplicates(frame, "open_at")


def checked_duplicates(frame, key):
    frame = frame.drop_duplicates().sort_values(key).reset_index(drop=True)
    if frame[key].duplicated().any():
        raise ValueError(f"Conflicting duplicate {key} observations")
    return frame


def candles(payload, spec, received, multiplier=None):
    params = spec["params"]
    market, symbol = params["tradeType"], params["symbol"]
    if payload.get("tradeType") != market or payload.get("symbol") != symbol:
        raise ValueError("Unexpected KuCoin candle symbol or market")
    is_trade = params["klineType"] == "TRADE"
    raw = payload["list"]
    # UTA v2 is open/high/low/close. Classic spot uses a different layout.
    if not isinstance(raw, list) or any(not isinstance(row, list) or len(row) not in ((7,) if is_trade else (5, 7))
                                       for row in raw):
        raise ValueError("Unexpected unified v2 candle field count")
    columns = ["open_at", *PRICES]
    if is_trade:
        columns += ["volume_native", "turnover_quote"]
    frame = pd.DataFrame([row[:len(columns)] for row in raw], columns=columns)
    frame["open_at"] = timestamps(frame.open_at, "s")
    if is_trade:
        frame["volume_native"] = pd.to_numeric(frame.volume_native, errors="raise").astype(float)
        frame["turnover_quote"] = pd.to_numeric(frame.turnover_quote, errors="raise").astype(float)
        if (not np.isfinite(frame.turnover_quote).all() or (frame.turnover_quote < 0).any()):
            raise ValueError("Invalid quote turnover")
        if market == "FUTURES" and (multiplier is None or multiplier <= 0):
            raise ValueError("Futures trade volume requires a verified contract multiplier")
        frame["volume"] = frame.volume_native * (multiplier if market == "FUTURES" else 1)
    step = pd.Timedelta(INTERVALS[params["interval"]])
    frame = validate_candles(frame, step, is_trade)
    start, end = utc(spec["start"]), utc(spec["end_exclusive"])
    result = frame[(frame.open_at >= start) & (frame.open_at < end) & (frame.close_at <= received)].copy()
    return result


def settled_rates(payload, spec):
    if payload.get("symbol") != spec["params"]["symbol"]:
        raise ValueError("Unexpected KuCoin funding symbol")
    rows = payload["list"]
    frame = pd.DataFrame({"settlement_at": timestamps([row["ts"] for row in rows], "ms"),
                          "rate": [float(row["fundingRate"]) for row in rows]})
    if not np.isfinite(frame.rate).all():
        raise ValueError("Invalid funding rate")
    left, right = utc(spec["start"]), utc(spec["end_exclusive"])
    return checked_duplicates(frame[(frame.settlement_at >= left) & (frame.settlement_at < right)], "settlement_at")


def market_quote(payload, symbol, trade_type, received):
    quote = matching(payload, symbol, trade_type)
    at = timestamps([payload["ts"]], "ns")[0]
    values = {name: float(quote[key]) for name, key in
              (("last", "lastPrice"), ("bid", "bestBidPrice"), ("ask", "bestAskPrice"))}
    if not all(np.isfinite(value) and value > 0 for value in values.values()) or values["bid"] > values["ask"]:
        raise ValueError("Invalid KuCoin quote")
    return {"at": at.isoformat(), "received_at": received.isoformat(), **values}


def mark_quote(payload, received):
    if payload["symbol"] != "XBTUSDTM":
        raise ValueError("Unexpected current mark instrument")
    values = {"mark": float(payload["value"]), "index": float(payload["indexPrice"])}
    if not all(np.isfinite(value) and value > 0 for value in values.values()):
        raise ValueError("Invalid mark or index")
    return {"at": timestamps([payload["timePoint"]], "ms")[0].isoformat(),
            "received_at": received.isoformat(), **values}


def metadata(folder=None):
    folder = folder or latest("metadata")
    if not folder:
        return {"status": "DATA_NOT_READY", "reason": "Public metadata has not been collected"}
    result = {"plan": folder.name, "scope": "OBSERVED_CURRENT_RULES", "as_of": None, "sources": {}, "problems": {}}
    for evidence in attempts(folder):
        key = evidence["request"]["key"]
        if evidence["status"] != "RECEIVED":
            result["problems"][key] = evidence["status"]
            continue
        result["sources"][key] = {name: evidence[name] for name in ("received_at", "raw_sha256")}
        result["as_of"] = evidence["received_at"]
        try:
            value = load_payload(folder, evidence)
            if key in ("spot", "perp"):
                symbol, market = ("BTC-USDT", "SPOT") if key == "spot" else ("XBTUSDTM", "FUTURES")
                result[key] = matching(value, symbol, market)
            elif key == "contract":
                if (value["symbol"] != "XBTUSDTM" or value["quoteCurrency"] != "USDT" or value["settleCurrency"] != "USDT"
                        or value["baseCurrency"] not in ("XBT", "BTC") or value["isInverse"] or value["expireDate"] is not None
                        or min(float(value[field]) for field in ("multiplier", "lotSize", "tickSize", "maxLeverage")) <= 0):
                    raise ValueError("Unsupported perpetual definition")
                result[key] = {name: value.get(name) for name in META_FIELDS}
            elif key == "isolated":
                if not value or any(row["symbol"] != "XBTUSDTM" for row in value):
                    raise ValueError("Unverified isolated risk tiers")
                result[key] = value
        except (KeyError, TypeError, ValueError) as exc:
            result["problems"][key] = str(exc)
    result["status"] = "OBSERVED" if all(key in result for key in ("spot", "contract", "isolated")) else "PARTIAL"
    return result


def coverage(frame, spec):
    key = "settlement_at" if spec["route"] == "funding" else "open_at"
    summary = {"rows": len(frame), "first": frame[key].iloc[0].isoformat() if len(frame) else None,
               "last": frame[key].iloc[-1].isoformat() if len(frame) else None}
    if key == "open_at":
        step = pd.Timedelta(INTERVALS[spec["params"]["interval"]])
        expected = int((utc(spec["end_exclusive"]) - utc(spec["start"])) / step)
        summary.update(expected=expected, missing=expected - len(frame), timestamp_unit="s")
    else:
        summary["timestamp_unit"] = "ms"
    return summary


def inspect_batch(folder, rules):
    report, frames, quotes = [], {}, {}
    contract = rules.get("contract", {})
    for evidence in attempts(folder):
        spec = evidence["request"]
        item = {"key": spec["key"], "route": spec["route"], "status": evidence["status"],
                "http_status": evidence.get("http_status"), "api_code": evidence.get("api_code"),
                "request": spec["params"], "received_at": evidence["received_at"], "raw_sha256": evidence.get("raw_sha256")}
        if evidence["status"] == "RECEIVED":
            try:
                data, received = load_payload(folder, evidence), utc(evidence["received_at"])
                if spec["route"] in ("kline", "funding"):
                    multiplier = float(contract["multiplier"]) if contract else None
                    frame = candles(data, spec, received, multiplier) if spec["route"] == "kline" else settled_rates(data, spec)
                    item.update(coverage(frame, spec))
                    frames[spec["key"]] = frame
                elif spec["route"] == "ticker":
                    quotes[spec["key"]] = market_quote(data, spec["params"]["symbol"], spec["params"]["tradeType"], received)
                elif spec["route"] == "mark_now":
                    quotes[spec["key"]] = mark_quote(data, received)
            except (KeyError, TypeError, ValueError) as exc:
                item.update(status="SCHEMA_OR_DATA_ERROR", error=str(exc))
        report.append(item)
    return report, frames, quotes


def import_data(path, descriptor_path):
    cfg = reference()
    record = read_import_json(descriptor_path)
    required = {"venue", "trade_type", "symbol", "quote", "kind", "timestamp_unit", "retrieved_at",
                "permission_evidence", "source", "start", "end_exclusive"}
    if not required <= set(record) or not all(record[key] for key in required):
        raise ValueError("Provide explicit dataset identity, access evidence, timestamps and bounded development dates")
    if record["kind"] not in ("trade", "mark", "index", "funding") or record["trade_type"] not in ("SPOT", "FUTURES"):
        raise ValueError("Unsupported dataset kind or market")
    if record["trade_type"] == "SPOT" and record["kind"] != "trade":
        raise ValueError("Spot inputs are trade candles")
    left, right = utc(record["start"]), utc(record["end_exclusive"])
    received = utc(record["retrieved_at"])
    if not left < right <= utc(parent_design()["design"]["holdout"]["start"]) or received > utc_now():
        raise ValueError("Development import must end by the sealed holdout boundary")
    limit = min(cfg["acquisition"]["maximum_import_rows"], parent_design()["design"]["settings"]["budget"]["max_native_rows"])
    path = Path(path)
    if path.stat().st_size > cfg["acquisition"]["maximum_local_bytes"]:
        raise ValueError("Import exceeds the local size budget")
    source_digest = file_hash(path)
    key = "settlement_at" if record["kind"] == "funding" else "open_at"
    fields = ["rate"] if key == "settlement_at" else [*PRICES, *(["volume"] if record["kind"] == "trade" else [])]
    if path.suffix.lower() == ".csv":
        raw = pd.read_csv(path, usecols=[key, *fields], nrows=limit + 1)
    elif path.suffix.lower() == ".parquet" and pq.ParquetFile(path).metadata.num_rows <= limit:
        raw = pd.read_parquet(path, columns=[key, *fields])
    else:
        raise ValueError("Use a bounded CSV or Parquet export")
    if len(raw) > limit:
        raise ValueError("Import exceeds the existing research row limit")
    raw[key] = timestamps(raw[key], record["timestamp_unit"])
    frame = raw[(raw[key] >= left) & (raw[key] < right)].copy()
    if frame.empty:
        raise ValueError("No rows inside the requested development interval")
    if key == "settlement_at":
        frame = frame[frame.settlement_at <= received].copy()
        frame["rate"] = pd.to_numeric(frame.rate, errors="raise").astype(float)
        if not np.isfinite(frame.rate).all():
            raise ValueError("Invalid funding rates")
        frame = checked_duplicates(frame, key)
    else:
        step = pd.Timedelta(INTERVALS[record["interval"]])
        is_trade = record["kind"] == "trade"
        if is_trade:
            units = record.get("volume_unit")
            if units == "contracts" and record["trade_type"] == "FUTURES":
                multiplier = float(record["base_per_contract"])
                if not np.isfinite(multiplier) or multiplier <= 0:
                    raise ValueError("Expected a documented positive contract multiplier")
                frame["volume"] = pd.to_numeric(frame.volume, errors="raise") * multiplier
            elif units != "base":
                raise ValueError("Declare base or futures contract volume units")
        frame = validate_candles(frame, step, is_trade)
        frame = frame[(frame.close_at <= right) & (frame.close_at <= received)].copy()
    if frame.empty:
        raise ValueError("No complete rows inside the declared import")
    if file_hash(path) != source_digest:
        raise ValueError("Source export changed while it was being imported")
    return register(frame, record, source_digest, key, step if key == "open_at" else None)


def register(frame, record, input_digest, key, step, *, acquisition=None):
    cfg = reference()
    evidence = {"descriptor": record, "input_sha256": input_digest, "scope": "PRE_HOLDOUT_RESEARCH",
                "rows": len(frame), "first": frame[key].iloc[0].isoformat(), "last": frame[key].iloc[-1].isoformat(),
                "missing_grid_rows": (int((frame[key].iloc[-1] - frame[key].iloc[0]) / step) + 1 - len(frame)
                                      if key == "open_at" else None)}
    if acquisition:
        evidence["acquisition"] = acquisition
    identity = fingerprint(evidence)
    target = OUTPUT / "imports" / identity
    with writer_lock(OUTPUT / ".imports.lock"):
        if (target / "manifest.json").exists():
            return read_json(target / "manifest.json")
        target.mkdir(parents=True, exist_ok=True)
        temporary = target / "normalized.tmp"
        try:
            frame.to_parquet(temporary, index=False)
            used = sum(item.stat().st_size for item in OUTPUT.rglob("*") if item.is_file())
            if used > cfg["acquisition"]["maximum_local_bytes"]:
                raise ValueError("Phase 9 storage ceiling reached")
            path = target / "normalized.parquet"
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
        result = {"id": identity, **evidence, "normalized_sha256": file_hash(path), "published_at": utc_now().isoformat()}
        write_json(target / "manifest.json", result)
    return result


def publish_history(folder):
    plan = read_json(folder / "plan.json")["design"]
    historical = plan["historical"]
    pinned_rules = plan.get("metadata_plan")
    rules = metadata(run_folder(pinned_rules)) if pinned_rules else metadata()
    rows, frames, _ = inspect_batch(folder, rules)
    if not frames or not any(len(frame) for frame in frames.values()):
        return {"status": "BLOCKED_DATA", "plan": folder.name, "attempts": rows}
    mode = read_frozen_reference(plan["reference_id"])["reference"]["profiles"][historical["profile"]]
    kind = historical["basis"]
    key = "settlement_at" if kind == "funding" else "open_at"
    frame = checked_duplicates(pd.concat(frames.values(), ignore_index=True), key)
    interval = "1hour" if mode["trade_type"] == "SPOT" else "1min"
    descriptor = {**{field: mode[field] for field in ("venue", "symbol", "trade_type", "quote")},
                  "kind": kind, "interval": interval, "timestamp_unit": "iso_utc", "volume_unit": "base",
                  "retrieved_at": max(row["received_at"] for row in rows),
                  "permission_evidence": "Unauthenticated official public GET response",
                  "source": plan["requests"][0]["url"], "start": historical["start"], "end_exclusive": historical["end"]}
    accepted = [row for row in rows if row["key"] in frames and "rows" in row]
    provenance = {"plan": folder.name, "rule_snapshot_plan": rules.get("plan"),
                  "pages_with_parsed_response": len({row["key"] for row in accepted}),
                  "planned_pages": len(plan["requests"])}
    return register(frame, descriptor, fingerprint([row["raw_sha256"] for row in accepted]), key,
                    pd.Timedelta(INTERVALS[interval]) if key == "open_at" else None, acquisition=provenance)
