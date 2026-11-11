"""Admission and read-only loading of bounded development bundles."""
from hashlib import sha256
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from lab.config import ROOT as PROJECT
from lab.data import OHLCV
from lab.learning.data import Market, validate_prices
from lab.learning.simulator import Contract
from lab.model import windows
from lab.phase10.partition import checked_funding
from lab.storage import file_hash, immutable_bytes, utc_now
from lab.strategy_lab.evaluator import number
from lab.strategy_lab.registry import PROFILES, canonical, digest, now


MAX_ROWS = 450_000
MAX_ASSET_BYTES = 48 * 1024**2
MAX_DATA_BYTES = 384 * 1024**2
DEVELOPMENT_END = pd.Timestamp("2025-11-08T00:00:00Z")
AVAILABILITY = ("RECORDED_RECEIPT", "HISTORICAL_CLOSE_PROXY")


def timestamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("An explicit timezone is required")
    return stamp.tz_convert("UTC")


def bounds(body):
    start, end = timestamp(body["start"]), timestamp(body["end_exclusive"])
    cap = PROFILES[body["profile"]]
    step = pd.Timedelta(seconds=cap[3])
    latest = min(DEVELOPMENT_END, timestamp(windows().holdout_start), utc_now())
    if not start < end <= latest or start.value % step.value or end.value % step.value:
        raise ValueError("The development partition must be aligned and end before protected or future observations")
    if (end - start) // step > MAX_ROWS:
        raise ValueError("Dataset exceeds the native row budget")
    return start, end, step


def parquet(path, body, basis):
    start, end, step = bounds(body)
    prices = OHLCV if basis == "trade" else OHLCV[:4]
    columns = ["open_at", "close_at", *prices]
    if body["availability"] == "RECORDED_RECEIPT":
        columns.append("received_at")
    file = pq.ParquetFile(path)
    if not 0 < file.metadata.num_rows <= MAX_ROWS or not set(columns).issubset(file.schema_arrow.names):
        raise ValueError(f"{basis} requires bounded UTC OHLCV and the declared receipt evidence")
    for name in ("open_at", "close_at", "received_at"):
        if name in columns and getattr(file.schema_arrow.field(name).type, "tz", None) != "UTC":
            raise ValueError(f"{basis} {name} must be stored as a UTC timestamp")
    for name in prices:
        kind = file.schema_arrow.field(name).type
        if not (pa.types.is_floating(kind) or pa.types.is_integer(kind)):
            raise ValueError(f"{basis} {name} must be numeric")
    frame = file.read(columns=columns, use_threads=False).to_pandas().set_index("open_at")
    frame.index = frame.index.as_unit("ns")
    if frame.index.hasnans or not frame.index.is_unique or not frame.index.is_monotonic_increasing or (
        frame.index[0] < start or frame.index[-1] + step > end
    ):
        raise ValueError(f"{basis} contains unaligned, unordered or out-of-partition observations")
    if basis == "mark":
        frame["volume"] = 0.  # Reuse the OHLC validator. Signals never use the mark frame.
    validate_prices(frame, step)
    if "received_at" not in frame:
        frame["received_at"] = frame.close_at
    if not frame.received_at.ge(frame.close_at).all() or frame.received_at.gt(utc_now()).any():
        raise ValueError(f"{basis} has invalid candle receipt ordering")
    return frame


def contract_for(body):
    config = body["contract"]
    contract = Contract.load(config["terms"])
    start, end, _ = bounds(body)
    expected = "spot" if body["market"] == "SPOT" else "linear_perp"
    if (contract.family != expected or contract.quote != "USDT" or
            not all(number(value, 0, 1e12) for key, value in config["terms"].items() if key not in ("family", "quote"))
            or not isinstance(config["reference"], str) or not config["reference"].strip()
            or timestamp(config["effective_from"]) > start or timestamp(config["effective_until"]) < end):
        raise ValueError("Dated instrument terms must cover the dataset and match the product")
    return contract


def settlements(body):
    start, end, step = bounds(body)
    if body["market"] == "SPOT":
        if body.get("funding") or body.get("funding_schedule"):
            raise ValueError("Spot has no funding or settlement schedule")
        return {}
    schedule = body.get("funding_schedule")
    if not isinstance(schedule, list) or not schedule or len(schedule) > 32:
        raise ValueError("Futures requires a dated funding schedule covering the full dataset")
    cursor, expected, tolerance = start, [], body.get("funding_tolerance_seconds", 0)
    if type(tolerance) is not int or not 0 <= tolerance < step.total_seconds():
        raise ValueError("Funding tolerance must be below one native bar")
    for part in schedule:
        left, right, anchor = map(timestamp, (part["start"], part["end_exclusive"], part["anchor"]))
        interval = part["interval_minutes"]
        if (left != cursor or not left < right <= end or type(interval) is not int or not 30 <= interval <= 1440
                or not isinstance(part["reference"], str) or not part["reference"].strip()):
            raise ValueError("Funding schedule segments must be chronological, contiguous and sourced")
        delta = pd.Timedelta(minutes=interval)
        expected.extend(pd.date_range(left + (anchor - left) % delta, right, inclusive="left", freq=delta))
        cursor = right
    if cursor != end or "funding" not in body or not isinstance(body["funding"], list) or len(body["funding"]) > 15000:
        raise ValueError("Missing or unbounded funding evidence")
    actual, rates = [], []
    for row in body["funding"]:
        at, receipt = timestamp(row["settlement_at"]), timestamp(row["received_at"])
        if not at <= receipt <= utc_now() or not number(row["rate"], -.1, .1):
            raise ValueError("Invalid published funding rate or receipt")
        actual.append(at)
        rates.append(row["rate"])
    index = pd.DatetimeIndex(actual, tz="UTC")
    nominal = pd.DatetimeIndex(expected, tz="UTC")
    return checked_funding(pd.Series(rates, index=index, dtype=float), nominal, pd.Timedelta(seconds=tolerance),
                           start, end).to_dict()


def data_id(db, identity):
    row = db.execute("SELECT body FROM dataset WHERE hash=?", (identity,)).fetchone()
    if row is None:
        raise ValueError("Unknown admitted development dataset")
    import json
    body = json.loads(row["body"])
    if digest(body) != identity:
        raise ValueError("Dataset manifest checksum mismatch")
    return body


def asset_path(root, key):
    return root / "datasets" / f"{key}.parquet"


def admit(db, root, draft, directory, approved_by):
    """Pin exact, externally supplied development artifacts after operator approval."""
    directory = directory.resolve()
    if not isinstance(approved_by, str) or not approved_by.strip() or draft.get("use") != "DEVELOPMENT_ONLY":
        raise ValueError("Explicit approval of development-only data is required")
    if draft.get("profile") not in PROFILES or (
        draft.get("market"), draft.get("instrument"), draft.get("native_seconds")
    ) != (*PROFILES[draft["profile"]][:2], PROFILES[draft["profile"]][3]) or draft.get("venue") != "KuCoin":
        raise ValueError("Dataset venue, product and cadence must match the trading profile")
    provenance = draft.get("provenance")
    if not isinstance(provenance, dict) or any(
        not isinstance(provenance.get(key), str) or not provenance[key].strip()
        for key in ("reference", "license", "origin", "exclusion_attestation")
    ) or draft.get("availability") not in AVAILABILITY:
        raise ValueError("Record source, license, origin, protected-data exclusions and availability assumptions")
    bounds(draft)
    contract_for(draft)
    settlements(draft)
    bases = {"trade"} if draft["market"] == "SPOT" else {"trade", "mark"}
    if set(draft["assets"]) != bases:
        raise ValueError("Futures requires independent trade and mark files")
    assets, payloads, indexes = {}, {}, {}
    for basis in sorted(bases):
        item = draft["assets"][basis]
        path = (directory / item["file"]).resolve(strict=True)
        if (not path.is_relative_to(directory) or path.is_relative_to(PROJECT) or path.suffix.lower() != ".parquet"
                or not 0 < path.stat().st_size <= MAX_ASSET_BYTES):
            raise ValueError("Use bounded Parquet exports from an external development intake directory")
        with path.open("rb") as stream:
            content = stream.read(MAX_ASSET_BYTES + 1)
        key = sha256(content).hexdigest()
        if len(content) > MAX_ASSET_BYTES or key != item["sha256"]:
            raise ValueError("Dataset source checksum or asset budget mismatch")
        frame = parquet(pa.BufferReader(content), draft, basis)
        indexes[basis], payloads[key] = frame.index, content
        assets[basis] = {"sha256": key, "filename": path.name, "rows": len(frame)}
    if "mark" in assets and assets["mark"]["sha256"] == assets["trade"]["sha256"]:
        raise ValueError("Futures mark evidence must be independent of trade candles")
    if "mark" in indexes and len(indexes["mark"].difference(indexes["trade"])):
        raise ValueError("Mark opens must belong to the trade grid")
    target_dir = root / "datasets"
    total = sum(path.stat().st_size for path in target_dir.glob("*.parquet"))
    for key, content in payloads.items():
        if asset_path(root, key).exists():
            if file_hash(asset_path(root, key)) != key:
                raise ValueError("Sandbox asset checksum mismatch")
        else:
            total += len(content)
    if total > MAX_DATA_BYTES:
        raise ValueError("Sandbox dataset storage ceiling reached")
    for content in payloads.values():
        immutable_bytes(target_dir, content, "parquet")
    body = {**draft, "assets": assets, "approved_by": approved_by.strip()}
    # Avoid a new dataset identity for identical content when re-admitting.
    identity = digest(body)
    db.execute("INSERT OR IGNORE INTO dataset VALUES(?,?,?)", (identity, now(), canonical(body)))
    admitted_at = db.execute("SELECT admitted_at FROM dataset WHERE hash=?", (identity,)).fetchone()[0]
    return {"hash": identity, "admitted_at": admitted_at, "body": body}


def load_market(root, body):
    start, end, step = bounds(body)
    frames = {}
    for basis, item in body["assets"].items():
        path = asset_path(root, item["sha256"])
        if file_hash(path) != item["sha256"]:
            raise ValueError("Sandbox asset checksum changed")
        frames[basis] = parquet(path, body, basis)
    trade = frames["trade"]
    mark = frames.get("mark", trade)
    if len(mark.index.difference(trade.index)):
        raise ValueError("Mark opens must belong to the trade grid")
    rates = settlements(body)
    market = Market(trade, mark, {}, step, "SANDBOX_HISTORICAL_HYPOTHETICAL")
    market.intrabar_funding = {}
    for at, rate in rates.items():
        key = at.floor(step)
        if at == key:
            market.funding[key] = rate
        else:
            market.intrabar_funding.setdefault(key, []).append((at, rate))
    expected = int((end - start) // step)
    coverage = {"expected": expected, "trade_bars": len(trade), "mark_bars": len(mark),
                "missing_trade": expected - len(trade), "missing_mark": expected - len(mark),
                "settlements": len(rates), "availability": body["availability"]}
    return market, contract_for(body), coverage
