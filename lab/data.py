from collections.abc import Iterator
from pathlib import Path
import time

import duckdb
import httpx
import numpy as np
import pandas as pd

from lab.config import ORIGIN, fingerprint
from lab.storage import (
    DATA, file_hash, immutable_bytes, read_json, utc, utc_now, write_json, writer_lock,
)

URL = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
SOURCE = "coinbase_exchange:spot:BTC-USD"
OHLCV = ["open", "high", "low", "close", "volume"]
TARGET_START = {3600: "2018-01-01", 300: "2023-11-08"}
MAX_SPAN = 299  # The server may also return the bar at end.


def feed(seconds: int, root: Path = DATA) -> Path:
    if seconds not in TARGET_START:
        raise ValueError("Supported candle resolutions are 300 and 3600 seconds")
    return root / "coinbase_exchange" / "spot" / "BTC-USD" / str(seconds)


def requests(start: pd.Timestamp, end: pd.Timestamp, seconds: int) -> Iterator[tuple]:
    step = pd.Timedelta(seconds=seconds)
    if start >= end or start.floor(step) != start or end.floor(step) != end:
        raise ValueError("Use aligned UTC boundaries with start < end")
    while start < end:
        stop = min(start + MAX_SPAN * step, end)
        yield start, stop
        start = stop


def normalize(payload: list, start: pd.Timestamp, end: pd.Timestamp, seconds: int,
              received: pd.Timestamp, raw_sha: str) -> pd.DataFrame:
    if not isinstance(payload, list) or len(payload) > 300:
        raise ValueError("Unexpected Coinbase candle payload")
    for row in payload:
        if not isinstance(row, list) or len(row) != 6 or type(row[0]) is not int or any(
            type(value) not in (int, float) for value in row[1:]
        ):
            raise ValueError("Expected Coinbase [time, low, high, open, close, volume]")
    frame = pd.DataFrame(payload, columns=["timestamp", "low", "high", "open", "close", "volume"])
    if (frame["timestamp"] % seconds != 0).any():
        raise ValueError("Misaligned raw candle")
    frame["open_at"] = pd.to_datetime(frame.pop("timestamp"), unit="s", utc=True)
    frame["close_at"] = frame["open_at"] + pd.Timedelta(seconds=seconds)
    frame[OHLCV] = frame[OHLCV].astype("float64")
    if (not np.isfinite(frame[OHLCV]).all().all()
            or (frame[OHLCV[:4]] <= 0).any().any()
            or (frame.volume < 0).any()
            or (frame.low > frame[["open", "close", "high"]].min(axis=1)).any()
            or (frame.high < frame[["open", "close"]].max(axis=1)).any()):
        raise ValueError("Invalid price, volume, or OHLC ordering")
    duplicates = int(frame.duplicated(subset=["open_at"]).sum())
    frame = frame.drop_duplicates()
    if frame.open_at.duplicated().any():
        raise ValueError("Conflicting raw candles for the same timestamp")
    frame = frame[(frame.open_at >= start) & (frame.open_at < end) & (frame.close_at <= received)].copy()
    frame["ingested_at"] = received
    frame["raw_sha256"] = pd.Series(raw_sha, index=frame.index, dtype="string")
    frame["source_id"] = pd.Series(SOURCE, index=frame.index, dtype="string")
    frame["resolution_seconds"] = seconds
    frame.attrs["raw_duplicates"] = duplicates
    return frame


def fetch(client: httpx.Client, start: pd.Timestamp, end: pd.Timestamp, seconds: int) -> httpx.Response:
    for attempt in range(5):
        time.sleep(0.26)
        try:
            response = client.get(URL, params={
                "start": start.isoformat(), "end": end.isoformat(), "granularity": seconds,
            })
        except httpx.TransportError:
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)
            continue
        if response.status_code not in (408, 429, 500, 502, 503, 504) or attempt == 4:
            response.raise_for_status()
            return response
        retry = response.headers.get("Retry-After", "")
        time.sleep(min(60, max(2 ** attempt, int(retry) if retry.isdecimal() else 0)))
    raise AssertionError("Unreachable retry state")


def publish(directory: Path, seconds: int) -> dict:
    checkpoints = [read_json(path) for path in sorted((directory / "requests").glob("*.json"))]
    if not checkpoints:
        return {}
    key = fingerprint(checkpoints)
    pointer = directory / "latest.json"
    if pointer.exists():
        previous = read_json(pointer)
        if previous["checkpoint_hash"] == key:
            return previous
    snapshots = directory / "snapshots"
    snapshots.mkdir(exist_ok=True)
    temporary = snapshots / ".building.parquet"
    paths = [str(directory / "chunks" / item["parquet"]) for item in checkpoints]
    try:
        with duckdb.connect(config={"threads": "1", "memory_limit": "512MB"}) as db:
            candles = db.from_parquet(paths)
            [(total, unique)] = candles.aggregate("count(*), count(distinct open_at)").fetchall()
            conflicts = candles.query("candles", """
                SELECT open_at FROM candles
                GROUP BY open_at HAVING count(DISTINCT (open, high, low, close, volume)) > 1
                ORDER BY open_at LIMIT 10
            """).fetchall()
            if conflicts:
                write_json(directory / "conflict.json", {"checkpoint_hash": key, "timestamps": conflicts})
                raise ValueError(f"Conflicting candles across requests: {conflicts}")
            candles.query("candles", """
                SELECT DISTINCT ON (open_at) * FROM candles ORDER BY open_at, ingested_at, raw_sha256
            """).write_parquet(str(temporary), compression="zstd")
        digest = file_hash(temporary)
        path = snapshots / f"{digest}.parquet"
        if not path.exists():
            temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    (directory / "conflict.json").unlink(missing_ok=True)
    record = {
        "source": SOURCE, "seconds": seconds, "sha256": digest, "checkpoint_hash": key,
        "rows": unique, "duplicates_across_requests": total - unique,
        "duplicates_within_responses": sum(item["duplicates"] for item in checkpoints),
        "requests": len(checkpoints), "published_at": utc_now(),
        "historical_availability": "Assume finalized at close; original publication time unknown",
    }
    write_json(snapshots / f"{key}.json", record)
    write_json(pointer, record)
    return record


def backfill(seconds: int, start: str, end: str, max_chunks: int, root: Path = DATA,
             client: httpx.Client | None = None) -> dict:
    if not 1 <= max_chunks <= 2000:
        raise ValueError("Choose a budget of 1–2000 new requests")
    directory = feed(seconds, root)
    first, last = utc(start), utc(end)
    if last > utc_now().floor(pd.Timedelta(seconds=seconds)):
        raise ValueError("End exceeds the last completed candle boundary")
    downloaded = 0
    pending_from = None
    with writer_lock(root / ".writer.lock"), httpx.Client(timeout=20) as default_client:
        client = client or default_client
        try:
            for left, right in requests(first, last, seconds):
                query = {"start": left.isoformat(), "end": right.isoformat(), "seconds": seconds}
                request_id = fingerprint(query)
                checkpoint = directory / "requests" / f"{request_id}.json"
                if checkpoint.exists():
                    continue
                if downloaded >= max_chunks:
                    pending_from = left
                    break
                response = fetch(client, left, right, seconds)
                received = utc_now()
                raw_sha = immutable_bytes(directory / "raw", response.content, "json")
                observation = {
                    **query, "raw_sha256": raw_sha, "ingested_at": received, "source": SOURCE,
                    "request_url": str(response.request.url),
                }
                write_json(directory / "observations" / f"{request_id}-{received.value}.json", observation)
                frame = normalize(response.json(), left, right, seconds, received, raw_sha)
                chunk = f"{request_id}.parquet"
                chunk_dir = directory / "chunks"
                chunk_dir.mkdir(exist_ok=True)
                temporary = chunk_dir / ".building.parquet"
                frame.to_parquet(temporary, index=False, compression="zstd")
                temporary.replace(chunk_dir / chunk)
                write_json(checkpoint, {
                    **observation, "parquet": chunk, "rows": len(frame),
                    "duplicates": frame.attrs["raw_duplicates"],
                })
                downloaded += 1
                if downloaded % 50 == 0:
                    print(f"{seconds}s: {downloaded} new checkpoints, through {right}", flush=True)
        finally:
            snapshot = publish(directory, seconds)
    return {"downloaded": downloaded, "pending_from": pending_from, "snapshot": snapshot}


def snapshot(seconds: int, root: Path = DATA) -> tuple[pd.DataFrame, dict]:
    directory = feed(seconds, root)
    pointer = directory / "latest.json"
    if not pointer.exists():
        return pd.DataFrame(columns=OHLCV), {}
    record = read_json(pointer)
    path = directory / "snapshots" / f"{record['sha256']}.parquet"
    if file_hash(path) != record["sha256"]:
        raise ValueError(f"Snapshot failed hash verification: {path}")
    frame = pd.read_parquet(path, engine="pyarrow")
    frame["open_at"] = frame.open_at.dt.as_unit("ns")
    frame["close_at"] = frame.close_at.dt.as_unit("ns")
    return frame.set_index("open_at").sort_index(), record


def coverage(frame: pd.DataFrame, seconds: int, start: pd.Timestamp | None = None,
             end: pd.Timestamp | None = None) -> dict:
    step = pd.Timedelta(seconds=seconds)
    start = utc(TARGET_START[seconds]) if start is None else start
    end = utc_now().floor(step) if end is None else end
    index = frame.index[(frame.index >= start) & (frame.index < end)] if len(frame) else []
    expected = max(0, int((end - start) // step))
    internal = int((index[-1] - index[0]) // step + 1 - len(index)) if len(index) else 0
    gaps = []
    if internal:
        for i in np.flatnonzero(np.diff(index.as_unit("ns").asi8) > step.value)[:20]:
            gaps.append({"start": index[i] + step, "end": index[i + 1]})
    return {
        "bars": len(frame), "first_open": frame.index.min() if len(frame) else None,
        "last_close": frame.index.max() + step if len(frame) else None,
        "freshness_seconds": (utc_now() - frame.index.max() - step).total_seconds() if len(frame) else None,
        "target_start": start, "target_end": end, "target_bars": expected,
        "missing_target_bars": expected - len(index), "internal_gap_bars": internal,
        "missing_before_first_target_bar": int((index[0] - start) // step) if len(index) else None,
        "missing_after_last_target_bar": int((end - index[-1] - step) // step) if len(index) else None,
        "first_internal_gaps": gaps, "backfill_complete": len(index) == expected and expected > 0,
    }


def aggregate(frame: pd.DataFrame, resolution: str, source_step: pd.Timedelta) -> pd.DataFrame:
    duration = pd.Timedelta(resolution)
    if duration < source_step or duration % source_step:
        raise ValueError("Only aggregate integer multiples of observed resolution")
    groups = frame.resample(resolution, origin=ORIGIN, label="left", closed="left")
    counts = groups["close"].count()
    result = groups.agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    result = result[counts == duration // source_step].copy()
    result["close_at"] = result.index + duration
    return result


def compare_feeds(hourly: pd.DataFrame, fine: pd.DataFrame) -> dict:
    if hourly.empty or fine.empty:
        return {"overlapping_hours": 0}
    derived = aggregate(fine, "1h", pd.Timedelta(minutes=5))
    shared = hourly.index.intersection(derived.index)
    delta = (hourly.loc[shared, OHLCV] - derived.loc[shared, OHLCV]).abs()
    tolerance = pd.Series({"open": 0.01, "high": 0.01, "low": 0.01, "close": 0.01, "volume": 1e-6})
    return {"overlapping_hours": len(shared), "different_hours": int(delta.gt(tolerance).any(axis=1).sum()),
            "differences_by_field": delta.gt(tolerance).sum().to_dict(),
            "max_abs_delta": delta.max().dropna().to_dict()}
