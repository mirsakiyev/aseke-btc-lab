"""Offline adapters for fixed, read-only BTC/USD Spot candle endpoints.

Collectors supply the recorded public request, original response bytes, and
actual request, receipt and local availability clocks. Source grants and
immutable observation snapshots are handled by the shared market-data layer.
"""

from bisect import bisect_left
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from hashlib import sha256
import json
from pathlib import Path
from operator import attrgetter
from types import MappingProxyType

import pandas as pd

from lab.config import ORIGIN, fingerprint
from lab.data import OHLCV
from lab.strategy_lab.market_data import (
    MAX_OBSERVATIONS, MAX_RAW_BYTES, MAX_READ_BYTES, Observation, ObservationKind, SourceIdentity,
)


class Venue(StrEnum):
    KRAKEN = "Kraken"
    COINBASE = "Coinbase Advanced Trade"


INSTRUMENT = "BTC-USD"
KRAKEN_URL = "https://api.kraken.com/0/public/OHLC"
COINBASE_URL = "https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD/candles"
KRAKEN_MAX_ROWS = 720
KRAKEN_MAX_RESPONSE_ROWS = KRAKEN_MAX_ROWS + 1
COINBASE_MAX_ROWS = 350
KRAKEN_BTC_USD_KEYS = frozenset({"XXBTZUSD", "XBTUSD"})
COINBASE_GRANULARITY = MappingProxyType({
    1: "ONE_MINUTE", 5: "FIVE_MINUTE", 15: "FIFTEEN_MINUTE", 30: "THIRTY_MINUTE",
    60: "ONE_HOUR", 240: "FOUR_HOUR", 1440: "ONE_DAY",
})


def _utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Spot source clocks require an explicit UTC offset")
    return stamp.tz_convert("UTC")


def _epoch(value):
    if not (type(value) is int or (isinstance(value, str) and value.isascii() and value.isdecimal()
                                    and len(value) <= 12)):
        raise ValueError("Candle open must be a Unix timestamp in seconds")
    seconds = int(value)
    if not 0 <= seconds <= 9_223_372_036:
        raise ValueError("Candle open is outside the supported Unix timestamp range")
    return pd.Timestamp(seconds, unit="s", tz="UTC")


def _seconds(stamp):
    return stamp.value // 1_000_000_000


@dataclass(frozen=True, slots=True)
class SpotCandleRequest:
    """Canonical request and optional local half-open window for one Spot feed."""

    venue: Venue
    interval_minutes: int
    start: pd.Timestamp | None = None
    end: pd.Timestamp | None = None
    wire_end: pd.Timestamp | None = None

    def __post_init__(self):
        object.__setattr__(self, "venue", Venue(self.venue))
        if type(self.interval_minutes) is not int or self.interval_minutes not in COINBASE_GRANULARITY:
            raise ValueError("Unsupported BTC/USD Spot candle interval")
        for name in ("start", "end"):
            value = getattr(self, name)
            if value is not None:
                stamp = _utc(value)
                if (stamp - ORIGIN) % self.interval:
                    raise ValueError("Spot request bounds must align to the UTC candle interval")
                object.__setattr__(self, name, stamp)
        if self.venue == Venue.COINBASE and (self.start is None or self.end is None):
            raise ValueError("Coinbase candles require explicit start and end")
        if self.wire_end is not None:
            sent = _utc(self.wire_end)
            if self.venue != Venue.COINBASE or sent not in (self.end, self.end - pd.Timedelta(seconds=1)):
                raise ValueError("Unexpected Coinbase wire end for the declared candle range")
            object.__setattr__(self, "wire_end", sent)
        if self.start is not None and self.end is not None:
            if self.start >= self.end:
                raise ValueError("Spot candle range must be nonempty")
            if (self.end - self.start) // self.interval > self.max_rows:
                raise ValueError("Spot request range exceeds the documented candle limit")

    @property
    def interval(self):
        return pd.Timedelta(minutes=self.interval_minutes)

    @property
    def max_rows(self):
        return KRAKEN_MAX_ROWS if self.venue == Venue.KRAKEN else COINBASE_MAX_ROWS

    @property
    def url(self):
        return KRAKEN_URL if self.venue == Venue.KRAKEN else COINBASE_URL

    @property
    def params(self):
        if self.venue == Venue.KRAKEN:
            values = {"pair": "XBTUSD", "interval": self.interval_minutes}
            if self.start is not None:
                values["since"] = _seconds(self.start)
            return values
        # Coinbase can include a candle opening at the wire end, displacing the first bar.
        sent = self.wire_end if self.wire_end is not None else self.end - pd.Timedelta(seconds=1)
        return {"start": str(_seconds(self.start)), "end": str(_seconds(sent)),
                "granularity": COINBASE_GRANULARITY[self.interval_minutes], "limit": self.max_rows}

    @property
    def headers(self):
        return {"Accept": "application/json", "Cache-Control": "no-cache"}


@dataclass(frozen=True, slots=True)
class SpotCandleReceipt:
    """Immutable captured public GET response; constructing it performs no I/O."""

    request: SpotCandleRequest
    started_at: pd.Timestamp
    received_at: pd.Timestamp
    available_at: pd.Timestamp
    status: int
    raw: bytes
    raw_sha256: str = field(init=False)
    id: str = field(init=False)

    def __post_init__(self):
        if not isinstance(self.request, SpotCandleRequest):
            raise ValueError("Expected a canonical BTC/USD Spot candle request")
        started, received, available = map(_utc, (self.started_at, self.received_at, self.available_at))
        if not started <= received <= available:
            raise ValueError("Request, response and local availability clocks are out of order")
        if type(self.status) is not int or not 100 <= self.status <= 599:
            raise ValueError("Expected a recorded HTTP status")
        if type(self.raw) is not bytes or not 0 < len(self.raw) <= MAX_RAW_BYTES:
            raise ValueError("Spot receipt exceeds the raw response byte limit or is empty")
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(self, "available_at", available)
        raw_sha = sha256(self.raw).hexdigest()
        object.__setattr__(self, "raw_sha256", raw_sha)
        request = self.request
        object.__setattr__(self, "id", fingerprint([
            "spot-candle-receipt-v1", request.url, request.params, request.headers,
            str(request.start), str(request.end), started.isoformat(), received.isoformat(),
            available.isoformat(), self.status, raw_sha,
        ]))

    @property
    def latency_seconds(self):
        return (self.received_at - self.started_at).total_seconds()


def _document(receipt):
    if receipt.status != 200:
        raise ValueError(f"Spot endpoint returned HTTP {receipt.status}")
    payload = json.loads(receipt.raw)
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object from the Spot endpoint")
    return payload


def _prices(values):
    if any(type(value) not in (str, int, float) for value in values):
        raise ValueError("Expected numeric values for all OHLCV fields")
    return tuple(map(float, values))


def _kraken_rows(payload):
    if payload.get("error") != [] or not isinstance(result := payload.get("result"), dict):
        raise ValueError("Kraken OHLC response is unsuccessful")
    pair_keys = set(result) - {"last"}
    if len(pair_keys) != 1 or not pair_keys <= KRAKEN_BTC_USD_KEYS:
        raise ValueError("Kraken OHLC response does not identify exactly one XBT/USD pair")
    rows = result[pair_keys.pop()]
    # The service can return 720 completed rows plus its mandatory unfinished tail.
    if not isinstance(rows, list) or not 1 <= len(rows) <= KRAKEN_MAX_RESPONSE_ROWS:
        raise ValueError("Kraken OHLC response exceeds the closed candle and unfinished row limit")
    if any(not isinstance(row, list) or len(row) != 8 for row in rows):
        raise ValueError("Unexpected Kraken OHLC row shape")
    opens = tuple(_epoch(row[0]) for row in rows)
    if any(current >= following for current, following in zip(opens, opens[1:])):
        raise ValueError("Kraken OHLC candles must be ordered and unique")
    # The final row is explicitly uncommitted, even if the response crossed a close.
    return len(rows), tuple((stamp, row[1:5] + [row[6]]) for stamp, row in zip(opens[:-1], rows[:-1])), 1


def _coinbase_rows(payload):
    rows = payload.get("candles")
    if "error" in payload or not isinstance(rows, list) or len(rows) > COINBASE_MAX_ROWS:
        raise ValueError("Coinbase candle response is unsuccessful or exceeds its documented bound")
    if payload.get("product_id", INSTRUMENT) != INSTRUMENT:
        raise ValueError("Coinbase candle response conflicts with the requested BTC-USD product")
    if any(not isinstance(row, dict) or not {"start", *OHLCV} <= row.keys() for row in rows):
        raise ValueError("Unexpected Coinbase candle row shape")
    if any(row.get("product_id", INSTRUMENT) != INSTRUMENT for row in rows):
        raise ValueError("Coinbase candle row conflicts with the requested BTC-USD product")
    parsed = tuple((_epoch(row["start"]), [row[key] for key in OHLCV]) for row in rows)
    if len({stamp for stamp, _ in parsed}) != len(parsed):
        raise ValueError("Coinbase response contains duplicate candle opens")
    return len(rows), tuple(sorted(parsed, key=lambda row: row[0])), 0


def _closed_rows(receipt):
    request, step = receipt.request, receipt.request.interval
    decoder = _kraken_rows if request.venue == Venue.KRAKEN else _coinbase_rows
    total, raw_rows, unfinished = decoder(_document(receipt))
    observations, outside = [], 0
    for stamp, values in raw_rows:
        if (stamp - ORIGIN) % step:
            raise ValueError("Spot candle open is off its UTC interval grid")
        if stamp + step > receipt.started_at:
            # The response could contain a value observed while the request was in flight.
            unfinished += 1
        elif (request.start is not None and stamp < request.start
              or request.end is not None and stamp >= request.end):
            outside += 1
        else:
            opening, high, low, closing, volume = _prices(values)
            observations.append((stamp, (opening, high, low, closing, volume)))
    return total, tuple(observations), unfinished, outside


@dataclass(frozen=True, slots=True)
class SpotReceiptAudit:
    receipt_id: str
    raw_sha256: str
    received_at: pd.Timestamp
    latency_seconds: float
    raw_rows: int
    complete_rows: int
    excluded_unfinished: int
    excluded_outside_range: int
    coverage_start: pd.Timestamp | None
    coverage_end: pd.Timestamp | None
    longest_contiguous_run: int
    missing_opens: tuple[pd.Timestamp, ...]
    minimum_base_volume: float | None
    maximum_base_volume: float | None
    zero_volume_rows: int


def _observation(receipt, source_id, opening, values):
    return Observation(source_id, ObservationKind.TRADE_OHLCV, opening, opening + receipt.request.interval,
                       receipt.received_at, receipt.available_at, receipt.raw_sha256, dict(zip(OHLCV, values)))


def inspect_receipt(receipt):
    """Summarize actual receipt and candle coverage without freezing an approved snapshot."""
    total, closed, unfinished, outside = _closed_rows(receipt)
    request, step = receipt.request, receipt.request.interval
    seen = {stamp: _observation(receipt, receipt.id, stamp, values) for stamp, values in closed}
    left = min(seen) if seen else None
    right = max(seen) + step if seen else None
    start = request.start or left
    cutoff = ORIGIN + (receipt.started_at - ORIGIN).floor(step)
    end = min(request.end, cutoff) if request.end is not None else cutoff
    if start is not None and end is not None and end > start:
        if (end - start) // step > MAX_OBSERVATIONS:
            raise ValueError("Spot receipt coverage interval exceeds the audit budget")
        gaps = tuple(stamp for stamp in pd.date_range(start, end, freq=step, inclusive="left") if stamp not in seen)
    else:
        gaps = ()
    longest, current, previous = 0, 0, None
    for opening in seen:
        current = current + 1 if previous is not None and opening - previous == step else 1
        longest = max(longest, current)
        previous = opening
    volumes = [row.values["volume"] for row in seen.values()]
    return SpotReceiptAudit(receipt.id, receipt.raw_sha256, receipt.received_at, receipt.latency_seconds,
                            total, len(seen), unfinished, outside, left, right, longest, gaps,
                            min(volumes, default=None), max(volumes, default=None), volumes.count(0))


@dataclass(frozen=True, slots=True, init=False)
class SpotCandleProvider:
    """Bind immutable captured Spot receipts to one reviewed source in MarketData."""

    source: SourceIdentity
    _raw: MappingProxyType
    _rows: MappingProxyType

    def __init__(self, source, receipts):
        if not isinstance(source, SourceIdentity) or source.market != "SPOT" or source.instrument != INSTRUMENT:
            raise ValueError("Spot provider requires a reviewed BTC-USD Spot source")
        venue = Venue(source.provider)
        raw, rows, receipt_ids = {}, {}, set()
        count, raw_bytes = 0, 0
        for receipt in receipts:
            if not isinstance(receipt, SpotCandleReceipt) or receipt.request.venue != venue:
                raise ValueError("Spot receipt venue differs from the source")
            if receipt.id in receipt_ids:
                continue
            receipt_ids.add(receipt.id)
            if len(receipt_ids) > MAX_OBSERVATIONS:
                raise ValueError("Spot provider exceeds the receipt budget")
            if receipt.raw_sha256 not in raw:
                raw_bytes += len(receipt.raw)
                if raw_bytes > MAX_READ_BYTES:
                    raise ValueError("Spot provider exceeds the raw receipt budget")
                raw[receipt.raw_sha256] = receipt.raw
            _, closed, _, _ = _closed_rows(receipt)
            count += len(closed)
            if count > MAX_OBSERVATIONS:
                raise ValueError("Spot provider exceeds the normalized observation budget")
            interval_rows = rows.setdefault(receipt.request.interval, [])
            interval_rows.extend(_observation(receipt, source.id, stamp, values) for stamp, values in closed)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "_raw", MappingProxyType(raw))
        index = {interval: tuple(sorted(values, key=attrgetter("start"))) for interval, values in rows.items()}
        object.__setattr__(self, "_rows", MappingProxyType(index))

    def read(self, kind, start, end, as_of, interval):
        if kind != ObservationKind.TRADE_OHLCV:
            raise ValueError("Spot candle provider only supplies trade-price OHLCV")
        rows = self._rows.get(interval, ())
        key = attrgetter("start")
        first = bisect_left(rows, start, key=key)
        last = bisect_left(rows, end, lo=first, key=key)
        return (row for row in rows[first:last] if row.available_at <= as_of)

    def raw(self, digest):
        return self._raw[digest]


def _read_bounded(path, limit):
    with path.open("rb") as stream:
        payload = stream.read(limit + 1)
    if not 0 < len(payload) <= limit:
        raise ValueError("Captured receipt file is empty or exceeds its byte limit")
    return payload


def load_captured_receipt(path):
    """Read local audit metadata and verify the checksum of its original response bytes."""
    path = Path(path)
    metadata = json.loads(_read_bounded(path, 64 * 1024))
    if not isinstance(metadata, dict):
        raise ValueError("Captured receipt metadata must be a JSON object")
    venue = Venue(metadata["venue"])
    end = metadata.get("end")
    # Older capture metadata used the declared boundary as the actual wire end.
    sent = metadata.get("wire_end", end) if venue == Venue.COINBASE else None
    if venue == Venue.COINBASE and sent is None:
        raise ValueError("Coinbase capture requires the recorded wire end")
    request = SpotCandleRequest(venue, metadata["interval_minutes"], metadata.get("start"), end, sent)
    raw = _read_bounded(path.parent / metadata["raw_path"], MAX_RAW_BYTES)
    receipt = SpotCandleReceipt(request, metadata["started_at"], metadata["received_at"],
                                metadata["available_at"], metadata["status"], raw)
    if receipt.raw_sha256 != metadata["raw_sha256"]:
        raise ValueError("Captured Spot response checksum mismatch")
    return receipt


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Inspect a captured BTC/USD Spot receipt offline.")
    parser.add_argument("receipt", type=Path, help="JSON audit metadata pointing to a local raw response")
    args = parser.parse_args()
    try:
        report = inspect_receipt(load_captured_receipt(args.receipt))
    except (OSError, KeyError, TypeError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(asdict(report), default=lambda clock: clock.isoformat(), indent=2))


if __name__ == "__main__":
    main()
