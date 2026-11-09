from bisect import bisect_right
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from lab.config import ORIGIN
from lab.data import OHLCV, aggregate
from lab.features import contiguous_groups
from lab.learning.data import validate_prices
from lab.learning.features import REGISTRY, segment_features
from lab.storage import file_hash, utc


STEP = pd.Timedelta("1min")
MAX_CHUNK_ROWS = 50_000


@dataclass(frozen=True)
class Chunk:
    trade: Path
    mark: Path
    trade_sha256: str
    mark_sha256: str


def checked_funding(series, expected, tolerance, start, end):
    """Match dated schedule slots to published actual settlements and keep actual times."""
    actual = pd.DatetimeIndex(series.index)
    nominal = pd.DatetimeIndex(expected)
    if (str(actual.tz) != "UTC" or str(nominal.tz) != "UTC" or not nominal.is_unique
            or not nominal.is_monotonic_increasing or not actual.is_unique or not actual.is_monotonic_increasing
            or not np.isfinite(series).all() or len(nominal) != len(actual)):
        raise ValueError("Missing, duplicated or invalid funding settlement evidence")
    limit = pd.Timedelta(tolerance)
    if (limit < pd.Timedelta(0) or limit >= STEP or
            (len(actual) and (actual[0] < start or actual[-1] >= end
                              or (abs(actual - nominal) > limit).any()))):
        raise ValueError("Published settlement falls outside its evidenced funding slot")
    return series


class PriceRows:
    def __init__(self, market, basis):
        self.market, self.basis = market, basis

    def __getitem__(self, position):
        chunk = bisect_right(self.market.starts, position) - 1
        return self.market.read(chunk)[self.basis][position - self.market.starts[chunk]]


class PartitionedMarket:
    """Keep a compact global clock and at most two native OHLC chunks resident."""

    scope = "HYPOTHETICAL_REPLAY"
    step = STEP

    def __init__(self, chunks, funding, embargo, total_limit, *, chunk_limit=MAX_CHUNK_ROWS):
        self.chunks, self.starts, times = tuple(chunks), [], []
        self.missing_mark = 0
        if not self.chunks or not 0 < chunk_limit <= MAX_CHUNK_ROWS:
            raise ValueError("No ordered replay chunks or invalid chunk ceiling")
        self.embargo = utc(embargo)
        self.total_rows = {"trade": 0, "mark": 0}
        left = None
        for chunk in self.chunks:
            self.starts.append(self.total_rows["trade"])
            indexes = {}
            for basis in self.total_rows:
                path = getattr(chunk, basis)
                if file_hash(path) != getattr(chunk, f"{basis}_sha256"):
                    raise ValueError(f"Immutable replay checksum changed for {path.name}")
                table = pq.ParquetFile(path)
                count = table.metadata.num_rows
                if not 0 < count <= chunk_limit:
                    raise ValueError("Native partition exceeds the chunk limit or is empty")
                self.total_rows[basis] += count
                if self.total_rows[basis] > total_limit:
                    raise ValueError("Native price basis exceeds the frozen total workload cap")
                index = pd.DatetimeIndex(table.read(columns=["open_at"], use_threads=False).column(0).to_pandas())
                if (str(index.tz) != "UTC" or not index.is_unique or not index.is_monotonic_increasing
                        or index[-1] + self.step > self.embargo or (left is not None and index[0] <= left)
                        or (index.as_unit("ns").asi8 % self.step.value).any()):
                    raise ValueError("Replay partition is unordered, overlaps, or crosses the embargo")
                indexes[basis] = index
            index = indexes["trade"]
            if len(indexes["mark"].difference(index)):
                raise ValueError("Mark rows fall outside the paired trade partition")
            self.missing_mark += len(index) - len(indexes["mark"])
            left = index[-1]
            times.append(index.as_unit("ns").asi8)
        self.times = np.concatenate(times)
        self.index = pd.to_datetime(self.times, utc=True)
        self.trade = pd.DataFrame({"close_at": self.index + self.step}, index=self.index)
        self.trade_prices, self.mark_prices = PriceRows(self, 0), PriceRows(self, 1)
        self.read = lru_cache(maxsize=2)(self._read)
        self.set_funding(funding)

    def set_funding(self, funding):
        self.funding, self.intrabar_funding = {}, {}
        for at, rate in sorted(funding.items()):
            at = utc(at)
            key = at.floor(self.step)
            if not self.index[0] <= at < self.index[-1] + self.step or not np.isfinite(rate):
                raise ValueError("Settlement falls outside paired observations")
            if at == key:
                self.funding[at] = float(rate)
            else:
                self.intrabar_funding.setdefault(key, []).append((at, float(rate)))

    def coverage(self):
        first, end = self.index[0], self.index[-1] + self.step
        return {"first_open": first.isoformat(), "end_exclusive": end.isoformat(), "rows": self.total_rows,
                "missing_trade": int((end - first) // self.step) - len(self.index), "missing_mark": self.missing_mark,
                "settlements": len(self.funding) + sum(map(len, self.intrabar_funding.values())),
                "off_grid_settlements": sum(map(len, self.intrabar_funding.values()))}

    def _frame(self, partition, basis):
        chunk = self.chunks[partition]
        path = getattr(chunk, basis)
        if file_hash(path) != getattr(chunk, f"{basis}_sha256"):
            raise ValueError("Replay partition changed after admission")
        columns = list(OHLCV if basis == "trade" else OHLCV[:4])
        frame = pq.read_table(path, columns=["open_at", *columns], use_threads=False).to_pandas().set_index("open_at")
        if basis == "mark":
            frame["volume"] = 0.
        frame["close_at"] = frame.index + self.step
        validate_prices(frame, self.step)
        return frame

    def _read(self, partition):
        trade = self._frame(partition, "trade")
        mark = self._frame(partition, "mark").reindex(trade.index)
        return trade[list(OHLCV)].to_numpy(), mark[list(OHLCV[:4])].to_numpy()

    def bars(self, resolutions):
        # Carry the unfinished native bucket. Aggregate features only after joining
        # the small derived frames so EMA, confirmed pivots and gap resets are exact.
        pieces = {resolution: [] for resolution in resolutions}
        tails = {resolution: None for resolution in resolutions}
        for partition in range(len(self.chunks)):
            frame = self._frame(partition, "trade")
            for resolution in pieces:
                combined = frame if tails[resolution] is None else pd.concat([tails[resolution], frame])
                # Aggregate's fixed origin is also used to identify incomplete buckets.
                duration = pd.Timedelta(resolution)
                end = ORIGIN + ((combined.index[-1] + self.step - ORIGIN) // duration) * duration
                done = combined.loc[combined.index < end]
                if len(done):
                    pieces[resolution].append(aggregate(done, resolution, self.step))
                tails[resolution] = combined.loc[combined.index >= end]
        return {resolution: pd.concat(parts).set_index("close_at") if parts else pd.DataFrame()
                for resolution, parts in pieces.items()}


def partition_features(market, profile):
    resolutions = dict.fromkeys([profile["decision"], *(item["frame"] for item in profile["context"])])
    aggregated = market.bars(resolutions)
    bars = aggregated[profile["decision"]]
    if bars.empty:
        raise ValueError("No complete decision buckets")
    groups = contiguous_groups(bars.index, pd.Timedelta(profile["decision"]))
    features = pd.concat([segment_features(segment) for _, segment in bars.groupby(groups)])
    columns = [name for name in REGISTRY if not name.startswith("context_")]
    contexts = []
    for index, item in enumerate(profile["context"]):
        frame = aggregated[item["frame"]]
        duration = pd.Timedelta(item["frame"])
        distance, slope = f"context_distance_{index}", f"context_slope_{index}"
        if frame.empty:
            features[distance], features[slope] = np.nan, np.nan
        else:
            groups = contiguous_groups(frame.index, duration)
            mean = frame.close.groupby(groups).transform(lambda values: values.rolling(item["sma"]).mean())
            previous = mean.groupby(groups).shift()
            fill = {"method": "ffill", "tolerance": duration - pd.Timedelta("1ns")}
            features[distance] = (frame.close / mean - 1).reindex(features.index, **fill)
            features[slope] = (mean / previous - 1).reindex(features.index, **fill)
        columns.extend((distance, slope))
        contexts.append(distance)
    features["uptrend"] = (features[contexts] > 0).all(axis=1)
    features["downtrend"] = (features[contexts] < 0).all(axis=1)
    features["context_abs"] = features[contexts].abs().max(axis=1, skipna=False)
    features["ready"] = np.isfinite(features[columns]).all(axis=1) & features.atr.gt(0)
    return features, columns
