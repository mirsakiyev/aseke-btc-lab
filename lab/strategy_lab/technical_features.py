"""Causal UTC candle construction and reviewed technical indicators.

Only a caller with the applicable grant may supply observations. This module
does not fetch market data, authorize a source, or evaluate trade rules.
"""
from collections import OrderedDict, deque
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from hashlib import sha256
from math import fsum
import struct
from threading import Lock

import numpy as np
import pandas as pd

from lab.config import ORIGIN, fingerprint
from lab.data import OHLCV, aggregate
from lab.features import contiguous_groups
from lab.strategy_lab.ema_pullback import _rma


MAX_NATIVE_ROWS = 220_000
MAX_CHART_ROWS = 50_000
MAX_INDICATORS = 32
INDICATOR_VERSION = "strategy-technical-indicators-v1"
INCREMENTAL_VERSION = "strategy-technical-indicators-stream-v1"
MAX_INCREMENTAL_WINDOW_VALUES = 4_096


def _utc_time(value, name):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{name} must be a timezone-aware timestamp")
    return stamp.tz_convert("UTC")


def _interval(value):
    step = pd.Timedelta(value)
    if pd.isna(step) or step <= pd.Timedelta(0):
        raise ValueError("Expected a positive fixed interval")
    return step


def _on_grid(index, step):
    return (index - ORIGIN) % step == pd.Timedelta(0)


def _utc_index(frame):
    if not isinstance(frame.index, pd.DatetimeIndex) or str(frame.index.tz) != "UTC":
        raise ValueError("Expected a UTC DatetimeIndex of candle opens")


def _clock(frame, field):
    values = pd.DatetimeIndex(frame[field])
    if values.tz is None:
        raise ValueError(f"{field} must contain timezone-aware timestamps")
    return values.tz_convert("UTC")


def _valid_ohlcv(frame):
    prices = frame[["open", "high", "low", "close"]]
    return (np.isfinite(frame).all(axis=1) & frame.volume.ge(0) & prices.gt(0).all(axis=1)
            & frame.high.ge(frame[["open", "close"]].max(axis=1))
            & frame.low.le(frame[["open", "close"]].min(axis=1)))


@dataclass(frozen=True)
class CandleBatch:
    """Observed chart candles and expected coverage over [start, end)."""

    bars: pd.DataFrame
    start: pd.Timestamp
    end: pd.Timestamp
    timeframe: pd.Timedelta
    native_step: pd.Timedelta
    as_of: pd.Timestamp

    @property
    def expected_bars(self) -> int:
        return int((self.end - self.start) // self.timeframe)

    @property
    def missing_bars(self) -> int:
        return self.expected_bars - len(self.bars)

    @property
    def complete(self) -> bool:
        return self.missing_bars == 0


def build_candles(native: pd.DataFrame, *, timeframe, native_step, as_of, start, end,
                  max_native_rows=MAX_NATIVE_ROWS) -> CandleBatch:
    """Build complete UTC buckets visible as of an actual knowledge cutoff.

    Native bars carry both their source close and their actual availability.
    Invalid, duplicate, unfinished, or unavailable inputs leave a gap. Each
    chart bar carries the latest constituent availability, including for a
    retrospectively retrieved feature snapshot. The caller applies any
    tighter receipt deadlines of its experiment.
    """
    _utc_index(native)
    step, chart = _interval(native_step), _interval(timeframe)
    if chart < step or chart % step:
        raise ValueError("Chart interval must be an integer multiple of the native interval")
    left, right, cutoff = (_utc_time(value, label)
                           for value, label in ((start, "start"), (end, "end"), (as_of, "as_of")))
    if left >= right or not _on_grid(pd.DatetimeIndex([left, right]), chart).all():
        raise ValueError("Requested candle range must align to nonempty chart intervals")
    if (type(max_native_rows) is not int or not 1 <= max_native_rows <= MAX_NATIVE_ROWS
            or max(len(native), (right - left) // step) > max_native_rows):
        raise ValueError("Requested candle range exceeds the native row budget")
    if not set([*OHLCV, "close_at", "available_at"]).issubset(native.columns) or not native.columns.is_unique:
        raise ValueError("Native candles require unique OHLCV, close_at and available_at columns")

    native = native.loc[(native.index >= left) & (native.index < right)].sort_index()
    observed = native[OHLCV].apply(pd.to_numeric, errors="coerce").astype(float)
    close, available = _clock(native, "close_at"), _clock(native, "available_at")
    valid = (_valid_ohlcv(observed).to_numpy() & _on_grid(native.index, step)
             & ~native.index.duplicated(keep=False) & (close == native.index + step)
             & (close <= available) & (available <= cutoff))
    bars = aggregate(observed.loc[valid], chart, step)
    bars = bars.loc[np.isfinite(bars.volume)].copy()
    knowledge = pd.Series(available[valid], index=observed.index[valid])
    bars["available_at"] = knowledge.resample(chart, origin=ORIGIN).max().reindex(bars.index)
    return CandleBatch(bars, left, right, chart, step, cutoff)


_DEFAULT_INPUT = {"ema": "close", "rsi": "close", "atr": "ohlc",
                  "sma": "volume", "rolling_high": "high", "rolling_low": "low"}
_DEFINITION = {
    "ema": "recursive; alpha=2/(length+1); first-observation seed",
    "rsi": "Wilder gains/losses; arithmetic seed; flat series undefined",
    "atr": "Wilder true range; arithmetic seed; first high-low",
    "sma": "arithmetic mean; full trailing window including current",
    "rolling_high": "maximum; full trailing window including current",
    "rolling_low": "minimum; full trailing window including current",
}


@dataclass(frozen=True)
class IndicatorSpec:
    name: str
    length: int
    input_series: str | None = None

    def __post_init__(self):
        if self.name not in _DEFAULT_INPUT:
            raise ValueError("Unknown technical indicator")
        if type(self.length) is not int or not 1 <= self.length <= MAX_CHART_ROWS:
            raise ValueError("Indicator length exceeds the supported chart budget")
        series = _DEFAULT_INPUT[self.name] if self.input_series is None else self.input_series
        if series not in (("ohlc",) if self.name == "atr" else OHLCV):
            raise ValueError("Unsupported input series for the indicator")
        object.__setattr__(self, "input_series", series)

    @property
    def definition(self) -> str:
        return _DEFINITION[self.name]

    @property
    def version(self) -> str:
        return INDICATOR_VERSION


def _indicator(spec, bars):
    length = spec.length
    if spec.name == "atr":
        previous = bars.close.shift()
        tr = pd.concat([bars.high - bars.low, (bars.high - previous).abs(),
                        (bars.low - previous).abs()], axis=1).max(axis=1)
        return _rma(tr, length)
    series = bars[spec.input_series]
    if spec.name == "ema":
        return series.ewm(span=length, adjust=False).mean()
    if spec.name == "rsi":
        change = series.diff()
        gain, loss = _rma(change.clip(lower=0), length), _rma(-change.clip(upper=0), length)
        return (100 - 100 / (1 + gain / loss)).where(gain.gt(0) | loss.gt(0))
    rolling = series.rolling(length)
    return {"sma": rolling.mean, "rolling_high": rolling.max, "rolling_low": rolling.min}[spec.name]()


def _validate_chart(bars, step, indicators):
    _utc_index(bars)
    if (len(bars) > MAX_CHART_ROWS or not 1 <= len(indicators) <= MAX_INDICATORS
            or not bars.index.is_monotonic_increasing or not bars.index.is_unique
            or not _on_grid(bars.index, step).all() or not set(OHLCV).issubset(bars.columns)
            or not bars.columns.is_unique):
        raise ValueError("Expected bounded, ordered, unique UTC chart inputs and indicators")
    _validate_plan(indicators)
    values = bars[OHLCV].apply(pd.to_numeric, errors="coerce").astype(float)
    if not _valid_ohlcv(values).all():
        raise ValueError("Chart inputs contain invalid OHLCV")
    return values


def _validate_plan(indicators):
    if (not 1 <= len(indicators) <= MAX_INDICATORS
            or any(not isinstance(name, str) or not name or len(name) > 128 or not isinstance(spec, IndicatorSpec)
                   for name, spec in indicators.items())):
        raise ValueError("Expected a bounded set of reviewed indicators under short output names")


def _compute(values, step, indicators):
    groups = contiguous_groups(values.index, step)
    segments = []
    for _, segment in values.groupby(groups, sort=False):
        # Aliases of a single reviewed specification share one calculation.
        computed = {spec: _indicator(spec, segment) for spec in dict.fromkeys(indicators.values())}
        segments.append(pd.DataFrame({name: computed[spec] for name, spec in indicators.items()}))
    return (pd.concat(segments) if segments else
            pd.DataFrame(index=values.index, columns=list(indicators), dtype=float))


def compute_indicators(bars: pd.DataFrame, *, timeframe,
                       indicators: Mapping[str, IndicatorSpec]) -> pd.DataFrame:
    """Compute technical features on the candle-open index, resetting at gaps.

    Feature values are only actionable after their chart close and receipt
    clocks. This function never chooses a decision time.
    """
    step = _interval(timeframe)
    return _compute(_validate_chart(bars, step, indicators), step, indicators)


@dataclass(frozen=True)
class FeatureIdentity:
    source_id: str
    snapshot_id: str
    instrument: str
    market: str
    timeframe: str | pd.Timedelta
    authorization_id: str | None = None

    def __post_init__(self):
        required = (self.source_id, self.snapshot_id, self.instrument, self.market)
        fields = required if self.authorization_id is None else (*required, self.authorization_id)
        if any(not isinstance(value, str) or not value.strip() or len(value) > 256 for value in fields):
            raise ValueError("Feature identity requires bounded source, snapshot, instrument and market identifiers")
        object.__setattr__(self, "timeframe", _interval(self.timeframe))


@dataclass
class _RecursiveAverage:
    length: int
    alpha: float
    seed: list[float] = field(default_factory=list)
    value: float = np.nan

    def update(self, sample):
        if len(self.seed) < self.length:
            self.seed.append(sample)
            if len(self.seed) == self.length:
                self.value = float(np.asarray(self.seed).mean())
        elif self.value != sample:
            # Match the adjust=False recurrence used by the batch evaluator.
            old = 1 - self.alpha
            self.value = (old * self.value + self.alpha * sample) / (old + self.alpha)
        return self.value


_ROLLING = {"sma", "rolling_high", "rolling_low"}


class _IndicatorCursor:
    def __init__(self, spec):
        self.spec = spec
        self.previous = None
        self.average = self.loss = self.window = None
        length = spec.length
        if spec.name in _ROLLING:
            self.window = deque(maxlen=length)
        elif spec.name == "ema":
            self.average = _RecursiveAverage(1, 2 / (length + 1))
        else:
            self.average = _RecursiveAverage(length, 1 / length)
            if spec.name == "rsi":
                self.loss = _RecursiveAverage(length, 1 / length)

    def update(self, candle):
        name = self.spec.name
        if name == "atr":
            high, low, close = candle["high"], candle["low"], candle["close"]
            distance = high - low
            if self.previous is not None:
                distance = max(distance, abs(high - self.previous), abs(low - self.previous))
            self.previous = close
            return self.average.update(distance)
        value = candle[self.spec.input_series]
        if name == "ema":
            return self.average.update(value)
        if name == "rsi":
            previous, self.previous = self.previous, value
            if previous is None:
                return np.nan
            change = value - previous
            gain, loss = self.average.update(max(change, 0.)), self.loss.update(max(-change, 0.))
            if not (gain > 0 or loss > 0):
                return np.nan
            return 100. if loss == 0 else 100. - 100. / (1. + gain / loss)
        self.window.append(value)
        if len(self.window) < self.spec.length:
            return np.nan
        if name == "sma":
            return fsum(self.window) / self.spec.length
        return (max if name == "rolling_high" else min)(self.window)


class IncrementalFeatures:
    """Append-only indicator state over one authorized chart lineage.

    The caller verifies its grant and candle provenance before each update.
    Each chart row needs its real close and availability clocks. Corrections
    and stale or overlapping updates require a new state and authorized replay.
    No decisions are emitted. A restart can replay the approved prefix and
    check the resulting tip, which is independent of the update block sizes.
    """

    def __init__(self, identity: FeatureIdentity, indicators: Mapping[str, IndicatorSpec],
                 *, require_contiguous=True):
        if not isinstance(identity, FeatureIdentity) or type(require_contiguous) is not bool:
            raise ValueError("Expected an immutable feature identity and explicit continuity rule")
        indicators = dict(indicators)
        _validate_plan(indicators)
        specs = tuple(dict.fromkeys(indicators.values()))
        windows = sum(spec.length for spec in specs if spec.name in _ROLLING)
        seeds = sum(spec.length * (1 + (spec.name == "rsi"))
                    for spec in specs if spec.name in {"atr", "rsi"})
        if windows + seeds > MAX_INCREMENTAL_WINDOW_VALUES:
            raise ValueError("Incremental indicator windows and seeds exceed the state budget")
        self._identity = identity
        self._indicators = indicators
        self._require_contiguous = require_contiguous
        self._cursors = {spec: _IndicatorCursor(spec) for spec in specs}
        self._tip = fingerprint([INCREMENTAL_VERSION, identity.source_id, identity.snapshot_id,
                                 identity.instrument, identity.market, identity.timeframe.value,
                                 identity.authorization_id, require_contiguous,
                                 [(alias, spec.name, spec.input_series, spec.length, spec.definition, spec.version)
                                  for alias, spec in indicators.items()]])
        self._last_open = self._as_of = None
        self._lock = Lock()

    @property
    def tip(self) -> str:
        """Content hash of the verified input prefix, including causal clocks."""
        with self._lock:
            return self._tip

    def update(self, bars: pd.DataFrame, *, as_of, expected_tip: str) -> pd.DataFrame:
        """Return features for new chart rows only and advance the state atomically."""
        cutoff = _utc_time(as_of, "as_of")
        step = self._identity.timeframe
        values = _validate_chart(bars, step, self._indicators)
        if values.empty or not {"close_at", "available_at"}.issubset(bars.columns):
            raise ValueError("Incremental input needs closed chart rows with availability clocks")
        closes, available = _clock(bars, "close_at"), _clock(bars, "available_at")
        received = _clock(bars, "received_at") if "received_at" in bars else available
        if not ((closes == values.index + step) & (closes <= received) & (received <= available)
                & (available <= cutoff)).all():
            raise ValueError("Incremental input includes incomplete or causally unavailable candles")

        with self._lock:
            if expected_tip != self._tip or (self._as_of is not None and cutoff < self._as_of):
                raise ValueError("Stale incremental lineage or as-of clock")
            if self._last_open is not None and values.index[0] <= self._last_open:
                raise ValueError("Incremental input overlaps prior candles; authorized replay is required")
            boundaries = values.index if self._last_open is None else values.index.insert(0, self._last_open)
            if self._require_contiguous and (boundaries[1:] - boundaries[:-1] != step).any():
                raise ValueError("Incremental input has a missing chart candle")
            cursors = deepcopy(self._cursors)
            last, tip = self._last_open, bytes.fromhex(self._tip)
            columns = {spec: np.empty(len(values)) for spec in cursors}
            for pos, (opened, row, close, receipt, known) in enumerate(
                    zip(values.index, values.itertuples(index=False, name=None), closes, received, available)):
                if last is not None and opened - last != step:
                    cursors = {spec: _IndicatorCursor(spec) for spec in cursors}
                candle = dict(zip(OHLCV, row))
                for spec, cursor in cursors.items():
                    columns[spec][pos] = cursor.update(candle)
                evidence = struct.pack("<qqqq5d", opened.value, close.value, receipt.value, known.value, *row)
                tip = sha256(tip + evidence).digest()
                last = opened
            result = pd.DataFrame({name: columns[spec] for name, spec in self._indicators.items()}, index=values.index)
            self._cursors, self._tip, self._last_open, self._as_of = cursors, tip.hex(), last, cutoff
            return result


def _input_digest(bars, values):
    digest = sha256()
    digest.update(len(values).to_bytes(8, "little"))
    digest.update(values.index.as_unit("ns").asi8.astype("<i8", copy=False).tobytes())
    digest.update(values.to_numpy(dtype="<f8").tobytes())
    for field in ("close_at", "received_at", "available_at"):
        if field in bars:
            digest.update(field.encode())
            digest.update(_clock(bars, field).as_unit("ns").asi8.astype("<i8", copy=False).tobytes())
    return digest.hexdigest()


class FeatureCache:
    """Bounded in-memory reuse of individual indicators on exact snapshots.

    Check the experiment's grant before using a cache. Include an authorization
    partition when consumers must not share cached computation. Changes to
    candle data, receipts, inputs, parameters, formula or scope miss the cache.
    """

    def __init__(self, *, max_entries=64, max_bytes=64 * 1024**2):
        if (type(max_entries) is not int or not 1 <= max_entries <= 64
                or type(max_bytes) is not int or not 1 <= max_bytes <= 64 * 1024**2):
            raise ValueError("Feature cache budget exceeds the workstation limit")
        self._max_entries, self._max_bytes = max_entries, max_bytes
        self._cache = OrderedDict()
        self._bytes = 0
        self._lock = Lock()

    def compute(self, bars: pd.DataFrame, *, identity: FeatureIdentity,
                indicators: Mapping[str, IndicatorSpec]) -> pd.DataFrame:
        indicators = dict(indicators)
        step = identity.timeframe
        values = _validate_chart(bars, step, indicators)
        context = (identity, _input_digest(bars, values), values.index.name, values.index.freqstr)
        keys = {spec: (*context, spec, spec.definition, spec.version)
                for spec in dict.fromkeys(indicators.values())}
        with self._lock:
            resolved, missing = {}, {}
            for spec, key in keys.items():
                if key in self._cache:
                    self._cache.move_to_end(key)
                    resolved[spec] = self._cache[key][0]
                else:
                    missing[str(len(missing))] = spec
            if missing:
                result = _compute(values, step, missing)
                for name, spec in missing.items():
                    resolved[spec] = result[name].copy(deep=True)
                    self._remember(keys[spec], resolved[spec])
            return pd.DataFrame({alias: resolved[spec] for alias, spec in indicators.items()}, copy=True)

    def _remember(self, key, series):
        size = int(series.memory_usage(deep=True))
        if size > self._max_bytes:
            return
        while self._cache and (len(self._cache) >= self._max_entries or self._bytes + size > self._max_bytes):
            _, (_, evicted_size) = self._cache.popitem(last=False)
            self._bytes -= evicted_size
        self._cache[key] = (series, size)
        self._bytes += size
