"""Feature-only Kraken-to-KuCoin continuation for the original EMA recipe.

The caller supplies a grant-checked warmup snapshot and separately authorized
decision candles. This module has no collection, registration or trade side effects.
"""
from dataclasses import asdict, dataclass
from math import isfinite
import re
from types import MappingProxyType

import numpy as np
import pandas as pd

from lab.config import ORIGIN
from lab.data import OHLCV
from lab.strategy_lab import ema_pullback
from lab.strategy_lab.market_data import ObservationKind, ObservationSnapshot, ProvenanceClass, Purpose
from lab.strategy_lab.registry import digest
from lab.strategy_lab.technical_features import MAX_CHART_ROWS


VERSION = "CROSS_VENUE_FEATURE_WARMUP_V1"
KRAKEN_PROXY_SEEDED = "KRAKEN_PROXY_SEEDED"
KUCOIN_NATIVE_READY = "KUCOIN_NATIVE_READY"
DATA_INCOMPLETE = "DATA_INCOMPLETE"
SOURCE_BLOCKED = "SOURCE_BLOCKED"
NATIVE_SIGNAL_BARS = ema_pullback.ENTRY_PARAMETERS["volume_length"]
NATIVE_READY_BARS = ema_pullback.WARMUP_BARS

POLICY = MappingProxyType({
    "version": VERSION,
    "entry_recipe": ema_pullback.FAMILY,
    "ema_spans": (50, 200),
    "wilder_length": 14,
    "seed_minimum_bars": ema_pullback.WARMUP_BARS,
    "native_signal_minimum_bars": NATIVE_SIGNAL_BARS,
    "native_swing_bars": ema_pullback.EXIT_PARAMETERS["swing_bars"],
    "native_replacement_minimum_bars": NATIVE_READY_BARS,
    "ema_transition": "reviewed unscaled close recurrence from frozen Kraken states",
    "rsi_transition": "omit first cross-venue return and carry Wilder gain and loss unchanged",
    "atr_transition": "first native true range is native high minus low, then use preceding native close",
    "abnormal_basis": "absolute first native open minus last Kraken close exceeds frozen Kraken ATR14",
    "abnormal_basis_effect": "suppress proxy decisions through the native-only replacement threshold",
    "continuity": "proxy requires exact UTC seam and contiguous native bars from the seam",
    "native_after_gap": "once native-only threshold is reached, do not revert to the proxy seed",
    "volume_extremes_and_prior_bar": "use only native KuCoin candles",
})
POLICY_ID = digest(dict(POLICY))

_STEP_MARKETS = {900: ("FUTURES", "XBTUSDTM"), 14_400: ("FUTURES", "XBTUSDTM"),
                 86_400: ("SPOT", "BTC-USDT")}
_STATE_FIELDS = ("ema_fast", "ema_slow", "average_gain", "average_loss", "atr")
_HASH = re.compile(r"[0-9a-f]{64}")


def _utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Cross-venue evidence requires a timestamp with an explicit UTC offset")
    return stamp.tz_convert("UTC")


@dataclass(frozen=True, slots=True)
class ProxySeed:
    book_id: str
    source_id: str
    source_name: str
    rights_reference: str
    rights_evidence_sha256: str
    grant_id: str
    snapshot_id: str
    content_id: str
    timeframe_seconds: int
    start: pd.Timestamp
    end: pd.Timestamp
    as_of: pd.Timestamp
    available_at: pd.Timestamp
    count: int
    last_close: float
    ema_fast: float
    ema_slow: float
    average_gain: float
    average_loss: float
    atr: float

    @property
    def state_id(self):
        return digest({"policy_id": POLICY_ID, "content_id": self.content_id, "end": self.end.isoformat(),
                       "last_close": self.last_close, **{name: getattr(self, name) for name in _STATE_FIELDS}})

    def body(self):
        body = asdict(self)
        for name in ("start", "end", "as_of", "available_at"):
            body[name] = body[name].isoformat()
        return {"version": VERSION, "policy_id": POLICY_ID, "state_id": self.state_id,
                "provider": "Kraken", "instrument": "BTC-USD", "market": "SPOT", **body}

    @property
    def id(self):
        return digest(self.body())


@dataclass(frozen=True, slots=True)
class TransitionResult:
    frame: pd.DataFrame
    comparison: pd.DataFrame
    evidence: dict

    @property
    def id(self):
        return digest(self.evidence)


def _check_seed(seed):
    if (type(seed.timeframe_seconds) is not int or seed.timeframe_seconds not in _STEP_MARKETS
            or type(seed.count) is not int or not ema_pullback.WARMUP_BARS <= seed.count <= MAX_CHART_ROWS):
        raise ValueError("Invalid Kraken seed timeframe or candle count")
    step = pd.Timedelta(seconds=seed.timeframe_seconds)
    if ((seed.start - ORIGIN) % step or seed.end - seed.start != seed.count * step
            or not seed.end <= seed.available_at <= seed.as_of):
        raise ValueError("Invalid Kraken seed range or causal timestamps")
    if any(not _HASH.fullmatch(getattr(seed, name)) for name in
           ("source_id", "rights_evidence_sha256", "grant_id", "snapshot_id", "content_id")):
        raise ValueError("Invalid Kraken seed provenance")
    if any(not isinstance(getattr(seed, name), str) or not getattr(seed, name).strip()
           for name in ("book_id", "source_name", "rights_reference")):
        raise ValueError("Incomplete Kraken seed identity")
    if (any(type(getattr(seed, name)) not in (int, float) or not isfinite(getattr(seed, name))
            or getattr(seed, name) < 0 for name in (*_STATE_FIELDS, "last_close"))
            or min(seed.last_close, seed.ema_fast, seed.ema_slow) <= 0):
        raise ValueError("Invalid Kraken recursive indicator state")


def freeze_proxy_seed(snapshot, *, timeframe):
    """Freeze reviewed EMA and Wilder states from a complete authorized chart snapshot."""
    step = pd.Timedelta(timeframe)
    seconds = int(step.total_seconds())
    if step != pd.Timedelta(seconds=seconds) or seconds not in _STEP_MARKETS:
        raise ValueError("Cross-venue warmup supports 15m, 4h and 1D chart candles")
    if (not isinstance(snapshot, ObservationSnapshot) or snapshot.purpose != Purpose.WARMUP
            or snapshot.kind != ObservationKind.TRADE_OHLCV or snapshot.source.provider.casefold() != "kraken"
            or snapshot.source.instrument != "BTC-USD" or snapshot.source.market != "SPOT"
            or snapshot.source.provenance_class != ProvenanceClass.INDEPENDENT):
        raise PermissionError("An independent, authorized Kraken BTC-USD Spot warmup snapshot is required")
    count = len(snapshot.observations)
    if (snapshot.interval != step or not snapshot.complete or snapshot.end - snapshot.start != count * step
            or not ema_pullback.WARMUP_BARS <= count <= MAX_CHART_ROWS):
        raise ValueError("Kraken seed requires at least 200 consecutive completed candles on the decision timeframe")

    bars = snapshot.frame()
    grid = pd.date_range(snapshot.start, snapshot.end, freq=step, inclusive="left")
    if (not bars.index.equals(grid) or not bars.close_at.eq(bars.index + step).all()
            or not bars.available_at.ge(bars.close_at).all() or not bars.available_at.le(snapshot.as_of).all()
            or any(row.source_id != snapshot.source.id or row.kind != snapshot.kind for row in snapshot.observations)):
        raise ValueError("Kraken seed candles are incomplete, misaligned, or unavailable at the snapshot cutoff")
    reference = ema_pullback.features(bars, step, step)
    if len(reference) != len(bars):
        raise ValueError("Kraken seed contains invalid decision candles")
    latest = reference.iloc[-1]
    change = reference.close.diff()
    length = ema_pullback.ENTRY_PARAMETERS["rsi_length"]
    gain = ema_pullback._rma(change.clip(lower=0), length).iloc[-1]
    loss = ema_pullback._rma(-change.clip(upper=0), length).iloc[-1]
    seed = ProxySeed(snapshot.book_id, snapshot.source.id, snapshot.source.source_id,
                     snapshot.source.rights_reference, snapshot.source.rights_evidence_sha256, snapshot.grant_id,
                     snapshot.id, snapshot.content_id, seconds, snapshot.start, snapshot.end, snapshot.as_of,
                     bars.available_at.max(), len(bars), float(latest.close), float(latest.ema_fast),
                     float(latest.ema_slow), float(gain), float(loss), float(latest.atr))
    _check_seed(seed)
    return seed


def load_proxy_seed(body, identifier):
    """Verify and rehydrate a previously frozen content-addressed seed."""
    if (not isinstance(body, dict) or digest(body) != identifier or body.get("version") != VERSION
            or body.get("policy_id") != POLICY_ID or not ProxySeed.__dataclass_fields__.keys() <= body.keys()):
        raise ValueError("Cross-venue seed checkpoint or policy hash mismatch")
    values = {name: body[name] for name in ProxySeed.__dataclass_fields__}
    for name in ("start", "end", "as_of", "available_at"):
        values[name] = _utc(values[name])
    seed = ProxySeed(**values)
    _check_seed(seed)
    if seed.body() != body:
        raise ValueError("Cross-venue seed checkpoint identity mismatch")
    return seed


def _authorized_native(native, seam, step, as_of):
    """Validate the bounded frame and omit unavailable, malformed, or ambiguous buckets."""
    if native is None or native.empty:
        return pd.DataFrame({**{name: pd.Series(dtype=float) for name in OHLCV},
                             **{name: pd.Series(dtype="datetime64[ns, UTC]") for name in ("close_at", "available_at")}},
                            index=pd.DatetimeIndex([], tz="UTC")), []
    if (len(native) > MAX_CHART_ROWS or not isinstance(native.index, pd.DatetimeIndex)
            or str(native.index.tz) != "UTC" or not {*OHLCV, "close_at", "available_at"} <= set(native.columns)):
        raise ValueError("Native input requires bounded OHLCV candles with UTC opens, closes and availability")
    timestamp_fields = ["close_at", "available_at"]
    if "received_at" in native:
        timestamp_fields.append("received_at")
    if any(not isinstance(native[name].dtype, pd.DatetimeTZDtype) for name in timestamp_fields):
        raise ValueError("Native close, availability and receipt timestamps require explicit UTC offsets")
    columns = [*OHLCV, *timestamp_fields]
    if "raw_sha256" in native:
        columns.append("raw_sha256")
    native = native.loc[(native.index >= seam) & (native.index + step <= as_of)
                        & native.available_at.le(as_of), columns].sort_index().copy()
    for name in timestamp_fields:
        native[name] = native[name].dt.tz_convert("UTC")
    native[OHLCV] = native[OHLCV].apply(pd.to_numeric, errors="coerce").astype(float)
    valid = (np.isfinite(native[OHLCV]).all(axis=1) & native[OHLCV[:-1]].gt(0).all(axis=1)
             & native.volume.ge(0) & native.high.ge(native[["open", "close"]].max(axis=1))
             & native.low.le(native[["open", "close"]].min(axis=1))
             & native.close_at.eq(native.index + step) & native.available_at.ge(native.close_at)
             & ((native.index - ORIGIN) % step == pd.Timedelta(0)) & ~native.index.duplicated(keep=False))
    if "received_at" in native:
        valid &= native.received_at.ge(native.close_at) & native.received_at.le(native.available_at)
    if "raw_sha256" in native:
        valid &= native.raw_sha256.map(lambda value: isinstance(value, str) and bool(_HASH.fullmatch(value)))
    invalid = sorted(set(at.isoformat() for at in native.index[~valid]))
    return native.loc[valid], invalid


def _continue_average(seed, samples, alpha):
    values = pd.Series(np.r_[seed, samples.to_numpy()], dtype=float)
    updated = values.ewm(alpha=alpha, adjust=False).mean().iloc[1:]
    return pd.Series(updated.to_numpy(), index=samples.index, dtype=float)


def _proxy_features(seed, native):
    proxy = native.copy()
    previous = proxy.close.shift()
    for name in ("ema_fast", "ema_slow"):
        alpha = 2 / (ema_pullback.ENTRY_PARAMETERS[name] + 1)
        proxy[name] = _continue_average(getattr(seed, name), proxy.close, alpha)
    proxy["prior_ema_fast"] = proxy.ema_fast.shift()
    change = (proxy.close - previous).iloc[1:]
    gain = _continue_average(seed.average_gain, change.clip(lower=0), 1 / ema_pullback.ENTRY_PARAMETERS["rsi_length"])
    loss = _continue_average(seed.average_loss, -change.clip(upper=0), 1 / ema_pullback.ENTRY_PARAMETERS["rsi_length"])
    gain = gain.reindex(proxy.index, fill_value=seed.average_gain)
    loss = loss.reindex(proxy.index, fill_value=seed.average_loss)
    proxy["rsi"] = (100 - 100 / (1 + gain / loss)).where(gain.gt(0) | loss.gt(0))
    true_range = pd.concat([proxy.high - proxy.low, (proxy.high - previous).abs(),
                            (proxy.low - previous).abs()], axis=1).max(axis=1)
    proxy["atr"] = _continue_average(seed.atr, true_range, 1 / ema_pullback.ENTRY_PARAMETERS["atr_length"])
    required = [*ema_pullback.COMPARISONS, "atr", "swing_low", "swing_high"]
    proxy["ready"] = proxy.contiguous_bars.ge(NATIVE_SIGNAL_BARS) & np.isfinite(proxy[required]).all(axis=1)
    proxy["uptrend"] = proxy.ema_fast.round(9).gt(proxy.ema_slow.round(9))
    proxy["downtrend"] = proxy.ema_fast.round(9).lt(proxy.ema_slow.round(9))
    return proxy


def _bracket(frame, side):
    extreme = frame.swing_low.where(side.eq(1), frame.swing_high)
    stop = (extreme - side * ema_pullback.EXIT_PARAMETERS["stop_atr"] * frame.atr).where(side.ne(0))
    target = (frame.close + side * ema_pullback.EXIT_PARAMETERS["target_atr"] * frame.atr).where(side.ne(0))
    valid = (np.isfinite(frame.atr) & np.isfinite(stop) & np.isfinite(target) & frame.atr.gt(0) & stop.gt(0)
             & target.gt(0) & (side * (frame.close - stop)).gt(0) & (side * (target - frame.close)).gt(0))
    return stop, target, valid


def _compare(proxy, native):
    measures = ["ema_fast", "ema_slow", "rsi", "atr"]
    comparison = proxy[measures].add_prefix("proxy_").join(native[measures].add_prefix("native_"))
    for measure in measures:
        comparison[f"{measure}_difference"] = proxy[measure] - native[measure]
    proxy_side = ema_pullback.sides(proxy)
    native_side = ema_pullback.sides(native).reindex(proxy.index)
    comparison["proxy_signal_side"], comparison["native_signal_side"] = proxy_side, native_side
    comparable = proxy.ready & native.ready.reindex(proxy.index)
    comparison["entry_signal_agreement"] = proxy_side.eq(native_side).astype("boolean").where(comparable)
    proxy_stop, proxy_target, proxy_valid = _bracket(proxy, proxy_side)
    native_stop, native_target, native_valid = _bracket(native.reindex(proxy.index), native_side)
    matching = comparable & proxy_side.eq(native_side) & proxy_valid & native_valid
    comparison["stop_difference"] = (proxy_stop - native_stop).where(matching)
    comparison["target_difference"] = (proxy_target - native_target).where(matching)
    return comparison


def _rows_hash(native):
    columns = [*OHLCV, "close_at", "available_at"]
    columns.extend(name for name in ("received_at", "raw_sha256") if name in native)
    return digest({"columns": columns, "rows": [_json_row(row) for row in native[columns].to_dict("records")],
                   "opens": [at.isoformat() for at in native.index]})


def _json_row(row):
    if row is None:
        return None
    result = {}
    for key, value in row.items():
        if pd.isna(value):
            result[key] = None
        elif isinstance(value, (np.bool_, bool)):
            result[key] = bool(value)
        elif isinstance(value, pd.Timestamp):
            result[key] = value.isoformat()
        elif isinstance(value, (int, float, np.integer, np.floating)):
            result[key] = float(value) if np.isfinite(value) else None
        else:
            result[key] = str(value)
    return result


def evaluate_transition(seed, native, *, as_of, forward_provider, forward_market, forward_instrument,
                        source_blocked=False):
    """Replay causal feature evidence without creating decisions, fills or performance."""
    _check_seed(seed)
    if (forward_provider.casefold() != "kucoin"
            or (forward_market, forward_instrument) != _STEP_MARKETS[seed.timeframe_seconds]):
        raise ValueError("Forward venue, instrument and chart must match the original KuCoin EMA recipe")
    as_of = _utc(as_of)
    step = pd.Timedelta(seconds=seed.timeframe_seconds)
    seed_visible = as_of >= seed.as_of
    native, rejected = _authorized_native(native if seed_visible and not source_blocked else None,
                                          seed.end, step, as_of)
    expected_close = ORIGIN + ((as_of - ORIGIN) // step) * step
    reference = ema_pullback.features(native, step, step)
    prefix = native.index == seed.end + np.arange(len(native)) * step
    prefix_count = int(np.logical_and.accumulate(prefix).sum())
    prefix_index = native.index[:prefix_count] + step
    proxy = _proxy_features(seed, reference.loc[prefix_index]) if prefix_count else reference.iloc[:0]
    comparison = _compare(proxy, reference)

    frame = reference.copy()
    frame["source_mode"] = DATA_INCOMPLETE
    frame["reason"] = "CONTIGUOUS_NATIVE_HISTORY_REQUIRED"
    segments = frame.index.to_series().diff().ne(step).cumsum()
    chart_availability = native.available_at.set_axis(native.index + step)
    frame["available_at"] = pd.to_datetime(chart_availability.groupby(segments).cummax(), utc=True)
    first = native.iloc[0] if prefix_count else None
    basis = float(first.open - seed.last_close) if first is not None else None
    abnormal = basis is not None and abs(basis) > seed.atr
    native_ready = reference.ready
    frame.loc[native_ready, "source_mode"] = KUCOIN_NATIVE_READY
    frame.loc[native_ready, "reason"] = "NATIVE_INDICATORS_READY"

    # A completed native-only threshold is a permanent protocol switch, including
    # after a later gap. The proxy is also permanently severed by its first gap.
    proxy_rows = proxy.index[proxy.contiguous_bars.lt(NATIVE_READY_BARS)]
    frame.loc[proxy_rows, proxy.columns] = proxy.loc[proxy_rows]
    if len(proxy_rows):
        frame.loc[proxy_rows, "available_at"] = frame.loc[proxy_rows, "available_at"].clip(lower=seed.as_of)
        frame.loc[proxy_rows, "reason"] = "NATIVE_SIGNAL_WINDOW_INCOMPLETE"
        if abnormal:
            frame.loc[proxy_rows, "reason"] = "ABNORMAL_CROSS_VENUE_PRICE_BASIS"
        else:
            proxy_ready = proxy_rows[proxy.loc[proxy_rows, "ready"]]
            frame.loc[proxy_ready, "source_mode"] = KRAKEN_PROXY_SEEDED
            frame.loc[proxy_ready, "reason"] = "PROXY_INDICATORS_AND_NATIVE_SIGNAL_WINDOWS_READY"
    frame["ready"] = frame.source_mode.isin((KRAKEN_PROXY_SEEDED, KUCOIN_NATIVE_READY))
    frame["signal_side"] = ema_pullback.sides(frame)
    stop, target, valid = _bracket(frame, frame.signal_side)
    invalid_bracket = frame.signal_side.ne(0) & ~valid
    frame.loc[invalid_bracket, "reason"] = "INVALID_SIGNAL_BRACKET"
    frame.loc[invalid_bracket, "signal_side"] = 0
    frame["stop"], frame["target"] = stop.where(valid), target.where(valid)

    latest = frame.loc[expected_close] if expected_close in frame.index else None
    mode = latest.source_mode if latest is not None else DATA_INCOMPLETE
    reason = latest.reason if latest is not None else "LATEST_COMPLETED_NATIVE_CANDLE_UNAVAILABLE"
    if not seed_visible:
        mode, reason = DATA_INCOMPLETE, "KRAKEN_SEED_UNAVAILABLE_AT_CUTOFF"
    if source_blocked:
        mode, reason = SOURCE_BLOCKED, "KUCOIN_FORWARD_SOURCE_AUTHORIZATION_UNAVAILABLE"
    native_progress = int(latest.contiguous_bars) if latest is not None else 0
    evidence = {
        "version": VERSION, "policy_id": POLICY_ID, "seed_id": seed.id, "seed_snapshot_id": seed.snapshot_id,
        "seed_content_id": seed.content_id, "seed_state_id": seed.state_id, "seed_book_id": seed.book_id,
        "seed_source_id": seed.source_id, "seed_grant_id": seed.grant_id, "warmup_provider": "Kraken",
        "warmup_instrument": "BTC-USD", "warmup_count": seed.count, "warmup_start": seed.start.isoformat(),
        "warmup_end": seed.end.isoformat(), "warmup_available_at": seed.available_at.isoformat(),
        "forward_provider": "KuCoin", "forward_market": forward_market, "forward_instrument": forward_instrument,
        "as_of": as_of.isoformat(), "expected_decision_close": expected_close.isoformat(), "source_mode": mode,
        "reason": reason, "native_count": len(native), "native_contiguous_bars": native_progress,
        "native_volume_progress": min(native_progress, NATIVE_SIGNAL_BARS),
        "native_swing_progress": min(native_progress, ema_pullback.EXIT_PARAMETERS["swing_bars"]),
        "proxy_feature_ready": seed_visible,
        "native_signal_ready": bool(latest.ready) if latest is not None else False,
        "native_input_hash": _rows_hash(native), "invalid_native_opens": rejected,
        "transition_at": seed.end.isoformat(), "first_native_open_at": first.name.isoformat() if first is not None else None,
        "first_native_candle": _json_row(first), "kraken_last_close": seed.last_close,
        "native_open_minus_kraken_close": basis, "abnormal_basis_threshold_kraken_atr14": seed.atr,
        "abnormal_basis": abnormal,
        "current_comparison": _json_row(comparison.loc[expected_close] if expected_close in comparison.index else None),
    }
    return TransitionResult(frame, comparison, evidence)
