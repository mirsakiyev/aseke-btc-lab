"""Feature-only initialization and prospective decision gates for approved sources."""

from dataclasses import dataclass
from hashlib import sha256
from math import isfinite
from pathlib import Path
import json
import re

import pandas as pd

from lab.config import ORIGIN
from lab.data import OHLCV
from lab.storage import immutable_bytes, put_bytes, writer_lock
from lab.strategy_lab import ema_pullback
from lab.strategy_lab.market_data import ObservationKind, Purpose
from lab.strategy_lab.registry import canonical, digest
from lab.strategy_lab.technical_features import (
    MAX_CHART_ROWS, MAX_INDICATORS, FeatureIdentity, IndicatorSpec, build_candles, compute_indicators,
)


VERSION = "feature-warmup-v1"
TRADE = ObservationKind.TRADE_OHLCV
MARK = ObservationKind.MARK_OHLCV
CHECKPOINT_LIMIT = 64 * 1024


def _utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Warmup clocks must be timezone-aware")
    return stamp.tz_convert("UTC")


def _iso(value):
    return value.isoformat()


def _floor(value, step):
    return ORIGIN + (value - ORIGIN).floor(step)


@dataclass(frozen=True)
class WarmupPolicy:
    """Inputs pinned by a separately reviewed experiment registration."""

    registration_id: str
    book_id: str
    registered_at: pd.Timestamp
    seed_grant_id: str
    forward_grant_id: str
    seed_content_id: str
    seed_start: pd.Timestamp
    seed_end: pd.Timestamp
    seed_native_step: pd.Timedelta
    timeframe: pd.Timedelta
    native_step: pd.Timedelta
    indicators: tuple[tuple[str, IndicatorSpec], ...]
    minimum_bars: int
    require_mark: bool = False
    chart_grace: pd.Timedelta = pd.Timedelta(seconds=120)
    native_grace: pd.Timedelta = pd.Timedelta(minutes=17)
    processing: pd.Timedelta = pd.Timedelta(seconds=30)
    publication_grace: pd.Timedelta = pd.Timedelta(minutes=6)
    horizon: pd.Timedelta = pd.Timedelta(days=90)

    def __post_init__(self):
        for field in ("registration_id", "book_id", "seed_grant_id", "forward_grant_id"):
            if not isinstance(value := getattr(self, field), str) or not value.strip():
                raise ValueError(f"Warmup policy requires {field}")
        if self.seed_grant_id == self.forward_grant_id:
            raise ValueError("Warmup and forward decisions require separate grants")
        if not isinstance(self.seed_content_id, str) or re.fullmatch(r"[0-9a-f]{64}", self.seed_content_id) is None:
            raise ValueError("Warmup policy requires the approved seed content hash")
        for field in ("registered_at", "seed_start", "seed_end"):
            object.__setattr__(self, field, _utc(getattr(self, field)))
        for field in ("timeframe", "native_step", "seed_native_step", "chart_grace", "native_grace", "processing",
                      "publication_grace", "horizon"):
            value = pd.Timedelta(getattr(self, field))
            if pd.isna(value) or value <= pd.Timedelta(0):
                raise ValueError(f"{field} must be a positive duration")
            object.__setattr__(self, field, value)
        if self.timeframe % self.native_step or self.timeframe % self.seed_native_step or self.horizon % self.timeframe:
            raise ValueError("Chart and horizon must align with their input grids")
        if (self.seed_end <= self.seed_start or self.seed_start != _floor(self.seed_start, self.timeframe)
                or self.seed_end != _floor(self.seed_end, self.timeframe)
                or (self.seed_end - self.seed_start + self.horizon) // self.timeframe > MAX_CHART_ROWS):
            raise ValueError("Approved seed and horizon must fit the aligned chart budget")
        if self.chart_grace + self.processing > self.publication_grace:
            raise ValueError("Publication window does not allow timely processing")
        if type(self.minimum_bars) is not int or self.minimum_bars < 1 or type(self.require_mark) is not bool:
            raise ValueError("Invalid minimum bar count or mark requirement")
        indicators = tuple(sorted(tuple(self.indicators), key=lambda item: item[0]))
        if (not indicators or len(indicators) > MAX_INDICATORS
                or any(not isinstance(name, str) or not name.strip() or len(name) > 128 or name in OHLCV
                       or not isinstance(spec, IndicatorSpec)
                       for name, spec in indicators)
                or len(dict(indicators)) != len(indicators)):
            raise ValueError("Supply a bounded set of distinct named indicator specifications")
        object.__setattr__(self, "indicators", indicators)

    @property
    def id(self):
        indicators = [(name, spec.name, spec.length, spec.input_series, spec.definition, spec.version)
                      for name, spec in self.indicators]
        durations = {name: getattr(self, name).value for name in
                     ("timeframe", "native_step", "seed_native_step", "chart_grace", "native_grace", "processing",
                      "publication_grace", "horizon")}
        return digest({"version": VERSION, "registration": self.registration_id, "book": self.book_id,
                       "registered_at": _iso(self.registered_at), "seed_grant": self.seed_grant_id,
                       "forward_grant": self.forward_grant_id, "seed_content_id": self.seed_content_id,
                       "seed_start": _iso(self.seed_start), "seed_end": _iso(self.seed_end), "durations_ns": durations,
                       "minimum_bars": self.minimum_bars, "require_mark": self.require_mark,
                       "indicators": indicators})


def ema_indicators():
    """The existing EMA Pullback recipe's reviewed indicator parameters."""
    entry, exit_rules = ema_pullback.ENTRY_PARAMETERS, ema_pullback.EXIT_PARAMETERS
    return (
        ("ema_fast", IndicatorSpec("ema", entry["ema_fast"])),
        ("ema_slow", IndicatorSpec("ema", entry["ema_slow"])),
        ("rsi", IndicatorSpec("rsi", entry["rsi_length"])),
        ("atr", IndicatorSpec("atr", entry["atr_length"])),
        ("volume_sma", IndicatorSpec("sma", entry["volume_length"])),
        ("swing_high", IndicatorSpec("rolling_high", exit_rules["swing_bars"])),
        ("swing_low", IndicatorSpec("rolling_low", exit_rules["swing_bars"])),
    )


@dataclass(frozen=True)
class WarmupSeed:
    policy_id: str
    snapshot_id: str
    start: pd.Timestamp
    end: pd.Timestamp
    initialized_at: pd.Timestamp
    first_provider_receipt_at: pd.Timestamp
    last_provider_receipt_at: pd.Timestamp
    source_available_at: pd.Timestamp
    bars: int
    values: tuple[tuple[str, float], ...]

    def body(self):
        return {"type": "seed", "version": VERSION, "policy_id": self.policy_id,
                "snapshot_id": self.snapshot_id, "start": _iso(self.start), "end": _iso(self.end),
                "initialized_at": _iso(self.initialized_at),
                "first_provider_receipt_at": _iso(self.first_provider_receipt_at),
                "last_provider_receipt_at": _iso(self.last_provider_receipt_at),
                "source_available_at": _iso(self.source_available_at),
                "bars": self.bars, "values": dict(self.values)}

    @property
    def id(self):
        return digest(self.body())


@dataclass(frozen=True)
class ForwardEligibility:
    seed_id: str
    bridge_snapshot_ids: tuple[str, ...]
    recorded_at: pd.Timestamp
    first_eligible_at: pd.Timestamp
    primary_end_exclusive: pd.Timestamp

    def body(self):
        return {"type": "eligibility", "version": VERSION, "seed_id": self.seed_id,
                "bridge_snapshot_ids": list(self.bridge_snapshot_ids), "recorded_at": _iso(self.recorded_at),
                "first_eligible_at": _iso(self.first_eligible_at),
                "primary_end_exclusive": _iso(self.primary_end_exclusive)}

    @property
    def id(self):
        return digest(self.body())


@dataclass(frozen=True)
class DecisionFeatures:
    decision_at: pd.Timestamp
    evidence_available_at: pd.Timestamp
    snapshot_id: str
    current: tuple[tuple[str, float], ...]
    previous: tuple[tuple[str, float], ...]


def _grants(data, policy):
    seed = data.grant(policy.seed_grant_id)
    forward = data.grant(policy.forward_grant_id)
    for grant in (seed, forward):
        if grant.book_id != policy.book_id or grant.registered_at != policy.registered_at:
            raise ValueError("Source grant does not bind the registered experiment")
    if seed.purpose != Purpose.WARMUP or forward.purpose != Purpose.FORWARD_DECISION:
        raise ValueError("The source grants must separately authorize warmup and forward decisions")
    for field in ("provider", "market", "instrument"):
        if getattr(seed.source, field) != getattr(forward.source, field):
            raise ValueError("Seed and forward stream must describe the same venue market and instrument")
    if policy.require_mark and forward.source.market != "FUTURES":
        raise ValueError("Mark-price requirement is only supported for futures")
    return seed, forward


def _read(data, policy, grant_id, purpose, kind, start, end, as_of, native_step):
    return data.read(grant_id=grant_id, book_id=policy.book_id, purpose=purpose, kind=kind,
                     start=start, end=end, as_of=as_of, interval=native_step)


def _batch(snapshot, policy, native_step):
    batch = build_candles(snapshot.frame(), timeframe=policy.timeframe, native_step=native_step,
                          as_of=snapshot.as_of, start=snapshot.start, end=snapshot.end)
    if not snapshot.complete or not batch.complete:
        raise ValueError("A complete contiguous candle interval is required")
    return batch.bars


def _features(bars, policy, source, identity, cache):
    specs = dict(policy.indicators)
    if cache is None:
        return compute_indicators(bars, timeframe=policy.timeframe, indicators=specs)
    context = FeatureIdentity(source.id, identity, source.instrument, source.market, policy.timeframe)
    return cache.compute(bars, identity=context, indicators=specs)


def _values(row):
    values = tuple(sorted((name, float(value)) for name, value in row.items()))
    if not all(isfinite(value) for _, value in values):
        raise ValueError("Required indicators are undefined at the last completed candle")
    return values


def _seed_data(data, policy, as_of, cache):
    start, end = policy.seed_start, policy.seed_end
    seed_grant, forward_grant = _grants(data, policy)
    if not policy.registered_at <= as_of or end > as_of or end < forward_grant.start:
        raise ValueError("Seed must be closed by initialization and end within the forward grant's clock")
    if end + policy.timeframe > forward_grant.end:
        raise ValueError("Forward grant cannot cover a complete bridge after the seed")
    if seed_grant.source.market == "SPOT" and policy.seed_native_step != policy.native_step:
        raise ValueError("Spot warmup must use the registered native candle interval")
    snapshot = _read(data, policy, policy.seed_grant_id, Purpose.WARMUP, TRADE,
                     start, end, as_of, policy.seed_native_step)
    bars = _batch(snapshot, policy, policy.seed_native_step)
    if snapshot.content_id != policy.seed_content_id:
        raise ValueError("Historical evidence does not match the approved seed content hash")
    if len(bars) < policy.minimum_bars:
        raise ValueError(f"Warmup requires {policy.minimum_bars} completed chart candles")
    features = _features(bars, policy, snapshot.source, snapshot.content_id, cache)
    seed = WarmupSeed(policy.id, snapshot.id, start, end, as_of,
                      min(row.received_at for row in snapshot.observations),
                      max(row.received_at for row in snapshot.observations),
                      bars.available_at.max(), len(bars), _values(features.iloc[-1]))
    return seed, snapshot.content_id, bars


def freeze_seed(data, policy, *, as_of, cache=None):
    """Validate archived receipts and freeze the last indicator state without evaluating signals."""
    return _seed_data(data, policy, _utc(as_of), cache)[0]


def _timely_forward(data, policy, kind, start, end, as_of):
    snapshot = _read(data, policy, policy.forward_grant_id, Purpose.FORWARD_DECISION, kind,
                     start, end, as_of, policy.native_step)
    if not snapshot.complete:
        raise ValueError("Forward evidence is missing a native candle")
    for row in snapshot.observations:
        chart_end = _floor(row.end - pd.Timedelta(1, unit="ns"), policy.timeframe) + policy.timeframe
        if row.available_at > min(row.end + policy.native_grace, chart_end + policy.chart_grace):
            raise ValueError("Forward candle missed the preregistered receipt deadline")
    return snapshot


def _forward(data, policy, start, end, as_of):
    # Keep reads below the receipt budget even across a full 90-day minute horizon.
    block_bars = max(1, pd.Timedelta(days=7) // policy.timeframe)
    block = block_bars * policy.timeframe
    ids, content, frames = [], [], []
    left = start
    while left < end:
        right = min(left + block, end)
        trade = _timely_forward(data, policy, TRADE, left, right, as_of)
        ids.append(trade.id)
        content.append(trade.content_id)
        frames.append(_batch(trade, policy, policy.native_step))
        left = right
    available = max(frame.available_at.max() for frame in frames)
    if policy.require_mark:
        mark = _timely_forward(data, policy, MARK, end - policy.timeframe, end, as_of)
        ids.append(mark.id)
        available = max(available, max(row.available_at for row in mark.observations))
    if as_of < max(end, available) + policy.processing:
        raise ValueError("Processing allowance after forward evidence has not elapsed")
    return tuple(ids), digest(content), pd.concat(frames), available


def _qualify(data, policy, seed, as_of):
    if seed.policy_id != policy.id or as_of < seed.initialized_at:
        raise ValueError("Seed is outside this registration or not yet initialized")
    bridge_end = seed.end + policy.timeframe
    if as_of > bridge_end + policy.publication_grace:
        raise ValueError("The registered bridge observation window elapsed")
    ids, _, _, _ = _forward(data, policy, seed.end, bridge_end, as_of)
    first = _floor(as_of, policy.timeframe) + policy.timeframe
    end = first + policy.horizon
    if end > data.grant(policy.forward_grant_id).end:
        raise ValueError("Forward grant must cover the entire prospective horizon")
    return ForwardEligibility(seed.id, ids, as_of, first, end)


def qualify_bridge(data, policy, seed, *, as_of, cache=None):
    """Prove one complete new chart bar and freeze a strictly future feature-eligibility clock."""
    if freeze_seed(data, policy, as_of=seed.initialized_at, cache=cache) != seed:
        raise ValueError("Seed evidence does not match the checkpoint")
    return _qualify(data, policy, seed, _utc(as_of))


def decision_features(data, policy, seed, eligibility, *, decision_at, as_of, cache=None):
    """Return current and prior forward-bar features, with no historical rows or signals."""
    decision_at, as_of = _utc(decision_at), _utc(as_of)
    if (decision_at != _floor(decision_at, policy.timeframe)
            or not eligibility.first_eligible_at <= decision_at < eligibility.primary_end_exclusive
            or not decision_at <= as_of <= decision_at + policy.publication_grace):
        raise ValueError("Decision falls outside the frozen prospective or publication window")
    original, history_id, history = _seed_data(data, policy, seed.initialized_at, cache)
    if original != seed or _qualify(data, policy, original, eligibility.recorded_at) != eligibility:
        raise ValueError("Seed or bridge evidence does not match the checkpoint")
    if decision_at < seed.end + 2 * policy.timeframe:
        raise ValueError("Decision requires current and prior forward bars")
    ids, forward_id, new_bars, available = _forward(data, policy, seed.end, decision_at, as_of)
    bars = pd.concat([history, new_bars])
    identity = digest([history_id, forward_id])
    source = data.grant(policy.forward_grant_id).source
    features = _features(bars, policy, source, identity, cache).iloc[-2:]
    output = features.join(new_bars[OHLCV])
    return DecisionFeatures(decision_at, available, digest([seed.id, *ids]),
                            _values(output.iloc[-1]), _values(output.iloc[-2]))


def save_checkpoint(directory, evidence):
    """Content-address a feature artifact in a caller-supplied review or experiment directory."""
    if not isinstance(evidence, (WarmupSeed, ForwardEligibility)):
        raise ValueError("Expected a feature seed or eligibility checkpoint")
    payload = canonical(evidence.body()).encode("utf-8")
    if len(payload) > CHECKPOINT_LIMIT:
        raise ValueError("Feature checkpoint exceeds the byte limit")
    directory = Path(directory)
    identifier = immutable_bytes(directory, payload, "json")
    if load_checkpoint(directory, identifier) != evidence:
        raise ValueError("Feature checkpoint failed round-trip verification")
    return identifier


def load_checkpoint(directory, identifier):
    """Recover a checksum-verified artifact. Decision use still revalidates the source and bridge."""
    if not isinstance(identifier, str) or re.fullmatch(r"[0-9a-f]{64}", identifier) is None:
        raise ValueError("Invalid checkpoint identifier")
    with (Path(directory) / f"{identifier}.json").open("rb") as stream:
        payload = stream.read(CHECKPOINT_LIMIT + 1)
    if len(payload) > CHECKPOINT_LIMIT:
        raise ValueError("Feature checkpoint exceeds the byte limit")
    if sha256(payload).hexdigest() != identifier:
        raise ValueError("Feature checkpoint checksum or version failed")
    body = json.loads(payload)
    if not isinstance(body, dict) or body.get("version") != VERSION:
        raise ValueError("Feature checkpoint checksum or version failed")
    if body["type"] == "seed":
        return WarmupSeed(body["policy_id"], body["snapshot_id"], _utc(body["start"]), _utc(body["end"]),
                          _utc(body["initialized_at"]), _utc(body["first_provider_receipt_at"]),
                          _utc(body["last_provider_receipt_at"]), _utc(body["source_available_at"]),
                          body["bars"], tuple(sorted(body["values"].items())))
    if body["type"] == "eligibility":
        return ForwardEligibility(body["seed_id"], tuple(body["bridge_snapshot_ids"]), _utc(body["recorded_at"]),
                                  _utc(body["first_eligible_at"]), _utc(body["primary_end_exclusive"]))
    raise ValueError("Unknown feature checkpoint type")


def record_eligibility(directory, data, policy, seed, *, as_of, cache=None):
    """Record one eligibility checkpoint per registration and recover it on retry."""
    directory, as_of = Path(directory), _utc(as_of)
    key = digest([policy.registration_id, policy.book_id])
    pointer = directory / f"eligibility-{key}.txt"
    with writer_lock(pointer.with_suffix(".lock")):
        if pointer.is_file():
            with pointer.open("rb") as stream:
                identifier = stream.read(65).decode("ascii")
            evidence = load_checkpoint(directory, identifier)
            if (not isinstance(evidence, ForwardEligibility) or evidence.recorded_at > as_of
                    or qualify_bridge(data, policy, seed, as_of=evidence.recorded_at, cache=cache) != evidence):
                raise ValueError("Stored eligibility does not match this registration and seed")
            return evidence
        evidence = qualify_bridge(data, policy, seed, as_of=as_of, cache=cache)
        save_checkpoint(directory, seed)
        identifier = save_checkpoint(directory, evidence)
        put_bytes(pointer, identifier.encode("ascii"))
        return evidence
