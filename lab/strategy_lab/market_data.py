"""Permission-checked reads from an independently reviewed market-data provider.

The composition root supplies reviewed grants and provider adapters. This module
has no way to grant itself access, open a production store, or poll a venue.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from hashlib import sha256
import json
from math import isfinite
import re
from types import MappingProxyType
from typing import Protocol

import pandas as pd

from lab.config import ORIGIN, fingerprint
from lab.data import OHLCV


MAX_OBSERVATIONS = 100_000
MAX_RAW_BYTES = 2 * 1024**2
MAX_VALUE_BYTES = 16 * 1024
MAX_READ_BYTES = 64 * 1024**2
PRICES = tuple(OHLCV[:4])


class ObservationKind(StrEnum):
    TRADE_OHLCV = "trade_ohlcv"
    MARK_OHLCV = "mark_ohlcv"
    FUNDING_SETTLEMENT = "funding_settlement"
    CONTRACT_SPEC = "contract_spec"


class Purpose(StrEnum):
    WARMUP = "warmup"
    FORWARD_DECISION = "forward_decision"
    EXECUTION_EVIDENCE = "execution_evidence"


class ProvenanceClass(StrEnum):
    INDEPENDENT = "independent"
    PROTECTED = "protected"
    UNVERIFIED = "unverified"


CANDLES = frozenset((ObservationKind.TRADE_OHLCV, ObservationKind.MARK_OHLCV))
FUTURES_ONLY = frozenset((ObservationKind.MARK_OHLCV, ObservationKind.FUNDING_SETTLEMENT))


def _timestamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Market data requires a timestamp with an explicit UTC offset")
    return stamp.tz_convert("UTC")


def _text(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 1000:
        raise ValueError("Market data identity and rights fields must be nonempty text")
    return value


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Market data evidence requires a lowercase SHA-256 digest")
    return value


def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _number(value):
    return type(value) in (float, int) and isfinite(value)


def _value_document(kind, values):
    if not isinstance(values, Mapping) or not values:
        raise ValueError("A normalized observation requires values")
    values = dict(values)
    if kind in CANDLES:
        if any(not _number(values.get(key)) or values[key] <= 0 for key in PRICES):
            raise ValueError("Candle prices must be finite and positive")
        opening, high, low, closing = (values[key] for key in PRICES)
        if low > min(opening, closing) or high < max(opening, closing):
            raise ValueError("Candle OHLC ordering is invalid")
        if kind == ObservationKind.TRADE_OHLCV and (
            not _number(values.get("volume")) or values["volume"] < 0
        ):
            raise ValueError("Trade candle volume must be observed, finite and nonnegative")
        if kind == ObservationKind.MARK_OHLCV and "volume" in values:
            raise ValueError("Mark candles do not carry trade volume")
        for key in OHLCV if kind == ObservationKind.TRADE_OHLCV else PRICES:
            values[key] = float(values[key])
    if kind == ObservationKind.FUNDING_SETTLEMENT:
        if not _number(values.get("rate")):
            raise ValueError("A funding settlement requires an observed finite rate")
        values["rate"] = float(values["rate"])
    document = json.dumps(values, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(document) > MAX_VALUE_BYTES:
        raise ValueError("Normalized observation exceeds its size bound")
    return document


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """A reviewed provider namespace and its independent data-use evidence."""

    provider: str
    source_id: str
    instrument: str
    market: str
    rights_reference: str
    rights_evidence_sha256: str
    provenance_class: ProvenanceClass
    id: str = field(init=False)

    def __post_init__(self):
        if self.market not in {"SPOT", "FUTURES"}:
            raise ValueError("Market must be SPOT or FUTURES")
        provenance = ProvenanceClass(self.provenance_class)
        object.__setattr__(self, "provenance_class", provenance)
        identity = [
            "strategy-lab-source-v1", _text(self.provider), _text(self.source_id), _text(self.instrument),
            self.market, _text(self.rights_reference), _digest(self.rights_evidence_sha256), provenance,
        ]
        object.__setattr__(self, "id", fingerprint(identity))


@dataclass(frozen=True, slots=True)
class SourceGrant:
    """One immutable approval for a book, purpose, source, roles and date range."""

    source: SourceIdentity
    book_id: str
    purpose: Purpose
    kinds: frozenset[ObservationKind]
    start: pd.Timestamp
    end: pd.Timestamp
    approved_at: pd.Timestamp
    registered_at: pd.Timestamp
    approval_reference: str
    id: str = field(init=False)

    def __post_init__(self):
        if not isinstance(self.source, SourceIdentity) or self.source.provenance_class != ProvenanceClass.INDEPENDENT:
            raise PermissionError("Protected or unverified evidence is outside this grant interface")
        purpose = Purpose(self.purpose)
        kinds = frozenset(ObservationKind(kind) for kind in self.kinds)
        if not kinds or (self.source.market == "SPOT" and kinds & FUTURES_ONLY):
            raise ValueError("Observation kinds must match the granted market")
        if purpose == Purpose.WARMUP and kinds != {ObservationKind.TRADE_OHLCV}:
            raise ValueError("Warmup grants permit trade candles for features only")
        start, end, approved, registered = map(_timestamp, (self.start, self.end, self.approved_at, self.registered_at))
        if start >= end or approved < registered:
            raise ValueError("Grant range and registration/approval clocks are invalid")
        if purpose != Purpose.WARMUP and start < approved:
            raise PermissionError("A prospective grant cannot cover data before approval and registration")
        object.__setattr__(self, "purpose", purpose)
        object.__setattr__(self, "kinds", kinds)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "approved_at", approved)
        object.__setattr__(self, "registered_at", registered)
        identity = [
            "strategy-lab-grant-v1", self.source.id, _text(self.book_id), purpose, sorted(kinds),
            start.isoformat(), end.isoformat(), approved.isoformat(), registered.isoformat(),
            _text(self.approval_reference),
        ]
        object.__setattr__(self, "id", fingerprint(identity))


@dataclass(frozen=True, slots=True)
class Observation:
    """A normalized value linked to its actual first receipt and local availability."""

    source_id: str
    kind: ObservationKind
    start: pd.Timestamp
    end: pd.Timestamp
    received_at: pd.Timestamp
    available_at: pd.Timestamp
    raw_sha256: str
    values: Mapping
    _document: str = field(init=False, repr=False)

    def __post_init__(self):
        _digest(self.source_id)
        _digest(self.raw_sha256)
        kind = ObservationKind(self.kind)
        start, end, received, available = map(_timestamp, (self.start, self.end, self.received_at, self.available_at))
        if end > received or received > available or (start >= end if kind in CANDLES else start != end):
            raise ValueError("Observation clocks must identify closed data and actual causal availability")
        document = _value_document(kind, self.values)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "values", _freeze(json.loads(document)))
        object.__setattr__(self, "_document", document)


class ObservationProvider(Protocol):
    """A read-only adapter for a reviewed source.

    The reviewed adapter verifies venue, instrument and basis while decoding the
    original receipt and binds each row to the actual acquisition/handoff clocks.
    Trade volume is in base-asset units, using dated contract terms for futures.
    The reader checks raw hashes and applies its own scope and causal filters.
    Hash verification alone cannot authenticate a decoder or attest venue rights.
    """

    def read(self, kind: ObservationKind, start: pd.Timestamp, end: pd.Timestamp,
             as_of: pd.Timestamp, interval: pd.Timedelta | None) -> Iterable[Observation]: ...

    def raw(self, digest: str) -> bytes: ...


@dataclass(frozen=True, slots=True)
class ObservationSnapshot:
    source: SourceIdentity
    grant_id: str
    book_id: str
    purpose: Purpose
    kind: ObservationKind
    start: pd.Timestamp
    end: pd.Timestamp
    as_of: pd.Timestamp
    observations: tuple[Observation, ...]
    missing_opens: tuple[pd.Timestamp, ...]
    interval: pd.Timedelta | None
    content_id: str
    id: str

    @property
    def complete(self):
        """Candle coverage only. Event completeness requires a separate schedule."""
        return self.interval is not None and not self.missing_opens

    def frame(self):
        if self.kind not in CANDLES:
            raise ValueError("Only candle snapshots can be converted to an OHLC frame")
        fields = OHLCV if self.kind == ObservationKind.TRADE_OHLCV else PRICES
        index = pd.DatetimeIndex([row.start for row in self.observations], tz="UTC", name="open_at")
        values = {key: [row.values[key] for row in self.observations] for key in fields}
        return pd.DataFrame({
            **values, "close_at": pd.DatetimeIndex([row.end for row in self.observations], tz="UTC"),
            "received_at": pd.DatetimeIndex([row.received_at for row in self.observations], tz="UTC"),
            "available_at": pd.DatetimeIndex([row.available_at for row in self.observations], tz="UTC"),
            "raw_sha256": [row.raw_sha256 for row in self.observations],
        }, index=index)


class MarketData:
    """Bounded per-book reads over shared local providers and reviewed grants."""

    def __init__(self, grants: Iterable[SourceGrant], providers: Mapping[str, ObservationProvider],
                 *, max_observations=MAX_OBSERVATIONS):
        if type(max_observations) is not int or not 1 <= max_observations <= MAX_OBSERVATIONS:
            raise ValueError("Observation budget exceeds the hard limit")
        indexed = {}
        for grant in grants:
            if not isinstance(grant, SourceGrant):
                raise ValueError("Market data requires immutable reviewed source grants")
            indexed[grant.id] = grant
        self._grants = MappingProxyType(indexed)
        self._providers = MappingProxyType(dict(providers))
        self._limit = max_observations

    def grant(self, grant_id):
        """Return the immutable, externally reviewed grant record."""
        if grant_id not in self._grants:
            raise PermissionError("No reviewed market-data grant has this identifier")
        return self._grants[grant_id]

    def read(self, grant_id, book_id, purpose, kind, start, end, as_of, interval=None):
        purpose, kind = Purpose(purpose), ObservationKind(kind)
        start, end, as_of = map(_timestamp, (start, end, as_of))
        grant = self.grant(grant_id)
        if (grant.book_id != book_id or grant.purpose != purpose or kind not in grant.kinds
                or not grant.start <= start < end <= grant.end or not grant.approved_at <= as_of):
            raise PermissionError("Read is outside the reviewed book, purpose, role or time grant")
        if end > as_of:
            raise ValueError("A market-data read cannot extend beyond its as-of clock")
        interval = self._checked_interval(kind, interval, start, end)
        provider = self._providers.get(grant.source.id)
        if provider is None:
            raise LookupError("No reviewed provider is configured for the authorized source")
        rows = self._visible_rows(provider, grant.source.id, kind, start, end, as_of, interval)
        gaps = tuple(at for at in pd.date_range(start, end, freq=interval, inclusive="left") if at not in rows) if interval else ()
        observations = tuple(rows[at] for at in sorted(rows))
        content_id = self._content_id(grant.source, kind, start, end, interval, observations)
        identity = fingerprint(["strategy-lab-snapshot-v1", content_id, grant.id, as_of.isoformat()])
        return ObservationSnapshot(grant.source, grant.id, book_id, purpose, kind, start, end, as_of,
                                   observations, gaps, interval, content_id, identity)

    def _checked_interval(self, kind, interval, start, end):
        if kind not in CANDLES:
            if interval is not None:
                raise ValueError("A point observation has no candle interval")
            return None
        step = pd.Timedelta(interval)
        if (pd.isna(step) or step <= pd.Timedelta(0) or (start - ORIGIN) % step
                or (end - ORIGIN) % step or (end - start) // step > self._limit):
            raise ValueError("Candle range requires aligned UTC boundaries within the observation budget")
        return step

    def _visible_rows(self, provider, source_id, kind, start, end, as_of, interval):
        rows = {}
        checked = set()
        total = 0
        for count, row in enumerate(provider.read(kind, start, end, as_of, interval), 1):
            if count > self._limit:
                raise ValueError("Provider exceeded the observation budget")
            if not isinstance(row, Observation):
                raise ValueError("Provider returned an invalid observation")
            if row.available_at > as_of:
                continue
            if row.source_id != source_id or row.kind != kind or not start <= row.start < end or row.end > end:
                raise ValueError("Provider returned an observation outside the requested source or range")
            if interval is not None and (row.end - row.start != interval or (row.start - start) % interval):
                raise ValueError("Provider returned an incomplete or misaligned native candle")
            total += len(row._document)
            if row.raw_sha256 not in checked:
                raw = provider.raw(row.raw_sha256)
                if type(raw) is not bytes or not 0 < len(raw) <= MAX_RAW_BYTES or sha256(raw).hexdigest() != row.raw_sha256:
                    raise ValueError("Original market receipt failed raw hash or size verification")
                checked.add(row.raw_sha256)
                total += len(raw)
            if total > MAX_READ_BYTES:
                raise ValueError("Provider exceeded the market-data read byte budget")
            prior = rows.get(row.start)
            if prior is not None and prior._document != row._document:
                raise ValueError("Conflicting observations for the same source timestamp")
            first = (row.received_at, row.available_at, row.raw_sha256)
            if prior is None or first < (prior.received_at, prior.available_at, prior.raw_sha256):
                rows[row.start] = row
        return rows

    @staticmethod
    def _content_id(source, kind, start, end, interval, observations):
        digest = sha256()
        header = ["strategy-lab-content-v1", source.id, kind, start.isoformat(), end.isoformat(), str(interval)]
        digest.update(bytes.fromhex(fingerprint(header)))
        for row in observations:
            evidence = [row.start.isoformat(), row.end.isoformat(), row.received_at.isoformat(),
                        row.available_at.isoformat(), row.raw_sha256, row._document]
            digest.update(bytes.fromhex(fingerprint(evidence)))
        return digest.hexdigest()
