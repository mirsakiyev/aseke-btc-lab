"""One scoped, public Kraken BTC/USD receipt producer for paper experiments."""

from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from email.utils import parsedate_to_datetime
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sqlite3
from threading import local
from time import sleep
from types import MappingProxyType

import httpx
import pandas as pd
import psutil

from lab.config import fingerprint
from lab.storage import RUNS, utc_now, writer_lock
from lab.strategy_lab.market_data import MarketData, Observation, ObservationKind, ProvenanceClass, Purpose, SourceGrant
from lab.strategy_lab.spot_sources import (
    INSTRUMENT, KRAKEN_MAX_ROWS, MAX_RAW_BYTES, SpotCandleProvider, SpotCandleReceipt, SpotCandleRequest,
    Venue, inspect_receipt,
)


ROOT = RUNS / "kraken_paper"
SOURCE_ROOT = ROOT / "source"
FILE = "source.sqlite3"
TRADE = ObservationKind.TRADE_OHLCV
CHART_MINUTES = frozenset((15, 240, 1440))
MIN_REQUEST_SPACING = pd.Timedelta(milliseconds=1100)
STORE_LIMIT = 512 * 1024**2
DISK_RESERVE = 2 * 1024**3
MAX_RSS = 700 * 1024**2
MIN_FREE_MEMORY = 768 * 1024**2
METADATA_MAX_AGE = pd.Timedelta(days=1)
METADATA_REFRESH = pd.Timedelta(hours=23)
GAP_RECHECK = pd.Timedelta(seconds=30)
TIMELY_CHART = pd.Timedelta(seconds=120)
TERMS_FIELDS = ("price_tick", "quantity_step", "min_quantity", "min_quote")
SCHEMA = """
CREATE TABLE IF NOT EXISTS raw(sha TEXT PRIMARY KEY, body BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS receipt(
    id TEXT PRIMARY KEY, stream TEXT NOT NULL, minutes INTEGER NOT NULL,
    started INTEGER NOT NULL, received INTEGER NOT NULL, available INTEGER NOT NULL,
    left_edge INTEGER NOT NULL, right_edge INTEGER NOT NULL, http_status INTEGER NOT NULL,
    raw_sha TEXT NOT NULL REFERENCES raw(sha)
);
CREATE TABLE IF NOT EXISTS observation(
    stream TEXT NOT NULL, minutes INTEGER NOT NULL, opened INTEGER NOT NULL, values_json TEXT NOT NULL,
    receipt_id TEXT NOT NULL REFERENCES receipt(id),
    PRIMARY KEY(stream,minutes,opened,values_json)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS checkpoint(
    stream TEXT NOT NULL, minutes INTEGER NOT NULL, boundary INTEGER NOT NULL,
    receipt_id TEXT, complete_rows INTEGER NOT NULL, missing_rows INTEGER NOT NULL,
    state TEXT NOT NULL, recorded INTEGER NOT NULL, PRIMARY KEY(stream,minutes)
);
CREATE TABLE IF NOT EXISTS request_clock(
    stream TEXT PRIMARY KEY, last_started INTEGER NOT NULL, retry_at INTEGER NOT NULL,
    failures INTEGER NOT NULL, last_error TEXT
);
CREATE TABLE IF NOT EXISTS instrument(
    id TEXT PRIMARY KEY, stream TEXT NOT NULL, started INTEGER NOT NULL,
    received INTEGER NOT NULL, available INTEGER NOT NULL, raw_sha TEXT NOT NULL REFERENCES raw(sha),
    state TEXT NOT NULL, terms_json TEXT, reason TEXT
);
CREATE INDEX IF NOT EXISTS instrument_source_time ON instrument(stream,available);
"""


def _utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Kraken source requires timezone-aware clocks")
    return stamp.tz_convert("UTC")


def _stamp(nanoseconds):
    return pd.Timestamp(nanoseconds, unit="ns", tz="UTC")


def _kraken_source(source):
    return (source.provider == Venue.KRAKEN and source.instrument == INSTRUMENT and source.market == "SPOT"
            and source.provenance_class == ProvenanceClass.INDEPENDENT)


@dataclass(frozen=True)
class CollectionScope:
    """Validate the source and the three distinct purposes for each reviewed book."""

    grants: tuple[SourceGrant, ...]
    chart_intervals: dict[str, int]

    def __post_init__(self):
        grants = tuple(self.grants)
        charts = dict(self.chart_intervals)
        if not grants or not charts or not all(isinstance(grant, SourceGrant) for grant in grants):
            raise PermissionError("Kraken collection requires registered, reviewed source grants")
        if not _kraken_source(grants[0].source) or len({g.source.id for g in grants}) != 1:
            raise PermissionError("Kraken collection requires one independently approved BTC/USD Spot source")
        if {g.book_id for g in grants} != set(charts) or set(charts.values()) - CHART_MINUTES:
            raise PermissionError("Kraken grant books and chart cadences do not match")
        for book, minutes in charts.items():
            lanes = [g for g in grants if g.book_id == book]
            by_purpose = {g.purpose: g for g in lanes}
            if (len(lanes) != len(Purpose) or set(by_purpose) != set(Purpose)
                    or any(g.kinds != {TRADE} for g in lanes)
                    or len({g.registered_at for g in lanes}) != 1):
                raise PermissionError("Each Kraken paper book needs separate warmup, forward and execution grants")
            seed, forward, execution = (by_purpose[key] for key in (
                Purpose.WARMUP, Purpose.FORWARD_DECISION, Purpose.EXECUTION_EVIDENCE,
            ))
            step = pd.Timedelta(minutes=minutes)
            if (seed.end != forward.start or not 200 * step <= seed.end - seed.start <= KRAKEN_MAX_ROWS * step
                    or any(boundary != boundary.floor(step) for boundary in (seed.start, seed.end, forward.start))
                    or any(boundary != boundary.floor("min") for boundary in (execution.start, execution.end))):
                raise PermissionError("Kraken grants must define one aligned continuous seed and forward seam")
        object.__setattr__(self, "grants", grants)
        object.__setattr__(self, "chart_intervals", MappingProxyType(charts))

    @property
    def source(self):
        return self.grants[0].source

    def for_interval(self, minutes, at):
        return tuple(g for g in self.grants if g.approved_at <= at and
                     (g.purpose == Purpose.EXECUTION_EVIDENCE if minutes == 1 else
                      g.purpose != Purpose.EXECUTION_EVIDENCE and self.chart_intervals[g.book_id] == minutes))


def _read_connection(root):
    path = Path(root) / FILE
    if not path.is_file():
        return None
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)


@contextmanager
def _writer(root):
    root = Path(root)
    path = root / FILE
    with writer_lock(root / ".kraken-source.lock"):
        _check_budget(root)
        with closing(sqlite3.connect(path, timeout=5)) as db:
            db.execute("PRAGMA foreign_keys=ON")
            db.executescript(SCHEMA)
            for table in ("raw", "receipt", "observation", "instrument"):
                for action in ("UPDATE", "DELETE"):
                    db.execute(f"CREATE TRIGGER IF NOT EXISTS immutable_{table}_{action} BEFORE {action} ON {table} "
                               "BEGIN SELECT RAISE(ABORT,'Kraken receipt evidence is immutable'); END")
            yield db


def _check_budget(root):
    base = Path(root) / FILE
    size = sum(p.stat().st_size for p in (base, base.with_suffix(".sqlite3-journal")) if p.exists())
    if shutil.disk_usage(root).free < DISK_RESERVE or size + 2 * MAX_RAW_BYTES >= STORE_LIMIT:
        raise RuntimeError("Kraken source disk budget exhausted")
    if psutil.virtual_memory().available < MIN_FREE_MEMORY or psutil.Process().memory_info().rss > MAX_RSS:
        raise RuntimeError("Kraken source memory budget exhausted")


class SQLiteSpotProvider:
    """Provide bounded read-only observations with their original receipt clocks."""

    def __init__(self, source, root):
        if not _kraken_source(source):
            raise PermissionError("The Kraken paper store serves only the approved BTC/USD Spot source")
        self.source, self.root = source, Path(root)
        self._reader = local()

    def read(self, kind, start, end, as_of, interval):
        if kind != TRADE or interval not in {pd.Timedelta(minutes=n) for n in (*CHART_MINUTES, 1)}:
            raise ValueError("The Kraken paper store serves only its four Spot candle cadences")
        db = _read_connection(self.root)
        if db is None:
            return
        with closing(db):
            previous = getattr(self._reader, "db", None)
            self._reader.db = db
            try:
                rows = db.execute(
                    "SELECT o.opened,o.values_json,r.received,r.available,r.raw_sha "
                    "FROM observation o JOIN receipt r ON r.id=o.receipt_id "
                    "WHERE o.stream=? AND o.minutes=? AND o.opened>=? AND o.opened<? AND r.available<=? "
                    "ORDER BY o.opened,r.received,r.available,r.raw_sha",
                    (self.source.id, interval // pd.Timedelta(minutes=1), start.value, end.value, as_of.value),
                )
                for opened, values, received, available, digest in rows:
                    at = _stamp(opened)
                    yield Observation(self.source.id, TRADE, at, at + interval, _stamp(received),
                                      _stamp(available), digest, json.loads(values))
            finally:
                self._reader.db = previous

    def raw(self, digest):
        db = getattr(self._reader, "db", None)
        if db is not None:
            return self._raw(db, digest)
        db = _read_connection(self.root)
        if db is None:
            raise KeyError(digest)
        with closing(db):
            return self._raw(db, digest)

    @staticmethod
    def _raw(db, digest):
        result = db.execute("SELECT body FROM raw WHERE sha=?", (digest,)).fetchone()
        if result is None:
            raise KeyError(digest)
        return result[0]


def market_data(grants, *, root=SOURCE_ROOT):
    """Build the existing grant-checked interface for the registered workers."""
    grants = tuple(grants)
    providers = {g.source.id: SQLiteSpotProvider(g.source, root) for g in grants}
    return MarketData(grants, providers)


def _fetch_window(db, scope, minutes, at):
    step = pd.Timedelta(minutes=minutes)
    cutoff = at.floor(step)
    prior = db.execute("SELECT boundary,state,recorded FROM checkpoint WHERE stream=? AND minutes=?",
                       (scope.source.id, minutes)).fetchone()
    if prior:
        same_boundary_retry = (cutoff.value == prior[0] and prior[1] == "GAP"
                               and _stamp(prior[2]) + GAP_RECHECK <= at <= cutoff + TIMELY_CHART)
        if cutoff.value <= prior[0] and not same_boundary_retry:
            return None
    grants = scope.for_interval(minutes, at)
    current = [g for g in grants if g.purpose != Purpose.WARMUP and g.start <= cutoff - step and cutoff <= g.end]
    starts = [max(g.start, cutoff - 2 * step) for g in current]
    for grant in grants:
        if grant.purpose != Purpose.WARMUP:
            continue
        # Prefetch only the already closed part of the approved seed. A long
        # outage can roll its oldest observations beyond Kraken's recent window.
        right = min(grant.end, cutoff)
        left = max(grant.start, cutoff - KRAKEN_MAX_ROWS * step)
        if left >= right or grant.end - left < 200 * step:
            continue
        observed = {row[0] for row in db.execute(
            "SELECT DISTINCT opened FROM observation WHERE stream=? AND minutes=? AND opened>=? AND opened<?",
            (scope.source.id, minutes, left.value, right.value),
        )}
        first_missing = next((opened for opened in range(left.value, right.value, step.value)
                              if opened not in observed), None)
        if first_missing is not None:
            starts.append(_stamp(first_missing))
    if not starts:
        return None
    return SpotCandleRequest(Venue.KRAKEN, minutes, min(starts), cutoff), grants


def _rate_state(db, stream):
    row = db.execute("SELECT last_started,retry_at,failures,last_error FROM request_clock WHERE stream=?",
                     (stream,)).fetchone()
    return row if row is not None else (0, 0, 0, None)


def _reserve_request(db, stream, clock, pause):
    last, retry_at, failures, error = _rate_state(db, stream)
    at = _utc(clock())
    if at.value < retry_at:
        return None
    wait = _stamp(last) + MIN_REQUEST_SPACING - at
    if wait > pd.Timedelta(0):
        pause(wait.total_seconds())
        at = _utc(clock())
        if at.value < last + MIN_REQUEST_SPACING.value:
            return None
    with db:
        db.execute("INSERT OR REPLACE INTO request_clock VALUES(?,?,?,?,?)",
                   (stream, at.value, retry_at, failures, error))
    return at


def _raw_response(client, url, params, headers, clock):
    with client.stream("GET", url, params=params, headers=headers) as response:
        retry_after = response.headers.get("Retry-After")
        if response.status_code != 200:
            raise _EndpointError(f"HTTP {response.status_code}", retry_after)
        raw = bytearray()
        for part in response.iter_bytes():
            if len(raw) + len(part) > MAX_RAW_BYTES:
                raise _EndpointError("Response exceeds the raw receipt budget", retry_after)
            raw.extend(part)
        received = _utc(clock())
    return bytes(raw), received


def _response(client, request, started, clock):
    raw, received = _raw_response(client, request.url, request.params, request.headers, clock)
    return SpotCandleReceipt(request, started, received, _utc(clock()), 200, raw)


class _EndpointError(ValueError):
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


def _backoff(db, stream, failed, error):
    last, _, failures, _ = _rate_state(db, stream)
    delay = pd.Timedelta(seconds=min(900, 15 * 2 ** min(failures, 6)))
    header = getattr(error, "retry_after", None)
    if header:
        try:
            instructed = (pd.Timedelta(seconds=int(header)) if header.isdecimal()
                          else _utc(parsedate_to_datetime(header)) - failed)
            delay = max(delay, instructed)
        except (OverflowError, TypeError, ValueError):
            # Invalid Retry-After leaves the local exponential delay in effect.
            pass
    retry = min(failed.value + delay.value, pd.Timestamp.max.value)
    # Error text is generated locally, so unsuccessful response bodies are discarded.
    message = str(error) if isinstance(error, _EndpointError) else type(error).__name__
    with db:
        db.execute("UPDATE request_clock SET retry_at=?,failures=?,last_error=? WHERE stream=?",
                   (retry, failures + 1, message, stream))
    return {"state": "BACKOFF", "reason": message, "retry_at": _stamp(retry).isoformat(),
            "request_started_at": _stamp(last).isoformat()}


def _record(db, receipt, scope, grants, clock):
    audit = inspect_receipt(receipt)
    start, end = receipt.request.start, receipt.request.end
    minutes, step = receipt.request.interval_minutes, receipt.request.interval
    provider = SpotCandleProvider(scope.source, (receipt,))
    rows = []
    for row in provider.read(TRADE, start, end, receipt.available_at, step):
        if not any(g.start <= row.start and row.end <= g.end and g.approved_at <= row.received_at for g in grants):
            continue
        values = json.dumps(dict(row.values), separators=(",", ":"), sort_keys=True, allow_nan=False)
        key = (scope.source.id, minutes, row.start.value, values)
        if db.execute("SELECT 1 FROM observation WHERE stream=? AND minutes=? AND opened=? AND values_json=?",
                      key).fetchone() is None:
            rows.append(key)
    receipt = replace(receipt, available_at=_utc(clock()))
    with db:
        if rows:
            db.execute("INSERT OR IGNORE INTO raw VALUES(?,?)", (receipt.raw_sha256, receipt.raw))
            db.execute("INSERT OR IGNORE INTO receipt VALUES(?,?,?,?,?,?,?,?,?,?)",
                       (receipt.id, scope.source.id, minutes, receipt.started_at.value, receipt.received_at.value,
                        receipt.available_at.value, start.value, end.value, receipt.status, receipt.raw_sha256))
            db.executemany("INSERT OR IGNORE INTO observation VALUES(?,?,?,?,?)", ((*key, receipt.id) for key in rows))
        state = "GAP" if audit.missing_opens else ("CAPTURED" if rows else "NO_NEW_CANDLES")
        db.execute("INSERT OR REPLACE INTO checkpoint VALUES(?,?,?,?,?,?,?,?)",
                   (scope.source.id, minutes, end.value, receipt.id if rows else None,
                    audit.complete_rows, len(audit.missing_opens), state, receipt.available_at.value))
        db.execute("UPDATE request_clock SET retry_at=0,failures=0,last_error=NULL WHERE stream=?",
                   (scope.source.id,))
    return {"state": state, "closed_through": end.isoformat(), "complete_rows": audit.complete_rows,
            "missing_rows": len(audit.missing_opens), "new_rows": len(rows),
            "received_at": receipt.received_at.isoformat(), "available_at": receipt.available_at.isoformat(),
            "receipt_id": receipt.id if rows else None}


def _latest_instrument(db, stream, as_of):
    return db.execute("SELECT id,received,available,raw_sha,state,terms_json,reason "
                      "FROM instrument WHERE stream=? AND available<=? ORDER BY available DESC,id DESC LIMIT 1",
                      (stream, as_of.value)).fetchone()


def _reset_backoff(db, stream):
    db.execute("UPDATE request_clock SET retry_at=0,failures=0,last_error=NULL WHERE stream=?", (stream,))


def _instrument_result(row, as_of):
    if row is None:
        return {"state": "WAITING", "reason": "No independently received Kraken AssetPairs evidence"}
    key, received, available, digest, state, encoded, reason = row
    if as_of.value - received >= METADATA_MAX_AGE.value and state == "READY":
        state, reason = "STALE", "The Kraken AssetPairs receipt exceeded its 24-hour freshness window"
    return {"state": state, "reason": reason, "receipt_id": key, "raw_sha256": digest,
            "received_at": _stamp(received).isoformat(), "available_at": _stamp(available).isoformat(),
            "terms": json.loads(encoded) if encoded else None}


def instrument_evidence(grants, *, as_of, root=SOURCE_ROOT):
    """Read and verify the latest eligible public terms without making a request."""
    as_of = _utc(as_of)
    identities = {g.source.id for g in grants if _kraken_source(g.source) and g.approved_at <= as_of}
    if len(identities) != 1:
        return {"state": "WAITING", "reason": "No single approved Kraken Spot instrument source"}
    db = _read_connection(root)
    if db is None:
        return _instrument_result(None, as_of)
    with closing(db):
        row = _latest_instrument(db, identities.pop(), as_of)
        if row is not None:
            found = db.execute("SELECT body FROM raw WHERE sha=?", (row[3],)).fetchone()
            if found is None or sha256(found[0]).hexdigest() != row[3]:
                return {"state": "INVALID", "reason": "Kraken AssetPairs raw receipt checksum failed"}
        return _instrument_result(row, as_of)


def _refresh_instrument(db, scope, expected, client, clock, pause):
    from . import catalog

    stream = scope.source.id
    at = _utc(clock())
    prior = _latest_instrument(db, stream, at)
    if prior is not None and (at.value - prior[1] < (
        METADATA_REFRESH if prior[4] == "READY" else pd.Timedelta(minutes=5)
    ).value):
        return _instrument_result(prior, at), False
    started = _reserve_request(db, stream, clock, pause)
    if started is None:
        return _deferred(db, stream), False
    try:
        raw, received = _raw_response(client, catalog.PAIR_PATH, catalog.PAIR_PARAMS,
                                      {"Accept": "application/json", "Cache-Control": "no-cache"}, clock)
    except (httpx.HTTPError, ValueError) as error:
        return _backoff(db, stream, _utc(clock()), error), True
    digest = sha256(raw).hexdigest()
    terms, state, reason = None, "READY", None
    try:
        terms = catalog.instrument_terms(raw, received, _utc(clock()))
        if any(terms[field] != expected[field] for field in TERMS_FIELDS):
            state, reason = "MISMATCH", "Current Kraken AssetPairs terms differ from the frozen execution terms"
    except (AttributeError, KeyError, OverflowError, TypeError, ValueError):
        state, reason = "INVALID", "Kraken AssetPairs did not verify the online BTC/USD instrument"
    available = _utc(clock())
    key = fingerprint(["kraken-paper-assetpairs-v1", stream, started.isoformat(),
                       received.isoformat(), available.isoformat(), digest])
    with db:
        db.execute("INSERT OR IGNORE INTO raw VALUES(?,?)", (digest, raw))
        db.execute("INSERT OR IGNORE INTO instrument VALUES(?,?,?,?,?,?,?,?,?)",
                   (key, stream, started.value, received.value, available.value, digest, state,
                    json.dumps(terms) if terms is not None else None, reason))
        _reset_backoff(db, stream)
    return _instrument_result(_latest_instrument(db, stream, available), available), True


def _deferred(db, stream):
    _, retry, _, error = _rate_state(db, stream)
    return {"state": "BACKOFF" if retry else "WAITING_RATE_LIMIT",
            "retry_at": _stamp(retry).isoformat() if retry else None, "reason": error}


def _collect(scope, root, client, clock, pause, *, instrument_terms=None):
    """Internal transport seam for exercising the scoped producer with fixtures."""
    if not isinstance(scope, CollectionScope):
        raise PermissionError("Kraken source has no validated collection scope")
    at = _utc(clock())
    if not any(grant.approved_at <= at < grant.end for grant in scope.grants):
        return {}
    results = {}
    with _writer(root) as db:
        if instrument_terms is not None:
            current, fetched = _refresh_instrument(db, scope, instrument_terms, client, clock, pause)
            if fetched or current["state"] != "READY":
                results["instrument"] = current
            if current["state"] != "READY":
                return results
        for minutes in (1, *sorted(set(scope.chart_intervals.values()))):
            at = _utc(clock())
            if not any(grant.approved_at <= at < grant.end for grant in scope.grants):
                break
            window = _fetch_window(db, scope, minutes, at)
            if window is None:
                continue
            request, grants = window
            _check_budget(root)
            started = _reserve_request(db, scope.source.id, clock, pause)
            if started is None:
                results[str(minutes)] = _deferred(db, scope.source.id)
                break
            try:
                receipt = _response(client, request, started, clock)
                results[str(minutes)] = _record(db, receipt, scope, grants, clock)
            except (httpx.HTTPError, OverflowError, TypeError, ValueError) as error:
                results[str(minutes)] = _backoff(db, scope.source.id, _utc(clock()), error)
                break
    return results


def collect_registered(*, root=ROOT, source_root=None):
    """The only live entry point. Revalidate local consent and registrations before HTTP."""
    from . import catalog

    at = utc_now()
    with closing(catalog.db_open(readonly=True, root=root)) as db:
        authorization = catalog.approved_authorization(db, at=at)
        registrations = catalog.registrations(db)
        grants = catalog.grants(db)
    if not authorization or not registrations:
        raise PermissionError("Kraken source requires locally validated consent and paper registrations")
    intervals = {row["body"]["book_id"]: row["body"]["execution_clock"]["decision_seconds"] // 60
                 for row in registrations.values()}
    terms = {tuple(row["body"]["instrument_terms"][field] for field in TERMS_FIELDS)
             for row in registrations.values()}
    if len(terms) != 1:
        raise PermissionError("Kraken paper registrations must share verified BTC/USD instrument terms")
    expected = dict(zip(TERMS_FIELDS, terms.pop()))
    scope = CollectionScope(grants, intervals)
    with httpx.Client(timeout=8, follow_redirects=False, headers={"User-Agent": "ASEKE-BTC-Lab-paper-market-data/1.0"}) as client:
        return _collect(scope, Path(root) / "source" if source_root is None else source_root, client, utc_now, sleep,
                        instrument_terms=expected)


def status(*, root=SOURCE_ROOT):
    """Read persisted producer state without touching any venue."""
    db = _read_connection(root)
    if db is None:
        return {"checkpoints": [], "request_clocks": []}
    with closing(db):
        return {
            "checkpoints": [
                {"source_id": stream, "interval_minutes": minutes, "closed_through": _stamp(boundary).isoformat(),
                 "receipt_id": receipt, "complete_rows": complete, "missing_rows": missing, "state": state,
                 "recorded_at": _stamp(recorded).isoformat()}
                for stream, minutes, boundary, receipt, complete, missing, state, recorded
                in db.execute("SELECT * FROM checkpoint ORDER BY stream,minutes")
            ],
            "request_clocks": [
                {"source_id": stream, "last_started": _stamp(started).isoformat(),
                 "retry_at": _stamp(retry).isoformat() if retry else None, "failures": failures, "last_error": error}
                for stream, started, retry, failures, error in db.execute("SELECT * FROM request_clock ORDER BY stream")
            ],
        }
