"""Read-only adapter for an independently reviewed Strategy Lab producer spool.

The upstream producer owns decoding, data-use evidence, and original receipt
retention. Its append-only SQLite spool supplies normalized observations linked
to those immutable receipts. This adapter opens only the explicitly supplied
spool. MarketData separately checks the consumer grant and raw receipt hashes.
"""

from contextlib import closing, contextmanager
import json
from pathlib import Path
import re
import sqlite3

import pandas as pd

from lab.strategy_lab.market_data import (
    CANDLES, MAX_OBSERVATIONS, MAX_RAW_BYTES, MAX_VALUE_BYTES, Observation,
    ObservationKind, ProvenanceClass, SourceIdentity,
)


SPOOL_FILENAME = "market-receipts.sqlite3"
SPOOL_VERSION = "strategy-lab-reviewed-receipt-spool-v1"
SPOOL_SCHEMA = """
CREATE TABLE market_source(
    source_id TEXT PRIMARY KEY, version TEXT NOT NULL, provider TEXT NOT NULL,
    instrument TEXT NOT NULL, market TEXT NOT NULL, rights_evidence_sha256 TEXT NOT NULL
);
CREATE TABLE market_raw(
    sha256 TEXT PRIMARY KEY, body BLOB NOT NULL
);
CREATE TABLE market_observation(
    source_id TEXT NOT NULL REFERENCES market_source(source_id), kind TEXT NOT NULL,
    start_ns INTEGER NOT NULL, end_ns INTEGER NOT NULL, received_ns INTEGER NOT NULL,
    available_ns INTEGER NOT NULL, raw_sha256 TEXT NOT NULL REFERENCES market_raw(sha256),
    values_json TEXT NOT NULL,
    PRIMARY KEY(source_id,kind,start_ns,end_ns,received_ns,available_ns,raw_sha256)
);
CREATE INDEX market_observation_raw ON market_observation(source_id,raw_sha256);
"""
_SHA = re.compile(r"[0-9a-f]{64}")


def _utc_ns(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Receipt spool boundaries require timezone-aware timestamps")
    return stamp.value


class ReviewedSqliteSpool:
    """Expose one reviewed source from a local spool without a writer or poller."""

    def __init__(self, source: SourceIdentity, *, root):
        if not isinstance(source, SourceIdentity) or source.provenance_class != ProvenanceClass.INDEPENDENT:
            raise PermissionError("Only an independently reviewed source may bind a receipt spool")
        self._source = source
        self._root = Path(root).resolve()
        self._path = self._root / SPOOL_FILENAME

    @contextmanager
    def _read_db(self):
        path = self._path.resolve(strict=True)
        if path != self._path or not path.is_file():
            raise PermissionError("Receipt spool must be a regular file at its reviewed local path")
        # A live WAL producer must keep its coordination file available. Readers
        # can otherwise create both files even with SQLite mode=ro.
        with path.open("rb") as handle:
            header = handle.read(20)
        if header[:16] != b"SQLite format 3\0" or len(header) != 20:
            raise ValueError("Receipt spool is not a supported SQLite database")
        if 2 in header[18:20]:
            sidecars = (path.with_name(path.name + suffix) for suffix in ("-wal", "-shm"))
            if any(not sidecar.is_file() or sidecar.resolve() != sidecar for sidecar in sidecars):
                raise LookupError("Receipt spool WAL is unavailable for a read-only connection")
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA trusted_schema=OFF")
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                                  "AND name IN ('market_source','market_raw','market_observation')")}
            if tables != {"market_source", "market_raw", "market_observation"}:
                raise ValueError("Receipt spool does not expose the reviewed table schema")
            expected = (SPOOL_VERSION, self._source.provider, self._source.instrument, self._source.market,
                        self._source.rights_evidence_sha256)
            manifest = db.execute("SELECT 1 FROM market_source WHERE source_id=? AND version=? AND provider=? "
                                  "AND instrument=? AND market=? AND rights_evidence_sha256=?",
                                  (self._source.id, *expected)).fetchone()
            if manifest is None:
                raise PermissionError("Receipt spool manifest differs from the independently reviewed source")
            yield db

    def verify(self):
        """Confirm the independently reviewed source manifest is available read-only."""
        with self._read_db():
            pass

    def read(self, kind, start, end, as_of, interval):
        kind = ObservationKind(kind)
        left, right, cutoff = map(_utc_ns, (start, end, as_of))
        if not left < right <= cutoff:
            raise ValueError("Receipt spool requires a nonempty range within the causal cutoff")
        step = pd.Timedelta(interval).value if interval is not None else 0
        if (kind in CANDLES and step <= 0) or (kind not in CANDLES and interval is not None):
            raise ValueError("Receipt spool intervals must match the requested observation kind")
        with self._read_db() as db:
            rows = db.execute("""
                SELECT start_ns,end_ns,received_ns,available_ns,
                       CASE WHEN typeof(raw_sha256)='text' AND length(raw_sha256)=64 THEN raw_sha256 END,
                       CASE WHEN typeof(values_json)='text' AND length(CAST(values_json AS BLOB))<=?
                            THEN values_json END
                FROM market_observation
                WHERE source_id=? AND kind=? AND start_ns>=? AND start_ns<? AND end_ns<=?
                      AND available_ns<=? AND end_ns-start_ns=?
                ORDER BY start_ns,received_ns,available_ns,raw_sha256 LIMIT ?
            """, (MAX_VALUE_BYTES, self._source.id, kind.value, left, right, right, cutoff, step,
                  MAX_OBSERVATIONS + 1))
            for count, (opened, closed, received, available, raw_hash, values) in enumerate(rows, 1):
                if count > MAX_OBSERVATIONS:
                    raise ValueError("Receipt spool observation budget exceeded")
                if (any(type(clock) is not int for clock in (opened, closed, received, available))
                        or values is None or raw_hash is None):
                    raise ValueError("Receipt spool has invalid UTC clocks or oversized normalized values")
                clocks = (pd.Timestamp(clock, unit="ns", tz="UTC") for clock in (opened, closed, received, available))
                yield Observation(self._source.id, kind, *clocks, raw_hash, json.loads(values))

    def raw(self, digest):
        if not isinstance(digest, str) or not _SHA.fullmatch(digest):
            raise ValueError("Receipt spool requires an original SHA-256")
        with self._read_db() as db:
            row = db.execute("""
                SELECT body FROM market_raw WHERE sha256=? AND typeof(body)='blob' AND length(body) BETWEEN 1 AND ?
                  AND EXISTS(SELECT 1 FROM market_observation WHERE source_id=? AND raw_sha256=?)
            """, (digest, MAX_RAW_BYTES, self._source.id, digest)).fetchone()
        if row is None:
            raise LookupError("Original market receipt is absent, oversized, or outside the reviewed source")
        return row[0]
