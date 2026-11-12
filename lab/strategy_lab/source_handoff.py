"""Book-scoped receipt delivery over the shared, read-only MarketData interface."""

from dataclasses import dataclass

import pandas as pd

from lab.config import ORIGIN, fingerprint
from lab.strategy_lab.market_data import CANDLES, MarketData, ObservationKind, ObservationSnapshot, ProvenanceClass, Purpose


_VERSION = "strategy-lab-receipt-handoff-v1"
_CURSOR_TABLE = "strategy_source_cursor"
_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {_CURSOR_TABLE}(
    consumer TEXT PRIMARY KEY,
    revision INTEGER NOT NULL CHECK(revision > 0),
    next_at TEXT NOT NULL,
    as_of TEXT NOT NULL,
    batch_id TEXT NOT NULL
)
"""


def _utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Handoff clocks require timezone-aware timestamps")
    return stamp.tz_convert("UTC")


def install_handoff_schema(db):
    """Install cursor state in the consumer's own SQLite database."""
    db.execute(_SCHEMA)


@dataclass(frozen=True, slots=True)
class ReceiptCursor:
    revision: int
    next_at: pd.Timestamp
    as_of: pd.Timestamp
    batch_id: str


@dataclass(frozen=True, slots=True)
class ReceiptBatch:
    consumer_id: str
    prior: ReceiptCursor | None
    snapshot: ObservationSnapshot
    missing_at: pd.Timestamp | None

    @property
    def through(self):
        """Exclusive end of the verified contiguous prefix."""
        return self.snapshot.end if self.missing_at is None else self.missing_at

    @property
    def observations(self):
        return tuple(row for row in self.snapshot.observations if row.start < self.through)

    @property
    def id(self):
        return fingerprint([_VERSION, self.consumer_id, self.snapshot.id,
                            self.through.isoformat(), self.prior.revision if self.prior else 0])


class ReceiptHandoff:
    """One strategy consumer of one independently authorized role and timeframe.

    A consumer may replay a read for feature evidence without moving its cursor.
    Use pending/acknowledge for durable delivery, with the acknowledgement and
    effects in the same SQLite transaction. External delivery belongs in the
    consumer's existing durable outbox.
    """

    def __init__(self, market_data, *, consumer_id, book_id, grant_id, purpose, kind, interval=None):
        if not isinstance(market_data, MarketData):
            raise TypeError("Receipt handoff requires the reviewed MarketData interface")
        if not isinstance(consumer_id, str) or not consumer_id.strip() or len(consumer_id) > 1_000:
            raise ValueError("Receipt handoff requires a stable consumer identifier")
        purpose, kind = Purpose(purpose), ObservationKind(kind)
        grant = market_data.grant(grant_id)
        if (purpose not in {Purpose.FORWARD_DECISION, Purpose.EXECUTION_EVIDENCE}
                or grant.book_id != book_id or grant.purpose != purpose or kind not in grant.kinds
                or grant.source.provenance_class != ProvenanceClass.INDEPENDENT):
            raise PermissionError("Handoff requires an independent prospective grant for this exact book and role")
        step = pd.Timedelta(interval) if interval is not None else None
        if (kind in CANDLES and (step is None or pd.isna(step) or step <= pd.Timedelta(0))
                or kind not in CANDLES and step is not None):
            raise ValueError("Receipt handoff interval must match the observation kind")
        self._data = market_data
        self.grant = grant
        self.kind = kind
        self.interval = step
        self.id = fingerprint([_VERSION, consumer_id, grant.id, kind, str(step)])

    def read(self, start, end, as_of):
        """Read one bounded grant-checked snapshot without moving any cursor."""
        return self._data.read(self.grant.id, self.grant.book_id, self.grant.purpose,
                               self.kind, start, end, as_of, self.interval)

    def cursor(self, db):
        row = db.execute(f"SELECT revision,next_at,as_of,batch_id FROM {_CURSOR_TABLE} WHERE consumer=?",
                         (self.id,)).fetchone()
        return ReceiptCursor(row[0], _utc(row[1]), _utc(row[2]), row[3]) if row else None

    def pending(self, db, *, end, as_of, start=None, expected_points=None):
        """Prepare a batch without advancing it, stopping before any missing receipt.

        Point observations need an explicit expected schedule to establish a
        complete interval. Unscheduled metadata is available through read().
        """
        prior = self.cursor(db)
        end, as_of = _utc(end), _utc(as_of)
        left = self.grant.start
        if self.interval is not None:
            left = ORIGIN + (left - ORIGIN).ceil(self.interval)
        start = _utc(start) if start is not None else (prior.next_at if prior else left)
        if not (left <= start <= self.grant.end and left <= end <= self.grant.end
                and self.grant.approved_at <= as_of):
            raise PermissionError("Handoff cursor range exceeds the prospective grant")
        if prior is not None and (start != prior.next_at or as_of < prior.as_of):
            raise ValueError("Handoff start or as-of clock cannot move away from its durable cursor")
        if end > as_of or (self.interval is not None and ((start - ORIGIN) % self.interval or (end - ORIGIN) % self.interval)):
            raise ValueError("Handoff range must use causal and aligned UTC boundaries")
        if end <= start:
            return None
        if self.kind in CANDLES and expected_points is not None:
            raise ValueError("Candle handoff uses the bound interval for completeness")
        if self.kind not in CANDLES and expected_points is None:
            raise ValueError("Sparse handoff delivery requires explicit expected point timestamps")
        snapshot = self.read(start, end, as_of)
        if self.kind in CANDLES:
            missing = snapshot.missing_opens[0] if snapshot.missing_opens else None
        else:
            points = [_utc(at) for at in expected_points]
            expected = set(points)
            present = {row.start for row in snapshot.observations}
            if len(points) != len(expected) or any(not start <= at < end for at in expected) or present - expected:
                raise ValueError("Point observations do not match the expected handoff schedule")
            missing = min(expected - present, default=None)
        return ReceiptBatch(self.id, prior, snapshot, missing)

    def acknowledge(self, db, batch):
        """Compare-and-set the cursor. Process effects only when this returns True."""
        if not isinstance(batch, ReceiptBatch) or batch.consumer_id != self.id:
            raise PermissionError("Receipt batch is bound to a different consumer")
        snapshot = batch.snapshot
        if (snapshot.source.id != self.grant.source.id or snapshot.grant_id != self.grant.id
                or snapshot.book_id != self.grant.book_id or snapshot.purpose != self.grant.purpose
                or snapshot.kind != self.kind or snapshot.interval != self.interval
                or not self.grant.start <= snapshot.start <= batch.through <= snapshot.end <= self.grant.end
                or not self.grant.approved_at <= snapshot.as_of or snapshot.end > snapshot.as_of):
            raise PermissionError("Receipt batch exceeds the bound source, purpose, role or time grant")
        if self.kind in CANDLES and any(at < batch.through for at in snapshot.missing_opens):
            raise ValueError("Receipt batch cannot advance past a missing candle")
        if batch.through == snapshot.start:
            return False
        if not db.in_transaction and (db.isolation_level is None or getattr(db, "autocommit", None) is True):
            raise ValueError("Acknowledge and consumer effects must share a transaction")
        committed = (batch.through.isoformat(), snapshot.as_of.isoformat(), batch.id)
        if batch.prior is None:
            result = db.execute(f"INSERT INTO {_CURSOR_TABLE} VALUES (?,1,?,?,?) ON CONFLICT(consumer) DO NOTHING",
                                (self.id, *committed))
        else:
            prior = batch.prior
            if snapshot.start != prior.next_at or snapshot.as_of < prior.as_of:
                raise ValueError("Receipt batch must extend the recorded causal cursor")
            result = db.execute(f"UPDATE {_CURSOR_TABLE} SET revision=revision+1,next_at=?,as_of=?,batch_id=? "
                                "WHERE consumer=? AND revision=? AND next_at=? AND as_of=? AND batch_id=?",
                                (*committed, self.id, prior.revision, prior.next_at.isoformat(),
                                 prior.as_of.isoformat(), prior.batch_id))
        return result.rowcount == 1
