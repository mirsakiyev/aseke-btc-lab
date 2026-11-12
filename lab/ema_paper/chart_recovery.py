"""Recover historical EMA context from the original authorized chart stream."""

from dataclasses import dataclass
import json
import time

import numpy as np
import pandas as pd

from lab.config import ORIGIN
from lab.data import OHLCV
from lab.execution import execution_open
from lab.strategy_lab import ema_pullback, feature_warmup as warmup
from lab.strategy_lab.market_data import ObservationKind
from lab.strategy_lab.registry import canonical, digest
from lab.strategy_lab.technical_features import FeatureIdentity, IncrementalFeatures, MAX_CHART_ROWS, compute_indicators


VERSION = "EMA_KRAKEN_CHART_RECOVERY_V1"
TRADE = ObservationKind.TRADE_OHLCV
MAX_READS = 48
MAX_MISSING_REPORT = 12
SCHEMA = """
CREATE TABLE IF NOT EXISTS chart_recovery_event (
    book TEXT NOT NULL, policy TEXT NOT NULL, subject TEXT NOT NULL, kind TEXT NOT NULL,
    digest TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(book,policy,subject,kind)
);
CREATE TRIGGER IF NOT EXISTS chart_recovery_event_update BEFORE UPDATE ON chart_recovery_event
    BEGIN SELECT RAISE(ABORT, 'append-only EMA chart recovery evidence'); END;
CREATE TRIGGER IF NOT EXISTS chart_recovery_event_delete BEFORE DELETE ON chart_recovery_event
    BEGIN SELECT RAISE(ABORT, 'append-only EMA chart recovery evidence'); END;
"""


def install_schema(db):
    """Install the isolated journal before starting the caller's ledger transaction."""
    db.executescript(SCHEMA)


def has_recorded_gap(db, book_id):
    """Require the recovery gate for a book with an audited chart discontinuity."""
    row = db.execute("SELECT digest,body FROM chart_recovery_event WHERE book=? AND kind='GAP_DETECTED' LIMIT 1",
                     (book_id,)).fetchone()
    if row is None:
        return False
    identifier, payload = row
    body = json.loads(payload)
    if (digest(body) != identifier or body.get("version") != VERSION or body.get("book_id") != book_id
            or body.get("kind") != "GAP_DETECTED"):
        raise ValueError("Stored EMA chart-recovery gap failed checksum verification")
    return True


@dataclass(frozen=True)
class RecoveryResult:
    state: str
    reason: str
    frame: pd.DataFrame | None
    evidence_available_at: pd.Timestamp | None
    snapshot_id: str | None
    resume_at: pd.Timestamp | None
    detail: dict
    changes: tuple[dict, ...]

    @property
    def has_discontinuity(self):
        return self.detail["disqualified_candles"] > 0


def _records(db, policy):
    rows = db.execute("SELECT subject,kind,digest,body FROM chart_recovery_event WHERE book=? AND policy=?",
                      (policy.book_id, policy.id))
    result = {}
    for subject, kind, identifier, payload in rows:
        body = json.loads(payload)
        if (digest(body) != identifier or body["subject"] != subject or body["kind"] != kind
                or body["book_id"] != policy.book_id or body["policy_id"] != policy.id or body["version"] != VERSION):
            raise ValueError("Stored EMA chart-recovery evidence failed checksum verification")
        result[subject, kind] = (identifier, body)
    return result


def _record(db, policy, records, changes, subject, kind, now, detail):
    if (subject, kind) in records:
        return records[subject, kind]
    body = {"version": VERSION, "book_id": policy.book_id, "policy_id": policy.id, "subject": subject,
            "kind": kind, "recorded_at": now.isoformat(), "timeframe_seconds": int(policy.timeframe.total_seconds()),
            **detail}
    identifier = digest(body)
    db.execute("INSERT INTO chart_recovery_event VALUES(?,?,?,?,?,?)",
               (policy.book_id, policy.id, subject, kind, identifier, canonical(body)))
    changes.append({"id": identifier, **body})
    records[subject, kind] = (identifier, body)
    return identifier, body


def _read_forward(data, policy, end, as_of, check_deadline):
    """Read at most seven days per snapshot and isolate rejected source buckets."""
    step = policy.timeframe
    block = max(1, pd.Timedelta(days=7) // step) * step
    remaining = MAX_READS
    observations, snapshots, rejected = {}, [], set()
    ranges = [(left, min(left + block, end)) for left in
              reversed(pd.date_range(policy.seed_end, end, freq=block, inclusive="left"))]
    while ranges:
        if not remaining or check_deadline is not None and time.monotonic() >= check_deadline:
            raise TimeoutError("Bounded authorized chart recovery read budget exhausted")
        remaining -= 1
        left, right = ranges.pop()
        try:
            snapshot = data.read(policy.forward_grant_id, policy.book_id, warmup.Purpose.FORWARD_DECISION, TRADE,
                                 left, right, as_of, step)
        except (KeyError, ValueError):
            count = int((right - left) // step)
            if count == 1:
                rejected.add(left)
            else:
                middle = left + (count // 2) * step
                ranges.extend(((middle, right), (left, middle)))
        else:
            snapshots.append(snapshot)
            observations.update({row.start: row for row in snapshot.observations})
    return observations, snapshots, rejected


def _observation(row):
    return {"received_at": row.received_at.isoformat(), "available_at": row.available_at.isoformat(),
            "raw_sha256": row.raw_sha256} if row is not None else {}


def _indicators(history, forward, policy, seed, source, now):
    """Prove incremental replay matches a full rebuild and the frozen recipe."""
    bars = pd.concat([history, forward])
    specs = dict(policy.indicators)
    context = FeatureIdentity(source.id, seed.id, source.instrument, source.market, policy.timeframe,
                              authorization_id=policy.forward_grant_id)
    incremental = IncrementalFeatures(context, specs)
    prehistory = incremental.update(history, as_of=seed.initialized_at, expected_tip=incremental.tip)
    advanced = incremental.update(forward, as_of=now, expected_tip=incremental.tip)
    batch = compute_indicators(bars, timeframe=policy.timeframe, indicators=specs)
    replay = pd.concat([prehistory, advanced])
    if not replay.index.equals(batch.index) or not np.allclose(replay, batch, rtol=1e-13, atol=1e-12, equal_nan=True):
        raise ValueError("Incremental EMA recovery indicators differ from complete recomputation")
    reference = ema_pullback.features(bars, policy.timeframe, policy.timeframe)
    if (not reference.index.equals(bars.index + policy.timeframe)
            or not np.allclose(reference[list(specs)], batch, rtol=1e-13, atol=1e-12, equal_nan=True)):
        raise ValueError("Rebuilt indicators differ from the frozen EMA decision implementation")
    return reference, incremental.tip


def recover(db, data, policy, seed, *, decision_at, as_of, check_deadline=None):
    """Inspect first receipts, repair context, and gate future-only evaluation.

    Only the shared Kraken receipt provider supplies candles. No market request
    or paper event occurs here. Commit recovery rows in the caller transaction.
    """
    at, now = map(warmup._utc, (decision_at, as_of))
    step = policy.timeframe
    if (policy.require_mark or policy.native_step != step or policy.seed_native_step != step
            or dict(policy.indicators) != dict(warmup.ema_indicators())):
        raise ValueError("Chart recovery requires the frozen native Kraken EMA chart policy")
    seed_scope, scope = warmup._grants(data, policy)
    if (seed_scope.source.id != scope.source.id or scope.source.provider.casefold() != "kraken"
            or (scope.source.instrument, scope.source.market) != ("BTC-USD", "SPOT")):
        raise PermissionError("Chart recovery requires the exact registered Kraken BTC-USD Spot source")
    if (seed.policy_id != policy.id or seed.end != policy.seed_end or seed.initialized_at > now
            or (at - ORIGIN) % step or not seed.end < at <= now
            or at > scope.end or (at - seed.start) // step > MAX_CHART_ROWS):
        raise ValueError("Chart recovery is outside the frozen seed, source, clock or replay bounds")
    original, _, history = warmup._seed_data(data, policy, seed.initialized_at, None)
    if original != seed:
        raise ValueError("Chart recovery cannot replace the immutable Kraken seed")

    records, changes = _records(db, policy), []
    prior_gaps = [body for _, body in records.values() if body["kind"] == "GAP_DETECTED"]
    detail = {"version": VERSION, "book_id": policy.book_id, "seed_id": seed.id, "source_id": scope.source.id,
              "decision_at": at.isoformat(), "chart_seconds": int(step.total_seconds()),
              "disqualified_candles": len(prior_gaps), "unresolved_candles": 0, "recovered_candles": 0,
              "missing_closes": [], "latest_gap_at": max((item["subject"] for item in prior_gaps), default=None),
              "history_tip": None, "readiness_recorded_at": None}

    def result(state, reason, *, frame=None, available=None, snapshot_id=None, resume=None):
        evidence = {**detail, "state": state, "has_discontinuity": detail["disqualified_candles"] > 0}
        return RecoveryResult(state, reason, frame, available, snapshot_id, resume, evidence, tuple(changes))

    try:
        observations, snapshots, rejected = _read_forward(data, policy, at, now, check_deadline)
    except TimeoutError as exc:
        return result("WAITING_FOR_DATA", str(exc))
    rows, unresolved, missing, gaps, restored = [], [], [], [], []
    current_pending = False
    current_timely = False
    for left in pd.date_range(seed.end, at, freq=step, inclusive="left"):
        right = left + step
        key = right.isoformat()
        deadline = min(right + policy.chart_grace, right + policy.native_grace)
        row = observations.get(left)
        prior = records.get((key, "GAP_DETECTED"))
        if left in rejected:
            reason = "DISQUALIFIED_CHART_EVIDENCE"
        elif row is None:
            reason = "MISSING_CHART_RECEIPT"
        elif row.available_at > deadline:
            reason = "LATE_CHART_RECEIPT"
        else:
            reason = None
        if right == at:
            current_timely = reason is None and prior is None
            current_pending = row is None and prior is None and now <= deadline
        if current_pending and right == at:
            continue
        if prior is not None or reason is not None:
            gap_id, _ = prior or _record(db, policy, records, changes, key, "GAP_DETECTED", now,
                                        {"reason": reason, "open_at": left.isoformat(), "close_at": key,
                                         "decision_receipt_deadline": deadline.isoformat(), "source_id": scope.source.id,
                                         **_observation(row)})
            gaps.append(gap_id)
            detail["latest_gap_at"] = key
        if row is None:
            unresolved.append(key)
            if reason == "MISSING_CHART_RECEIPT":
                missing.append(key)
            continue
        rows.append(row)
        if (key, "GAP_DETECTED") in records:
            restored.append((key, row))

    detail.update(disqualified_candles=len(gaps), unresolved_candles=len(unresolved), missing_closes=missing[:MAX_MISSING_REPORT],
                  first_unresolved_at=unresolved[0] if unresolved else None)
    if unresolved:
        return result("WAITING_FOR_DATA", f"Authorized Kraken chart context is incomplete at {unresolved[0]}")
    if not rows:
        return result("WAITING_FOR_DATA", "Awaiting the first completed Kraken chart receipt")
    if check_deadline is not None and time.monotonic() >= check_deadline:
        return result("WAITING_FOR_DATA", "Bounded chart recovery compute budget exhausted")

    opens = pd.DatetimeIndex([row.start for row in rows], name=history.index.name)
    forward = pd.DataFrame([{**{name: row.values[name] for name in OHLCV}, "close_at": row.end,
                             "available_at": row.available_at} for row in rows], index=opens)
    frame, tip = _indicators(history, forward, policy, seed, scope.source, now)
    if not bool(frame.iloc[-1].ready):
        return result("WAITING_FOR_DATA", "Rebuilt indicator state is undefined at the last completed candle")
    available = max(row.available_at for row in rows)
    detail.update(history_tip=tip, history_through=rows[-1].end.isoformat())
    resume = None
    if gaps:
        restored_ids = []
        for key, row in restored:
            restored_id, _ = _record(db, policy, records, changes, key, "CONTEXT_RESTORED", now,
                                     {"open_at": row.start.isoformat(), "close_at": key, "source_id": scope.source.id,
                                      "context_only": True, **_observation(row)})
            restored_ids.append(restored_id)
        detail["recovered_candles"] = len(restored_ids)
        if now < max(rows[-1].end, available) + policy.processing:
            return result("WAITING_FOR_DATA", "Waiting for the frozen processing allowance after recovered chart context")
        episode = digest([VERSION, seed.id, gaps, restored_ids])
        _, checkpoint = _record(db, policy, records, changes, episode, "RECOVERY_READY", now,
                                {"source_id": scope.source.id, "feature_tip": tip, "seed_id": seed.id,
                                 "through_close": rows[-1].end.isoformat(), "gaps": gaps,
                                 "first_eligible_at": execution_open(now, step).isoformat()})
        recorded, resume = map(warmup._utc, (checkpoint["recorded_at"], checkpoint["first_eligible_at"]))
        detail["readiness_recorded_at"] = recorded.isoformat()
        if at < resume or recorded >= at:
            return result("WAITING_FOR_START", "Chart context restored. Waiting for a strictly future decision boundary", resume=resume)
    if current_pending:
        return result("WAITING_FOR_DATA", "Awaiting timely first receipt for the current Kraken chart", resume=resume)
    if not current_timely or now > at + policy.publication_grace:
        return result("NO_TRADE", "Current chart cannot meet its original prospective timing", resume=resume)
    current = observations[at - step]
    if now < max(at, available) + policy.processing:
        return result("WAITING_FOR_DATA", "Waiting for the frozen processing allowance after chart receipt", resume=resume)
    # Report the current receipt and retain all context availability separately.
    # Recovery readiness predates a resumed decision, so context cannot backdate it.
    detail.update(current_received_at=current.received_at.isoformat(), current_available_at=current.available_at.isoformat(),
                  latest_context_available_at=available.isoformat())
    snapshot_id = digest([VERSION, seed.id, tip, *[snapshot.id for snapshot in snapshots]])
    return result("READY", "Recovered EMA context and timely prospective Kraken chart" if gaps else
                  "Timely Kraken chart and complete EMA context", frame=frame, available=available,
                  snapshot_id=snapshot_id, resume=resume)
