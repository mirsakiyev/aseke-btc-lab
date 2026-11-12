from contextlib import closing
from dataclasses import replace
import sqlite3

import pandas as pd
import pytest

from lab.ema_paper import chart_recovery as recovery
from lab.strategy_lab import ema_pullback, feature_warmup as warmup
from lab.strategy_lab.market_data import MarketData, Observation, Purpose, SourceGrant
from test_cross_venue_warmup import RAW, RAW_HASH, SEAM, STEP, snapshot


SECOND = pd.Timedelta(seconds=1)
MINUTE = pd.Timedelta(minutes=1)
TRADE = recovery.TRADE


def setup(step=STEP):
    archived = snapshot(step)
    book, source = archived.book_id, archived.source
    registered = SEAM - 10 * step
    now = SEAM + 10 * SECOND
    warmup_grant = SourceGrant(source, book, Purpose.WARMUP, {TRADE}, archived.start, SEAM,
                               registered, registered, "fixture scope")
    forward_grant = SourceGrant(source, book, Purpose.FORWARD_DECISION, {TRADE}, SEAM, SEAM + pd.Timedelta(days=92),
                                registered, registered, "fixture scope")
    rows = list(archived.observations)

    class Provider:
        def read(self, kind, start, end, as_of, interval):
            return (row for row in rows if row.kind == kind and row.end - row.start == interval and start <= row.start < end)

        def raw(self, digest):
            assert digest == RAW_HASH
            return RAW

    data = MarketData((warmup_grant, forward_grant), {source.id: Provider()})
    policy = warmup.WarmupPolicy("fixture registration", book, registered, warmup_grant.id, forward_grant.id,
                                 archived.content_id, archived.start, SEAM, step, step, step, warmup.ema_indicators(), 200)
    seed = warmup.freeze_seed(data, policy, as_of=now)

    def candle(i, *, delay=4, close=None):
        left = SEAM + i * step
        end = left + step
        price = 106. + i * .15 if close is None else close
        return Observation(source.id, TRADE, left, end, end + delay * SECOND, end + (delay + 1) * SECOND,
                           RAW_HASH, {"open": price, "high": price + 2., "low": price - .4, "close": price,
                                      "volume": 7. + i})

    return rows, data, policy, seed, candle


def run(db, data, policy, seed, at, *, now=None):
    return recovery.recover(db, data, policy, seed, decision_at=at, as_of=now if now is not None else at + MINUTE)


@pytest.mark.parametrize("step", [STEP, pd.Timedelta("4h"), pd.Timedelta("1D")])
def test_late_candle_restores_context_then_only_future_chart_is_eligible(step):
    rows, data, policy, seed, candle = setup(step)
    first, second, third = (SEAM + i * step for i in range(1, 4))
    rows.extend((candle(0, delay=125), candle(1), candle(2)))
    with closing(sqlite3.connect(":memory:")) as db, db:
        recovery.install_schema(db)
        pending = run(db, data, policy, seed, first)
        assert pending.state == "WAITING_FOR_DATA" and not pending.has_discontinuity
        assert not pending.changes and pending.frame is None
        late = run(db, data, policy, seed, first, now=first + 3 * MINUTE)
        assert late.state == "WAITING_FOR_START" and late.resume_at == second
        assert late.has_discontinuity and late.frame is None
        gap, restored, ready = late.changes
        assert [event["kind"] for event in (gap, restored, ready)] == ["GAP_DETECTED", "CONTEXT_RESTORED", "RECOVERY_READY"]
        assert gap["reason"] == "LATE_CHART_RECEIPT" and restored["context_only"]
        assert restored["received_at"] == (first + 125 * SECOND).isoformat()
        assert restored["available_at"] == (first + 126 * SECOND).isoformat()
        assert recovery.recover(db, data, policy, seed, decision_at=first, as_of=first + 3 * MINUTE).changes == ()
        resumed = run(db, data, policy, seed, second)
        assert resumed.state == "READY" and resumed.frame.index[-1] == second and third not in resumed.frame.index
        assert resumed.evidence_available_at == second + 5 * SECOND
        assert resumed.detail["readiness_recorded_at"] == (first + 3 * MINUTE).isoformat()
        reference = ema_pullback.features(pd.concat([snapshot(step).frame(), data.read(
            policy.forward_grant_id, policy.book_id, Purpose.FORWARD_DECISION, TRADE,
            SEAM, second, second + MINUTE, step).frame()]), step, step)
        pd.testing.assert_frame_equal(resumed.frame, reference)
        assert resumed.snapshot_id == run(db, data, policy, seed, second).snapshot_id


def test_multiple_missing_candles_survive_restart_and_preserve_late_handoffs(tmp_path):
    rows, data, policy, seed, candle = setup()
    current = SEAM + 3 * STEP
    rows.append(candle(2))
    path = tmp_path / "recovery.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        recovery.install_schema(db)
        missing = run(db, data, policy, seed, current)
        assert missing.state == "WAITING_FOR_DATA"
        assert missing.detail["unresolved_candles"] == 2 and missing.frame is None
        assert missing.detail["missing_closes"] == [(SEAM + STEP).isoformat(), (SEAM + 2 * STEP).isoformat()]
        assert len(missing.changes) == 2

    with closing(sqlite3.connect(path)) as db, db:
        assert run(db, data, policy, seed, current).changes == ()
        rows.append(candle(0, delay=32 * 60))
        still_missing = run(db, data, policy, seed, current, now=current + 3 * MINUTE)
        assert still_missing.detail["unresolved_candles"] == 1 and still_missing.frame is None
        rows.append(candle(1, delay=18 * 60))
        unprocessed = run(db, data, policy, seed, current, now=current + 3 * MINUTE + SECOND)
        assert unprocessed.state == "WAITING_FOR_DATA" and unprocessed.detail["recovered_candles"] == 2
        restored = run(db, data, policy, seed, current, now=current + 4 * MINUTE)
        assert restored.state == "WAITING_FOR_START" and restored.resume_at == current + STEP
        assert db.execute("SELECT kind,count(*) FROM chart_recovery_event GROUP BY kind ORDER BY kind").fetchall() == [
            ("CONTEXT_RESTORED", 2), ("GAP_DETECTED", 2), ("RECOVERY_READY", 1)]
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            db.execute("UPDATE chart_recovery_event SET body='{}'")


def test_conflicting_candle_is_reported_but_never_recovered_or_admitted():
    rows, data, policy, seed, candle = setup()
    at = SEAM + 2 * STEP
    rows.extend((candle(0), candle(0, close=130.), candle(1)))
    with closing(sqlite3.connect(":memory:")) as db, db:
        recovery.install_schema(db)
        result = run(db, data, policy, seed, at)
        assert result.state == "WAITING_FOR_DATA" and result.has_discontinuity and result.frame is None
        assert result.detail["unresolved_candles"] == 1 and result.detail["missing_closes"] == []
        assert result.changes[0]["reason"] == "DISQUALIFIED_CHART_EVIDENCE"
        with pytest.raises(ValueError, match="immutable Kraken seed"):
            run(db, data, policy, replace(seed, bars=seed.bars + 1), at)


def test_recorded_gap_is_not_reopened_by_a_reader_claiming_earlier_availability():
    rows, data, policy, seed, candle = setup()
    first, second = SEAM + STEP, SEAM + 2 * STEP
    with closing(sqlite3.connect(":memory:")) as db, db:
        recovery.install_schema(db)
        assert not recovery.has_recorded_gap(db, policy.book_id)
        gap = run(db, data, policy, seed, first, now=first + 3 * MINUTE)
        assert gap.detail["has_discontinuity"] and gap.detail["state"] == "WAITING_FOR_DATA"
        assert recovery.has_recorded_gap(db, policy.book_id)
        assert not recovery.has_recorded_gap(db, "different book")
        rows.append(candle(0))  # Simulates a source that later claims an on-time original receipt.
        restored = run(db, data, policy, seed, first, now=first + 4 * MINUTE)
        assert restored.state == "WAITING_FOR_START" and restored.resume_at == second
        assert restored.frame is None
        rows.append(candle(1))
        assert run(db, data, policy, seed, second).state == "READY"


def test_recovery_read_budget_preserves_already_recorded_block():
    _, data, policy, seed, _ = setup()
    first = SEAM + STEP
    with closing(sqlite3.connect(":memory:")) as db, db:
        recovery.install_schema(db)
        assert run(db, data, policy, seed, first, now=first + 3 * MINUTE).has_discontinuity
        unavailable = recovery.recover(db, data, policy, seed, decision_at=first, as_of=first + 4 * MINUTE,
                                       check_deadline=0.)
        assert unavailable.state == "WAITING_FOR_DATA" and unavailable.has_discontinuity
        assert unavailable.detail["latest_gap_at"] == first.isoformat() and not unavailable.changes
