from contextlib import closing
from hashlib import sha256
import json
import sqlite3

import pandas as pd
import pytest

from lab.strategy_lab.market_data import MarketData, ObservationKind, ProvenanceClass, Purpose, SourceGrant, SourceIdentity
from lab.strategy_lab.source_handoff import ReceiptHandoff, install_handoff_schema
from lab.strategy_lab.source_spool import ReviewedSqliteSpool, SPOOL_FILENAME, SPOOL_SCHEMA, SPOOL_VERSION


START = pd.Timestamp("2027-01-04T00:00Z")
MINUTE = pd.Timedelta(minutes=1)
RAW = b"synthetic authorized original producer response"
RAW_SHA = sha256(RAW).hexdigest()
TRADE = ObservationKind.TRADE_OHLCV
FUNDING = ObservationKind.FUNDING_SETTLEMENT


def spool(tmp_path):
    source = SourceIdentity("licensed fixture", "kucoin-origin fixture", "XBTUSDTM", "FUTURES", "test only",
                            RAW_SHA, ProvenanceClass.INDEPENDENT)
    path = tmp_path / SPOOL_FILENAME
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript(SPOOL_SCHEMA)
        db.execute("INSERT INTO market_source VALUES (?,?,?,?,?,?)",
                   (source.id, SPOOL_VERSION, source.provider, source.instrument, source.market, RAW_SHA))
        db.execute("INSERT INTO market_raw VALUES (?,?)", (RAW_SHA, RAW))
        for index, delay in ((0, 0), (1, 3), (2, 0), (3, 0)):
            at = START + index * MINUTE
            close = at + MINUTE
            observed = close + delay * MINUTE
            body = json.dumps({"open": 100., "high": 102., "low": 99., "close": 101., "volume": 4.})
            db.execute("INSERT INTO market_observation VALUES (?,?,?,?,?,?,?,?)",
                       (source.id, TRADE.value, at.value, close.value, observed.value, observed.value, RAW_SHA, body))
        funding = START + 2 * MINUTE
        db.execute("INSERT INTO market_observation VALUES (?,?,?,?,?,?,?,?)",
                   (source.id, FUNDING.value, funding.value, funding.value, funding.value, funding.value, RAW_SHA,
                    json.dumps({"rate": .0001, "actual_settlement": True})))
    return source, path


def grant(source, book, purpose=Purpose.FORWARD_DECISION, kind=TRADE):
    return SourceGrant(source, book, purpose, frozenset({kind}), START, START + 30 * MINUTE,
                       START, START - 10 * MINUTE, "fixture review")


def handoff(data, grant, *, consumer="fixture evaluator", kind=TRADE):
    return ReceiptHandoff(data, consumer_id=consumer, book_id=grant.book_id, grant_id=grant.id,
                          purpose=grant.purpose, kind=kind, interval=MINUTE if kind == TRADE else None)


def test_shared_spool_preserves_original_receipts_and_is_book_scoped(tmp_path):
    source, path = spool(tmp_path)
    before = path.read_bytes()
    one, two = grant(source, "one"), grant(source, "two")
    warmup = grant(source, "one", Purpose.WARMUP)
    provider = ReviewedSqliteSpool(source, root=tmp_path)
    data = MarketData([one, two, warmup], {source.id: provider})
    cutoff = START + 4 * MINUTE
    first = handoff(data, one).read(START, cutoff, cutoff)
    second = handoff(data, two).read(START, cutoff, cutoff)
    assert first.observations == second.observations and first.id != second.id
    assert first.missing_opens == (START + MINUTE,)
    assert first.observations[0].raw_sha256 == RAW_SHA and provider.raw(RAW_SHA) == RAW
    assert first.observations[0].received_at == first.observations[0].available_at == START + MINUTE
    with pytest.raises(PermissionError, match="outside the reviewed"):
        handoff(data, one).read(START - MINUTE, cutoff, cutoff)
    with pytest.raises(PermissionError, match="exact book and role"):
        ReceiptHandoff(data, consumer_id="bad", book_id=two.book_id, grant_id=one.id,
                       purpose=one.purpose, kind=TRADE, interval=MINUTE)
    with pytest.raises(PermissionError, match="exact book and role"):
        handoff(data, warmup)
    assert path.read_bytes() == before


def test_cursors_stop_at_gaps_resume_late_and_keep_books_independent(tmp_path):
    source, _ = spool(tmp_path)
    one, two = grant(source, "one"), grant(source, "two")
    data = MarketData([one, two], {source.id: ReviewedSqliteSpool(source, root=tmp_path)})
    first, second = handoff(data, one), handoff(data, two)
    end = START + 4 * MINUTE
    path = tmp_path / "consumers.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        install_handoff_schema(db)
        db.execute("CREATE TABLE effect (consumer TEXT, at TEXT, PRIMARY KEY (consumer,at))")
        batch = first.pending(db, end=end, as_of=end)
        assert batch.through == batch.missing_at == START + MINUTE
        assert [row.start for row in batch.observations] == [START]
        assert first.cursor(db) is None
        assert first.acknowledge(db, batch)
        db.execute("INSERT INTO effect VALUES (?,?)", (first.id, START.isoformat()))
        assert not first.acknowledge(db, batch)
        assert second.cursor(db) is None
        with pytest.raises(PermissionError, match="different consumer"):
            second.acknowledge(db, batch)

    restarted = handoff(data, one)
    with closing(sqlite3.connect(path)) as db, db:
        assert restarted.id == first.id and restarted.cursor(db).next_at == START + MINUTE
        batch = restarted.pending(db, end=end, as_of=end)
        assert not batch.observations and not restarted.acknowledge(db, batch)
        late = restarted.pending(db, end=end, as_of=end + MINUTE)
        assert late.missing_at is None and late.through == end
        assert [row.start for row in late.observations] == [START + i * MINUTE for i in range(1, 4)]
        assert restarted.acknowledge(db, late)
        db.executemany("INSERT INTO effect VALUES (?,?)", [(restarted.id, row.start.isoformat()) for row in late.observations])
        assert db.execute("SELECT COUNT(*) FROM effect").fetchone()[0] == 4
        assert restarted.pending(db, end=end, as_of=end + MINUTE) is None
        assert restarted.cursor(db).revision == 2
        assert second.cursor(db) is None


def test_cursor_rollback_and_point_schedule_fail_closed(tmp_path):
    source, _ = spool(tmp_path)
    execution = grant(source, "one", Purpose.EXECUTION_EVIDENCE, kind=FUNDING)
    data = MarketData([execution], {source.id: ReviewedSqliteSpool(source, root=tmp_path)})
    consumer = handoff(data, execution, kind=FUNDING)
    end = START + 4 * MINUTE
    point = START + 2 * MINUTE
    with closing(sqlite3.connect(":memory:")) as db:
        install_handoff_schema(db)
        with pytest.raises(ValueError, match="explicit expected point"):
            consumer.pending(db, end=end, as_of=end)
        with pytest.raises(ValueError, match="expected handoff schedule"):
            consumer.pending(db, end=end, as_of=end, expected_points=[])
        expected = [point, START + 3 * MINUTE]
        batch = consumer.pending(db, end=end, as_of=end, expected_points=expected)
        assert batch.through == expected[-1] and len(batch.observations) == 1
        with pytest.raises(RuntimeError, match="simulated crash"):
            with db:
                assert consumer.acknowledge(db, batch)
                raise RuntimeError("simulated crash before committing ledger effects")
        assert consumer.cursor(db) is None
        with db:
            assert consumer.acknowledge(db, batch)
        assert consumer.cursor(db).next_at == expected[-1]


def test_spool_rejects_bad_manifest_tampered_raw_and_missing_path(tmp_path):
    source, path = spool(tmp_path)
    one = grant(source, "one")
    provider = ReviewedSqliteSpool(source, root=tmp_path)
    data = MarketData([one], {source.id: provider})
    consumer = handoff(data, one)
    end = START + MINUTE
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("UPDATE market_raw SET body=?", (b"tampered",))
    with pytest.raises(ValueError, match="raw hash or size verification"):
        consumer.read(START, end, end)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("UPDATE market_source SET provider='other'")
    with pytest.raises(PermissionError, match="manifest differs"):
        consumer.read(START, end, end)
    absent = tmp_path / "no upstream configured"
    with pytest.raises(FileNotFoundError):
        ReviewedSqliteSpool(source, root=absent).raw(RAW_SHA)
    assert not absent.exists()


def test_spool_does_not_create_missing_upstream_wal_files(tmp_path):
    source, path = spool(tmp_path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    before = sorted(tmp_path.iterdir())
    assert before == [path]
    with pytest.raises(LookupError, match="WAL is unavailable"):
        ReviewedSqliteSpool(source, root=tmp_path).verify()
    assert sorted(tmp_path.iterdir()) == before
