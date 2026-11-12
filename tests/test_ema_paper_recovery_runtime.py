from contextlib import closing
from dataclasses import replace
import json
import time

import pandas as pd

from lab.ema_paper import catalog, recovery_runtime, synthetic_protocol, synthetic_runtime, worker
from lab.phase15 import paper
from lab.strategy_lab.market_data import MarketData, Observation, Purpose
from test_cross_venue_warmup import RAW, RAW_HASH, SEAM, STEP, snapshot
from test_ema_paper_worker import registered


def test_repair_resumes_future_only_and_keeps_protection_separate(registered):
    root, _, _, _, originals = registered
    original = originals["day"]
    lane = original["body"]
    archive = snapshot()
    amendment = synthetic_protocol._lane(original, {"id": "fixture predecessor"}, archive.source,
                                         SEAM - pd.Timedelta(minutes=5))
    operation = {"id": "fixture operational amendment", "body": {"recorded_at": (SEAM + pd.Timedelta(seconds=30)).isoformat()}}
    scopes = {item.purpose: item for item in map(synthetic_protocol._grant, amendment["body"]["grants"])}
    rows = list(archive.observations)
    minute = pd.Timedelta(minutes=1)
    gap, repair_context, resume = SEAM + STEP, SEAM + 2 * STEP, SEAM + 3 * STEP
    for opened in pd.date_range(scopes[Purpose.EXECUTION_EVIDENCE].start, resume + 2 * STEP, freq=minute):
        closed = opened + minute
        rows.append(Observation(archive.source.id, synthetic_protocol.TRADE, opened, closed, closed + pd.Timedelta(seconds=3),
                                closed + pd.Timedelta(seconds=3), RAW_HASH,
                                {"open": 108., "high": 108.1, "low": 107.9, "close": 108., "volume": 30.}))
    for index, closed in enumerate((repair_context, resume, resume + STEP)):
        arrived = closed + pd.Timedelta(seconds=5)
        rows.append(Observation(archive.source.id, synthetic_protocol.TRADE, closed - STEP, closed, arrived, arrived,
                                RAW_HASH, {"open": 108., "high": 110., "low": 95. if index == 0 else 106.,
                                           "close": 109., "volume": 10_000. if index == 1 else 50.}))

    class Provider:
        def read(self, kind, start, end, as_of, interval):
            return (row for row in rows if row.kind == kind and row.end - row.start == interval and start <= row.start < end)

        def raw(self, digest):
            assert digest == RAW_HASH
            return RAW

    data = MarketData(scopes.values(), {archive.source.id: Provider()})
    from lab.ema_paper import chart_recovery

    def run(now):
        with closing(catalog.db_open(root=root)) as db:
            db.executescript(synthetic_runtime.SCHEMA)
            chart_recovery.install_schema(db)
            with db:
                return recovery_runtime._book(db, original, amendment, operation, scopes, data, now, time.monotonic() + 15)

    assert run(SEAM + minute)["first_eligible_at"] == gap.isoformat()
    assert run(gap + 3 * minute)["state"] == "WAITING_FOR_DATA"
    with closing(catalog.db_open(True, root=root)) as db:
        assert db.execute("SELECT count(*) FROM event").fetchone() == (0,)
    arrived = repair_context + pd.Timedelta(seconds=5)
    rows.append(Observation(archive.source.id, synthetic_protocol.TRADE, SEAM, gap, arrived, arrived, RAW_HASH,
                            {"open": 107., "high": 109., "low": 106., "close": 108., "volume": 45.}))
    assert run(repair_context + minute)["state"] == "WAITING_FOR_DATA"
    first = run(resume + minute)
    assert first["state"] == "ACTIVE", first["reason"]
    assert run(resume + minute)["state"] == "ACTIVE"
    with closing(catalog.db_open(True, root=root)) as db:
        assert catalog.registrations(db) == originals
        fixed = worker._record(db, "eligibility", lane["book_id"])
        assert fixed["first_eligible_at"] == gap.isoformat()
        event, = db.execute("SELECT body FROM event WHERE kind='PUBLISHED'").fetchall()
        planned = json.loads(event[0])
        assert planned["boundary"] == resume.isoformat()
        assert planned["plan"]["synthetic_model"]["leverage"] == 3
        assert planned["plan"]["evidence"]["recovery_amendment_id"] == operation["id"]
    opening = resume + STEP
    assert run(opening + 2 * minute)["source_protocol"]["protective_execution_state"] == "MONITORING"
    run(opening + 2 * minute)
    with closing(catalog.db_open(True, root=root)) as db:
        assert db.execute("SELECT count(*) FROM event WHERE kind='HYPOTHETICAL_FILL'").fetchone() == (1,)
        assert paper.load(db, originals["swing"]["body"])["equity"] == 10000

    # Remove just the next minute from this temporary provider. It must never
    # become a guessed stop or produce a retroactive exit.
    missing = opening + 2 * minute
    rows[:] = [row for row in rows if row.start != missing or row.end - row.start != minute]
    stopped = run(missing + 19 * minute)
    assert stopped["state"] == "BLOCKED_DATA"
    assert stopped["source_protocol"]["protective_execution_state"] == "UNRESOLVED"
    assert stopped["source_protocol"]["unresolved_execution_minute"] == missing.isoformat()
    run(missing + 20 * minute)
    with closing(catalog.db_open(True, root=root)) as db:
        account = paper.load(db, lane)
        assert account["position"] and account["permanent_block"]
        assert db.execute("SELECT count(*) FROM event WHERE kind='UNRESOLVED'").fetchone() == (1,)
        assert db.execute("SELECT count(*) FROM event WHERE kind='CLOSED'").fetchone() == (0,)
