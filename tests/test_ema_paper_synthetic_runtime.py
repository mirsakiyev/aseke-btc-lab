from contextlib import closing
from dataclasses import replace
import time

import pandas as pd

from lab.ema_paper import catalog, synthetic_protocol as protocol, synthetic_runtime as runtime
from lab.phase15 import paper
from lab.strategy_lab.market_data import MarketData, Observation, Purpose
from test_cross_venue_warmup import RAW, RAW_HASH, SEAM, STEP, snapshot
from test_ema_paper_worker import registered


def test_original_book_same_source_seam_and_strict_future_decision(registered):
    root, _, _, _, registrations = registered
    record = registrations["day"]
    lane = record["body"]
    archive = snapshot()
    revision = protocol._lane(record, {"id": "previous fixture amendment"}, archive.source,
                              SEAM - pd.Timedelta(minutes=5))
    grants = {grant.purpose: grant for grant in map(protocol._grant, revision["body"]["grants"])}
    rows = list(archive.observations)
    rows[-1] = replace(rows[-1], values={**rows[-1].values, "low": 95.})
    minute = pd.Timedelta(minutes=1)
    boundary = SEAM + STEP
    for left in pd.date_range(grants[Purpose.EXECUTION_EVIDENCE].start, boundary + minute, freq=minute):
        close = left + minute
        rows.append(Observation(archive.source.id, protocol.TRADE, left, close, close + pd.Timedelta(seconds=3),
                                close + pd.Timedelta(seconds=3), RAW_HASH,
                                {"open": 108., "high": 109., "low": 107., "close": 108., "volume": 10.}))
    rows.append(Observation(archive.source.id, protocol.TRADE, SEAM, boundary, boundary + pd.Timedelta(seconds=4),
                            boundary + pd.Timedelta(seconds=4), RAW_HASH,
                            {"open": 107., "high": 109., "low": 106., "close": 108., "volume": 20_000.}))

    class Provider:
        def read(self, kind, start, end, as_of, interval):
            return (row for row in rows if row.kind == kind and row.end - row.start == interval and start <= row.start < end)

        def raw(self, digest):
            assert digest == RAW_HASH
            return RAW

    data = MarketData(grants.values(), {archive.source.id: Provider()})

    def run(now):
        with closing(catalog.db_open(root=root)) as db:
            db.executescript(runtime.SCHEMA)
            with db:
                return runtime._cycle_book(db, record, revision, grants, data, now, time.monotonic() + 8)

    assert run(SEAM - minute)["state"] == "WAITING_FOR_DATA"
    ready = run(SEAM + minute)
    assert ready["state"] == "WAITING_FOR_START"
    assert ready["first_eligible_at"] == boundary.isoformat()
    assert ready["source_protocol"]["warmup_count"] == 220
    assert ready["source_protocol"]["readiness"] == {"same_market_seed_ready": True, "execution_minute_ready": True}
    with closing(catalog.db_open(True, root=root)) as db:
        assert db.execute("SELECT count(*) FROM event").fetchone() == (0,)
    assert run(boundary + minute)["state"] == "ACTIVE"
    assert run(boundary + minute)["state"] == "ACTIVE"
    with closing(catalog.db_open(True, root=root)) as db:
        assert catalog.registrations(db) == registrations
        assert db.execute("SELECT count(*) FROM eligibility").fetchone() == (1,)
        assert db.execute("SELECT count(*) FROM event WHERE kind='PUBLISHED'").fetchone() == (1,)
        account = paper.load(db, lane)
        assert account["pending"]["plan"]["evidence"]["source_mode"] == "KRAKEN_SPOT_REFERENCE"
        assert account["pending"]["plan"]["synthetic_model"]["leverage"] == 3
        assert paper.load(db, registrations["swing"]["body"])["equity"] == 10000
