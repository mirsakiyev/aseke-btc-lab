from contextlib import closing
import json
import time

import httpx
import pandas as pd
import pytest

from lab.config import fingerprint
from lab.ema_paper import catalog, private_notifications, recovery_runtime, synthetic_protocol, synthetic_runtime, worker
from lab.health import private_telegram, state
from lab.phase15 import paper
from lab.strategy_lab.market_data import MarketData, Observation, Purpose
from test_cross_venue_warmup import RAW, RAW_HASH, SEAM, STEP, snapshot
from test_ema_paper_worker import registered


@pytest.mark.parametrize("signal_volume", [10_000., 1.], ids=["qualifying-signal", "no-trade"])
def test_repair_resumes_future_only_and_keeps_protection_separate(registered, monkeypatch, signal_volume):
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
    books = {profile: synthetic_protocol._lane(record, {"id": "fixture predecessor"}, archive.source,
                                               SEAM - pd.Timedelta(minutes=5))
             for profile, record in originals.items()}
    assert books["day"] == amendment
    revision_body = {"books": books}
    revision = {"id": fingerprint(revision_body), "body": revision_body}
    route = private_telegram.Route("a" * 64, 123, "fixture-token")
    monkeypatch.setattr(state, "ROOT", root.parent / "watchdog")
    with closing(catalog.db_open(root=root)) as journal, state.connect() as outbox:
        private_notifications.enable(outbox, route.identifier, SEAM, originals, journal, revision=revision)
    for opened in pd.date_range(scopes[Purpose.EXECUTION_EVIDENCE].start, resume + 2 * STEP, freq=minute):
        closed = opened + minute
        rows.append(Observation(archive.source.id, synthetic_protocol.TRADE, opened, closed, closed + pd.Timedelta(seconds=3),
                                closed + pd.Timedelta(seconds=3), RAW_HASH,
                                {"open": 108., "high": 108.1, "low": 107.9, "close": 108., "volume": 30.}))
    for index, closed in enumerate((repair_context, resume, resume + STEP)):
        arrived = closed + pd.Timedelta(seconds=5)
        rows.append(Observation(archive.source.id, synthetic_protocol.TRADE, closed - STEP, closed, arrived, arrived,
                                RAW_HASH, {"open": 108., "high": 110., "low": 95. if index == 0 else 106.,
                                           "close": 109., "volume": signal_volume if index == 1 else 50.}))

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
    if signal_volume == 1.:
        with closing(catalog.db_open(True, root=root)) as journal, state.connect() as outbox:
            assert worker._record(journal, "readiness", lane["book_id"])["last_signal"]["decision"] == "NO_TRADE"
            private_notifications.collect(outbox, resume + minute, originals, journal, revision=revision)
            assert journal.execute("SELECT count(*) FROM event").fetchone() == (0,)
            assert outbox.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
        return
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

    messages = []

    def post(request):
        payload = json.loads(request.content)
        assert payload["chat_id"] == 123
        assert payload["protect_content"] is False
        messages.append(payload["text"])
        if len(messages) == 1:
            return httpx.Response(429, json={"parameters": {"retry_after": 60}})
        return httpx.Response(200, json={"ok": True, "result": {
            "message_id": len(messages), "chat": {"id": 123, "type": "private"}}})

    transport = httpx.MockTransport(post)
    cfg = {**state.DEFAULTS, "mute": False}
    collected_at = opening + 2 * minute
    for delay in (0, 30, 60, 180):
        now = collected_at + pd.Timedelta(seconds=delay)
        with closing(catalog.db_open(True, root=root)) as journal, state.connect() as outbox:
            private_notifications.collect(outbox, now, originals, journal, revision=revision)
            outbox.commit()
            state.flush(outbox, now.timestamp(), cfg, None, private_route_id=route.identifier,
                        private_send=lambda message: private_telegram.send(route, message, transport=transport))
            if delay in (0, 30):
                assert state.status(outbox)["delivery_counts"] == {"PENDING": 1, "RETRY": 1}
    assert len(messages) == 3 and messages[0] == messages[1]
    assert "SIMULATED PAPER EXPERIMENTAL IDEA" in messages[1]
    assert "SIMULATED PAPER ENTRY" in messages[2]
    assert all("EMA Pullback Day 15m · LONG · 3×" in message for message in messages)
    with state.connect() as outbox:
        assert state.status(outbox)["delivery_counts"] == {"DELIVERED": 2}
        assert [tuple(row) for row in outbox.execute("SELECT category,attempts FROM outbox ORDER BY rowid")] == [
            ("ideas", 2), ("paper_entry", 1)]

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
