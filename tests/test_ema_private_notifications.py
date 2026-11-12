from contextlib import closing
import json

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.ema_paper import catalog, private_notifications as alerts, synthetic_protocol
from lab.health import state
from lab.phase15 import paper
from lab.storage import utc


NOW = utc("2026-11-23T00:00:00Z")
MINUTE = pd.Timedelta(minutes=1)
ROUTE = "a" * 64


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path / "watchdog")
    root = tmp_path / "ema"
    registrations, books = {}, {}
    for profile in alerts.PROFILES:
        lane = {"book_id": fingerprint(["book", profile]), "profile": profile,
                "registered_at": (NOW - 60 * MINUTE).isoformat(),
                "risk": {"starting_equity": 10000, "risk_fraction": .0025}}
        original = {"id": fingerprint(lane), "body": lane}
        model = {"registration_id": original["id"], "book_id": lane["book_id"], "paper_only": True,
                 "live_orders": False, "source_mode": synthetic_protocol.SOURCE_MODE,
                 "execution_model": {"leverage": 1 if profile == "position" else 3}}
        registrations[profile] = original
        books[profile] = {"id": fingerprint(model), "body": model}
    body = {"books": books}
    return root, registrations, {"id": fingerprint(body), "body": body}


def _idea(db, lane, model, identifier, when, *, with_close=False):
    evidence = {"source_mode": synthetic_protocol.SOURCE_MODE, "protocol_amendment_id": model["id"],
                "raw_ohlcv": [[70654.3982]], "path": r"C:\Users\private\scope.json", "secret": "PRIVATE_TOKEN"}
    published = {"direction": "SHORT", "plan": {"evidence": evidence, "stop": 73456.123, "target": 67987.321}}
    paper.event(db, lane, "PUBLISHED", identifier, when, when, published)
    paper.event(db, lane, "HYPOTHETICAL_FILL", identifier, when + MINUTE, when + 2 * MINUTE,
                {"price": 71234.99, "quantity": 1.024135, "notional": 65432.123, "raw_bar": evidence})
    if with_close:
        paper.event(db, lane, "CLOSED", identifier, when + 2 * MINUTE, when + 3 * MINUTE,
                    {"reason": "PRIVATE_TOKEN", "exit_kind": "stop", "exit_price": 72345.91, "trigger_bar": evidence,
                     "gross_price_pnl": -19, "fees": .9, "funding": -.1, "net": -20,
                     "equity_before": 10000, "equity_after": 9980, "holding_minutes": 1})


def test_future_only_allowlist_and_deduplication_across_restart(stores):
    root, registrations, revision = stores
    lane = registrations["day"]["body"]
    model = revision["body"]["books"]["day"]
    with closing(catalog.db_open(root=root)) as journal:
        _idea(journal, lane, model, "old-fixture", NOW - 3 * MINUTE, with_close=True)
        journal.commit()
        with state.connect() as db:
            state.enqueue(db, "paper:phase15", "ideas", "existing phase 15", NOW.timestamp())
            state.set_meta(db, "paper_lifecycle", "unchanged")
            assert alerts.collect(db, NOW, registrations, journal, revision=revision)["state"] == "PRIVATE_ROUTE_NOT_ENABLED"
            assert alerts.enable(db, ROUTE, NOW, registrations, journal, revision=revision)["route_id"] == ROUTE
        _idea(journal, lane, model, "new-fixture", NOW + MINUTE, with_close=True)
        # A post-baseline import with modeled pre-baseline time must still be excluded.
        paper.event(journal, lane, "HYPOTHETICAL_FILL", "old-fixture", NOW - MINUTE, NOW + MINUTE, {"price": 999999.})
        _idea(journal, lane, {"id": "wrong-model"}, "wrong-model", NOW + 2 * MINUTE)
        journal.commit()
        for tick in (10, 11):
            with state.connect() as db:
                now = NOW + tick * MINUTE
                assert alerts.collect(db, now, registrations, journal, revision=revision)["state"] == "PRIVATE_SUMMARIES_ENABLED"
                assert state.get_meta(db, "paper_lifecycle") == "unchanged"
                db.commit()
                entries = db.execute("SELECT text,event_json FROM outbox WHERE id LIKE 'ema-private:%' ORDER BY rowid").fetchall()
                assert [json.loads(row[1])["event"] for row in entries] == ["PUBLISHED", "HYPOTHETICAL_FILL", "CLOSED"]
                assert all(json.loads(row[1])["private_route_id"] == ROUTE for row in entries)
        text = "\n".join(row[0] for row in entries)
        assert "EMA Pullback Day 15m · SHORT · 3×" in text and "0.25% of paper equity" in text
        assert "Kraken BTC/USD Spot reference · Synthetic execution · nominal USDT" in text
        assert "Ledger recorded 2026-11-23 00:01:00 UTC" in text
        assert "Stop-loss" in text and "Net return -0.20% of pre-trade paper equity" in text
        for forbidden in ("70654", "73456", "67987", "71234", "72345", "65432", "PRIVATE_TOKEN", "scope.json", "old-fixture"):
            assert forbidden not in text


def test_idea_category_upgrade_keeps_fill_cursor_and_never_replays_old_ideas(stores):
    root, registrations, revision = stores
    lane = registrations["day"]["body"]
    effective = revision["body"]["books"]["day"]
    published = {"direction": "LONG", "plan": {"stop": 72345.91, "target": 81234.56, "evidence": {
        "source_mode": synthetic_protocol.SOURCE_MODE, "protocol_amendment_id": effective["id"],
        "source_received_at": "2026-11-23T00:01:19.49326Z",
    }}}
    with closing(catalog.db_open(root=root)) as journal, state.connect() as db:
        alerts.enable(db, ROUTE, NOW, registrations, journal, revision=revision)
        db.execute("DELETE FROM meta WHERE key LIKE 'ema_private_telegram_idea_cursor_v1:%'")
        _idea(journal, lane, effective, "legacy", NOW + MINUTE, with_close=True)
        journal.commit()
        alerts.collect(db, NOW + 6 * MINUTE, registrations, journal, revision=revision)
        established = state.get_meta(db, alerts._cursor(effective["id"]))
        assert established == "3"
        paper.event(journal, lane, "PUBLISHED", "delayed-old-idea", NOW + 4 * MINUTE, NOW + 7 * MINUTE, published)
        for index, kind in enumerate(("EXPIRED", "INVALIDATED"), 1):
            idea = f"new-idea-{index}"
            when = NOW + (6 + index) * MINUTE
            paper.event(journal, lane, "PUBLISHED", idea, when, when, published)
            paper.event(journal, lane, kind, idea, when + MINUTE, when + 2 * MINUTE,
                        {"reason": "private detail 92345.91"})
            # A conflicting terminal record cannot send a second no-entry alert.
            paper.event(journal, lane, "INVALIDATED", idea, when + 2 * MINUTE, when + 3 * MINUTE,
                        {"reason": "private detail 92345.91"})
        journal.commit()
        alerts.collect(db, NOW + 20 * MINUTE, registrations, journal, revision=revision)
        assert state.get_meta(db, alerts._cursor(effective["id"])) == established
        events = db.execute("SELECT text,event_json FROM outbox ORDER BY rowid").fetchall()
        assert [json.loads(row[1])["event"] for row in events] == [
            "HYPOTHETICAL_FILL", "CLOSED", "PUBLISHED", "EXPIRED", "PUBLISHED", "INVALIDATED",
        ]
        assert "IDEA EXPIRED" in events[3][0] and "IDEA INVALIDATED" in events[5][0]
        for row in events[2:]:
            assert "No real exchange order was placed" in row[0]
            for forbidden in ("72345", "81234", "92345", "00:01:19", "private detail"):
                assert forbidden not in row[0] and forbidden not in row[1]


def test_partial_idea_upgrade_state_is_rebaselined_without_touching_trade_cursor(stores):
    root, registrations, revision = stores
    lane = registrations["day"]["body"]
    effective = revision["body"]["books"]["day"]
    with closing(catalog.db_open(root=root)) as journal, state.connect() as db:
        alerts.enable(db, ROUTE, NOW, registrations, journal, revision=revision)
        db.execute("DELETE FROM meta WHERE key LIKE 'ema_private_telegram_idea_enabled_v1:%'")
        _idea(journal, lane, effective, "already-committed", NOW + MINUTE)
        journal.commit()
        observed = NOW + 5 * MINUTE
        alerts.collect(db, observed, registrations, journal, revision=revision)
        assert state.get_meta(db, alerts._idea_start(effective["id"])) == observed.isoformat()
        assert state.get_meta(db, alerts._cursor(effective["id"])) == "2"
        assert [row[0] for row in db.execute("SELECT category FROM outbox")] == ["paper_entry"]


def test_reverification_and_route_change_are_future_only(stores):
    root, registrations, revision = stores
    with closing(catalog.db_open(root=root)) as journal, state.connect() as db:
        saved = alerts.enable(db, ROUTE, NOW, registrations, journal, revision=revision)
        assert alerts.enable(db, ROUTE, NOW + MINUTE, registrations, journal, revision=revision) == saved
        state.enqueue(db, alerts.PREFIX + "retry", "paper_exit", "old destination", NOW.timestamp())
        state.enqueue(db, "paper:phase15", "paper_exit", "legacy destination", NOW.timestamp())
        new = alerts.enable(db, "b" * 64, NOW + MINUTE, registrations, journal, revision=revision)
        assert new["enabled_at"] == (NOW + MINUTE).isoformat()
        assert dict(map(tuple, db.execute("SELECT id,status FROM outbox"))) == {
            alerts.PREFIX + "retry": "SUPERSEDED", "paper:phase15": "PENDING",
        }


@pytest.mark.parametrize(("key", "label"), [("collector", "Kraken reference collection"),
                                          ("chart_recovery", "EMA chart recovery")])
def test_operational_alerts_are_guarded_and_debounced(stores, key, label):
    root, registrations, revision = stores
    with closing(catalog.db_open(root=root)) as journal, state.connect() as db:
        alerts.enable(db, ROUTE, NOW, registrations, journal, revision=revision)
        for tick in (1, 2, 2, 3, 4):
            alerts.observe(db, NOW + tick * MINUTE, {key: False})
        for tick in (5, 5, 6, 7):
            alerts.observe(db, NOW + tick * MINUTE, {key: True})
        rows = db.execute("SELECT category,text,event_json FROM outbox ORDER BY rowid").fetchall()
        assert [json.loads(row[2])["event"] for row in rows] == ["INTERRUPTED", "RECOVERED"]
        assert all(row[0] == "health" and json.loads(row[2])["private_route_id"] == ROUTE for row in rows)
        assert all(label in row[1] for row in rows)


def test_watchdog_gap_notifies_on_the_first_resumed_check(stores):
    root, registrations, revision = stores
    with closing(catalog.db_open(root=root)) as journal, state.connect() as db:
        alerts.enable(db, ROUTE, NOW, registrations, journal, revision=revision)
        alerts.observe(db, NOW + 8 * MINUTE, {"watchdog": False})
        alerts.observe(db, NOW + 9 * MINUTE, {"watchdog": True})
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1
        alerts.observe(db, NOW + 10 * MINUTE, {"watchdog": True})
        messages = [row[0] for row in db.execute("SELECT text FROM outbox ORDER BY rowid")]
        assert len(messages) == 2 and messages[0].endswith("INTERRUPTED") and messages[1].endswith("RECOVERED")
