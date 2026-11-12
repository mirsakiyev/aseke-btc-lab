from contextlib import closing
import json

import pandas as pd

from lab.config import fingerprint
from lab.ema_paper import paper_view
from lab.health import delivery, state
from lab.kraken_paper import catalog, notifications, presentation
from lab.phase15 import paper
from lab.storage import utc


REGISTERED = utc("2026-11-20T00:00Z")
FIRST = REGISTERED + pd.Timedelta(hours=2)


def _registration(profile):
    return {"id": "kraken-fixture-" + profile, "body": {
        "book_id": "kraken-USD-" + profile, "profile": profile, "registered_at": REGISTERED.isoformat(),
        "risk": {"starting_equity": 10000}, "permissions": {"telegram_lifecycle": True},
        "strategy_version": "kraken_fixture", "strategy_hash": "fixture",
    }}


def _fix(db, lane):
    db.execute("INSERT INTO eligibility VALUES (?,?)", (lane["book_id"], paper.encode({
        "first_eligible_at": FIRST, "primary_end_exclusive": FIRST + pd.Timedelta(days=90),
    })))


def _publish(db, lane, identifier, publication, *, boundary=None):
    eligible = publication.ceil(pd.Timedelta(minutes=15))
    plan = {"entry": {"minimum": 93, "maximum": 101, "reference": 99}, "stop": 93, "target": 111,
            "eligible_from": eligible, "entry_expiry": eligible + pd.Timedelta(minutes=1),
            "evidence": {"latest_provider_received_at": publication - pd.Timedelta(seconds=35),
                         "latest_handoff_at": publication - pd.Timedelta(seconds=32)}}
    paper.event(db, lane, "PUBLISHED", identifier, publication, publication, {
        "id": identifier, "direction": "LONG", "boundary": boundary if boundary is not None else publication.floor("15min"),
        "published_at": publication, "plan": plan,
    })
    return eligible


def test_kraken_lifecycle_uses_existing_outbox_without_historical_or_duplicate_alerts(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path / "health")
    records = {profile: _registration(profile) for profile in ("day", "swing")}
    day, swing = (records[name]["body"] for name in ("day", "swing"))
    swing["permissions"]["telegram_lifecycle"] = False
    early = REGISTERED + pd.Timedelta(minutes=1)
    published = FIRST + pd.Timedelta(minutes=1)
    with closing(catalog.db_open(root=tmp_path / "paper")) as db, state.connect() as health:
        _publish(db, day, "warmup", early)
        _publish(db, day, "historical-boundary", published, boundary=REGISTERED)
        filled_at = _publish(db, day, "trade", published)
        received = filled_at + pd.Timedelta(minutes=1)
        paper.event(db, day, "HYPOTHETICAL_FILL", "trade", filled_at, received, {
            "price": 99, "fee": .792, "quantity": 1, "source_received_at": received,
        })
        closed_at = filled_at + pd.Timedelta(minutes=10)
        close_received = closed_at + pd.Timedelta(minutes=1)
        paper.event(db, day, "CLOSED", "trade", closed_at, close_received, {
            "entry_price": 99, "exit_price": 111, "reason": "target", "net": 10.32, "fees": 1.68,
            "adverse_cost_in_gross": .3, "equity_after": 10010.32,
            "trade": {"received_at": close_received, "observed_at": close_received},
        })
        expired_at = _publish(db, day, "expiry", published)
        paper.event(db, day, "EXPIRED", "expiry", expired_at, received, {"reason": "Missing trade minute"})
        invalidated_at = _publish(db, day, "invalid", published)
        paper.event(db, day, "INVALIDATED", "invalid", invalidated_at, received, {"reason": "Price outside bracket"})
        _publish(db, swing, "unqualified-swing", published)
        db.commit()
        state.set_meta(health, "paper_lifecycle", "protected")
        assert notifications.collect(health, close_received, state.policy(), registrations=records, paper=db) == {}
        _fix(db, day)
        _fix(db, swing)
        db.commit()
        sent = []

        def fake_sender(message):
            sent.append(message)
            return delivery.Delivery("DELIVERED", "fixture", message_id=len(sent))

        for offset in (0, 3, 6):
            now = close_received + pd.Timedelta(minutes=offset)
            result = notifications.collect(health, now, state.policy(), registrations=records, paper=db)
            assert result["day"]["cursor_rowid"] == result["day"]["latest_rowid"]
            assert "swing" not in result
            state.flush(health, now.timestamp(), state.policy(), fake_sender)
        assert state.get_meta(health, "paper_lifecycle") == "protected"
        assert len(sent) == 7
        assert all("Kraken Spot BTC/USD · USD" in msg and "kraken-USD-day" in msg for msg in sent)
        assert all("USDT" not in msg for msg in sent)
        assert any("HYPOTHETICAL PAPER ENTRY CONFIRMED" in msg and "Entry fee 0.7920 USD" in msg for msg in sent)
        assert any("Net paper P&L +10.3200 USD" in msg and "Fees 1.6800 USD" in msg for msg in sent)
        assert {row[0] for row in health.execute("SELECT idea_id FROM outbox")} == {"trade", "expiry", "invalid"}
        context = json.loads(health.execute("SELECT event_json FROM outbox WHERE category='paper_exit'").fetchone()[0])
        assert context["source_received_at"] == context["source_handoff_at"] == close_received.isoformat()


def test_data_warning_and_recovery_count_distinct_committed_checks(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path / "health")
    records = {"day": _registration("day")}
    lane = records["day"]["body"]
    cfg = {**state.policy(), "warning_checks": 2, "recovery_checks": 2}
    with closing(catalog.db_open(root=tmp_path / "paper")) as db, state.connect() as health:
        _fix(db, lane)
        for offset, status in enumerate(("WAITING_FOR_DATA", "WAITING_FOR_DATA", "ACTIVE", "ACTIVE")):
            checked = FIRST + pd.Timedelta(minutes=offset * 3)
            db.execute("INSERT OR REPLACE INTO readiness VALUES (?,?)", (lane["book_id"], paper.encode({
                "last_checked_at": checked, "state": status, "reason": "Fixture data-source state",
                "coverage": {"latest_close_at": FIRST, "latest_provider_received_at": FIRST},
            })))
            for _ in range(2):
                notifications.collect(health, checked, cfg, registrations=records, paper=db)
        found = [(row["status"], json.loads(row["event_json"])["event"]) for row in
                 health.execute("SELECT status,event_json FROM outbox ORDER BY rowid")]
        assert found == [("SUPERSEDED", "SOURCE_WARNING"), ("PENDING", "SOURCE_RECOVERY")]
        assert health.execute("SELECT COUNT(*) FROM outbox_attempt").fetchone()[0] == 0


def test_dashboard_lists_ten_books_with_explicit_quote_and_kraken_evidence(monkeypatch):
    from streamlit.testing.v1 import AppTest

    def record(profile, key, label, quote):
        registration = _registration(profile)
        account = paper.initial(registration["body"])
        return {"profile": profile, "label": label, "instrument": "BTC/USD" if quote == "USD" else "BTC-USDT",
                "currency": quote, "registration_id": key, "registration": registration,
                "strategy_version": "fixture", "state": "WAITING_FOR_DATA", "account": account,
                "stats": paper_view._stats([]), "trades": []}

    current = {key: record(key.split("/")[1 if key.startswith("ema/") else 0], key, label, "USDT")
               for key, label in paper_view.LABELS.items()}
    kraken = {"kraken/" + name: {**record(name, name, label, "USD"), "venue": "Kraken Spot",
                                  "source_permission": "APPROVED", "warmup_values": {"ema_fast": 99, "ema_slow": 101}}
              for name, label in presentation.LABELS.items()}
    monkeypatch.setattr(paper_view, "report", lambda: {
        "books": current, "comparisons": [], "mode": "MANUAL_OUTSIDE_LAB", "approval": "No validated model",
    })
    monkeypatch.setattr(paper_view, "_delivery", lambda _book: [])
    monkeypatch.setattr(presentation, "report", lambda: kraken)
    app = AppTest.from_string("from lab.kraken_paper.presentation import panel\npanel()").run()
    assert not app.exception
    table = app.dataframe[0].value
    assert len(table) == 10 and table["Currency"].value_counts().to_dict() == {"USDT": 7, "USD": 3}
    assert table["Starting equity"].eq(10000).all()
    app.selectbox[0].select("kraken/day").run()
    assert not app.exception
    assert any(element.value == "Kraken Spot source and indicator evidence" for element in app.subheader)
    assert any('"ema50": 99' in item.value for item in app.json)
    assert any("Matched Kraken Spot comparisons" in item.value for item in app.caption)


def test_presentation_reads_actual_registration_snapshot_without_creating_a_book(tmp_path):
    root = tmp_path / "kraken_paper"
    assert presentation.report(root=root) == {} and not root.exists()
    lane = _registration("day")["body"]
    with closing(catalog.db_open(root=root)) as db:
        identifier = fingerprint(lane)
        db.execute("INSERT INTO registration VALUES(?,?,?)", ("day", identifier, paper.encode(lane)))
        paper.save(db, paper.initial(lane))
        db.commit()
    before = (root / "paper.sqlite3").read_bytes()
    book = presentation.report(root=root)["kraken/day"]
    assert book["registration_id"] == identifier
    assert book["currency"] == "USD" and book["account"]["equity"] == 10000
    assert book["stats"]["evidence"] == "INSUFFICIENT_EVIDENCE"
    assert (root / "paper.sqlite3").read_bytes() == before


def test_existing_watchdog_only_supervises_kraken_in_scheduled_mode(tmp_path, monkeypatch):
    from lab.health import __main__ as watchdog, monitor
    from lab.kraken_paper import managed, worker

    health = tmp_path / "health"
    kraken = tmp_path / "kraken_paper"
    kraken.mkdir()
    (kraken / "paper.sqlite3").touch()
    monkeypatch.setattr(state, "ROOT", health)
    monkeypatch.setattr(watchdog, "ROOT", health)
    monkeypatch.setattr(watchdog, "task_evidence", lambda: {})
    monkeypatch.setattr(monitor, "collect", lambda *args: ({}, {}))
    calls = []

    def producer():
        calls.append("producer")
        raise PermissionError("Synthetic unresolved producer permission")

    def cycle():
        calls.append("cycle")
        return {"state": "WAITING_WARMUP"}

    def collect(*args):
        calls.append("collect")
        return {}

    monkeypatch.setattr(managed, "ensure", producer)
    monkeypatch.setattr(worker, "cycle", cycle)
    monkeypatch.setattr(notifications, "collect", collect)
    assert "kraken_paper" not in watchdog.check(deliver=False, mode="operator") and calls == []
    result = watchdog.check(deliver=False, mode="scheduled")
    assert calls == ["producer", "cycle", "collect"]
    assert result["kraken_source_producer"]["state"] == "CHECK_FAILED"
    assert result["kraken_paper"]["state"] == "WAITING_WARMUP"
    assert result["kraken_paper_notifications"] == {}
