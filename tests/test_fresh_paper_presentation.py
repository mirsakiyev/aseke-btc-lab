from contextlib import closing
import json
import sqlite3
from types import SimpleNamespace

import httpx
import pytest
from streamlit.testing.v1 import AppTest

from lab.ema_paper import paper_view
from lab.ema_paper.private_notifications import ROUTE_META
from lab.health import __main__ as watchdog, fresh_paper, private_telegram, state
from lab.paper_run import gate, presentation
import lab.paper_run as paper_run
from lab.storage import utc


CUTOFF = utc("2026-11-17T06:15:00Z")
AT = utc("2026-11-17T06:16:03Z")
ROUTE = "0" * 64
SLOTS = (
    "breakout/day/fixed", "breakout/swing/fixed", "breakout/swing/trail", "breakout/position/fixed",
    "ema/day", "ema/swing", "ema/position",
)
EVENT_TABLE = """CREATE TABLE event (
    key TEXT PRIMARY KEY, book TEXT, kind TEXT, idea TEXT, source_at TEXT, recorded_at TEXT, body TEXT
);"""


def add_event(db, key, *, book, kind="HYPOTHETICAL_FILL", idea="idea", at=AT, body=None):
    db.execute("INSERT INTO event VALUES (?,?,?,?,?,?,?)",
               (key, book, kind, idea, at.isoformat(), at.isoformat(), json.dumps(body or {})))


def books(generation="successor"):
    return {
        slot: {"book_id": f"{generation}-{slot}", "run_id": f"run-{generation}-{slot}", "label": slot,
               "price_basis": "Kraken BTC/USD Spot" if slot.startswith("ema/") else "KuCoin XBTUSDTM mark",
               "freshness_seconds": 2, "state": "ACTIVE", "entry_model": "Fresh reference",
               "exit_model": "Fresh reference", "cash": 10000., "equity": 10000., "closed_net": 0.,
               "accounting_complete": True, "counts": {}}
        for slot in SLOTS
    }


@pytest.fixture
def ledgers(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path / "health")
    path = tmp_path / "paper.sqlite3"
    with state.connect() as health, closing(sqlite3.connect(path)) as source:
        source.executescript(EVENT_TABLE)
        state.set_meta(health, ROUTE_META, json.dumps({"route_id": ROUTE, "enabled_at": CUTOFF.isoformat()}))
        health.commit()
        yield health, source, path


def test_successor_outbox_reads_committed_current_rows_and_keeps_ema_observations_private(ledgers):
    health, source, path = ledgers
    journal = {"generation_id": "successor", "cutoff": CUTOFF, "books": books()}
    breakout, ema = journal["books"]["breakout/day/fixed"], journal["books"]["ema/day"]
    observation = {"price": 64279.82, "exchange_source_at": "2026-11-17T06:16:02Z",
                   "source_received_at": "2026-11-17T06:16:02.5Z", "paper_decision_at": "2026-11-17T06:16:02.6Z"}
    add_event(source, "opening", book=breakout["book_id"], kind="OPENING_BALANCE", idea="")
    add_event(source, "old", book="retired", body=observation)
    add_event(source, "too-early", book=breakout["book_id"], at=CUTOFF.replace(hour=5), body=observation)
    add_event(source, "breakout-entry", book=breakout["book_id"], body=observation)
    add_event(source, "ema-entry", book=ema["book_id"], body=observation)
    with closing(sqlite3.connect(path)) as reader:
        assert fresh_paper.collect(health, AT, view=journal, paper_db=reader)["state"] == "PRIVATE_SUMMARIES_ENABLED"
        assert not health.execute("SELECT 1 FROM outbox").fetchone()
        source.commit()
        results = fresh_paper.collect(health, AT, view=journal, paper_db=reader)
        assert set(results["books"]) == set(SLOTS)
        fresh_paper.collect(health, AT, view=journal, paper_db=reader)

    documents = [(row[0], json.loads(row[1])) for row in health.execute("SELECT text,event_json FROM outbox ORDER BY rowid")]
    assert len(documents) == 2
    assert [data["generation_id"] for _, data in documents] == ["successor"] * 2
    assert "64279.82" in documents[0][0] and "64279.82" not in documents[1][0]
    assert "Exchange event" in documents[0][0] and "Exchange event" not in documents[1][0]
    assert all(data["exchange_source_at"] == observation["exchange_source_at"] for _, data in documents)
    assert health.execute("SELECT count(*) FROM meta WHERE key LIKE 'fresh_paper_cursor_v1:%'").fetchone()[0] == 7
    assert health.execute("SELECT count(DISTINCT idea_id) FROM outbox").fetchone()[0] == 2

    messages = []

    def telegram(request):
        document = json.loads(request.content)
        assert document["chat_id"] == 123 and document["protect_content"] is False
        messages.append(document["text"])
        return httpx.Response(200, json={"ok": True, "result": {
            "message_id": len(messages), "chat": {"id": 123, "type": "private"}, "date": int(AT.timestamp())}})

    def private(text):
        route = private_telegram.Route(ROUTE, 123, "1:fake")
        return private_telegram.send(route, text, transport=httpx.MockTransport(telegram))

    def unexpected(_):
        pytest.fail("Fresh lifecycle events must use the verified private route")

    state.flush(health, AT.timestamp(), state.DEFAULTS, unexpected, private_send=private, private_route_id=ROUTE)
    assert len(messages) == 2
    assert health.execute("SELECT count(*) FROM outbox_attempt WHERE status='DELIVERED' AND dispatch_at IS NOT NULL "
                          "AND api_accepted_at IS NOT NULL").fetchone()[0] == 2


def test_legacy_queue_supersession_preserves_unrelated_incidents_and_delivery_history(ledgers):
    health, _, _ = ledgers
    items = [
        ("paper:retired", "paper_entry", "retired-book", "old-idea", "PENDING"),
        ("ema-private:retired", "paper_exit", "retired-book", "hashed-idea", "RETRY"),
        ("idea:old-idea", "ideas", None, None, "PENDING"),
        ("paper-update:old", "open_position", None, "old-idea", "PENDING"),
        ("paper:sent", "paper_exit", "retired-book", "old-idea", "DELIVERED"),
        ("ema-private:incident", "health", "retired-book", "old-idea", "PENDING"),
        ("paper:other", "paper_entry", "other-book", "old-idea", "PENDING"),
        ("kraken-paper:other", "paper_entry", "retired-book", "old-idea", "PENDING"),
        (fresh_paper.PREFIX + "successor", "paper_entry", "retired-book", "old-idea", "PENDING"),
    ]
    for identifier, category, book, idea, status in items:
        state.enqueue(health, identifier, category, "fixture", AT.timestamp(), idea_id=idea,
                      event_json=json.dumps({"book_id": book}) if book else None)
        health.execute("UPDATE outbox SET status=? WHERE id=?", (status, identifier))
    retired = fresh_paper.supersede_legacy(health, ["retired-book"], ["old-idea"], "successor", AT)
    assert retired == [item[0] for item in items[:4]]
    states = dict(health.execute("SELECT id,status FROM outbox"))
    assert all(states[name] == "SUPERSEDED" for name in retired)
    assert all(states[name] == status for name, _, _, _, status in items[4:])
    assert fresh_paper.supersede_legacy(health, ["retired-book"], ["old-idea"], "successor", AT) == []


def test_current_inventory_and_trade_stats_never_include_retired_book(ledgers, monkeypatch):
    _, source, path = ledgers
    view = {"generation_id": "successor", "cutoff": CUTOFF, "books": books()}
    marker = {"id": "retirement", "body": {"path": "private-archive", "cutoff": CUTOFF}}
    monkeypatch.setattr(gate, "retired", lambda: marker)
    monkeypatch.setattr(paper_run, "runtime", SimpleNamespace(
        status=lambda: view, connect=lambda readonly: sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)),
        raising=False)
    book = view["books"]["breakout/day/fixed"]
    add_event(source, "historic-loss", book="retired-book", kind="CLOSED", body={"net": -900.})
    add_event(source, "only-current", book=book["book_id"], kind="CLOSED",
              body={"net": 10., "net_r": 1., "fees": 0., "funding": 0., "holding_minutes": 1.})
    source.commit()
    from lab.paper_run import protection_reporting

    for item in view["books"].values():
        item.update(protection_reporting.summarize(source, item, item))
    current = presentation.current_view()
    assert set(current["books"]) == set(SLOTS)
    assert current["archive"] == "private-archive"
    assert current["books"]["breakout/day/fixed"]["win_rate"] == 1.
    assert current["books"]["ema/day"]["win_rate"] is None
    assert len(presentation.current_view(family="ema")["books"]) == 3
    assert [row["id"] for row in presentation.events("breakout/day/fixed", 10)] == ["only-current"]


def test_verified_retirement_routes_scheduled_watchdog_to_new_supervisor(tmp_path, monkeypatch):
    monkeypatch.setattr(watchdog, "ROOT", tmp_path)
    monkeypatch.setattr(gate, "retired", lambda: {"id": "archive-id"})
    monkeypatch.setattr(watchdog, "_dispatch_ema", lambda: pytest.fail("Legacy EMA writer ran after archive"))
    monkeypatch.setattr(watchdog, "_supervise_kraken_reference", lambda evidence: evidence.update(reference="supervised"))
    monkeypatch.setattr(paper_run, "runtime", SimpleNamespace(ensure_runner=lambda: {"state": "RUNNING"}), raising=False)
    monkeypatch.setattr(watchdog, "_check_health", lambda evidence, baseline, **options: evidence)
    assert watchdog.check(mode="scheduled", deliver=False) == {
        "fresh_paper_runner": {"state": "RUNNING"}, "reference": "supervised",
    }


def test_dashboard_renders_seven_current_slots_and_one_archive_pointer(monkeypatch):
    view = {"generation_id": "successor", "cutoff": CUTOFF, "retirement_id": "old",
            "archive": "private-archive", "books": books()}
    monkeypatch.setattr(presentation, "current_view", lambda **options: view)
    monkeypatch.setattr(paper_view, "_delivery", lambda _: [])
    app = AppTest.from_string("from lab.paper_run.presentation import panel\npanel()").run()
    assert not app.exception
    assert app.dataframe[0].value["Paper book"].tolist() == list(SLOTS)
    assert [item.label for item in app.expander] == ["Archived paper history"]
    assert "retired-book" not in str(app.dataframe[0].value)
