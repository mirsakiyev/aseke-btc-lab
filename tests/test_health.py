from contextlib import closing
from io import StringIO
import sqlite3
from types import SimpleNamespace

import httpx
import pandas as pd
import psutil
import pytest

from lab.health import dashboard_service, delivery, monitor, state
from lab.health import paper_events
from lab.phase15 import catalog, paper
from lab.storage import utc


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path)
    return state.connect


def test_incident_debounce_cooldown_recovery_and_restart(ledger):
    sent = []

    def send(message):
        sent.append(message)
        return delivery.Delivery("DELIVERED", "accepted", message_id=len(sent))

    cfg = state.policy()
    failure = {"Coinbase 5m": ("CRITICAL", "stalled", False)}
    for now in (100, 280, 460):
        with ledger() as db:
            state.observe(db, failure, now, cfg)
            state.flush(db, now, cfg, send)
        assert len(sent) == (0 if now == 100 else 1)

    now = 280 + cfg["reminder_minutes"] * 60
    with ledger() as db:
        state.observe(db, failure, now, cfg)
        state.flush(db, now, cfg, send)
    assert len(sent) == 2
    for now in (now + 180, now + 360):
        with ledger() as db:
            state.observe(db, {"Coinbase 5m": (None, "advanced", False)}, now, cfg)
            state.flush(db, now, cfg, send)
    with ledger() as db:
        assert not state.status(db)["incidents"]
        assert state.status(db)["delivery_counts"] == {"DELIVERED": 3}
    assert "RECOVERY" in sent[-1] and "Degraded for approximately" in sent[-1]


def test_durable_rate_limit_and_ambiguous_delivery(ledger, monkeypatch):
    monkeypatch.setattr(delivery, "credentials", lambda: ("test-secret", "fixture-chat"))
    replies = iter((429, 200, "read timeout"))
    called = []

    def post(request):
        called.append(request)
        answer = next(replies)
        if answer == "read timeout":
            raise httpx.ReadTimeout("simulated timeout")
        if answer == 429:
            return httpx.Response(429, headers={"Retry-After": "75"}, json={"parameters": {"retry_after": 90}})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

    send = lambda message: delivery.send(message, transport=httpx.MockTransport(post))
    cfg = state.policy()
    with ledger() as db:
        state.enqueue(db, "a", "health", "first", 10)
        state.enqueue(db, "b", "health", "second", 10)
        state.flush(db, 10, cfg, send)
        assert state.status(db)["delivery_counts"] == {"PENDING": 1, "RETRY": 1}
    with ledger() as db:
        state.flush(db, 99, cfg, send)
        assert len(called) == 1
        state.flush(db, 100, cfg, send)
        state.flush(db, 1000, cfg, send)
        assert len(called) == 3
        assert state.status(db)["delivery_counts"] == {"DELIVERED": 1, "UNCERTAIN": 1}
        assert db.execute("SELECT message_id FROM outbox WHERE id='a'").fetchone()[0] == 42
        state.enqueue(db, "interrupted", "health", "already attempted", 1000)
        db.execute("UPDATE outbox SET status='IN_FLIGHT' WHERE id='interrupted'")
        db.commit()
    with ledger() as db:
        state.flush(db, 2000, cfg, send)
        assert len(called) == 3
        assert state.status(db)["delivery_counts"] == {"DELIVERED": 1, "UNCERTAIN": 2}


def test_telegram_connect_failure_remains_retryable(monkeypatch):
    monkeypatch.setattr(delivery, "credentials", lambda: ("test-secret", "fixture-chat"))

    def post(_request):
        raise httpx.ConnectError("simulated refusal")

    result = delivery.send("fixture", transport=httpx.MockTransport(post))
    assert result.state == "RETRY" and "test-secret" not in result.detail


def test_api_acceptance_records_actual_request_and_server_times(ledger, monkeypatch):
    monkeypatch.setattr(delivery, "credentials", lambda: ("test-secret", "fixture-chat"))
    times = iter((100.2, 100.7))
    monkeypatch.setattr(delivery, "wall_time", lambda: next(times))

    def post(request):
        assert "Telegram request started" in request.content.decode()
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 42, "date": 100}})

    clock = iter((100.1, 100.8))
    with ledger() as db:
        state.enqueue(db,"entry","paper_entry","fixture",99,idea_id="test-idea")
        state.flush(db,100,state.policy(),lambda msg: delivery.send(msg, transport=httpx.MockTransport(post)),
                    clock=lambda: next(clock))
        row = state.status(db, idea="test-idea")["recent_alerts"][0]
        assert (row["created"],row["dispatch_at"],row["request_started"],row["request_completed"],
                row["delivered"],row["api_accepted_at"],row["telegram_date"],row["message_id"]) == (
                    99,100.1,100.2,100.7,100.7,100.7,100,42)
        assert row["attempt_history"][0]["api_accepted_at"] == 100.7


def test_per_idea_retry_order_and_bound(ledger):
    cfg = {**state.policy(), "telegram_max_attempts": 2}
    sent = []

    def send(text):
        sent.append(text)
        return delivery.Delivery("RETRY" if text == "entry" else "DELIVERED", "fixture")

    with ledger() as db:
        state.enqueue(db, "entry", "paper_entry", "entry", 10, idea_id="first")
        state.enqueue(db, "exit", "paper_exit", "exit", 10, idea_id="first")
        state.enqueue(db, "other", "paper_exit", "other", 10, idea_id="other")
        state.flush(db,10,cfg,send)
    with ledger() as db:
        state.flush(db,11,cfg,send)
        assert sent == ["entry", "other"]
        state.flush(db,40,cfg,send)
    with ledger() as db:
        state.flush(db,41,cfg,send)
        assert sent == ["entry", "other", "entry", "exit"]
        assert state.status(db)["delivery_counts"] == {"DELIVERED":2,"FAILED":1}


@pytest.mark.parametrize("fixed_target", [None, 110])
def test_saved_phase15_publication_once_and_no_watch_signals(ledger, tmp_path, monkeypatch, fixed_target):
    monkeypatch.setattr(catalog, "ROOT", tmp_path / "paper")
    now = utc("2026-11-10T17:00:00Z")
    epoch = int(now.timestamp())
    lane = {"profile": "swing", "mode": {"symbol": "XBTUSDTM"}, "risk": {"starting_equity": 1000}, "book_id": "fixture"}
    analysis = {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": now.isoformat(), "direction": "LONG",
                "strategy_version": "fixture-v1", "exit_policy": "trailing" if fixed_target is None else "fixed_r_v1",
                "plan": {"eligible_from": "2026-11-10T17:01:00Z", "entry_expiry": "2026-11-10T17:15:00Z",
                         "entry": {"trigger": 100, "minimum": 99, "maximum": 101}, "stop": 95, "fixed_target": fixed_target,
                         "target_r": 2 if fixed_target is not None else None, "trailing_atr": 2 if fixed_target is None else None,
                         "max_hold_minutes": 720}}
    sent = []

    def send(message):
        sent.append(message)
        return delivery.Delivery("DELIVERED", "accepted")

    with closing(catalog.db_open()) as source:
        book = paper.initial(lane)
        for observation in ("WATCHING", "AWAITING_CONFIRMATION", "NO_TRADE", "DATA_NOT_READY"):
            paper.event(source, book, observation, observation, now, now, {})
            assert paper.publish(source, book, {**analysis, "state": observation}, now) is None
        with ledger() as db:
            monitor.publications(source, db, {"fixture": lane}, epoch - 180, epoch)
            assert not state.status(db)["recent_alerts"]
        idea = paper.publish(source, book, analysis, now)
        source.commit()
    for checked in (epoch, epoch + 180):
        with closing(catalog.db_open(readonly=True)) as source, ledger() as db:
            monitor.publications(source, db, {"fixture": lane}, epoch - 180, checked)
            state.flush(db, checked, state.policy(), send)
            assert state.status(db)["delivery_counts"] == {"DELIVERED": 1}
    assert len(sent) == 1
    exit_rule = ("ATR trailing (2)" if fixed_target is None else
                 "Illustrative 2R target 110. Recomputed from the modeled fill.")
    assert all(value in sent[0] for value in (idea["id"], "XBTUSDTM", "99–101", exit_rule, "EST", "fixture-v1"))


@pytest.mark.parametrize(("code","reason"), [
    ("ENTRY_OUTSIDE_FROZEN_RANGE","outside the frozen range"),
    ("INSUFFICIENT_EXECUTABLE_CAPACITY","Insufficient executable capacity"),
    (None,"Strategy invalidated explicitly"),
])
def test_terminal_unfilled_dedup(ledger, tmp_path, monkeypatch, code, reason):
    monkeypatch.setattr(catalog,"ROOT",tmp_path/"paper")
    now = utc("2026-11-11T14:31Z")
    lane = {"profile":"day","mode":{"symbol":"XBTUSDTM"}}
    lanes = {"book":lane}
    with closing(catalog.db_open()) as source, ledger() as db:
        paper_events.scan(source,db,"reg",lanes,{},now,state.policy())
        book = {"book_id":"book"}
        idea = {"id":"idea","exit_policy":"fixed_r_v1","direction":"SHORT","published_at":now.isoformat()}
        paper.event(source,book,"PUBLISHED","idea",now,now,idea)
        for delay in (1,2):
            at = utc("2026-11-11T14:32Z") + pd.Timedelta(minutes=delay)
            paper.event(source,book,"INVALIDATED","idea",at,at,
                        {"reason":"Strategy invalidated explicitly","reason_code":code})
        paper_events.scan(source,db,"reg",lanes,{},now,state.policy())
        paper_events.scan(source,db,"reg",lanes,{},now,state.policy())
        row = db.execute("SELECT text,event_json FROM outbox").fetchall()
        assert len(row) == 1 and reason in row[0]["text"] and "09:31:00 EST" in row[0]["text"]


def test_late_commit_resolves_orphan_close_in_fill_order(ledger, tmp_path, monkeypatch):
    monkeypatch.setattr(catalog,"ROOT",tmp_path/"paper")
    now = utc("2026-11-11T15:00Z")
    earlier = utc("2026-11-11T14:32Z")
    lane = {"profile":"day","mode":{"symbol":"XBTUSDTM"}}
    lanes = {"book":lane}
    book = {"book_id":"book"}
    idea = {"id":"idea","exit_policy":"fixed_r_v1","direction":"SHORT","published_at":earlier.isoformat(),
            "plan":{"stop":110,"target_r":2,"sizing":{"equity":1000}}}
    closed = {"reason":"stop","entry_at":earlier.isoformat(),"exit_price":110,"holding_minutes":28,
              "gross_price_pnl":-10,"fees":1,"funding":0,"liquidation_cost":0,"unfunded_deficit":0,
              "net":-11,"net_r":-1.1,"adverse_cost_in_gross":.5,"equity_before":1000,"equity_after":989}
    with closing(catalog.db_open()) as source, ledger() as db:
        paper_events.scan(source,db,"reg",lanes,{},now,state.policy())
        paper.event(source,book,"PUBLISHED","idea",earlier,earlier,idea)
        paper.event(source,book,"CLOSED","idea",now,now,closed)
        paper.event(source,book,"EXPIRED","idea",now,now,{"reason":"Entry window elapsed"})
        source.commit()
        paper_events.scan(source,db,"reg",lanes,{},now,state.policy())
        assert not db.execute("SELECT 1 FROM outbox").fetchone()
        db.commit()
    with closing(catalog.db_open()) as source, ledger() as db:
        paper.event(source,book,"HYPOTHETICAL_FILL","idea",earlier,earlier,
                    {"price":100,"quantity":1,"planned_risk":10,"target":80,"stop":110})
        source.commit()
        paper_events.scan(source,db,"reg",lanes,{},now,state.policy())
        assert [row[0] for row in db.execute("SELECT category FROM outbox ORDER BY rowid")] == ["paper_entry","paper_exit"]
        assert "+0.0000" in db.execute("SELECT text FROM outbox WHERE category='paper_exit'").fetchone()[0]


@pytest.mark.parametrize(("kind","label"), [
    ("stop","STOP"),("target","TARGET"),("trailing_stop","TRAILING STOP"),("session_flat","SESSION FLAT"),
])
def test_saved_exit_kind_and_accounting_warning(kind,label):
    t = "2026-11-11T15:00:00+00:00"
    event = (2,"book","idea",t,t,"CLOSED","{}","event")
    lane = {"profile":"swing","mode":{"symbol":"XBTUSDTM"}}
    idea = {"exit_policy":"atr_trail_v1","direction":"SHORT"}
    closed = {"reason":"stop","exit_kind":kind,"entry_at":t,"exit_price":105,"holding_minutes":60,
              "gross_price_pnl":-10,"fees":2,"funding":1,"liquidation_cost":0,"unfunded_deficit":0,
              "net":-11,"net_r":-.5,"adverse_cost_in_gross":.5,"equity_before":1000,"equity_after":989}
    text,_ = paper_events.close_text(lane,idea,event,closed,{"price":100})
    assert f"Exit {label}" in text and "Ledger net -11.00000000" in text and "989.0000" in text
    assert "included in gross" in text
    text,_ = paper_events.close_text(lane,idea,event,{**closed,"equity_after":999},{"price":100})
    assert "Accounting warning" in text


def test_open_position_updates_opt_in_and_expire_when_flat(ledger):
    now = utc("2026-11-11T15:45Z")
    cfg = state.policy()
    lane = {"profile":"day","mode":{"symbol":"XBTUSDTM","trade_type":"FUTURES"}}
    position = {"id":"idea","direction":"SHORT","exit_policy":"fixed_r_v1","entry_at":"2026-11-11T15:00Z",
                "deadline":"2026-11-11T23:00Z","last_mark":101,"stop":110,
                "mark_source_at":"2026-11-11T15:00Z","mark_received_at":"2026-11-11T15:01Z",
                "position":{"entry":100,"remaining":.04}}
    books = {"day":{"book_id":"book","position":position,"unrealized":-.04,"problem":None}}
    with ledger() as db:
        paper_events.open_updates(db,{"book":lane},books,now,cfg)
        assert not db.execute("SELECT 1 FROM outbox").fetchone()
        cfg["categories"]["open_position"] = True
        for _ in range(2):
            paper_events.open_updates(db,{"book":lane},books,now,cfg)
        rows = db.execute("SELECT text,status FROM outbox").fetchall()
        assert len(rows)==1 and "Stale or unverified at check" in rows[0]["text"]
        books["day"]["position"] = None
        paper_events.open_updates(db,{"book":lane},books,now,cfg)
        assert db.execute("SELECT status FROM outbox").fetchone()[0]=="SUPERSEDED"


def test_open_at_activation_is_reported_without_entry_alert(ledger, tmp_path, monkeypatch):
    monkeypatch.setattr(catalog,"ROOT",tmp_path/"paper")
    now = utc("2026-11-11T15:45Z")
    record = {"id":"open-idea","direction":"SHORT","entry_at":"2026-11-11T15:00Z",
              "position":{"entry":100,"remaining":.04}}
    with closing(catalog.db_open()) as source, ledger() as db:
        paper.event(source,{"book_id":"book"},"HYPOTHETICAL_FILL","open-idea",now,now,{})
        source.commit()
        baseline = paper_events.scan(source,db,"reg",{},{"day/fixed":{"position":record}},now,state.policy())
        assert baseline["baseline_rowid"] == 1
        assert baseline["open_at_activation"] == [{"book":"day/fixed","idea":"open-idea","direction":"SHORT",
                                                    "entry_at":"2026-11-11T15:00Z","entry_price":100,"quantity":.04}]
        assert not db.execute("SELECT 1 FROM outbox").fetchone()


def test_independent_check_detects_stopped_worker(ledger, monkeypatch):
    from lab.phase2 import store
    from lab.phase7 import operation

    monkeypatch.setattr(operation, "health", lambda: {
        "worker": {"intentionally_paused": False, "process_alive": False, "pid": 1000},
        "heartbeat_age_seconds": 10, "worker_lock_held": False, "process_state": "process_failed", "process": None,
        "feeds": {str(cadence): {"last_observed_close": None, "scanned_through": None} for cadence in (300, 3600)},
    })
    with closing(sqlite3.connect(":memory:")) as source, ledger() as db:
        source.execute("CREATE TABLE responses (id INTEGER, seconds INTEGER, problem TEXT)")
        monkeypatch.setattr(store, "connect", lambda **_: source)
        findings = {}
        cfg = state.policy()
        report = monitor.coinbase(utc("2026-11-10T17:00:00Z"), {monitor.FORWARD: {"state": "READY", "result": 0}}, cfg, findings)
        state.observe(db, findings, 100, cfg)
        assert report["state"] == "process_failed"
        assert state.status(db)["delivery_counts"] == {"PENDING": 1}
        assert state.status(db)["incidents"][0]["key"] == "Coinbase worker"


def test_dashboard_uses_bounded_restart_and_never_spawns_over_conflict(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard_service, "ROOT", tmp_path)
    monkeypatch.setattr(dashboard_service.time, "sleep", lambda _: None)
    monkeypatch.setattr(dashboard_service, "inspect", lambda **_: {"state": "NO_LISTENER"})
    spawned = []

    def spawn(args, **_):
        spawned.append(args)
        return SimpleNamespace(pid=len(spawned), stdout=StringIO("fixture exit\n"), wait=lambda: 1, poll=lambda: 1)

    monkeypatch.setattr(dashboard_service.subprocess, "Popen", spawn)
    assert dashboard_service.serve() == 1
    assert len(spawned) == 4 and "127.0.0.1" in spawned[0]
    assert dashboard_service.read_json(tmp_path / "dashboard-supervisor.json")["state"] == "RETRY_BUDGET_EXHAUSTED"
    monkeypatch.setattr(dashboard_service, "inspect", lambda **_: {"state": "PORT_CONFLICT"})
    assert dashboard_service.serve() == 1
    assert len(spawned) == 4


def test_dashboard_http_availability_does_not_prove_process_identity(monkeypatch):
    def denied():
        raise psutil.AccessDenied()

    process = SimpleNamespace(cmdline=denied, create_time=lambda: 1, memory_info=lambda: SimpleNamespace(rss=100))
    row = SimpleNamespace(status=psutil.CONN_LISTEN, pid=1000, laddr=SimpleNamespace(port=8501, ip="127.0.0.1"))
    monkeypatch.setattr(dashboard_service.psutil, "Process", lambda _: process)
    monkeypatch.setattr(dashboard_service.psutil, "net_connections", lambda **_: [row])

    class Response:
        status = 200

        def read(self, _):
            return b"ok"

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    monkeypatch.setattr(dashboard_service, "build_opener", lambda _: SimpleNamespace(open=lambda *_, **__: Response()))
    assert dashboard_service.inspect()["state"] == "HTTP_HEALTHY_IDENTITY_UNVERIFIED"
    assert dashboard_service.inspect(probe=False)["state"] == "IDENTITY_UNKNOWN"
