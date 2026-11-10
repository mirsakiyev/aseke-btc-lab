from contextlib import closing
from io import StringIO
import sqlite3
from types import SimpleNamespace

import httpx
import psutil
import pytest

from lab.health import dashboard_service, delivery, monitor, state
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


def test_saved_phase15_publication_once_and_no_watch_signals(ledger, tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "ROOT", tmp_path / "paper")
    now = utc("2026-11-10T17:00:00Z")
    epoch = int(now.timestamp())
    lane = {"profile": "swing", "mode": {"symbol": "XBTUSDTM"}, "risk": {"starting_equity": 1000}, "book_id": "fixture"}
    analysis = {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": now.isoformat(), "direction": "LONG",
                "strategy_version": "fixture-v1", "exit_policy": "trailing",
                "plan": {"eligible_from": "2026-11-10T17:01:00Z", "entry_expiry": "2026-11-10T17:15:00Z",
                         "entry": {"trigger": 100, "minimum": 99, "maximum": 101}, "stop": 95, "fixed_target": None,
                         "trailing_atr": 2, "max_hold_minutes": 720}}
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
    assert all(value in sent[0] for value in (idea["id"], "XBTUSDTM", "99–101", "ATR trailing (2)", "EST", "fixture-v1"))


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
