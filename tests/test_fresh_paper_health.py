from contextlib import closing
from copy import deepcopy
import json
import sqlite3
from types import SimpleNamespace
from types import ModuleType
from importlib.machinery import ModuleSpec
import sys

import pandas as pd
import pytest

from lab.ema_paper.private_notifications import ROUTE_META
from lab.health import delivery, fresh_paper_health as monitor, state
from lab.paper_run import gate
from lab.storage import utc


NOW = utc("2026-11-18T23:20:00Z")
ROUTE = "a" * 64


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path / "health")
    monkeypatch.setattr(gate, "ROOT", tmp_path / "paper")
    source = sqlite3.connect(":memory:")
    source.executescript("CREATE TABLE book(id TEXT PRIMARY KEY,state TEXT);"
                         "CREATE TABLE event(key TEXT PRIMARY KEY,book TEXT,kind TEXT,idea TEXT,"
                         "source_at TEXT,recorded_at TEXT,body TEXT);")
    record = {"id": "generation", "body": {"lanes": {}}}
    view = {"generation_id": "generation", "books": {}}
    with state.connect() as db, db:
        state.set_meta(db, ROUTE_META, json.dumps({"route_id": ROUTE, "enabled_at": NOW.isoformat(), "revision_id": "old"}))
    result = SimpleNamespace(source=source, record=record, view=view)
    add_book(result, "ema/day", blocked="UNRESOLVED_PROTECTION_INTERVAL")
    yield result
    source.close()


def add_book(world, slot, *, blocked=None, position=None, pending=None):
    book_id = "book-" + slot
    book = {"book_id": book_id, "run_id": book_id, "label": slot, "state": "BLOCKED" if blocked else "ACTIVE",
            "reason": blocked, "problem": blocked, "historical_problem": blocked, "permanent_block": False,
            "position": position, "pending": pending, "prospective_entry_permission": blocked is None,
            "prospective_admission": None, "qualification": {"state": "QUALIFIED"}}
    world.record["body"]["lanes"][slot] = {"book_id": book_id}
    world.view["books"][slot] = book
    save_book(world, slot)
    return book


def save_book(world, slot):
    book = world.view["books"][slot]
    world.source.execute("INSERT OR REPLACE INTO book VALUES(?,?)", (book["book_id"], json.dumps(book)))
    world.source.commit()


def event(world, kind, *, slot="ema/day", identifier=None, at=NOW, **body):
    number = world.source.execute("SELECT count(*) FROM event").fetchone()[0] + 1
    identifier = identifier or "event-" + str(number)
    world.source.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)", (
        identifier, world.record["body"]["lanes"][slot]["book_id"], kind, "position", at.isoformat(), at.isoformat(), json.dumps(body)))
    world.source.commit()
    return identifier


def scan(world, *, at=NOW, consistent=True):
    with state.connect() as db, db:
        return monitor.collect(db, at, view=world.view, paper_db=world.source,
                               registration=world.record, consistent=consistent, current_gates={
                                   "clock": True, "entries_enabled": True,
                                   "books": {lane["book_id"]: True for lane in world.record["body"]["lanes"].values()}})


def alerts():
    with state.connect() as db:
        return [dict(r) for r in db.execute("SELECT * FROM outbox ORDER BY rowid")]


def resume(world, *, slot="ema/day", effective=NOW, at=NOW):
    identifier = event(world, "BOOK_PROSPECTIVE_RESUME", slot=slot, at=at,
                       generation=world.record["id"], effective_at=effective.isoformat())
    book = world.view["books"][slot]
    book.update(state="ACTIVE", reason=None, prospective_entry_permission=True,
                prospective_admission={"event_id": identifier, "effective_at": effective.isoformat()})
    save_book(world, slot)
    return identifier


def test_commission_reports_current_block_without_replaying_history_and_restarts_deduplicate(world):
    event(world, "PROTECTION_INCIDENT_OPENED", incident_id="historic", reason="old")
    first = scan(world)
    assert first["books"]["ema/day"]["initial_baseline"]
    assert len(alerts()) == 1 and "PAPER BOOK BLOCKED" in alerts()[0]["text"]
    for _ in range(3):
        scan(world, at=NOW + pd.Timedelta(seconds=10))
    assert len(alerts()) == 1
    assert "historic" not in alerts()[0]["text"]


def test_changed_reason_and_distinct_incident_alert_while_already_blocked(world):
    scan(world)
    world.view["books"]["ema/day"]["reason"] = "WORKSTATION_CLOCK_SYNC_UNVERIFIED"
    save_book(world, "ema/day")
    scan(world)
    event(world, "PROTECTION_INCIDENT_OPENED", incident_id="new-incident", reason="KRAKEN_STREAM_DISCONTINUITY",
          source_correlation_id="shared-source-episode")
    scan(world)
    assert len(alerts()) == 3
    assert "WORKSTATION_CLOCK_SYNC_UNVERIFIED" in alerts()[1]["text"]
    assert "new-incident" in alerts()[2]["text"]
    assert json.loads(alerts()[2]["event_json"])["operational_event"]["source_correlation_id"] == "shared-source-episode"
    event(world, "PROTECTION_INCIDENT_OPENED", incident_id="new-incident", reason="duplicate same incident")
    scan(world)
    assert len(alerts()) == 3


def test_open_recovery_and_resume_between_polls_preserve_separate_truthful_notices(world):
    book = world.view["books"]["ema/day"]
    book.update(state="ACTIVE", problem=None, reason=None, historical_problem=None, prospective_entry_permission=True)
    save_book(world, "ema/day")
    scan(world)
    assert not alerts()
    event(world, "PROTECTION_INCIDENT_OPENED", incident_id="brief", reason="TRANSPORT_INTERRUPTED")
    event(world, "PROTECTION_INCIDENT_RECOVERED", incident_id="brief")
    book.update(problem="UNRESOLVED_PROTECTION_INTERVAL", historical_problem="UNRESOLVED_PROTECTION_INTERVAL")
    resume(world)
    result = scan(world)
    texts = [a["text"] for a in alerts()]
    assert len(texts) == 3
    assert "PROTECTION INCIDENT" in texts[0]
    assert "OBSERVATIONS RESUMED" in texts[1] and "alone does not restore" in texts[1]
    assert "ENTRY ELIGIBILITY RESTORED" in texts[2] and "Historical uncertainty" in texts[2]
    assert result["books"]["ema/day"]["entry_available"]


@pytest.mark.parametrize("obstruction", ["newer_incident", "backlog", "inconsistent", "occupied", "pending"])
def test_stale_snapshot_old_resume_backlog_and_occupancy_cannot_announce_eligibility(world, monkeypatch, obstruction):
    scan(world)
    event(world, "PROTECTION_INCIDENT_OPENED", incident_id="first")
    resume(world)
    if obstruction == "newer_incident":
        event(world, "PROTECTION_INCIDENT_OPENED", incident_id="newer")
    elif obstruction == "backlog":
        monkeypatch.setattr(monitor, "MAX_EVENTS", 1)
    elif obstruction in ("occupied", "pending"):
        world.view["books"]["ema/day"]["position" if obstruction == "occupied" else "pending"] = {"id": obstruction}
        save_book(world, "ema/day")
    scan(world, consistent=obstruction != "inconsistent")
    assert not any("ELIGIBILITY RESTORED" in a["text"] for a in alerts())


def test_new_book_is_discovered_after_start_and_needs_no_family_specific_code(world):
    scan(world)
    book = add_book(world, "arbitrage/eighth", blocked="CLOCK_UNVERIFIED", position={"id": "eighth-position"})
    result = scan(world)
    assert "arbitrage/eighth" in result["books"]
    assert any(book["book_id"] in a["text"] and "Exposure OPEN" in a["text"] for a in alerts())
    event(world, "PROTECTION_INCIDENT_OPENED", slot="arbitrage/eighth", incident_id="eighth-incident")
    scan(world)
    assert any("eighth-incident" in a["text"] for a in alerts())


@pytest.mark.parametrize("damage", ["missing", "malformed", "wrong_id", "bad_exposure", "corrupt_state", "corrupt_event"])
def test_one_bad_registered_book_does_not_suppress_other_books(world, damage):
    add_book(world, "other/book", blocked="SOURCE_RESTRICTED")
    scan(world)
    if damage == "missing":
        del world.view["books"]["ema/day"]
    elif damage == "malformed":
        world.view["books"]["ema/day"] = []
    elif damage == "wrong_id":
        world.view["books"]["ema/day"]["book_id"] = "wrong"
    elif damage == "bad_exposure":
        world.view["books"]["ema/day"]["position"] = "not-an-object"
    elif damage == "corrupt_state":
        world.source.execute("UPDATE book SET state='not json' WHERE id='book-ema/day'")
    else:
        event(world, "PROTECTION_INCIDENT_OPENED", incident_id="damaged")
        world.source.execute("UPDATE event SET body='not json' WHERE book='book-ema/day'")
    world.source.commit()
    event(world, "PROTECTION_INCIDENT_OPENED", slot="other/book", incident_id="other-new")
    result = scan(world)
    assert any("other-new" in a["text"] for a in alerts())
    assert any("INSPECTION FAILED" in a["text"] for a in alerts())
    assert result["books"]["other/book"]["cursor"] > 0


def test_future_admission_is_labelled_pending_not_restored(world):
    scan(world)
    book = world.view["books"]["ema/day"]
    book["reason"] = "A changed restriction"
    book["prospective_admission"] = {"effective_at": (NOW + pd.Timedelta(minutes=10)).isoformat(), "event_id": "future"}
    save_book(world, "ema/day")
    scan(world)
    assert "FUTURE ADMISSION PENDING" in alerts()[-1]["text"]
    assert "await that future boundary" in alerts()[-1]["text"]


def test_operational_queue_has_no_trade_idea_blocker_and_delivery_is_private(world):
    scan(world)
    with state.connect() as db, db:
        state.enqueue(db, "trade-retry", "paper_entry", "older trade", NOW.timestamp(), idea_id="idea")
        db.execute("UPDATE outbox SET status='RETRY',due=? WHERE id='trade-retry'", (NOW.timestamp() + 600,))
        sent = []
        state.flush(db, NOW.timestamp(), state.policy(), lambda _: pytest.fail("Private health used legacy sender"),
                    private_route_id=ROUTE, private_send=lambda text: sent.append(text) or delivery.Delivery(
                        "DELIVERED", "accepted", message_id=5, request_started=1, request_completed=2, api_accepted_at=2))
    assert len(sent) == 1 and "PAPER BOOK BLOCKED" in sent[0]
    row = alerts()[0]
    assert row["idea_id"] is None and row["message_id"] == 5


@pytest.mark.parametrize("failure", ["mute", "rate_limit", "uncertain"])
def test_delivery_preserves_mute_rate_limit_and_ambiguity(world, failure):
    scan(world)
    sends = []
    cfg = state.policy()
    cfg["mute"] = failure == "mute"
    result = delivery.Delivery("RETRY", "Telegram rate limit", retry_after=20) if failure == "rate_limit" else delivery.Delivery(
        "UNCERTAIN", "response lost")
    with state.connect() as db:
        state.flush(db, NOW.timestamp(), cfg, lambda _: pytest.fail("wrong sender"), private_route_id=ROUTE,
                    private_send=lambda text: sends.append(text) or result)
    row = alerts()[0]
    assert row["status"] == {"mute": "PENDING", "rate_limit": "RETRY", "uncertain": "UNCERTAIN"}[failure]
    assert len(sends) == (0 if failure == "mute" else 1)


def test_outbox_cursor_transaction_rolls_back_together_on_interruption(world, monkeypatch):
    scan(world)
    event(world, "PROTECTION_INCIDENT_OPENED", incident_id="later")
    before = deepcopy(alerts())
    original = monitor.set_meta

    def interrupted(db, key, value):
        original(db, key, value)
        if key.startswith(monitor.META) and key != monitor.META + "last":
            raise KeyboardInterrupt()

    monkeypatch.setattr(monitor, "set_meta", interrupted)
    with pytest.raises(KeyboardInterrupt):
        scan(world)
    assert alerts() == before
    monkeypatch.setattr(monitor, "set_meta", original)
    scan(world)
    assert len(alerts()) == 2


def test_scanner_failure_is_durable_deduplicated_and_has_no_raw_payload(world):
    with state.connect() as db, db:
        for _ in range(2):
            monitor.failed(db, NOW, "PAPER_OPERATIONAL_SCANNER_ValueError")
    assert len(alerts()) == 1 and "MONITOR CHECK FAILED" in alerts()[0]["text"]
    assert "token" not in alerts()[0]["text"]


def test_default_scan_detects_snapshot_journal_race(world, monkeypatch):
    from lab.paper_run import runtime

    scan(world)
    event(world, "PROTECTION_INCIDENT_OPENED", incident_id="reviewed")
    resume(world)
    snapshot = deepcopy(world.view)

    class Source:
        def execute(self, *args):
            return world.source.execute(*args)

        def close(self):
            pass

    def race():
        event(world, "PROTECTION_INCIDENT_OPENED", incident_id="new-after-snapshot")
        return snapshot

    monkeypatch.setattr(runtime, "current", lambda: world.record)
    monkeypatch.setattr(runtime, "connect", lambda **_: Source())
    monkeypatch.setattr(runtime, "status", race)
    monkeypatch.setattr(monitor, "_current_gates", lambda *a: {"clock": True, "entries_enabled": True,
                        "books": {lane["book_id"]: True for lane in world.record["body"]["lanes"].values()}})
    with state.connect() as db, db:
        result = monitor.collect(db, NOW)
    assert not result["snapshot_consistent"]
    assert not any("ELIGIBILITY RESTORED" in row["text"] for row in alerts())
    assert any("new-after-snapshot" in row["text"] for row in alerts())


def test_whole_status_failure_still_scans_valid_book_journal(world, monkeypatch):
    from lab.paper_run import runtime

    add_book(world, "valid/other", blocked="SOURCE_RESTRICTED")
    scan(world)
    event(world, "PROTECTION_INCIDENT_OPENED", slot="valid/other", incident_id="valid-incident")

    class Source:
        def execute(self, *args):
            return world.source.execute(*args)

        def close(self):
            pass

    def unavailable():
        raise ValueError("one malformed book")

    monkeypatch.setattr(runtime, "current", lambda: world.record)
    monkeypatch.setattr(runtime, "connect", lambda **_: Source())
    monkeypatch.setattr(runtime, "status", unavailable)
    monkeypatch.setattr(monitor, "_current_gates", lambda *a: {"clock": True, "entries_enabled": True,
                        "books": {lane["book_id"]: True for lane in world.record["body"]["lanes"].values()}})
    with state.connect() as db, db:
        result = monitor.collect(db, NOW)
    assert result["state"] == "PARTIAL"
    assert any("MONITOR CHECK FAILED" in row["text"] for row in alerts())
    assert any("valid-incident" in row["text"] for row in alerts())


def test_authorized_operational_test_is_idempotent_and_uses_normal_health_policy(world):
    with state.connect() as db, db:
        a = monitor.queue_test(db, NOW, "one-operator-authorized-test")
        b = monitor.queue_test(db, NOW, "one-operator-authorized-test")
    assert a["outbox_id"] == b["outbox_id"] and len(alerts()) == 1
    row = alerts()[0]
    assert row["category"] == "health" and row["idea_id"] is None
    assert "TEST" in row["text"] and "No real incident or trade occurred" in row["text"]
    assert len(row["text"]) <= 1000


def test_health_module_does_not_change_financial_runner_fingerprint():
    from lab.paper_run import runtime
    before = runtime.RUNNER_BUILD
    import importlib
    importlib.reload(monitor)
    assert runtime.RUNNER_BUILD == before


@pytest.mark.parametrize("gate", ["clock", "entries_enabled", "book", "qualification"])
def test_active_label_cannot_restore_when_real_current_gate_is_closed(world, monkeypatch, gate):
    from lab.paper_run import runtime

    add_book(world, "breakout/day/fixed", blocked="CLOCK_UNVERIFIED")
    scan(world)
    resume(world, slot="breakout/day/fixed")
    if gate == "qualification":
        world.view["books"]["breakout/day/fixed"]["qualification"]["state"] = "UNVERIFIED"
        save_book(world, "breakout/day/fixed")
    monkeypatch.setattr(runtime, "synchronized", lambda _: gate != "clock")
    monkeypatch.setattr(runtime, "_can_enter", lambda lane, *_: gate not in {"book", "entries_enabled"})
    monkeypatch.setattr(runtime, "_source_permission", lambda *_a, **_kw: None)
    with state.connect() as db, db:
        result = monitor.collect(db, NOW, view=world.view, paper_db=world.source, registration=world.record)
    assert not result["books"]["breakout/day/fixed"]["entry_available"]
    assert not any("ELIGIBILITY RESTORED" in row["text"] for row in alerts())


def test_membership_adapter_discovers_committed_eighth_registration_after_startup(world, monkeypatch):
    scan(world)
    module = ModuleType("lab.paper_run.membership")
    module.__spec__ = ModuleSpec(module.__name__, loader=None)

    def lanes(record, amendment=None, *, db=None):
        result = deepcopy(record["body"]["lanes"])
        for raw, in db.execute("SELECT body FROM event WHERE kind='PAPER_BOOK_REGISTERED_V1' ORDER BY rowid"):
            body = json.loads(raw)
            result[body["slot"]] = body["lane"]
        return result

    module.lanes = lanes
    monkeypatch.setitem(sys.modules, module.__name__, module)
    import lab.paper_run as paper_package
    monkeypatch.setattr(paper_package, "membership", module, raising=False)
    book = add_book(world, "mean_reversion/eighth", blocked="SOURCE_RESTRICTED")
    lane = world.record["body"]["lanes"].pop("mean_reversion/eighth")
    world.source.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)", (
        "registration", "REGISTRY", "PAPER_BOOK_REGISTERED_V1", "", NOW.isoformat(), NOW.isoformat(),
        json.dumps({"slot": "mean_reversion/eighth", "lane": lane})))
    world.source.commit()
    with state.connect() as db, db:
        result = monitor.collect(db, NOW, view=world.view, paper_db=world.source, registration=world.record,
                                 current_gates={"clock": True, "entries_enabled": True, "books": {book["book_id"]: True}})
    assert "mean_reversion/eighth" in result["books"]
    assert any(book["book_id"] in row["text"] for row in alerts())


def source_event(world, scope, availability, *, key):
    world.source.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)", (
        key, "SOURCE:" + scope, "SOURCE_OPERATIONAL_TRANSITION", "", NOW.isoformat(), NOW.isoformat(),
        json.dumps({"state": availability, "source_scope_id": scope, "reason_code": "SOURCE_FEED_UNAVAILABLE",
                    "correlation_id": scope + "-episode"})))
    world.source.commit()


def test_shared_source_restriction_matches_consumers_and_new_book_inherits_current_outage(world):
    first = world.view["books"]["ema/day"]
    first.update(state="ACTIVE", problem=None, historical_problem=None, reason=None, prospective_entry_permission=True,
                 source_scope_id="shared")
    save_book(world, "ema/day")
    second = add_book(world, "other/shared")
    second["source_scope_id"] = "shared"
    save_book(world, "other/shared")
    independent = add_book(world, "unrelated/source")
    independent["source_scope_id"] = "independent"
    save_book(world, "unrelated/source")
    scan(world)
    source_event(world, "shared", "UNAVAILABLE", key="source-down")
    result = scan(world)
    assert not result["books"]["ema/day"]["entry_available"]
    assert not result["books"]["other/shared"]["entry_available"]
    assert result["books"]["unrelated/source"]["entry_available"]
    assert not any("PROTECTION INCIDENT" in row["text"] for row in alerts())
    assert not any(independent["book_id"] in row["text"] for row in alerts())
    new = add_book(world, "new/during_outage")
    new["source_scope_id"] = "shared"
    save_book(world, "new/during_outage")
    result = scan(world)
    assert not result["books"]["new/during_outage"]["entry_available"]
    assert any(new["book_id"] in row["text"] and "Exposure FLAT" in row["text"] for row in alerts())
    source_event(world, "shared", "AVAILABLE", key="source-up")
    scan(world)
    assert any("ELIGIBILITY RESTORED" in row["text"] for row in alerts())


def test_ordinary_operational_clear_does_not_require_historical_resume(world):
    scan(world)
    book = world.view["books"]["ema/day"]
    book.update(state="ACTIVE", problem=None, reason=None, historical_problem=None, prospective_entry_permission=True)
    save_book(world, "ema/day")
    event(world, "OPERATIONAL_BLOCK_OPENED", episode_id="clock-episode", reason_code="CLOCK_UNVERIFIED",
          requires_incident_review=False)
    assert not scan(world)["books"]["ema/day"]["entry_available"]
    event(world, "OPERATIONAL_BLOCK_CLEARED", episode_id="clock-episode", reason_code="CLOCK_UNVERIFIED")
    assert scan(world)["books"]["ema/day"]["entry_available"]
    assert any("ELIGIBILITY RESTORED" in row["text"] for row in alerts())


def test_ephemeral_technical_entry_failure_uses_authoritative_operational_events(world):
    book = world.view["books"]["ema/day"]
    book.update(state="ACTIVE", problem=None, reason=None, historical_problem=None, prospective_entry_permission=True)
    save_book(world, "ema/day")
    scan(world)
    assert not alerts()
    event(world, "OPERATIONAL_BLOCK_OPENED", episode_id="technical-entry", reason_code="PRICE_SOURCE_UNAVAILABLE",
          source_scope_id="one-source", correlation_id="one-correlation", requires_incident_review=False,
          historical_coverage="NOT_APPLICABLE", restriction_started_at=NOW.isoformat())
    event(world, "ENTRY_EVIDENCE_REJECTED", reason="MISSING_REFERENCE")
    event(world, "NO_FILL", reason="PRICE_SOURCE_UNAVAILABLE")
    event(world, "OPERATIONAL_BLOCK_CLEARED", episode_id="technical-entry", reason_code="PRICE_SOURCE_UNAVAILABLE",
          source_scope_id="one-source", correlation_id="one-correlation", requires_incident_review=False,
          historical_coverage="NOT_APPLICABLE", cleared_at=NOW.isoformat())
    result = scan(world)
    assert result["books"]["ema/day"]["entry_available"]
    rows = alerts()
    assert len(rows) == 2
    assert "PAPER BOOK BLOCKED" in rows[0]["text"] and "ELIGIBILITY RESTORED" in rows[1]["text"]
    cause = json.loads(rows[0]["event_json"])["operational_event"]
    assert cause["episode_id"] == "technical-entry" and cause["source_scope_id"] == "one-source"
    assert cause["correlation_id"] == "one-correlation" and cause["requires_incident_review"] is False
    assert not any("PROTECTION INCIDENT" in row["text"] for row in rows)


def test_normal_entry_recipe_rejection_and_no_trade_are_not_operational_incidents(world):
    book = world.view["books"]["ema/day"]
    book.update(state="ACTIVE", problem=None, reason=None, historical_problem=None, prospective_entry_permission=True)
    save_book(world, "ema/day")
    scan(world)
    event(world, "ENTRY_EVIDENCE_REJECTED", reason="REFERENCE_HAS_NOT_REACHED_TRIGGER")
    event(world, "NO_FILL", reason="REFERENCE_OUTSIDE_FROZEN_RANGE")
    event(world, "EXPIRED", reason="ENTRY_WINDOW_ELAPSED")
    book["last_evaluation"] = {"state": "EVALUATED", "signal": {"decision": "NO_TRADE"}}
    result = scan(world)
    assert result["books"]["ema/day"]["entry_available"] and not alerts()


def test_historical_operational_clear_cannot_reuse_an_old_resume(world):
    scan(world)
    resume(world)
    event(world, "OPERATIONAL_BLOCK_OPENED", episode_id="history", reason_code="TRAILING_EXCEPTION",
          requires_incident_review=True)
    event(world, "OPERATIONAL_BLOCK_CLEARED", episode_id="history", reason_code="TRAILING_EXCEPTION")
    assert not scan(world)["books"]["ema/day"]["entry_available"]
    resume(world)
    assert scan(world)["books"]["ema/day"]["entry_available"]


def test_alerts_redact_credential_shaped_failure_details(world):
    world.view["books"]["ema/day"]["reason"] = "Authorization: Bearer fixture_secret token=fixture_token password=fixture_password"
    save_book(world, "ema/day")
    scan(world)
    text = alerts()[0]["text"]
    assert "fixture_secret" not in text and "fixture_token" not in text and "fixture_password" not in text


def test_scanner_integration_failure_queues_visible_failure(world, monkeypatch):
    from lab.health import __main__ as operator

    monkeypatch.setattr(monitor, "collect", lambda *_: (_ for _ in ()).throw(ValueError("fixture")))
    with state.connect() as db, db:
        result = operator._scan_fresh_operations(db, NOW)
    assert result["state"] == "CHECK_FAILED"
    assert len(alerts()) == 1 and "MONITOR CHECK FAILED" in alerts()[0]["text"]


def test_operator_commission_does_not_invoke_financial_supervision(world, monkeypatch):
    from lab.health import __main__ as operator
    from lab.paper_run import runtime

    monkeypatch.setattr(operator, "ROOT", state.ROOT)
    monkeypatch.setattr(runtime, "ensure_runner", lambda: pytest.fail("Operator check supervised a financial runner"))
    monkeypatch.setattr(operator, "_dispatch_fresh", lambda: pytest.fail("Operator check dispatched financial work"))
    monkeypatch.setattr(operator, "_check_health", lambda evidence, baseline, **kw: {"mode": kw["mode"], "evidence": evidence})
    result = operator.check(deliver=False, mode="operator")
    assert result == {"mode": "operator", "evidence": {}}


def test_operational_test_cli_only_queues_under_existing_check_lock(world, monkeypatch, capsys):
    from lab.health import __main__ as operator
    from lab.storage import writer_lock

    monkeypatch.setattr(operator, "ROOT", state.ROOT)
    monkeypatch.setattr(sys, "argv", ["aseke-health", "test-operational-alert", "--request-id", "approved-test"])
    with writer_lock(state.ROOT / ".check.lock"), pytest.raises(RuntimeError):
        operator.main()
    operator.main()
    output = json.loads(capsys.readouterr().out)
    assert output["state"] == "QUEUED" and len(alerts()) == 1
    assert alerts()[0]["status"] == "PENDING" and alerts()[0]["attempts"] == 0


def test_status_exposes_uncommissioned_and_stale_scanner(world):
    with state.connect() as db:
        assert monitor.status(db, NOW)["state"] == "NOT_COMMISSIONED"
    scan(world)
    with state.connect() as db:
        assert monitor.status(db, NOW)["state"] == "CHECKED"
        assert monitor.status(db, NOW + pd.Timedelta(seconds=421))["state"] == "STALE_MONITOR"
    with state.connect() as db, db:
        state.set_meta(db, monitor.META + "last", "not json")
        assert monitor.status(db, NOW)["state"] == "EVIDENCE_UNAVAILABLE"
