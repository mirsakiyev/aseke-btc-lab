from types import SimpleNamespace
from contextlib import closing

import pandas as pd
import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from lab.paper_run import kraken_live, protection_incidents, runtime, source_amendment
from lab.phase15 import paper
from test_paper_run_kraken_live import acknowledge, clock_at, trade, update
from test_paper_run_live_amendment import (
    APPROVED, DAY, QUALIFICATION, TICK, generation, intent, live_point, saved_book,
)


def activate_test_policy(record, lane):
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, lane)
        protection_incidents.append(db, book, "PROTECTION_POLICY_ACTIVATED", "POLICY", TICK,
                                    {"generation": record["id"]}, identity="fixture-activation")


def test_disconnect_witness_survives_failed_transaction_and_sanitizes_exception():
    feed = kraken_live.KrakenTradeFeed(read_clock=lambda: clock_at(100))
    session = acknowledge(feed)
    feed._message(update([trade(1, 35)]), clock_at(50), session, 1)
    error = RuntimeError("failed wss://user:password@ws.kraken.com/v2?token=secret authorization: Bearer abc")
    error.rcvd = SimpleNamespace(code=1011, reason="ping timeout token=server-secret")
    error.sent = SimpleNamespace(code=1001, reason="going away")
    error.__cause__ = OSError("reset password=private")
    feed._disconnected("ConnectionClosedError", error=error)
    first = feed.pending_witnesses()
    assert first == feed.pending_witnesses()
    disconnect = first[-1]
    assert disconnect["kind"] == "DISCONNECTED"
    assert disconnect["last_accepted_reference"]["event_id"] == "1"
    assert disconnect["discarded_count"] == 1
    assert disconnect["monotonic_ns"] == 100_000_000
    assert disconnect["exception"]["rcvd"]["code"] == 1011
    details = str(disconnect["exception"])
    assert "server-secret" not in details and "private" not in details and "abc" not in details
    assert "user:" not in details and "?token=" not in details
    first[-1]["reason"] = "changed copy"
    assert feed.pending_witnesses()[-1]["reason"] == "ConnectionClosedError"
    feed.acknowledge_witnesses(item["id"] for item in first)
    assert not feed.pending_witnesses()


def test_acknowledgement_and_replayed_rows_do_not_claim_recovery():
    feed = kraken_live.KrakenTradeFeed(read_clock=lambda: clock_at(100))
    first = acknowledge(feed)
    feed._message(update([trade(1, 35)]), clock_at(50), first, 1)
    feed._disconnected("ConnectionClosedError")
    feed.acknowledge_witnesses(item["id"] for item in feed.pending_witnesses())
    second = acknowledge(feed, ms=100, epoch=2)
    feed._message(update([trade(1, 140), trade(2, 160)]), clock_at(150), second, 2)
    assert [item["kind"] for item in feed.pending_witnesses()] == ["SUBSCRIPTION_ACKNOWLEDGED"]
    feed._message(update([trade(3, 155), trade(4, 156)]), clock_at(170), second, 2)
    recovered = [item for item in feed.pending_witnesses() if item["kind"] == "RECOVERY_REFERENCE"]
    assert len(recovered) == 1
    assert recovered[0]["observation"]["event_id"] == "3"
    assert recovered[0]["observation"]["subscription_epoch"] == 2
    assert recovered[0]["historical_coverage"] == "PENDING_REVIEW"
    assert recovered[0]["reference_quality"] == "ACCEPTED_AT_RECEIPT_REQUIRES_CONSUMER_VALIDATION"


def test_queue_loss_records_the_exact_dropped_reference():
    feed = kraken_live.KrakenTradeFeed(queue_size=1)
    session = acknowledge(feed)
    feed._message(update([trade(1, 35), trade(2, 40)]), clock_at(50), session, 1)
    dropped, = [item for item in feed.pending_witnesses() if item["kind"] == "QUEUE_DROPPED"]
    assert dropped["dropped_reference"]["event_id"] == "1"
    assert dropped["dropped_count"] == 1
    assert dropped["counts"]["queue_dropped"] == 1
    assert [item["event_id"] for item in feed.drain()] == ["2"]


def test_witness_overflow_is_bounded_and_old_ack_cannot_erase_new_overflow(monkeypatch):
    monkeypatch.setattr(kraken_live, "WITNESS_LIMIT", 2)
    feed = kraken_live.KrakenTradeFeed(read_clock=lambda: clock_at(100))
    acknowledge(feed)
    feed._disconnected("FIRST")
    feed._disconnected("SECOND")
    pending = feed.pending_witnesses()
    assert len(pending) == 3
    overflow = pending[0]
    assert overflow["kind"] == "WITNESS_OVERFLOW" and overflow["dropped_count"] == 1
    feed._disconnected("THIRD")
    feed.acknowledge_witnesses([overflow["id"]])
    updated = feed.pending_witnesses()[0]
    assert updated["id"] != overflow["id"]
    assert updated["dropped_count"] == 2
    assert updated["historical_coverage"] == "UNRESOLVED"
    assert feed.status()["counts"]["witness_overflow"] == 2


def test_owner_captures_websocket_close_frames_before_reconnecting(monkeypatch):
    error = ConnectionClosedError(Close(1011, "upstream error"), Close(1011, "ping failed"), True)
    error.__cause__ = TimeoutError("timed out waiting for pong")
    feed = kraken_live.KrakenTradeFeed(read_clock=lambda: clock_at(100), authorize=lambda _: None)

    def broken_session(_):
        feed._state_changed("STREAMING")
        raise error

    monkeypatch.setattr(feed, "_session", broken_session)
    monkeypatch.setattr(feed._stop, "wait", lambda _: True)
    feed._run()
    disconnected, = [item for item in feed.pending_witnesses() if item["kind"] == "DISCONNECTED"]
    assert disconnected["reason"] == "ConnectionClosedError"
    assert disconnected["exception"]["rcvd"] == {"code": 1011, "reason": "upstream error"}
    assert disconnected["exception"]["sent"] == {"code": 1011, "reason": "ping failed"}
    assert disconnected["exception"]["cause"]["type"] == "TimeoutError"


def test_committed_witness_survives_consumer_failure_and_retry_is_idempotent(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    activate_test_policy(record, lane)
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    feed = kraken_live.KrakenTradeFeed(read_clock=lambda: (now[0], 125_000_000))
    feed._disconnected("ConnectionClosedError")
    witnesses = feed.pending_witnesses()
    apply = runtime._apply_live_point
    monkeypatch.setattr(runtime, "_apply_live_point", lambda *_: (_ for _ in ()).throw(RuntimeError("fixture")))
    # The source cause commits independently of per-book processing. A failed
    # consumer cannot roll that evidence back or stop another book's protection.
    accepted = runtime.live_prices(record, amendment, [], {}, witnesses=witnesses)
    assert accepted == [item["id"] for item in witnesses]
    assert feed.pending_witnesses() == witnesses
    with closing(runtime.connect(readonly=True)) as db:
        assert len(protection_incidents.project(db, lane["book_id"])) == 1
        assert db.execute("SELECT 1 FROM event WHERE kind='OPERATIONAL_BLOCK_OPENED'").fetchone()
    monkeypatch.setattr(runtime, "_apply_live_point", apply)
    accepted = runtime.live_prices(record, amendment, [], {}, witnesses=feed.pending_witnesses())
    feed.acknowledge_witnesses(accepted)
    assert not feed.pending_witnesses()
    with closing(runtime.connect(readonly=True)) as db:
        count, = db.execute("SELECT count(*) FROM event WHERE kind='PROTECTION_TRANSPORT_WITNESS'").fetchone()
        records = protection_incidents.project(db, lane["book_id"])
    assert count == 1 and len(records) == 1


def test_a_pre_disconnect_point_cannot_claim_the_new_session_recovered(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    activate_test_policy(record, lane)
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    # The consumer drained this point before the socket owner noticed a close.
    buffered = live_point(92, at=TICK + pd.Timedelta(milliseconds=150))
    disconnect_at = TICK + pd.Timedelta(milliseconds=300)
    now[0] = disconnect_at + pd.Timedelta(milliseconds=50)
    witness = {"id": "fixture-old-epoch-close", "kind": "DISCONNECTED", "at": disconnect_at.isoformat(),
               "reason": "ConnectionClosedError", "subscription_epoch": 1,
               "last_accepted_reference": buffered}
    runtime.live_prices(record, amendment, [buffered], {}, witnesses=[witness])
    with closing(runtime.connect(readonly=True)) as db:
        incident, = protection_incidents.project(db, lane["book_id"])
    assert incident["current_feed_health"] == "UNAVAILABLE"
    assert incident["first_accepted_reference"] is None
    assert saved_book(lane)["position"]["unresolved_protection"]


def test_inactive_policy_does_not_apply_transport_witnesses_to_the_open_position(generation):
    record, _ = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    witness = {"id": "fixture-inactive", "kind": "DISCONNECTED", "at": TICK.isoformat(),
               "reason": "ConnectionClosedError"}
    accepted = runtime.live_prices(record, amendment, [], {}, witnesses=[witness])
    assert accepted == []
    assert not saved_book(lane)["position"].get("unresolved_protection")
    with closing(runtime.connect(readonly=True)) as db:
        assert not db.execute("SELECT 1 FROM event WHERE kind='PROTECTION_TRANSPORT_WITNESS'").fetchone()
