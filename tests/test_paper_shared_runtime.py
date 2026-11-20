"""Exercise registered dispatch and financial checkpoint integration, not mocks of it."""
from contextlib import closing
from copy import deepcopy
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.paper_run import capabilities, delivery, execution, membership, operations, protection_incidents as incidents
from lab.paper_run import runtime, source_amendment
from lab.phase15 import paper
from test_paper_run_live_amendment import (
    APPROVED, DAY, QUALIFICATION, TICK, generation, history, intent, live_point, saved_book,
)
from test_paper_run_runtime import (MARK_BOOK, MARK_PENDING_2, NOW, _idea, _terms, initialize,
                                   observation, setup as runtime_setup)


def setup_eighth(record, amendment, at):
    lane = deepcopy(source_amendment.lanes(record, amendment)[DAY])
    slot = "orbital/reversion"
    lane.update(book_id=fingerprint([record["id"], slot]), family="ORBITAL_REVERSION", label="Orbital research",
                execution_mode="MANUAL_OUTSIDE_LAB", paper_only=True, approval="No validated model")
    lane["capabilities"] = capabilities.declare(lane, signal_adapter="ema_chart_v1", entry_adapter="ema_reference_v1",
                                               generation=record["id"], slot=slot)
    request = {"generation": record["id"], "slot": slot, "lane": lane, "request_id": "isolated-orbital",
               "reason": "Isolated shared runner test"}
    with closing(runtime.connect()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        membership.register(db, record, request, at=at, approved=True)
    return lane


def activate(record, lane):
    with closing(runtime.connect()) as db, db:
        incidents.append(db, {"book_id": lane["book_id"]}, "PROTECTION_POLICY_ACTIVATED", record["id"], APPROVED,
                         {"generation": record["id"]}, identity="shared-isolated-policy")


def events(kind):
    with closing(runtime.connect(readonly=True)) as db:
        return [(book, json.loads(raw)) for book, raw in db.execute("SELECT book,body FROM event WHERE kind=? ORDER BY rowid", (kind,))]


def diag(now):
    return {"state": "STREAMING", "at": now.isoformat()}


def test_actual_registered_eighth_receives_source_incident_without_day_anchor(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = setup_eighth(record, amendment, now[0])
    activate(record, lane)
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(1001)], diag(now[0]))
    assert saved_book(lane)["position"]
    unrelated = record["body"]["lanes"]["breakout/swing/fixed"]
    detected = TICK + pd.Timedelta(milliseconds=300)
    now[0] = detected
    witness = {"id": "actual-source-disconnect", "kind": "DISCONNECTED", "at": detected.isoformat(),
               "reason": "ConnectionClosedError", "subscription_epoch": 1}
    runtime.live_prices(record, amendment, [], {"state": "RECONNECTING", "at": detected.isoformat()}, witnesses=[witness])
    scope = capabilities.source_scope_id(lane)
    book_id, receipt = events("PROTECTION_TRANSPORT_WITNESS")[0]
    assert book_id == "SOURCE:" + scope and receipt["source_scope_id"] == scope
    with closing(runtime.connect(readonly=True)) as db:
        incident, = incidents.project(db, lane["book_id"])
        assert incident["source_scope_id"] == scope
        assert incident["source_correlation_id"] == witness["id"]
        assert not incidents.project(db, unrelated["book_id"])
        assert not incidents.project(db, source_amendment.lanes(record, amendment)[DAY]["book_id"])
    assert not saved_book(unrelated).get("problem")
    assert len(events("PROTECTION_TRANSPORT_WITNESS")) == 1
    # Exact duplicate witness after a restart is idempotent.
    runtime.live_prices(record, amendment, [], {"state": "RECONNECTING"}, witnesses=[witness])
    assert len(events("PROTECTION_TRANSPORT_WITNESS")) == 1


def test_failed_book_transaction_retains_work_other_book_closes_and_retry_exactly_once(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    other = setup_eighth(record, amendment, now[0])
    activate(record, lane)
    for consumer in (lane, other):
        intent(consumer, amendment)
    runtime.live_prices(record, amendment, [live_point(1101)], diag(now[0]))
    actual_protect = execution.protect
    failed_calls = []

    def fail_one(db, book, *args, **kwargs):
        result = actual_protect(db, book, *args, **kwargs)
        if book["book_id"] == lane["book_id"]:
            failed_calls.append(book["book_id"])
            raise OSError("isolated financial transaction failure")
        return result

    monkeypatch.setattr(execution, "protect", fail_one)
    tick = TICK + pd.Timedelta(seconds=1)
    now[0] = tick + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(1102, price="94.0", at=tick)], diag(now[0]))
    scope = capabilities.source_scope_id(lane)
    assert failed_calls == [lane["book_id"]]
    assert saved_book(lane)["position"] and not saved_book(other)["position"]
    assert len(history(other, "CLOSED")) == 1
    with closing(runtime.connect(readonly=True)) as db:
        assert delivery.pending(db, lane["book_id"], scope)[0]["event_id"] == "1102"
        assert not delivery.pending(db, other["book_id"], scope)
        assert runtime._lane_source_state(db, lane)["last_observation"]["event_id"] == "1102"
    monkeypatch.setattr(execution, "protect", actual_protect)
    now[0] += pd.Timedelta(milliseconds=100)
    runtime.live_prices(record, amendment, [], diag(now[0]))
    assert not saved_book(lane)["position"] and len(history(lane, "CLOSED")) == 1
    with closing(runtime.connect(readonly=True)) as db:
        assert not delivery.pending(db, lane["book_id"], scope)
        assert delivery.state(db, lane["book_id"], scope)["checkpoint"]["event_id"] == "1102"
    runtime.live_prices(record, amendment, [live_point(1102, price="94.0", at=tick)], diag(now[0]))
    assert len(history(lane, "CLOSED")) == 1 and len(history(other, "CLOSED")) == 1


def test_actual_late_durable_pending_work_never_fills_or_creates_exposure_incident(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = setup_eighth(record, amendment, now[0])
    activate(record, lane)
    intent(lane, amendment)
    with closing(runtime.connect()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        delivery.reserve(db, lane, paper.load(db, lane), live_point(1201), at=now[0])
    now[0] += pd.Timedelta(minutes=15)
    runtime.live_prices(record, amendment, [], diag(now[0]))
    value = saved_book(lane)
    assert value["position"] is None and value["pending"] is None
    assert not history(lane, "HYPOTHETICAL_FILL") and history(lane, "EXPIRED")
    with closing(runtime.connect(readonly=True)) as db:
        assert not delivery.pending(db, lane["book_id"])
        assert not delivery.losses(db, lane["book_id"], capabilities.source_scope_id(lane))
        assert not incidents.project(db, lane["book_id"])


def test_runtime_technical_entry_rejection_has_durable_short_block_and_no_protection_incident(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment)
    now[0] += pd.Timedelta(seconds=3)
    runtime.live_prices(record, amendment, [live_point(1301)], diag(now[0]))
    opened = [raw for book, raw in events(operations.OPENED) if book == lane["book_id"]]
    assert any(row["reason_code"] == "ENTRY_GATE_STALE_REFERENCE" for row in opened)
    assert not history(lane, "HYPOTHETICAL_FILL") and not events("PROTECTION_INCIDENT_OPENED")
    tick = now[0] + pd.Timedelta(seconds=1)
    now[0] = tick + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(1302, at=tick)], diag(now[0]))
    cleared = [raw for book, raw in events(operations.CLEARED) if book == lane["book_id"]]
    assert any(row["reason_code"] == "ENTRY_GATE_STALE_REFERENCE" for row in cleared)
    assert len(history(lane, "HYPOTHETICAL_FILL")) == 1


def test_actual_trail_calculation_failure_has_incident_and_other_books_keep_protection(runtime_setup, monkeypatch):
    _, _, body = runtime_setup
    body["lanes"][MARK_PENDING_2]["exit"] = "atr_trail_v1"
    record, lanes = initialize(runtime_setup)
    trail_lane = lanes[MARK_PENDING_2]
    activate(record, trail_lane)
    monkeypatch.setattr(runtime, "_source_permission", lambda *_a, **_kw: None)
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(runtime.protocol, "current_metadata", lambda: {})
    monkeypatch.setattr(runtime, "_settle", lambda *_a, **_kw: False)
    monkeypatch.setattr(runtime, "evaluate", lambda *_a: None)
    monkeypatch.setattr(execution, "utc_now", lambda: NOW)
    monkeypatch.setattr(execution, "time", SimpleNamespace(monotonic_ns=lambda: 1_300_000_000))
    monkeypatch.setattr(execution, "clock", lambda: (NOW, 1_300_000_000))
    previous = NOW-pd.Timedelta(seconds=30)
    with closing(runtime.connect()) as db, db:
        for slot in (MARK_BOOK, MARK_PENDING_2):
            lane = lanes[slot]
            value = paper.load(db, lane)
            value["pending"] = _idea(value, 63000.)
            execution.entry(db, value, lane, observation(previous, event="entry-"+slot, price=64800.), _terms,
                            read_clock=lambda:(previous, 1_300_000_000))
            paper.save(db, value)
    def broken_trail(*_a):
        raise ArithmeticError("isolated ATR computation failure")
    monkeypatch.setattr(runtime.breakout, "trail", broken_trail)
    monkeypatch.setattr(runtime, "_observe", lambda *_a: observation(NOW, event="next-real-reference"))
    runtime.cycle()
    with closing(runtime.connect(readonly=True)) as db:
        affected = paper.load(db, trail_lane)
        assert affected["position"]["unresolved_protection"]
        incident, = incidents.project(db, trail_lane["book_id"])
        assert incident["reason"] == "TRAIL_CALCULATION_FAILED" and incident["source_scope_id"]
        assert affected["position"]["mark_event_id"] == "next-real-reference"
        healthy = paper.load(db, lanes[MARK_BOOK])
        assert healthy["position"]["mark_event_id"] == "next-real-reference"
        assert not incidents.project(db, healthy["book_id"])
        fault, = [json.loads(raw) for raw, in db.execute("SELECT body FROM event WHERE book=? AND kind=?",
                                                        (trail_lane["book_id"], operations.OPENED))
                   if json.loads(raw)["reason_code"] == "TRAIL_CALCULATION_FAILED"]
        assert fault["next_action"] and fault["evidence"]["exception_type"] == "ArithmeticError"


def test_flat_signal_exception_is_durable_recoverable_and_does_not_suppress_eighth(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    other = setup_eighth(record, amendment, now[0])
    now[0] = TICK.ceil("15min")+pd.Timedelta(seconds=1)
    selected = {DAY: lane, "orbital/reversion": other}
    def confirmation(item, *_a, **_kw):
        if item["book_id"] == lane["book_id"]:
            raise OSError("isolated chart evidence unavailable")
        return {"state": "EVALUATED", "signal": {"decision": "NO_TRADE"}}
    monkeypatch.setattr(runtime.ema, "confirmation", confirmation)
    with closing(runtime.connect()) as db, db:
        books = {slot: runtime._load_book(db, item) for slot, item in selected.items()}
        runtime.evaluate(db, record, books, selected, now[0], {})
        runtime._capture_operations(db, record, selected, books, now[0])
        assert books[DAY]["operational_faults"]["SIGNAL_EVALUATION_FAILED"]
        assert books["orbital/reversion"]["last_evaluation"]["signal"]["decision"] == "NO_TRADE"
        assert not incidents.project(db, lane["book_id"])
    now[0] += pd.Timedelta(minutes=15)
    monkeypatch.setattr(runtime.ema, "confirmation", lambda *_a, **_kw: {"state": "EVALUATED", "signal": {"decision": "NO_TRADE"}})
    with closing(runtime.connect()) as db, db:
        books = {slot: runtime._load_book(db, item) for slot, item in selected.items()}
        runtime.evaluate(db, record, books, selected, now[0], {})
        runtime._capture_operations(db, record, selected, books, now[0])
        assert not books[DAY].get("operational_faults")
    assert any(book==lane["book_id"] and body["reason_code"]=="SIGNAL_EVALUATION_FAILED"
               for book,body in events(operations.OPENED))
    assert any(book==lane["book_id"] and body["reason_code"]=="SIGNAL_EVALUATION_FAILED"
               for book,body in events(operations.CLEARED))


def test_healthy_raw_trade_burst_does_not_create_artificial_consumer_loss(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = setup_eighth(record, amendment, now[0])
    activate(record, lane)
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(1401)], diag(now[0]))
    points = [live_point(1402+i, at=TICK+pd.Timedelta(milliseconds=200+i)) for i in range(delivery.MAX_PENDING*2)]
    now[0] = TICK+pd.Timedelta(seconds=1)
    runtime.live_prices(record, amendment, points, diag(now[0]))
    with closing(runtime.connect(readonly=True)) as db:
        assert not delivery.pending(db, lane["book_id"])
        assert not delivery.losses(db, lane["book_id"], capabilities.source_scope_id(lane))
        assert not incidents.project(db, lane["book_id"])
        assert runtime._load_book(db, lane)["position"]["mark_event_id"] == points[-1]["event_id"]


def test_fresh_retry_queue_is_drained_before_new_trade_can_displace_its_head(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    activate(record, lane)
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(1501)], diag(now[0]))
    points = [live_point(1502+i, at=TICK+pd.Timedelta(milliseconds=200+i)) for i in range(delivery.MAX_PENDING+1)]
    now[0] = TICK + pd.Timedelta(seconds=1)
    with closing(runtime.connect()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        book = runtime._load_book(db, lane)
        for point in points[:-1]:
            delivery.reserve(db, lane, book, point, at=now[0])

    runtime.live_prices(record, amendment, [points[-1]], diag(now[0]))

    with closing(runtime.connect(readonly=True)) as db:
        assert not delivery.pending(db, lane["book_id"])
        assert not incidents.project(db, lane["book_id"])
        assert runtime._load_book(db, lane)["position"]["mark_event_id"] == points[-1]["event_id"]


def test_late_trade_from_the_entry_batch_does_not_create_an_exposure_incident(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    activate(record, lane)
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(1701)], diag(now[0]))
    original = saved_book(lane)["position"]

    stale = live_point(1702, at=TICK)
    assert stale["source_at"] < original["entry_at"]
    now[0] = TICK + pd.Timedelta(milliseconds=2050)
    runtime.live_prices(record, amendment, [stale], diag(now[0]))

    position = saved_book(lane)["position"]
    assert position["mark_event_id"] == original["mark_event_id"]
    assert not position.get("unresolved_protection")
    with closing(runtime.connect(readonly=True)) as db:
        assert not incidents.project(db, lane["book_id"])


def test_earlier_stale_verdict_survives_a_small_wall_clock_correction(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    activate(record, lane)
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(1801)], diag(now[0]))
    position = saved_book(lane)["position"]
    assert position

    stop = live_point(1802, price=str(position["stop"] + 0.1), at=TICK + pd.Timedelta(milliseconds=500))
    receipt = execution.stamp(stop["received_at"])
    received_mono = stop["received_mono_ns"]
    initial = receipt + pd.Timedelta(milliseconds=1910), received_mono + 1_860_000_000
    corrected = receipt + pd.Timedelta(milliseconds=1890), received_mono + 1_870_000_000
    assert execution.valid_snapshot(stop, lane, *initial) == "STALE_REFERENCE"
    assert execution.valid_snapshot(stop, lane, *corrected) is None
    now[0] = initial[0]
    clocks = iter((initial, corrected))
    protect = execution.protect

    def read_clock():
        return next(clocks, corrected)

    def protect_with_corrected_clock(*args, **kwargs):
        return protect(*args, read_clock=read_clock, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(execution, "clock", read_clock)
        patch.setattr(execution, "protect", protect_with_corrected_clock)
        runtime.live_prices(record, amendment, [stop], diag(now[0]))

    assert not history(lane, "EXIT_OBSERVED")
    assert saved_book(lane)["position"]["mark_event_id"] == "1801"
    with closing(runtime.connect(readonly=True)) as db:
        incident, = incidents.project(db, lane["book_id"])
        assert incident["evidence"]["loss"]["first_event_id"] == "1802"
        assert incident["evidence"]["loss"]["reason"] == "DELIVERY_STALE"


def test_ema_protection_drains_each_fresh_reference_before_later_enqueues_age_it(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lanes = source_amendment.lanes(record, amendment)
    lane = lanes[DAY]
    activate(record, lane)
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(116976482)], diag(now[0]))
    original = saved_book(lane)["position"]
    assert original["position"]["direction"] == -1

    # The first live loss arrived 39.057 ms after the exchange timestamp. A
    # completed batch must not defer otherwise valid protection until every
    # later reference has been durably enqueued.
    exchange = TICK + pd.Timedelta(seconds=8, microseconds=948217)
    burst = []
    for index in range(5):
        source_at = exchange + pd.Timedelta(milliseconds=300 * index)
        burst.append(live_point(116976483 + index, at=source_at,
                                receipt=source_at + pd.Timedelta(microseconds=39057)))
    now[0] = pd.Timestamp(burst[-1]["received_at"]) + pd.Timedelta(milliseconds=5)
    reserve = runtime._reserve_reference
    protect = execution.protect
    applied = []

    def reserve_with_latency(*args, **kwargs):
        result = reserve(*args, **kwargs)
        now[0] += pd.Timedelta(milliseconds=375)
        return result

    def observe_applied(db, book, consumer, obs, **kwargs):
        result = protect(db, book, consumer, obs, **kwargs)
        position = book.get("position") or {}
        if obs.get("event_id") and position.get("mark_event_id") == obs["event_id"]:
            applied.append((obs["event_id"], now[0] - pd.Timestamp(obs["source_at"])))
        return result

    monkeypatch.setattr(runtime, "_reserve_reference", reserve_with_latency)
    monkeypatch.setattr(execution, "protect", observe_applied)
    runtime.live_prices(record, amendment, burst, diag(now[0]))

    assert [event_id for event_id, _ in applied] == [point["event_id"] for point in burst]
    assert all(pd.Timedelta(0) <= age < pd.Timedelta(seconds=2) for _, age in applied)
    position = saved_book(lane)["position"]
    assert position["id"] == original["id"] and position["last_protection_reference"]["event_id"] == burst[-1]["event_id"]
    assert not history(lane, "PROTECTION_SAMPLE_REJECTED")
    assert not history(lane, "PROTECTION_INCIDENT_OPENED")


def test_ema_late_delivery_retains_incident_through_fresh_recovery_and_genuine_exit(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lanes = source_amendment.lanes(record, amendment)
    lane = lanes[DAY]
    activate(record, lane)
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(116976480)], diag(now[0]))
    clock = now[0] + pd.Timedelta(seconds=1)
    now[0] = clock + pd.Timedelta(milliseconds=125)
    first = live_point(116976481, at=clock)
    runtime.live_prices(record, amendment, [first], diag(now[0]))
    assert saved_book(lane)["position"]["last_protection_reference"]["event_id"] == first["event_id"]

    exchange = now[0] + pd.Timedelta(seconds=1)
    received = exchange + pd.Timedelta(microseconds=39057)
    stale = live_point(116976483, price="94.0", at=exchange, receipt=received)
    with closing(runtime.connect()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        delivery.reserve(db, lane, paper.load(db, lane), stale, at=received)
    now[0] = exchange + pd.Timedelta(seconds=2, microseconds=45045)
    runtime.live_prices(record, amendment, [], diag(now[0]))
    day = saved_book(lane)
    assert day["position"]["unresolved_protection"] and day["position"]["last_protection_reference"]["event_id"] == first["event_id"]
    assert not history(lane, "EXIT_OBSERVED") and not history(lane, "CLOSED")
    assert any(row["reason"] == "DELIVERY_STALE_REFERENCE" for row in history(lane, "PROTECTION_SAMPLE_REJECTED"))
    original, = history(lane, "PROTECTION_INCIDENT_OPENED")
    assert original["reason"] == "UNCOMMITTED_PROTECTION_REFERENCE"

    exchange = now[0] + pd.Timedelta(milliseconds=500)
    recovery = live_point(116976496, at=exchange, receipt=exchange + pd.Timedelta(microseconds=39057))
    now[0] = pd.Timestamp(recovery["received_at"]) + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [recovery], diag(now[0]))
    day = saved_book(lane)
    assert day["position"]["last_protection_reference"]["event_id"] == recovery["event_id"]
    assert not day["position"].get("operational_faults")
    with closing(runtime.connect(readonly=True)) as db:
        incident, = incidents.project(db, lane["book_id"])
        assert incident["incident_id"] == original["incident_id"]
        assert incident["historical_coverage"] == "UNRESOLVED" and incident["current_feed_health"] == "RECOVERED"
        assert incident["interval"]["ends_at"] == recovery["received_at"]
        for slot in (slot for slot in source_amendment.SLOTS if slot != DAY):
            sibling = lanes[slot]
            book = paper.load(db, sibling)
            assert not book["position"] and not book["pending"] and not book.get("operational_faults")
            assert not incidents.project(db, sibling["book_id"])
            assert incidents.admission_allowed(book, now[0])

    exit_at = now[0] + pd.Timedelta(seconds=1)
    qualified_exit = live_point(116976497, price="94.0", at=exit_at)
    now[0] = exit_at + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [qualified_exit], diag(now[0]))
    runtime.live_prices(record, amendment, [qualified_exit], diag(now[0]))
    day = saved_book(lane)
    assert len(history(lane, "HYPOTHETICAL_FILL")) == len(history(lane, "EXIT_OBSERVED")) == len(history(lane, "CLOSED")) == 1
    assert not day["position"] and day["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    assert not incidents.admission_allowed(day, now[0])
    assert history(lane, "PROTECTION_INCIDENT_OPENED") == [original]
