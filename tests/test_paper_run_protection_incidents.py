"""Isolated reproductions of current incident and accounting behavior."""

from contextlib import closing

import pandas as pd
import pytest

from lab.paper_run import ema, execution, protection_incidents as incidents, runtime, source_amendment
from lab.phase15 import paper
from test_paper_run_live_amendment import (
    APPROVED, DAY, QUALIFICATION, TICK, generation, history, intent, live_point, saved_book,
)


def test_brief_disconnect_latches_uncertainty_without_a_structured_interval(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    before = saved_book(lane)

    now[0] = TICK + pd.Timedelta(milliseconds=250)
    runtime.live_prices(record, amendment, [], {"reason": "ConnectionClosedError"}, detect_gap=True)
    interrupted = saved_book(lane)
    assert interrupted["position"]["unresolved_protection"]
    assert interrupted["position"].get("protection_gap") is None
    assert interrupted["cash"] == before["cash"]
    assert interrupted["fees"] == before["fees"]
    gap, = history(lane, "PROTECTION_GAP")
    assert gap["reason"] == "KRAKEN_STREAM_DISCONTINUITY"
    assert gap["last_reference"] == before["position"]["mark_received_at"]
    assert now[0] - execution.stamp(gap["last_reference"]) < execution.PROTECTION_GAP

    resumed_at = TICK + pd.Timedelta(seconds=2)
    now[0] = resumed_at + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(92, at=resumed_at, epoch=2)], {})
    recovered = saved_book(lane)
    assert recovered["position"]["mark_event_id"] == "92"
    assert recovered["position"]["last_protection_rejection"] is None
    assert recovered["position"]["unresolved_protection"]
    assert recovered["position"].get("protection_gap") is None
    assert not history(lane, "PROTECTION_RESUMED")
    assert not history(lane, "EXIT_OBSERVED")


def test_empty_consumer_read_alone_is_not_a_45_second_coverage_gap(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    now[0] = TICK + pd.Timedelta(seconds=15)
    runtime.live_prices(record, amendment, [], {})
    book = saved_book(lane)
    assert book["position"]["last_protection_rejection"] == "MISSING_REFERENCE"
    assert not book["position"].get("unresolved_protection")
    assert not history(lane, "PROTECTION_GAP")


def test_silent_coverage_gap_has_an_interval_and_preserves_history_on_resume(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    now[0] = TICK + pd.Timedelta(seconds=46)
    runtime.live_prices(record, amendment, [], {})
    incident = saved_book(lane)["position"]["protection_gap"]
    assert incident["started_after"] == live_point(91)["received_at"]
    assert incident["reason"] == "MISSING_REFERENCE" and not incident.get("ended_at")

    resumed_at = TICK + pd.Timedelta(seconds=47)
    now[0] = resumed_at + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(92, at=resumed_at)], {})
    book = saved_book(lane)
    resumed, = history(lane, "PROTECTION_RESUMED")
    assert resumed["started_after"] == incident["started_after"]
    assert resumed["ended_at"] == now[0].isoformat()
    assert book["position"]["unresolved_protection"]


def test_active_feed_recovery_preserves_the_transport_fence_without_replaying_history(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    book = saved_book(lane)
    position = book["position"]
    fence = TICK + pd.Timedelta(seconds=1)
    with closing(runtime.connect()) as db, db:
        incidents.append(db, book, "PROTECTION_POLICY_ACTIVATED", record["id"], APPROVED,
                         {"generation": record["id"]}, identity="test-hot-path-policy")
        legacy_at = TICK - pd.Timedelta(minutes=2)
        paper.event(db, book, "PROTECTION_GAP", "legacy-position", legacy_at, legacy_at,
                    {"reason": "ARCHIVED_GAP", "started_after": legacy_at.isoformat()})
        with monkeypatch.context() as patch:
            patch.setattr(incidents, "project", lambda *_args: pytest.fail("active price replayed full incident history"))
            identifier = incidents.open_incident(db, book, fence, "TRANSPORT_LOST", witness_id="drop-1", lane=lane,
                                                diagnostics={"kind": "DISCONNECTED", "at": fence.isoformat()})
            evidence_at = fence + pd.Timedelta(milliseconds=50)
            assert incidents.open_incident(db, book, evidence_at, "UNCOMMITTED_PROTECTION_REFERENCE",
                                           diagnostics={"loss": "late in the consumer"}, witness_id="delivery-2",
                                           lane=lane) == identifier
            before_fence = live_point(92, at=fence - pd.Timedelta(milliseconds=200))
            incidents.recovered(db, book, before_fence, evidence_at, 0)
            assert not position["protection_gap"].get("ended_at")
            after_fence = live_point(93, at=evidence_at, epoch=2)
            recovered_at = evidence_at + pd.Timedelta(milliseconds=150)
            incidents.recovered(db, book, after_fence, recovered_at, 0)
            incidents.recovered(db, book, live_point(94, at=recovered_at, epoch=2),
                                recovered_at + pd.Timedelta(milliseconds=150), 0)
            assert incidents.open_incident(db, book, recovered_at, "TRANSPORT_LOST", witness_id="drop-1",
                                           lane=lane) == identifier
            assert position["protection_gap"]["ended_at"] == recovered_at.isoformat()
            assert position["unresolved_protection"]
        incident, = incidents.project_rows(incidents.event_rows(db, book["book_id"]))
        assert incident["incident_id"] == identifier
        assert [item["kind"] for item in incident["history"]] == [
            "PROTECTION_INCIDENT_OPENED", "PROTECTION_INCIDENT_EVIDENCE_ADDED", "PROTECTION_INCIDENT_RECOVERED"]
        assert incident["interval"]["ends_at"] == after_fence["received_at"]
        assert incident["first_accepted_reference"]["event_id"] == "93"
        assert incident["historical_coverage"] == "UNRESOLVED"
        assert incident["transport_fence_at"] == fence.isoformat()
        assert position["last_protection_reference"]["event_id"] == "94"
        active, legacy = incidents.summaries(db, book, record["id"])
        assert active == {key: incident[key] for key in (
            "incident_id", "position_id", "current_feed_health", "historical_coverage")}
        assert legacy["position_id"] == "legacy-position" and legacy["historical_coverage"] == "UNRESOLVED"


@pytest.mark.parametrize("action,price", [("stop", "105.1"), ("target", "94.9"), ("time_exit", "100.")])
def test_uncertain_short_keeps_protecting_then_closes_provisionally_and_blocks_new_ideas(
        generation, monkeypatch, action, price):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    now[0] = TICK + pd.Timedelta(milliseconds=250)
    runtime.live_prices(record, amendment, [], {"reason": "ConnectionClosedError"}, detect_gap=True)
    source_at = (execution.stamp(saved_book(lane)["position"]["deadline"])
                 if action == "time_exit" else TICK + pd.Timedelta(seconds=2))
    now[0] = source_at + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(92, price=price, at=source_at, epoch=2)], {})
    closed, = history(lane, "CLOSED")
    observed, = history(lane, "EXIT_OBSERVED")
    assert observed["reason"] == closed["reason"] == action
    assert closed["provisional"] and closed["source_event_id"] == "92"
    book = saved_book(lane)
    assert book["position"] is None
    assert book["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    assert book["cash"] == pytest.approx(10_000. + book["gross"] + book["funding"] - book["fees"])

    monkeypatch.setattr(ema, "confirmation", lambda *_args, **_kwargs: pytest.fail("Unresolved book evaluated"))
    with closing(runtime.connect()) as db, db:
        runtime.evaluate(db, record, {DAY: book}, {DAY: lane}, now[0], {})
        report = paper.book_report(db, lane)
    assert not report["accounting_complete"]
    assert report["cash_reconciliation_difference"] == pytest.approx(0., abs=1e-10)
    # Raw reporting includes provisional closes, so later resumption needs
    # a validated-results filter independent of simply clearing book.problem.
    assert report["closed_net"] == pytest.approx(closed["net"])
