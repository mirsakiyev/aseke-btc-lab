"""Isolated reproductions of current incident and accounting behavior."""

from contextlib import closing

import pandas as pd
import pytest

from lab.paper_run import ema, execution, runtime, source_amendment
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
