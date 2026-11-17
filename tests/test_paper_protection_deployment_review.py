from contextlib import closing

import pandas as pd
import pytest

from lab.paper_run import protection_commands as commands, runtime
from lab.phase15 import paper
from test_paper_protection_lifecycle import (
    adjudicate, generation, interruption, lifecycle, records, request, insufficient_note,
)


def funding_marker(context, at):
    record, now, _, lane = context
    incident, = records(lane)
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, lane)
        paper.event(db, book, "FUNDING", incident["position_id"], at, at,
                    {"settlement_at": at.isoformat(), "cash_flow": 0.})
    return incident


def test_closed_position_review_ignores_financing_after_the_incident(lifecycle):
    _, _, _, lane = lifecycle
    interruption(lifecycle, close=True)
    incident = funding_marker(lifecycle, pd.Timestamp("2026-11-18T00:00:00Z"))
    with closing(runtime.connect(readonly=True)) as db:
        book = paper.load(db, lane)
        original = commands._position(db, book, incident["position_id"],
                                      through=incident["interval"]["ends_at"])
    assert original["position"]["funding"] == 0.
    assert original["stop"] == 105. and original["target"] == 95.


def test_financing_within_reviewed_interval_requires_a_segmented_model(lifecycle):
    _, _, _, lane = lifecycle
    interruption(lifecycle, close=True)
    incident, = records(lane)
    change_at = pd.Timestamp(incident["detected_at"]) + pd.Timedelta(milliseconds=50)
    funding_marker(lifecycle, change_at)
    with closing(runtime.connect(readonly=True)) as db:
        book = paper.load(db, lane)
        with pytest.raises(ValueError, match="segmented"):
            commands._position(db, book, incident["position_id"], through=incident["interval"]["ends_at"])


def test_explicit_insufficiency_still_supports_a_closed_changed_model(lifecycle, tmp_path):
    interruption(lifecycle, close=True)
    incident, = records(lifecycle[-1])
    funding_marker(lifecycle, pd.Timestamp(incident["detected_at"]) + pd.Timedelta(milliseconds=50))
    result, _ = adjudicate(lifecycle, tmp_path)
    assert result["record"]["outcome"] == "INSUFFICIENT_EVIDENCE"
    assert result["record"]["historical_coverage"] == "UNRESOLVED"


def test_interval_change_at_right_boundary_is_not_inside_half_open_review(lifecycle):
    _, _, _, lane = lifecycle
    interruption(lifecycle, close=True)
    incident, = records(lane)
    funding_marker(lifecycle, pd.Timestamp(incident["interval"]["ends_at"]))
    with closing(runtime.connect(readonly=True)) as db:
        book = paper.load(db, lane)
        assert commands._position(db, book, incident["position_id"], through=incident["interval"]["ends_at"])
