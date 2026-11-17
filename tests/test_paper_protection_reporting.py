import json
import sqlite3
from contextlib import closing

import pandas as pd
import pytest

from lab.paper_run import ema, execution, presentation, runtime, source_amendment
from lab.paper_run import protection_reporting as reporting
from lab.phase15 import paper
from test_paper_run_live_amendment import (
    APPROVED, DAY, QUALIFICATION, TICK, generation, history, intent, live_point, mono,
)
from test_paper_run_runtime import KRAKEN_BOOK, initialize, setup as runtime_setup


@pytest.fixture
def journal():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE event(key TEXT PRIMARY KEY,book TEXT,kind TEXT,idea TEXT,source_at TEXT,recorded_at TEXT,body TEXT)")
    yield db
    db.close()


def append(db, kind, position, *, book_id="book", **body):
    number = db.execute("SELECT count(*) FROM event").fetchone()[0]
    db.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)",
               (str(number), book_id, kind, position, "2026-11-17T10:00:00Z", "2026-11-17T10:00:00Z", json.dumps(body)))


def close(db, position, net, *, book_id="book", **body):
    append(db, "CLOSED", position, book_id=book_id,
           net=net, net_r=net / 10, fees=1., funding=0., holding_minutes=5., **body)


def summary(db, **book):
    return reporting.summarize(db, {"book_id": "book"}, {"position": None, **book})


def test_clean_old_outcomes_keep_validated_aggregates(journal):
    close(journal, "one", 10.)
    close(journal, "two", -4.)
    before = journal.total_changes
    result = summary(journal)
    assert result["research_state"] == "VALIDATED"
    assert result["nominal_closed_net"] == result["validated_closed_net"] == 6.
    assert result["validated_expectancy"] == 3.
    assert result["validated_expectancy_r"] == pytest.approx(.3)
    assert result["validated_win_rate"] == .5
    assert journal.total_changes == before


def test_provisional_close_and_later_capital_dependents_stay_excluded_after_problem_cleared(journal):
    close(journal, "clean", 10.)
    close(journal, "uncertain", -5., provisional=True)
    append(journal, "BOOK_PROSPECTIVE_RESUME", "uncertain", operator="operator", reason="Continue forward")
    close(journal, "dependent", 20., provisional=False)
    result = summary(journal, problem=None)
    assert result["nominal_closed_net"] == 25.
    assert result["validated_closed_net"] == 10.
    assert result["validated_closed_count"] == 1 and result["excluded_closed_count"] == 2
    assert result["outcomes"][1]["exclusion_reasons"] == ["PROVISIONAL_CLOSE"]
    assert "CAPITAL_DEPENDENCY" in result["outcomes"][2]["exclusion_reasons"]
    assert result["outcomes"][2]["capital_dependency"]["position_ids"] == ["uncertain"]
    assert result["research_state"] == "QUARANTINED"


def test_adjudication_does_not_silently_promote_provisional_close(journal):
    append(journal, "PROTECTION_INCIDENT_OPENED", "uncertain", incident_id="incident", position_id="uncertain")
    close(journal, "uncertain", 20., provisional=True)
    append(journal, "PROTECTION_INCIDENT_ADJUDICATED", "uncertain", incident_id="incident",
           position_id="uncertain", outcome="NO_EXIT_THRESHOLD_CROSSING_SUPPORTED")
    result = summary(journal)
    assert result["validated_closed_count"] == 0 and result["excluded_closed_count"] == 1
    assert result["outcomes"][0]["incident_ids"] == ["incident"]
    assert result["capital_dependency"]["incident_ids"] == ["incident"]


@pytest.mark.parametrize("annotation", [
    {"operator_intervened": True}, {"exit_kind": "OPERATOR_REQUESTED_PAPER_EXIT"},
])
def test_operator_exit_and_dependent_results_remain_excluded(journal, annotation):
    close(journal, "administrative", 20., provisional=False, **annotation)
    close(journal, "dependent", 4., provisional=False)
    result = summary(journal)
    assert result["validated_closed_count"] == 0
    assert result["nominal_closed_net"] == 24.
    assert result["outcomes"][0]["exclusion_reasons"] == ["OPERATOR_INTERVENED"]
    assert "CAPITAL_DEPENDENCY" in result["outcomes"][1]["exclusion_reasons"]


def test_later_legacy_import_invalidates_matching_historical_close_and_dependents(journal):
    close(journal, "earlier", 3.)
    close(journal, "uncertain", 10., provisional=False)
    close(journal, "dependent", 6.)
    append(journal, "PROTECTION_INCIDENT_IMPORTED", "uncertain", incident_id="legacy-incident", position_id="uncertain")
    result = summary(journal)
    assert result["validated_closed_net"] == 3.
    assert result["excluded_closed_count"] == 2
    assert "HISTORICAL_PROTECTION_INCIDENT" in result["outcomes"][1]["exclusion_reasons"]
    assert "CAPITAL_DEPENDENCY" in result["outcomes"][2]["exclusion_reasons"]


def test_explicit_resume_and_fill_provenance_survive_restart(journal, tmp_path):
    append(journal, "BOOK_PROSPECTIVE_RESUME", "old-position", capital_dependency={
        "incident_ids": ["incident"], "position_ids": ["old-position"], "segment_id": "forward-segment"})
    close(journal, "dependent", 4., research_provenance={"incident_ids": ["incident"], "position_ids": ["old-position"]})
    path = tmp_path / "saved.sqlite3"
    target = sqlite3.connect(path)
    journal.commit()
    journal.backup(target)
    target.close()
    with sqlite3.connect(path) as restored:
        result = summary(restored)
    assert result["excluded_closed_count"] == 1
    assert result["outcomes"][0]["capital_dependency"] == {
        "incident_ids": ["incident"], "position_ids": ["old-position"], "segment_ids": ["forward-segment"]}


def test_current_open_uncertainty_preserves_earlier_clean_results(journal):
    close(journal, "earlier", 8.)
    append(journal, "PROTECTION_GAP", "open-position", reason="KRAKEN_STREAM_DISCONTINUITY")
    result = summary(journal, position={"id": "open-position", "unresolved_protection": True})
    assert result["validated_closed_net"] == 8.
    assert result["validated_closed_count"] == 1 and result["current_position_historical_uncertainty"]
    assert result["research_state"] == "QUARANTINED"


def test_malformed_close_cannot_enter_validation_or_leave_capital_clean(journal):
    append(journal, "CLOSED", "incomplete", net=2.)
    close(journal, "dependent", 5.)
    result = summary(journal)
    assert result["validated_closed_count"] == 0
    assert result["outcomes"][0]["exclusion_reasons"] == ["INCOMPLETE_OUTCOME_ACCOUNTING"]
    assert "CAPITAL_DEPENDENCY" in result["outcomes"][1]["exclusion_reasons"]


def test_runtime_and_presentation_use_validated_results_after_problem_removed(runtime_setup):
    _, lanes = initialize(runtime_setup)
    lane = lanes[KRAKEN_BOOK]
    book_id = lane["book_id"]
    with closing(runtime.connect()) as db, db:
        close(db, "clean", 10., book_id=book_id)
        close(db, "uncertain", -5., provisional=True, book_id=book_id)
        append(db, "BOOK_PROSPECTIVE_RESUME", "uncertain", book_id=book_id,
               capital_dependency={"incident_ids": ["incident"], "position_ids": ["uncertain"],
                                   "segment_id": "continued-research"})
        close(db, "dependent", 20., book_id=book_id)
        book = paper.load(db, lane)
        book.update(cash=10_025., equity=10_025., fees=3., gross=28., raw_gross=28., problem=None)
        paper.save(db, book)
    with closing(runtime.connect(readonly=True)) as db:
        before = db.execute("SELECT state FROM book WHERE id=?", (book_id,)).fetchone()[0]

    status = runtime.status()["books"][KRAKEN_BOOK]
    assert status["cash"] == 10_025. and status["financial_reconciliation_complete"]
    assert status["nominal_closed_net"] == 25.
    assert status["closed_net"] == status["validated_closed_net"] == 10.
    assert status["expectancy"] == 10. and status["expectancy_r"] == 1.
    assert status["closed_fees"] == 1. and status["validated_closed_count"] == 1
    assert status["excluded_closed_count"] == 2 and status["research_state"] == "QUARANTINED"
    view = presentation.current_view(family="ema")["books"][KRAKEN_BOOK]
    assert view["win_rate"] == 1.  # Counting all three closes would yield 2/3.
    assert view["closed_net"] == 10. and view["nominal_closed_net"] == 25.
    with closing(runtime.connect(readonly=True)) as db:
        assert db.execute("SELECT state FROM book WHERE id=?", (book_id,)).fetchone()[0] == before


def test_forward_fill_and_close_keep_research_dependency_annotation(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment, side=-1)
    dependency = {"incident_ids": ["old-incident"], "position_ids": ["old-position"], "segment_id": "forward"}
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, lane)
        book["prospective_admission"] = {"capital_dependency": dependency}

        def terms(account, definition, plan, price, at):
            return ema.entry_terms(account, definition, plan, {}, record["body"]["config"], price, at)

        execution.entry(db, book, lane, live_point(91), terms,
                        read_clock=lambda: (now[0], mono(now[0])))
        assert book["position"]["research_provenance"] == dependency
        source_at = TICK + pd.Timedelta(seconds=2)
        now[0] = source_at + pd.Timedelta(milliseconds=125)
        execution.protect(db, book, lane, live_point(92, price="105.1", at=source_at),
                          read_clock=lambda: (now[0], mono(now[0])))
        paper.save(db, book)
        result = reporting.summarize(db, lane, book)
    assert history(lane, "HYPOTHETICAL_FILL")[0]["research_provenance"] == dependency
    closed, = history(lane, "CLOSED")
    assert not closed["provisional"] and closed["research_provenance"] == dependency
    assert result["validated_closed_count"] == 0 and result["excluded_closed_count"] == 1
    assert result["outcomes"][0]["exclusion_reasons"] == ["CAPITAL_DEPENDENCY"]
