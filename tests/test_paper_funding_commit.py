"""Financial receipt identity is authoritative over a mutable done list."""
from copy import deepcopy
from dataclasses import asdict
import json

import pandas as pd
import pytest

from lab.learning.simulator import Position
from lab.paper_run import execution, protection_reporting
from lab.phase15 import paper
from test_paper_run_execution import OBSERVED, clock, journal, lane, observation, planned, terms


def opened(db, identifier="funding-book", side=-1):
    value = {**lane(), "book_id": identifier}
    book = planned(db, value, side=side, hold=90)
    execution.entry(db, book, value, observation(OBSERVED, event_id="entry-"+identifier), terms, read_clock=clock(OBSERVED))
    paper.save(db, book)
    return value, book


def apply(db, book, *, rate=.0001, reference=100., evidence=None):
    record = book["position"]
    pos = Position(**record["position"])
    slot = OBSERVED+pd.Timedelta(minutes=1)
    result = execution.funding(db, book, record, pos, slot, (slot+pd.Timedelta(hours=1)).isoformat(),
                               rate, reference, slot+pd.Timedelta(hours=1), "KUCOIN_SETTLED_RATE_FORWARD_MARK_VALUATION",
                               evidence=evidence)
    paper.save(db, book)
    return result


def funding_rows(db):
    return [json.loads(raw) for raw, in db.execute("SELECT body FROM event WHERE kind='FUNDING' ORDER BY rowid")]


@pytest.mark.parametrize("side,rate", [(1,.0001),(-1,.0001),(-1,0.)])
def test_lost_settled_projection_and_restart_cannot_double_apply(journal, side, rate):
    with journal:
        value, book = opened(journal, side=side)
        first = apply(journal, book, rate=rate)
    expected = deepcopy(book)
    with journal:
        loaded = paper.load(journal, value)
        loaded["position"]["settled_funding"] = []
        second = apply(journal, loaded, rate=rate)
    assert first["state"] == "APPLIED" and second["state"] == "ALREADY_APPLIED"
    assert first["event_id"] == second["event_id"] and len(funding_rows(journal)) == 1
    assert loaded["cash"] == expected["cash"] and loaded["funding"] == expected["funding"]
    assert loaded["position"]["position"] == expected["position"]["position"]


def test_conflicted_receipt_or_mismatched_financial_projection_never_changes_money(journal):
    with journal:
        _, book = opened(journal)
        apply(journal, book)
    original = deepcopy(book)
    with pytest.raises(ValueError, match="FUNDING_RECEIPT_EVIDENCE_CONFLICT"):
        apply(journal, book, rate=.0002)
    assert book == original and len(funding_rows(journal)) == 1
    book["funding"] += .01
    before = deepcopy(book)
    with pytest.raises(ValueError, match="FUNDING_LEDGER_PROJECTION_MISMATCH"):
        apply(journal, book)
    assert book == before and len(funding_rows(journal)) == 1


def test_interrupted_receipt_transaction_rolls_back_and_retry_applies_once(journal):
    with journal:
        value, book = opened(journal)
    before = deepcopy(book)
    with pytest.raises(OSError, match="interrupted"):
        with journal:
            apply(journal, book)
            raise OSError("interrupted")
    assert not funding_rows(journal)
    loaded = paper.load(journal, value)
    assert loaded == before
    with journal:
        assert apply(journal, loaded)["state"] == "APPLIED"
    assert len(funding_rows(journal)) == 1


def test_same_source_obligation_has_independent_book_receipts_and_cash_updates_without_a_new_mark(journal):
    with journal:
        _, first = opened(journal, "one")
        _, second = opened(journal, "two")
        mark_at = first["position"]["mark_received_at"]
        before = first["cash"]
        one = apply(journal, first)
        two = apply(journal, second)
    assert one["event_id"] != two["event_id"] and len(funding_rows(journal)) == 2
    assert first["cash"] == pytest.approx(before+first["funding"])
    assert first["cash"] == pytest.approx(first["position"]["position"]["wallet"])
    assert first["position"]["mark_received_at"] == mark_at
    assert first["funding"] == second["funding"]


def test_native_and_forward_evidence_lineages_are_retained_without_claiming_missing_raw_bytes(journal):
    with journal:
        _, book = opened(journal)
        evidence = {"rate": {"actual_at": "2026-11-16T06:17:00Z", "raw_sha256": "a"*64,
                              "raw_bytes_verified": True},
                    "valuation": {"origin": "ORIGINAL_FORWARD_CAPTURE", "observation_sha256": "b"*64,
                                  "stored_raw_sha256": "c"*64, "original_http_bytes_retained": False}}
        apply(journal, book, evidence=evidence)
    row, = funding_rows(journal)
    assert row["evidence"] == evidence
    assert row["protocol"] == "FUNDING_SETTLEMENT_RECONCILIATION_V1"
    assert row["valuation_model"] == "ORIGINAL_FORWARD_MARK_REFERENCE"


def test_funding_provisional_close_does_not_invent_a_protection_gap_or_validate_performance(journal):
    with journal:
        value, book = opened(journal)
        book["position"]["funding_provisional"] = True
        book["position"]["funding_reconciliation"] = {"basis": "PRESERVED_ACCEPTED_FORWARD_EVIDENCE"}
        apply(journal, book)
        exit_at = OBSERVED+pd.Timedelta(minutes=70)
        # Unit precondition: regular accepted monitoring continued while the
        # source rate was pending; this test isolates the financial annotation.
        book["position"].update(mark_source_at=(exit_at-pd.Timedelta(seconds=1)).isoformat(),
                                mark_received_at=(exit_at-pd.Timedelta(milliseconds=875)).isoformat())
        execution.protect(journal, book, value, observation(exit_at, 200., event_id="exit"), read_clock=clock(exit_at))
        paper.save(journal, book)
    closed = json.loads(journal.execute("SELECT body FROM event WHERE kind='CLOSED'").fetchone()[0])
    assert closed["provisional"] and closed["funding_provisional"]
    assert book["position"] is None
    assert book["problem"] is None
    assert not journal.execute("SELECT 1 FROM event WHERE kind='PROTECTION_GAP'").fetchone()
    assert protection_reporting.summarize(journal, value, book)["validated_closed_count"] == 0
