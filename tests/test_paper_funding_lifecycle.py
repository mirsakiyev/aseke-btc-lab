"""Funding costs remain separate from exposure, protection and source finality."""

from copy import deepcopy
from dataclasses import asdict
import json
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from lab.learning.simulator import Contract, Position
from lab.paper_run import execution, funding
from lab.phase15 import paper
from lab.storage import utc
from test_paper_capabilities_membership import seven


SLOT = utc("2026-11-19T00:00:00Z")


@pytest.fixture
def finance(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE event(key TEXT PRIMARY KEY,book TEXT,kind TEXT,idea TEXT,source_at TEXT,recorded_at TEXT,body TEXT)")
    db.execute("CREATE TABLE book(id TEXT PRIMARY KEY,state TEXT)")
    lane = seven()["breakout/swing/fixed"]
    book = paper.initial(lane)
    schedule = {"currentFundingRateGranularity": 8*60*60*1000,
                "fundingRateGranularity": 8*60*60*1000, "nextFundingRateDateTime": SLOT.value//1_000_000}
    book["position"] = {"id": "open-position", "entry_at": (SLOT-pd.Timedelta(hours=1)).isoformat(),
        "entry_cash": 10000., "last_mark": 100., "entry_fee": 0., "planned_risk": 10.,
        "deadline": (SLOT+pd.Timedelta(days=1)).isoformat(), "stop": 110., "target": 90.,
        "plan": {"funding_schedule": schedule}, "position": asdict(Position(-1, 2., 100., 10000., 100., 0., 200.))}
    now = [SLOT+pd.Timedelta(seconds=1.2)]
    state = {"state": "PENDING", "reason_code": "AWAIT_EXISTING_SCHEDULED_COLLECTION",
             "first_eligible_collection_at": (SLOT+pd.Timedelta(hours=1, seconds=45)).isoformat()}
    def rates(body, slots, at):
        return {slot.isoformat(): deepcopy(state) for slot in slots}
    monkeypatch.setattr(funding, "_rates", rates)
    monkeypatch.setattr(funding, "_archived", lambda *_: [])
    monkeypatch.setattr(execution, "clock", lambda: (now[0], now[0].value))
    metadata = {"as_of": (SLOT-pd.Timedelta(minutes=1)).isoformat(), "contract": deepcopy(schedule)}
    def observation(at=SLOT+pd.Timedelta(seconds=1), **updates):
        requested, received = at+pd.Timedelta(milliseconds=50), at+pd.Timedelta(milliseconds=100)
        point = {"source": lane["source"], "symbol": lane["symbol"], "price": 100., "price_text": "100.0",
            "status": "RECEIVED", "event_id": at.isoformat(), "source_at": at.isoformat(),
            "requested_at": requested.isoformat(), "received_at": received.isoformat(),
            "request_mono_ns": requested.value, "received_mono_ns": received.value,
            "elapsed_ns": (received-requested).value, "raw_sha256": "a"*64}
        point.update(updates)
        return point
    def available(rate=.001, actual=SLOT, receipt=None, digest="b"*64):
        state.clear()
        state.update(state="AVAILABLE", candidates=[{"raw_sha256": digest}], selected={
            "rate": rate, "actual_at": actual.isoformat(), "received_at": (receipt or now[0]).isoformat(),
            "raw_sha256": digest, "store_id": "authorized-test-store", "revision_kind": "ORIGINAL"})
    def settle(point=None):
        if not db.in_transaction:
            db.execute("BEGIN IMMEDIATE")
        return funding.settle(db, book, lane, {}, now[0], metadata, point, generation="generation")
    def assess():
        return funding.assess(db, book, lane, {}, now[0], metadata, generation="generation")
    yield SimpleNamespace(db=db, book=book, lane=lane, now=now, state=state, metadata=metadata,
                          point=observation, available=available, settle=settle, assess=assess)
    db.close()


def rows(finance, kind):
    return [json.loads(raw) for raw, in finance.db.execute("SELECT body FROM event WHERE kind=? ORDER BY rowid", (kind,))]


def test_assessment_is_read_only_and_normal_pending_is_not_a_protection_gap(finance):
    before, changes = deepcopy(finance.book), finance.db.total_changes
    result = finance.assess()
    assert result["state"] == "FUNDING_SETTLEMENT_PENDING" and not result["actionable"]
    assert not result["financial_complete"] and result["obligations"][0]["slot"] == SLOT.isoformat()
    assert finance.book == before and finance.db.total_changes == changes
    assert not rows(finance, "PROTECTION_GAP")


def test_forward_capture_is_exact_and_retains_original_full_provenance(finance):
    point = finance.point()
    assert finance.settle(point)
    reference = finance.book["position"]["funding_references"][SLOT.isoformat()]
    assert reference["price"] == point["price"]
    assert reference["source_at"] == point["source_at"] and reference["received_at"] == point["received_at"]
    assert reference["raw_sha256"] == point["raw_sha256"] and reference["request_mono_ns"] == point["request_mono_ns"]
    assert finance.book["position"]["funding_state"]["state"] == "FUNDING_SETTLEMENT_PENDING"
    assert not rows(finance, "PROTECTION_GAP")


@pytest.mark.parametrize("direction", [-1, 1])
def test_original_forward_model_settles_long_and_short_once_with_complete_hashes(finance, direction):
    finance.book["position"]["position"]["direction"] = direction
    finance.available()
    assert not finance.settle(finance.point())
    receipt, = rows(finance, "FUNDING")
    assert receipt["cash_flow"] == pytest.approx(-direction*2*100*.001)
    assert receipt["valuation_reference"] == 100. and receipt["basis"] == funding.BASIS
    assert receipt["evidence"]["rate"]["raw_sha256"] == "b"*64
    assert receipt["evidence"]["valuation"]["raw_sha256"] == "a"*64
    before = deepcopy(finance.book)
    assert not finance.settle()
    assert finance.book == before and len(rows(finance, "FUNDING")) == 1


def test_late_rate_uses_preserved_forward_value_and_exposes_uncertain_economic_interval(finance):
    finance.settle(finance.point())
    finance.now[0] = SLOT+pd.Timedelta(hours=1, seconds=46)
    finance.available()
    assert not finance.settle()
    item, = finance.book["position"]["funding_state"]["obligations"]
    assert item["state"] == "SETTLED"
    assert item["economic_timing_interval"]["effective_settlement_at"] == SLOT.isoformat()
    assert item["economic_timing_interval"]["accounted_at"] == finance.now[0].isoformat()
    assert finance.book["position"]["funding_provisional"]
    assert not finance.book["position"].get("unresolved_protection") and not rows(finance, "PROTECTION_GAP")


def test_missed_window_never_uses_a_later_current_mark_or_fabricates_a_reference(finance):
    finance.now[0] = SLOT+pd.Timedelta(minutes=15)
    finance.available()
    assert finance.settle(finance.point(at=finance.now[0]-pd.Timedelta(milliseconds=200)))
    value = finance.book["position"]["funding_state"]
    assert value["state"] == "FUNDING_EVIDENCE_MISSING" and value["reason_code"] == "FORWARD_VALUATION_WINDOW_ELAPSED"
    assert not finance.book["position"]["funding_references"] and not rows(finance, "FUNDING")
    assert not finance.book["position"].get("unresolved_protection")


@pytest.mark.parametrize("damage", ["future_receipt", "stale_receipt", "wrong_source", "outside_window"])
def test_invalid_stored_reference_stays_unresolved(finance, damage):
    point = finance.point()
    point = {key: point[key] for key in ("price", "source_at", "received_at", "source")}
    if damage == "future_receipt":point["received_at"] = (SLOT+pd.Timedelta(days=1)).isoformat()
    if damage == "stale_receipt":point["received_at"] = (SLOT+pd.Timedelta(seconds=4)).isoformat()
    if damage == "wrong_source":point["source"] = "KRAKEN_BTC_USD_LAST_TRADE"
    if damage == "outside_window":point["source_at"] = (SLOT+pd.Timedelta(seconds=46)).isoformat()
    finance.book["position"]["funding_references"] = {SLOT.isoformat(): point}
    finance.available()
    assert finance.settle()
    assert finance.book["position"]["funding_state"]["reason_code"] == "INVALID_FORWARD_VALUATION_EVIDENCE"
    assert not rows(finance, "FUNDING")


def test_preserved_candidate_is_review_only_until_supported_restoration(finance, monkeypatch):
    finance.now[0] = SLOT+pd.Timedelta(minutes=5)
    finance.available()
    point = finance.point()
    monkeypatch.setattr(funding, "_archived", lambda *_: [point])
    assert finance.settle()
    item, = finance.book["position"]["funding_state"]["obligations"]
    assert item["preserved_forward_candidates"] == [point] and not rows(finance, "FUNDING")
    assert not finance.book["position"]["funding_references"]
    finance.book["position"]["funding_references"][SLOT.isoformat()] = {**point, "recovery_label": "PRESERVED_ACCEPTED_FORWARD_EVIDENCE"}
    assert not finance.settle()
    assert finance.book["position"]["funding_provisional"]


def test_late_rate_correction_never_rewrites_an_applied_amount(finance):
    finance.available()
    finance.settle(finance.point())
    before = deepcopy(finance.book)
    receipt = rows(finance, "FUNDING")
    finance.available(rate=.002, digest="c"*64)
    assert finance.settle()
    assert finance.book["position"]["funding_state"]["reason_code"] == "APPLIED_FUNDING_CORRECTION_REVIEW_REQUIRED"
    assert finance.book["funding"] == before["funding"] and finance.book["position"]["position"] == before["position"]["position"]
    assert rows(finance, "FUNDING") == receipt


def test_conflicting_native_revisions_block_even_before_first_application(finance):
    finance.state.update(state="CONFLICT", reason_code="NATIVE_REVISION_CONFLICT")
    assert finance.settle(finance.point())
    assert finance.book["position"]["funding_state"]["state"] == "FUNDING_CONFLICT"
    assert not rows(finance, "FUNDING")


def test_zero_is_an_observed_rate_and_not_inferred_from_missing_evidence(finance):
    finance.available(rate=0.)
    assert not finance.settle(finance.point())
    receipt, = rows(finance, "FUNDING")
    assert receipt["rate"] == receipt["cash_flow"] == finance.book["funding"] == 0.


def test_matching_grid_accepts_a_rolled_next_anchor_but_rejects_an_early_regime_change(finance):
    finance.metadata["contract"]["nextFundingRateDateTime"] += 8*60*60*1000
    assert finance.assess()["schedule"]["state"] == "COMPATIBLE"
    finance.metadata["contract"]["currentFundingRateGranularity"] = 4*60*60*1000
    result = finance.assess()
    assert result["state"] == "FUNDING_CONFLICT"
    assert result["schedule"]["frozen_slots"] == result["schedule"]["current_slots"]
    assert result["schedule"]["frozen_grid"] != result["schedule"]["current_grid"]


def test_obligation_capacity_is_checked_before_allocating_or_reading_source_history(finance, monkeypatch):
    finance.metadata["contract"]["currentFundingRateGranularity"] = 1
    monkeypatch.setattr(funding, "_rates", lambda *_: (_ for _ in ()).throw(AssertionError("Source lookup must not run")))
    result = finance.assess()
    assert result["state"] == "FINANCIAL_RECONCILIATION_BLOCKED"
    assert result["reason_code"] == "FUNDING_OBLIGATION_LIMIT_REVIEW_REQUIRED"


def test_verified_actual_settlement_before_entry_is_excluded_without_a_fake_zero_payment(finance):
    finance.book["position"]["entry_at"] = (SLOT-pd.Timedelta(seconds=2)).isoformat()
    finance.available(actual=SLOT-pd.Timedelta(seconds=3))
    assert not finance.settle()
    item, = finance.book["position"]["funding_state"]["obligations"]
    assert item["state"] == "NOT_APPLICABLE_EXCHANGE_TIMESTAMP" and item["financial_complete"]
    assert not rows(finance, "FUNDING") and finance.book["funding"] == 0.


def test_available_but_future_native_rate_is_not_applied(finance):
    finance.available(receipt=SLOT+pd.Timedelta(days=1))
    assert finance.settle(finance.point())
    assert finance.book["position"]["funding_state"]["reason_code"] == "INVALID_SETTLED_RATE_EVIDENCE"
    assert not rows(finance, "FUNDING")


@pytest.mark.parametrize("as_of,reason", [
    (None, "FUNDING_CURRENT_METADATA_UNVERIFIED"),
    ("invalid", "FUNDING_CURRENT_METADATA_INVALID"),
    ("2026-11-18T23:59:00", "FUNDING_CURRENT_METADATA_INVALID"),
    ("2026-11-20T00:00:00Z", "FUNDING_CURRENT_METADATA_FUTURE"),
])
def test_unqualified_current_metadata_blocks_payment_without_hiding_frozen_obligations(finance, as_of, reason):
    before = finance.assess()
    obligation_id = before["obligations"][0]["id"]
    finance.metadata["as_of"] = as_of
    finance.available()
    assert finance.settle(finance.point())
    value = finance.book["position"]["funding_state"]
    assert value["state"] == "FINANCIAL_RECONCILIATION_BLOCKED" and value["reason_code"] == reason
    assert value["schedule"]["frozen_slots"] == [SLOT.isoformat()]
    item, = value["obligations"]
    assert item["id"] == obligation_id and item["slot"] == SLOT.isoformat()
    assert item["valuation_deadline"] == (SLOT+pd.Timedelta(seconds=45)).isoformat()
    assert finance.book["position"]["funding_references"][SLOT.isoformat()]["source_at"] == finance.point()["source_at"]
    assert not rows(finance, "FUNDING") and finance.book["funding"] == 0.
    assert not finance.book["position"].get("unresolved_protection")


def test_fresh_metadata_can_release_payment_using_preserved_timely_mark_without_new_age_policy(finance):
    finance.available()
    finance.metadata.pop("as_of")
    assert finance.settle(finance.point())
    finance.now[0] += pd.Timedelta(seconds=15)
    finance.metadata["as_of"] = (SLOT-pd.Timedelta(days=2)).isoformat()
    # This release checks missing/invalid/future timestamps. It does not invent
    # a new metadata maximum age; changing age policy requires separate review.
    assert not finance.settle()
    assert len(rows(finance, "FUNDING")) == 1
    assert rows(finance, "FUNDING")[0]["valuation_reference"] == 100.


def test_incompatible_current_schedule_is_not_unioned_or_applied(finance):
    finance.now[0] = SLOT+pd.Timedelta(hours=5)
    finance.metadata["contract"]["currentFundingRateGranularity"] = 4*60*60*1000
    finance.available()
    assert finance.settle()
    value = finance.book["position"]["funding_state"]
    assert value["state"] == "FUNDING_CONFLICT" and value["reason_code"] == "FUNDING_SCHEDULE_CONFLICT"
    assert value["obligations"] == [] and len(value["schedule"]["frozen_slots"]) == 1
    assert len(value["schedule"]["current_slots"]) == 2
    assert not rows(finance, "FUNDING")


def test_lost_done_projection_is_recovered_from_receipt_without_duplicate_application(finance):
    finance.available()
    finance.settle(finance.point())
    before = finance.book["funding"]
    finance.book["position"]["settled_funding"] = []
    assert not finance.settle()
    assert finance.book["position"]["settled_funding"] == [SLOT.isoformat()]
    assert finance.book["funding"] == before and len(rows(finance, "FUNDING")) == 1


def test_claimed_settled_slot_without_a_receipt_is_a_financial_block(finance):
    finance.available()
    finance.book["position"]["settled_funding"] = [SLOT.isoformat()]
    assert finance.settle(finance.point())
    assert finance.book["position"]["funding_state"]["state"] == "FINANCIAL_RECONCILIATION_BLOCKED"
    assert not rows(finance, "FUNDING")


def test_existing_receipt_never_hides_a_changed_account_financial_projection(finance):
    finance.available()
    finance.settle(finance.point())
    finance.book["funding"] += 1.
    amount = finance.book["funding"]
    assert finance.settle()
    assert finance.book["position"]["funding_state"]["state"] == "FINANCIAL_RECONCILIATION_BLOCKED"
    assert finance.book["funding"] == amount and len(rows(finance, "FUNDING")) == 1


def test_execution_guard_failure_remains_a_financial_block_without_stopping_protection(finance, monkeypatch):
    finance.available()
    before = deepcopy(finance.book["position"]["position"])
    monkeypatch.setattr(execution, "funding", lambda *_a, **_k: (_ for _ in ()).throw(ValueError("FUNDING_LEDGER_PROJECTION_MISMATCH")))
    assert finance.settle(finance.point())
    assert finance.book["position"]["position"] == before
    assert finance.book["position"]["funding_state"]["state"] == "FINANCIAL_RECONCILIATION_BLOCKED"
    assert not finance.book["position"].get("unresolved_protection")


@pytest.mark.parametrize("field,value", [("direction", 0), ("direction", True), ("remaining", 0.), ("wallet", float("inf"))])
def test_invalid_position_financial_model_cannot_manufacture_a_zero_or_nonfinite_cost(finance, field, value):
    finance.available()
    finance.book["position"]["position"][field] = value
    assert finance.settle(finance.point())
    assert finance.book["position"]["funding_state"]["state"] == "FINANCIAL_RECONCILIATION_BLOCKED"
    assert finance.book["funding"] == 0. and not rows(finance, "FUNDING")


def test_status_transition_retries_and_restarts_do_not_duplicate_events(finance):
    finance.settle(finance.point())
    before = len(rows(finance, funding.TRANSITION))
    finance.settle()
    assert len(rows(finance, funding.TRANSITION)) == before
    finance.book["position"].pop("funding_state")
    finance.settle()
    assert len(rows(finance, funding.TRANSITION)) == before
    finance.state.update(state="CONFLICT", reason_code="NATIVE_REVISION_CONFLICT")
    finance.settle()
    assert len(rows(finance, funding.TRANSITION)) == before+1
    finance.state.update(state="PENDING", reason_code="AWAIT_EXISTING_SCHEDULED_COLLECTION")
    finance.settle()
    finance.state.update(state="CONFLICT", reason_code="NATIVE_REVISION_CONFLICT")
    finance.settle()
    assert len(rows(finance, funding.TRANSITION)) == before+3
    assert rows(finance, funding.TRANSITION)[-1]["transition_sequence"] == before+3
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        finance.db.execute("DELETE FROM event WHERE kind=?", (funding.TRANSITION,))


def test_simultaneous_books_share_source_evidence_and_keep_independent_cash_flows(finance):
    other = deepcopy(finance.book)
    other["book_id"] = "separate-trailing-book"
    other["position"]["id"] = "separate-trailing-position"
    other_lane = {**finance.lane, "book_id": other["book_id"]}
    finance.available()
    finance.settle(finance.point())
    assert not funding.settle(finance.db, other, other_lane, {}, finance.now[0], finance.metadata, finance.point(), generation="generation")
    assert finance.book["funding"] == other["funding"] and len(rows(finance, "FUNDING")) == 2
    assert len({row["evidence"]["obligation_id"] for row in rows(finance, "FUNDING")}) == 2


def test_interrupted_transaction_rolls_back_cost_and_status_journals(finance):
    finance.available()
    original = deepcopy(finance.book)
    finance.settle(finance.point())
    finance.db.rollback()
    assert not rows(finance, "FUNDING") and not rows(finance, funding.TRANSITION)
    finance.book.clear();finance.book.update(original)
    assert not finance.settle(finance.point()) and len(rows(finance, "FUNDING")) == 1


def test_exit_observation_waits_for_costs_without_reinventing_an_exit(finance, monkeypatch):
    finance.book["position"]["exit_observed"] = {"exit_at": finance.now[0].isoformat(), "price": 100., "reason": "time_exit"}
    calls = []
    monkeypatch.setattr(execution, "close", lambda *_args: calls.append(True))
    assert finance.settle(finance.point()) and not calls
    finance.available()
    assert not finance.settle() and calls == [True]
    assert not rows(finance, "EXIT_OBSERVED")  # This module never fabricates an exit.


def test_real_target_protection_observes_an_exit_while_financial_evidence_is_pending(finance):
    record = finance.book["position"]
    record["plan"].update(contract=asdict(Contract("linear_perp", "USDT", 1., 1., .1, 1., 999999., 10., .004, .005)),
                          cost={"taker": .001})
    prior = SLOT-pd.Timedelta(seconds=10)
    record.update(mark_source_at=prior.isoformat(), mark_received_at=prior.isoformat(), mark_event_id=prior.isoformat())
    point = finance.point(price=89., price_text="89.0")
    assert finance.settle(point)
    execution.protect(finance.db, finance.book, finance.lane, point, unresolved_costs=True, read_clock=execution.clock)
    observed, = rows(finance, "EXIT_OBSERVED")
    assert observed["price"] == 89. and observed["reason"] == "target"
    assert finance.book["position"] and finance.book["position"]["exit_observed"]
    assert not rows(finance, "CLOSED") and not rows(finance, "PROTECTION_GAP")


def test_funding_settlement_requires_a_writer_transaction(finance):
    with pytest.raises(ValueError, match="authoritative writer transaction"):
        funding.settle(finance.db, finance.book, finance.lane, {}, finance.now[0], finance.metadata)
