from copy import deepcopy

import pandas as pd
import pytest

from lab.paper_run import operations
from test_paper_capabilities_membership import AT, registered
from test_paper_operations import book, capture, events, position, status


def projection(value, state="FUNDING_SETTLEMENT_PENDING", *, obligation="slot-a", actionable=False, complete=False):
    result = {"schema": operations.FUNDING_SCHEMA, "state": state, "financial_complete": complete,
              "actionable": actionable, "position_id": value["position"]["id"], "obligations": [{
                  "id": obligation, "state": state, "actionable": actionable, "reason_code": "SETTLED_RATE_PENDING" if not actionable else "RATE_MISSING_AFTER_FINALITY",
                  "settlement_due_at": AT.isoformat(), "valuation_deadline": (AT+pd.Timedelta(seconds=45)).isoformat(),
                  "rate_status": "PENDING", "valuation_status": "FORWARD_CAPTURED"}]}
    value["position"]["funding_state"] = result
    return result


def opened(registered):
    db, record = registered
    lane = record["body"]["lanes"]["breakout/swing/fixed"]
    value = book(db, lane)
    value["position"] = position(lane, "protected")
    value["position"]["unresolved_costs"] = "WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION"
    return db, record, lane, value


def test_normal_funding_finality_wait_is_not_a_protection_or_operational_incident(registered):
    db, record, lane, value = opened(registered)
    projection(value)
    with db:
        capture(db, record, lane, value)
    result = status(db, record, lane, value, financial=False, research={"research_state": "VALIDATED"})
    assert result["position_state"] == "POSITION_OPEN"
    assert result["protection_state"] == "PROTECTION_HEALTHY"
    assert result["entry_state"] == "ENTRY_OCCUPIED"
    assert result["entry_permission"]["reason_code"] == "POSITION_OPEN"
    assert result["financial_state"] == "FINANCIAL_RECONCILIATION_PENDING"
    assert not result["nominal_financial_reconciliation"]
    assert result["research_funding_state"] == "RESEARCH_PROVISIONAL"
    assert not events(db, operations.OPENED)
    assert not events(db, "PROTECTION_INCIDENT_OPENED")


@pytest.mark.parametrize("state", ["FUNDING_EVIDENCE_MISSING", "FUNDING_CONFLICT", "FINANCIAL_RECONCILIATION_BLOCKED"])
def test_actionable_funding_has_obligation_specific_durable_cause_without_fake_gap(registered, state):
    db, record, lane, value = opened(registered)
    funding = projection(value, state, actionable=True)
    funding["obligations"].append({**funding["obligations"][0], "id": "slot-b"})
    before = deepcopy(value)
    with db:
        capture(db, record, lane, value)
        capture(db, record, lane, value, AT+pd.Timedelta(seconds=5))
    opened_events = events(db, operations.OPENED)
    assert len(opened_events) == 2
    assert {e["evidence"]["funding_obligation_id"] for e in opened_events} == {"slot-a", "slot-b"}
    assert all(e["reason_code"] == state and not e["requires_incident_review"] for e in opened_events)
    assert all(e["evidence"]["settlement_due_at"] == AT.isoformat() for e in opened_events)
    assert value == before
    assert not events(db, "PROTECTION_INCIDENT_OPENED")
    result = status(db, record, lane, value)
    assert result["entry_state"] == "ENTRY_OCCUPIED"
    assert result["financial_state"] == "FINANCIAL_RECONCILIATION_BLOCKED"
    assert result["entry_permission"]["reason_code"] == state
    assert result["protection_state"] == "PROTECTION_HEALTHY"


def test_source_recovery_does_not_clear_funding_obligation_and_resolution_preserves_exposure(registered):
    db, record, lane, value = opened(registered)
    projection(value, "FUNDING_EVIDENCE_MISSING", actionable=True)
    with db:
        capture(db, record, lane, value)
        capture(db, record, lane, value, AT+pd.Timedelta(seconds=1), source={"available": False, "reason_code": "SOURCE_DOWN"})
        capture(db, record, lane, value, AT+pd.Timedelta(seconds=2))
    assert len([e for e in events(db, operations.OPENED) if e["reason_code"] == "FUNDING_EVIDENCE_MISSING"]) == 1
    assert [e["reason_code"] for e in events(db, operations.CLEARED)] == ["SOURCE_DOWN"]
    projection(value, "SETTLED", complete=True)
    value["position"]["unresolved_costs"] = None
    with db:
        capture(db, record, lane, value, AT+pd.Timedelta(seconds=3))
    assert len(events(db, operations.CLEARED)) == 2
    result = status(db, record, lane, value)
    assert result["entry_state"] == "ENTRY_OCCUPIED" and not result["entry_permission"]["available"]
    assert result["financial_state"] == "RECONCILED"
    assert value["position"]["id"] == "protected"


def test_schedule_failure_has_top_level_explicit_cause_without_invented_obligations(registered):
    db, record, lane, value = opened(registered)
    funding = projection(value, "FUNDING_CONFLICT", actionable=True)
    funding.update(obligations=[], reason_code="FUNDING_SCHEDULE_INCONSISTENT")
    with db:
        capture(db, record, lane, value)
    event = events(db, operations.OPENED)[0]
    assert event["reason_code"] == "FUNDING_CONFLICT"
    assert event["evidence"]["funding_reason_code"] == "FUNDING_SCHEDULE_INCONSISTENT"
    assert not funding["obligations"]


def test_legacy_unversioned_wait_is_not_silently_reclassified_as_healthy(registered):
    db, record, lane, value = opened(registered)
    result = status(db, record, lane, value, financial=False)
    assert result["funding"]["state"] == "LEGACY_NOT_CLASSIFIED"
    assert result["entry_permission"]["reason_code"] == "FINANCIAL_RECONCILIATION_REQUIRED"
    value["position"]["funding_state"] = {"state": "SETTLED"}
    with pytest.raises(ValueError, match="FUNDING_STATUS_PROJECTION_INVALID"):
        status(db, record, lane, value)
