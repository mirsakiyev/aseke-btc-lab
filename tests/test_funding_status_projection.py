from dataclasses import asdict

import pytest

from lab.learning.simulator import Position
from lab.paper_run import operational_status, operations, runtime
from lab.phase15 import paper
from test_paper_capabilities_membership import registered
from test_paper_operational_status import monitor


def funding_book(monitor, state="FUNDING_SETTLEMENT_PENDING", *, actionable=False, complete=False):
    db, record, publish, _, _ = monitor
    lane = record["body"]["lanes"]["breakout/swing/fixed"]
    value = paper.load(db, lane)
    funding = {"schema": operations.FUNDING_SCHEMA, "state": state, "financial_complete": complete,
               "actionable": actionable, "obligations": [{"id": "due-slot", "state": state,
                   "reason_code": "RATE_PENDING" if not actionable else "RATE_UNAVAILABLE",
                   "actionable": actionable}]}
    value["position"] = {"id": "current-exposure", "position": asdict(Position(-1, 1., 100., 10000., 100., 0., 100.)),
                         "planned_risk": 1., "last_mark": 100., "funding_state": funding,
                         "unresolved_costs": None if complete else "WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION"}
    paper.save(db, value)
    publish()
    return db, record, lane, value, publish


def test_read_only_status_displays_pending_funding_without_claiming_complete_finances(monitor):
    db, _, _, _, _ = funding_book(monitor)
    before = db.total_changes
    result = operational_status.read_status()["books"]["breakout/swing/fixed"]
    assert result["state"] == "ACTIVE" and result["reason"] is None
    assert result["position_state"] == "POSITION_OPEN"
    assert result["protection_state"] == "PROTECTION_HEALTHY"
    assert result["entry_state"] == "ENTRY_OCCUPIED"
    assert not result["prospective_entry_permission"]
    assert not result["financial_reconciliation_complete"] and not result["accounting_complete"]
    assert result["financial_state"] == "FINANCIAL_RECONCILIATION_PENDING"
    assert result["research_funding_state"] == "RESEARCH_PROVISIONAL"
    assert db.total_changes == before


@pytest.mark.parametrize("state", ["FUNDING_EVIDENCE_MISSING", "FUNDING_CONFLICT", "FINANCIAL_RECONCILIATION_BLOCKED"])
def test_read_only_status_retains_actionable_funding_cause_and_healthy_protection(monitor, state):
    funding_book(monitor, state, actionable=True)
    result = operational_status.read_status()["books"]["breakout/swing/fixed"]
    assert result["state"] == "BLOCKED" and result["reason"] == state
    assert result["entry_state"] == "ENTRY_OCCUPIED"
    assert result["protection_state"] == "PROTECTION_HEALTHY"
    assert result["financial_state"] == "FINANCIAL_RECONCILIATION_BLOCKED"


def test_source_loss_is_independent_of_pending_funding_and_nominal_error_is_not_hidden(monitor, monkeypatch):
    db, _, lane, value, publish = funding_book(monitor)
    monkeypatch.setattr(runtime, "_source_health", lambda *_args, **_kw: {"available": False, "reason_code": "SOURCE_DOWN"})
    result = operational_status.read_status()["books"]["breakout/swing/fixed"]
    assert result["state"] == "BLOCKED" and result["protection_state"] == "PROTECTION_UNAVAILABLE"
    assert result["funding_state"]["state"] == "FUNDING_SETTLEMENT_PENDING"
    value["cash"] += 1.
    paper.save(db, value)
    publish()
    result = operational_status.read_status()["books"]["breakout/swing/fixed"]
    assert result["financial_state"] == "FINANCIAL_RECONCILIATION_BLOCKED"


def test_no_projection_retains_explicit_legacy_classification_without_inventing_healthy_funding(monitor):
    db, _, _, value, publish = funding_book(monitor)
    value["position"].pop("funding_state")
    paper.save(db, value)
    publish()
    result = operational_status.read_status()["books"]["breakout/swing/fixed"]
    assert result["state"] == "BLOCKED"
    assert result["operational"]["funding"]["state"] == "LEGACY_NOT_CLASSIFIED"
    assert "funding_state" not in result
