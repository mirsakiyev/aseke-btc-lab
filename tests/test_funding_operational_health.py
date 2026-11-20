import json

import pandas as pd

from lab.health import fresh_paper_health as monitor
from test_fresh_paper_health import NOW, alerts, event, save_book, scan, world


def occupied(world, state="FUNDING_SETTLEMENT_PENDING", *, actionable=False, complete=False):
    book = world.view["books"]["ema/day"]
    obligation = {"id": "exact-obligation", "state": state, "actionable": actionable,
                  "reason_code": "RATE_PENDING" if not actionable and not complete else "RATE_MISSING" if actionable else "SETTLED",
                  "settlement_due_at": NOW.isoformat(), "valuation_deadline": (NOW+pd.Timedelta(seconds=45)).isoformat()}
    funding = {"schema": monitor.FUNDING_SCHEMA, "state": state, "financial_complete": complete,
               "actionable": actionable, "obligations": [obligation]}
    book.update(state="BLOCKED" if actionable else "ACTIVE", reason=state if actionable else None,
                problem=None, historical_problem=None, prospective_entry_permission=False,
                position={"id": "existing", "funding_state": funding,
                          "unresolved_costs": None if complete else "WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION"},
                operational={"entry_permission": {"available": False, "reason_code": state if actionable else "POSITION_OPEN"},
                             "funding": funding})
    save_book(world, "ema/day")
    return obligation


def funding_event(world, obligation):
    return event(world, "FUNDING_STATUS_CHANGED", schema=monitor.FUNDING_SCHEMA,
                 obligation_id=obligation["id"], position_id="existing", **{k: v for k, v in obligation.items() if k != "id"})


def test_normal_pending_funding_never_sends_a_false_protection_or_urgent_book_block(world):
    occupied(world)
    first = scan(world)
    funding_event(world, occupied(world))
    scan(world, at=NOW+pd.Timedelta(seconds=2))
    assert not alerts()
    assert not first["books"]["ema/day"]["entry_available"]


def test_funding_canonical_event_op_transition_and_snapshot_have_one_notice(world):
    occupied(world)
    scan(world)
    obligation = occupied(world, "FUNDING_EVIDENCE_MISSING", actionable=True)
    funding_event(world, obligation)
    event(world, "OPERATIONAL_BLOCK_OPENED", episode_id="funding-episode", reason_code="FUNDING_EVIDENCE_MISSING",
          evidence={"funding_status_schema": monitor.FUNDING_SCHEMA, "funding_obligation_id": obligation["id"]})
    scan(world)
    scan(world, at=NOW+pd.Timedelta(seconds=5))
    rows = alerts()
    assert len(rows) == 1
    assert "FUNDING EVIDENCE REQUIRED" in rows[0]["text"]
    assert "PROTECTION INCIDENT" not in rows[0]["text"]
    cause = json.loads(rows[0]["event_json"])["operational_event"]
    assert cause["obligation_id"] == "exact-obligation" and cause["settlement_due_at"] == NOW.isoformat()


def test_current_actionable_funding_is_visible_on_commission_without_replaying_old_events(world):
    obligation = occupied(world, "FUNDING_CONFLICT", actionable=True)
    funding_event(world, obligation)
    scan(world)
    scan(world)
    assert len(alerts()) == 1
    assert "FUNDING EVIDENCE REQUIRED" in alerts()[0]["text"]


def test_resolved_funding_notice_says_position_occupied_and_preserves_uncertainty(world):
    occupied(world)
    scan(world)
    funding_event(world, occupied(world, "FUNDING_EVIDENCE_MISSING", actionable=True))
    event(world, "OPERATIONAL_BLOCK_OPENED", episode_id="funding-episode", reason_code="FUNDING_EVIDENCE_MISSING",
          evidence={"funding_status_schema": monitor.FUNDING_SCHEMA})
    scan(world)
    obligation = occupied(world, "SETTLED", complete=True)
    world.view["books"]["ema/day"]["historical_problem"] = "UNRESOLVED_PROTECTION_INTERVAL"
    save_book(world, "ema/day")
    funding_event(world, obligation)
    event(world, "OPERATIONAL_BLOCK_CLEARED", episode_id="funding-episode", reason_code="FUNDING_EVIDENCE_MISSING",
          evidence={"funding_status_schema": monitor.FUNDING_SCHEMA})
    result = scan(world)
    texts = [row["text"] for row in alerts()]
    assert len(texts) == 2
    assert "FUNDING EVIDENCE RESOLVED" in texts[-1]
    assert "Position remains open; entry is occupied" in texts[-1]
    assert "Historical uncertainty" in texts[-1]
    assert not any("ENTRY ELIGIBILITY RESTORED" in text for text in texts)
    assert not result["books"]["ema/day"]["entry_available"]


def test_source_reason_churn_does_not_repeat_same_funding_obligation(world):
    occupied(world, "FUNDING_EVIDENCE_MISSING", actionable=True)
    scan(world)
    book = world.view["books"]["ema/day"]
    book["reason"] = "STALE_REFERENCE"
    book["operational"]["entry_permission"]["reason_code"] = "STALE_REFERENCE"
    save_book(world, "ema/day")
    scan(world)
    occupied(world, "FUNDING_EVIDENCE_MISSING", actionable=True)
    scan(world)
    assert sum("FUNDING EVIDENCE REQUIRED" in row["text"] for row in alerts()) == 1
    assert sum("Reason STALE_REFERENCE" in row["text"] for row in alerts()) == 1


def test_later_conflict_reopening_for_same_obligation_is_a_new_deduplicated_transition(world):
    occupied(world)
    scan(world)
    for state, actionable, complete in (("FUNDING_CONFLICT", True, False), ("SETTLED", False, True),
                                        ("FUNDING_CONFLICT", True, False)):
        funding_event(world, occupied(world, state, actionable=actionable, complete=complete))
        scan(world)
    assert len(alerts()) == 3
    assert len({row["id"] for row in alerts()}) == 3
    scan(world)
    assert len(alerts()) == 3


def test_failure_clearance_that_still_awaits_settlement_does_not_claim_financial_resolution(world):
    occupied(world, "FUNDING_EVIDENCE_MISSING", actionable=True)
    scan(world)
    funding_event(world, occupied(world))
    scan(world)
    text = alerts()[-1]["text"]
    assert "FUNDING EVIDENCE IMPROVED" in text
    assert "settlement remains pending" in text
    assert "FUNDING EVIDENCE RESOLVED" not in text
    assert "ENTRY ELIGIBILITY RESTORED" not in text


def test_funding_schedule_event_and_snapshot_share_exact_synthetic_identity(world):
    occupied(world)
    scan(world)
    book = world.view["books"]["ema/day"]
    funding = book["position"]["funding_state"]
    funding.update(state="FUNDING_CONFLICT", reason_code="FUNDING_SCHEDULE_CONFLICT", actionable=True,
                   status_obligation_id="actual-schedule-scope", obligations=[])
    book.update(state="BLOCKED", reason="FUNDING_CONFLICT")
    book["operational"]["entry_permission"]["reason_code"] = "FUNDING_CONFLICT"
    save_book(world, "ema/day")
    event(world, "FUNDING_STATUS_CHANGED", schema=monitor.FUNDING_SCHEMA, obligation_id="actual-schedule-scope",
          state="FUNDING_CONFLICT", actionable=True, reason_code="FUNDING_SCHEDULE_CONFLICT")
    event(world, "OPERATIONAL_BLOCK_OPENED", episode_id="schedule-cause", reason_code="FUNDING_CONFLICT",
          evidence={"funding_status_schema": monitor.FUNDING_SCHEMA})
    scan(world)
    assert len(alerts()) == 1


def test_inconsistent_snapshot_cannot_claim_funding_resolution_without_a_committed_event(world):
    occupied(world, "FUNDING_EVIDENCE_MISSING", actionable=True)
    scan(world)
    occupied(world, "SETTLED", complete=True)
    scan(world, consistent=False)
    assert len(alerts()) == 1
    assert "FUNDING EVIDENCE REQUIRED" in alerts()[0]["text"]
    scan(world)
    assert len(alerts()) == 2
    assert "FUNDING EVIDENCE RESOLVED" in alerts()[-1]["text"]
